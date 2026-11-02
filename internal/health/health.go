// Package health decides which replicas may receive traffic. It combines
// active checks (GET /health on a timer) with passive outlier detection
// (consecutive failed requests), ejects a failing replica for an
// exponentially growing period, and re-admits it only after it passes a
// health check.
package health

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/pool"
)

type Config struct {
	// Interval between active checks of every replica; 0 disables them, and
	// an ejected replica is then re-admitted as soon as its ejection expires.
	Interval time.Duration
	Timeout  time.Duration // per check (default 1s)
	// UnhealthyThreshold consecutive failed checks eject a replica (default 2).
	UnhealthyThreshold int

	// ConsecutiveFailures failed requests in a row eject a replica; 0
	// disables passive detection. Only failures that implicate the replica
	// count: unreachable, 5xx, broken streams, first-token timeouts. A 503 is
	// backpressure, not a fault, and a 4xx is the client's.
	ConsecutiveFailures int
	// MaxEjectedFraction caps how much of the pool passive detection may
	// eject (default 0.5), so a fleet-wide problem -- a bad prompt, a client
	// bug -- can't eject every replica. Failed health checks are not capped.
	MaxEjectedFraction float64

	// BaseEjection is the first ejection's length (default 2s). Each further
	// ejection doubles it, up to MaxEjection (default 60s); a replica that
	// stays in service for MaxEjection starts again from BaseEjection.
	BaseEjection time.Duration
	MaxEjection  time.Duration

	// OnEject, if set, is called (with the monitor's lock held) for each
	// ejection; passive is true when failed requests caused it.
	OnEject func(b *backend.Backend, passive bool)

	Logger *slog.Logger
}

// Monitor tracks the health of a pool of replicas, which may change. A nil
// *Monitor treats every replica as healthy.
type Monitor struct {
	cfg  Config
	pool pool.Source
	now  func() time.Time

	mu    sync.Mutex
	state map[*backend.Backend]*state
}

type state struct {
	ejected      bool
	until        time.Time // earliest re-admission
	ejections    int       // ejections in the current backoff run
	readmitted   time.Time
	failedChecks int
	failedReqs   int
}

func New(cfg Config, src pool.Source) *Monitor {
	if cfg.Timeout <= 0 {
		cfg.Timeout = time.Second
	}
	if cfg.UnhealthyThreshold <= 0 {
		cfg.UnhealthyThreshold = 2
	}
	if cfg.MaxEjectedFraction <= 0 {
		cfg.MaxEjectedFraction = 0.5
	}
	if cfg.BaseEjection <= 0 {
		cfg.BaseEjection = 2 * time.Second
	}
	if cfg.MaxEjection < cfg.BaseEjection {
		cfg.MaxEjection = max(60*time.Second, cfg.BaseEjection)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Monitor{cfg: cfg, pool: src, now: time.Now, state: map[*backend.Backend]*state{}}
}

// stateLocked returns b's state, creating it for a replica new to the pool.
func (m *Monitor) stateLocked(b *backend.Backend) *state {
	s := m.state[b]
	if s == nil {
		s = &state{}
		m.state[b] = s
	}
	return s
}

// Run performs active checks every Interval until ctx is done.
func (m *Monitor) Run(ctx context.Context) {
	if m == nil || m.cfg.Interval <= 0 {
		return
	}
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	for {
		m.checkAll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// checkAll checks every replica concurrently and applies the results.
func (m *Monitor) checkAll(ctx context.Context) {
	replicas := m.pool.Backends()
	errs := make([]error, len(replicas))
	var wg sync.WaitGroup
	for i, b := range replicas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
			defer cancel()
			errs[i] = b.Check(cctx)
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	current := make(map[*backend.Backend]bool, len(replicas))
	for _, b := range replicas {
		current[b] = true
	}
	for b := range m.state {
		if !current[b] {
			delete(m.state, b) // left the pool
		}
	}
	for i, b := range replicas {
		s := m.stateLocked(b)
		if errs[i] == nil {
			s.failedChecks = 0
			if s.ejected && !now.Before(s.until) {
				m.readmit(b, s, now)
			}
			continue
		}
		s.failedChecks++
		if !s.ejected && s.failedChecks >= m.cfg.UnhealthyThreshold {
			m.eject(b, s, now, false, errs[i])
		}
	}
}

// Observe records the outcome of one request to b: nil for success, or the
// error the attempt ended with.
func (m *Monitor) Observe(b *backend.Backend, err error) {
	if m == nil || m.cfg.ConsecutiveFailures <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stateLocked(b)
	if s.ejected {
		return
	}
	if err == nil {
		s.failedReqs = 0
		return
	}
	var e *backend.Error
	if !errors.As(err, &e) || (e.Kind != backend.Unreachable && e.Kind != backend.Failed) {
		return
	}
	s.failedReqs++
	if s.failedReqs < m.cfg.ConsecutiveFailures {
		return
	}
	if m.ejectedLocked()+1 > int(m.cfg.MaxEjectedFraction*float64(len(m.pool.Backends()))) {
		return // keep counting; eject once another replica is re-admitted
	}
	m.eject(b, s, m.now(), true, err)
}

// Available filters out ejected replicas. If every candidate is ejected it
// returns them all: a replica that might have recovered beats failing the
// request outright.
func (m *Monitor) Available(candidates []*backend.Backend) []*backend.Backend {
	if m == nil {
		return candidates
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	ok := slices.DeleteFunc(slices.Clone(candidates), func(b *backend.Backend) bool {
		s := m.state[b]
		if s == nil || !s.ejected {
			return false
		}
		if m.cfg.Interval <= 0 && !now.Before(s.until) {
			m.readmit(b, s, now) // no checks to wait for
			return false
		}
		return true
	})
	if len(ok) == 0 {
		return candidates
	}
	return ok
}

// Ejected reports whether b is currently out of rotation.
func (m *Monitor) Ejected(b *backend.Backend) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.state[b]
	return s != nil && s.ejected
}

func (m *Monitor) ejectedLocked() int {
	n := 0
	for _, s := range m.state {
		if s.ejected {
			n++
		}
	}
	return n
}

func (m *Monitor) eject(b *backend.Backend, s *state, now time.Time, passive bool, err error) {
	reason := "health check failed"
	if passive {
		reason = "consecutive request failures"
	}
	if m.cfg.OnEject != nil {
		m.cfg.OnEject(b, passive)
	}
	if !s.readmitted.IsZero() && now.Sub(s.readmitted) >= m.cfg.MaxEjection {
		s.ejections = 0 // it stayed in service long enough to be forgiven
	}
	s.ejections++
	d := m.cfg.BaseEjection << min(s.ejections-1, 30)
	if d <= 0 || d > m.cfg.MaxEjection {
		d = m.cfg.MaxEjection
	}
	s.ejected, s.until = true, now.Add(d)
	s.failedChecks, s.failedReqs = 0, 0
	m.cfg.Logger.Warn("replica ejected", "backend", b.Name, "reason", reason, "for", d, "ejections", s.ejections, "err", err)
}

func (m *Monitor) readmit(b *backend.Backend, s *state, now time.Time) {
	s.ejected, s.readmitted = false, now
	m.cfg.Logger.Info("replica re-admitted", "backend", b.Name)
}
