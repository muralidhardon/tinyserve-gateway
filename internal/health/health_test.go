package health

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/fakebackend"
	"github.com/muralidhardon/tinyserve-gateway/internal/pool"
)

// clock is a settable time source.
type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fixture struct {
	m     *Monitor
	clk   *clock
	fakes []*fakebackend.Server
	pool  []*backend.Backend
}

func newFixture(t *testing.T, n int, cfg Config) *fixture {
	t.Helper()
	f := &fixture{clk: &clock{time.Unix(1000, 0)}}
	for range n {
		fake := fakebackend.New(fakebackend.Config{})
		h := httptest.NewServer(fake.Handler())
		t.Cleanup(h.Close)
		f.fakes = append(f.fakes, fake)
		f.pool = append(f.pool, backend.New(h.URL, nil))
	}
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	f.m = New(cfg, pool.Static(f.pool))
	f.m.now = f.clk.now
	return f
}

func failed(b *backend.Backend) error {
	return &backend.Error{Kind: backend.Failed, Backend: b.Name, Msg: "500"}
}

func TestConsecutiveFailuresEject(t *testing.T) {
	f := newFixture(t, 3, Config{ConsecutiveFailures: 3})
	b := f.pool[0]
	f.m.Observe(b, failed(b))
	f.m.Observe(b, failed(b))
	f.m.Observe(b, nil) // a success resets the run
	f.m.Observe(b, failed(b))
	f.m.Observe(b, failed(b))
	if f.m.Ejected(b) {
		t.Fatal("ejected after 2 consecutive failures, threshold is 3")
	}
	f.m.Observe(b, failed(b))
	if !f.m.Ejected(b) {
		t.Fatal("not ejected after 3 consecutive failures")
	}
	if got := f.m.Available(f.pool); len(got) != 2 || got[0] == b || got[1] == b {
		t.Fatalf("Available = %v, want the other two", got)
	}
}

func TestBackpressureAndClientErrorsDoNotEject(t *testing.T) {
	f := newFixture(t, 3, Config{ConsecutiveFailures: 2})
	b := f.pool[0]
	for _, err := range []error{
		&backend.Error{Kind: backend.Overloaded, Backend: b.Name},
		&backend.Error{Kind: backend.Rejected, Backend: b.Name},
		context.Canceled,
		errors.New("client went away"),
	} {
		f.m.Observe(b, err)
		f.m.Observe(b, err)
	}
	if f.m.Ejected(b) {
		t.Fatal("ejected for 503s, 4xxs or client-side errors")
	}
}

func TestPassiveEjectionIsCapped(t *testing.T) {
	f := newFixture(t, 4, Config{ConsecutiveFailures: 1, MaxEjectedFraction: 0.5})
	for _, b := range f.pool {
		f.m.Observe(b, failed(b))
	}
	ejected := 0
	for _, b := range f.pool {
		if f.m.Ejected(b) {
			ejected++
		}
	}
	if ejected != 2 {
		t.Fatalf("%d of 4 ejected, want 2 (MaxEjectedFraction 0.5)", ejected)
	}
}

func TestAllEjectedFallsBackToAll(t *testing.T) {
	f := newFixture(t, 2, Config{UnhealthyThreshold: 1, Interval: time.Second})
	for _, fake := range f.fakes {
		fake.Configure(fakebackend.Config{Stall: true})
	}
	f.m.cfg.Timeout = 50 * time.Millisecond
	f.m.checkAll(context.Background())
	if !f.m.Ejected(f.pool[0]) || !f.m.Ejected(f.pool[1]) {
		t.Fatal("stalled replicas not ejected by health checks")
	}
	if got := f.m.Available(f.pool); len(got) != 2 {
		t.Fatalf("Available = %d replicas, want all 2 when all are ejected", len(got))
	}
}

func TestHealthChecksEjectAndReadmitWithBackoff(t *testing.T) {
	f := newFixture(t, 2, Config{Interval: time.Second, Timeout: 50 * time.Millisecond,
		UnhealthyThreshold: 2, BaseEjection: 2 * time.Second, MaxEjection: 60 * time.Second})
	b, fake := f.pool[0], f.fakes[0]
	ctx := context.Background()

	fake.Configure(fakebackend.Config{Stall: true})
	f.m.checkAll(ctx)
	if f.m.Ejected(b) {
		t.Fatal("ejected after one failed check, threshold is 2")
	}
	f.m.checkAll(ctx)
	if !f.m.Ejected(b) {
		t.Fatal("not ejected after two failed checks")
	}
	if f.m.Ejected(f.pool[1]) {
		t.Fatal("healthy replica ejected")
	}

	// Recovered, but the 2 s ejection hasn't passed: stays out.
	fake.Configure(fakebackend.Config{})
	f.clk.advance(time.Second)
	f.m.checkAll(ctx)
	if !f.m.Ejected(b) {
		t.Fatal("re-admitted before its ejection expired")
	}
	f.clk.advance(time.Second)
	f.m.checkAll(ctx)
	if f.m.Ejected(b) {
		t.Fatal("not re-admitted after ejection expired and a check passed")
	}

	// Fails again soon after: the second ejection lasts 4 s.
	fake.Configure(fakebackend.Config{Stall: true})
	f.m.checkAll(ctx)
	f.m.checkAll(ctx)
	fake.Configure(fakebackend.Config{})
	f.clk.advance(3 * time.Second)
	f.m.checkAll(ctx)
	if !f.m.Ejected(b) {
		t.Fatal("second ejection was not longer than the first")
	}
	f.clk.advance(time.Second)
	f.m.checkAll(ctx)
	if f.m.Ejected(b) {
		t.Fatal("not re-admitted after the 4 s second ejection")
	}

	// In service for MaxEjection: the backoff starts over at 2 s.
	f.clk.advance(60 * time.Second)
	for range 2 {
		fake.Configure(fakebackend.Config{Stall: true})
		f.m.checkAll(ctx)
	}
	fake.Configure(fakebackend.Config{})
	f.clk.advance(2 * time.Second)
	f.m.checkAll(ctx)
	if f.m.Ejected(b) {
		t.Fatal("backoff did not reset after a long healthy period")
	}
}

func TestWithoutHealthChecksEjectionExpires(t *testing.T) {
	f := newFixture(t, 2, Config{ConsecutiveFailures: 1, BaseEjection: 2 * time.Second})
	b := f.pool[0]
	f.m.Observe(b, failed(b))
	if len(f.m.Available(f.pool)) != 1 {
		t.Fatal("not ejected")
	}
	f.clk.advance(2 * time.Second)
	if len(f.m.Available(f.pool)) != 2 {
		t.Fatal("not re-admitted when its ejection expired")
	}
}

func TestNilMonitor(t *testing.T) {
	var m *Monitor
	pool := []*backend.Backend{backend.New("http://a", nil)}
	m.Observe(pool[0], errors.New("x"))
	if m.Ejected(pool[0]) || len(m.Available(pool)) != 1 {
		t.Fatal("nil Monitor should treat every replica as healthy")
	}
	m.Run(context.Background()) // returns immediately
}
