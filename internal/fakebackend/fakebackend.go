// Package fakebackend is a stand-in TinyServe replica for tests, demos and
// chaos experiments: it speaks the same HTTP API (/generate/stream, /health,
// /metrics) with configurable latency and injected faults, and needs no model.
package fakebackend

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Config is safe to change while the server runs; use Server.Configure.
type Config struct {
	TokenDelay   time.Duration // per generated token
	PrefillDelay time.Duration // before the first token
	TokensPerSSE int           // tokens per `token` event, like TinyServe's steps_per_call (default 8)

	// Capacity, modelled on TinyServe's scheduler: at most Slots sequences
	// generate at once (0 = unlimited); the rest wait in a queue of at most
	// MaxQueue (0 = unlimited), and beyond that requests get 503.
	Slots    int
	MaxQueue int

	// Faults. FailRate and OverloadRate are probabilities in [0, 1].
	FailRate     float64 // answer 500 before streaming
	OverloadRate float64 // answer 503 before streaming
	DropAfter    int     // if > 0, cut the connection after this many tokens
}

type Server struct {
	mu  sync.RWMutex
	cfg Config

	slots    chan struct{} // nil when Slots == 0; sized at New, not by Configure
	queued   atomic.Int64
	active   atomic.Int64
	requests atomic.Int64
	canceled atomic.Int64
}

func New(cfg Config) *Server {
	s := &Server{}
	if cfg.Slots > 0 {
		s.slots = make(chan struct{}, cfg.Slots)
	}
	s.Configure(cfg)
	return s
}

func (s *Server) Configure(cfg Config) {
	if cfg.TokensPerSSE <= 0 {
		cfg.TokensPerSSE = 8
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
}

func (s *Server) config() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// Requests is the number of /generate/stream calls received.
func (s *Server) Requests() int64 { return s.requests.Load() }

// Canceled is the number of streams the client abandoned before they finished.
func (s *Server) Canceled() int64 { return s.canceled.Load() }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /generate/stream", s.stream)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok","model":"fake"}`)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprintf(w, "tinyserve_queue_depth %d\ntinyserve_active_sequences %d\n", s.queued.Load(), s.active.Load())
	})
	return mux
}

// Token returns the fake completion's i-th token, so tests can predict output.
func Token(i int) string { return fmt.Sprintf(" t%d", i) }

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	var req struct {
		Prompt    string  `json:"prompt"`
		MaxTokens int     `json:"max_tokens"`
		Seed      *uint32 `json:"seed"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Prompt == "" || req.MaxTokens < 1 {
		http.Error(w, `{"detail":"invalid request"}`, http.StatusUnprocessableEntity)
		return
	}
	cfg := s.config()
	switch roll := rand.Float64(); {
	case roll < cfg.FailRate:
		http.Error(w, `{"detail":"injected failure"}`, http.StatusInternalServerError)
		return
	case roll < cfg.FailRate+cfg.OverloadRate:
		overloaded(w)
		return
	}

	start := time.Now()
	if s.slots != nil {
		if n := s.queued.Add(1); cfg.MaxQueue > 0 && n > int64(cfg.MaxQueue) {
			s.queued.Add(-1)
			overloaded(w)
			return
		}
		select {
		case s.slots <- struct{}{}:
			s.queued.Add(-1)
			defer func() { <-s.slots }()
		case <-r.Context().Done():
			s.queued.Add(-1)
			s.canceled.Add(1)
			return
		}
	}
	queueWait := time.Since(start)

	s.active.Add(1)
	defer s.active.Add(-1)
	flusher := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	wait := func(d time.Duration) bool {
		if d <= 0 {
			return true
		}
		select {
		case <-time.After(d):
			return true
		case <-r.Context().Done():
			s.canceled.Add(1)
			return false
		}
	}
	if !wait(cfg.PrefillDelay) {
		return
	}

	var text string
	var ttft time.Duration
	for i := 0; i < req.MaxTokens; {
		n := min(cfg.TokensPerSSE, req.MaxTokens-i)
		if !wait(time.Duration(n) * cfg.TokenDelay) {
			return
		}
		var delta string
		for j := 0; j < n; j++ {
			delta += Token(i + j)
		}
		i += n
		if cfg.DropAfter > 0 && i > cfg.DropAfter {
			panic(http.ErrAbortHandler) // closes the connection without finishing the response
		}
		if ttft == 0 {
			ttft = time.Since(start)
		}
		text += delta
		writeEvent(w, "token", map[string]string{"text": delta})
		flusher.Flush()
	}

	seed := uint32(0)
	if req.Seed != nil {
		seed = *req.Seed
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	writeEvent(w, "done", map[string]any{
		"text": text, "prompt_tokens": len(req.Prompt) / 4, "tokens_generated": req.MaxTokens,
		"latency_ms": ms(time.Since(start)), "queue_ms": ms(queueWait), "ttft_ms": ms(ttft), "seed": seed,
	})
	flusher.Flush()
}

func overloaded(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	http.Error(w, `{"detail":"queue is full, retry later"}`, http.StatusServiceUnavailable)
}

func writeEvent(w http.ResponseWriter, event string, data any) {
	b, _ := json.Marshal(data)
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
}
