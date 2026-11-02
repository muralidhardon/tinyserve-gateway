package server

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/fakebackend"
	"github.com/muralidhardon/tinyserve-gateway/internal/health"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestStalledReplicaHitsFirstTokenTimeout(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{Stall: true}, fakebackend.Config{})
	c := startGatewayConfig(t, rs, "round_robin", func(cfg *Config) {
		cfg.FirstTokenTimeout = 100 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	text, usage, err := generate(ctx, c, &pb.GenerateRequest{Prompt: "hi", MaxTokens: 8})
	if err != nil {
		t.Fatal(err)
	}
	if text != expected(8) || usage.Backend != rs[1].http.URL || usage.Attempts != 2 {
		t.Fatalf("got %q from %s in %d attempts, want the healthy replica on attempt 2", text, usage.Backend, usage.Attempts)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %v; the stalled attempt should have been abandoned after 100ms", d)
	}
}

func TestFirstTokenTimeoutDoesNotCutSlowStreams(t *testing.T) {
	// The timeout is on the first token only; a long generation may take longer in total.
	rs := startReplicas(t, fakebackend.Config{TokenDelay: 10 * time.Millisecond, TokensPerSSE: 1})
	c := startGatewayConfig(t, rs, "round_robin", func(cfg *Config) {
		cfg.FirstTokenTimeout = 50 * time.Millisecond
	})
	text, usage, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "hi", MaxTokens: 20})
	if err != nil || text != expected(20) || usage.Attempts != 1 {
		t.Fatalf("got %q, %v, attempts %d", text, err, usage.GetAttempts())
	}
}

func TestRetryBudgetBoundsRetries(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{FailRate: 1}, fakebackend.Config{FailRate: 1}, fakebackend.Config{FailRate: 1})
	c := startGatewayConfig(t, rs, "round_robin", func(cfg *Config) {
		cfg.RetryBudget = NewRetryBudget(0.1, 0)
	})
	for range 50 {
		generate(context.Background(), c, &pb.GenerateRequest{Prompt: "hi", MaxTokens: 4})
	}
	var attempts int64
	for _, r := range rs {
		attempts += r.fake.Requests()
	}
	// Unbudgeted, each request would try all 3 replicas: 150 attempts.
	if attempts > 55 {
		t.Fatalf("%d attempts for 50 requests; the budget allows 50 + 10%%", attempts)
	}
}

func TestShedsLowPriorityFirst(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{PrefillDelay: 500 * time.Millisecond})
	c := startGatewayConfig(t, rs, "round_robin", func(cfg *Config) { cfg.MaxInflight = 10 })

	// Fill 7 of 10 slots with long requests.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range 7 {
		wg.Add(1)
		go func() { defer wg.Done(); generate(ctx, c, &pb.GenerateRequest{Prompt: "hold", MaxTokens: 1}) }()
	}
	t.Cleanup(wg.Wait)
	waitFor(t, func() bool { return rs[0].fake.Requests() == 7 })

	quick := func(p pb.Priority) codes.Code {
		qctx, qcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer qcancel()
		_, _, err := generate(qctx, c, &pb.GenerateRequest{Prompt: "x", MaxTokens: 1, Priority: p})
		return status.Code(err)
	}
	// 7 in flight: an 8th low-priority request would exceed 70%.
	if got := quick(pb.Priority_PRIORITY_LOW); got != codes.ResourceExhausted {
		t.Fatalf("low priority at 70%%: %v, want ResourceExhausted", got)
	}
	// Normal and high are admitted (and then time out waiting on the slow replica).
	if got := quick(pb.Priority_PRIORITY_NORMAL); got != codes.DeadlineExceeded {
		t.Fatalf("normal priority at 70%%: %v, want admitted", got)
	}
	if got := quick(pb.Priority_PRIORITY_UNSPECIFIED); got != codes.DeadlineExceeded {
		t.Fatalf("unspecified priority at 70%%: %v, want admitted as normal", got)
	}
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); generate(ctx, c, &pb.GenerateRequest{Prompt: "hold", MaxTokens: 1}) }()
	}
	waitFor(t, func() bool { return rs[0].fake.Requests() == 7+2+2 })
	// 9 in flight: normal is shed at 90%, high still gets the last slot.
	if got := quick(pb.Priority_PRIORITY_NORMAL); got != codes.ResourceExhausted {
		t.Fatalf("normal priority at 90%%: %v, want ResourceExhausted", got)
	}
	if got := quick(pb.Priority_PRIORITY_HIGH); got != codes.DeadlineExceeded {
		t.Fatalf("high priority at 90%%: %v, want admitted", got)
	}
}

func TestFailingReplicaIsEjected(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{FailRate: 1}, fakebackend.Config{}, fakebackend.Config{})
	var monitor *health.Monitor
	c := startGatewayConfig(t, rs, "round_robin", func(cfg *Config) {
		monitor = health.New(health.Config{ConsecutiveFailures: 3, BaseEjection: time.Minute, Logger: quietLogger()}, cfg.Backends)
		cfg.Health = monitor
	})
	for range 30 {
		if _, _, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: distinctPrompt(), MaxTokens: 2}); err != nil {
			t.Fatal(err) // failover hides the failures from clients
		}
	}
	// Round-robin hits the failing replica every third request until it has
	// failed 3 times in a row; after that it's out of rotation.
	if got := rs[0].fake.Requests(); got != 3 {
		t.Fatalf("failing replica got %d requests, want 3 (then ejected)", got)
	}
}

// TestChaos runs steady load while one replica stalls and then another is
// killed, and checks that clients barely notice.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test takes a few seconds")
	}
	fast := fakebackend.Config{TokenDelay: 500 * time.Microsecond, PrefillDelay: 2 * time.Millisecond}
	rs := startReplicas(t, fast, fast, fast)
	var monitor *health.Monitor
	var pool []*backend.Backend
	c := startGatewayConfig(t, rs, "least_loaded", func(cfg *Config) {
		pool = cfg.Backends
		monitor = health.New(health.Config{
			Interval: 50 * time.Millisecond, Timeout: 50 * time.Millisecond, UnhealthyThreshold: 2,
			ConsecutiveFailures: 3, BaseEjection: 500 * time.Millisecond, Logger: quietLogger(),
		}, cfg.Backends)
		cfg.Health = monitor
		cfg.FirstTokenTimeout = 200 * time.Millisecond
		cfg.RetryBudget = NewRetryBudget(0.2, 20)
	})
	mctx, stop := context.WithCancel(context.Background())
	defer stop()
	go monitor.Run(mctx)

	var ok, failed atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, _, err := generate(ctx, c, &pb.GenerateRequest{Prompt: distinctPrompt(), MaxTokens: 16})
				cancel()
				if err != nil {
					failed.Add(1)
				} else {
					ok.Add(1)
				}
			}
		}()
	}

	time.Sleep(800 * time.Millisecond)
	stalled := fast
	stalled.Stall = true
	rs[1].fake.Configure(stalled) // gray failure: accepts connections, never answers
	time.Sleep(800 * time.Millisecond)
	rs[2].http.Listener.Close() // crash: in-flight streams cut, new connections refused
	rs[2].http.CloseClientConnections()
	time.Sleep(1200 * time.Millisecond)
	close(done)
	wg.Wait()

	total := ok.Load() + failed.Load()
	rate := float64(ok.Load()) / float64(total)
	t.Logf("%d requests, %d failed (%.2f%% success)", total, failed.Load(), 100*rate)
	if rate < 0.99 {
		t.Fatalf("success rate %.2f%%, want >= 99%%", 100*rate)
	}
	if monitor.Ejected(pool[0]) || !monitor.Ejected(pool[1]) || !monitor.Ejected(pool[2]) {
		t.Fatal("want the stalled and killed replicas ejected, and only them")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
