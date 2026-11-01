package backend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func serve(t *testing.T, h http.HandlerFunc) *Backend {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return New(s.URL, nil)
}

func sse(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, e := range events {
		fmt.Fprint(w, e)
	}
}

func TestStreamDecodesEvents(t *testing.T) {
	b := serve(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate/stream" || r.Method != http.MethodPost {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		sse(w,
			"event: token\ndata: {\"text\": \" a\"}\n\n",
			"event: token\ndata: {\"text\": \" b \\u00e9\"}\n\n",
			"event: done\ndata: {\"text\": \" a b \\u00e9\", \"tokens_generated\": 3, \"ttft_ms\": 1.5, \"seed\": 9}\n\n")
	})
	var got []string
	res, err := b.Stream(context.Background(), Request{Prompt: "x", MaxTokens: 3}, func(s string) error {
		got = append(got, s)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != " a" || got[1] != " b é" {
		t.Errorf("tokens = %q", got)
	}
	if res.TokensGenerated != 3 || res.TTFTMS != 1.5 || res.Seed != 9 || res.Text != " a b é" {
		t.Errorf("result = %+v", res)
	}
	if b.Inflight() != 0 {
		t.Errorf("inflight = %d after stream ended", b.Inflight())
	}
}

func TestStreamErrors(t *testing.T) {
	cases := []struct {
		name      string
		handler   http.HandlerFunc
		kind      Kind
		retryable bool
	}{
		{"503", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "full", 503) }, Overloaded, true},
		{"422", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "bad", 422) }, Rejected, false},
		{"500", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", 500) }, Failed, true},
		{"error event", func(w http.ResponseWriter, _ *http.Request) {
			sse(w, "event: error\ndata: {\"detail\": \"engine crashed\"}\n\n")
		}, Failed, true},
		{"truncated", func(w http.ResponseWriter, _ *http.Request) {
			sse(w, "event: token\ndata: {\"text\": \"a\"}\n\n")
		}, Failed, true},
		{"garbage", func(w http.ResponseWriter, _ *http.Request) {
			sse(w, "event: token\ndata: not json\n\n")
		}, Failed, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := serve(t, tc.handler)
			_, err := b.Stream(context.Background(), Request{Prompt: "x", MaxTokens: 1}, func(string) error { return nil })
			var e *Error
			if !errors.As(err, &e) || e.Kind != tc.kind {
				t.Fatalf("err = %v, want kind %s", err, tc.kind)
			}
			if IsRetryable(err) != tc.retryable {
				t.Errorf("IsRetryable = %v, want %v", !tc.retryable, tc.retryable)
			}
		})
	}
}

func TestUnreachable(t *testing.T) {
	s := httptest.NewServer(http.NotFoundHandler())
	s.Close()
	_, err := New(s.URL, nil).Stream(context.Background(), Request{Prompt: "x"}, func(string) error { return nil })
	var e *Error
	if !errors.As(err, &e) || e.Kind != Unreachable || !IsRetryable(err) {
		t.Fatalf("err = %v, want retryable Unreachable", err)
	}
}

func TestCanceledContextIsNotABackendError(t *testing.T) {
	b := serve(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := b.Stream(ctx, Request{Prompt: "x"}, func(string) error { return nil })
	if !errors.Is(err, context.Canceled) || IsRetryable(err) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestOverloadedMarksBackend(t *testing.T) {
	b := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "2")
		http.Error(w, "full", 503)
	})
	if b.Avoided() {
		t.Fatal("overloaded before any request")
	}
	b.Stream(context.Background(), Request{Prompt: "x"}, func(string) error { return nil })
	if !b.Avoided() {
		t.Fatal("503 did not mark the backend overloaded")
	}
}

func TestScrape(t *testing.T) {
	b := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		// as TinyServe renders it, with other metrics around the two we need
		fmt.Fprint(w, `# HELP tinyserve_queue_depth Requests waiting for a batch slot.
# TYPE tinyserve_queue_depth gauge
tinyserve_queue_depth 3
tinyserve_queue_wait_seconds_count 10
# TYPE tinyserve_active_sequences gauge
tinyserve_active_sequences 16.0
tinyserve_requests_total{status="ok"} 12
`)
	})
	if err := b.Scrape(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.Load() != 19 {
		t.Errorf("Load = %d, want 19", b.Load())
	}
}

func TestScrapeRejectsMissingGauges(t *testing.T) {
	b := serve(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "tinyserve_queue_depth 1\n") })
	b.SetScraped(7, 7)
	if err := b.Scrape(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	if b.Load() != 14 {
		t.Errorf("failed scrape changed Load to %d", b.Load())
	}
}

func TestLoadIsMaxOfInflightAndScraped(t *testing.T) {
	release := make(chan struct{})
	b := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		<-release
		sse(w, "event: done\ndata: {}\n\n")
	})
	done := make(chan struct{})
	for i := 0; i < 3; i++ {
		go func() {
			b.Stream(context.Background(), Request{Prompt: "x"}, func(string) error { return nil })
			done <- struct{}{}
		}()
	}
	for b.Inflight() < 3 {
		time.Sleep(time.Millisecond)
	}
	b.SetScraped(0, 1) // stale scrape from before these requests
	if b.Load() != 3 {
		t.Errorf("Load = %d, want in-flight 3", b.Load())
	}
	b.SetScraped(2, 8) // other gateways' traffic too
	if b.Load() != 10 {
		t.Errorf("Load = %d, want scraped 10", b.Load())
	}
	close(release)
	for i := 0; i < 3; i++ {
		<-done
	}
}

func TestOnTokenErrorAborts(t *testing.T) {
	b := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		sse(w, "event: token\ndata: {\"text\": \"a\"}\n\n", "event: done\ndata: {}\n\n")
	})
	stop := errors.New("client gone")
	_, err := b.Stream(context.Background(), Request{Prompt: "x"}, func(string) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("err = %v, want %v", err, stop)
	}
}
