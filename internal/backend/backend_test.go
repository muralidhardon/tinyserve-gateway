package backend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
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
