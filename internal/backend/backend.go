// Package backend is the gateway's client for one TinyServe replica: it calls
// POST /generate/stream and decodes the server-sent events it returns.
package backend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Request is TinyServe's /generate body. Nil fields take the server's defaults.
type Request struct {
	Prompt      string   `json:"prompt"`
	MaxTokens   uint32   `json:"max_tokens"`
	Temperature *float32 `json:"temperature,omitempty"`
	TopK        *uint32  `json:"top_k,omitempty"`
	Seed        *uint32  `json:"seed,omitempty"`
}

// Result is the body of TinyServe's final `done` event.
type Result struct {
	Text            string  `json:"text"`
	PromptTokens    uint32  `json:"prompt_tokens"`
	TokensGenerated uint32  `json:"tokens_generated"`
	LatencyMS       float64 `json:"latency_ms"`
	QueueMS         float64 `json:"queue_ms"`
	TTFTMS          float64 `json:"ttft_ms"`
	Seed            uint32  `json:"seed"`
	// Absent from TinyServe builds without a prefix cache, and so 0.
	PrefixTokensReused uint32 `json:"prefix_tokens_reused"`
}

// Kind classifies a failure so callers can decide whether to retry elsewhere.
type Kind int

const (
	// Unreachable: the request never got a response (connection refused, reset, DNS).
	Unreachable Kind = iota
	// Overloaded: the replica answered 503 because its queue is full.
	Overloaded
	// Rejected: the replica refused the request itself (4xx); another replica would too.
	Rejected
	// Failed: a 5xx, a malformed stream, or a stream that ended before `done`.
	Failed
)

func (k Kind) String() string {
	return [...]string{"unreachable", "overloaded", "rejected", "failed"}[k]
}

// Error is returned by Stream for every backend-side failure. Context
// cancellation is returned as the context's own error instead.
type Error struct {
	Kind    Kind
	Backend string
	Status  int // HTTP status, when there was a response
	Msg     string
}

func (e *Error) Error() string {
	return fmt.Sprintf("backend %s: %s: %s", e.Backend, e.Kind, e.Msg)
}

// Backend is one TinyServe replica.
type Backend struct {
	Name   string // base URL, e.g. http://10.0.0.7:8000
	client *http.Client

	inflight atomic.Int64
	// From the replica's last /metrics scrape: requests waiting for a batch
	// slot, and sequences in the running batch. They include other gateways'
	// traffic but are up to one scrape interval old.
	queueDepth      atomic.Int64
	activeSequences atomic.Int64
	avoidUntil      atomic.Int64 // unix nanos; see Avoid
}

// unreachableBackoff is how long a replica that refused a connection is
// avoided. Phase 4's health checks will replace this with proper ejection.
const unreachableBackoff = time.Second

// New returns a client for the replica at baseURL. The http.Client should have
// no overall Timeout: streams are bounded by the request context instead.
func New(baseURL string, client *http.Client) *Backend {
	if client == nil {
		client = http.DefaultClient
	}
	return &Backend{Name: strings.TrimRight(baseURL, "/"), client: client}
}

// Inflight is the number of streams this gateway has open to the replica.
func (b *Backend) Inflight() int64 { return b.inflight.Load() }

// Load estimates how many requests the replica is working on or holding. The
// scraped figures see every client but lag; the in-flight count is current
// but sees only this gateway. The larger of the two is a lower bound on the
// true load that reacts immediately to this gateway's own routing.
func (b *Backend) Load() int64 {
	return max(b.inflight.Load(), b.queueDepth.Load()+b.activeSequences.Load())
}

// Avoided reports whether routers should steer around the replica: it
// answered 503 within its Retry-After window, or recently couldn't be reached.
// Without this, a dead replica would look idle to least-loaded routing (no
// in-flight streams, frozen scrape figures) and attract traffic.
func (b *Backend) Avoided() bool {
	return time.Now().UnixNano() < b.avoidUntil.Load()
}

// Avoid makes Avoided report true for d.
func (b *Backend) Avoid(d time.Duration) {
	b.avoidUntil.Store(time.Now().Add(d).UnixNano())
}

// SetScraped records the replica's queue depth and running batch size.
func (b *Backend) SetScraped(queueDepth, activeSequences int64) {
	b.queueDepth.Store(queueDepth)
	b.activeSequences.Store(activeSequences)
}

func retryAfter(header string) time.Duration {
	if secs, err := strconv.Atoi(header); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Second
}

// Scrape refreshes the replica's load from GET /metrics.
func (b *Backend) Scrape(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.Name+"/metrics", nil)
	if err != nil {
		return err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s/metrics: %s", b.Name, resp.Status)
	}
	queue, active, err := parseLoad(resp.Body)
	if err != nil {
		return fmt.Errorf("GET %s/metrics: %w", b.Name, err)
	}
	b.SetScraped(queue, active)
	return nil
}

// parseLoad reads the two gauges routing needs from Prometheus text format.
func parseLoad(r io.Reader) (queue, active int64, err error) {
	found := 0
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		name, value, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			continue
		}
		var dst *int64
		switch name {
		case "tinyserve_queue_depth":
			dst = &queue
		case "tinyserve_active_sequences":
			dst = &active
		default:
			continue
		}
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("%s: %w", name, err)
		}
		*dst = int64(f)
		found++
	}
	if err := sc.Err(); err != nil {
		return 0, 0, err
	}
	if found != 2 {
		return 0, 0, errors.New("tinyserve_queue_depth or tinyserve_active_sequences missing")
	}
	return queue, active, nil
}

// ScrapeLoop scrapes every backend each interval until ctx is done. A replica
// that can't be scraped keeps its last figures.
func ScrapeLoop(ctx context.Context, backends []*Backend, interval time.Duration, onErr func(*Backend, error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		for _, b := range backends {
			go func() {
				sctx, cancel := context.WithTimeout(ctx, interval)
				defer cancel()
				if err := b.Scrape(sctx); err != nil && ctx.Err() == nil && onErr != nil {
					onErr(b, err)
				}
			}()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Stream runs one generation, calling onToken with each text delta as it
// arrives. onToken returning an error aborts the stream with that error.
func (b *Backend) Stream(ctx context.Context, req Request, onToken func(string) error) (*Result, error) {
	b.inflight.Add(1)
	defer b.inflight.Add(-1)

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, b.Name+"/generate/stream", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := b.client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		b.Avoid(unreachableBackoff)
		return nil, &Error{Kind: Unreachable, Backend: b.Name, Msg: err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		e := &Error{Backend: b.Name, Status: resp.StatusCode, Msg: strings.TrimSpace(string(detail))}
		switch {
		case resp.StatusCode == http.StatusServiceUnavailable:
			e.Kind = Overloaded
			b.Avoid(retryAfter(resp.Header.Get("Retry-After")))
		case resp.StatusCode >= 400 && resp.StatusCode < 500:
			e.Kind = Rejected
		default:
			e.Kind = Failed
		}
		return nil, e
	}

	result, err := b.readEvents(resp.Body, onToken)
	if err != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return result, err
}

// readEvents decodes the SSE stream until `done`, `error`, or EOF.
func (b *Backend) readEvents(r io.Reader, onToken func(string) error) (*Result, error) {
	fail := func(format string, args ...any) error {
		return &Error{Kind: Failed, Backend: b.Name, Status: http.StatusOK, Msg: fmt.Sprintf(format, args...)}
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024) // the done event repeats the whole completion
	var event, data string
	for sc.Scan() {
		line := sc.Text()
		if line != "" {
			field, value, _ := strings.Cut(line, ": ")
			switch field {
			case "event":
				event = value
			case "data":
				data = value
			}
			continue
		}
		// a blank line ends one event
		switch event {
		case "token":
			var tok struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal([]byte(data), &tok); err != nil {
				return nil, fail("bad token event: %v", err)
			}
			if err := onToken(tok.Text); err != nil {
				return nil, err
			}
		case "done":
			var res Result
			if err := json.Unmarshal([]byte(data), &res); err != nil {
				return nil, fail("bad done event: %v", err)
			}
			return &res, nil
		case "error":
			var e struct {
				Detail string `json:"detail"`
			}
			_ = json.Unmarshal([]byte(data), &e)
			return nil, fail("generation failed: %s", e.Detail)
		}
		event, data = "", ""
	}
	if err := sc.Err(); err != nil {
		return nil, fail("stream broken: %v", err)
	}
	return nil, fail("stream ended before done")
}

// IsRetryable reports whether a request that failed with err, before any output
// reached the client, can be sent to a different replica.
func IsRetryable(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Kind != Rejected
}
