// Package server implements the gateway's gRPC Inference service.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/health"
	"github.com/muralidhardon/tinyserve-gateway/internal/metrics"
	"github.com/muralidhardon/tinyserve-gateway/internal/pool"
	"github.com/muralidhardon/tinyserve-gateway/internal/router"
)

const (
	defaultMaxTokens = 128
	maxPromptBytes   = 8192 // TinyServe's limit
)

type Config struct {
	// Backends lists the replicas; it may change between requests.
	Backends pool.Source
	Router   router.Router
	// MaxAttempts bounds how many replicas one request may try before its
	// first token. 0 means every replica.
	MaxAttempts int
	// Health filters out ejected replicas and is told how each attempt went.
	// Nil means every replica is always eligible.
	Health *health.Monitor
	// RetryBudget limits retries after the first attempt. Nil means unlimited.
	RetryBudget *RetryBudget
	// FirstTokenTimeout is how long an attempt may take to produce its first
	// text before it is abandoned and the request moves to another replica.
	// It catches replicas that accept connections but stall (a hung process,
	// a wedged GPU) -- a gray failure that connection errors never reveal.
	// 0 disables it.
	FirstTokenTimeout time.Duration
	// MaxInflight is how many streams the gateway serves at once; beyond it,
	// requests are shed with RESOURCE_EXHAUSTED instead of queueing. Lower
	// priorities are shed first: low-priority requests are admitted only
	// below 70% of MaxInflight, normal ones below 90%, high ones up to the
	// limit. 0 means no limit.
	MaxInflight int
	// Metrics records request, attempt and routing metrics. Nil records nothing.
	Metrics *metrics.Metrics
	Logger  *slog.Logger
}

// tracer creates the per-attempt spans, under the server span that otelgrpc
// starts for each call. With no tracer provider installed it does nothing.
var tracer = otel.Tracer("github.com/muralidhardon/tinyserve-gateway/internal/server")

type Server struct {
	pb.UnimplementedInferenceServer
	cfg      Config
	inflight atomic.Int64
}

// errNoFirstToken is the cause an attempt is cancelled with when it hits FirstTokenTimeout.
var errNoFirstToken = errors.New("no first token")

func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Server{cfg: cfg}
}

// Inflight reports the streams the gateway is serving.
func (s *Server) Inflight() int64 { return s.inflight.Load() }

// Generate forwards the request to one replica and relays its stream. If a
// replica fails before producing any text, the request moves to another;
// once text has reached the client a failure ends the stream, since the
// client has already seen part of one completion.
func (s *Server) Generate(req *pb.GenerateRequest, stream pb.Inference_GenerateServer) error {
	start := time.Now()
	err := s.generate(req, stream, start)
	code := status.Code(err)
	s.cfg.Metrics.Request(code.String(), priorityLabel(req.Priority), time.Since(start))
	if span := trace.SpanFromContext(stream.Context()); err != nil {
		// otelgrpc marks only some codes as errors on a server span; a request
		// that failed for any reason is one here.
		span.SetStatus(otelcodes.Error, err.Error())
	}
	return err
}

func (s *Server) generate(req *pb.GenerateRequest, stream pb.Inference_GenerateServer, start time.Time) error {
	ctx := stream.Context()
	if err := validate(req); err != nil {
		return err
	}
	breq := backend.Request{
		Prompt: req.Prompt, MaxTokens: req.MaxTokens,
		Temperature: req.Temperature, TopK: req.TopK, Seed: req.Seed,
	}
	if breq.MaxTokens == 0 {
		breq.MaxTokens = defaultMaxTokens
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.Int("tsgw.prompt_bytes", len(req.Prompt)),
		attribute.Int("tsgw.max_tokens", int(breq.MaxTokens)),
		attribute.String("tsgw.priority", priorityLabel(req.Priority)),
	)

	if !s.admit(req.Priority) {
		s.cfg.Metrics.Shed(priorityLabel(req.Priority))
		span.AddEvent("shed")
		return status.Errorf(codes.ResourceExhausted, "gateway at capacity, shedding %s requests", priorityName(req.Priority))
	}
	defer s.inflight.Add(-1)
	s.cfg.RetryBudget.Request()

	var (
		tried   []*backend.Backend
		lastErr error
		ttft    time.Duration
	)
	all := s.cfg.Backends.Backends()
	maxAttempts := len(all)
	if s.cfg.MaxAttempts > 0 {
		maxAttempts = min(maxAttempts, s.cfg.MaxAttempts)
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		candidates := slices.DeleteFunc(slices.Clone(all), func(b *backend.Backend) bool {
			return slices.Contains(tried, b)
		})
		if len(candidates) == 0 {
			break
		}
		if attempt > 1 && !s.cfg.RetryBudget.TryRetry() {
			s.cfg.Metrics.RetryBudgetExhausted()
			span.AddEvent("retry budget exhausted")
			s.cfg.Logger.Warn("retry budget exhausted, not retrying", "err", lastErr)
			break
		}
		b, decision := router.Explain(s.cfg.Router, req.Prompt, s.cfg.Health.Available(candidates))
		s.cfg.Metrics.Route(decision.Reason, decision.MatchBlocks)
		tried = append(tried, b)

		actx, aspan := tracer.Start(ctx, "attempt", trace.WithAttributes(
			attribute.String("tsgw.backend", b.Name),
			attribute.Int("tsgw.attempt", attempt),
			attribute.String("tsgw.route.reason", decision.Reason),
			attribute.Int("tsgw.route.prefix_match_blocks", decision.MatchBlocks),
		))
		sent := 0
		var last time.Time
		result, err := s.attempt(actx, b, breq, func(text string) error {
			now := time.Now()
			if sent == 0 {
				ttft = now.Sub(start)
				s.cfg.Metrics.FirstToken(ttft)
				aspan.AddEvent("first token")
			} else {
				s.cfg.Metrics.InterChunk(now.Sub(last))
			}
			last = now
			sent++
			return stream.Send(&pb.GenerateChunk{Text: text})
		})
		s.cfg.Health.Observe(b, err)
		s.cfg.Metrics.Attempt(attemptResult(err), attempt > 1)
		endAttempt(aspan, result, err, sent)
		log := s.cfg.Logger.With("backend", b.Name, "attempt", attempt)
		if err == nil {
			log.Debug("generate ok", "tokens", result.TokensGenerated, "elapsed", time.Since(start))
			s.cfg.Metrics.Usage(result.PromptTokens, result.PrefixTokensReused, result.TokensGenerated)
			return stream.Send(&pb.GenerateChunk{Usage: &pb.Usage{
				PromptTokens: result.PromptTokens, TokensGenerated: result.TokensGenerated, Seed: result.Seed,
				Backend: b.Name, Attempts: uint32(attempt),
				BackendLatencyMs: result.LatencyMS, BackendQueueMs: result.QueueMS, BackendTtftMs: result.TTFTMS,
				GatewayTtftMs:             float64(ttft) / float64(time.Millisecond),
				BackendPrefixTokensReused: result.PrefixTokensReused,
			}})
		}
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		if sent > 0 {
			log.Warn("backend failed mid-stream", "chunks_sent", sent, "err", err)
			return status.Errorf(codes.Unavailable, "backend failed after %d chunks: %v", sent, err)
		}
		if !backend.IsRetryable(err) {
			return toStatus(err)
		}
		log.Warn("backend failed before first token, trying another", "err", err)
		lastErr = err
	}
	if lastErr == nil {
		return status.Error(codes.Unavailable, "no backends")
	}
	return toStatus(lastErr)
}

// attempt runs one stream on b, abandoning it if no text arrives within FirstTokenTimeout.
func (s *Server) attempt(ctx context.Context, b *backend.Backend, req backend.Request, onToken func(string) error) (*backend.Result, error) {
	if s.cfg.FirstTokenTimeout <= 0 {
		return b.Stream(ctx, req, onToken)
	}
	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := time.AfterFunc(s.cfg.FirstTokenTimeout, func() { cancel(errNoFirstToken) })
	defer timer.Stop()

	first := true
	result, err := b.Stream(actx, req, func(text string) error {
		if first {
			first = false
			if !timer.Stop() {
				return errNoFirstToken // the timer won the race; nothing has been sent yet
			}
		}
		return onToken(text)
	})
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(actx), errNoFirstToken) {
		err = &backend.Error{Kind: backend.Failed, Backend: b.Name,
			Msg: fmt.Sprintf("no first token within %v", s.cfg.FirstTokenTimeout)}
	}
	return result, err
}

// admit reserves an in-flight slot for a request of priority p, or reports
// that the gateway is too busy for that priority.
func (s *Server) admit(p pb.Priority) bool {
	n := s.inflight.Add(1)
	if s.cfg.MaxInflight <= 0 {
		return true
	}
	limit := float64(s.cfg.MaxInflight)
	switch p {
	case pb.Priority_PRIORITY_LOW:
		limit *= 0.7
	case pb.Priority_PRIORITY_HIGH:
	default:
		limit *= 0.9
	}
	if float64(n) > limit {
		s.inflight.Add(-1)
		return false
	}
	return true
}

// attemptResult names an attempt's outcome for metrics.
func attemptResult(err error) string {
	var e *backend.Error
	switch {
	case err == nil:
		return "ok"
	case errors.As(err, &e):
		return e.Kind.String()
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	}
	return "failed"
}

// endAttempt records an attempt's outcome on its span. The replica's own
// timings split the attempt into queueing, prefill and decode without the
// engine having to emit spans of its own.
func endAttempt(span trace.Span, result *backend.Result, err error, sent int) {
	span.SetAttributes(
		attribute.String("tsgw.result", attemptResult(err)),
		attribute.Int("tsgw.chunks_sent", sent),
	)
	if result != nil {
		span.SetAttributes(
			attribute.Int("tsgw.backend.prompt_tokens", int(result.PromptTokens)),
			attribute.Int("tsgw.backend.prefix_tokens_reused", int(result.PrefixTokensReused)),
			attribute.Int("tsgw.backend.tokens_generated", int(result.TokensGenerated)),
			attribute.Float64("tsgw.backend.queue_ms", result.QueueMS),
			attribute.Float64("tsgw.backend.ttft_ms", result.TTFTMS),
			attribute.Float64("tsgw.backend.latency_ms", result.LatencyMS),
		)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, attemptResult(err))
	}
	span.End()
}

// priorityLabel is a request priority as a metric label.
func priorityLabel(p pb.Priority) string {
	switch p {
	case pb.Priority_PRIORITY_LOW:
		return "low"
	case pb.Priority_PRIORITY_HIGH:
		return "high"
	}
	return "normal"
}

func priorityName(p pb.Priority) string {
	switch p {
	case pb.Priority_PRIORITY_LOW:
		return "low-priority"
	case pb.Priority_PRIORITY_HIGH:
		return "high-priority"
	}
	return "normal-priority"
}

func validate(req *pb.GenerateRequest) error {
	switch {
	case req.Prompt == "":
		return status.Error(codes.InvalidArgument, "prompt must not be empty")
	case len(req.Prompt) > maxPromptBytes:
		return status.Errorf(codes.InvalidArgument, "prompt must be at most %d bytes", maxPromptBytes)
	}
	return nil
}

// toStatus maps a failure on the last replica tried to a gRPC status.
func toStatus(err error) error {
	var e *backend.Error
	if !errors.As(err, &e) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return status.FromContextError(err).Err()
		}
		return status.Error(codes.Internal, err.Error())
	}
	switch e.Kind {
	case backend.Overloaded:
		return status.Error(codes.ResourceExhausted, e.Error())
	case backend.Rejected:
		return status.Error(codes.InvalidArgument, e.Msg)
	default:
		return status.Error(codes.Unavailable, e.Error())
	}
}
