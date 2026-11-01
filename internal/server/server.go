// Package server implements the gateway's gRPC Inference service.
package server

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/router"
)

const (
	defaultMaxTokens = 128
	maxPromptBytes   = 8192 // TinyServe's limit
)

type Config struct {
	Backends []*backend.Backend
	Router   router.Router
	// MaxAttempts bounds how many replicas one request may try before its
	// first token. Defaults to every replica.
	MaxAttempts int
	Logger      *slog.Logger
}

type Server struct {
	pb.UnimplementedInferenceServer
	cfg Config
}

func New(cfg Config) *Server {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = len(cfg.Backends)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Server{cfg: cfg}
}

// Generate forwards the request to one replica and relays its stream. If a
// replica fails before producing any text, the request moves to another;
// once text has reached the client a failure ends the stream, since the
// client has already seen part of one completion.
func (s *Server) Generate(req *pb.GenerateRequest, stream pb.Inference_GenerateServer) error {
	start := time.Now()
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

	var (
		tried   []*backend.Backend
		lastErr error
		ttft    time.Duration
	)
	for attempt := 1; attempt <= s.cfg.MaxAttempts; attempt++ {
		candidates := slices.DeleteFunc(slices.Clone(s.cfg.Backends), func(b *backend.Backend) bool {
			return slices.Contains(tried, b)
		})
		if len(candidates) == 0 {
			break
		}
		b := s.cfg.Router.Pick(req.Prompt, candidates)
		tried = append(tried, b)

		sent := 0
		result, err := b.Stream(ctx, breq, func(text string) error {
			if sent == 0 {
				ttft = time.Since(start)
			}
			sent++
			return stream.Send(&pb.GenerateChunk{Text: text})
		})
		log := s.cfg.Logger.With("backend", b.Name, "attempt", attempt)
		if err == nil {
			log.Debug("generate ok", "tokens", result.TokensGenerated, "elapsed", time.Since(start))
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
