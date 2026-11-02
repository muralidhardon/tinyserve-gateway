package server

import (
	"context"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/muralidhardon/tinyserve-gateway/gen/inference/v1"
	"github.com/muralidhardon/tinyserve-gateway/internal/fakebackend"
	"github.com/muralidhardon/tinyserve-gateway/internal/metrics"
	"github.com/muralidhardon/tinyserve-gateway/internal/pool"
)

// metric returns the value of the series name{labels} in m's registry: a
// counter or gauge's value, or a histogram's sample count. Missing series are 0.
func metric(t *testing.T, m *metrics.Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	series:
		for _, s := range f.GetMetric() {
			for k, v := range labels {
				if labelValue(s, k) != v {
					continue series
				}
			}
			switch {
			case s.Counter != nil:
				return s.Counter.GetValue()
			case s.Gauge != nil:
				return s.Gauge.GetValue()
			case s.Histogram != nil:
				return float64(s.Histogram.GetSampleCount())
			}
		}
	}
	return 0
}

func labelValue(s *dto.Metric, name string) string {
	for _, l := range s.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func TestMetricsCountFailovers(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{FailRate: 1}, fakebackend.Config{})
	var m *metrics.Metrics
	c := startGatewayConfig(t, rs, "round_robin", func(cfg *Config) {
		m = metrics.New(metrics.Replicas{Pool: cfg.Backends}, func() float64 { return 0 })
		cfg.Metrics = m
	})
	const n = 10
	for i := 0; i < n; i++ {
		if _, _, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "hi", MaxTokens: 4}); err != nil {
			t.Fatal(err)
		}
	}
	// Round-robin advances on the retry too, so with two replicas every
	// request starts on the failing one and fails over to the healthy one.
	for _, want := range []struct {
		name   string
		labels map[string]string
		value  float64
	}{
		{"tsgw_requests_total", map[string]string{"code": "OK", "priority": "normal"}, n},
		{"tsgw_attempts_total", map[string]string{"result": "ok"}, n},
		{"tsgw_attempts_total", map[string]string{"result": "failed"}, n},
		{"tsgw_retries_total", nil, n},
		{"tsgw_routing_decisions_total", map[string]string{"reason": "round_robin"}, 2 * n},
		{"tsgw_ttft_seconds", nil, n},
		{"tsgw_request_duration_seconds", nil, n},
		{"tsgw_tokens_generated_total", nil, 4 * n},
		{"tsgw_replicas", nil, 2},
		{"tsgw_backend_ejected", map[string]string{"backend": rs[0].http.URL}, 0},
	} {
		if got := metric(t, m, want.name, want.labels); got != want.value {
			t.Errorf("%s%v = %v, want %v", want.name, want.labels, got, want.value)
		}
	}
}

// rate() can't see a counter's first increment, so failure series must
// exist, at 0, before the first failure.
func TestFailureSeriesStartAtZero(t *testing.T) {
	m := metrics.New(metrics.Replicas{Pool: pool.Static{}}, func() float64 { return 0 })
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, f := range families {
		for _, s := range f.GetMetric() {
			key := f.GetName()
			for _, l := range s.GetLabel() {
				key += "," + l.GetName() + "=" + l.GetValue()
			}
			have[key] = true
		}
	}
	for _, want := range []string{
		"tsgw_requests_total,code=Unavailable,priority=normal",
		"tsgw_attempts_total,result=failed",
		"tsgw_attempts_total,result=unreachable",
		"tsgw_ejections_total,trigger=passive",
		"tsgw_shed_total,priority=low",
	} {
		if !have[want] {
			t.Errorf("no series %s before any request", want)
		}
	}
}

func TestMetricsCountShedRequests(t *testing.T) {
	rs := startReplicas(t, fakebackend.Config{})
	var m *metrics.Metrics
	c := startGatewayConfig(t, rs, "round_robin", func(cfg *Config) {
		m = metrics.New(metrics.Replicas{Pool: cfg.Backends}, func() float64 { return 0 })
		cfg.Metrics = m
		cfg.MaxInflight = 1 // low priority is admitted below 0.7 streams: never
	})
	_, _, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "hi", Priority: pb.Priority_PRIORITY_LOW})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v, want ResourceExhausted", err)
	}
	if got := metric(t, m, "tsgw_shed_total", map[string]string{"priority": "low"}); got != 1 {
		t.Errorf("shed_total{low} = %v, want 1", got)
	}
	if got := metric(t, m, "tsgw_requests_total", map[string]string{"code": "ResourceExhausted", "priority": "low"}); got != 1 {
		t.Errorf("requests_total{ResourceExhausted,low} = %v, want 1", got)
	}
}

func TestTraceShowsEachAttempt(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	t.Cleanup(func() { tp.Shutdown(context.Background()) })
	// The server's per-attempt tracer comes from the global provider.
	otel.SetTracerProvider(tp)

	rs := startReplicas(t, fakebackend.Config{FailRate: 1}, fakebackend.Config{})
	c := startGatewayConfig(t, rs, "round_robin", nil,
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tp))))
	if _, _, err := generate(context.Background(), c, &pb.GenerateRequest{Prompt: "hi", MaxTokens: 4}); err != nil {
		t.Fatal(err)
	}
	tp.ForceFlush(context.Background())

	var root sdktrace.ReadOnlySpan
	var attempts []sdktrace.ReadOnlySpan
	for _, s := range spans.Ended() {
		switch s.Name() {
		case "tinyserve.inference.v1.Inference/Generate":
			root = s
		case "attempt":
			attempts = append(attempts, s)
		}
	}
	if root == nil {
		t.Fatal("no server span")
	}
	if len(attempts) != 2 {
		t.Fatalf("%d attempt spans, want 2 (failed, then failed over)", len(attempts))
	}
	attr := func(s sdktrace.ReadOnlySpan, key string) string {
		for _, kv := range s.Attributes() {
			if string(kv.Key) == key {
				return kv.Value.String()
			}
		}
		return ""
	}
	for i, s := range attempts {
		if s.Parent().SpanID() != root.SpanContext().SpanID() {
			t.Errorf("attempt %d is not a child of the server span", i+1)
		}
		if got := attr(s, "tsgw.attempt"); got != []string{"1", "2"}[i] {
			t.Errorf("attempt span %d has tsgw.attempt=%s", i+1, got)
		}
	}
	first, second := attempts[0], attempts[1]
	if first.Status().Code != otelcodes.Error || attr(first, "tsgw.result") != "failed" {
		t.Errorf("first attempt: status %v, result %q; want an error, failed", first.Status(), attr(first, "tsgw.result"))
	}
	if attr(first, "tsgw.backend") == attr(second, "tsgw.backend") {
		t.Error("both attempts went to the same replica")
	}
	if attr(second, "tsgw.result") != "ok" || attr(second, "tsgw.backend.tokens_generated") != "4" {
		t.Errorf("second attempt: result %q, tokens %q; want ok, 4",
			attr(second, "tsgw.result"), attr(second, "tsgw.backend.tokens_generated"))
	}
	if root.Status().Code == otelcodes.Error {
		t.Errorf("server span marked as an error for a request that succeeded: %v", root.Status())
	}
}
