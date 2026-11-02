// Package metrics holds the gateway's Prometheus metrics.
//
// Request metrics are recorded by the server as requests run. Per-replica
// state (load, ejection) is read from the pool when Prometheus scrapes, so a
// replica that leaves the pool leaves no stale series behind.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/muralidhardon/tinyserve-gateway/internal/backend"
	"github.com/muralidhardon/tinyserve-gateway/internal/pool"
)

const namespace = "tsgw"

// Metrics is safe for concurrent use. A nil *Metrics records nothing.
type Metrics struct {
	Registry *prometheus.Registry

	requests      *prometheus.CounterVec
	shed          *prometheus.CounterVec
	attempts      *prometheus.CounterVec
	retries       prometheus.Counter
	budgetDenied  prometheus.Counter
	routing       *prometheus.CounterVec
	prefixBlocks  prometheus.Histogram
	ejections     *prometheus.CounterVec
	ttft          prometheus.Histogram
	interChunk    prometheus.Histogram
	duration      prometheus.Histogram
	promptTokens  prometheus.Counter
	reusedTokens  prometheus.Counter
	tokens        prometheus.Counter
	inflightGauge prometheus.GaugeFunc
}

// Replicas is what the per-replica collector reads at scrape time.
type Replicas struct {
	Pool    pool.Source
	Ejected func(*backend.Backend) bool // nil: never ejected
}

// New registers the gateway's metrics, plus Go runtime and process metrics.
// inflight reports the streams the gateway is serving.
func New(replicas Replicas, inflight func() float64) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "requests_total",
			Help: "Generate requests by final gRPC code and priority.",
		}, []string{"code", "priority"}),
		shed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "shed_total",
			Help: "Requests refused by admission control, by priority.",
		}, []string{"priority"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "attempts_total",
			Help: "Attempts on a replica, by outcome: ok, unreachable, overloaded, failed, rejected, canceled.",
		}, []string{"result"}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "retries_total",
			Help: "Attempts after a request's first, i.e. failovers before the first token.",
		}),
		budgetDenied: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "retry_budget_exhausted_total",
			Help: "Retries not made because the retry budget was spent.",
		}),
		routing: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "routing_decisions_total",
			Help: "Routing decisions by reason: prefix_match, spill (matched replica over its load bound), no_match, or the router's name.",
		}, []string{"reason"}),
		prefixBlocks: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "prefix_match_blocks",
			Help:    "prefix_aware: prompt blocks the chosen replica was predicted to have cached.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64, 128},
		}),
		ejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "ejections_total",
			Help: "Replica ejections, by trigger: active (health checks) or passive (failed requests).",
		}, []string{"trigger"}),
		ttft: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "ttft_seconds",
			Help:    "Time from receiving a request to sending its first text, failovers included.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		interChunk: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "inter_chunk_seconds",
			Help:    "Time between successive chunks of one stream. A chunk is one event from the replica, usually several tokens (TinyServe's steps_per_call).",
			Buckets: []float64{.0005, .001, .002, .005, .01, .02, .05, .1, .25, .5, 1},
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "request_duration_seconds",
			Help:    "Time from receiving a request to the end of its stream, for every outcome.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}),
		promptTokens: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "prompt_tokens_total",
			Help: "Prompt tokens of completed requests, as the replicas counted them.",
		}),
		reusedTokens: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "prefix_tokens_reused_total",
			Help: "Prompt tokens the replicas served from their prefix caches.",
		}),
		tokens: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "tokens_generated_total",
			Help: "Tokens generated for completed requests.",
		}),
		inflightGauge: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "inflight",
			Help: "Streams the gateway is serving.",
		}, inflight),
	}
	reg.MustRegister(
		m.requests, m.shed, m.attempts, m.retries, m.budgetDenied, m.routing, m.prefixBlocks,
		m.ejections, m.ttft, m.interChunk, m.duration, m.promptTokens, m.reusedTokens, m.tokens,
		m.inflightGauge, &replicaCollector{replicas},
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	// Start every series we expect at 0. A labelled counter otherwise
	// appears only at its first increment, and rate() and increase() can't
	// see the jump from nothing to that first value -- so a burst of
	// failures in one scrape interval would never show on a graph.
	for _, p := range []string{"low", "normal", "high"} {
		m.shed.WithLabelValues(p)
		for _, c := range []string{"OK", "Canceled", "InvalidArgument", "DeadlineExceeded", "ResourceExhausted", "Unavailable", "Internal"} {
			m.requests.WithLabelValues(c, p)
		}
	}
	for _, r := range []string{"ok", "unreachable", "overloaded", "rejected", "failed", "canceled"} {
		m.attempts.WithLabelValues(r)
	}
	m.ejections.WithLabelValues("active")
	m.ejections.WithLabelValues("passive")
	return m
}

// InitRoutes starts the routing-decision series for these reasons at 0.
func (m *Metrics) InitRoutes(reasons ...string) {
	if m == nil {
		return
	}
	for _, r := range reasons {
		m.routing.WithLabelValues(r)
	}
}

// Request records a finished request: its final gRPC code, and how long it took.
func (m *Metrics) Request(code, priority string, d time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(code, priority).Inc()
	m.duration.Observe(d.Seconds())
}

func (m *Metrics) Shed(priority string) {
	if m != nil {
		m.shed.WithLabelValues(priority).Inc()
	}
}

// Attempt records one attempt's outcome; retry is true for every attempt after a request's first.
func (m *Metrics) Attempt(result string, retry bool) {
	if m == nil {
		return
	}
	m.attempts.WithLabelValues(result).Inc()
	if retry {
		m.retries.Inc()
	}
}

func (m *Metrics) RetryBudgetExhausted() {
	if m != nil {
		m.budgetDenied.Inc()
	}
}

// Route records a routing decision. matchBlocks < 0 means the router doesn't track prefixes.
func (m *Metrics) Route(reason string, matchBlocks int) {
	if m == nil {
		return
	}
	m.routing.WithLabelValues(reason).Inc()
	if matchBlocks >= 0 {
		m.prefixBlocks.Observe(float64(matchBlocks))
	}
}

// Ejection records a replica ejection; passive is true when failed requests caused it.
func (m *Metrics) Ejection(passive bool) {
	if m == nil {
		return
	}
	trigger := "active"
	if passive {
		trigger = "passive"
	}
	m.ejections.WithLabelValues(trigger).Inc()
}

func (m *Metrics) FirstToken(d time.Duration) {
	if m != nil {
		m.ttft.Observe(d.Seconds())
	}
}

func (m *Metrics) InterChunk(d time.Duration) {
	if m != nil {
		m.interChunk.Observe(d.Seconds())
	}
}

// Usage records a completed request's token counts.
func (m *Metrics) Usage(prompt, reused, generated uint32) {
	if m == nil {
		return
	}
	m.promptTokens.Add(float64(prompt))
	m.reusedTokens.Add(float64(reused))
	m.tokens.Add(float64(generated))
}

// replicaCollector reports per-replica state as the gateway sees it.
type replicaCollector struct{ Replicas }

var (
	replicasDesc = prometheus.NewDesc(namespace+"_replicas", "Replicas in the pool.", nil, nil)
	loadDesc     = prometheus.NewDesc(namespace+"_backend_load",
		"A replica's load as the router sees it: the larger of its scraped queue + active sequences and this gateway's in-flight streams to it.",
		[]string{"backend"}, nil)
	inflightDesc = prometheus.NewDesc(namespace+"_backend_inflight", "This gateway's open streams to a replica.", []string{"backend"}, nil)
	ejectedDesc  = prometheus.NewDesc(namespace+"_backend_ejected", "1 if a replica is ejected.", []string{"backend"}, nil)
)

func (c *replicaCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- replicasDesc
	ch <- loadDesc
	ch <- inflightDesc
	ch <- ejectedDesc
}

func (c *replicaCollector) Collect(ch chan<- prometheus.Metric) {
	bs := c.Pool.Backends()
	ch <- prometheus.MustNewConstMetric(replicasDesc, prometheus.GaugeValue, float64(len(bs)))
	for _, b := range bs {
		ch <- prometheus.MustNewConstMetric(loadDesc, prometheus.GaugeValue, float64(b.Load()), b.Name)
		ch <- prometheus.MustNewConstMetric(inflightDesc, prometheus.GaugeValue, float64(b.Inflight()), b.Name)
		ejected := 0.0
		if c.Ejected != nil && c.Ejected(b) {
			ejected = 1
		}
		ch <- prometheus.MustNewConstMetric(ejectedDesc, prometheus.GaugeValue, ejected, b.Name)
	}
}
