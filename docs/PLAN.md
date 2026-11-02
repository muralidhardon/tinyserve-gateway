# TinyServe Gateway: A Go Inference Gateway for TinyServe

> **This is the original plan, kept as written.** What was built and measured
> is in the [README](../README.md), and why it works that way is in
> [DESIGN.md](DESIGN.md). Differences from the plan:
> - The build ran in seven phases instead of the milestones below.
> - There is no circuit breaker. Outlier ejection with backoff does that job.
> - Autoscaling is configured but not verified.
> - There is no cost model. The real engine's prefix cache made the routing
>   wins measurable without one.

> "I built the engine and the distributed system that serves it."

[TinyServe](https://github.com/muralidhardon/tinyserve) is a from-scratch LLM inference server: a hand-written C engine (KV cache, continuous batching, int8 quantization, 100x faster than its NanoForge baseline) behind a FastAPI server with Prometheus metrics.

**TinyServe Gateway** is a Go service that fronts a fleet of TinyServe replicas. It adds gRPC streaming, prefix-aware routing, fault tolerance, admission control and Kubernetes-native deployment.

**Tags:** AI Infrastructure · AI Inference Optimization · ML Systems · Backend · Kubernetes · Networking

---

## 1. Where TinyServe is today (what the gateway builds on)

| TinyServe has | Gateway uses it for |
|---|---|
| `POST /generate` (prompt, max_tokens, temperature, top_k, seed). Returns the full completion, returns **503 when the queue is full** | Backend call, plus a backpressure signal for routing and retries |
| `GET /health` | Active health checks |
| `GET /metrics`: `queue_depth`, `active_sequences`, `tokens_per_second`, `request_latency` p50/p95, `kv_cache_hit_ratio`, `context_shifts_total`, `batch_steps/sequences_total` | Load-aware routing, autoscaling signals, dashboards |
| Per-sequence KV cache, continuous batching, bounded queue (`TINYSERVE_MAX_QUEUE`) | The gateway sits on top of this single-node scheduler |
| Dockerfile and docker-compose | Starting point for K8s manifests |

**What TinyServe lacks, so the gateway project includes the engine-side work:**

1. **Token streaming.** `/generate` returns the whole completion. Add an SSE or chunked streaming endpoint, with the C engine yielding each token to the scheduler. This enables TTFT and inter-token latency metrics, and mid-stream failure handling.
2. **Cross-request prefix cache.** Today the KV cache is per-sequence. Add a block-hashed prefix cache so a new request with a known prefix skips prefill for those tokens, and export real hit metrics. Without this, prefix-aware routing has nothing to exploit. (Engine-side work in C is itself the "I built both halves" story.)
3. **Lightweight routing-feedback endpoint** (optional): `GET /cache/prefixes` returning the hashes of resident prefix blocks, so the gateway can correct its index.

## 2. Problem

- **Cache locality.** Requests sharing a system prompt, few-shot examples or chat history should land on the replica that already holds that prefix's KV blocks.
- **Hot spots.** Pure locality routing overloads whichever replica owns a popular prefix.
- **Failures.** Replicas crash, stall or get preempted, and streams can die mid-generation.
- **Overload.** Unbounded queues turn spikes into timeouts everywhere. TinyServe's 503 is the right signal, but something has to act on it.
- **Operations.** Replicas scale and roll on Kubernetes, and gRPC/HTTP2 connection-level balancing in a normal Service sends all of one client's requests to one pod.

## 3. Goals and Non-Goals

**Goals:** gRPC streaming API · prefix-aware + load-aware routing · health checking, outlier ejection, retries with budgets, circuit breaking · admission control and per-tenant limits · K8s discovery, drain and custom-metric autoscaling · metrics, traces, logs · reproducible benchmarks and chaos tests.

**Non-goals:** new model architectures, GPU support, multi-region routing (stretch).

## 4. Architecture

```
                 ┌──────────────────────────────────────────┐
  clients ──gRPC─▶│            TinyServe Gateway (Go)        │
 (stream)         │  ┌─────────┐  ┌──────────┐  ┌─────────┐  │
                  │  │ AuthN / │─▶│Admission │─▶│ Router  │  │
                  │  │ RateLim │  │ & Queue  │  │(prefix +│  │
                  │  └─────────┘  └──────────┘  │  load)  │  │
                  │                             └────┬────┘  │
                  │  ┌─────────────┐   ┌─────────────▼────┐  │
                  │  │ Discovery   │◀──│ Backend pool:    │  │
                  │  │(K8s EndptSl)│   │ health, breaker, │  │
                  │  └─────────────┘   │ /metrics scraper │  │
                  │                    └─────────────┬────┘  │
                  └──────────────────────────────────┼───────┘
                                    HTTP (SSE) streams│
                    ┌──────────────┬──────────────────┼─────────┐
                    ▼              ▼                  ▼         ▼
              tinyserve-0     tinyserve-1        tinyserve-2   ...N
            (FastAPI + C engine, KV + prefix cache)

   Prometheus ◀── metrics     OTel collector ◀── traces     HPA/KEDA ◀── queue depth / TTFT
```

| Component | Responsibility |
|---|---|
| **API layer** | gRPC `Generate` (server-streaming), `Health`; optional OpenAI-style HTTP/SSE shim. Deadline and cancellation propagation (client cancel → abort backend request). |
| **Admission and queue** | Bounded priority queue; load shedding with `RESOURCE_EXHAUSTED` + retry hint; per-tenant token buckets (requests and tokens). |
| **Router** | Pluggable strategies: `round_robin`, `least_loaded` (power-of-two-choices), `consistent_hash`, `prefix_aware`. |
| **Backend pool** | Per-replica connection, health state, circuit breaker, in-flight count, and the latest scraped `/metrics` (queue depth, active sequences). |
| **Discovery** | Watches EndpointSlices of a headless Service; static list for local dev. |
| **Telemetry** | Prometheus, OpenTelemetry, `slog`. |

## 5. Prefix-Aware Routing (the centerpiece)

1. **Chunk the prompt** into blocks matching the engine's prefix-cache block size. Hash with chaining (`h_i = H(h_{i-1} ‖ block_i)`) so each hash identifies the whole prefix up to that block. Reuse the engine's tokenizer, or hash bytes/characters consistently on both sides.
2. **Gateway radix tree** maps hash chains to the replicas that served them (LRU + TTL). Optionally reconcile against the replicas' `/cache/prefixes`.
3. **Score** each replica: `α·matched_blocks − β·normalized_load − γ·error_penalty`, where load comes from scraped `queue_depth` + `active_sequences` and the gateway's own in-flight count.
4. **Bounded-load guard:** if the best match exceeds `c × mean load`, fall to the next-best match, else power-of-two-choices on load.
5. **Cold path:** least-loaded P2C, then record the mapping.

Compare against round-robin, random, least-loaded and consistent-hash-on-system-prompt. Benchmark the radix lookup itself with `go test -bench` and `pprof`.

## 6. Fault Tolerance

| Failure | Handling |
|---|---|
| Replica down or unreachable | Active `/health` checks + passive outlier detection → eject with exponential backoff, probe to re-admit. |
| Replica overloaded (503) | Treat as a soft signal: retry elsewhere, lower that replica's score, no breaker trip on 503 alone. |
| Gray failure (slow) | Per-request deadlines, latency-based ejection, optional hedging for short generations. |
| Failure before first token | Transparent retry on another replica under a **retry budget** (≤10% of traffic). |
| Failure mid-stream | (a) Clean error with `partial` status; (b) **resume**: resend prompt + tokens so far to another replica (seeded sampling makes this deterministic at temperature 0), continuing the stream. |
| Gateway failure | Stateless apart from the rebuildable prefix index; multiple replicas; graceful shutdown drains in-flight streams. |
| Replica scale-down | Gateway "draining" state + `preStop` hook. |

Prove it with chaos tests: kill pods under load, inject latency and connection drops via a fault-injecting fake backend, and assert success-rate SLOs.

## 7. API Sketch

```proto
service Inference {
  rpc Generate(GenerateRequest) returns (stream GenerateChunk);
}
message GenerateRequest {
  string prompt = 1;
  uint32 max_tokens = 2;
  float temperature = 3;
  uint32 top_k = 4;
  optional uint64 seed = 5;
  string tenant_id = 6;
  Priority priority = 7;
}
message GenerateChunk {
  string text = 1;
  bool done = 2;
  Usage usage = 3;          // final chunk: tokens, queue_ms, ttft_ms, replica
}
```

Fields mirror TinyServe's existing `/generate` parameters.

## 8. Kubernetes

- Helm chart (or kustomize): gateway `Deployment`, TinyServe `Deployment`, **headless Service** for discovery, PDB, resource requests/limits, readiness gated on model load, liveness and startup probes.
- `client-go` EndpointSlice informer with minimal RBAC.
- **Autoscaling** the TinyServe tier with HPA/KEDA on `tinyserve_queue_depth` or p95 latency from TinyServe's existing metrics, shown as a graph during a load ramp.
- README note: why ClusterIP balancing doesn't work for long-lived HTTP/2 streams, which is why L7 routing lives in the gateway.
- `make demo` with `kind`, Prometheus and Grafana. Stretch: Terraform to GKE/EKS on CPU nodes (cheap, since TinyServe is CPU-only).

## 9. Observability

Gateway metrics: `requests_total{status,tenant}`, `ttft_seconds`, `inter_token_latency_seconds`, `queue_wait_seconds`, `inflight`, `routing_decisions_total{reason}`, `prefix_match_blocks`, `retries_total`, `shed_total`, `backend_state`. Traces: admit → route → backend stream, with the replica and matched prefix as attributes. A checked-in Grafana dashboard joins gateway and TinyServe metrics.

## 10. Benchmarks and Evidence

One important caveat: TinyServe's model is tiny (1.17M parameters, 128-token context), so a request takes about 5 ms and the prompts are short. Raw prefill savings will be small and the gateway could become the bottleneck. Handle this honestly:

- Add a configurable **cost model to the fake/real backend** (e.g. `TINYSERVE_PREFILL_COST_MS_PER_TOKEN`) or use a larger model, so prefix hits translate into measurable TTFT savings. State clearly which results come from the real engine and which from the simulated cost.
- Run the load generator on a separate machine or CPU quota so it doesn't skew results.

Workloads: shared system prompt · multi-turn chat · RAG-style shared context · unique prompts (worst case, must show no regression). Report: tokens/s, TTFT p50/p95/p99, cache hit ratio, success rate under chaos, and routing-strategy comparison tables and plots. Target headline: *"Prefix-aware routing cut p95 TTFT X% vs round-robin on shared-prefix workloads, with no loss on unique prompts; 99.x% success while killing a pod every N seconds."* Use the same methodology style as TinyServe's README (hardware, build, median of runs, raw JSON checked in).

## 11. Milestones

| Phase | Deliverable |
|---|---|
| **0. Engine prep** | SSE streaming endpoint in TinyServe; expose per-request TTFT; (optional) prefix cache in the C engine with block hashing and hit metrics. |
| **1. Skeleton** | Proto, Go gateway with static backends, round-robin, streaming passthrough, cancellation, graceful shutdown. |
| **2. Routing** | `/metrics` scraper, P2C least-loaded, consistent hash, prefix index + prefix-aware strategy, microbenchmarks. |
| **3. Resilience** | Health checks, ejection, breaker, retry budget, deadlines, admission control, shedding, fake fault-injecting backend. |
| **4. Kubernetes** | Dockerfiles, Helm, EndpointSlice discovery, drain, HPA on custom metrics, `kind` demo. |
| **5. Evidence** | Dashboards, load generator, chaos tests, benchmark README with raw results. |
| **6. Stretch** | Mid-stream resume, OpenAI shim, tenant quotas and priorities, index reconciliation, multi-model routing. |

## 12. Repo Layout

```
tinyserve-gateway/
├── api/proto/
├── cmd/{gateway,loadgen,fakebackend}/
├── internal/{router,pool,admission,discovery,server,telemetry}/
├── deploy/{helm,k8s,grafana}/
├── bench/            # scripts, raw JSON, plots (same style as tinyserve/bench)
├── docs/             # design notes / ADRs
└── Makefile          # build, test, demo, bench
```

Engine changes (streaming, prefix cache) land as PRs in `tinyserve`, linked from the gateway README.

## 13. Testing

Unit tests (scoring, radix tree with eviction, breaker state machine, token buckets) · fuzz tests (prefix hashing) · `go test -race` · in-process integration with `bufconn` and fake backends · chaos tests under load · CI with golangci-lint, tests, image builds, plus a compatibility test against the real TinyServe container.

## 14. What This Demonstrates

- **Go depth:** goroutine lifecycles, context cancellation across streaming hops, bounded queues, lock design, pprof.
- **Networking:** gRPC/HTTP2 streaming, L4 vs L7 balancing, backpressure, deadlines.
- **ML systems:** KV and prefix cache economics, TTFT vs throughput, continuous batching from the scheduler's perspective.
- **Distributed systems:** failure modes, retry budgets, circuit breaking, graceful drain.
- **Kubernetes:** discovery, probes, custom-metric autoscaling, observability.
- **Ownership:** C engine → Python server → Go gateway → K8s, all yours.

## 15. Resume Hooks

- *Built a Go inference gateway for a custom C/Python LLM engine: gRPC streaming, prefix-aware routing, circuit breaking and load shedding, deployed on Kubernetes with autoscaling on engine-level metrics.*
- *Added token streaming and a block-hashed prefix cache to the C inference engine; prefix-aware routing cut p95 TTFT by X% vs round-robin on shared-prefix workloads.*
- *Sustained Y% success under chaos testing (random pod kills).*

## 16. Risks

| Risk | Mitigation |
|---|---|
| Scope creep | Phases 0–3 already make a strong project; ship those first. |
| Tiny model makes routing wins invisible | Cost model or larger model, disclosed clearly (§10). |
| Prefix index drifts from engine cache | TTL + LRU, `/cache/prefixes` reconciliation, measure real hit ratio. |
| Python server is the bottleneck, not the engine | Profile first; the gateway's resilience and routing value remains, and it's a finding worth reporting. |
| Cost | Everything runs on CPU in `kind`; cloud deployment is optional. |
