# TinyServe Gateway

A Go gRPC gateway in front of a fleet of [TinyServe](https://github.com/muralidhardon/tinyserve)
replicas. TinyServe is the engine (a hand-written C transformer with a KV cache,
continuous batching and int8 quantization); this is the distributed system that
serves it.

```
client ──gRPC stream──▶ gateway ──HTTP/SSE──▶ TinyServe replicas (C engine + FastAPI)
```

The full plan, from this skeleton to prefix-aware routing on Kubernetes, is in
[`docs/PLAN.md`](docs/PLAN.md).

## Results so far

All numbers are client-side, from `cmd/loadgen`, with Poisson arrivals, on one
Apple M3 Pro laptop. Phases 2–4 ran three replicas as local processes; Phase 5
ran on a 3-node kind cluster. Raw per-request data and setup for each table are
in [`bench/results/`](bench/results/).

### Observability (Phase 6)

A TinyServe process is killed with SIGKILL while serving 120 req/s, on the kind
cluster with Prometheus, Jaeger and Grafana installed and every request
traced. 28,948 of 28,951 requests succeed. The gateway's metrics and traces
show exactly how the 3 that failed were lost:

![Grafana dashboard during the crash](docs/img/phase6_dashboard.png)

| Time | What the gateway saw |
|---|---|
| 07:17:29.72 | The process dies. 3 streams already generating break mid-stream. Text had reached those clients, so they end `UNAVAILABLE`: the only failures. |
| same instant | 2 new requests find the replica unreachable and fail over. Both succeed. |
| +0.8 s | Kubernetes marks the pod not-ready, and discovery drops it from the pool. No ejection was needed: Kubernetes noticed before the gateway's 5-failure threshold was reached. |
| +2.8 s | The restarted container passes its readiness probe and rejoins the pool. |
| next ~60 s | p99 TTFT rises to about 200 ms, and so does the engine's own p99. The survivor carries all the traffic while the restarted replica's prefix cache refills. |

One of the failovers, in Jaeger. The first attempt hit the dead replica
and failed in 14 ms; the second succeeded on the other replica:

![Trace of a failover: two attempt spans](docs/img/phase6_trace.png)

**What telemetry costs.** These runs put the gateway in front of three fake
replicas that answer instantly, so the gateway itself is the bottleneck: a
closed loop of 64 clients, 30,000 requests per run, 3 rounds. The table shows
the median of each.

| Telemetry | Throughput | p99 latency | Gateway CPU per request |
|---|---:|---:|---:|
| off | 27,100 req/s | 4.7 ms | 159 µs |
| metrics | 27,400 req/s | 4.8 ms | 158 µs |
| metrics + tracing, 0% sampled | 24,900 req/s | 5.5 ms | 191 µs |
| metrics + tracing, 10% sampled | 25,000 req/s | 5.6 ms | 186 µs |
| metrics + tracing, 100% sampled | 24,500 req/s | 6.5 ms | 197 µs |

- **Metrics are free** at this resolution.
- **Tracing costs about 30 µs per request, whatever the sample rate.**
  - With tracing installed, the gRPC and HTTP instrumentation runs on every
    request: context propagation, span plumbing, and `traceparent` injection.
    Sampling 0% costs as much as sampling 10%.
  - Recording and exporting every span adds only about 8 µs more.
  - So lowering the sample rate barely saves CPU. The real choice is whether
    to install tracing at all.
- **Against real inference, this is noise.** A TinyServe request takes
  milliseconds of engine CPU, so tens of microseconds in the gateway don't
  matter. They would matter for a gateway in front of something much
  cheaper than an LLM.

Three earlier runs on the same cluster each taught something:

- **Deleting a pod isn't a crash.** The first demo used
  `kubectl delete --force --grace-period 0`, and all 28,620 requests
  succeeded. The pod leaves the EndpointSlice at once, but the container
  still gets SIGTERM, so its streams finish
  ([`phase6_demo_force_delete.md`](bench/results/phase6_demo_force_delete.md)).
  A SIGKILL from the node is a crash.
- **A rate graph missed the first crash.** The gateway logged the failures,
  but every failure counter was created by the crash itself, so `rate()`
  showed nothing. Counters now start at 0.
- **I starved the engines myself.** Building an image during a run took the
  CPU the engines needed: both replicas hit the 5 s first-token timeout and
  were ejected in turn. Those numbers are not reported; the run above was
  done on an idle machine.

Reproduce with `OBSERVABILITY=1 deploy/kind/up.sh`, then
`bench/phase6_demo.sh`, and `bench/phase6_overhead.sh` (without the cluster)
([`phase6_overhead.md`](bench/results/phase6_overhead.md)).

### Rolling updates on Kubernetes (Phase 5)

The setup is real TinyServe on a 3-node kind cluster: 2 TinyServe pods, 2
gateway pods, and a loadgen pod in the cluster. Loadgen sends 100 req/s for
75 s, 32 tokens per request. It uses gRPC `round_robin` over the gateways'
headless Service, so it holds a connection to every gateway pod.

At t=10 s every TinyServe pod is replaced (`kubectl rollout restart`). Once
that finishes, every gateway pod is replaced too.

| ~7,500 requests, every pod replaced | Failed | p99 TTFT | p99 latency |
|---|---:|---:|---:|
| no drain (preStop 0, 1 s grace period) | 2 | 53.4 ms | 104.5 ms |
| drain, client re-resolves DNS at most every 30 s (gRPC default) | 768 (10%) | 39.7 ms | 84.1 ms |
| **drain, client re-resolves DNS at most every 1 s** | **0** | 69.9 ms | 144.1 ms |

- **The failures came from the client, not the gateway.** Every failure in
  the second row falls in one ~8 s window, right after the gateway pods were
  replaced:
  - gRPC re-resolves a `dns:///` name when a connection drops, but at most
    once per interval, and the default interval is 30 s.
  - The first old gateway pod leaves, and the client re-resolves. It gets
    the new pod and the second old pod.
  - Seconds later the second old pod leaves. The 30 s interval hasn't passed,
    so the client keeps only that stale address and every request fails with
    `UNAVAILABLE`. That lasts until the interval passes.
  - With `-dns-min-interval 1s`, the same rollout loses nothing. Any gRPC
    client that balances over a headless Service in a rolling deployment
    needs this setting, or something like xDS.
- **Replacing TinyServe pods cost nothing in any run.**
  - The gateway watches EndpointSlices. When a pod starts terminating, it is
    marked not-ready and the gateway stops sending it new streams, while
    streams already open finish.
  - New pods join once their readiness probe passes.
- **Drain barely mattered here, because the requests are short.** With
  ~10 ms requests, few streams are open when a pod is killed. A request
  refused by a pod that is shutting down fails over to another replica
  before its first token, so even the no-drain run lost only 2 requests.
  Long generations would leave more streams open at the moment of the kill;
  that case is not measured here.
- The p99 differences between rows are within run-to-run noise. A single
  slow second (pods starting on the same 3 nodes) moves p99 by about 50 ms.

Per-5-second timelines are in
[`phase5_rollout.md`](bench/results/phase5_rollout.md). Reproduce with
`deploy/kind/up.sh` then `bench/phase5_rollout.sh`.

### Fault tolerance (Phase 4)

Four fake replicas take 150 req/s, 64 tokens per request, with a 10 s client
deadline. Partway through the run, one replica is frozen with `SIGSTOP` at
t=8s, another is killed with `SIGKILL` at t=16s, and the frozen one is resumed
with `SIGCONT` at t=24s. A frozen replica is the hard case. Its kernel still
accepts connections, so nothing errors; requests just hang. In the "no
protection" rows, Phase 4 is switched off, so the gateway behaves as it did
after Phase 3.

| 4,800+ requests, replica frozen then another killed | Failed | Retries | p95 TTFT | p99 TTFT |
|---|---:|---:|---:|---:|
| round_robin, no protection | 125 (2.5%) | 1,278 | 2,015 ms | 2,024 ms |
| **round_robin, Phase 4** | **8 (0.16%)** | **77** | **19 ms** | 5,015 ms |
| least_loaded, no protection | 13 | 16 | 16.5 ms | 36.8 ms |
| **least_loaded, Phase 4** | **2** | **12** | **15.5 ms** | **30.1 ms** |

- **Load-blind routing, no protection.** Round-robin keeps sending the frozen
  replica a quarter of the traffic for the full 16 s:
  - Each request waits for the 2 s connect timeout, then retries elsewhere.
    That is the 2 s p95, and a quarter of all requests (1,278) become retries.
  - Requests that had already connected when the replica froze hang until the
    client deadline.
- **Phase 4 ejects the frozen replica within about 2 s**, after two failed
  health checks:
  - Requests sent to it in that window wait for the 5 s first-token timeout,
    then fail over. That window is the 5 s p99.
  - From then on the run looks as if the replica wasn't there, and it is
    re-admitted once a health check passes after `SIGCONT`.
  - The killed replica is ejected after 5 consecutive failed requests.
- **What still fails** is streams that had already started on a replica when
  it died. The gateway doesn't retry those, because the client has seen part
  of a completion.
- **Least-loaded with no protection** already survives a frozen replica
  fairly well. The replica's in-flight count climbs, so power-of-two-choices
  routes around it. Even so, without health checks:
  - the requests it does send there wait until the replica resumes or the
    deadline passes;
  - nothing takes the replica out of rotation.

Per-second timelines and raw data are in
[`phase4_chaos.md`](bench/results/phase4_chaos.md). Reproduce with
`bench/phase4_chaos.sh`.

### Prefix-aware routing (Phase 3)

The workload is 24 tenants, each with its own ~85-token system prompt plus a
short question, generating 16 tokens per request. Each TinyServe replica has a
48-block prefix cache. The tenants' prefixes need about 120 blocks: more than
one replica holds, but less than the fleet does. So whether a request finds its
prefix cached depends on where it is routed.

| 600 req/s, one hot tenant (Zipf 2.0, ~60% of traffic) | Prompt reused | p50 TTFT | p99 TTFT | p99 latency | Busiest replica |
|---|---:|---:|---:|---:|---:|
| least_loaded | 78.9% | 3.0 ms | 52.8 ms | 79.2 ms | 35% |
| consistent_hash | 84.4% | 4.0 ms | 55.0 ms | 69.3 ms | 68% |
| **prefix_aware** | **83.6%** | **3.6 ms** | **17.0 ms** | **21.9 ms** | **35%** |

| 600 req/s, Zipf 1.1 | Prompt reused | p50 TTFT | p95 TTFT | p99 TTFT | Errors |
|---|---:|---:|---:|---:|---:|
| prefix cache off | 0% | 1,565 ms | 1,674 ms | 1,752 ms | 1,247 rejected |
| round_robin | 57.3% | 4.7 ms | 23.2 ms | 39.1 ms | 0 |
| least_loaded | 56.4% | 5.7 ms | 26.4 ms | 61.6 ms | 0 |
| consistent_hash | 82.1% | 3.4 ms | 9.3 ms | 22.6 ms | 0 |
| **prefix_aware** | **80.2%** | **3.4 ms** | **10.6 ms** | **22.5 ms** | **0** |

Three findings, in order of size:

1. **The engine's prefix cache matters most.** At 600 req/s, the fleet without
   it is past capacity: prefill alone saturates it, so queues back up and 10% of
   requests are shed. With it, every router keeps p50 TTFT under 6 ms. That is
   TinyServe's half of the work.
2. **Cache-affine routing raises prompt reuse from about 57% to about 82%**,
   and cuts p95 TTFT about 2x compared with round-robin and least-loaded.
   Load-blind and cache-blind routers spread every tenant over every replica,
   and each replica's cache thrashes.
3. **Bounded load is what separates prefix-aware routing from consistent
   hashing.** Both hash on the prompt's start, so with moderate skew they tie.
   When one tenant is hot, consistent hashing sends it all to one replica (68% of
   traffic) and that replica's tail suffers. Prefix-aware routing spills the
   overflow once a replica exceeds its bounded share, and the spill replica then
   caches that prefix too. That keeps the hit rate and cuts p99 TTFT 3x.

At 300 req/s with Zipf 1.1, the fleet has headroom and the gaps shrink:
`prefix_aware` and `consistent_hash` both reach 82.5% reuse and 5.7 ms p95 TTFT,
against 9.9 ms for round-robin
([`phase3_prefix.md`](bench/results/phase3_prefix.md)). Reproduce with
`bench/phase3_prefix.sh` (set `RATE`, `ZIPF`, `ROUTERS`).

### Load-aware routing (Phase 2)

Three replicas, one of them slow, 64 tokens per request.

| Real TinyServe, 100 req/s, one replica uncached (~19x slower) | p95 TTFT | p95 latency | Timeouts | Throughput |
|---|---:|---:|---:|---:|
| round_robin | 24,097 ms | 25,680 ms | 25 / 1,524 | 34 req/s |
| **least_loaded** | **10 ms** | **196 ms** | **0** | **100 req/s** |

| Fake replicas, 150 req/s, one replica 3x slower | p95 TTFT | p95 latency | Failovers |
|---|---:|---:|---:|
| round_robin | 1,632 ms | 1,805 ms | 215 |
| random | 1,622 ms | 1,795 ms | 134 |
| consistent_hash | 1,399 ms | 1,572 ms | 5 |
| **least_loaded** | **31 ms** | **203 ms** | **0** |

Round-robin keeps sending the slow replica a third of the traffic, more than it
can serve, so its queue grows until it answers 503 and requests fail over (or,
on the real engine, wait past their deadline). Least-loaded sees the queue in
the replica's `/metrics` and sends it only what it can absorb (6% of traffic on
the real engine, 18% on the fakes). Consistent hashing is no better than
round-robin here, because it pins keys, not load.

The real-engine slow replica is an extreme case (an uncached replica).
Reproduce the fake run with `bench/phase2_uneven.sh`.

## Status

**Phase 6 (done): observability**

- **Prometheus metrics** on `-metrics-listen` (default `:9090`, path
  `/metrics`), all prefixed `tsgw_`:
  - **Requests:** `requests_total{code,priority}`, `ttft_seconds`,
    `inter_chunk_seconds`, `request_duration_seconds`, `inflight`.
  - **Routing:** `routing_decisions_total{reason}`, where the reason is
    `prefix_match`, `spill` or `no_match` for `prefix_aware`, and the router's
    name otherwise. Also `prefix_match_blocks`.
  - **Faults:** `attempts_total{result}`, `retries_total`,
    `retry_budget_exhausted_total`, `shed_total{priority}`,
    `ejections_total{trigger}`.
  - **Tokens:** `prompt_tokens_total`, `prefix_tokens_reused_total`,
    `tokens_generated_total`, as the replicas report them.
  - **Per replica:** `replicas`, `backend_load`, `backend_inflight` and
    `backend_ejected`. These are read from the pool at scrape time, so a
    replica that leaves leaves no stale series behind.
  - **Every labelled counter starts at 0.** Without that, a counter has no
    series until its first increment, and `rate()` can't see the jump from
    nothing to that first value. That was not hypothetical: in the first
    crash run, the gateway logged 161 failed attempts and 2 ejections, and
    every rate graph showed zero.
  - `inter_chunk_seconds` is per chunk, not per token. TinyServe sends several
    tokens per event (`steps_per_call`).
- **OpenTelemetry traces** with `-otlp-endpoint host:port` (OTLP/gRPC), sampling
  `-trace-sample` of requests (default 10%). A caller's `traceparent` decision
  is kept.
  - **The span tree** is the gRPC server span (otelgrpc), one `attempt` span
    per replica tried, and the HTTP client span (otelhttp) under each attempt.
  - **Each `attempt` span** carries the replica, the routing reason, the
    predicted prefix match and the outcome. It also carries the replica's own
    timings: queue wait, time to first token, latency, and prefix tokens
    reused. Together these split an attempt into queueing, prefill and decode
    without the Python server emitting spans of its own.
  - **Only generation requests are traced.** Health checks and `/metrics`
    scrapes run several times a second per replica and would bury the
    spans that matter.
- **Grafana dashboard** (`deploy/grafana/tinyserve-gateway.json`) puts gateway
  and engine metrics side by side:
  - the gateway's TTFT against the engine's own, where the gap is what the
    gateway adds;
  - prefix reuse as reported to the gateway against the engines' cache
    counters;
  - each replica's load as the router sees it against what the engine
    reports.
- **`OBSERVABILITY=1 deploy/kind/up.sh`** installs Prometheus, Jaeger and
  Grafana, and traces every request.
- **Not built:**
  - Spans inside TinyServe. The gateway does send `traceparent`, but the
    Python server doesn't read it yet.
  - Exemplars linking latency buckets to traces.
  - Alerting rules.

**Phase 5 (done): Kubernetes**

- **Live replica pool.** The replica set can change while the gateway runs.
  - A replica that stays across an update keeps its load figures, health
    history and open streams.
  - A removed replica finishes its open streams but gets no new ones.
  - The consistent-hash ring is rebuilt on each change. Adding a fourth
    replica moves about a quarter of keys.
- **EndpointSlice discovery** (`-k8s-service`, using client-go informers):
  - The gateway routes only to endpoints marked ready, so a terminating pod
    stops getting new streams at once.
  - It needs only `get`/`list`/`watch` on EndpointSlices in its own
    namespace.
  - Static `-backends` still works outside Kubernetes.
- **Readiness.** The gateway's gRPC health service reports NOT_SERVING until
  discovery finds a replica, and again once SIGTERM starts the drain.
- **Helm chart** (`deploy/helm/tinyserve-gateway`):
  - **TinyServe:** a headless Service, so the gateway can address each pod
    directly. Plus startup, readiness and liveness probes on `/health`, a
    `preStop` sleep, a non-root user, and soft anti-affinity across nodes.
  - **Gateway:** a ServiceAccount with its Role, gRPC probes, a read-only root
    filesystem, and a termination grace period sized to the drain timeout.
  - **Both:** rolling updates that add a pod before removing one
    (`maxSurge 1`, `maxUnavailable 0`), and PodDisruptionBudgets.
  - **Two gateway Services.** A ClusterIP Service balances connections, not
    requests, so a client's single HTTP/2 connection sticks to one gateway
    pod. The second, headless Service lets gRPC clients balance requests
    across every gateway pod themselves.
- **Autoscaling**, chosen with `autoscaling.type`:
  - `cpu`: a standard HorizontalPodAutoscaler.
  - `keda`: a KEDA ScaledObject driven by Prometheus. It scales on TinyServe's
    own queue depth plus active sequences (8 per replica by default).
  - CPU is a poor signal for this engine: a replica with a full batch and a
    long queue can sit at the same CPU use as one just keeping up.
  - **Not verified.** The chart installs both modes, but neither has been run
    under load. The `keda` path did have a bug in `up.sh`: Prometheus would
    not start, because its scrape timeout was longer than its 5 s interval.
    That was found and fixed in Phase 6. A KEDA install has still not
    completed here, because Docker on this machine stalls pulling KEDA's
    images from ghcr.io.
- **One image** (distroless, non-root) holds the gateway, loadgen, tsctl and the
  fake backend.
- **kind setup:** `deploy/kind/up.sh` creates the cluster, builds and loads the
  images, and installs the chart. With `AUTOSCALING=keda` it also installs
  Prometheus and KEDA.
- **Not built:**
  - TLS between client, gateway and replicas.
  - Topology-aware routing that prefers replicas in the gateway's own zone.
  - Scaling the gateway itself on stream count.

**Phase 4 (done): fault tolerance**

- **Active health checks.** `GET /health` runs every second with a 1 s
  timeout, and two failures in a row eject the replica.
- **Passive outlier detection.** Five failed requests in a row eject the
  replica:
  - These count as failures: unreachable, 5xx, a broken stream, or a
    first-token timeout.
  - A 503 is backpressure rather than a fault, and a 4xx is the client's own
    error, so neither counts.
  - Passive detection ejects at most half the pool, so a fleet-wide problem
    (a bad prompt, a client bug) can't drain every replica. Health-check
    ejections aren't capped.
- **Ejection backoff.**
  - An ejection lasts 2 s, doubling each time the same replica is ejected
    again, up to 60 s.
  - The replica comes back only once a health check passes.
  - A replica that stays in service for 60 s starts over at 2 s.
  - If every replica is ejected, the gateway uses them all anyway. A replica
    that may have recovered beats failing the request outright.
- **First-token timeout (5 s).**
  - An attempt that produces no text in that time is abandoned, and the
    request fails over to another replica. This catches replicas that accept
    connections and then stall, which connection errors never reveal.
  - Only the first token is timed, so a long generation isn't cut off.
- **Retry budget.**
  - Over the last 10 s, retries may be at most 10% of requests, plus 10 per
    second.
  - Without a budget, a fleet of n failing replicas turns each request into
    n attempts, and the retries add load to a fleet that is already failing.
- **Admission control.**
  - `-max-inflight` (default 1024) caps the streams the gateway serves at
    once. Beyond it, requests are shed with `RESOURCE_EXHAUSTED` rather than
    queued.
  - A new `priority` field on `GenerateRequest` decides who is shed first:
    low priority is admitted below 70% of the cap, normal below 90%, and high
    up to the cap.
- **Chaos tests.**
  - `go test ./internal/server -run Chaos` freezes one in-process replica and
    kills another under load. It asserts at least 99% success; the result is
    typically 99.9%.
  - `fakebackend -stall` and `bench/phase4_chaos.sh` (real processes,
    `SIGSTOP`/`SIGKILL`) do the same out of process.
- **Not built:**
  - Resuming a stream that fails mid-generation on another replica. This
    needs the engine to continue from given tokens with the same random
    state.
  - Request hedging.
  - Latency-based outlier ejection.

**Phase 3 (done): prefix cache in the engine, prefix-aware routing**

- TinyServe ([`streaming` branch](https://github.com/muralidhardon/tinyserve))
  has a prefix cache. K/V rows of prompt prefixes are shared across requests in
  16-token blocks, keyed by chained hashes, with LRU eviction. Reuse is exact,
  and TTFT for a prompt with a cached 64-token prefix drops 5.7–7x at the engine.
- `-router prefix_aware` keeps an index of chained hashes of 64-byte prompt
  blocks, each mapped to the replicas recently sent that prefix. The gateway has
  no tokenizer, so 64 bytes stands in for roughly one 16-token engine block. A
  request goes to the replica with the longest match, with ties going to the
  most recent owner, so prefixes don't hop between replicas.
- That choice is bounded: the replica may hold at most
  `ceil(1.25 × (total+1) / n) + 4` sequences
  ([consistent hashing with bounded loads](https://arxiv.org/abs/1608.01350),
  plus slack). The slack is there because continuous batching makes a few extra
  sequences nearly free, while a cache miss costs a full prefill. Over the
  bound, or with no match, the request falls back to least-loaded, and the
  chosen replica is recorded as an owner too.
- The index is LRU-bounded (65,536 blocks by default), with roots refreshed
  after their descendants so a prefix's start is evicted last. One routing
  decision takes about 1.3 µs for a 600-byte prompt.
- The index predicts the engines' caches but doesn't mirror them. The engines
  report what they actually reused (`backend_prefix_tokens_reused` in `Usage`),
  and that is the hit rate the tables show.
- `loadgen -workload tenants` generates Zipf-popular tenants with their own
  system prompts.

**Phase 2 (done): load-aware routing**

- The gateway scrapes each replica's `/metrics` (`tinyserve_queue_depth`,
  `tinyserve_active_sequences`) every 250 ms. A replica's load is the larger of
  that scraped figure (sees every client, but lags) and the gateway's own
  in-flight count (current, but sees only this gateway).
- `-router` picks the strategy: `round_robin`, `random`, `least_loaded`
  (power-of-two-choices, the default) or `consistent_hash` (keyed on the first
  64 bytes of the prompt, 128 virtual nodes per replica).
- A 503 is a soft signal: the replica is skipped by `least_loaded` and
  `consistent_hash` for its `Retry-After` window instead of being retried. A
  replica that refuses a connection is skipped for 1 s the same way; without
  that, a dead replica (no in-flight streams, frozen scrape figures) would look
  idle to `least_loaded` and attract traffic.
- `cmd/loadgen`: open-loop (Poisson) or closed-loop load with client-side
  TTFT and latency percentiles, per-replica share and raw JSON output.
- `fakebackend` models TinyServe's scheduler: `-slots` concurrent sequences, a
  `-max-queue` deep queue, then 503.

**Phase 1 (done): streaming gateway**

- `Inference.Generate` streams the completion over gRPC as TinyServe produces it
  (`api/proto/inference/v1/inference.proto`).
- **Failover before the first token**: if a replica is unreachable, overloaded
  (503) or fails, the request moves to the next one, trying each at most once.
  After text has reached the client, a failure ends the stream with
  `UNAVAILABLE` instead of retrying, because the client has already seen part of
  one completion.
- **Cancellation and deadlines propagate**: a client that cancels or times out
  closes the backend request, and TinyServe drops the sequence from its batch
  and frees its KV cache.
- **Graceful shutdown**: on SIGTERM the gRPC health service reports
  NOT_SERVING, open streams finish (up to `-drain-timeout`), then the process
  exits.
- Error mapping: overloaded → `RESOURCE_EXHAUSTED`, bad request →
  `INVALID_ARGUMENT`, replica down/failed → `UNAVAILABLE`, deadline →
  `DEADLINE_EXCEEDED`.
- `fakebackend`: a TinyServe stand-in with configurable latency and injected
  faults (500s, 503s, connections cut mid-stream), for tests and chaos runs.

**Requires** TinyServe's `POST /generate/stream` endpoint (server-sent events).

**Next**: the write-up (Phase 7).

## Run it

Against fake replicas:

```bash
make fake                                # two fake replicas + gateway on :9000
bin/tsctl -max-tokens 40 "Alice was"     # in another terminal
```

Against real TinyServe:

```bash
# in the tinyserve repo, two replicas
uvicorn server.main:app --port 8001 &
uvicorn server.main:app --port 8002 &

# here
make build
bin/gateway -listen :9000 -backends http://localhost:8001,http://localhost:8002
bin/tsctl -seed 1 -max-tokens 40 "Alice was"
```

```
Alice was a moment the way off," the Hatter, it got down
was thinking of a feafe a eanarre

[24 tokens from http://localhost:8001, attempts=1, gateway ttft=7.7ms, backend latency=6.1ms]
```

Server reflection is on, so `grpcurl -plaintext localhost:9000 list` works.

On Kubernetes (local kind cluster; needs Docker, kind, kubectl and helm):

```bash
deploy/kind/up.sh                        # 3-node cluster, both images, the chart
kubectl port-forward svc/ts-gateway 9000:9000 &
bin/tsctl -max-tokens 40 "Alice was"
kubectl scale deploy/ts-tinyserve --replicas 3   # the gateway picks it up within seconds
kind delete cluster --name tinyserve
```

With dashboards and traces:

```bash
OBSERVABILITY=1 deploy/kind/up.sh        # adds Prometheus, Jaeger, Grafana
kubectl -n monitoring port-forward svc/grafana 3000 &   # http://localhost:3000
kubectl -n monitoring port-forward svc/jaeger 16686 &   # http://localhost:16686
bench/phase6_demo.sh                     # 4 min of traffic, one TinyServe crash at t=60s
```

`deploy/kind/up.sh` expects the TinyServe checkout at `../tinyserve`
(`TINYSERVE_DIR` to change it).

| Gateway flag | Default | |
|---|---|---|
| `-listen` | `:9000` | gRPC address |
| `-backends` | `http://localhost:8000` | Comma-separated TinyServe base URLs (ignored with `-k8s-service`) |
| `-k8s-service` | | Discover replicas from this Service's EndpointSlices |
| `-k8s-namespace` | the pod's own | Namespace of `-k8s-service` |
| `-k8s-port` | `http` | Name of the Service port to send requests to |
| `-router` | `least_loaded` | `round_robin`, `random`, `least_loaded`, `consistent_hash`, `prefix_aware` |
| `-prefix-block-bytes` | `64` | `prefix_aware`: bytes per prefix block |
| `-prefix-load-factor`, `-prefix-slack` | `1.25`, `4` | `prefix_aware`: the load bound above |
| `-prefix-index-blocks` | `65536` | `prefix_aware`: prefix blocks remembered |
| `-scrape-interval` | `250ms` | How often to read each replica's `/metrics` |
| `-max-attempts` | `0` (all) | Replicas one request may try before its first token |
| `-health-interval`, `-health-timeout` | `1s`, `1s` | Active `/health` checks (`0` = off) |
| `-unhealthy-threshold` | `2` | Failed checks in a row that eject a replica |
| `-outlier-failures` | `5` | Failed requests in a row that eject a replica (`0` = off) |
| `-max-ejected` | `0.5` | Largest share of the pool that failed requests may eject |
| `-ejection-base`, `-ejection-max` | `2s`, `60s` | Ejection length, doubling with each repeat ejection |
| `-first-token-timeout` | `5s` | Abandon an attempt with no text and fail over (`0` = off) |
| `-retry-budget`, `-retry-min-per-sec` | `0.1`, `10` | Retries allowed per request, plus per second (negative = unlimited) |
| `-max-inflight` | `1024` | Streams served at once before shedding, lowest priority first (`0` = no limit) |
| `-drain-timeout` | `30s` | How long SIGTERM waits for open streams |
| `-metrics-listen` | `:9090` | HTTP address for Prometheus `/metrics` (empty = off) |
| `-otlp-endpoint` | | OTLP/gRPC collector `host:port` for traces (empty = tracing off) |
| `-trace-sample` | `0.1` | Fraction of requests traced, unless the caller's `traceparent` decided |
| `-v` | off | Debug logging |

## Develop

```bash
make test    # unit + in-process gRPC tests (bufconn + fake replicas)
make race    # same, with the race detector
make proto   # regenerate gen/ after editing the .proto (needs protoc, protoc-gen-go, protoc-gen-go-grpc)
```

## Layout

```
api/proto/            gRPC API
gen/                  generated code (committed)
cmd/gateway/          the gateway
cmd/fakebackend/      fault-injecting TinyServe stand-in
cmd/tsctl/            CLI client
cmd/loadgen/          load generator
bench/                benchmark scripts and raw results
deploy/helm/          Helm chart: TinyServe, gateway, autoscaling
deploy/kind/          local 3-node cluster, setup script, Jaeger and Grafana
deploy/grafana/       the Grafana dashboard
docs/img/             dashboard and trace screenshots
internal/backend/     TinyServe SSE client, /metrics scraping, health check, load and overload state
internal/pool/        the live replica set, shared by router, health checks and scraping
internal/discovery/   replicas from Kubernetes EndpointSlices
internal/health/      active health checks, outlier ejection with backoff
internal/metrics/     Prometheus metrics
internal/router/      round-robin, random, least-loaded (P2C), consistent hash, prefix-aware
internal/server/      gRPC service: admission, routing, failover, retry budget, error mapping
internal/fakebackend/ fake replica, shared by tests and cmd/fakebackend
```
