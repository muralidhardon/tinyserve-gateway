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

All numbers are client-side, from `cmd/loadgen`, with Poisson arrivals and three
replicas on one Apple M3 Pro laptop. Raw per-request data and setup for each
table are in [`bench/results/`](bench/results/).

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

**Next**: health checks, outlier ejection and circuit breaking (Phase 4), then
Kubernetes (Phase 5).
Until then a dead replica costs one failed connection attempt per second with
the load-aware routers, and one per request routed to it first with
`round_robin` and `random`.

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

| Gateway flag | Default | |
|---|---|---|
| `-listen` | `:9000` | gRPC address |
| `-backends` | `http://localhost:8000` | Comma-separated TinyServe base URLs |
| `-router` | `least_loaded` | `round_robin`, `random`, `least_loaded`, `consistent_hash`, `prefix_aware` |
| `-prefix-block-bytes` | `64` | `prefix_aware`: bytes per prefix block |
| `-prefix-load-factor`, `-prefix-slack` | `1.25`, `4` | `prefix_aware`: the load bound above |
| `-prefix-index-blocks` | `65536` | `prefix_aware`: prefix blocks remembered |
| `-scrape-interval` | `250ms` | How often to read each replica's `/metrics` |
| `-max-attempts` | `0` (all) | Replicas one request may try before its first token |
| `-drain-timeout` | `30s` | How long SIGTERM waits for open streams |
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
internal/backend/     TinyServe SSE client, /metrics scraping, load and overload state
internal/router/      round-robin, random, least-loaded (P2C), consistent hash, prefix-aware
internal/server/      gRPC service: routing, failover, error mapping
internal/fakebackend/ fake replica, shared by tests and cmd/fakebackend
```
