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

Three replicas, one of them slow; Poisson arrivals; 64 tokens per request.
Client-side numbers from `cmd/loadgen`.

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
round-robin here: it pins keys, not load, which is what Phase 3's bounded-load
prefix routing addresses.

The real-engine slow replica is an extreme case (an uncached replica), and
everything ran on one laptop. Raw per-request data and setup:
[`bench/results/`](bench/results/). Reproduce the fake run with
`bench/phase2_uneven.sh`.

## Status

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

**Next**: a prefix cache in the engine with bounded-load prefix-aware routing
(Phase 3), then health checks, outlier ejection and circuit breaking (Phase 4).
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
| `-router` | `least_loaded` | `round_robin`, `random`, `least_loaded`, `consistent_hash` |
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
internal/router/      round-robin, random, least-loaded (P2C), consistent hash
internal/server/      gRPC service: routing, failover, error mapping
internal/fakebackend/ fake replica, shared by tests and cmd/fakebackend
```
