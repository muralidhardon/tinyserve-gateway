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

## Status

**Phase 1 (done): streaming gateway**

- `Inference.Generate` streams the completion over gRPC as TinyServe produces it
  (`api/proto/inference/v1/inference.proto`).
- Round-robin routing across replicas.
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

**Next**: load-aware routing from replicas' `/metrics`, then a prefix cache in
the engine with prefix-aware routing, then health checks, outlier ejection and
circuit breaking. Today a dead replica is retried on every request that is routed
to it first, until health checking ejects it in Phase 4.

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
internal/backend/     TinyServe SSE client, error classification
internal/router/      routing strategies
internal/server/      gRPC service: routing, failover, error mapping
internal/fakebackend/ fake replica, shared by tests and cmd/fakebackend
```
