# Design

This document explains the gateway's design decisions, the alternatives I
rejected, and what measuring it taught me. The numbers referenced are in the
[README](../README.md#results) and [`bench/results/`](../bench/results/).

## The request path

1. **Admit.** A `Generate` call is admitted, or shed if the gateway is too busy
   for its priority.
2. **Route.** The router picks a replica from the pool, skipping ejected ones.
3. **Stream.** The gateway POSTs to that replica's `/generate/stream` and
   relays each server-sent event as a gRPC message.
4. **Fail over, or not.** If the attempt fails before any text reaches the
   client, the request moves to another replica, within the retry budget.
   After the first text, a failure ends the stream.
5. **Finish.** The last message carries usage: tokens, which replica served
   it, how many attempts it took, and the replica's own queue time, time to
   first token and prefix reuse.

Cancellation and deadlines propagate the whole way. If the client goes away,
the HTTP request is cancelled, and TinyServe drops the sequence from its batch
and frees its KV cache.

## Failover only before the first token

Once a client has seen part of a completion, the gateway can't hand the
request to another replica. That replica would generate a different
continuation, or the same tokens again. Resuming would need the engine to
continue from given tokens with the same random state. TinyServe can't do
that, so a mid-stream failure ends the stream with `UNAVAILABLE`.

**What this costs:** these are the only failures left in the crash test. 3 of
28,951 requests were already generating on the replica that died.
Everything that hadn't produced text yet failed over and succeeded.

## Load: two signals, take the larger

The router needs each replica's load. It has two sources, each wrong in its own
way:

- **Scraped**, from the replica's `/metrics` (queue depth plus active
  sequences) every 250 ms. This sees every client of the replica, including
  other gateways, but it is up to 250 ms old.
- **Local**: this gateway's own open streams to the replica. This is
  current, but sees only this gateway.

The router uses the larger of the two. Underestimating load is the dangerous
mistake, because it sends more traffic to a replica that is already full.

That rule has one trap. A dead replica has no open streams and frozen scrape
figures, so it looks idle and attracts traffic. A refused connection therefore
marks the replica avoided for 1 s. A 503 marks it avoided for its
`Retry-After`.

**Why power of two choices** instead of the least-loaded replica overall:
between scrapes, every gateway sees the same stale numbers. If each picked the
global minimum, they would all pile onto the same replica. Comparing two random
replicas keeps them apart, and still balances far better than random. On a
replica 19x slower than the rest, least-loaded sent it 6% of traffic, and p95
time to first token (TTFT) went from 24 s to 10 ms.

## Prefix-aware routing

TinyServe caches the K/V rows of prompt prefixes in 16-token blocks. A request
whose prefix is cached skips most of its prefill. The router's job is to send
it to a replica that has the prefix.

- **No tokenizer in the gateway.**
  - The gateway hashes 64-byte blocks of the prompt. At about four bytes per
    token, that is roughly one engine block.
  - Each block's key hashes the whole prefix up to that block, not just the
    block's own bytes. Equal keys therefore mean equal prefixes.
- **Pick the longest match.** The index maps each key to the replicas recently
  sent that prefix, and the router walks the prompt's keys, narrowing to the
  replicas that still match.
  - Ties go to the replica that most recently got the prefix.
  - Breaking ties on current load would make a prefix hop between replicas.
    It would end up cached everywhere, evicting other prefixes.
- **Bounded load.** A matching replica may hold at most
  `ceil(1.25 × (total + 1) / n) + 4` sequences
  ([consistent hashing with bounded loads](https://arxiv.org/abs/1608.01350),
  plus slack).
  - The slack is there because continuous batching makes a few extra
    sequences nearly free, while a cache miss costs a full prefill.
  - Over the bound, the request spills to the least-loaded replica, which
    then caches the prefix too.
  - With one hot tenant, this is what kept p99 TTFT at 17 ms, against 55 ms
    for consistent hashing. Consistent hashing put 68% of all traffic on one
    replica.
- **The index is a prediction, not a mirror.**
  - Engines evict blocks the index still lists. The index is LRU-bounded
    (65,536 blocks by default), and refreshes a prefix's first block after its
    later ones, so the start of a prefix is evicted last.
  - The real hit rate comes from the engines: each response reports the
    prompt tokens it reused. Prefix reuse reported to the gateway (83.4%) and
    the engines' own cache counters (83.6%) agree on the dashboard.
- One routing decision takes about 1.3 µs for a 600-byte prompt.

## Fault tolerance

Each mechanism covers a failure the others miss.

| Failure | What catches it |
|---|---|
| Replica down, refusing connections | Failover; the replica is avoided for 1 s, and ejected after 5 failures in a row |
| Replica frozen: accepts connections, never answers | Active health checks (2 failures eject it); a 5 s first-token timeout for requests already sent |
| Replica returning errors | Passive ejection after 5 consecutive failed requests |
| Replica overloaded | A 503 is backpressure, not a fault: the replica is avoided for its `Retry-After` and not ejected |
| Whole fleet failing | A retry budget, a cap on passive ejection, and panic mode |
| Gateway overloaded | Admission control, shedding low priority first |

- **What counts as a failure.** A 4xx is the client's fault: another replica
  would refuse it too. A 503 means the replica is protecting its own queue.
  Ejecting a replica for either would shrink the fleet exactly when it is
  busiest.
- **The cap.** Failed requests may eject at most half the pool. A bad prompt
  or a client bug fails on every replica, and must not drain the fleet.
  Health-check ejections aren't capped, since a failed health check
  implicates the replica, not the request.
- **Backoff.** An ejection lasts 2 s and doubles on each repeat, up to 60 s. A
  replica comes back only after a passing health check.
- **Panic mode.** If every replica is ejected, the gateway uses them all
  anyway. A replica that might have recovered beats failing every request.
- **The retry budget.** Without one, n failing replicas turn every request
  into n attempts, and the extra load lands on a fleet that is already
  failing. Retries are capped at 10% of requests plus 10 per second, over a
  10 s window. `TestRetryBudgetBoundsRetries` checks the cap with every
  replica failing. (In the chaos test, retries fell from 1,278 to 77, but
  that is mostly ejection: once the frozen replica is out of rotation, there
  is little left to retry.)
- **The first-token timeout.** Only the first token is timed, so a long
  generation isn't cut off.

## Kubernetes

- **EndpointSlices, not a Service IP.**
  - A ClusterIP Service would hand each connection to kube-proxy, which
    picks a pod at random. That would destroy cache affinity and hide
    per-replica load.
  - Instead, the gateway watches the TinyServe Service's EndpointSlices with
    a client-go informer, and addresses each pod directly.
  - It routes only to endpoints marked ready. A terminating pod is marked
    not-ready at once, so the gateway stops sending it new streams while its
    open ones finish.
  - In the crash test, Kubernetes marked the crashed pod not-ready 0.8 s after
    the crash. That was before the gateway's own outlier detection needed to
    act.
- **A live pool.** Replicas come and go without disturbing the rest:
  - one that stays across an update keeps its load figures, health history
    and open streams;
  - one that is removed finishes its open streams.
- **Drain, step by step.**
  1. The `preStop` sleep keeps a terminating pod serving for 5 s, while
     clients and Services stop picking it.
  2. On SIGTERM, the gRPC health service reports NOT_SERVING.
  3. `GracefulStop` lets open streams finish, bounded by `-drain-timeout`.
  4. The termination grace period is sized to cover all of it.
- **Balancing across gateways.** A ClusterIP Service balances connections, not
  requests. A gRPC client keeps one HTTP/2 connection, so it would stick to one
  gateway pod forever. The chart therefore also offers a headless Service:
  - clients resolve every gateway pod and balance requests themselves;
  - they need gRPC's `round_robin` policy, and a short DNS re-resolution
    interval (see below).
- **The gateway's readiness tracks discovery.** A gateway pod reports
  NOT_SERVING until it has found a replica, so a rollout waits for discovery.
- **Autoscaling on the engine's own numbers.**
  - CPU is a poor signal: a replica with a full batch and a long queue can
    show the same CPU as one just keeping up.
  - The KEDA option scales on TinyServe's queue depth plus active sequences.
    It is not verified (see the README's Limits).

## Observability

- **One span per attempt, carrying the engine's timings.**
  - Each `attempt` span records the replica, the routing reason and the
    predicted prefix match.
  - It also records the replica's own queue wait, time to first token,
    latency and prefix reuse, from the usage it reports.
  - So a trace shows where an attempt's time went without the Python server
    emitting spans. The gateway does send `traceparent`, for when it does.
- **Only generation requests are traced.** Health checks and `/metrics` scrapes
  run several times a second per replica and would bury the spans that matter.
- **Every counter starts at 0** (see below for why).
- **Tracing's cost doesn't depend on the sample rate.** The instrumentation
  runs on every request, about 30 µs of CPU. Recording and exporting every span
  adds only about 8 µs. So the real choice is whether to install tracing at
  all, and not sampling much buys back little.

## What measuring taught me

Most of these were found by running something and seeing a number that didn't
make sense.

1. **A rate graph can't see a counter's first increment.**
   - A Prometheus counter with labels has no series until its first
     increment, and `rate()` and `increase()` can't see the jump from nothing
     to that first value.
   - In the first crash run, the gateway logged 161 failed attempts and 2
     ejections, yet every rate graph showed zero: each failure series was
     created by the crash itself.
   - Every expected series now starts at 0, and a test checks it.
2. **gRPC clients re-resolve DNS at most every 30 s.**
   - A rolling update of the gateways failed 10% of requests for about 8 s.
   - The first old pod's exit triggered a re-resolution, which returned the
     second old pod. When that pod left seconds later, the client wasn't
     allowed to re-resolve again.
   - With `dns.SetMinResolutionInterval(1s)`, the same rollout failed 0 of
     7,606 requests.
3. **Deleting a pod is not a crash.**
   - `kubectl delete --force --grace-period 0` still removes the pod from its
     EndpointSlice first, and sends the container SIGTERM. So its streams
     finish, and that run failed 0 of 28,620 requests.
   - A real crash needs a SIGKILL from the node.
4. **My own build starved the engines.**
   - I built a Docker image during one crash run. The build shares the Docker
     VM with the cluster nodes, and took the CPU the engines needed.
   - Both replicas hit the 5 s first-token timeout and were ejected in turn.
     It looked like a cascading failure, and the traces showed attempts cut
     off at exactly 5,000 ms.
   - I reran on an idle machine and report only that run.
5. **Least-loaded routing hides a frozen replica.** Without health checks,
   power-of-two choices still routed around a frozen replica, because its
   in-flight count kept climbing (13 failures, against round-robin's 125). The
   comparison needed round-robin to show what the fault-tolerance machinery
   actually adds.
6. **Go's HTTP server notices a client hang-up only after the handler reads
   the request body.** The fake "stalled replica" ignored the body, so its
   handler never saw the gateway give up, and the test hung. The fake now
   drains the body before stalling.
7. **Smaller things:**
   - A Prometheus scrape timeout longer than the scrape interval is a config
     error, and Prometheus won't start.
   - `runAsNonRoot` can't be enforced for an image whose `USER` is a name
     rather than a number.
   - The Prometheus chart repository's index often timed out; the same chart
     pulled fine from its OCI registry.

## What I'd do next

- **Resume mid-stream failures.** That needs an engine API that continues from
  given tokens with a given random state. It would remove the last failures
  in the crash test.
- **Hedge the first token.** If a replica hasn't produced text within its
  usual p95, send the same request to a second replica and keep whichever
  answers first.
- **Reconcile the prefix index with the engines.** The engines could publish
  which blocks they hold, instead of the gateway predicting it.
- **Verify autoscaling** on a machine where KEDA installs, scaling on queue
  depth against a load ramp.
- **Trace inside TinyServe**, reading `traceparent` and adding spans for
  queueing, prefill and decode.
