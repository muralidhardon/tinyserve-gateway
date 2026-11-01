# Phase 3: prefix-aware routing, real TinyServe

rate=600 req/s, duration=20s, max_tokens=16, 24 tenants with Zipf(1.1) popularity;
3 replicas, TINYSERVE_THREADS=2, prefix cache 48 blocks each (working set ~120 blocks).

```
== no prefix cache
requests 12042  ok 10795  errors map[ResourceExhausted:1247]  retried 1356  elapsed 21.5s
throughput 502.5 req/s  8040 tokens/s
ttft     p50  1564.5ms  p95  1673.8ms  p99  1752.4ms
latency  p50  1576.3ms  p95  1685.9ms  p99  1764.9ms
prefix   0.0% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 33%  http://localhost:8402 33%  http://localhost:8403 33%

== round_robin
requests 11837  ok 11837  errors map[]  retried 0  elapsed 20.0s
throughput 591.6 req/s  9465 tokens/s
ttft     p50     4.7ms  p95    23.2ms  p99    39.1ms
latency  p50     7.5ms  p95    27.0ms  p99    43.7ms
prefix   57.3% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 33%  http://localhost:8402 33%  http://localhost:8403 33%

== least_loaded
requests 11835  ok 11835  errors map[]  retried 0  elapsed 20.0s
throughput 591.6 req/s  9466 tokens/s
ttft     p50     5.7ms  p95    26.4ms  p99    61.6ms
latency  p50     8.9ms  p95    29.9ms  p99    78.4ms
prefix   56.4% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 35%  http://localhost:8402 33%  http://localhost:8403 33%

== consistent_hash
requests 12155  ok 12155  errors map[]  retried 0  elapsed 20.0s
throughput 607.5 req/s  9721 tokens/s
ttft     p50     3.4ms  p95     9.3ms  p99    22.6ms
latency  p50     5.4ms  p95    12.8ms  p99    32.5ms
prefix   82.1% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 22%  http://localhost:8402 32%  http://localhost:8403 46%

== prefix_aware
requests 12113  ok 12113  errors map[]  retried 0  elapsed 20.0s
throughput 605.4 req/s  9687 tokens/s
ttft     p50     3.4ms  p95    10.6ms  p99    22.5ms
latency  p50     5.4ms  p95    14.1ms  p99    26.2ms
prefix   80.2% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 37%  http://localhost:8402 29%  http://localhost:8403 34%

```
