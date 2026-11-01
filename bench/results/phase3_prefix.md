# Phase 3: prefix-aware routing, real TinyServe

rate=300 req/s, duration=20s, max_tokens=16, 24 tenants with Zipf(1.1) popularity;
3 replicas, TINYSERVE_THREADS=2, prefix cache 48 blocks each (working set ~120 blocks).

```
== no prefix cache
requests 5938  ok 5938  errors map[]  retried 0  elapsed 20.0s
throughput 296.8 req/s  4749 tokens/s
ttft     p50    10.4ms  p95    19.5ms  p99    31.5ms
latency  p50    12.1ms  p95    22.0ms  p99    35.0ms
prefix   0.0% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 36%  http://localhost:8402 32%  http://localhost:8403 32%

== round_robin
requests 5872  ok 5872  errors map[]  retried 0  elapsed 20.0s
throughput 293.5 req/s  4697 tokens/s
ttft     p50     2.8ms  p95     9.9ms  p99    12.8ms
latency  p50     4.2ms  p95    11.5ms  p99    14.7ms
prefix   57.4% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 33%  http://localhost:8402 33%  http://localhost:8403 33%

== least_loaded
requests 5942  ok 5942  errors map[]  retried 0  elapsed 20.0s
throughput 296.9 req/s  4751 tokens/s
ttft     p50     3.0ms  p95    11.2ms  p99    14.9ms
latency  p50     4.7ms  p95    12.8ms  p99    16.9ms
prefix   57.5% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 34%  http://localhost:8402 32%  http://localhost:8403 33%

== consistent_hash
requests 5942  ok 5942  errors map[]  retried 0  elapsed 20.0s
throughput 297.1 req/s  4753 tokens/s
ttft     p50     2.5ms  p95     5.6ms  p99    10.3ms
latency  p50     4.0ms  p95     7.9ms  p99    12.8ms
prefix   82.5% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 22%  http://localhost:8402 32%  http://localhost:8403 46%

== prefix_aware
requests 5884  ok 5884  errors map[]  retried 0  elapsed 20.0s
throughput 294.2 req/s  4707 tokens/s
ttft     p50     2.4ms  p95     5.7ms  p99    12.1ms
latency  p50     4.0ms  p95     7.9ms  p99    14.7ms
prefix   82.6% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 35%  http://localhost:8402 43%  http://localhost:8403 22%

```
