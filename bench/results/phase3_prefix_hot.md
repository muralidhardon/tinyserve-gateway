# Phase 3: prefix-aware routing, real TinyServe

rate=600 req/s, duration=20s, max_tokens=16, 24 tenants with Zipf(2.0) popularity;
3 replicas, TINYSERVE_THREADS=2, prefix cache 48 blocks each (working set ~120 blocks).

```
== least_loaded
requests 11843  ok 11843  errors map[]  retried 0  elapsed 20.0s
throughput 592.0 req/s  9472 tokens/s
ttft     p50     3.0ms  p95    13.2ms  p99    52.8ms
latency  p50     4.9ms  p95    15.9ms  p99    79.2ms
prefix   78.9% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 35%  http://localhost:8402 32%  http://localhost:8403 33%

== consistent_hash
requests 12053  ok 12053  errors map[]  retried 0  elapsed 20.0s
throughput 602.4 req/s  9639 tokens/s
ttft     p50     4.0ms  p95    13.9ms  p99    55.0ms
latency  p50     6.2ms  p95    20.9ms  p99    69.3ms
prefix   84.4% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 10%  http://localhost:8402 22%  http://localhost:8403 68%

== prefix_aware
requests 11709  ok 11709  errors map[]  retried 0  elapsed 20.0s
throughput 585.3 req/s  9365 tokens/s
ttft     p50     3.6ms  p95     9.9ms  p99    17.0ms
latency  p50     5.8ms  p95    13.8ms  p99    21.9ms
prefix   83.6% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8401 34%  http://localhost:8402 31%  http://localhost:8403 35%

```
