# Phase 2: routing on uneven real TinyServe replicas

Three real TinyServe replicas (TINYSERVE_THREADS=2): :8301 and :8302 normal, :8303 with TINYSERVE_USE_CACHE=0 (NanoForge's uncached forward pass, ~19x slower per token). Poisson arrivals at 100 req/s for 15 s, 64 tokens each, 30 s client deadline. All on one Apple M3 Pro, so replicas share CPU with each other and the load generator.

```
== round_robin (real TinyServe)
requests 1524  ok 1499  errors map[DeadlineExceeded:25]  retried 176  elapsed 43.9s
throughput 34.2 req/s  2187 tokens/s
ttft     p50     2.5ms  p95 24096.6ms  p99 28228.7ms
latency  p50     9.2ms  p95 25680.3ms  p99 29740.1ms
served   http://localhost:8301 33%  http://localhost:8302 43%  http://localhost:8303 24%

== least_loaded (real TinyServe)
requests 1519  ok 1519  errors map[]  retried 0  elapsed 15.1s
throughput 100.5 req/s  6433 tokens/s
ttft     p50     2.2ms  p95    10.0ms  p99    53.5ms
latency  p50     8.8ms  p95   195.6ms  p99   313.9ms
served   http://localhost:8301 47%  http://localhost:8302 47%  http://localhost:8303 6%

```
