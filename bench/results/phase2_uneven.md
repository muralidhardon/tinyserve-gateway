# Phase 2: routing on uneven replicas

rate=150 req/s, duration=20s, max_tokens=64; replicas :8201 and :8202 at 1 ms/token, :8203 at 3 ms/token; 8 slots, 64-deep queue each.

```
== round_robin
requests 3006  ok 3006  errors map[]  retried 215  elapsed 21.7s
throughput 138.2 req/s  8846 tokens/s
ttft     p50    14.6ms  p95  1632.3ms  p99  1643.8ms
latency  p50    73.4ms  p95  1804.8ms  p99  1816.9ms
served   http://localhost:8201 33%  http://localhost:8202 38%  http://localhost:8203 28%

== random
requests 2994  ok 2994  errors map[]  retried 134  elapsed 21.8s
throughput 137.2 req/s  8782 tokens/s
ttft     p50    14.6ms  p95  1622.2ms  p99  1643.0ms
latency  p50    73.4ms  p95  1794.7ms  p99  1816.1ms
served   http://localhost:8201 36%  http://localhost:8202 35%  http://localhost:8203 29%

== least_loaded
requests 2979  ok 2979  errors map[]  retried 0  elapsed 20.2s
throughput 147.6 req/s  9448 tokens/s
ttft     p50    14.3ms  p95    30.8ms  p99    32.3ms
latency  p50    72.6ms  p95   202.8ms  p99   205.6ms
served   http://localhost:8201 42%  http://localhost:8202 40%  http://localhost:8203 18%

== consistent_hash
requests 2969  ok 2969  errors map[]  retried 5  elapsed 21.2s
throughput 140.1 req/s  8966 tokens/s
ttft     p50    14.7ms  p95  1398.6ms  p99  1577.1ms
latency  p50    73.7ms  p95  1572.2ms  p99  1749.0ms
served   http://localhost:8201 34%  http://localhost:8202 38%  http://localhost:8203 28%

```
