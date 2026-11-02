# Phase 6: telemetry overhead

30000 requests per run, closed loop of 64 clients, 16 tokens; 3 instant fake replicas; 3 rounds.

```
--- round 1
== off
throughput 27146.7 req/s  434348 tokens/s
latency  p50     2.3ms  p95     3.9ms  p99     4.7ms
gateway CPU 159 ms per 1,000 requests

== metrics
throughput 27516.5 req/s  440264 tokens/s
latency  p50     2.2ms  p95     3.8ms  p99     4.8ms
gateway CPU 159 ms per 1,000 requests

== metrics + tracing 0%
throughput 24905.1 req/s  398481 tokens/s
latency  p50     2.5ms  p95     4.4ms  p99     5.5ms
gateway CPU 191 ms per 1,000 requests

== metrics + tracing 10%
throughput 24868.2 req/s  397892 tokens/s
latency  p50     2.4ms  p95     4.2ms  p99     5.6ms
gateway CPU 190 ms per 1,000 requests

== metrics + tracing 100%
throughput 24871.5 req/s  397943 tokens/s
latency  p50     2.4ms  p95     4.5ms  p99     6.5ms
gateway CPU 196 ms per 1,000 requests

--- round 2
== off
throughput 26895.6 req/s  430330 tokens/s
latency  p50     2.3ms  p95     3.9ms  p99     4.8ms
gateway CPU 161 ms per 1,000 requests

== metrics
throughput 27427.6 req/s  438841 tokens/s
latency  p50     2.2ms  p95     3.8ms  p99     4.5ms
gateway CPU 158 ms per 1,000 requests

== metrics + tracing 0%
throughput 23772.6 req/s  380362 tokens/s
latency  p50     2.5ms  p95     4.5ms  p99     6.1ms
gateway CPU 198 ms per 1,000 requests

== metrics + tracing 10%
throughput 25582.0 req/s  409312 tokens/s
latency  p50     2.4ms  p95     4.3ms  p99     5.6ms
gateway CPU 183 ms per 1,000 requests

== metrics + tracing 100%
throughput 24301.5 req/s  388823 tokens/s
latency  p50     2.4ms  p95     4.5ms  p99     8.9ms
gateway CPU 197 ms per 1,000 requests

--- round 3
== off
throughput 27808.4 req/s  444935 tokens/s
latency  p50     2.2ms  p95     3.7ms  p99     4.6ms
gateway CPU 155 ms per 1,000 requests

== metrics
throughput 27391.7 req/s  438267 tokens/s
latency  p50     2.2ms  p95     3.9ms  p99     4.8ms
gateway CPU 153 ms per 1,000 requests

== metrics + tracing 0%
throughput 25059.3 req/s  400949 tokens/s
latency  p50     2.4ms  p95     4.3ms  p99     5.4ms
gateway CPU 187 ms per 1,000 requests

== metrics + tracing 10%
throughput 25036.4 req/s  400582 tokens/s
latency  p50     2.4ms  p95     4.3ms  p99     5.7ms
gateway CPU 186 ms per 1,000 requests

== metrics + tracing 100%
throughput 24470.6 req/s  391530 tokens/s
latency  p50     2.4ms  p95     4.4ms  p99     6.0ms
gateway CPU 202 ms per 1,000 requests

```
