# Phase 4: chaos under load

rate=150 req/s, duration=32s, max_tokens=64, client deadline 10s; 4 fake replicas
(1 ms/token, 8 slots, 64-deep queue). t=8s SIGSTOP :8502, t=16s SIGKILL :8503, t=24s SIGCONT :8502.

```
== round_robin, no protection
requests 4925  ok 4800  errors map[DeadlineExceeded:122 Unavailable:3]  retried 1278  elapsed 32.1s
throughput 149.7 req/s  9579 tokens/s
ttft     p50    14.6ms  p95  2015.1ms  p99  2023.9ms
latency  p50    73.1ms  p95  2073.4ms  p99  2081.3ms
prefix   0.0% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8501 37%  http://localhost:8502 19%  http://localhost:8503 14%  http://localhost:8504 30%
 sent at     ok failed retried    p99 lat
      0s    304      0       0       76ms
      2s    315      0       0       76ms
      4s    326      0       0       76ms
      6s    308      6       0       79ms
      8s    229     76       0       83ms
     10s    243     40      31     2074ms
     12s    330      0      81     2077ms
     14s    320      3      89     2080ms
     16s    287      0     176     2084ms
     18s    310      0     198     2086ms
     20s    291      0     171     2104ms
     22s    295      0     119     2445ms
     24s    294      0      97      617ms
     26s    313      0     105       77ms
     28s    340      0     113      130ms
     30s    295      0      98       83ms
gateway events:
  none

== round_robin, phase 4
requests 4862  ok 4854  errors map[DeadlineExceeded:3 Unavailable:5]  retried 77  elapsed 32.1s
throughput 151.4 req/s  9687 tokens/s
ttft     p50    14.3ms  p95    19.0ms  p99  5014.6ms
latency  p50    72.5ms  p95    79.3ms  p99  5072.3ms
prefix   0.0% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8501 36%  http://localhost:8502 15%  http://localhost:8503 14%  http://localhost:8504 36%
 sent at     ok failed retried    p99 lat
      0s    324      0       0       83ms
      2s    308      0       0      190ms
      4s    297      0       0       79ms
      6s    304      3       6     5072ms
      8s    282      0      70     5074ms
     10s    330      0       0       75ms
     12s    272      0       0       77ms
     14s    304      5       1       75ms
     16s    286      0       0      105ms
     18s    317      0       0      102ms
     20s    308      0       0       91ms
     22s    316      0       0       95ms
     24s    311      0       0       75ms
     26s    286      0       0       76ms
     28s    323      0       0       75ms
     30s    286      0       0       76ms
gateway events:
   1 "replica ejected","http://localhost:8502","health check failed"
   1 "replica ejected","http://localhost:8503","consecutive request failures"
   1 "replica re-admitted","http://localhost:8502"

== least_loaded, no protection
requests 4860  ok 4847  errors map[DeadlineExceeded:9 Unavailable:4]  retried 16  elapsed 32.1s
throughput 151.2 req/s  9676 tokens/s
ttft     p50    14.3ms  p95    16.5ms  p99    36.8ms
latency  p50    72.6ms  p95    76.9ms  p99    94.7ms
prefix   0.0% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8501 36%  http://localhost:8502 15%  http://localhost:8503 14%  http://localhost:8504 35%
 sent at     ok failed retried    p99 lat
      0s    319      0       0       80ms
      2s    271      0       0       81ms
      4s    300      0       0       77ms
      6s    311      4       0       83ms
      8s    298      5       0       76ms
     10s    288      0       0       76ms
     12s    304      0       0       75ms
     14s    290      4       0       76ms
     16s    302      0       2     6127ms
     18s    320      0       2     4786ms
     20s    296      0       2       96ms
     22s    301      0       2      124ms
     24s    332      0       2       89ms
     26s    297      0       2       76ms
     28s    301      0       2       76ms
     30s    317      0       2       77ms
gateway events:
  none

== least_loaded, phase 4
requests 4765  ok 4763  errors map[DeadlineExceeded:1 Unavailable:1]  retried 12  elapsed 32.0s
throughput 148.7 req/s  9514 tokens/s
ttft     p50    14.3ms  p95    15.5ms  p99    30.1ms
latency  p50    72.6ms  p95    75.7ms  p99    88.3ms
prefix   0.0% of prompt tokens reused from the engines' prefix caches
served   http://localhost:8501 35%  http://localhost:8502 14%  http://localhost:8503 15%  http://localhost:8504 35%
 sent at     ok failed retried    p99 lat
      0s    288      0       0       77ms
      2s    281      0       0       77ms
      4s    312      0       0       76ms
      6s    307      1       0       77ms
      8s    281      0       8     5073ms
     10s    320      0       0       75ms
     12s    312      0       0       75ms
     14s    278      1       1       76ms
     16s    297      0       3      104ms
     18s    316      0       0       95ms
     20s    287      0       0       92ms
     22s    281      0       0       91ms
     24s    301      0       0       76ms
     26s    323      0       0       75ms
     28s    282      0       0       77ms
     30s    297      0       0       77ms
gateway events:
   1 "replica ejected","http://localhost:8502","health check failed"
   1 "replica ejected","http://localhost:8503","consecutive request failures"
   1 "replica re-admitted","http://localhost:8502"

```
