# Phase 5: rolling updates under load (kind, 3 nodes)

rate=100 req/s for 75s, tenants workload, 32 tokens, client deadline 15s; 2 TinyServe + 2 gateway pods.
t=10s: rollout restart of TinyServe; once done, rollout restart of the gateways.

```
t=10s: rolling TinyServe
t=14s: TinyServe rolled; rolling the gateways
t=17s: gateways rolled
== no drain
requests 7381  ok 7379  errors map[Unavailable:2]  retried 0  elapsed 75.0s
throughput 98.4 req/s  3149 tokens/s
ttft     p50     4.6ms  p95    15.7ms  p99    53.4ms
latency  p50    11.3ms  p95    40.6ms  p99   104.5ms
prefix   84.6% of prompt tokens reused from the engines' prefix caches
served   http://10.244.1.15:8000 7%  http://10.244.1.17:8000 48%  http://10.244.2.19:8000 11%  http://10.244.2.21:8000 34%
 sent at     ok failed retried    p99 lat
      0s    504      0       0       46ms
      5s    502      0       0       31ms
     10s    467      0       0       75ms
     15s    496      2       0      127ms
     20s    504      0       0       42ms
     25s    456      0       0      327ms
     30s    485      0       0      102ms
     35s    466      0       0       87ms
     40s    533      0       0       44ms
     45s    483      0       0       57ms
     50s    522      0       0      130ms
     55s    480      0       0       66ms
     60s    511      0       0      111ms
     65s    502      0       0       43ms
     70s    468      0       0       56ms

t=10s: rolling TinyServe
t=14s: TinyServe rolled; rolling the gateways
t=17s: gateways rolled
== drain, client DNS interval 30s (gRPC default)
requests 7437  ok 6669  errors map[Unavailable:768]  retried 0  elapsed 75.0s
throughput 88.9 req/s  2845 tokens/s
ttft     p50     4.4ms  p95    13.8ms  p99    39.7ms
latency  p50    10.8ms  p95    36.7ms  p99    84.1ms
prefix   84.4% of prompt tokens reused from the engines' prefix caches
served   http://10.244.1.19:8000 9%  http://10.244.1.21:8000 56%  http://10.244.2.24:8000 11%  http://10.244.2.26:8000 24%
 sent at     ok failed retried    p99 lat
      0s    510      0       0       69ms
      5s    478      0       0       50ms
     10s    532      0       0      207ms
     15s    492      0       0       61ms
     20s    235    269       0       42ms
     25s      0    497       0        0ms
     30s    499      2       0       37ms
     35s    510      0       0       44ms
     40s    454      0       0       36ms
     45s    504      0       0       46ms
     50s    445      0       0       38ms
     55s    500      0       0       41ms
     60s    515      0       0       41ms
     65s    533      0       0       50ms
     70s    461      0       0       45ms
     75s      1      0       0       10ms

t=10s: rolling TinyServe
t=15s: TinyServe rolled; rolling the gateways
t=18s: gateways rolled
== drain, client DNS interval 1s
requests 7606  ok 7606  errors map[]  retried 0  elapsed 75.0s
throughput 101.4 req/s  3245 tokens/s
ttft     p50     4.2ms  p95    13.8ms  p99    69.9ms
latency  p50    10.4ms  p95    35.4ms  p99   144.1ms
prefix   84.9% of prompt tokens reused from the engines' prefix caches
served   http://10.244.1.21:8000 11%  http://10.244.1.23:8000 48%  http://10.244.2.26:8000 8%  http://10.244.2.29:8000 34%
 sent at     ok failed retried    p99 lat
      0s    553      0       0       39ms
      5s    510      0       0      143ms
     10s    473      0       0      216ms
     15s    501      0       0       41ms
     20s    516      0       0       31ms
     25s    551      0       0       32ms
     30s    489      0       0      501ms
     35s    515      0       0       84ms
     40s    476      0       0       32ms
     45s    468      0       0       55ms
     50s    528      0       0       34ms
     55s    507      0       0       43ms
     60s    533      0       0       40ms
     65s    499      0       0       59ms
     70s    487      0       0       41ms

```
