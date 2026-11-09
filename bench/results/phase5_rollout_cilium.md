# Phase 5: rolling updates under load (kind, 3 nodes)

rate=100 req/s for 75s, tenants workload, 32 tokens, client deadline 15s; 2 TinyServe + 2 gateway pods.
t=10s: rollout restart of TinyServe; once done, rollout restart of the gateways.

```
t=10s: rolling TinyServe
t=14s: TinyServe rolled; rolling the gateways
t=17s: gateways rolled
== no drain
requests 7412  ok 7410  errors map[Unavailable:2]  retried 0  elapsed 75.0s
throughput 98.8 req/s  3161 tokens/s
ttft     p50     4.4ms  p95    11.3ms  p99    23.7ms
latency  p50    10.4ms  p95    28.3ms  p99    52.3ms
prefix   84.5% of prompt tokens reused from the engines' prefix caches
served   http://10.244.1.10:8000 9%  http://10.244.1.184:8000 41%  http://10.244.2.178:8000 41%  http://10.244.2.210:8000 9%
 sent at     ok failed retried    p99 lat
      0s    507      0       0       37ms
      5s    505      0       0       35ms
     10s    472      0       0       37ms
     15s    479      2       0       33ms
     20s    504      0       0       31ms
     25s    477      0       0       31ms
     30s    518      0       0       51ms
     35s    489      0       0       30ms
     40s    504      0       0      111ms
     45s    474      0       0      100ms
     50s    464      0       0       42ms
     55s    490      0       0       71ms
     60s    504      0       0       62ms
     65s    511      0       0       54ms
     70s    512      0       0       35ms

t=10s: rolling TinyServe
t=14s: TinyServe rolled; rolling the gateways
t=17s: gateways rolled
== drain, client DNS interval 30s (gRPC default)
requests 7646  ok 6884  errors map[Unavailable:762]  retried 0  elapsed 75.0s
throughput 91.8 req/s  2937 tokens/s
ttft     p50     4.5ms  p95    14.4ms  p99    29.8ms
latency  p50    10.7ms  p95    35.5ms  p99    63.2ms
prefix   84.4% of prompt tokens reused from the engines' prefix caches
served   http://10.244.1.132:8000 10%  http://10.244.1.53:8000 47%  http://10.244.2.236:8000 33%  http://10.244.2.30:8000 10%
 sent at     ok failed retried    p99 lat
      0s    474      0       0       46ms
      5s    557      0       0       46ms
     10s    524      0       0       93ms
     15s    517      0       0       77ms
     20s    213    272       0       41ms
     25s      0    488       0        0ms
     30s    490      2       0       65ms
     35s    522      0       0       33ms
     40s    487      0       0       37ms
     45s    508      0       0       60ms
     50s    535      0       0       39ms
     55s    488      0       0       41ms
     60s    561      0       0       99ms
     65s    532      0       0       64ms
     70s    476      0       0       38ms

t=10s: rolling TinyServe
t=14s: TinyServe rolled; rolling the gateways
t=17s: gateways rolled
== drain, client DNS interval 1s
requests 7555  ok 7555  errors map[]  retried 0  elapsed 75.0s
throughput 100.8 req/s  3224 tokens/s
ttft     p50     4.5ms  p95    10.7ms  p99    17.9ms
latency  p50    10.4ms  p95    30.5ms  p99    47.9ms
prefix   85.0% of prompt tokens reused from the engines' prefix caches
served   http://10.244.1.201:8000 55%  http://10.244.1.53:8000 13%  http://10.244.2.236:8000 6%  http://10.244.2.78:8000 27%
 sent at     ok failed retried    p99 lat
      0s    529      0       0       26ms
      5s    474      0       0       24ms
     10s    516      0       0       54ms
     15s    478      0       0       49ms
     20s    491      0       0       38ms
     25s    483      0       0       39ms
     30s    520      0       0       50ms
     35s    493      0       0       29ms
     40s    545      0       0      132ms
     45s    463      0       0       47ms
     50s    501      0       0       55ms
     55s    537      0       0       45ms
     60s    525      0       0       47ms
     65s    471      0       0       40ms
     70s    529      0       0       39ms

```
