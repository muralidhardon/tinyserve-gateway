# Phase 6: traffic with one TinyServe process crashing

rate=120 req/s for 240s, tenants workload, 48 tokens; 2 TinyServe + 2 gateway pods; one TinyServe process SIGKILLed at t=60s (container restarts).

```
== TinyServe process killed at t=60s
requests 28951  ok 28948  errors map[Unavailable:3]  retried 3  elapsed 240.0s
throughput 120.6 req/s  5789 tokens/s
ttft     p50     6.8ms  p95    29.0ms  p99   277.8ms
latency  p50    38.9ms  p95   148.8ms  p99   474.3ms
prefix   84.9% of prompt tokens reused from the engines' prefix caches
served   http://10.244.1.15:8000 52%  http://10.244.2.10:8000 48%
 sent at     ok failed retried    p99 lat
      0s   1221      0       0      168ms
     10s   1259      0       0      200ms
     20s   1191      0       0      110ms
     30s   1265      0       0      154ms
     40s   1209      0       0      171ms
     50s   1211      0       0      107ms
     60s   1162      3       3      407ms
     70s   1130      0       0      126ms
     80s   1219      0       0      144ms
     90s   1193      0       0      168ms
    100s   1188      0       0      154ms
    110s   1174      0       0      116ms
    120s   1215      0       0      112ms
    130s   1225      0       0      231ms
    140s   1219      0       0      121ms
    150s   1218      0       0      115ms
    160s   1223      0       0      150ms
    170s   1214      0       0      195ms
    180s   1185      0       0      188ms
    190s   1201      0       0      140ms
    200s   1146      0       0      114ms
    210s   1229      0       0      273ms
    220s   1220      0       0      217ms
    230s   1231      0       0      977ms
```
