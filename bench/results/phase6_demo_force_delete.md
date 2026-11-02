# Phase 6: traffic with one TinyServe pod force-deleted (kubectl delete --force --grace-period 0)

rate=120 req/s for 240s, tenants workload, 48 tokens; 2 TinyServe + 2 gateway pods; one TinyServe pod force-deleted at t=60s.

```
== kill one TinyServe pod at t=60s
requests 28620  ok 28620  errors map[]  retried 0  elapsed 240.0s
throughput 119.2 req/s  5723 tokens/s
ttft     p50     8.3ms  p95    72.5ms  p99   253.9ms
latency  p50    46.4ms  p95   237.9ms  p99   441.2ms
prefix   84.8% of prompt tokens reused from the engines' prefix caches
served   http://10.244.1.12:8000 37%  http://10.244.1.9:8000 12%  http://10.244.2.8:8000 51%
 sent at     ok failed retried    p99 lat
      0s   1237      0       0      435ms
     10s   1197      0       0      419ms
     20s   1203      0       0      314ms
     30s   1141      0       0      181ms
     40s   1199      0       0      233ms
     50s   1127      0       0      307ms
     60s   1205      0       0      591ms
     70s   1192      0       0      284ms
     80s   1250      0       0      253ms
     90s   1186      0       0      177ms
    100s   1231      0       0      282ms
    110s   1159      0       0      125ms
    120s   1186      0       0      161ms
    130s   1235      0       0      161ms
    140s   1283      0       0      306ms
    150s   1193      0       0      465ms
    160s   1169      0       0      171ms
    170s   1177      0       0      325ms
    180s   1156      0       0      263ms
    190s   1147      0       0      283ms
    200s   1165      0       0      254ms
    210s   1171      0       0      613ms
    220s   1182      0       0      210ms
    230s   1229      0       0      292ms
```
