# Phase 7: Cilium network policy (kind, 3 nodes, Cilium with kube-proxy replacement)

## Policy checks

```
unlabelled pod      GET  /health           -> no connection (dropped)
pod with gateway's  GET  /health           -> HTTP 200
  labels            POST /generate/stream  -> HTTP 200
                    POST /generate         -> HTTP 403
                    GET  /docs             -> HTTP 403
TinyServe pod -> API server :443         -> blocked (TimeoutError)
```

Hubble, the drops and the L7 denials above:

```
Nov  9 00:01:33.649: default/intruder:50804 (ID:45247) <> default/ts-tinyserve-6d8b8cffc8-ccdmk:8000 (ID:50939) policy-verdict:none INGRESS DENIED (TCP Flags: SYN)
Nov  9 00:01:33.649: default/intruder:50804 (ID:45247) <> default/ts-tinyserve-6d8b8cffc8-ccdmk:8000 (ID:50939) Policy denied DROPPED (TCP Flags: SYN)
Nov  9 00:01:45.321: default/as-gateway-3:52482 (ID:29632) -> default/ts-tinyserve-6d8b8cffc8-ccdmk:8000 (ID:50939) http-request DROPPED (HTTP/1.1 POST http://10.244.1.9:8000/generate)
Nov  9 00:01:47.785: default/as-gateway-4:54064 (ID:29632) -> default/ts-tinyserve-6d8b8cffc8-ccdmk:8000 (ID:50939) http-request DROPPED (HTTP/1.1 GET http://10.244.1.9:8000/docs)
Nov  9 00:01:45.321: default/as-gateway-3:52482 (ID:29632) <- default/ts-tinyserve-6d8b8cffc8-ccdmk:8000 (ID:50939) http-response FORWARDED (HTTP/1.1 403 0ms (POST http://10.244.1.9:8000/generate))
Nov  9 00:01:47.785: default/as-gateway-4:54064 (ID:29632) <- default/ts-tinyserve-6d8b8cffc8-ccdmk:8000 (ID:50939) http-response FORWARDED (HTTP/1.1 403 0ms (GET http://10.244.1.9:8000/docs))
Nov  9 00:01:52.849: default/ts-tinyserve-6d8b8cffc8-ccdmk:36762 (ID:50939) <> 172.19.0.4:6443 (kube-apiserver) policy-verdict:all EGRESS DENIED (TCP Flags: SYN)
Nov  9 00:01:52.849: default/ts-tinyserve-6d8b8cffc8-ccdmk:36762 (ID:50939) <> 172.19.0.4:6443 (kube-apiserver) Policy denied by denylist DROPPED (TCP Flags: SYN)
```

## Cost

rate=100 req/s for 60s, tenants workload, 32 tokens; 2 TinyServe + 2 gateway pods.

```
== no policy
requests 6150  ok 6150  errors map[]  retried 0  elapsed 60.0s
ttft     p50     4.1ms  p95    10.7ms  p99    26.1ms
latency  p50     9.9ms  p95    28.9ms  p99    59.0ms

== L3/L4 policy
requests 5937  ok 5937  errors map[]  retried 0  elapsed 60.0s
ttft     p50     4.1ms  p95    10.6ms  p99    25.6ms
latency  p50    10.1ms  p95    30.6ms  p99    59.9ms

== L3/L4 + L7 HTTP policy (Envoy)
requests 6013  ok 6013  errors map[]  retried 0  elapsed 60.0s
ttft     p50     4.3ms  p95    10.0ms  p99    17.1ms
latency  p50    10.2ms  p95    27.4ms  p99    48.0ms

```
