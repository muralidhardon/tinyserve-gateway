#!/usr/bin/env bash
# Phase 7: Cilium network policy around the gateway and TinyServe (kind).
#
#   1. Policy checks: who can reach TinyServe, which requests get through, and
#      what TinyServe itself can reach, with Hubble's record of each verdict.
#   2. Cost: the same load with no policy, an L3/L4 policy, and L3/L4 plus
#      the L7 HTTP rules (which send TinyServe traffic through Envoy).
#
# Needs the cluster from CNI=cilium deploy/kind/up.sh.
#
#   bench/phase7_cilium.sh      # writes bench/results/phase7_cilium.md
set -euo pipefail
cd "$(dirname "$0")/.."

RELEASE=${RELEASE:-ts}
RATE=${RATE:-100}
DURATION=${DURATION:-60s}
OUT=bench/results
mkdir -p "$OUT"
kubectl config use-context kind-tinyserve >/dev/null

TS_SVC="$RELEASE-tinyserve"
GW_LABELS="app.kubernetes.io/name=tinyserve-gateway,app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=gateway"

install() {
  helm upgrade --install "$RELEASE" deploy/helm/tinyserve-gateway --set autoscaling.type=none "$@" --wait --timeout 5m >/dev/null
  kubectl rollout status deploy/"$RELEASE"-tinyserve --timeout 5m >/dev/null
  kubectl rollout status deploy/"$RELEASE"-gateway --timeout 5m >/dev/null
  sleep 5 # let the agents program the new policy
}

# probe NAME LABELS METHOD PATH: one curl from a throwaway pod to a TinyServe
# pod, printing the HTTP status or how the connection failed.
probe() {
  local name=$1 labels=$2 method=$3 path=$4 pod code phase
  pod=$(kubectl get pod -l "app.kubernetes.io/component=tinyserve" -o jsonpath='{.items[0].status.podIP}')
  kubectl delete pod "$name" --ignore-not-found --wait >/dev/null
  kubectl run "$name" --image curlimages/curl:8.11.1 --restart Never \
    ${labels:+--labels "$labels"} --command -- \
    curl -s -o /dev/null -w '%{http_code}' -m 3 -X "$method" -H 'Content-Type: application/json' \
    -d '{"prompt":"hi","max_tokens":1}' "http://$pod:8000$path" >/dev/null
  # Attaching to a pod this short-lived can miss its output; wait, then read the log.
  for _ in $(seq 60); do
    phase=$(kubectl get pod "$name" -o jsonpath='{.status.phase}')
    [ "$phase" = Succeeded ] || [ "$phase" = Failed ] && break
    sleep 1
  done
  code=$(kubectl logs "$name" 2>/dev/null || true)
  kubectl delete pod "$name" --wait >/dev/null
  case $code in
    000|"") echo "no connection (dropped)" ;;
    *) echo "HTTP $code" ;;
  esac
}

# load LABEL: a short run through the gateways, printing the summary.
load() {
  kubectl delete pod loadgen --ignore-not-found --wait >/dev/null
  kubectl run loadgen --image tinyserve-gateway:dev --image-pull-policy IfNotPresent --restart Never \
    --command -- /usr/local/bin/loadgen -addr "dns:///$RELEASE-gateway-headless:9000" -balance round_robin \
    -label "$1" -workload tenants -rate "$RATE" -duration "$DURATION" -max-tokens 32 -timeout 15s >/dev/null
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen --timeout 5m >/dev/null
  kubectl logs loadgen | grep -E '^(==|requests|ttft|latency)'
  kubectl delete pod loadgen --wait >/dev/null
  echo
}

# Each agent records only its own node's flows, so ask every one.
hubble() {
  local agent
  for agent in $(kubectl -n kube-system get pod -l k8s-app=cilium -o name); do
    kubectl -n kube-system exec "$agent" -c cilium-agent -- hubble observe --since 5m "$@" 2>/dev/null || true
  done
}

{
  echo "# Phase 7: Cilium network policy (kind, 3 nodes, Cilium with kube-proxy replacement)"
  echo
  echo "## Policy checks"
  echo
  install --set networkPolicy.enabled=true --set networkPolicy.l7=true
  echo '```'
  echo "unlabelled pod      GET  /health           -> $(probe intruder '' GET /health)"
  echo "pod with gateway's  GET  /health           -> $(probe as-gateway-1 "$GW_LABELS" GET /health)"
  echo "  labels            POST /generate/stream  -> $(probe as-gateway-2 "$GW_LABELS" POST /generate/stream)"
  echo "                    POST /generate         -> $(probe as-gateway-3 "$GW_LABELS" POST /generate)"
  echo "                    GET  /docs             -> $(probe as-gateway-4 "$GW_LABELS" GET /docs)"
  tspod=$(kubectl get pod -l app.kubernetes.io/component=tinyserve -o jsonpath='{.items[0].metadata.name}')
  egress=$(kubectl exec "$tspod" -- python -c "
import socket
s = socket.socket(); s.settimeout(3)
try:
    s.connect(('10.96.0.1', 443)); print('connected')
except Exception as e:
    print('blocked (' + type(e).__name__ + ')')
" 2>&1 | tail -1)
  echo "TinyServe pod -> API server :443         -> $egress"
  echo '```'
  echo
  echo "Hubble, the drops and the L7 denials above:"
  echo
  echo '```'
  hubble --verdict DROPPED --to-label app.kubernetes.io/component=tinyserve -o compact | tail -4
  hubble --protocol http --http-status 403 -o compact | tail -2
  hubble --verdict DROPPED --from-label app.kubernetes.io/component=tinyserve -o compact | tail -2
  echo '```'
  echo
  echo "## Cost"
  echo
  echo "rate=$RATE req/s for $DURATION, tenants workload, 32 tokens; 2 TinyServe + 2 gateway pods."
  echo
  echo '```'
  install --set networkPolicy.enabled=false
  load "no policy"
  install --set networkPolicy.enabled=true --set networkPolicy.l7=false
  load "L3/L4 policy"
  install --set networkPolicy.enabled=true --set networkPolicy.l7=true
  load "L3/L4 + L7 HTTP policy (Envoy)"
  echo '```'
} | tee "$OUT/phase7_cilium.md"
