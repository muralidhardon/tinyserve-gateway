#!/usr/bin/env bash
# Phase 5: rolling updates under load on Kubernetes (kind).
#
# A loadgen pod in the cluster sends Poisson traffic to the gateways through
# their headless Service (gRPC round_robin over every gateway pod). Partway
# through, the TinyServe Deployment is rolled -- every replica replaced, one at
# a time -- and then the gateway Deployment is rolled too.
#
# Each run reinstalls the chart with a different drain setup:
#   no drain   preStop 0 and a 1 s grace period, so a pod is killed with
#              streams open (what you get from a bare Deployment)
#   drain      the chart's defaults: preStop delay, graceful shutdown, discovery
#              drops terminating pods
# and the drain setup runs twice: with gRPC's default DNS re-resolution
# interval (30 s) on the client, and with 1 s.
#
# Needs the cluster from deploy/kind/up.sh.
#
#   bench/phase5_rollout.sh       # writes bench/results/phase5_rollout.md
#   NAME=phase5_rollout_cilium HELM_ARGS="--set networkPolicy.enabled=true" bench/phase5_rollout.sh
#                                 # the same on a CNI=cilium cluster, policy on
set -euo pipefail
cd "$(dirname "$0")/.."

RELEASE=${RELEASE:-ts}
RATE=${RATE:-100}
DURATION=${DURATION:-75s}
OUT=bench/results
NAME=${NAME:-phase5_rollout}
mkdir -p "$OUT"
kubectl config use-context kind-tinyserve >/dev/null

install() {
  helm upgrade --install "$RELEASE" deploy/helm/tinyserve-gateway --set autoscaling.type=none ${HELM_ARGS:-} "$@" --wait --timeout 5m >/dev/null
  kubectl rollout status deploy/"$RELEASE"-tinyserve --timeout 5m >/dev/null
  kubectl rollout status deploy/"$RELEASE"-gateway --timeout 5m >/dev/null
}

# run LABEL [loadgen flags...]
run() {
  local label=$1 start
  shift
  kubectl delete pod loadgen --ignore-not-found --wait >/dev/null
  kubectl run loadgen --image tinyserve-gateway:dev --image-pull-policy IfNotPresent --restart Never \
    --command -- /usr/local/bin/loadgen -addr "dns:///$RELEASE-gateway-headless:9000" -balance round_robin \
    -label "$label" -workload tenants -rate "$RATE" -duration "$DURATION" -max-tokens 32 -timeout 15s -timeline 5s "$@" >/dev/null
  kubectl wait --for=condition=Ready pod/loadgen --timeout 60s >/dev/null
  start=$(date +%s)
  at() { echo "t=$(($(date +%s) - start))s: $*"; }
  sleep 10
  at "rolling TinyServe"
  kubectl rollout restart deploy/"$RELEASE"-tinyserve >/dev/null
  kubectl rollout status deploy/"$RELEASE"-tinyserve --timeout 5m >/dev/null
  at "TinyServe rolled; rolling the gateways"
  kubectl rollout restart deploy/"$RELEASE"-gateway >/dev/null
  kubectl rollout status deploy/"$RELEASE"-gateway --timeout 5m >/dev/null
  at "gateways rolled"
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen --timeout 5m >/dev/null
  kubectl logs loadgen
  kubectl delete pod loadgen --wait >/dev/null
  echo
}

{
  echo "# Phase 5: rolling updates under load (kind, 3 nodes)"
  echo
  echo "rate=$RATE req/s for $DURATION, tenants workload, 32 tokens, client deadline 15s; 2 TinyServe + 2 gateway pods."
  echo "t=10s: rollout restart of TinyServe; once done, rollout restart of the gateways."
  echo
  echo '```'
  install --set tinyserve.preStopSleepSeconds=0 --set tinyserve.terminationGracePeriodSeconds=1 \
    --set gateway.preStopSleepSeconds=0 --set gateway.drainTimeoutSeconds=0
  run "no drain" -dns-min-interval 1s
  install --reset-values
  run "drain, client DNS interval 30s (gRPC default)"
  run "drain, client DNS interval 1s" -dns-min-interval 1s
  echo '```'
} | tee "$OUT/$NAME.md"
