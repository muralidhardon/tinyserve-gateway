#!/usr/bin/env bash
# Phase 6: traffic for the dashboard and traces, with one fault.
#
# A loadgen pod sends Poisson traffic through the gateways' headless Service
# for DURATION. At t=60s one TinyServe process is killed with SIGKILL from
# its node -- a crash, like a segfault or the OOM killer. Kubernetes still
# lists the pod as ready until its probes fail, so the gateway has to notice
# on its own: streams open on it break, new requests fail over, and health
# checks eject it until the restarted container answers. The dashboard and
# traces should show all of that.
#
# (Deleting the pod, even with --force --grace-period 0, is not a crash: the
# pod leaves the EndpointSlice at once and the container still gets SIGTERM,
# so its streams finish. Under this load that loses nothing.)
#
# Needs the cluster from OBSERVABILITY=1 deploy/kind/up.sh.
#
#   bench/phase6_demo.sh        # writes bench/results/phase6_demo.md
set -euo pipefail
cd "$(dirname "$0")/.."

RELEASE=${RELEASE:-ts}
RATE=${RATE:-120}
DURATION=${DURATION:-240s}
OUT=bench/results
mkdir -p "$OUT"
kubectl config use-context kind-tinyserve >/dev/null

kubectl delete pod loadgen --ignore-not-found --wait >/dev/null
kubectl run loadgen --image tinyserve-gateway:dev --image-pull-policy IfNotPresent --restart Never \
  --command -- /usr/local/bin/loadgen -addr "dns:///$RELEASE-gateway-headless:9000" -balance round_robin \
  -dns-min-interval 1s -label "TinyServe process killed at t=60s" -workload tenants -rate "$RATE" \
  -duration "$DURATION" -max-tokens 48 -timeout 15s -timeline 10s >/dev/null
kubectl wait --for=condition=Ready pod/loadgen --timeout 60s >/dev/null
start=$(date +%s)
sleep 60
victim=$(kubectl get pod -l app.kubernetes.io/component=tinyserve -o jsonpath='{.items[0].metadata.name}')
node=$(kubectl get pod "$victim" -o jsonpath='{.spec.nodeName}')
echo "t=$(($(date +%s) - start))s: SIGKILL to TinyServe in $victim" >&2
docker exec "$node" sh -c "kill -9 \$(crictl inspect --output go-template --template '{{.info.pid}}' \
  \$(crictl ps -q --name tinyserve --pod \$(crictl pods -q --name $victim)))"
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/loadgen --timeout 10m >/dev/null
{
  echo "# Phase 6: traffic with one TinyServe process crashing"
  echo
  echo "rate=$RATE req/s for $DURATION, tenants workload, 48 tokens; 2 TinyServe + 2 gateway pods; one TinyServe process SIGKILLed at t=60s (container restarts)."
  echo
  echo '```'
  kubectl logs loadgen
  echo '```'
} | tee "$OUT/phase6_demo.md"
kubectl delete pod loadgen --wait >/dev/null
