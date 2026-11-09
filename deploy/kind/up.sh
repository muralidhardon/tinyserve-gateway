#!/usr/bin/env bash
# Local Kubernetes: a 3-node kind cluster running TinyServe behind the gateway.
#
#   deploy/kind/up.sh                 # cluster, images, chart
#   AUTOSCALING=keda deploy/kind/up.sh  # also Prometheus + KEDA, scaling on queue depth
#   OBSERVABILITY=1 deploy/kind/up.sh   # also Prometheus, Jaeger and Grafana; every request traced
#   CNI=cilium deploy/kind/up.sh        # Cilium instead of kindnet + kube-proxy, Hubble,
#                                       # and CiliumNetworkPolicies around both components
#   kind delete cluster --name tinyserve
#
# TINYSERVE_DIR points at the TinyServe checkout (default ../tinyserve).
# CNI only takes effect when the cluster is created; delete it to switch.
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=tinyserve
RELEASE=${RELEASE:-ts}
TS=${TINYSERVE_DIR:-../tinyserve}
AUTOSCALING=${AUTOSCALING:-none}
OBSERVABILITY=${OBSERVABILITY:-}
CNI=${CNI:-kindnet}
CILIUM_VERSION=${CILIUM_VERSION:-1.20.4}

CLUSTER_CONFIG=deploy/kind/cluster.yaml
[ "$CNI" = cilium ] && CLUSTER_CONFIG=deploy/kind/cluster-cilium.yaml
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --config "$CLUSTER_CONFIG"
kubectl config use-context "kind-$CLUSTER" >/dev/null

CHART_ARGS=(--set autoscaling.type="$AUTOSCALING")
if [ "$CNI" = cilium ]; then
  helm repo add cilium https://helm.cilium.io >/dev/null
  helm repo update cilium >/dev/null
  helm upgrade --install cilium cilium/cilium --version "$CILIUM_VERSION" \
    -n kube-system -f deploy/kind/cilium-values.yaml --wait --timeout 10m
  kubectl -n kube-system rollout status ds/cilium deploy/hubble-relay --timeout 5m
  CHART_ARGS+=(--set networkPolicy.enabled=true)
fi

docker build -q -t tinyserve-gateway:dev .
docker build -q -t tinyserve:dev "$TS"
kind load docker-image --name "$CLUSTER" tinyserve-gateway:dev tinyserve:dev

if [ "$AUTOSCALING" = keda ] || [ -n "$OBSERVABILITY" ]; then
  # Just the Prometheus server, scraping pods annotated prometheus.io/scrape.
  # The timeout must not exceed the interval; the chart's default is 10s.
  # From the OCI registry: the classic repo's index is large and slow to fetch.
  helm upgrade --install prometheus oci://ghcr.io/prometheus-community/charts/prometheus --version 29.40.3 \
    -n monitoring --create-namespace \
    --set alertmanager.enabled=false --set prometheus-pushgateway.enabled=false \
    --set prometheus-node-exporter.enabled=false --set kube-state-metrics.enabled=false \
    --set server.global.scrape_interval=5s --set server.global.scrape_timeout=4s \
    --set server.persistentVolume.enabled=false --wait
fi
if [ "$AUTOSCALING" = keda ]; then
  helm repo add kedacore https://kedacore.github.io/charts >/dev/null
  helm repo update kedacore >/dev/null
  helm upgrade --install keda kedacore/keda -n keda --create-namespace --wait
fi
if [ -n "$OBSERVABILITY" ]; then
  kubectl -n monitoring create configmap grafana-dashboards --from-file deploy/grafana/ \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl apply -f deploy/kind/observability.yaml >/dev/null
  kubectl -n monitoring rollout restart deploy/grafana >/dev/null # pick up dashboard changes
  kubectl -n monitoring rollout status deploy/jaeger deploy/grafana --timeout 5m
  # Trace every request: this is a demo, not a busy fleet.
  CHART_ARGS+=(--set tracing.otlpEndpoint=jaeger.monitoring.svc:4317 --set tracing.sampleRatio=1)
fi

helm upgrade --install "$RELEASE" deploy/helm/tinyserve-gateway \
  "${CHART_ARGS[@]}" ${HELM_ARGS:-} --wait --timeout 5m
# Pick up rebuilt :dev images on a re-run.
kubectl rollout restart deploy/"$RELEASE"-tinyserve deploy/"$RELEASE"-gateway >/dev/null
kubectl rollout status deploy/"$RELEASE"-tinyserve --timeout 5m
kubectl rollout status deploy/"$RELEASE"-gateway --timeout 5m
kubectl get pods -o wide

cat <<EOF

Try it:
  kubectl port-forward svc/$RELEASE-gateway 9000:9000 &
  bin/tsctl -max-tokens 40 "Alice was"
EOF
if [ -n "$OBSERVABILITY" ]; then
  cat <<EOF
  kubectl -n monitoring port-forward svc/grafana 3000 &   # http://localhost:3000, the gateway dashboard
  kubectl -n monitoring port-forward svc/jaeger 16686 &   # http://localhost:16686, service tinyserve-gateway
EOF
fi
