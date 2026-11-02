#!/usr/bin/env bash
# Local Kubernetes: a 3-node kind cluster running TinyServe behind the gateway.
#
#   deploy/kind/up.sh                 # cluster, images, chart
#   AUTOSCALING=keda deploy/kind/up.sh  # also Prometheus + KEDA, scaling on queue depth
#   kind delete cluster --name tinyserve
#
# TINYSERVE_DIR points at the TinyServe checkout (default ../tinyserve).
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=tinyserve
RELEASE=${RELEASE:-ts}
TS=${TINYSERVE_DIR:-../tinyserve}
AUTOSCALING=${AUTOSCALING:-none}

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --config deploy/kind/cluster.yaml
kubectl config use-context "kind-$CLUSTER" >/dev/null

docker build -q -t tinyserve-gateway:dev .
docker build -q -t tinyserve:dev "$TS"
kind load docker-image --name "$CLUSTER" tinyserve-gateway:dev tinyserve:dev

if [ "$AUTOSCALING" = keda ]; then
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
  helm repo add kedacore https://kedacore.github.io/charts >/dev/null
  helm repo update >/dev/null
  # Just the Prometheus server, scraping pods annotated prometheus.io/scrape.
  helm upgrade --install prometheus prometheus-community/prometheus -n monitoring --create-namespace \
    --set alertmanager.enabled=false --set prometheus-pushgateway.enabled=false \
    --set prometheus-node-exporter.enabled=false --set kube-state-metrics.enabled=false \
    --set server.global.scrape_interval=5s --set server.persistentVolume.enabled=false --wait
  helm upgrade --install keda kedacore/keda -n keda --create-namespace --wait
fi

helm upgrade --install "$RELEASE" deploy/helm/tinyserve-gateway \
  --set autoscaling.type="$AUTOSCALING" ${HELM_ARGS:-} --wait --timeout 5m
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
