#!/usr/bin/env bash
# Phase 6: what metrics and tracing cost the gateway.
#
# Three fake replicas that answer instantly (no prefill or token delay), so
# the gateway itself is the bottleneck. A closed loop of CONCURRENCY clients
# sends REQUESTS requests of 16 tokens (2 SSE events each). The same run is
# repeated with telemetry off, metrics on, and metrics plus tracing at 0%
# (instrumentation installed, nothing sampled), 10% and 100% sampling,
# exporting to a local Jaeger over OTLP. Reports
# throughput, latency and the gateway's CPU time per 1,000 requests.
#
# Needs Docker for Jaeger. Run it on an otherwise idle machine.
#
#   bench/phase6_overhead.sh         # writes bench/results/phase6_overhead.md
set -euo pipefail
cd "$(dirname "$0")/.."

REQUESTS=${REQUESTS:-30000}
CONCURRENCY=${CONCURRENCY:-64}
ROUNDS=${ROUNDS:-3}
OUT=bench/results
mkdir -p "$OUT"
make -s build

pids=()
cleanup() {
  kill ${pids[@]+"${pids[@]}"} 2>/dev/null || true
  wait 2>/dev/null || true
  pids=()
}
trap 'cleanup; docker rm -f tsgw-jaeger >/dev/null 2>&1 || true' EXIT

docker run -d --rm --name tsgw-jaeger -p 4317:4317 -p 16686:16686 jaegertracing/jaeger:2.9.0 >/dev/null
for p in 8301 8302 8303; do
  bin/fakebackend -listen :$p -token-delay 0 -prefill-delay 0 2>/dev/null & pids+=($!)
done
sleep 2

# cpu_seconds PID: user+system CPU time of a process, from ps (M:SS.ss).
cpu_seconds() { ps -o time= -p "$1" | awk -F: '{ print $1 * 60 + $2 }'; }

run() {
  local label=$1 gw before after
  shift
  bin/gateway -listen :9300 -router least_loaded -backends http://localhost:8301,http://localhost:8302,http://localhost:8303 \
    -health-interval 0 "$@" 2>/dev/null &
  gw=$!
  sleep 1
  # Warm up: open connections to every replica before measuring.
  bin/loadgen -addr localhost:9300 -concurrency "$CONCURRENCY" -requests 2000 -max-tokens 16 >/dev/null
  before=$(cpu_seconds $gw)
  bin/loadgen -addr localhost:9300 -label "$label" -concurrency "$CONCURRENCY" -requests "$REQUESTS" -max-tokens 16 |
    grep -E '^(==|throughput|latency)'
  after=$(cpu_seconds $gw)
  awk -v b="$before" -v a="$after" -v n="$REQUESTS" 'BEGIN { printf "gateway CPU %.0f ms per 1,000 requests\n", (a - b) * 1000000 / n }'
  kill $gw
  wait $gw 2>/dev/null || true
  echo
}

{
  echo "# Phase 6: telemetry overhead"
  echo
  echo "$REQUESTS requests per run, closed loop of $CONCURRENCY clients, 16 tokens; 3 instant fake replicas; $ROUNDS rounds."
  echo
  echo '```'
  for round in $(seq "$ROUNDS"); do
    echo "--- round $round"
    run "off" -metrics-listen ""
    run "metrics"
    run "metrics + tracing 0%" -otlp-endpoint localhost:4317 -trace-sample 0
    run "metrics + tracing 10%" -otlp-endpoint localhost:4317 -trace-sample 0.1
    run "metrics + tracing 100%" -otlp-endpoint localhost:4317 -trace-sample 1
  done
  echo '```'
} | tee "$OUT/phase6_overhead.md"
