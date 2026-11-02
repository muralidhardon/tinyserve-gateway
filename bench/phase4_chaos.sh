#!/usr/bin/env bash
# Phase 4: replica failures under load, with and without the gateway's fault tolerance.
#
# Four fake TinyServe replicas (8 batch slots and a 64-deep queue each), Poisson
# arrivals at RATE req/s, 64 tokens per request, under each router in ROUTERS.
# During the run:
#
#   t=8s   SIGSTOP replica 2 -- a gray failure: the kernel still accepts its
#          connections, but nothing answers (a hung process, a wedged GPU)
#   t=16s  SIGKILL replica 3 -- a crash: open streams cut, connections refused
#   t=24s  SIGCONT replica 2 -- it recovers
#
# "no protection" turns off health checks, outlier ejection, the first-token
# timeout, the retry budget and admission control, which is the gateway as of
# Phase 3; "phase 4" uses the defaults.
#
#   bench/phase4_chaos.sh            # writes bench/results/phase4_chaos.md
set -euo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-150}
DURATION=${DURATION:-32s}
TOKENS=${TOKENS:-64}
ROUTERS=${ROUTERS:-"round_robin least_loaded"}
OUT=bench/results
mkdir -p "$OUT"
make -s build

pids=()
cleanup() {
  kill -CONT ${pids[@]+"${pids[@]}"} 2>/dev/null || true
  kill ${pids[@]+"${pids[@]}"} 2>/dev/null || true
  wait 2>/dev/null || true
  pids=()
}
trap cleanup EXIT

slug() { echo "$1" | tr -c 'a-z0-9\n' _; }

run() {
  local label=$1; shift
  pids=()
  for port in 8501 8502 8503 8504; do
    bin/fakebackend -listen :$port -token-delay 1ms -slots 8 -max-queue 64 2>/dev/null & pids+=($!)
  done
  local stall=${pids[1]} crash=${pids[2]}
  bin/gateway -listen :9300 "$@" \
    -backends http://localhost:8501,http://localhost:8502,http://localhost:8503,http://localhost:8504 \
    2>"$OUT/phase4_chaos_$(slug "$label").log" & pids+=($!)
  sleep 1
  ( sleep 8; kill -STOP "$stall"; sleep 8; kill -KILL "$crash"; sleep 8; kill -CONT "$stall" ) &
  local chaos=$!
  bin/loadgen -addr localhost:9300 -label "$label" -rate "$RATE" -duration "$DURATION" \
    -max-tokens "$TOKENS" -timeout 10s -timeline 2s -out "$OUT/phase4_chaos_$(slug "$label").json"
  wait "$chaos" || true
  echo "gateway events:"
  grep -oE '"msg":"replica (ejected|re-admitted)","backend":"[^"]*"(,"reason":"[^"]*")?' \
    "$OUT/phase4_chaos_$(slug "$label").log" | sed 's/"msg"://; s/"backend"://; s/"reason"://' | uniq -c || echo "  none"
  cleanup
  echo
}

{
  echo "# Phase 4: chaos under load"
  echo
  echo "rate=$RATE req/s, duration=$DURATION, max_tokens=$TOKENS, client deadline 10s; 4 fake replicas"
  echo "(1 ms/token, 8 slots, 64-deep queue). t=8s SIGSTOP :8502, t=16s SIGKILL :8503, t=24s SIGCONT :8502."
  echo
  echo '```'
  for r in $ROUTERS; do
    run "$r, no protection" -router "$r" \
      -health-interval 0 -outlier-failures 0 -first-token-timeout 0 -retry-budget -1 -max-inflight 0
    run "$r, phase 4" -router "$r"
  done
  echo '```'
} | tee "$OUT/phase4_chaos.md"
rm -f "$OUT"/phase4_chaos_*.log
gzip -9f "$OUT"/phase4_chaos_*.json
