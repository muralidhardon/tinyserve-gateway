#!/usr/bin/env bash
# Phase 3: prefix-aware routing on real TinyServe replicas.
#
# Workload: 24 tenants, each with its own ~85-token system prompt (5 prefix-
# cache blocks), followed by a short per-request question; tenant popularity
# is Zipf(1.1), so a few tenants are hot. 16 generated tokens per request,
# so time to first token -- prefill -- is most of the work.
#
# Three replicas, each with a 48-block prefix cache. The tenants' prefixes
# need ~120 blocks: more than one replica holds, less than the fleet does. So
# whether a request finds its prefix cached depends on where it is routed.
#
# Each strategy gets freshly started replicas (cold caches).
#
#   TINYSERVE_DIR=../tinyserve bench/phase3_prefix.sh
set -euo pipefail
cd "$(dirname "$0")/.."

TS=${TINYSERVE_DIR:-../tinyserve}
RATE=${RATE:-300}
DURATION=${DURATION:-20s}
TOKENS=${TOKENS:-16}
BLOCKS=${BLOCKS:-48}
NAME=${NAME:-phase3_prefix}
ZIPF=${ZIPF:-1.1}
ROUTERS=${ROUTERS:-"none round_robin least_loaded consistent_hash prefix_aware"}
OUT=bench/results
mkdir -p "$OUT"
make -s build

pids=()
cleanup() { kill ${pids[@]+"${pids[@]}"} 2>/dev/null || true; wait 2>/dev/null || true; pids=(); }
trap cleanup EXIT

replicas() {
  local blocks=$1
  for port in 8401 8402 8403; do
    (cd "$TS" && TINYSERVE_THREADS=2 TINYSERVE_PREFIX_CACHE_BLOCKS=$blocks \
      exec .venv/bin/uvicorn server.main:app --port $port --log-level warning) 2>/dev/null & pids+=($!)
  done
  for port in 8401 8402 8403; do
    until curl -sf localhost:$port/health >/dev/null; do sleep 0.2; done
  done
}

run() {
  local label=$1 router=$2 blocks=$3
  replicas "$blocks"
  bin/gateway -listen :9200 -router "$router" \
    -backends http://localhost:8401,http://localhost:8402,http://localhost:8403 2>/dev/null & pids+=($!)
  sleep 0.5
  bin/loadgen -addr localhost:9200 -label "$label" -workload tenants -tenants 24 -zipf "$ZIPF" \
    -rate "$RATE" -duration "$DURATION" -max-tokens "$TOKENS" -out "$OUT/${NAME}_${label// /_}.json"
  cleanup
  echo
}

{
  echo "# Phase 3: prefix-aware routing, real TinyServe"
  echo
  echo "rate=$RATE req/s, duration=$DURATION, max_tokens=$TOKENS, 24 tenants with Zipf($ZIPF) popularity;"
  echo "3 replicas, TINYSERVE_THREADS=2, prefix cache $BLOCKS blocks each (working set ~120 blocks)."
  echo
  echo '```'
  for r in $ROUTERS; do
    if [ "$r" = none ]; then run "no prefix cache" least_loaded 0; else run "$r" "$r" "$BLOCKS"; fi
  done
  echo '```'
} | tee "$OUT/$NAME.md"
gzip -9f "$OUT/${NAME}"_*.json
