#!/usr/bin/env bash
# Phase 2: routing strategies on replicas of uneven speed.
#
# Three fake TinyServe replicas, each with 8 batch slots and a 64-deep queue
# (then 503), like TinyServe's scheduler. Two generate at 1 ms/token, one at
# 3 ms/token -- a slower node, a noisy neighbour, or a replica mid-GC.
# Poisson arrivals at RATE req/s, 64 tokens each, for DURATION.
#
# Capacity: a fast replica serves 8 / (5 ms + 64 ms) ~= 116 req/s, the slow
# one 8 / (5 ms + 192 ms) ~= 41 req/s. At 150 req/s round-robin sends the slow
# replica 50 req/s, more than it can serve.
#
#   bench/phase2_uneven.sh            # writes bench/results/phase2_uneven.md
set -euo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-150}
DURATION=${DURATION:-20s}
TOKENS=${TOKENS:-64}
OUT=bench/results
mkdir -p "$OUT"
make -s build

pids=()
cleanup() { kill ${pids[@]+"${pids[@]}"} 2>/dev/null || true; wait 2>/dev/null || true; pids=(); }
trap cleanup EXIT

run() {
  local router=$1
  pids=()
  bin/fakebackend -listen :8201 -token-delay 1ms -slots 8 -max-queue 64 2>/dev/null & pids+=($!)
  bin/fakebackend -listen :8202 -token-delay 1ms -slots 8 -max-queue 64 2>/dev/null & pids+=($!)
  bin/fakebackend -listen :8203 -token-delay 3ms -slots 8 -max-queue 64 2>/dev/null & pids+=($!)
  bin/gateway -listen :9100 -router "$router" \
    -backends http://localhost:8201,http://localhost:8202,http://localhost:8203 2>/dev/null & pids+=($!)
  sleep 1
  bin/loadgen -addr localhost:9100 -label "$router" -rate "$RATE" -duration "$DURATION" \
    -max-tokens "$TOKENS" -out "$OUT/phase2_uneven_$router.json"
  cleanup
  echo
}

{
  echo "# Phase 2: routing on uneven replicas"
  echo
  echo "rate=$RATE req/s, duration=$DURATION, max_tokens=$TOKENS; replicas :8201 and :8202 at 1 ms/token, :8203 at 3 ms/token; 8 slots, 64-deep queue each."
  echo
  echo '```'
  for r in round_robin random least_loaded consistent_hash; do run "$r"; done
  echo '```'
} | tee "$OUT/phase2_uneven.md"
