#!/usr/bin/env bash
# RELAY A/B scaling sweep runner (RELAY-PROFILING.md STEP 2).
#
# Boots ONE relay (native rust OR js) and runs the concurrency sweep through it,
# passing RELAY_PID so the bench can attribute the relay's CPU-seconds to each
# level's measured window. Writes the result JSON to $OUT.
#
#   bash scripts/run-relay-sweep.sh native /tmp/relay-native.json
#   bash scripts/run-relay-sweep.sh js     /tmp/relay-js.json
#
# Env knobs forwarded to the bench: PAYLOAD_BYTES, CONCURRENCY, RUNS, WARMUP.
# STRICTLY SEQUENTIAL: run one relay at a time, never concurrent with a build or
# another sweep. No idle-wait: bounded polls only.
set -euo pipefail

WHICH="${1:-native}"
OUT="${2:-/tmp/relay-${WHICH}.json}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$HERE/.." && pwd)"
JS_DIR="$PKG_DIR/js"

RELAY_LOG="$(mktemp)"
BENCH_OUT="$(mktemp)"
RELAY_PID=""
cleanup() {
  [[ -n "$RELAY_PID" ]] && kill "$RELAY_PID" 2>/dev/null || true
  [[ -n "$RELAY_PID" ]] && wait "$RELAY_PID" 2>/dev/null || true
  rm -f "$RELAY_LOG" "$BENCH_OUT"
}
trap cleanup EXIT

echo "== booting $WHICH relay =="
if [[ "$WHICH" == "native" ]]; then
  ( "$PKG_DIR/target/release/relay_node" ) >"$RELAY_LOG" 2>&1 &
  RELAY_PID=$!
elif [[ "$WHICH" == "js" ]]; then
  ( cd "$JS_DIR" && node relay-js.mjs ) >"$RELAY_LOG" 2>&1 &
  RELAY_PID=$!
else
  echo "usage: run-relay-sweep.sh [native|js] [out.json]"; exit 2
fi

RELAY_ADDR=""
for _ in $(seq 1 60); do
  if ! kill -0 "$RELAY_PID" 2>/dev/null; then echo "!! relay exited early:"; cat "$RELAY_LOG"; exit 1; fi
  RELAY_ADDR="$(grep -m1 '^RELAY_ADDR=' "$RELAY_LOG" 2>/dev/null | sed 's/^RELAY_ADDR=//' || true)"
  [[ -n "$RELAY_ADDR" ]] && break
  sleep 0.3
done
[[ -z "$RELAY_ADDR" ]] && { echo "!! relay never printed RELAY_ADDR:"; cat "$RELAY_LOG"; exit 1; }

# The relay prints its OWN pid (RELAY_SELF_PID). Using $! is unreliable: a
# `( cd .. && node .. ) &` subshell does not exec-replace, so $! is the subshell
# and `ps` on it shows ~0 CPU. RELAY_SELF_PID is the actual forwarding process.
RELAY_SELF_PID="$(grep -m1 '^RELAY_SELF_PID=' "$RELAY_LOG" 2>/dev/null | sed 's/^RELAY_SELF_PID=//' || true)"
[[ -z "$RELAY_SELF_PID" ]] && RELAY_SELF_PID="$RELAY_PID"
echo "== $WHICH relay up (pid=$RELAY_SELF_PID): $RELAY_ADDR =="

# Run the sweep. RELAY_PID lets the bench snapshot relay CPU-time per level.
( cd "$JS_DIR" && \
    RELAY_ADDR="$RELAY_ADDR" RELAY_PID="$RELAY_SELF_PID" LABEL="$WHICH" \
    PAYLOAD_BYTES="${PAYLOAD_BYTES:-262144}" \
    CONCURRENCY="${CONCURRENCY:-1,10,50,100}" \
    RUNS="${RUNS:-6}" WARMUP="${WARMUP:-2}" \
    node relay-bench.mjs ) >"$BENCH_OUT" 2>&1 &
BENCH_PID=$!

# Poll for completion (bounded). High concurrency + big payloads can take a
# while; cap generously and never idle-wait beyond it.
for _ in $(seq 1 1200); do  # up to 600s
  kill -0 "$BENCH_PID" 2>/dev/null || break
  sleep 0.5
done
if kill -0 "$BENCH_PID" 2>/dev/null; then
  echo "!! sweep exceeded time budget, killing"; kill -9 "$BENCH_PID" 2>/dev/null || true
fi
wait "$BENCH_PID" 2>/dev/null; BENCH_EXIT=$?

echo "== sweep stderr (progress + per-level) =="
grep -E '\[bench:|SWEEP_LEVEL|fatal|Error' "$BENCH_OUT" || true

# Extract the RESULT_JSON and merge in the js relay's event-loop lag samples.
RESULT_JSON="$(grep -m1 '^RESULT_JSON=' "$BENCH_OUT" | sed 's/^RESULT_JSON=//' || true)"
if [[ -z "$RESULT_JSON" ]]; then
  echo "!! no RESULT_JSON produced (exit=$BENCH_EXIT); full output:"; cat "$BENCH_OUT"; exit 1
fi

# Event-loop lag summary for the js relay (max over the run; the JS saturation
# signal). Native relay has none.
if [[ "$WHICH" == "js" ]]; then
  ELL_MAX="$(grep 'EVENT_LOOP_LAG' "$RELAY_LOG" | sed -E 's/.*max=([0-9.]+).*/\1/' | sort -n | tail -1 || echo '')"
  ELL_P99MAX="$(grep 'EVENT_LOOP_LAG' "$RELAY_LOG" | sed -E 's/.*p99=([0-9.]+).*max.*/\1/' | sort -n | tail -1 || echo '')"
  echo "== js relay event-loop lag: worst p99=${ELL_P99MAX}ms worst max=${ELL_MAX}ms =="
  RESULT_JSON="$(node -e "const r=JSON.parse(process.argv[1]); r.event_loop_lag_worst_p99_ms=${ELL_P99MAX:-null}; r.event_loop_lag_worst_max_ms=${ELL_MAX:-null}; process.stdout.write(JSON.stringify(r));" "$RESULT_JSON")"
fi

printf '%s\n' "$RESULT_JSON" >"$OUT"
echo "== wrote $OUT =="
