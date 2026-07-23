#!/usr/bin/env bash
# RELAY interop gate runner (RELAY-PROFILING.md STEP 1).
#
# Boots a relay (native rust OR js), then runs the js↔js circuit-relay gate
# through it and asserts RELAY_GATE=PASS. Prints the relay's own transport-layer
# evidence (RESERVATION_ACCEPTED / CIRCUIT_ACCEPTED / CIRCUIT_CLOSED for the js
# peers) alongside the js-side end-to-end byte-exactness proof.
#
#   bash scripts/run-relay-gate.sh native    # gate the native rust relay (the one that matters)
#   bash scripts/run-relay-gate.sh js         # gate the js relay (harness control)
#
# Exit 0 = gate PASS; non-zero = FAIL (logs printed). No idle-wait: bounded polls.
set -euo pipefail

WHICH="${1:-native}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$HERE/.." && pwd)"
JS_DIR="$PKG_DIR/js"

RELAY_LOG="$(mktemp)"
GATE_OUT="$(mktemp)"
RELAY_PID=""
cleanup() {
  [[ -n "$RELAY_PID" ]] && kill "$RELAY_PID" 2>/dev/null || true
  [[ -n "$RELAY_PID" ]] && wait "$RELAY_PID" 2>/dev/null || true
  rm -f "$RELAY_LOG" "$GATE_OUT"
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
  echo "usage: run-relay-gate.sh [native|js]"; exit 2
fi

RELAY_ADDR=""
for _ in $(seq 1 60); do
  if ! kill -0 "$RELAY_PID" 2>/dev/null; then echo "!! relay exited early:"; cat "$RELAY_LOG"; exit 1; fi
  RELAY_ADDR="$(grep -m1 '^RELAY_ADDR=' "$RELAY_LOG" 2>/dev/null | sed 's/^RELAY_ADDR=//' || true)"
  [[ -n "$RELAY_ADDR" ]] && break
  sleep 0.3
done
[[ -z "$RELAY_ADDR" ]] && { echo "!! relay never printed RELAY_ADDR:"; cat "$RELAY_LOG"; exit 1; }
echo "== $WHICH relay up: $RELAY_ADDR =="

( cd "$JS_DIR" && RELAY_ADDR="$RELAY_ADDR" node relay-gate.mjs ) >"$GATE_OUT" 2>&1 &
GATE_PID=$!
for _ in $(seq 1 80); do  # up to 40s
  kill -0 "$GATE_PID" 2>/dev/null || break
  sleep 0.5
done
if kill -0 "$GATE_PID" 2>/dev/null; then echo "!! gate hung >40s, killing"; kill -9 "$GATE_PID" 2>/dev/null || true; fi
wait "$GATE_PID" 2>/dev/null; GATE_EXIT=$?
sleep 0.4  # let the relay flush CIRCUIT_CLOSED

echo "== gate output =="
cat "$GATE_OUT"
echo "== $WHICH relay transport-layer evidence =="
grep -E 'RESERVATION_ACCEPTED|CIRCUIT_ACCEPTED|CIRCUIT_CLOSED|RESERVATION_DENIED|CIRCUIT_DENIED' "$RELAY_LOG" \
  || echo "(js relay does not emit per-circuit lines; end-to-end byte-exactness above is the proof)"

if [[ $GATE_EXIT -ne 0 ]] || ! grep -q '^RELAY_GATE=PASS' "$GATE_OUT"; then
  echo "!! RELAY GATE FAIL ($WHICH), exit=$GATE_EXIT"
  exit 1
fi
echo "== RELAY GATE PASS ($WHICH) =="
