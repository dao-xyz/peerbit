#!/usr/bin/env bash
# Live-interop harness runner: boot the js-libp2p Peerbit-config listener,
# extract its DIAL_ME multiaddr, run the rust peerbit_transport dialer bin, and
# assert a clean PASS. Used by the test_native CI job and reproducible locally.
#
#   bash packages/transport/transport-rust/scripts/run-interop.sh
#
# Exit 0 = interop PASS; non-zero = FAIL (with logs printed).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$HERE/.." && pwd)"
JS_DIR="$PKG_DIR/js"

cleanup() {
  if [[ -n "${LISTENER_PID:-}" ]] && kill -0 "$LISTENER_PID" 2>/dev/null; then
    kill "$LISTENER_PID" 2>/dev/null || true
    wait "$LISTENER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "== installing js listener deps =="
# The js harness depends on the real @peerbit/stream-interface via file:.., so
# it needs that workspace package built. In CI the workspace dist is restored
# by the job; locally, build it first if the dist is missing.
if [[ ! -f "$PKG_DIR/../stream-interface/dist/src/index.js" ]]; then
  echo "-- building @peerbit/stream-interface (dist missing) --"
  ( cd "$PKG_DIR/.." && pnpm --filter @peerbit/stream-interface... run build )
fi
( cd "$JS_DIR" && npm install --no-audit --no-fund --loglevel=error )

echo "== booting js-libp2p listener =="
LISTENER_LOG="$(mktemp)"
( cd "$JS_DIR" && node listener.mjs ) >"$LISTENER_LOG" 2>&1 &
LISTENER_PID=$!

# Poll for the DIAL_ME line (no idle sleep loop beyond a bounded timeout).
DIAL_ME=""
for _ in $(seq 1 60); do
  if ! kill -0 "$LISTENER_PID" 2>/dev/null; then
    echo "!! listener exited early; log:"; cat "$LISTENER_LOG"; exit 1
  fi
  DIAL_ME="$(grep -m1 '^DIAL_ME=' "$LISTENER_LOG" 2>/dev/null | sed 's/^DIAL_ME=//' || true)"
  [[ -n "$DIAL_ME" ]] && break
  sleep 0.5
done

if [[ -z "$DIAL_ME" ]]; then
  echo "!! listener never printed DIAL_ME; log:"; cat "$LISTENER_LOG"; exit 1
fi
echo "== js listener up: $DIAL_ME =="

echo "== running rust dialer =="
set +e
( cd "$PKG_DIR" && cargo run --quiet --bin interop_dial_js -- "$DIAL_ME" )
RUST_EXIT=$?
set -e

echo "== js listener log =="
cat "$LISTENER_LOG"

if [[ $RUST_EXIT -ne 0 ]]; then
  echo "!! rust dialer failed (exit $RUST_EXIT)"
  exit $RUST_EXIT
fi
echo "== LIVE INTEROP PASS =="
