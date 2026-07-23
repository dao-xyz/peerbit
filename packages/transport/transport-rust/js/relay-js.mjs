// Path A relay: the js-libp2p circuitRelayServer node with Peerbit's exact
// relay config (applyDefaultLimit:false, maxReservations:1000). Prints
// RELAY_ADDR for the bench driver, and periodically emits EVENT_LOOP_LAG lines
// — the known JS-relay-under-load failure mode the A/B is looking for.
//
//   node relay-js.mjs
// prints:
//   RELAY_ADDR=/ip4/127.0.0.1/tcp/<port>/p2p/<peerId>
//   EVENT_LOOP_LAG mean=<ms> p99=<ms> max=<ms>   (every 500ms)
//
// Runs until SIGINT/SIGTERM.

import { monitorEventLoopDelay } from 'perf_hooks'
import { createJsRelayNode, relayDialAddr } from './relay-common.mjs'

const relay = await createJsRelayNode()
await relay.start()

console.log(`RELAY_ADDR=${relayDialAddr(relay)}`)
console.log(`RELAY_SELF_PID=${process.pid}`)
console.log(`relay peerId=${relay.peerId.toString()}`)

// Event-loop delay histogram — the direct measure of JS-relay saturation.
// resolution 1ms (fine enough that the sampling floor does not swamp real lag);
// we report mean/p99/max over each 500ms window and reset.
const h = monitorEventLoopDelay({ resolution: 1 })
h.enable()
const lagTimer = setInterval(() => {
  const meanMs = h.mean / 1e6
  const p99Ms = h.percentile(99) / 1e6
  const maxMs = h.max / 1e6
  console.log(
    `EVENT_LOOP_LAG mean=${meanMs.toFixed(3)} p99=${p99Ms.toFixed(3)} max=${maxMs.toFixed(3)}`
  )
  h.reset()
}, 500)

const shutdown = async () => {
  clearInterval(lagTimer)
  h.disable()
  try {
    await relay.stop()
  } catch {}
  process.exit(0)
}
process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
