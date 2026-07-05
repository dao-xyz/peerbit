// INTEROP GATE (RELAY-PROFILING.md STEP 1).
//
// Proves a relay forwards real bytes between two js-libp2p circuit-relay
// CLIENTS. Given RELAY_ADDR (a NATIVE rust relay for the gate that matters, or
// a js relay as a control), this:
//
//   1. boots two js-libp2p circuit-relay client peers — DEST and SOURCE —
//      configured like a Peerbit node peer,
//   2. DEST reserves on the relay (dials it; /p2p-circuit in its listen set),
//   3. SOURCE dials DEST THROUGH the relay via
//      <relayAddr>/p2p-circuit/p2p/<destPeerId>,
//   4. SOURCE opens a /relay-bench stream over that relayed circuit and sends a
//      known payload; DEST receives it, verifies the exact bytes, and echoes an
//      ack carrying the received length,
//   5. asserts the relayed connection is a "limited" (circuit) connection and
//      that the payload arrived byte-exact end-to-end.
//
// PASS prints RELAY_GATE=PASS with the evidence (peer ids, addr, bytes). Any
// failure prints RELAY_GATE=FAIL and exits non-zero. The native relay's own
// stdout (CIRCUIT_ACCEPTED/CIRCUIT_CLOSED between the two peer ids) is the
// independent transport-layer confirmation, captured by the runner script.

import { multiaddr } from '@multiformats/multiaddr'
import { createClientNode, BENCH_PROTOCOL } from './relay-common.mjs'

const RELAY_ADDR = process.env.RELAY_ADDR
if (!RELAY_ADDR) {
  console.error('RELAY_ADDR env is required')
  process.exit(2)
}

const PAYLOAD_BYTES = Number(process.env.GATE_PAYLOAD_BYTES ?? 65536)

function fail (msg) {
  console.log(`RELAY_GATE=FAIL reason="${msg}"`)
  process.exit(1)
}

async function main () {
  const relayMa = multiaddr(RELAY_ADDR)

  const dest = await createClientNode()
  await dest.start()
  const source = await createClientNode()
  await source.start()

  const destPeer = dest.peerId.toString()
  const srcPeer = source.peerId.toString()

  // Known payload, verified byte-exact on the dest side.
  const payload = new Uint8Array(PAYLOAD_BYTES)
  for (let i = 0; i < payload.length; i++) payload[i] = (i * 131 + 17) & 0xff

  let destReceived = -1
  let destByteExact = false
  let destConnLimited = null
  const destDone = new Promise((resolve) => {
    dest.handle(
      BENCH_PROTOCOL,
      (stream, connection) => {
        ;(async () => {
          try {
            // The relayed connection MUST be a circuit connection. The robust,
            // relay-config-independent signal is a /p2p-circuit component in the
            // remote address (an unlimited js relay with applyDefaultLimit:false
            // sets NO `.limits`, so `.limits` alone is not a reliable signal).
            const raddr = connection?.remoteAddr?.toString() ?? ''
            destConnLimited =
              raddr.includes('/p2p-circuit') || connection?.limits != null
            const parts = []
            let total = 0
            for await (const chunk of stream) {
              const u = chunk instanceof Uint8Array ? chunk : chunk.subarray()
              parts.push(u)
              total += u.byteLength
            }
            destReceived = total
            // Verify byte-exactness of the reassembled payload.
            const joined = new Uint8Array(total)
            let o = 0
            for (const p of parts) { joined.set(p, o); o += p.byteLength }
            destByteExact =
              total === payload.length && joined.every((b, i) => b === payload[i])
            stream.send(new Uint8Array([total & 0xff, (total >> 8) & 0xff]))
            await stream.close()
          } catch (e) {
            console.error('dest handler error:', e?.stack ?? e)
          } finally {
            resolve()
          }
        })()
      },
      { runOnLimitedConnection: true }
    )
  })

  // DEST reserves on the relay.
  await dest.dial(relayMa)
  const destRelayed = relayMa
    .encapsulate('/p2p-circuit')
    .encapsulate(`/p2p/${destPeer}`)

  console.error(`gate: relay=${RELAY_ADDR}`)
  console.error(`gate: dest=${destPeer} reserved; relayed addr=${destRelayed.toString()}`)
  console.error(`gate: source=${srcPeer} dialing dest through relay...`)

  // SOURCE dials DEST through the relay and streams the payload. We dial the
  // connection first so we can read its `.limits` (the relayed-circuit signal),
  // then open the stream over it.
  let ackLen = -1
  let srcConnLimited = null
  try {
    const conn = await source.dial(destRelayed, { runOnLimitedConnection: true })
    const sraddr = conn?.remoteAddr?.toString() ?? ''
    srcConnLimited = sraddr.includes('/p2p-circuit') || conn?.limits != null
    const stream = await conn.newStream(BENCH_PROTOCOL, {
      runOnLimitedConnection: true
    })
    stream.send(payload)
    await stream.close()
    for await (const chunk of stream) {
      const u = chunk instanceof Uint8Array ? chunk : chunk.subarray()
      ackLen = u[0] | (u[1] << 8)
      break
    }
  } catch (e) {
    fail(`source dial/stream through relay failed: ${e?.message ?? e}`)
  }

  await destDone

  console.error(
    `gate: destReceived=${destReceived} byteExact=${destByteExact} ackLen=${ackLen} ` +
      `destLimited=${destConnLimited} srcLimited=${srcConnLimited}`
  )

  if (destReceived !== PAYLOAD_BYTES) fail(`dest received ${destReceived}, expected ${PAYLOAD_BYTES}`)
  if (!destByteExact) fail('payload not byte-exact end-to-end through relay')
  if ((ackLen & 0xffff) !== (PAYLOAD_BYTES & 0xffff)) fail(`ack length ${ackLen} != payload ${PAYLOAD_BYTES}`)
  if (destConnLimited !== true) fail('dest connection was NOT a limited/circuit connection (not actually relayed)')
  if (srcConnLimited !== true) fail('source connection was NOT a limited/circuit connection (not actually relayed)')

  console.log(
    `RELAY_GATE=PASS relay="${RELAY_ADDR}" src=${srcPeer} dst=${destPeer} ` +
      `bytes_forwarded=${PAYLOAD_BYTES} byte_exact=true relayed_circuit=true ack=${ackLen}`
  )

  await Promise.all([source.stop().catch(() => {}), dest.stop().catch(() => {})])
  process.exit(0)
}

main().catch((err) => {
  fail(`unexpected: ${err?.stack ?? err}`)
})
