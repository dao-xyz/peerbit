// LIVE INTEROP listener — a js-libp2p 3.3.4 node configured EXACTLY like a
// Peerbit node peer, registering ALL THREE frozen /peerbit/* multicodecs and
// decoding each inbound frame with the REAL @peerbit/stream-interface
// DataMessage codec (the production Borsh schema + Ed25519/SHA-256 signable
// rules), then re-serializing with `.bytes()` and echoing the byte-identical
// envelope back to the rust dialer.
//
// This extends spikes/rust-libp2p-poc/js/listener.mjs from one representative
// /peerbit/direct-stream/2.0.0 id + a toy [tag][payload] frame to the three
// real protocol ids + the actual peerbit_wire envelope through the production
// js codec. Both stacks therefore agree on the same bytes with production
// codecs on each side.
//
// Node transport spec (config research / FEASIBILITY.md §3):
//   - transports: TCP (rust<->js interop path; node also does ws + circuit)
//   - connectionEncrypters: noise    (@chainsafe/libp2p-noise)
//   - streamMuxers:         yamux    (@chainsafe/libp2p-yamux)
//   - identity:             Ed25519  (Peerbit HARD-REQUIRES ed25519)
//   - services:             identify (@libp2p/identify)
//   - each /peerbit/* handler mounted with runOnLimitedConnection:false
//
// Requires the workspace to be built (so @peerbit/stream-interface resolves).
// In CI this runs in the test_native job with the workspace dist restored.
//
//   node listener.mjs
// prints:  DIAL_ME=/ip4/127.0.0.1/tcp/<port>/p2p/<peerId>

import { createLibp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { generateKeyPair } from '@libp2p/crypto/keys'
import { Uint8ArrayList } from 'uint8arraylist'
import { Message } from '@peerbit/stream-interface'

// The three frozen /peerbit/* multicodecs (byte-identical to the repo).
const PEERBIT_PROTOCOLS = [
  '/peerbit/direct-block/1.0.0',
  '/peerbit/topic-control-plane/2.0.0',
  '/peerbit/fanout-tree/0.5.0'
]

/** Read one unsigned-varint-length-prefixed frame off a v3 MessageStream. */
async function readOneFrame (stream) {
  let buf = new Uint8Array(0)
  const append = (chunk) => {
    const bytes = chunk instanceof Uint8Array ? chunk : chunk.subarray()
    const next = new Uint8Array(buf.length + bytes.length)
    next.set(buf, 0)
    next.set(bytes, buf.length)
    buf = next
  }
  const tryDecode = () => {
    let len = 0
    let shift = 0
    for (let i = 0; i < buf.length; i++) {
      const b = buf[i]
      len |= (b & 0x7f) << shift
      if ((b & 0x80) === 0) {
        return { len, headerLen: i + 1 }
      }
      shift += 7
    }
    return null
  }
  for await (const chunk of stream) {
    append(chunk)
    const hdr = tryDecode()
    if (hdr && buf.length >= hdr.headerLen + hdr.len) {
      return buf.subarray(hdr.headerLen, hdr.headerLen + hdr.len)
    }
  }
  throw new Error('stream ended before a full frame arrived')
}

/** Prepend an unsigned-varint length prefix to an envelope. */
function frameEnvelope (envelope) {
  const header = []
  let n = envelope.length
  do {
    let b = n & 0x7f
    n >>>= 7
    if (n !== 0) b |= 0x80
    header.push(b)
  } while (n !== 0)
  const out = new Uint8Array(header.length + envelope.length)
  out.set(header, 0)
  out.set(envelope, header.length)
  return out
}

// Ed25519 identity — same key type Peerbit derives its peerId from.
const privateKey = await generateKeyPair('Ed25519')

const node = await createLibp2p({
  privateKey,
  addresses: {
    listen: ['/ip4/127.0.0.1/tcp/0']
  },
  transports: [tcp()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()],
  services: {
    identify: identify()
  }
})

// Register all three /peerbit/* protocols. Each handler:
//   1. reads one length-prefixed envelope off the stream,
//   2. decodes it with the REAL production codec (Message.from), proving the
//      js side decodes the rust peerbit_wire envelope identically,
//   3. re-serializes via `.bytes()` and echoes the byte-identical envelope,
//      proving js's production codec round-trips rust's bytes.
for (const protocol of PEERBIT_PROTOCOLS) {
  await node.handle(
    protocol,
    async (stream, _connection) => {
      try {
        const envelope = await readOneFrame(stream)
        const list = new Uint8ArrayList(envelope)

        // Production decode: dispatch on the leading variant tag.
        const message = Message.from(list)
        const variant = envelope[0]
        console.log(
          `js: [${protocol}] decoded variant=${variant} id=${Buffer.from(message.header.id).toString('hex').slice(0, 16)}...`
        )

        // Production re-serialize. `.bytes()` may return a Uint8ArrayList.
        const reBytes = message.bytes()
        const reEnvelope =
          reBytes instanceof Uint8Array ? reBytes : reBytes.subarray()

        // Sanity: the production round-trip must reproduce the exact bytes rust
        // sent. If not, mixed-fleet interop would be broken — fail loudly.
        const identical =
          reEnvelope.length === envelope.length &&
          reEnvelope.every((b, i) => b === envelope[i])
        if (!identical) {
          console.log(
            `js: [${protocol}] PARITY FAIL — re-encode differs from received`
          )
          await stream.close()
          return
        }

        stream.send(frameEnvelope(reEnvelope))
        console.log(`js: [${protocol}] echoed ${reEnvelope.length}-byte envelope`)
        await stream.close()
      } catch (err) {
        console.log(`js: [${protocol}] handler error: ${err?.message ?? err}`)
        console.log(err?.stack ?? '')
      }
    },
    { runOnLimitedConnection: false }
  )
}

await node.start()

const peerId = node.peerId.toString()
const addr = node
  .getMultiaddrs()
  .map((m) => m.toString())
  .find((m) => m.includes('/tcp/'))

console.log(`js-libp2p node up. peerId=${peerId}`)
console.log(`registered protocols=${PEERBIT_PROTOCOLS.join(', ')}`)
console.log(`DIAL_ME=${addr}`)
console.log('waiting for a rust peer to dial...')

process.on('SIGINT', async () => {
  await node.stop()
  process.exit(0)
})
