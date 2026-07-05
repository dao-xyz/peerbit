// GOAL 2 counterpart: a standalone js-libp2p listener configured like a
// Peerbit NODE peer, so a rust-libp2p peer can dial it and open a stream.
//
// Matches the Peerbit node transport spec from the config research:
//   - transport: TCP (node peers also do webSockets + circuit-relay; TCP is the
//     rust<->js interop path so we listen on TCP here)
//   - connectionEncrypters: noise    (@chainsafe/libp2p-noise)
//   - streamMuxers:         yamux    (@chainsafe/libp2p-yamux)
//   - identity:             Ed25519  (Peerbit HARD-REQUIRES ed25519)
//   - a stream handler on /peerbit/direct-stream/2.0.0 that reads one
//     it-length-prefixed frame and echoes it back, mirroring the Rust side.
//
// Versions pinned to what packages/transport/stream/package.json declares:
//   libp2p ^3.1.7, @chainsafe/libp2p-noise ^17, @chainsafe/libp2p-yamux ^8,
//   @libp2p/tcp ^11, @libp2p/identify, @libp2p/crypto ^5.
//
// Install + run:
//   cd spikes/rust-libp2p-poc/js && npm install && node listener.mjs
// It prints:  DIAL_ME=/ip4/127.0.0.1/tcp/<port>/p2p/<peerId>

import { createLibp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import { generateKeyPair } from '@libp2p/crypto/keys'

const PEERBIT_PROTOCOL = '/peerbit/direct-stream/2.0.0'
const DATA_MESSAGE_TAG = 0

// NOTE: libp2p v3 replaced the old `it-stream` { source, sink } duplex with a
// MessageStream (AsyncIterable + .send()). `it-length-prefixed-stream` v2 still
// expects `.sink`, so we do the unsigned-varint framing by hand here — the same
// framing the Rust side uses — to prove byte-for-byte wire compatibility.

/** Read one unsigned-varint-length-prefixed frame off a v3 MessageStream. */
async function readOneFrame (stream) {
  let buf = new Uint8Array(0)
  const append = (chunk) => {
    // chunk may be a Uint8Array or a Uint8ArrayList; normalise to bytes.
    const bytes = chunk instanceof Uint8Array ? chunk : chunk.subarray()
    const next = new Uint8Array(buf.length + bytes.length)
    next.set(buf, 0)
    next.set(bytes, buf.length)
    buf = next
  }
  const tryDecode = () => {
    // Decode the varint length; return {len, headerLen} or null if incomplete.
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

/** Frame a payload: [varint(len)] [DATA_MESSAGE_TAG] [payload]. */
function frame (payloadStr) {
  const payload = Buffer.from(payloadStr, 'utf8')
  const body = new Uint8Array(payload.length + 1)
  body[0] = DATA_MESSAGE_TAG
  body.set(payload, 1)
  // encode unsigned-varint length
  const header = []
  let n = body.length
  do {
    let b = n & 0x7f
    n >>>= 7
    if (n !== 0) b |= 0x80
    header.push(b)
  } while (n !== 0)
  const out = new Uint8Array(header.length + body.length)
  out.set(header, 0)
  out.set(body, header.length)
  return out
}

// Ed25519 identity — same key type Peerbit derives its peerId from.
const privateKey = await generateKeyPair('Ed25519')

const node = await createLibp2p({
  privateKey,
  addresses: {
    // Peerbit node listens on /ip4/127.0.0.1/tcp/0 (+ /ws + /p2p-circuit).
    listen: ['/ip4/127.0.0.1/tcp/0']
  },
  transports: [tcp()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()],
  services: {
    identify: identify()
  }
})

// Register the Peerbit-style protocol. runOnLimitedConnection:false mirrors
// Peerbit's registrar options for all /peerbit/* streams.
await node.handle(
  PEERBIT_PROTOCOL,
  // libp2p v3 stream-handler signature is positional: (stream, connection).
  // (v2 passed a single { stream, connection } object — a breaking change.)
  async (stream, _connection) => {
    try {
      console.log('js: inbound Peerbit stream negotiated, reading frame...')
      const body = await readOneFrame(stream)
      const tag = body[0]
      const payload = Buffer.from(body.subarray(1)).toString('utf8')
      console.log(`js: received frame tag=${tag} payload=${payload}`)

      // Echo back with the same varint framing so the rust peer confirms a round-trip.
      stream.send(frame(`echo:${payload}`))
      console.log(`js: sent reply echo:${payload}`)
      await stream.close()
    } catch (err) {
      console.log(`js: handler error: ${err?.message ?? err}`)
      console.log(err?.stack ?? '')
    }
  },
  { runOnLimitedConnection: false }
)

await node.start()

const peerId = node.peerId.toString()
const addr = node
  .getMultiaddrs()
  .map((m) => m.toString())
  .find((m) => m.includes('/tcp/'))

console.log(`js-libp2p node up. peerId=${peerId}`)
console.log(`registered protocol=${PEERBIT_PROTOCOL}`)
console.log(`DIAL_ME=${addr}`)
console.log('waiting for a rust peer to dial...')

process.on('SIGINT', async () => {
  await node.stop()
  process.exit(0)
})
