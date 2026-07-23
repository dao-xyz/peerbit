// Shared helpers for the RELAY A/B harness (RELAY-PROFILING.md).
//
// Both the js relay (relay-js.mjs) and the source/dest circuit-relay CLIENTS
// (relay-bench.mjs) are js-libp2p 3.3.4 nodes configured EXACTLY like a Peerbit
// node peer's transport stack:
//   - transports:            TCP + circuitRelayTransport(client)
//                            (clients/peerbit/src/transports.ts:10-18)
//   - connectionEncrypters:  noise
//   - streamMuxers:          yamux
//   - identity:              Ed25519 (Peerbit hard-requires it)
//   - services:              identify (+ relay server, only on the js relay)
// The relay-SERVER service mirrors clients/peerbit/src/transports.ts:19-23:
//   circuitRelayServer({ reservations: { applyDefaultLimit: false,
//                                        maxReservations: 1000 } })
//
// This is the SAME js config the native rust relay is A/B'd against, so the
// only variable between Path A and Path B is which process forwards the bytes.

import { createLibp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { identify } from '@libp2p/identify'
import {
  circuitRelayServer,
  circuitRelayTransport
} from '@libp2p/circuit-relay-v2'
import { generateKeyPair } from '@libp2p/crypto/keys'

// The custom byte-streaming protocol the source/dest peers use over a relayed
// circuit. It is NOT a /peerbit/* protocol — the RELAY never sees these bytes
// decoded; it only pipes them socket→socket. A dedicated id keeps the harness
// stream separate from identify/relay control streams.
export const BENCH_PROTOCOL = '/relay-bench/throughput/1.0.0'

/**
 * Build a js-libp2p circuit-relay CLIENT node (Peerbit node transport spec).
 * Used for BOTH the source and the destination peers in the A/B.
 *
 * `runOnLimitedConnection` matters: a relayed (circuit) connection is a
 * "limited" connection in js-libp2p; Peerbit mounts its /peerbit/* handlers
 * with `runOnLimitedConnection:false` but ALSO dials through relays, so for the
 * bench we register the throughput handler with runOnLimitedConnection:true so
 * it runs over the relayed circuit (that is the whole point — measure bytes
 * over the relay).
 */
export async function createClientNode () {
  const privateKey = await generateKeyPair('Ed25519')
  const node = await createLibp2p({
    privateKey,
    addresses: {
      // /p2p-circuit in the listen set makes this node reserve on any relay it
      // dials and advertise its relayed address — exactly Peerbit's
      // clients/peerbit/src/transports.ts:25-30 listen set (minus ws/webrtc).
      listen: ['/ip4/127.0.0.1/tcp/0', '/p2p-circuit']
    },
    transports: [tcp(), circuitRelayTransport({ reservationCompletionTimeout: 5000 })],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      identify: identify()
    },
    connectionManager: {
      // The sweep opens many concurrent connections; do not let the default
      // connection-manager prune them mid-measurement.
      maxConnections: 4096
    }
  })
  return node
}

/**
 * Build the js-libp2p RELAY node (Path A) — Peerbit's exact relay config.
 * circuitRelayServer with applyDefaultLimit:false + maxReservations:1000.
 */
export async function createJsRelayNode () {
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
      identify: identify(),
      // clients/peerbit/src/transports.ts:19-23 — the Peerbit node relay.
      relay: circuitRelayServer({
        reservations: { applyDefaultLimit: false, maxReservations: 1000 }
      })
    },
    connectionManager: {
      maxConnections: 4096
    }
  })
  return node
}

/** The relay's dial-through TCP multiaddr (with /p2p/<peerId>). */
export function relayDialAddr (relayNode) {
  const peerId = relayNode.peerId.toString()
  const addr = relayNode
    .getMultiaddrs()
    .map((m) => m.toString())
    .find((m) => m.includes('/tcp/') && !m.includes('/p2p-circuit'))
  return addr ?? `/ip4/127.0.0.1/tcp/0/p2p/${peerId}`
}
