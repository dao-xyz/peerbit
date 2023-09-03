import { multiaddr } from '@multiformats/multiaddr'
import { abortableSource } from 'abortable-iterator'
import { duplexPair } from 'it-pair/duplex'
import type { MultiaddrConnection } from '@libp2p/interface/connection'
import type { PeerId } from '@libp2p/interface/peer-id'
import type { Multiaddr } from '@multiformats/multiaddr'
import type { Duplex } from 'it-stream-types'

export function mockMultiaddrConnection(source: Duplex<AsyncGenerator<Uint8Array>> & Partial<MultiaddrConnection>, peerId: PeerId): MultiaddrConnection {
    const maConn: MultiaddrConnection = {
        async close() {

        },
        abort: () => { },
        timeline: {
            open: Date.now()
        },
        remoteAddr: multiaddr(`/ip4/127.0.0.1/tcp/4001/p2p/${peerId.toString()}`),
        ...source
    }

    return maConn
}

export interface MockMultiaddrConnPairOptions {
    addrs: Multiaddr[]
    remotePeer: PeerId
}

/**
 * Returns both sides of a mocked MultiaddrConnection
 */
export function mockMultiaddrConnPair(opts: MockMultiaddrConnPairOptions): { inbound: MultiaddrConnection, outbound: MultiaddrConnection } {
    const { addrs, remotePeer } = opts
    const controller = new AbortController()
    const [localAddr, remoteAddr] = addrs
    const [inboundStream, outboundStream] = duplexPair<Uint8Array>()

    const outbound: MultiaddrConnection = {
        ...outboundStream,
        remoteAddr: remoteAddr.toString().includes(`/p2p/${remotePeer.toString()}`) ? remoteAddr : remoteAddr.encapsulate(`/p2p/${remotePeer.toString()}`),
        timeline: {
            open: Date.now()
        },
        close: async () => {
            outbound.timeline.close = Date.now()
            controller.abort()
        },
        abort: (err: Error) => {
            outbound.timeline.close = Date.now()
            controller.abort(err)
        }
    }

    const inbound: MultiaddrConnection = {
        ...inboundStream,
        remoteAddr: localAddr,
        timeline: {
            open: Date.now()
        },
        close: async () => {
            inbound.timeline.close = Date.now()
            controller.abort()
        },
        abort: (err: Error) => {
            outbound.timeline.close = Date.now()
            controller.abort(err)
        }
    }

    // Make the sources abortable so we can close them easily
    inbound.source = abortableSource(inbound.source, controller.signal)
    outbound.source = abortableSource(outbound.source, controller.signal)

    return { inbound, outbound }
}

import type { ConnectionGater } from '@libp2p/interface/connection-gater'

export function mockConnectionGater(): ConnectionGater {
    return {
        denyDialPeer: async () => Promise.resolve(false),
        denyDialMultiaddr: async () => Promise.resolve(false),
        denyInboundConnection: async () => Promise.resolve(false),
        denyOutboundConnection: async () => Promise.resolve(false),
        denyInboundEncryptedConnection: async () => Promise.resolve(false),
        denyOutboundEncryptedConnection: async () => Promise.resolve(false),
        denyInboundUpgradedConnection: async () => Promise.resolve(false),
        denyOutboundUpgradedConnection: async () => Promise.resolve(false),
        denyInboundRelayReservation: async () => Promise.resolve(false),
        denyOutboundRelayedConnection: async () => Promise.resolve(false),
        denyInboundRelayedConnection: async () => Promise.resolve(false),
        filterMultiaddrForPeer: async () => Promise.resolve(true)
    }
}


