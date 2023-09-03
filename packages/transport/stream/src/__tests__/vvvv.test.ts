import { yamux } from '@chainsafe/libp2p-yamux'
import { EventEmitter } from '@libp2p/interface/events'
import { mplex } from '@libp2p/mplex'
import { createEd25519PeerId } from '@libp2p/peer-id-factory'
import { PersistentPeerStore } from '@libp2p/peer-store'
import { webSockets } from '@libp2p/websockets'
import * as filters from '@libp2p/websockets/filters'
import { multiaddr } from '@multiformats/multiaddr'
import { MemoryDatastore } from 'datastore-core'
import delay from 'delay'
import all from 'it-all'
import type { StubbedInstance } from 'sinon-ts'
import { Components, defaultComponents } from '/Users/admin/git/web3-orbit/node_modules/libp2p/dist/src/components'
import { stubInterface } from 'sinon-ts'
import drain from 'it-drain'
import { pipe } from 'it-pipe'
import sinon from 'sinon'
import { fromString as uint8ArrayFromString } from 'uint8arrays/from-string'

import { plaintext } from 'libp2p/insecure'
import { DefaultUpgrader } from '/Users/admin/git/web3-orbit/node_modules/libp2p/dist/src/upgrader'
import type { Libp2p } from '@libp2p/interface'
import type { Connection, ConnectionProtector, Stream } from '@libp2p/interface/connection'
import type { ConnectionEncrypter, SecuredConnection } from '@libp2p/interface/connection-encrypter'
import type { PeerId } from '@libp2p/interface/peer-id'
import type { StreamMuxer, StreamMuxerFactory, StreamMuxerInit } from '@libp2p/interface/stream-muxer'
import type { Upgrader } from '@libp2p/interface/transport'
import { mockConnectionGater, mockMultiaddrConnPair } from './m1'
import { mockConnectionManager } from './m2'
import { mockRegistrar } from './m4'

const addrs = [
  multiaddr('/ip4/127.0.0.1/tcp/0'),
  multiaddr('/ip4/127.0.0.1/tcp/0')
]

describe('Upgrader', () => {
  let localUpgrader: Upgrader
  let localMuxerFactory: StreamMuxerFactory
  let localYamuxerFactory: StreamMuxerFactory
  let localConnectionEncrypter: ConnectionEncrypter
  let localConnectionProtector: StubbedInstance<ConnectionProtector>
  let remoteUpgrader: Upgrader
  let remoteMuxerFactory: StreamMuxerFactory
  let remoteYamuxerFactory: StreamMuxerFactory
  let remoteConnectionEncrypter: ConnectionEncrypter
  let remoteConnectionProtector: StubbedInstance<ConnectionProtector>
  let localPeer: PeerId
  let remotePeer: PeerId
  let localComponents: Components
  let remoteComponents: Components

  beforeEach(async () => {
    ([
      localPeer,
      remotePeer
    ] = await Promise.all([
      createEd25519PeerId(),
      createEd25519PeerId()
    ]))

    localConnectionProtector = stubInterface<ConnectionProtector>()
    localConnectionProtector.protect.resolvesArg(0)

    localComponents = defaultComponents({
      peerId: localPeer,
      connectionGater: mockConnectionGater(),
      registrar: mockRegistrar(),
      datastore: new MemoryDatastore(),
      connectionProtector: localConnectionProtector,
      events: new EventEmitter()
    })
    localComponents.peerStore = new PersistentPeerStore(localComponents)
    localComponents.connectionManager = mockConnectionManager(localComponents)
    localMuxerFactory = mplex()()
    localYamuxerFactory = yamux()()
    localConnectionEncrypter = plaintext()()
    localUpgrader = new DefaultUpgrader(localComponents, {
      connectionEncryption: [
        localConnectionEncrypter
      ],
      muxers: [
        localMuxerFactory,
        localYamuxerFactory
      ],
      inboundUpgradeTimeout: 1000
    })

    remoteConnectionProtector = stubInterface<ConnectionProtector>()
    remoteConnectionProtector.protect.resolvesArg(0)

    remoteComponents = defaultComponents({
      peerId: remotePeer,
      connectionGater: mockConnectionGater(),
      registrar: mockRegistrar(),
      datastore: new MemoryDatastore(),
      connectionProtector: remoteConnectionProtector,
      events: new EventEmitter()
    })
    remoteComponents.peerStore = new PersistentPeerStore(remoteComponents)
    remoteComponents.connectionManager = mockConnectionManager(remoteComponents)
    remoteMuxerFactory = mplex()()
    remoteYamuxerFactory = yamux()()
    remoteConnectionEncrypter = plaintext()()
    remoteUpgrader = new DefaultUpgrader(remoteComponents, {
      connectionEncryption: [
        remoteConnectionEncrypter
      ],
      muxers: [
        remoteMuxerFactory,
        remoteYamuxerFactory
      ],
      inboundUpgradeTimeout: 1000
    })

    await localComponents.registrar.handle('/echo/1.0.0', ({ stream }) => {
      void pipe(stream, stream)
    }, {
      maxInboundStreams: 10,
      maxOutboundStreams: 10
    })
    await remoteComponents.registrar.handle('/echo/1.0.0', ({ stream }) => {
      void pipe(stream, stream)
    }, {
      maxInboundStreams: 10,
      maxOutboundStreams: 10
    })
  })

  afterEach(() => {
    sinon.restore()
  })

  it('should upgrade with valid muxers and crypto', async () => {
    const { inbound, outbound } = mockMultiaddrConnPair({ addrs, remotePeer })

    const connections = await Promise.all([
      localUpgrader.upgradeOutbound(outbound),
      remoteUpgrader.upgradeInbound(inbound)
    ])

    expect(connections).toHaveLength(2)

    const stream = await connections[0].newStream('/echo/1.0.0')
    expect(stream['protocol']).toEqual('/echo/1.0.0')

    await new Promise<void>((resolve, reject) => {
      setTimeout(() => { resolve() }, 6000)
    })

    const hello = uint8ArrayFromString('hello there!')
    const result = await pipe(
      [hello],
      stream,
      function toBuffer(source) {
        return (async function* () {
          for await (const val of source) yield val.slice()
        })()
      },
      async (source) => all(source)
    )

    expect(result).toEqual([hello])
  })

})
