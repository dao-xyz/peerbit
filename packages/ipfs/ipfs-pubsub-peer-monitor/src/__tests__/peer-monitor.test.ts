
import assert, { equal } from 'assert'
import EventEmitter from 'events'
import { IpfsPubsubPeerMonitor } from '../index.js'
import { waitFor } from '@dao-xyz/time';
import type { PeerId } from '@libp2p/interface-peer-id';

const peers = ['A', 'B', 'C', 'D', 'E']
const topic = 'tests'

const mockPeerId = (p: string) => {
  return {
    equals: (other: PeerId | string) => other.toString() === p,
    toString: () => p
  } as PeerId
}
const mockPubsub = {
  peers: () => Promise.resolve(peers.map(mockPeerId))
}

describe('peer monitor', () => {
  it('finds peers', async () => {
    const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, {}, { pollInterval: 100 })
    const newPeers = await m.getPeers()
    assert.deepEqual(newPeers.map(p => p.toString()), peers)
  })

  it('emits \'join\' event for each peer', async () => {
    let resolved = false;
    let count = 0
    let res: PeerId[] = []
    const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, {
      onJoin: (peer) => {
        count++
        res.push(peer)
        expect(count <= peers.length).toEqual(true)
        if (count === peers.length) {
          resolved = true
        }
      }
    }, { pollInterval: 10 })

    await waitFor((() => resolved))
    expect(res.map(p => p.toString())).toEqual(peers)

  })

  it('emits joins', async () => {
    const ee = new EventEmitter()
    await new Promise((resolve, reject) => {
      let res: PeerId[] = []

      const done = () => {
        expect(res.length).toEqual(2)
        expect(res[0].toString()).toEqual('C')
        expect(res[1].toString()).toEqual('D')
        resolve(true)
      }
      IpfsPubsubPeerMonitor._emitJoinsAndLeaves(['A', 'B'].map(mockPeerId), ['A', 'B', 'C', 'D'].map(mockPeerId), {
        onJoin: (peer) => {
          res.push(peer)
          if (res.length === 2)
            done()
        }
      })
    })
  })

  it('emits leaves', async () => {
    const ee = new EventEmitter()
    await new Promise((resolve, reject) => {
      let res: PeerId[] = []

      const done = () => {
        expect(res.length).toEqual(2)
        expect(res[0].toString()).toEqual('A')
        expect(res[1].toString()).toEqual('B')
        resolve(true)
      }

      ee.on('leave', peer => {
        res.push(peer)
        if (res.length === 2)
          done()
      })

      IpfsPubsubPeerMonitor._emitJoinsAndLeaves(['A', 'B'].map(mockPeerId), [], {
        onLeave: (peer) => {
          res.push(peer)
          if (res.length === 2)
            done()
        }
      })
    })
  })
})
