
import assert from 'assert'
import EventEmitter from 'events'
import { IpfsPubsubPeerMonitor } from '..'

const peers = ['A', 'B', 'C', 'D', 'E']
const topic = 'tests'

const mockPubsub = {
  peers: () => Promise.resolve(peers)
}

describe('peer monitor', () => {
  it('finds peers', async () => {
    const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, { pollInterval: 100 })
    const newPeers = await m.getPeers()
    assert.deepEqual(newPeers, peers)
  })

  it('emits \'join\' event for each peer', async () => {
    const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, { pollInterval: 10 })
    await new Promise((resolve, reject) => {
      let count = 0
      let res = []
      m.on('join', peer => {
        try {
          count++
          res.push(peer)
          assert.equal(count <= peers.length, true)
          if (count === peers.length) {
            assert.deepEqual(res, peers)
            resolve(true)
          }
        } catch (e) {
          reject(e)
        }
      })
    })
  })

  it('emits joins', async () => {
    const ee = new EventEmitter()
    await new Promise((resolve, reject) => {
      let res = []

      const done = () => {
        expect(res.length).toEqual(2)
        expect(res[0]).toEqual('C')
        expect(res[1]).toEqual('D')
        resolve(true)
      }

      ee.on('join', peer => {
        res.push(peer)
        if (res.length === 2)
          done()
      })

      IpfsPubsubPeerMonitor._emitJoinsAndLeaves(new Set(['A', 'B']), new Set(['A', 'B', 'C', 'D']), ee)
    })
  })

  it('emits leaves', async () => {
    const ee = new EventEmitter()
    await new Promise((resolve, reject) => {
      let res = []

      const done = () => {
        expect(res.length).toEqual(2)
        expect(res[0]).toEqual('A')
        expect(res[1]).toEqual('B')
        resolve(true)
      }

      ee.on('leave', peer => {
        res.push(peer)
        if (res.length === 2)
          done()
      })

      IpfsPubsubPeerMonitor._emitJoinsAndLeaves(new Set(['A', 'B']), new Set(), ee)
    })
  })

  it('emits nothing', async () => {
    const ee = new EventEmitter()
    await new Promise((resolve, reject) => {
      ee.on('join', peer => expect(peer).toEqual(null))
      ee.on('leave', peer => expect(peer).toEqual(null))
      IpfsPubsubPeerMonitor._emitJoinsAndLeaves(new Set(['A', 'B']), new Set(['A', 'B']), ee)
      resolve(true)
    })
  })
})
