import assert from 'assert'
import { IpfsPubsubPeerMonitor } from '../index.js'
import { waitFor } from '@dao-xyz/time';

const peers = ['A', 'B', 'C']
const topic = 'tests'

const mockPubsub: any = {
  peers: () => Promise.resolve(peers)
}

describe('start and stop', () => {
  it('started property is immutable', async () => {
    const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, { onError: () => { }, onJoin: () => { }, onLeave: () => { } })
    let err
    try {
      m.started = true
    } catch (e) {
      err = e
    }
    expect(err.toString()).toEqual("Error: 'started' is read-only")
  })



  describe('poll loop', () => {
    it('starts polling peers', () => {
      const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, {})
      assert.notEqual(m, null)
      expect(m.started).toEqual(true)
    })

    it('doesn\'t start polling peers', () => {
      const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, {}, { start: false })
      assert.notEqual(m, null)
      expect(m.started).toEqual(false)
    })

    it('starts polling peers when started manually', () => {
      const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, {}, { start: false })
      m.start()
      expect(m.started).toEqual(true)
    })

    it('stops polling peers', () => {
      const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, {},)
      m.stop()
      expect(m.started).toEqual(false)
    })

    it('polls with the given interval', async () => {
      const interval = 100
      const margin = interval / 10
      let resolved = false;
      const startTime = new Date().getTime() - interval
      const m = new IpfsPubsubPeerMonitor(mockPubsub, topic, {
        onJoin: () => {
          const stopTime = new Date().getTime()
          const deltaTime = stopTime - startTime
          assert.equal(deltaTime >= interval, true)
          assert.equal(deltaTime < interval + margin, true, `Not within margin of ${margin} ms`)
          resolved = true;
        }
      }, { pollInterval: interval })
      // Substract the interval from the time since the interval also
      // fires immediately instead of waiting for the interval

      await waitFor(() => resolved);
    })
  })
})
