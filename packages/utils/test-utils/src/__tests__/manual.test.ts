
import { jest } from '@jest/globals'
import { startIpfs, getIpfsPeerId, stopIpfs, waitForPeers, connectPeers, testAPIs } from '../index.js'
import { Controller } from 'ipfsd-ctl'

describe('Manual Workflow', function () {
  Object.keys(testAPIs).forEach((api) => {
    describe(`Success: Start and stop ${api}`, function () {
      jest.setTimeout(10000)

      let ipfsd1: Controller, ipfsd2: Controller

      it('starts and stops two connected nodes', async () => {
        const topic = 'test-topic'

        ipfsd1 = await startIpfs(api as any)
        ipfsd2 = await startIpfs(api as any)

        const id1 = await getIpfsPeerId(ipfsd1.api)
        const id2 = await getIpfsPeerId(ipfsd2.api)
        expect(id1).not.toEqual(id2)

        await connectPeers(ipfsd1.api, ipfsd2.api)
        expect((await ipfsd1.api.swarm.peers()).length).toEqual(1)
        expect((await ipfsd2.api.swarm.peers()).length).toEqual(1)

        await ipfsd1.api.pubsub.subscribe(topic, () => { })
        await ipfsd2.api.pubsub.subscribe(topic, () => { })

        await waitForPeers(ipfsd1.api, [id2], topic)
      })

      afterEach(async () => {
        await stopIpfs(ipfsd1)
        await stopIpfs(ipfsd2)
      })
    })
  })

  describe('Errors', function () {
    it('startIpfs throws error if wrong api type passed', async () => {
      let ipfsd!: Controller

      try {
        ipfsd = await startIpfs('xxx' as any)
      } catch (e: any) {
        expect((e as any).message).toStartWith('Wanted API type "xxx" is unknown. Available types')
        await stopIpfs(ipfsd)
      }
    })
  })
})
