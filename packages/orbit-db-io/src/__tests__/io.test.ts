/* eslint-env mocha */
import { strict as assert } from 'assert'
import io from '../index'

// Test utils
import {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} from 'orbit-db-test-utils'

Object.keys(testAPIs).forEach((IPFS) => {
  describe(`IO tests (${IPFS})`, function () {
    jest.setTimeout(10000)

    let ipfs, ipfsd

    beforeAll(async () => {
      ipfsd = await startIpfs(IPFS, config)
      ipfs = ipfsd.api
    })

    afterAll(async () => {
      await stopIpfs(ipfsd)
    })

    describe('dag-cbor', () => {
      let cid1, cid2
      const data = { test: 'object' }

      it('writes', async () => {
        cid1 = await io.write(ipfs, 'dag-cbor', data, { pin: true })
        expect(cid1).toEqual('zdpuAwHevBbd7V9QXeP8zC1pdb3HmugJ7zgzKnyiWxJG3p2Y4')

        let obj = await io.read(ipfs, cid1, {})
        assert.deepStrictEqual(obj, data)

        data[cid1] = cid1
        cid2 = await io.write(ipfs, 'dag-cbor', data, { links: [cid1] })
        expect(cid2).toEqual('zdpuAqeyAtvp1ACxnWZLPW9qMEN5rJCD9N3vjUbMs4AAodTdz')

        obj = await io.read(ipfs, cid2, { links: [cid1] })
        data[cid1] = cid1
        assert.deepStrictEqual(obj, data)
      })
    })

    describe('dag-pb', () => {
      let cid
      const data = { test: 'object' }

      it('writes', async () => {
        cid = await io.write(ipfs, 'dag-pb', data, { pin: true })
        expect(cid).toEqual('QmaPXy3wcj4ds9baLreBGWf94zzwAUM41AiNG1eN51C9uM')

        const obj = await io.read(ipfs, cid, {})
        assert.deepStrictEqual(obj, data)
      })
    })

    describe('raw', () => {
      let cid
      const data = { test: 'object' }

      it('writes', async () => {
        cid = await io.write(ipfs, 'raw', data, { pin: true })
        expect(cid).toEqual('zdpuAwHevBbd7V9QXeP8zC1pdb3HmugJ7zgzKnyiWxJG3p2Y4')

        cid = await io.write(ipfs, 'raw', data, { format: 'dag-pb' })
        expect(cid).toEqual('QmaPXy3wcj4ds9baLreBGWf94zzwAUM41AiNG1eN51C9uM')

        const obj = await io.read(ipfs, cid, {})
        assert.deepStrictEqual(obj, data)
      })
    })
  })
})
