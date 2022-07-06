const assert = require('assert')
import * as io from '../index';
import { field } from '@dao-xyz/borsh';
// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

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

      test('writes', async () => {
        cid1 = await io.write(ipfs, 'dag-cbor', data, { pin: true })
        assert.strictEqual(cid1, 'zdpuAwHevBbd7V9QXeP8zC1pdb3HmugJ7zgzKnyiWxJG3p2Y4')
      })

      test('reads', async () => {
        const obj = await io.read(ipfs, cid1, {})
        assert.deepStrictEqual(obj, data)
      })

      test('writes with links', async () => {
        data[cid1] = cid1
        cid2 = await io.write(ipfs, 'dag-cbor', data, { links: [cid1] })
        assert.strictEqual(cid2, 'zdpuAqeyAtvp1ACxnWZLPW9qMEN5rJCD9N3vjUbMs4AAodTdz')
      })

      test('reads from links', async () => {
        const obj = await io.read(ipfs, cid2, { links: [cid1] })
        data[cid1] = cid1
        assert.deepStrictEqual(obj, data)
      })
    })

    describe('borsh', () => {
      let cid1, cid2

      class Struct {
        @field({ type: 'String' })
        test: string
      }
      const data = Object.assign(new Struct(), { test: 'object' });

      test('writes', async () => {
        cid1 = await io.write(ipfs, 'borsh', data, { pin: true })
        assert.strictEqual(cid1, 'zdpuAwHevBbd7V9QXeP8zC1pdb3HmugJ7zgzKnyiWxJG3p2Y4')
      })

      test('reads', async () => {
        const obj = await io.read(ipfs, cid1, {})
        assert.deepStrictEqual(obj, data)
      })

      test('writes with links', async () => {
        data[cid1] = cid1
        cid2 = await io.write(ipfs, 'borsh', data, { links: [cid1] })
        assert.strictEqual(cid2, 'zdpuAqeyAtvp1ACxnWZLPW9qMEN5rJCD9N3vjUbMs4AAodTdz')
      })

      test('reads from links', async () => {
        const obj = await io.read(ipfs, cid2, { links: [cid1] })
        data[cid1] = cid1
        assert.deepStrictEqual(obj, data)
      })
    })


    describe('dag-pb', () => {
      let cid
      const data = { test: 'object' }

      test('writes', async () => {
        cid = await io.write(ipfs, 'dag-pb', data, { pin: true })
        assert.strictEqual(cid, 'QmaPXy3wcj4ds9baLreBGWf94zzwAUM41AiNG1eN51C9uM')
      })

      test('reads', async () => {
        const obj = await io.read(ipfs, cid, {})
        assert.deepStrictEqual(obj, data)
      })
    })

    describe('raw', () => {
      let cid
      const data = { test: 'object' }

      test('writes', async () => {
        cid = await io.write(ipfs, 'raw', data, { pin: true })
        assert.strictEqual(cid, 'zdpuAwHevBbd7V9QXeP8zC1pdb3HmugJ7zgzKnyiWxJG3p2Y4')
      })

      test('writes formatted as dag-pb', async () => {
        cid = await io.write(ipfs, 'raw', data, { format: 'dag-pb' })
        assert.strictEqual(cid, 'QmaPXy3wcj4ds9baLreBGWf94zzwAUM41AiNG1eN51C9uM')
      })

      test('reads', async () => {
        const obj = await io.read(ipfs, cid, {})
        assert.deepStrictEqual(obj, data)
      })
    })
  })
})
