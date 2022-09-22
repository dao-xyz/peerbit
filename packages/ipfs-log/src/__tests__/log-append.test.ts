const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Log } from '../log'
import { assertPayload } from './utils/assert'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, signKey: SignKeyWithMeta

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Append', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config
    let keystore: Keystore, signingKeystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      signKey = await keystore.createKey(new Uint8Array([0]), SignKeyWithMeta);
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api

    })

    afterAll(async () => {
      await stopIpfs(ipfsd)
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)

      await keystore?.close()
      await signingKeystore?.close()
    })

    describe('append one', () => {
      let log: Log<string>

      beforeEach(async () => {
        log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
        await log.append('hello1')
      })

      it('added the correct amount of items', () => {
        assert.strictEqual(log.length, 1)
      })

      it('added the correct values', async () => {
        log.values.forEach((entry) => {
          assertPayload(entry.payload.value, 'hello1')
        })
      })

      it('added the correct amount of next pointers', async () => {
        log.values.forEach((entry) => {
          assert.strictEqual(entry.next.length, 0)
        })
      })

      it('has the correct heads', async () => {
        log.heads.forEach((head) => {
          assert.strictEqual(head.hash, log.values[0].hash)
        })
      })

      it('updated the clocks correctly', async () => {
        log.values.forEach((entry) => {
          assert.deepStrictEqual(entry.clock.id, new Uint8Array(signKey.publicKey.getBuffer()))
          assert.strictEqual(entry.clock.time, 1)
        })
      })
    })

    describe('append 100 items to a log', () => {
      const amount = 100
      const nextPointerAmount = 64

      let log: Log<string>

      beforeAll(async () => {
        log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
        for (let i = 0; i < amount; i++) {
          await log.append('hello' + i, { pin: false, refs: log.getPow2Refs(nextPointerAmount) })
          // Make sure the log has the right heads after each append
          const values = log.values
          assert.strictEqual(log.heads.length, 1)
          assert.strictEqual(log.heads[0].hash, values[values.length - 1].hash)
        }
      })

      it('added the correct amount of items', () => {
        assert.strictEqual(log.length, amount)
      })

      it('added the correct values', async () => {
        log.values.forEach((entry, index) => {
          assertPayload(entry.payload.value, 'hello' + index)
        })
      })

      it('updated the clocks correctly', async () => {
        log.values.forEach((entry, index) => {
          assert.strictEqual(entry.clock.time, index + 1)
          assert.deepStrictEqual(entry.clock.id, new Uint8Array(signKey.publicKey.getBuffer()))
        })
      })

      it('added the correct amount of refs pointers', async () => {
        log.values.forEach((entry, index) => {
          assert.strictEqual(entry.refs.length, index > 0 ? Math.ceil(Math.log2(Math.min(nextPointerAmount, index))) : 0)
        })
      })
    })
  })
})
