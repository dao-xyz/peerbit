const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Log } from '../log'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
const Keystore = require('orbit-db-keystore')

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Append (' + IPFS + ')', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore, signingKeystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      testIdentity = await Identities.createIdentity({ id: 'userA', keystore, signingKeystore })
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
      let log

      beforeEach(async () => {
        log = new Log(ipfs, testIdentity, { logId: 'A' })
        await log.append('hello1')
      })

      test('added the correct amount of items', () => {
        assert.strictEqual(log.length, 1)
      })

      test('added the correct values', async () => {
        log.values.forEach((entry) => {
          assert.strictEqual(entry.payload, 'hello1')
        })
      })

      test('added the correct amount of next pointers', async () => {
        log.values.forEach((entry) => {
          assert.strictEqual(entry.next.length, 0)
        })
      })

      test('has the correct heads', async () => {
        log.heads.forEach((head) => {
          assert.strictEqual(head.hash, log.values[0].hash)
        })
      })

      test('updated the clocks correctly', async () => {
        log.values.forEach((entry) => {
          assert.strictEqual(entry.clock.id, testIdentity.publicKey)
          assert.strictEqual(entry.clock.time, 1)
        })
      })
    })

    describe('append 100 items to a log', () => {
      const amount = 100
      const nextPointerAmount = 64

      let log

      beforeAll(async () => {
        log = new Log(ipfs, testIdentity, { logId: 'A' })
        for (let i = 0; i < amount; i++) {
          await log.append('hello' + i, nextPointerAmount)
          // Make sure the log has the right heads after each append
          const values = log.values
          assert.strictEqual(log.heads.length, 1)
          assert.strictEqual(log.heads[0].hash, values[values.length - 1].hash)
        }
      })

      test('added the correct amount of items', () => {
        assert.strictEqual(log.length, amount)
      })

      test('added the correct values', async () => {
        log.values.forEach((entry, index) => {
          assert.strictEqual(entry.payload, 'hello' + index)
        })
      })

      test('updated the clocks correctly', async () => {
        log.values.forEach((entry, index) => {
          assert.strictEqual(entry.clock.time, index + 1)
          assert.strictEqual(entry.clock.id, testIdentity.publicKey)
        })
      })

      test('added the correct amount of refs pointers', async () => {
        log.values.forEach((entry, index) => {
          assert.strictEqual(entry.refs.length, index > 0 ? Math.ceil(Math.log2(Math.min(nextPointerAmount, index))) : 0)
        })
      })
    })
  })
})
