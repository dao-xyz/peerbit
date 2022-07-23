import { EntryIO } from "../entry-io"
import { Log } from "../log"

const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
const Keystore = require('orbit-db-keystore')
import { Identities } from '@dao-xyz/orbit-db-identity-provider'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity, testIdentity2, testIdentity3, testIdentity4

const last = arr => arr[arr.length - 1]

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Entry - Persistency (' + IPFS + ')', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let options, keystore, signingKeystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)
      const defaultOptions = { identityKeysPath, signingKeysPath }

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      const users = ['userA', 'userB', 'userC', 'userD']
      options = users.map((user) => {
        return Object.assign({}, defaultOptions, { id: user, keystore, signingKeystore })
      })

      testIdentity = await Identities.createIdentity(options[0])
      testIdentity2 = await Identities.createIdentity(options[1])
      testIdentity3 = await Identities.createIdentity(options[2])
      testIdentity4 = await Identities.createIdentity(options[3])
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

    it('log with one entry', async () => {
      const log = new Log(ipfs, testIdentity, { logId: 'X' })
      await log.append('one')
      const hash = log.values[0].hash
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 1 })
      assert.strictEqual(res.length, 1)
    })

    it('log with 2 entries', async () => {
      const log = new Log(ipfs, testIdentity, { logId: 'X' })
      await log.append('one')
      await log.append('two')
      const hash = last(log.values).hash
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 2 })
      assert.strictEqual(res.length, 2)
    })

    it('loads max 1 entry from a log of 2 entry', async () => {
      const log = new Log(ipfs, testIdentity, { logId: 'X' })
      await log.append('one')
      await log.append('two')
      const hash = last(log.values).hash
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 1 })
      assert.strictEqual(res.length, 1)
    })

    it('log with 100 entries', async () => {
      const count = 100
      const log = new Log(ipfs, testIdentity, { logId: 'X' })
      for (let i = 0; i < count; i++) {
        await log.append('hello' + i)
      }
      const hash = await log.toMultihash()
      const result = await Log.fromMultihash(ipfs, testIdentity, hash, {})
      assert.strictEqual(result.length, count)
    })

    it('load only 42 entries from a log with 100 entries', async () => {
      const count = 100
      const log = new Log(ipfs, testIdentity, { logId: 'X' })
      let log2 = new Log(ipfs, testIdentity, { logId: 'X' })
      for (let i = 1; i <= count; i++) {
        await log.append('hello' + i)
        if (i % 10 === 0) {
          log2 = new Log(ipfs, testIdentity,
            { logId: log2.id, entries: log2.values, heads: log2.heads.concat(log.heads) })
          await log2.append('hi' + i)
        }
      }

      const hash = await log.toMultihash()
      const result = await Log.fromMultihash(ipfs, testIdentity, hash, { length: 42 })
      assert.strictEqual(result.length, 42)
    })

    it('load only 99 entries from a log with 100 entries', async () => {
      const count = 100
      const log = new Log(ipfs, testIdentity, { logId: 'X' })
      let log2 = new Log(ipfs, testIdentity, { logId: 'X' })
      for (let i = 1; i <= count; i++) {
        await log.append('hello' + i)
        if (i % 10 === 0) {
          log2 = new Log(ipfs, testIdentity, { logId: log2.id, entries: log2.values })
          await log2.append('hi' + i)
          await log2.join(log)
        }
      }

      const hash = await log2.toMultihash()
      const result = await Log.fromMultihash(ipfs, testIdentity, hash, { length: 99 })
      assert.strictEqual(result.length, 99)
    })

    it('load only 10 entries from a log with 100 entries', async () => {
      const count = 100
      const log = new Log(ipfs, testIdentity, { logId: 'X' })
      let log2 = new Log(ipfs, testIdentity, { logId: 'X' })
      let log3 = new Log(ipfs, testIdentity, { logId: 'X' })
      for (let i = 1; i <= count; i++) {
        await log.append('hello' + i)
        if (i % 10 === 0) {
          log2 = new Log(ipfs, testIdentity,
            { logId: log2.id, entries: log2.values, heads: log2.heads })
          await log2.append('hi' + i)
          await log2.join(log)
        }
        if (i % 25 === 0) {
          log3 = new Log(ipfs, testIdentity,
            { logId: log3.id, entries: log3.values, heads: log3.heads.concat(log2.heads) })
          await log3.append('--' + i)
        }
      }

      await log3.join(log2)
      const hash = await log3.toMultihash()
      const result = await Log.fromMultihash(ipfs, testIdentity, hash, { length: 10 })
      assert.strictEqual(result.length, 10)
    })

    it('load only 10 entries and then expand to max from a log with 100 entries', async () => {
      const count = 30

      const log = new Log(ipfs, testIdentity, { logId: 'X' })
      const log2 = new Log(ipfs, testIdentity2, { logId: 'X' })
      let log3 = new Log(ipfs, testIdentity3, { logId: 'X' })
      for (let i = 1; i <= count; i++) {
        await log.append('hello' + i)
        if (i % 10 === 0) {
          await log2.append('hi' + i)
          await log2.join(log)
        }
        if (i % 25 === 0) {
          log3 = new Log(ipfs, testIdentity3,
            { logId: log3.id, entries: log3.values, heads: log3.heads.concat(log2.heads) })
          await log3.append('--' + i)
        }
      }

      await log3.join(log2)

      const log4 = new Log(ipfs, testIdentity4, { logId: 'X' })
      await log4.join(log2)
      await log4.join(log3)

      const values3 = log3.values.map((e) => e.data.payload)
      const values4 = log4.values.map((e) => e.data.payload)

      assert.deepStrictEqual(values3, values4)
    })
  })
})
