import { EntryIO } from "../entry-io"
import { Log } from "../log"
import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs
} from '@dao-xyz/orbit-db-test-utils'

let ipfsd, ipfs, signKey: SignKeyWithMeta, signKey2: SignKeyWithMeta, signKey3: SignKeyWithMeta, signKey4: SignKeyWithMeta

const last = arr => arr[arr.length - 1]

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Entry - Persistency', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let options, keystore: Keystore, signingKeystore: Keystore

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

      signKey = await keystore.createKey(new Uint8Array([0]), SignKeyWithMeta)
      signKey2 = await keystore.createKey(new Uint8Array([1]), SignKeyWithMeta)
      signKey3 = await keystore.createKey(new Uint8Array([2]), SignKeyWithMeta)
      signKey4 = await keystore.createKey(new Uint8Array([3]), SignKeyWithMeta)
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
      const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      await log.append('one')
      const hash = log.values[0].hash
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 1 })
      expect(res.length).toEqual(1)
    })

    it('log with 2 entries', async () => {
      const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      await log.append('one')
      await log.append('two')
      const hash = last(log.values).hash
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 2 })
      expect(res.length).toEqual(2)
    })

    it('loads max 1 entry from a log of 2 entry', async () => {
      const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      await log.append('one')
      await log.append('two')
      const hash = last(log.values).hash
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 1 })
      expect(res.length).toEqual(1)
    })

    it('log with 100 entries', async () => {
      const count = 100
      const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      for (let i = 0; i < count; i++) {
        await log.append('hello' + i)
      }
      const hash = await log.toMultihash()
      const result = await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, {})
      expect(result.length).toEqual(count)
    })

    it('load only 42 entries from a log with 100 entries', async () => {
      const count = 100
      const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      let log2 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      for (let i = 1; i <= count; i++) {
        await log.append('hello' + i)
        if (i % 10 === 0) {
          log2 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey),
            { logId: log2._id, entries: log2.values, heads: log2.heads.concat(log.heads) })
          await log2.append('hi' + i)
        }
      }

      const hash = await log.toMultihash()
      const result = await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: 42 })
      expect(result.length).toEqual(42)
    })

    it('load only 99 entries from a log with 100 entries', async () => {
      const count = 100
      const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      let log2 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      for (let i = 1; i <= count; i++) {
        await log.append('hello' + i)
        if (i % 10 === 0) {
          log2 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: log2._id, entries: log2.values })
          await log2.append('hi' + i)
          await log2.join(log)
        }
      }

      const hash = await log2.toMultihash()
      const result = await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: 99 })
      expect(result.length).toEqual(99)
    })

    it('load only 10 entries from a log with 100 entries', async () => {
      const count = 100
      const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      let log2 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      let log3 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      for (let i = 1; i <= count; i++) {
        await log.append('hello' + i)
        if (i % 10 === 0) {
          log2 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey),
            { logId: log2._id, entries: log2.values, heads: log2.heads })
          await log2.append('hi' + i)
          await log2.join(log)
        }
        if (i % 25 === 0) {
          log3 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey),
            { logId: log3._id, entries: log3.values, heads: log3.heads.concat(log2.heads) })
          await log3.append('--' + i)
        }
      }

      await log3.join(log2)
      const hash = await log3.toMultihash()
      const result = await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: 10 })
      expect(result.length).toEqual(10)
    })

    it('load only 10 entries and then expand to max from a log with 100 entries', async () => {
      const count = 30

      const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
      const log2 = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })
      let log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
      for (let i = 1; i <= count; i++) {
        await log.append('hello' + i)
        if (i % 10 === 0) {
          await log2.append('hi' + i)
          await log2.join(log)
        }
        if (i % 25 === 0) {
          log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3),
            { logId: log3._id, entries: log3.values, heads: log3.heads.concat(log2.heads) })
          await log3.append('--' + i)
        }
      }

      await log3.join(log2)

      const log4 = new Log(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey4), { logId: 'X' })
      await log4.join(log2)
      await log4.join(log3)

      const values3 = log3.values.map((e) => e.payload.value)
      const values4 = log4.values.map((e) => e.payload.value)

      assert.deepStrictEqual(values3, values4)
    })
  })
})
