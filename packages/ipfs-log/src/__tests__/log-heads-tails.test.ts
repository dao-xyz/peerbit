const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Entry } from '@dao-xyz/ipfs-log-entry';
import { Log } from '../log'
import { Identities, Identity } from '@dao-xyz/orbit-db-identity-provider'
import { Keystore } from '@dao-xyz/orbit-db-keystore'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity: Identity, testIdentity2: Identity, testIdentity3: Identity, testIdentity4: Identity

const last = (arr) => {
  return arr[arr.length - 1]
}

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Heads and Tails', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore: Keystore, signingKeystore: Keystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
      testIdentity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore, signingKeystore })
      testIdentity2 = await Identities.createIdentity({ id: new Uint8Array([1]), keystore, signingKeystore })
      testIdentity3 = await Identities.createIdentity({ id: new Uint8Array([2]), keystore, signingKeystore })
      testIdentity4 = await Identities.createIdentity({ id: new Uint8Array([4]), keystore, signingKeystore })

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

    describe('heads', () => {
      it('finds one head after one entry', async () => {
        const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
        await log1.append('helloA1')
        assert.strictEqual(log1.heads.length, 1)
      })

      it('finds one head after two entries', async () => {
        const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
        await log1.append('helloA1')
        await log1.append('helloA2')
        assert.strictEqual(log1.heads.length, 1)
      })

      it('log contains the head entry', async () => {
        const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
        await log1.append('helloA1')
        await log1.append('helloA2')
        assert.deepStrictEqual(log1.get(log1.heads[0].hash), log1.heads[0])
      })

      it('finds head after a join and append', async () => {
        const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
        const log2 = new Log<string>(ipfs, testIdentity, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')

        await log2.join(log1)
        await log2.append('helloB2')
        const expectedHead = last(log2.values)

        assert.strictEqual(log2.heads.length, 1)
        assert.deepStrictEqual(log2.heads[0].hash, expectedHead.hash)
      })

      it('finds two heads after a join', async () => {
        const log2 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
        const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')
        const expectedHead1 = last(log1.values)

        await log2.append('helloB1')
        await log2.append('helloB2')
        const expectedHead2 = last(log2.values)

        await log1.join(log2)

        const heads = log1.heads
        assert.strictEqual(heads.length, 2)
        assert.strictEqual(heads[0].hash, expectedHead2.hash)
        assert.strictEqual(heads[1].hash, expectedHead1.hash)
      })

      it('finds two heads after two joins', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log2 = new Log(ipfs, testIdentity, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')

        await log2.append('helloB1')
        await log2.append('helloB2')

        await log1.join(log2)

        await log2.append('helloB3')

        await log1.append('helloA3')
        await log1.append('helloA4')
        const expectedHead2 = last(log2.values)
        const expectedHead1 = last(log1.values)

        await log1.join(log2)

        const heads = log1.heads
        assert.strictEqual(heads.length, 2)
        assert.strictEqual(heads[0].hash, expectedHead1.hash)
        assert.strictEqual(heads[1].hash, expectedHead2.hash)
      })

      it('finds two heads after three joins', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log2 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log3 = new Log(ipfs, testIdentity, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2)
        await log1.append('helloA3')
        await log1.append('helloA4')
        const expectedHead1 = last(log1.values)
        await log3.append('helloC1')
        await log3.append('helloC2')
        await log2.join(log3)
        await log2.append('helloB3')
        const expectedHead2 = last(log2.values)
        await log1.join(log2)

        const heads = log1.heads
        assert.strictEqual(heads.length, 2)
        assert.strictEqual(heads[0].hash, expectedHead1.hash)
        assert.strictEqual(heads[1].hash, expectedHead2.hash)
      })

      it('finds three heads after three joins', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log2 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log3 = new Log(ipfs, testIdentity, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2)
        await log1.append('helloA3')
        await log1.append('helloA4')
        const expectedHead1 = last(log1.values)
        await log3.append('helloC1')
        await log2.append('helloB3')
        await log3.append('helloC2')
        const expectedHead2 = last(log2.values)
        const expectedHead3 = last(log3.values)
        await log1.join(log2)
        await log1.join(log3)

        const heads = log1.heads
        assert.strictEqual(heads.length, 3)
        assert.deepStrictEqual(heads[0].hash, expectedHead1.hash)
        assert.deepStrictEqual(heads[1].hash, expectedHead2.hash)
        assert.deepStrictEqual(heads[2].hash, expectedHead3.hash)
      })
    })

    describe('tails', () => {
      it('returns a tail', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        await log1.append('helloA1')
        assert.strictEqual(log1.tails.length, 1)
      })

      it('tail is a Entry', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        await log1.append('helloA1')
        assert.strictEqual(Entry.isEntry(log1.tails[0]), true)
      })

      it('returns tail entries', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log2 = new Log(ipfs, testIdentity, { logId: 'A' })
        await log1.append('helloA1')
        await log2.append('helloB1')
        await log1.join(log2)
        assert.strictEqual(log1.tails.length, 2)
        assert.strictEqual(Entry.isEntry(log1.tails[0]), true)
        assert.strictEqual(Entry.isEntry(log1.tails[1]), true)
      })

      it('returns tail hashes', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log2 = new Log(ipfs, testIdentity, { logId: 'A' })
        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2, 2)
        assert.strictEqual(log1.tailHashes.length, 2)
      })

      it('returns no tail hashes if all entries point to empty nexts', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log2 = new Log(ipfs, testIdentity, { logId: 'A' })
        await log1.append('helloA1')
        await log2.append('helloB1')
        await log1.join(log2)
        assert.strictEqual(log1.tailHashes.length, 0)
      })

      it('returns tails after loading a partial log', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
        const log2 = new Log(ipfs, testIdentity2, { logId: 'A' })
        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2)
        const log4 = await Log.fromEntry(ipfs, testIdentity, log1.heads, { length: 2 })
        assert.strictEqual(log4.length, 2)
        assert.strictEqual(log4.tails.length, 2)
        assert.strictEqual(log4.tails[0].hash, log4.values[0].hash)
        assert.strictEqual(log4.tails[1].hash, log4.values[1].hash)
      })

      it('returns tails sorted by public key', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'XX' })
        const log2 = new Log(ipfs, testIdentity2, { logId: 'XX' })
        const log3 = new Log(ipfs, testIdentity3, { logId: 'XX' })
        const log4 = new Log(ipfs, testIdentity4, { logId: 'XX' })
        await log1.append('helloX1')
        await log2.append('helloB1')
        await log3.append('helloA1')
        await log3.join(log1)
        await log3.join(log2)
        await log4.join(log3)
        assert.strictEqual(log4.tails.length, 3)
        assert.strictEqual(log4.tails[0].data.id, 'XX')
        assert.deepStrictEqual(log4.tails[2].data.clock.id, testIdentity3.publicKey)
        assert.deepStrictEqual(log4.tails[1].data.clock.id, testIdentity2.publicKey)
        assert.deepStrictEqual(log4.tails[0].data.clock.id, testIdentity.publicKey)
        assert.deepStrictEqual(log4.clock.id, testIdentity4.publicKey)
      })
    })
  })
})
