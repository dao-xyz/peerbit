const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Log } from '../log'
import { SortByEntryHash } from '../log-sorting'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity

Object.keys(testAPIs).forEach(IPFS => {
  describe('Log - Join Concurrent Entries', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)
      testIdentity = await Identities.createIdentity({ id: new Uint8Array([0]), identityKeysPath, signingKeysPath })

      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api
    })

    afterAll(async () => {
      await stopIpfs(ipfsd)
      await testIdentity.provider.keystore.close()
      await testIdentity.provider.signingKeystore.close()
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
    })

    describe('join ', () => {
      let log1: Log<string>, log2: Log<string>

      beforeAll(async () => {

        log1 = new Log(ipfs, testIdentity, { logId: 'A', sortFn: SortByEntryHash })
        log2 = new Log(ipfs, testIdentity, { logId: 'A', sortFn: SortByEntryHash })
      })

      it('joins consistently', async () => {
        for (let i = 0; i < 10; i++) {
          await log1.append('hello1-' + i)
          await log2.append('hello2-' + i)
        }

        await log1.join(log2)
        await log2.join(log1)

        const hash1 = await log1.toMultihash()
        const hash2 = await log2.toMultihash()

        assert.strictEqual(hash1, hash2)
        assert.strictEqual(log1.length, 20)
        assert.deepStrictEqual(log1.values.map(e => e.data.payload), log2.values.map(e => e.data.payload))
      })

      it('Concurrently appending same payload after join results in same state', async () => {
        for (let i = 10; i < 20; i++) {
          await log1.append('hello1-' + i)
          await log2.append('hello2-' + i)
        }

        await log1.join(log2)
        await log2.join(log1)

        await log1.append('same')
        await log2.append('same')

        const hash1 = await log1.toMultihash()
        const hash2 = await log2.toMultihash()

        assert.strictEqual(hash1, hash2)
        assert.strictEqual(log1.length, 41)
        assert.strictEqual(log2.length, 41)
        assert.deepStrictEqual(log1.values.map(e => e.data.payload), log2.values.map(e => e.data.payload))
      })

      it('Joining after concurrently appending same payload joins entry once', async () => {
        await log1.join(log2)
        await log2.join(log1)

        assert.strictEqual(log1.length, log2.length)
        assert.strictEqual(log1.length, 41)
        assert.deepStrictEqual(log1.values.map(e => e.data.payload), log2.values.map(e => e.data.payload))
      })
    })
  })
})
