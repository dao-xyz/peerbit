const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Log } from '../log'
import { Identities, Identity } from '@dao-xyz/orbit-db-identity-provider'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { LogCreator } from './utils/log-creator'
import { assertPayload } from './utils/assert'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity: Identity, testIdentity2: Identity, testIdentity3: Identity

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Iterator', function () {
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

      testIdentity = await Identities.createIdentity({ id: new Uint8Array([3]), keystore, signingKeystore })
      testIdentity2 = await Identities.createIdentity({ id: new Uint8Array([2]), keystore, signingKeystore })
      testIdentity3 = await Identities.createIdentity({ id: new Uint8Array([1]), keystore, signingKeystore })
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

    describe('Basic iterator functionality', () => {
      let log1: Log<string>

      beforeEach(async () => {
        log1 = new Log(ipfs, testIdentity, { logId: 'X' })

        for (let i = 0; i <= 100; i++) {
          await log1.append('entry' + i)
        }
      })

      it('returns a Symbol.iterator object', async () => {
        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: 0
        })

        assert.strictEqual(typeof it[Symbol.iterator], 'function')
        assert.deepStrictEqual(it.next(), { value: undefined, done: true })
      })

      it('returns length with lte and amount', async () => {
        const amount = 10
        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: amount
        })
        const length = [...it].length;
        assert.strictEqual(length, 10)
      })

      it('returns entries with lte and amount and payload', async () => {
        const amount = 10

        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: amount
        })

        let i = 0
        for (const entry of it) {
          assertPayload(entry.payload.value, 'entry' + (67 - i++))
        }
        assert.strictEqual(i, amount)

      })



      it('returns correct length with gt and amount', async () => {
        const amount = 5
        const it = log1.iterator({
          gt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: amount
        })

        let i = 0
        for (const entry of it) {
          assertPayload(entry.payload.value, 'entry' + (72 - i++))
        }
        assert.strictEqual(i, amount)
      })



      it('returns entries with gte and amount and payload', async () => {
        const amount = 12

        const it = log1.iterator({
          gt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: amount
        })

        let i = 0
        for (const entry of it) {
          assertPayload(entry.payload.value, 'entry' + (79 - i++))
        }
        assert.strictEqual(i, amount);
      })

      /* eslint-disable camelcase */
      it('iterates with lt and gt', async () => {
        const it = log1.iterator({
          gt: 'zdpuAo48H2WjBVJJUJ9aPtmynJbHCFYUaXUdXyY8SsU38bE23',
          lt: 'zdpuAq4vvCjcNF99gJfgyuzr23Jggqg8HH69PkgJeAkSFEgbq'
        })
        const hashes = [...it].map(e => e.hash)

        // neither hash should appear in the array
        assert.strictEqual(hashes.indexOf('zdpuAo48H2WjBVJJUJ9aPtmynJbHCFYUaXUdXyY8SsU38bE23'), -1)
        assert.strictEqual(hashes.indexOf('zdpuAq4vvCjcNF99gJfgyuzr23Jggqg8HH69PkgJeAkSFEgbq'), -1)
        assert.strictEqual(hashes.length, 10)
      })

      it('iterates with lt and gte', async () => {
        const it = log1.iterator({
          gte: 'zdpuAwLMpxr8soCH1QC6XbvkHMxVDViBo1viPXJ48sRV7FUPc',
          lt: 'zdpuAyATysiVgqZKrgmLLs5V8MXb7XjTc1FrgDWm6KAfnhxbd'
        })
        const hashes = [...it].map(e => e.hash)

        // only the gte hash should appear in the array
        assert.strictEqual(hashes.indexOf('zdpuAwLMpxr8soCH1QC6XbvkHMxVDViBo1viPXJ48sRV7FUPc'), 24)
        assert.strictEqual(hashes.indexOf('zdpuAyATysiVgqZKrgmLLs5V8MXb7XjTc1FrgDWm6KAfnhxbd'), -1)
        assert.strictEqual(hashes.length, 25)
      })

      it('iterates with lte and gt', async () => {
        const it = log1.iterator({
          gt: 'zdpuAyUCayar44SgPxTC3P9UvVr2B4DARQ9mbiaTd9aGkwFwg',
          lte: 'zdpuAoxE82TvJQQYzvgNAh4UtXh8bd6ka8jVVosyK11dYGNDs'
        })
        const hashes = [...it].map(e => e.hash)

        // only the lte hash should appear in the array
        assert.strictEqual(hashes.indexOf('zdpuAyUCayar44SgPxTC3P9UvVr2B4DARQ9mbiaTd9aGkwFwg'), -1)
        assert.strictEqual(hashes.indexOf('zdpuAoxE82TvJQQYzvgNAh4UtXh8bd6ka8jVVosyK11dYGNDs'), 0)
        assert.strictEqual(hashes.length, 4)
      })

      it('iterates with lte and gte', async () => {
        const it = log1.iterator({
          gte: 'zdpuAv1v9krPN1ctgV8RzrqzFqpG7978nfyDyaawGSwhVjpGQ',
          lte: 'zdpuB2Y7A7TSwFhENZCUtDtt1WEiMttngkxxhaXHnkGEi9ttZ'
        })
        const hashes = [...it].map(e => e.hash)

        // neither hash should appear in the array
        assert.strictEqual(hashes.indexOf('zdpuAv1v9krPN1ctgV8RzrqzFqpG7978nfyDyaawGSwhVjpGQ'), 9)
        assert.strictEqual(hashes.indexOf('zdpuB2Y7A7TSwFhENZCUtDtt1WEiMttngkxxhaXHnkGEi9ttZ'), 0)
        assert.strictEqual(hashes.length, 10)
      })

      it('returns length with gt and default amount', async () => {
        const it = log1.iterator({
          gt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        assert.strictEqual([...it].length, 33)
      })

      it('returns entries with gt and default amount', async () => {
        const it = log1.iterator({
          gt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        let i = 0
        for (const entry of it) {
          assertPayload(entry.payload.value, 'entry' + (100 - i++))
        }
      })

      it('returns length with gte and default amount', async () => {
        const it = log1.iterator({
          gte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        assert.strictEqual([...it].length, 34)
      })

      it('returns entries with gte and default amount', async () => {
        const it = log1.iterator({
          gte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        let i = 0
        for (const entry of it) {
          assertPayload(entry.payload.value, 'entry' + (100 - i++))
        }
      })

      it('returns length with lt and default amount value', async () => {
        const it = log1.iterator({
          lt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        assert.strictEqual([...it].length, 67)
      })

      it('returns entries with lt and default amount value', async () => {
        const it = log1.iterator({
          lt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        let i = 0
        for (const entry of it) {
          assertPayload(entry.payload.value, 'entry' + (66 - i++))
        }
      })

      it('returns length with lte and default amount value', async () => {
        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        assert.strictEqual([...it].length, 68)
      })

      it('returns entries with lte and default amount value', async () => {
        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        let i = 0
        for (const entry of it) {
          assertPayload(entry.payload.value, 'entry' + (67 - i++))
        }
      })
    })

    describe('Iteration over forked/joined logs', () => {
      let fixture: {
        log: Log<string>;
        expectedData: string[];
        json: any;
      }, identities

      beforeAll(async () => {
        identities = [testIdentity3, testIdentity2, testIdentity3, testIdentity]
        fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
      })

      it('returns the full length from all heads', async () => {
        const it = fixture.log.iterator({
          lte: fixture.log.heads
        })

        assert.strictEqual([...it].length, 16)
      })

      it('returns partial entries from all heads', async () => {
        const it = fixture.log.iterator({
          lte: fixture.log.heads,
          amount: 6
        })

        assert.deepStrictEqual([...it].map(e => e.payload.value),
          ['entryA10', 'entryA9', 'entryA8', 'entryA7', 'entryC0', 'entryA6'])
      })

      it('returns partial logs from single heads #1', async () => {
        const it = fixture.log.iterator({
          lte: [fixture.log.heads[0]]
        })

        assert.strictEqual([...it].length, 10)
      })

      it('returns partial logs from single heads #2', async () => {
        const it = fixture.log.iterator({
          lte: [fixture.log.heads[1]]
        })

        assert.strictEqual([...it].length, 11)
      })

      it('throws error if lt/lte not a string or array of entries', async () => {
        let errMsg

        try {
          fixture.log.iterator({
            lte: fixture.log.heads[1]
          })
        } catch (e) {
          errMsg = e.message
        }

        assert.strictEqual(errMsg, 'lt or lte must be a string or array of Entries')
      })
    })
  })
})
