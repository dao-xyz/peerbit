const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Log } from '../log'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
const Keystore = require('orbit-db-keystore')
import { LogCreator } from './utils/log-creator'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity, testIdentity2, testIdentity3

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Iterator (' + IPFS + ')', function () {
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
      testIdentity2 = await Identities.createIdentity({ id: 'userB', keystore, signingKeystore })
      testIdentity3 = await Identities.createIdentity({ id: 'userC', keystore, signingKeystore })
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
      let log1

      beforeEach(async () => {
        log1 = new Log(ipfs, testIdentity, { logId: 'X' })

        for (let i = 0; i <= 100; i++) {
          await log1.append('entry' + i)
        }
      })

      test('returns a Symbol.iterator object', async () => {
        const it = log1.iterator({
          lte: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde',
          amount: 0
        })

        assert.strictEqual(typeof it[Symbol.iterator], 'function')
        assert.deepStrictEqual(it.next(), { value: undefined, done: true })
      })

      test('returns length with lte and amount', async () => {
        const amount = 10
        const it = log1.iterator({
          lte: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde',
          amount: amount
        })

        assert.strictEqual([...it].length, 10)
      })

      test('returns entries with lte and amount', async () => {
        const amount = 10

        const it = log1.iterator({
          lte: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde',
          amount: amount
        })

        let i = 0
        for (const entry of it) {
          assert.strictEqual(entry.payload, 'entry' + (67 - i++))
        }
      })

      test('returns length with lt and amount', async () => {
        const amount = 10

        const it = log1.iterator({
          lt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde',
          amount: amount
        })

        assert.strictEqual([...it].length, amount)
      })

      test('returns entries with lt and amount', async () => {
        const amount = 10

        const it = log1.iterator({
          lt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde',
          amount: amount
        })

        let i = 1
        for (const entry of it) {
          assert.strictEqual(entry.payload, 'entry' + (67 - i++))
        }
      })

      test('returns correct length with gt and amount', async () => {
        const amount = 5
        const it = log1.iterator({
          gt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde',
          amount: amount
        })

        let i = 0
        let count = 0
        for (const entry of it) {
          assert.strictEqual(entry.payload, 'entry' + (72 - i++))
          count++
        }
        assert.strictEqual(count, amount)
      })

      test('returns length with gte and amount', async () => {
        const amount = 12

        const it = log1.iterator({
          gt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde',
          amount: amount
        })

        assert.strictEqual([...it].length, amount)
      })

      test('returns entries with gte and amount', async () => {
        const amount = 12

        const it = log1.iterator({
          gt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde',
          amount: amount
        })

        let i = 0
        for (const entry of it) {
          assert.strictEqual(entry.payload, 'entry' + (79 - i++))
        }
      })

      /* eslint-disable camelcase */
      test('iterates with lt and gt', async () => {
        const it = log1.iterator({
          gt: 'zdpuAymZUrYbHgwfYK76xXYhzxNqwaXRWWrn5kmRsZJFdqBEz',
          lt: 'zdpuAoDcWRiChLXnGskymcGrM1VdAjsaFrsXvNZmcDattA7AF'
        })
        const hashes = [...it].map(e => e.hash)

        // neither hash should appear in the array
        assert.strictEqual(hashes.indexOf('zdpuAymZUrYbHgwfYK76xXYhzxNqwaXRWWrn5kmRsZJFdqBEz'), -1)
        assert.strictEqual(hashes.indexOf('zdpuAoDcWRiChLXnGskymcGrM1VdAjsaFrsXvNZmcDattA7AF'), -1)
        assert.strictEqual(hashes.length, 10)
      })

      test('iterates with lt and gte', async () => {
        const it = log1.iterator({
          gte: 'zdpuAt7YtNE1i9APJitGyKomcmxjc2BDHa57wkrjq4onqBNaR',
          lt: 'zdpuAr8N4vzqcB5sh5JLcr6Eszo4HnYefBWDbBBwwrTPo6kU6'
        })
        const hashes = [...it].map(e => e.hash)

        // only the gte hash should appear in the array
        assert.strictEqual(hashes.indexOf('zdpuAt7YtNE1i9APJitGyKomcmxjc2BDHa57wkrjq4onqBNaR'), 24)
        assert.strictEqual(hashes.indexOf('zdpuAr8N4vzqcB5sh5JLcr6Eszo4HnYefBWDbBBwwrTPo6kU6'), -1)
        assert.strictEqual(hashes.length, 25)
      })

      test('iterates with lte and gt', async () => {
        const it = log1.iterator({
          gt: 'zdpuAqUrGrPa4AaZAQbCH4yxQfEjB32rdFY743XCgyGW8iAuU',
          lte: 'zdpuAwkagwE9D2jUtLnDiCPqBGh9xhpnaX8iEDQ3K7HRmjggi'
        })
        const hashes = [...it].map(e => e.hash)

        // only the lte hash should appear in the array
        assert.strictEqual(hashes.indexOf('zdpuAqUrGrPa4AaZAQbCH4yxQfEjB32rdFY743XCgyGW8iAuU'), -1)
        assert.strictEqual(hashes.indexOf('zdpuAwkagwE9D2jUtLnDiCPqBGh9xhpnaX8iEDQ3K7HRmjggi'), 0)
        assert.strictEqual(hashes.length, 4)
      })

      test('iterates with lte and gte', async () => {
        const it = log1.iterator({
          gte: 'zdpuAzG5AD1GdeNffSskTErjjPbAb95QiNyoaQSrbB62eqYSD',
          lte: 'zdpuAuujURnUUxVw338Xwh47zGEFjjbaZXXARHPik6KYUcUVk'
        })
        const hashes = [...it].map(e => e.hash)

        // neither hash should appear in the array
        assert.strictEqual(hashes.indexOf('zdpuAzG5AD1GdeNffSskTErjjPbAb95QiNyoaQSrbB62eqYSD'), 9)
        assert.strictEqual(hashes.indexOf('zdpuAuujURnUUxVw338Xwh47zGEFjjbaZXXARHPik6KYUcUVk'), 0)
        assert.strictEqual(hashes.length, 10)
      })

      test('returns length with gt and default amount', async () => {
        const it = log1.iterator({
          gt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde'
        })

        assert.strictEqual([...it].length, 33)
      })

      test('returns entries with gt and default amount', async () => {
        const it = log1.iterator({
          gt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde'
        })

        let i = 0
        for (const entry of it) {
          assert.strictEqual(entry.payload, 'entry' + (100 - i++))
        }
      })

      test('returns length with gte and default amount', async () => {
        const it = log1.iterator({
          gte: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde'
        })

        assert.strictEqual([...it].length, 34)
      })

      test('returns entries with gte and default amount', async () => {
        const it = log1.iterator({
          gte: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde'
        })

        let i = 0
        for (const entry of it) {
          assert.strictEqual(entry.payload, 'entry' + (100 - i++))
        }
      })

      test('returns length with lt and default amount value', async () => {
        const it = log1.iterator({
          lt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde'
        })

        assert.strictEqual([...it].length, 67)
      })

      test('returns entries with lt and default amount value', async () => {
        const it = log1.iterator({
          lt: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde'
        })

        let i = 0
        for (const entry of it) {
          assert.strictEqual(entry.payload, 'entry' + (66 - i++))
        }
      })

      test('returns length with lte and default amount value', async () => {
        const it = log1.iterator({
          lte: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde'
        })

        assert.strictEqual([...it].length, 68)
      })

      test('returns entries with lte and default amount value', async () => {
        const it = log1.iterator({
          lte: 'zdpuAuNuQ4YBeXY5YStfrsJx6ykz4yBV2XnNcBR4uGmiojQde'
        })

        let i = 0
        for (const entry of it) {
          assert.strictEqual(entry.payload, 'entry' + (67 - i++))
        }
      })
    })

    describe('Iteration over forked/joined logs', () => {
      let fixture, identities

      beforeAll(async () => {
        identities = [testIdentity3, testIdentity2, testIdentity3, testIdentity]
        fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
      })

      test('returns the full length from all heads', async () => {
        const it = fixture.log.iterator({
          lte: fixture.log.heads
        })

        assert.strictEqual([...it].length, 16)
      })

      test('returns partial entries from all heads', async () => {
        const it = fixture.log.iterator({
          lte: fixture.log.heads,
          amount: 6
        })

        assert.deepStrictEqual([...it].map(e => e.payload),
          ['entryA10', 'entryA9', 'entryA8', 'entryA7', 'entryC0', 'entryA6'])
      })

      test('returns partial logs from single heads #1', async () => {
        const it = fixture.log.iterator({
          lte: [fixture.log.heads[0]]
        })

        assert.strictEqual([...it].length, 10)
      })

      test('returns partial logs from single heads #2', async () => {
        const it = fixture.log.iterator({
          lte: [fixture.log.heads[1]]
        })

        assert.strictEqual([...it].length, 11)
      })

      test('throws error if lt/lte not a string or array of entries', async () => {
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
