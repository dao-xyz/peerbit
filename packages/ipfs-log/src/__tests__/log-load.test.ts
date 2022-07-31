const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { LastWriteWins } from '../log-sorting'
import bigLogString from './fixtures/big-log.fixture';
import { Entry, JSON_IO_OPTIONS } from '@dao-xyz/ipfs-log-entry';
import { Log } from '../log'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { LogCreator } from './utils/log-creator'
import { assertPayload } from './utils/assert';
const v0Entries = require('./fixtures/v0-entries.fixture')
const v1Entries = require('./fixtures/v1-entries.fixture')

// Alternate tiebreaker. Always does the opposite of LastWriteWins
const FirstWriteWins = (a, b) => LastWriteWins(a, b) * -1
const BadComparatorReturnsZero = (a, b) => 0

// Test utils
const {
  config,
  MemStore,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity, testIdentity2, testIdentity3, testIdentity4

const last = <T>(arr: T[]): T => {
  return arr[arr.length - 1]
}

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Load', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    const firstWriteExpectedData = [
      'entryA6', 'entryA7', 'entryA8', 'entryA9',
      'entryA10', 'entryB1', 'entryB2', 'entryB3',
      'entryB4', 'entryB5', 'entryA1', 'entryA2',
      'entryA3', 'entryA4', 'entryA5', 'entryC0'
    ]

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
      testIdentity4 = await Identities.createIdentity({ id: new Uint8Array([0]), keystore, signingKeystore })
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api

      const memstore = new MemStore()
      ipfs.object.put = memstore.put.bind(memstore)
      ipfs.object.get = memstore.get.bind(memstore)
    })

    afterAll(async () => {
      await stopIpfs(ipfsd)
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)

      await keystore?.close()
      await signingKeystore?.close()
    })

    describe('fromJSON', () => {
      let identities

      beforeAll(async () => {
        identities = [testIdentity, testIdentity2, testIdentity3, testIdentity4]
      })

      it('creates a log from an entry', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const data = fixture.log
        const json = fixture.json
        json.heads = await Promise.all(json.heads.map(headHash => Entry.fromMultihash(ipfs, headHash))) as any
        const log = await Log.fromJSON(ipfs, testIdentity, json, {})
        assert.strictEqual(log.id, data.heads[0].data.id)
        assert.strictEqual(log.length, 16)
        assert.deepStrictEqual(log.values.map(e => e.init({ io: JSON_IO_OPTIONS }).data.payload), fixture.expectedData)
      })

      it('creates a log from an entry with custom tiebreaker', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const data = fixture.log
        const json = fixture.json

        json.heads = await Promise.all(json.heads.map(headHash => Entry.fromMultihash(ipfs, headHash))) as any

        const log = await Log.fromJSON(ipfs, testIdentity, json,
          { length: -1, sortFn: FirstWriteWins })

        assert.strictEqual(log.id, data.heads[0].data.id)
        assert.strictEqual(log.length, 16)
        assert.deepStrictEqual(log.values.map(e => e.init({ io: JSON_IO_OPTIONS }).data.payload), firstWriteExpectedData)
      })

      it('respects timeout parameter', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const json = fixture.json
        json.heads = [{ hash: 'zdpuAwNuRc2Kc1aNDdcdSWuxfNpHRJQw8L8APBNHCEFXbogus' }] as any

        const timeout = 500
        const st = new Date().getTime()
        const log = await Log.fromJSON(ipfs, testIdentity, json, { timeout })
        const et = new Date().getTime()
        // Allow for a few millseconds of skew
        assert.strictEqual((et - st) >= (timeout - 10), true, '' + (et - st) + ' should be greater than timeout ' + timeout)
        assert.strictEqual(log.length, 0)
        assert.deepStrictEqual(log.values.map(e => e.init({ io: JSON_IO_OPTIONS }).data.payload), [])
      })
    })

    describe('fromEntryHash', () => {
      let identities

      beforeAll(async () => {
        identities = [testIdentity, testIdentity2, testIdentity3, testIdentity4]
      })

      it('creates a log from an entry hash', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const data = fixture.log
        const json = fixture.json

        const log1 = await Log.fromEntryHash(ipfs, testIdentity, json.heads[0],
          { logId: 'X' })
        const log2 = await Log.fromEntryHash(ipfs, testIdentity, json.heads[1],
          { logId: 'X' })

        await log1.join(log2)

        assert.strictEqual(log1.id, data.heads[0].data.id)
        assert.strictEqual(log1.length, 16)
        assert.deepStrictEqual(log1.values.map(e => e.init({ io: JSON_IO_OPTIONS }).data.payload), fixture.expectedData)
      })

      it('creates a log from an entry hash with custom tiebreaker', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const data = fixture.log
        const json = fixture.json
        const log1 = await Log.fromEntryHash(ipfs, testIdentity, json.heads[0],
          { logId: 'X', sortFn: FirstWriteWins })
        const log2 = await Log.fromEntryHash(ipfs, testIdentity, json.heads[1],
          { logId: 'X', sortFn: FirstWriteWins })

        await log1.join(log2)

        assert.strictEqual(log1.id, data.heads[0].data.id)
        assert.strictEqual(log1.length, 16)
        assert.deepStrictEqual(log1.values.map(e => e.init({ io: JSON_IO_OPTIONS }).data.payload), firstWriteExpectedData)
      })

      it('respects timeout parameter', async () => {
        const timeout = 500
        const st = new Date().getTime()
        const log = await Log.fromEntryHash(ipfs, testIdentity, 'zdpuAwNuRc2Kc1aNDdcdSWuxfNpHRJQw8L8APBNHCEFXbogus', { logId: 'X', timeout })
        const et = new Date().getTime()
        assert.strictEqual((et - st) >= timeout, true, '' + (et - st) + ' should be greater than timeout ' + timeout)
        assert.strictEqual(log.length, 0)
        assert.deepStrictEqual(log.values.map(e => e.init({ io: JSON_IO_OPTIONS }).data.payload), [])
      })
    })

    describe('fromEntry', () => {
      let identities

      beforeAll(async () => {
        identities = [testIdentity, testIdentity2, testIdentity3, testIdentity4]
      })

      it('creates a log from an entry', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const data = fixture.log

        const log = await Log.fromEntry<string>(ipfs, testIdentity, data.heads, { length: -1 })
        assert.strictEqual(log.id, data.heads[0].data.id)
        assert.strictEqual(log.length, 16)
        assert.deepStrictEqual(log.values.map(e => e.init({ io: JSON_IO_OPTIONS }).data.payload), fixture.expectedData)
      })

      it('creates a log from an entry with custom tiebreaker', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const data = fixture.log

        const log = await Log.fromEntry<string>(ipfs, testIdentity, data.heads,
          { length: -1, sortFn: FirstWriteWins })
        assert.strictEqual(log.id, data.heads[0].data.id)
        assert.strictEqual(log.length, 16)
        assert.deepStrictEqual(log.values.map(e => e.init({ io: JSON_IO_OPTIONS }).data.payload), firstWriteExpectedData)
      })

      it('keeps the original heads', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const data = fixture.log
        const log1 = await Log.fromEntry<string>(ipfs, testIdentity, data.heads,
          { length: data.heads.length })
        assert.strictEqual(log1.id, data.heads[0].data.id)
        assert.strictEqual(log1.length, data.heads.length)
        assertPayload(log1.values[0].data.payload, 'entryC0')
        assertPayload(log1.values[1].data.payload, 'entryA10')

        const log2 = await Log.fromEntry<string>(ipfs, testIdentity, data.heads, { length: 4 })
        assert.strictEqual(log2.id, data.heads[0].data.id)
        assert.strictEqual(log2.length, 4)
        assertPayload(log2.values[0].data.payload, 'entryC0')
        assertPayload(log2.values[1].data.payload, 'entryA8')
        assertPayload(log2.values[2].data.payload, 'entryA9')
        assertPayload(log2.values[3].data.payload, 'entryA10')

        const log3 = await Log.fromEntry<string>(ipfs, testIdentity, data.heads, { length: 7 })
        assert.strictEqual(log3.id, data.heads[0].data.id)
        assert.strictEqual(log3.length, 7)
        assertPayload(log3.values[0].data.payload, 'entryB5')
        assertPayload(log3.values[1].data.payload, 'entryA6')
        assertPayload(log3.values[2].data.payload, 'entryC0')
        assertPayload(log3.values[3].data.payload, 'entryA7')
        assertPayload(log3.values[4].data.payload, 'entryA8')
        assertPayload(log3.values[5].data.payload, 'entryA9')
        assertPayload(log3.values[6].data.payload, 'entryA10')
      })

      it('onProgress callback is fired for each entry', async () => {
        const items1: Entry<string>[] = []
        const amount = 100
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const n1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'entryA' + i, next: [prev1] })
          items1.push(n1)
        }

        let i = 0
        const callback = (entry: Entry<string>) => {
          entry.init({ io: JSON_IO_OPTIONS });
          assert.notStrictEqual(entry, null)
          assert.strictEqual(entry.hash, items1[items1.length - i - 1].hash)
          assertPayload(entry.data.payload, items1[items1.length - i - 1].data.payload)
          i++
        }

        await Log.fromEntry<string>(ipfs, testIdentity, last(items1),
          { length: -1, exclude: [], onProgressCallback: callback })
      })

      it('retrieves partial log from an entry hash', async () => {
        const log1 = new Log<string>(ipfs, testIdentity, { logId: 'X' })
        const log2 = new Log<string>(ipfs, testIdentity2, { logId: 'X' })
        const log3 = new Log<string>(ipfs, testIdentity3, { logId: 'X' })
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 100
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          const n1 = await Entry.create({ ipfs, identity: log1._identity, logId: 'X', data: 'entryA' + i, next: [prev1] })
          const n2 = await Entry.create({ ipfs, identity: log2._identity, logId: 'X', data: 'entryB' + i, next: [prev2, n1] })
          const n3 = await Entry.create({ ipfs, identity: log3._identity, logId: 'X', data: 'entryC' + i, next: [prev3, n1, n2] })
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        // limit to 10 entries
        const a = await Log.fromEntry<string>(ipfs, testIdentity, last(items1), { length: 10 })
        assert.strictEqual(a.length, 10)

        // limit to 42 entries
        const b = await Log.fromEntry<string>(ipfs, testIdentity, last(items1), { length: 42 })
        assert.strictEqual(b.length, 42)
      })

      it('throws an error if trying to create a log from a hash of an entry', async () => {
        const items1: Entry<string>[] = []
        const amount = 5
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const n1 = await Entry.create({ ipfs, identity: testIdentity, logId: 'A', data: 'entryA' + i, next: [prev1] })
          items1.push(n1)
        }

        let err
        try {
          await Log.fromEntry<string>(ipfs, testIdentity, last(items1).hash as any, { length: 1 })
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, '\'sourceEntries\' argument must be an array of Entry instances or a single Entry')
      })

      it('retrieves full log from an entry hash', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'X' })
        const log2 = new Log(ipfs, testIdentity2, { logId: 'X' })
        const log3 = new Log(ipfs, testIdentity3, { logId: 'X' })
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 10
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          const n1 = await Entry.create({ ipfs, identity: log1._identity, logId: 'X', data: 'entryA' + i, next: [prev1] })
          const n2 = await Entry.create({ ipfs, identity: log2._identity, logId: 'X', data: 'entryB' + i, next: [prev2, n1] })
          const n3 = await Entry.create({ ipfs, identity: log3._identity, logId: 'X', data: 'entryC' + i, next: [prev3, n2] })
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        const a = await Log.fromEntry<string>(ipfs, testIdentity, [last(items1)],
          { length: amount })
        assert.strictEqual(a.length, amount)

        const b = await Log.fromEntry<string>(ipfs, testIdentity2, [last(items2)],
          { length: amount * 2 })
        assert.strictEqual(b.length, amount * 2)

        const c = await Log.fromEntry<string>(ipfs, testIdentity3, [last(items3)],
          { length: amount * 3 })
        assert.strictEqual(c.length, amount * 3)
      })

      it('retrieves full log from an entry hash 2', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'X' })
        const log2 = new Log(ipfs, testIdentity2, { logId: 'X' })
        const log3 = new Log(ipfs, testIdentity3, { logId: 'X' })
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 10
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          const n1 = await Entry.create({ ipfs, identity: log1._identity, logId: 'X', data: 'entryA' + i, next: [prev1] })
          const n2 = await Entry.create({ ipfs, identity: log2._identity, logId: 'X', data: 'entryB' + i, next: [prev2, n1] })
          const n3 = await Entry.create({ ipfs, identity: log3._identity, logId: 'X', data: 'entryC' + i, next: [prev3, n1, n2] })
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        const a = await Log.fromEntry<string>(ipfs, testIdentity, last(items1),
          { length: amount })
        assert.strictEqual(a.length, amount)

        const b = await Log.fromEntry<string>(ipfs, testIdentity2, last(items2),
          { length: amount * 2 })
        assert.strictEqual(b.length, amount * 2)

        const c = await Log.fromEntry<string>(ipfs, testIdentity3, last(items3),
          { length: amount * 3 })
        assert.strictEqual(c.length, amount * 3)
      })

      it('retrieves full log from an entry hash 3', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'X' })
        const log2 = new Log(ipfs, testIdentity2, { logId: 'X' })
        const log3 = new Log(ipfs, testIdentity4, { logId: 'X' })
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 10
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          log1.clock.tick()
          log2.clock.tick()
          log3.clock.tick()
          const n1 = await Entry.create({ ipfs, identity: log1._identity, logId: 'X', data: 'entryA' + i, next: [prev1], clock: log1.clock })
          const n2 = await Entry.create({ ipfs, identity: log2._identity, logId: 'X', data: 'entryB' + i, next: [prev2, n1], clock: log2.clock })
          const n3 = await Entry.create({ ipfs, identity: log3._identity, logId: 'X', data: 'entryC' + i, next: [prev3, n1, n2], clock: log3.clock })
          log1.clock.merge(log2.clock)
          log1.clock.merge(log3.clock)
          log2.clock.merge(log1.clock)
          log2.clock.merge(log3.clock)
          log3.clock.merge(log1.clock)
          log3.clock.merge(log2.clock)
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        const a = await Log.fromEntry<string>(ipfs, testIdentity, last(items1),
          { length: amount })
        assert.strictEqual(a.length, amount)

        const itemsInB = [
          'entryA1',
          'entryB1',
          'entryA2',
          'entryB2',
          'entryA3',
          'entryB3',
          'entryA4',
          'entryB4',
          'entryA5',
          'entryB5',
          'entryA6',
          'entryB6',
          'entryA7',
          'entryB7',
          'entryA8',
          'entryB8',
          'entryA9',
          'entryB9',
          'entryA10',
          'entryB10'
        ]

        const b = await Log.fromEntry<string>(ipfs, testIdentity2, last(items2),
          { length: amount * 2 })
        assert.strictEqual(b.length, amount * 2)
        assert.deepStrictEqual(b.values.map((e) => e.data.payload), itemsInB)

        const c = await Log.fromEntry<string>(ipfs, testIdentity4, last(items3),
          { length: amount * 3 })
        await c.append('EOF')
        assert.strictEqual(c.length, amount * 3 + 1)

        const tmp = [
          'entryA1',
          'entryB1',
          'entryC1',
          'entryA2',
          'entryB2',
          'entryC2',
          'entryA3',
          'entryB3',
          'entryC3',
          'entryA4',
          'entryB4',
          'entryC4',
          'entryA5',
          'entryB5',
          'entryC5',
          'entryA6',
          'entryB6',
          'entryC6',
          'entryA7',
          'entryB7',
          'entryC7',
          'entryA8',
          'entryB8',
          'entryC8',
          'entryA9',
          'entryB9',
          'entryC9',
          'entryA10',
          'entryB10',
          'entryC10',
          'EOF'
        ]
        assert.deepStrictEqual(c.values.map(e => e.data.payload), tmp)

        // make sure logX comes after A, B and C
        const logX = new Log<string>(ipfs, testIdentity4, { logId: 'X' })
        await logX.append('1')
        await logX.append('2')
        await logX.append('3')
        const d = await Log.fromEntry<string>(ipfs, testIdentity3, last(logX.values),
          { length: -1 })

        await c.join(d)
        await d.join(c)

        await c.append('DONE')
        await d.append('DONE')
        const f = await Log.fromEntry<string>(ipfs, testIdentity3, last(c.values),
          { length: -1, exclude: [] })
        const g = await Log.fromEntry<string>(ipfs, testIdentity3, last(d.values),
          { length: -1, exclude: [] })

        assert.strictEqual(f.toString(), bigLogString)
        assert.strictEqual(g.toString(), bigLogString)
      })

      it('retrieves full log of randomly joined log', async () => {
        const log1 = new Log(ipfs, testIdentity, { logId: 'X' })
        const log2 = new Log(ipfs, testIdentity3, { logId: 'X' })
        const log3 = new Log(ipfs, testIdentity4, { logId: 'X' })

        for (let i = 1; i <= 5; i++) {
          await log1.append('entryA' + i)
        }

        for (let i = 1; i <= 5; i++) {
          await log2.append('entryB' + i)
        }

        await log3.join(log1)
        await log3.join(log2)

        for (let i = 6; i <= 10; i++) {
          await log1.append('entryA' + i)
        }

        await log1.join(log3)

        for (let i = 11; i <= 15; i++) {
          await log1.append('entryA' + i)
        }

        const expectedData = [
          'entryA1', 'entryB1', 'entryA2', 'entryB2',
          'entryA3', 'entryB3', 'entryA4', 'entryB4',
          'entryA5', 'entryB5',
          'entryA6', 'entryA7', 'entryA8', 'entryA9', 'entryA10',
          'entryA11', 'entryA12', 'entryA13', 'entryA14', 'entryA15'
        ]

        assert.deepStrictEqual(log1.values.map(e => e.data.payload), expectedData)
      })

      it('retrieves randomly joined log deterministically', async () => {
        const logA = new Log(ipfs, testIdentity, { logId: 'X' })
        const logB = new Log(ipfs, testIdentity3, { logId: 'X' })
        const log3 = new Log(ipfs, testIdentity4, { logId: 'X' })
        const log = new Log(ipfs, testIdentity2, { logId: 'X' })

        for (let i = 1; i <= 5; i++) {
          await logA.append('entryA' + i)
        }

        for (let i = 1; i <= 5; i++) {
          await logB.append('entryB' + i)
        }

        await log3.join(logA)
        await log3.join(logB)

        for (let i = 6; i <= 10; i++) {
          await logA.append('entryA' + i)
        }

        await log.join(log3)
        await log.append('entryC0')
        await log.join(logA, 16)

        const expectedData = [
          'entryA1', 'entryB1', 'entryA2', 'entryB2',
          'entryA3', 'entryB3', 'entryA4', 'entryB4',
          'entryA5', 'entryB5',
          'entryA6',
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(log.values.map(e => e.data.payload), expectedData)
      })

      it('sorts', async () => {
        const testLog = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const log = testLog.log
        const expectedData = testLog.expectedData

        const expectedData2 = [
          'entryA1', 'entryB1', 'entryA2', 'entryB2',
          'entryA3', 'entryB3', 'entryA4', 'entryB4',
          'entryA5', 'entryB5',
          'entryA6', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        const expectedData3 = [
          'entryA1', 'entryB1', 'entryA2', 'entryB2',
          'entryA3', 'entryB3', 'entryA4', 'entryB4',
          'entryA5', 'entryB5', 'entryA6', 'entryC0',
          'entryA7', 'entryA8', 'entryA9'
        ]

        const expectedData4 = [
          'entryA1', 'entryB1', 'entryA2', 'entryB2',
          'entryA3', 'entryB3', 'entryA4', 'entryB4',
          'entryA5', 'entryA6', 'entryC0', 'entryA7',
          'entryA8', 'entryA9', 'entryA10'
        ]

        const fetchOrder = log.values.slice().sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder.map(e => e.data.payload), expectedData)

        const reverseOrder = log.values.slice().reverse().sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder, reverseOrder)

        const hashOrder = log.values.slice().sort((a, b) => a.hash.localeCompare(b.hash)).sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder, hashOrder)

        const randomOrder2 = log.values.slice().sort((a, b) => 0.5 - Math.random()).sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder, randomOrder2)

        // partial data
        const partialLog = log.values.filter(e => e.data.payload !== 'entryC0').sort(Entry.compare)
        assert.deepStrictEqual(partialLog.map(e => e.data.payload), expectedData2)

        const partialLog2 = log.values.filter(e => e.data.payload !== 'entryA10').sort(Entry.compare)
        assert.deepStrictEqual(partialLog2.map(e => e.data.payload), expectedData3)

        const partialLog3 = log.values.filter(e => e.data.payload !== 'entryB5').sort(Entry.compare)
        assert.deepStrictEqual(partialLog3.map(e => e.data.payload), expectedData4)
      })

      it('sorts deterministically from random order', async () => {
        const testLog = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const log = testLog.log
        const expectedData = testLog.expectedData

        const fetchOrder = log.values.slice().sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder.map(e => e.data.payload), expectedData)

        let sorted
        for (let i = 0; i < 1000; i++) {
          const randomOrder = log.values.slice().sort((a, b) => 0.5 - Math.random())
          sorted = randomOrder.sort(Entry.compare)
          assert.deepStrictEqual(sorted.map(e => e.data.payload), expectedData)
        }
      })

      it('sorts entries correctly', async () => {
        const testLog = await LogCreator.createLogWithTwoHundredEntries(Log, ipfs, identities)
        const log = testLog.log
        const expectedData = testLog.expectedData
        assert.deepStrictEqual(log.values.map(e => e.data.payload), expectedData)
      })

      it('sorts entries according to custom tiebreaker function', async () => {
        const testLog = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)

        const firstWriteWinsLog =
          new Log(ipfs, identities[0], { logId: 'X', sortFn: FirstWriteWins })
        await firstWriteWinsLog.join(testLog.log)
        assert.deepStrictEqual(firstWriteWinsLog.values.map(e => e.data.payload),
          firstWriteExpectedData)
      })

      it('throws an error if the tiebreaker returns zero', async () => {
        const testLog = await LogCreator.createLogWithSixteenEntries(Log, ipfs, identities)
        const firstWriteWinsLog =
          new Log(ipfs, identities[0], { logId: 'X', sortFn: BadComparatorReturnsZero })
        await firstWriteWinsLog.join(testLog.log)
        assert.throws(() => firstWriteWinsLog.values, Error, 'Error Thrown')
      })

      it('retrieves partially joined log deterministically - single next pointer', async () => {
        const nextPointerAmount = 1

        const logA = new Log(ipfs, testIdentity, { logId: 'X' })
        const logB = new Log(ipfs, testIdentity3, { logId: 'X' })
        const log3 = new Log(ipfs, testIdentity4, { logId: 'X' })
        const log = new Log(ipfs, testIdentity2, { logId: 'X' })

        for (let i = 1; i <= 5; i++) {
          await logA.append('entryA' + i, nextPointerAmount)
        }

        for (let i = 1; i <= 5; i++) {
          await logB.append('entryB' + i, nextPointerAmount)
        }

        await log3.join(logA)
        await log3.join(logB)

        for (let i = 6; i <= 10; i++) {
          await logA.append('entryA' + i, nextPointerAmount)
        }

        await log.join(log3)
        await log.append('entryC0', nextPointerAmount)

        await log.join(logA)

        const hash = await log.toMultihash()

        // First 5
        let res = await Log.fromMultihash(ipfs, testIdentity2, hash, { length: 5 })

        const first5 = [
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.data.payload), first5)

        // First 11
        res = await Log.fromMultihash(ipfs, testIdentity2, hash, { length: 11 })

        const first11 = [
          'entryB3', 'entryA4', 'entryB4',
          'entryA5', 'entryB5',
          'entryA6',
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.data.payload), first11)

        // All but one
        res = await Log.fromMultihash(ipfs, testIdentity2, hash, { length: 16 - 1 })

        const all = [
          /* excl */ 'entryB1', 'entryA2', 'entryB2', 'entryA3', 'entryB3',
          'entryA4', 'entryB4', 'entryA5', 'entryB5',
          'entryA6',
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.data.payload), all)
      })

      it('retrieves partially joined log deterministically - multiple next pointers', async () => {
        const nextPointersAmount = 64

        const logA = new Log(ipfs, testIdentity, { logId: 'X' })
        const logB = new Log(ipfs, testIdentity3, { logId: 'X' })
        const log3 = new Log(ipfs, testIdentity4, { logId: 'X' })
        const log = new Log(ipfs, testIdentity2, { logId: 'X' })

        for (let i = 1; i <= 5; i++) {
          await logA.append('entryA' + i, nextPointersAmount)
        }

        for (let i = 1; i <= 5; i++) {
          await logB.append('entryB' + i, nextPointersAmount)
        }

        await log3.join(logA)
        await log3.join(logB)

        for (let i = 6; i <= 10; i++) {
          await logA.append('entryA' + i, nextPointersAmount)
        }

        await log.join(log3)
        await log.append('entryC0', nextPointersAmount)

        await log.join(logA)

        const hash = await log.toMultihash()

        // First 5
        let res = await Log.fromMultihash(ipfs, testIdentity2, hash, { length: 5 })

        const first5 = [
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.data.payload), first5)

        // First 11
        res = await Log.fromMultihash(ipfs, testIdentity2, hash, { length: 11 })

        const first11 = [
          'entryB3', 'entryA4', 'entryB4', 'entryA5',
          'entryB5', 'entryA6',
          'entryC0',
          'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.data.payload), first11)

        // All but one
        res = await Log.fromMultihash(ipfs, testIdentity2, hash, { length: 16 - 1 })

        const all = [
          /* excl */ 'entryB1', 'entryA2', 'entryB2', 'entryA3', 'entryB3',
          'entryA4', 'entryB4', 'entryA5', 'entryB5',
          'entryA6',
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.data.payload), all)
      })

      it('throws an error if ipfs is not defined', async () => {
        let err
        try {
          await Log.fromEntry<string>(undefined as any, undefined, undefined as any, undefined as any)
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, null)
        assert.strictEqual(err.message, 'IPFS instance not defined')
      })

      describe('fetches a log', () => {
        const amount = 100
        let items1: Entry<string>[] = []
        let items2: Entry<string>[] = []
        let items3: Entry<string>[] = []
        let log1, log2, log3

        beforeEach(async () => {
          const ts = new Date().getTime()
          log1 = new Log(ipfs, testIdentity, { logId: 'X' })
          log2 = new Log(ipfs, testIdentity2, { logId: 'X' })
          log3 = new Log(ipfs, testIdentity3, { logId: 'X' })
          items1 = []
          items2 = []
          items3 = []
          for (let i = 1; i <= amount; i++) {
            const prev1 = last(items1)
            const prev2 = last(items2)
            const prev3 = last(items3)
            const n1 = await Entry.create({ ipfs, identity: log1._identity, logId: log1.id, data: 'entryA' + i + '-' + ts, next: [prev1], clock: log1.clock })
            const n2 = await Entry.create({ ipfs, identity: log2._identity, logId: log2.id, data: 'entryB' + i + '-' + ts, next: [prev2, n1], clock: log2.clock })
            const n3 = await Entry.create({ ipfs, identity: log3._identity, logId: log3.id, data: 'entryC' + i + '-' + ts, next: [prev3, n1, n2], clock: log3.clock })
            log1.clock.tick()
            log2.clock.tick()
            log3.clock.tick()
            log1.clock.merge(log2.clock)
            log1.clock.merge(log3.clock)
            log2.clock.merge(log1.clock)
            log2.clock.merge(log3.clock)
            log3.clock.merge(log1.clock)
            log3.clock.merge(log2.clock)
            items1.push(n1)
            items2.push(n2)
            items3.push(n3)
          }
        })

        it('returns all entries - no excluded entries', async () => {
          const a = await Log.fromEntry<string>(ipfs, testIdentity, last(items1),
            { length: -1 })
          assert.strictEqual(a.length, amount)
          assert.strictEqual(a.values[0].hash, items1[0].hash)
        })

        it('returns all entries - including excluded entries', async () => {
          // One entry
          const a = await Log.fromEntry<string>(ipfs, testIdentity, last(items1),
            { length: -1, exclude: [items1[0]] })
          assert.strictEqual(a.length, amount)
          assert.strictEqual(a.values[0].hash, items1[0].hash)

          // All entries
          const b = await Log.fromEntry<string>(ipfs, testIdentity, last(items1),
            { length: -1, exclude: items1 })
          assert.strictEqual(b.length, amount)
          assert.strictEqual(b.values[0].hash, items1[0].hash)
        })

        it('respects timeout parameter', async () => {
          const e = last(items1)
          e.hash = 'zdpuAwNuRc2Kc1aNDdcdSWuxfNpHRJQw8L8APBNHCEFXbogus'
          const timeout = 500
          const st = new Date().getTime()
          const log = await Log.fromEntry<string>(ipfs, testIdentity, e, { timeout })
          const et = new Date().getTime()
          assert.strictEqual((et - st) >= (timeout - 10), true, '' + (et - st) + ' should be greater than timeout ' + timeout)
          assert.strictEqual(log.length, 1)
          assert.deepStrictEqual(log.values.map(e => e.data.payload), [e.data.payload])
        })
      })
    })

    /*  describe('Backwards-compatibility v0', () => {
       const entries = [v0Entries.hello, v0Entries.helloWorld, v0Entries.helloAgain]
       beforeAll(async () => {
         await Promise.all(entries.map(e => io.write(ipfs, Entry.getWriteFormat(e), Entry.toEntry(e), { links: Entry.IPLD_LINKS })))
       })
 
       it('creates a log from v0 json', async () => {
         const headHash = await io.write(ipfs, 'dag-pb', Entry.toEntry(v0Entries.helloAgain), { links: Entry.IPLD_LINKS })
         const json = { id: 'A', heads: [headHash] }
         json.heads = await Promise.all(json.heads.map(headHash => Entry.fromMultihash(ipfs, headHash)))
         const log = await Log.fromJSON(ipfs, testIdentity, json, {})
         assert.strictEqual(log.length, 2)
       })
 
       it('creates a log from v0 entry', async () => {
         const log = await Log.fromEntry<string>(ipfs, testIdentity, [Entry.toEntry(v0Entries.helloAgain, { includeHash: true })], {})
         assert.strictEqual(log.length, 2)
       })
 
       it('creates a log from v0 entry hash', async () => {
         const log = await Log.fromEntryHash(ipfs, testIdentity, v0Entries.helloAgain.hash, { logId: 'A' })
         assert.strictEqual(log.length, 2)
       })
 
       it('creates a log from log hash of v0 entries', async () => {
         const log1 = new Log(ipfs, testIdentity, { entries: entries })
         const hash = await log1.toMultihash()
         const log = await Log.fromMultihash(ipfs, testIdentity, hash, {})
         assert.strictEqual(log.length, 3)
         assert.strictEqual(log.heads.length, 2)
       })
     })
 
     describe('Backwards-compatibility v1', () => {
       beforeAll(async () => {
         await Promise.all(v1Entries.map(e => io.write(ipfs, Entry.getWriteFormat(e), Entry.toEntry(e), { links: Entry.IPLD_LINKS })))
       })
 
       it('creates a log from v1 json', async () => {
         const headHash = await io.write(ipfs, 'dag-cbor', Entry.toEntry(v1Entries[v1Entries.length - 1]), { links: Entry.IPLD_LINKS })
         const json = { id: 'A', heads: [headHash] }
         json.heads = await Promise.all(json.heads.map(headHash => Entry.fromMultihash(ipfs, headHash)))
         const log = await Log.fromJSON(ipfs, testIdentity, json)
         assert.strictEqual(log.length, 5)
         assert.deepStrictEqual(log.values, v1Entries.map(e => Entry.toEntry(e, { includeHash: true })))
       })
 
       it('creates a log from v1 entry', async () => {
         const log = await Log.fromEntry<string>(ipfs, testIdentity, v1Entries[v1Entries.length - 1], {})
         assert.strictEqual(log.length, 5)
         assert.deepStrictEqual(log.values, v1Entries.map(e => Entry.toEntry(e, { includeHash: true })))
       })
 
       it('creates a log from v1 entry hash', async () => {
         const log = await Log.fromEntryHash(ipfs, testIdentity, v1Entries[v1Entries.length - 1].hash, { logId: 'A' })
         assert.strictEqual(log.length, 5)
         assert.deepStrictEqual(log.values, v1Entries.map(e => Entry.toEntry(e, { includeHash: true })))
       })
 
       it('creates a log from log hash of v1 entries', async () => {
         const log1 = new Log(ipfs, testIdentity, { entries: v1Entries })
         const hash = await log1.toMultihash()
         const log = await Log.fromMultihash(ipfs, testIdentity, hash, {})
         assert.strictEqual(log.length, 5)
         assert.deepStrictEqual(log.values, v1Entries.map(e => Entry.toEntry(e, { includeHash: true })))
       })
     }) */
  })
})
