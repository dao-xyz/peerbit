import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { LastWriteWins } from '../log-sorting.js'
import bigLogString from './fixtures/big-log.fixture.js';
import { Entry, JSON_ENCODING_OPTIONS } from '@dao-xyz/ipfs-log-entry';
import { Log } from '../log.js'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { LogCreator } from './utils/log-creator.js'
import { Ed25519PublicKey } from '@dao-xyz/peerbit-crypto';
import { arraysCompare } from '@dao-xyz/borsh-utils';
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
} = require('@dao-xyz/orbit-db-test-utils')

let ipfsd, ipfs, signKey: SignKeyWithMeta, signKey2: SignKeyWithMeta, signKey3: SignKeyWithMeta, signKey4: SignKeyWithMeta

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

    let keystore: Keystore, signingKeystore: Keystore
    let signKeys: SignKeyWithMeta[]

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      signKeys = []
      for (let i = 0; i < 4; i++) {
        signKeys.push(await keystore.createKey(new Uint8Array([i]), SignKeyWithMeta, undefined, { overwrite: true }))
      }
      signKeys.sort((a, b) => {
        return arraysCompare(new Uint8Array(a.publicKey.getBuffer()), new Uint8Array(b.publicKey.getBuffer()))
      });

      signKey = signKeys[0];
      signKey2 = signKeys[1];
      signKey3 = signKeys[2];
      signKey4 = signKeys[3];

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



      it('creates a log from an entry', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const data = fixture.log
        const json = fixture.json
        const log = await Log.fromJSON(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), json, {})
        expect(log.heads[0].gid).toEqual(data.heads[0].gid)
        expect(log.length).toEqual(16)
        assert.deepStrictEqual(log.values.map(e => e.init({ encoding: JSON_ENCODING_OPTIONS }).payload.value), fixture.expectedData)
      })

      it('creates a log from an entry with custom tiebreaker', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const data = fixture.log
        const json = fixture.json

        const log = await Log.fromJSON(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), json,
          { length: -1, sortFn: FirstWriteWins })

        expect(log.heads[0].gid).toEqual(data.heads[0].gid)
        expect(log.length).toEqual(16)
        assert.deepStrictEqual(log.values.map(e => e.init({ encoding: JSON_ENCODING_OPTIONS }).payload.value), firstWriteExpectedData)
      })

      it('respects timeout parameter', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const json = fixture.json
        json.heads = ['zdpuAwNuRc2Kc1aNDdcdSWuxfNpHRJQw8L8APBNHCEFXbogus'];
        const timeout = 500
        const st = new Date().getTime()
        const log = await Log.fromJSON(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), json, { timeout })
        const et = new Date().getTime()
        // Allow for a few millseconds of skew
        assert.strictEqual((et - st) >= (timeout - 10), true, '' + (et - st) + ' should be greater than timeout ' + timeout)
        expect(log.length).toEqual(0)
        assert.deepStrictEqual(log.values.map(e => e.init({ encoding: JSON_ENCODING_OPTIONS }).payload.value), [])
      })
    })

    describe('fromEntryHash', () => {

      it('creates a log from an entry hash', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const data = fixture.log
        const json = fixture.json

        const log1 = await Log.fromEntryHash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), json.heads[0],
          { logId: 'X' })
        const log2 = await Log.fromEntryHash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), json.heads[1],
          { logId: 'X' })

        await log1.join(log2)

        expect(log1.heads[0].gid).toEqual(data.heads[0].gid)
        expect(log1.length).toEqual(16)
        assert.deepStrictEqual(log1.values.map(e => e.init({ encoding: JSON_ENCODING_OPTIONS }).payload.value), fixture.expectedData)
      })

      it('creates a log from an entry hash with custom tiebreaker', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const data = fixture.log
        const json = fixture.json
        const log1 = await Log.fromEntryHash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), json.heads[0],
          { logId: 'X', sortFn: FirstWriteWins })
        const log2 = await Log.fromEntryHash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), json.heads[1],
          { logId: 'X', sortFn: FirstWriteWins })

        await log1.join(log2)

        expect(log1.heads[0].gid).toEqual(data.heads[0].gid)
        expect(log1.length).toEqual(16)
        assert.deepStrictEqual(log1.values.map(e => e.init({ encoding: JSON_ENCODING_OPTIONS }).payload.value), firstWriteExpectedData)
      })

      it('respects timeout parameter', async () => {
        const timeout = 500
        const st = new Date().getTime()
        const log = await Log.fromEntryHash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), 'zdpuAwNuRc2Kc1aNDdcdSWuxfNpHRJQw8L8APBNHCEFXbogus', { logId: 'X', timeout })
        const et = new Date().getTime()
        assert.strictEqual((et - st) >= timeout, true, '' + (et - st) + ' should be greater than timeout ' + timeout)
        expect(log.length).toEqual(0)
        assert.deepStrictEqual(log.values.map(e => e.init({ encoding: JSON_ENCODING_OPTIONS }).payload.value), [])
      })
    })

    describe('fromEntry', () => {

      it('creates a log from an entry', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const data = fixture.log

        const log = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), data.heads, { length: -1 })
        expect(log.heads[0].gid).toEqual(data.heads[0].gid)
        expect(log.length).toEqual(16)
        assert.deepStrictEqual(log.values.map(e => e.init({ encoding: JSON_ENCODING_OPTIONS }).payload.value), fixture.expectedData)
      })

      it('creates a log from an entry with custom tiebreaker', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const data = fixture.log

        const log = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), data.heads,
          { length: -1, sortFn: FirstWriteWins })
        expect(log.length).toEqual(16)
        assert.deepStrictEqual(log.values.map(e => e.init({ encoding: JSON_ENCODING_OPTIONS }).payload.value), firstWriteExpectedData)
      })

      it('keeps the original heads', async () => {
        const fixture = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const data = fixture.log
        const log1 = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), data.heads,
          { length: data.heads.length })
        expect(log1.heads[0].gid).toEqual(data.heads[0].gid)
        expect(log1.length).toEqual(data.heads.length)
        expect(log1.values[0].payload.value).toEqual('entryC0')
        expect(log1.values[1].payload.value).toEqual('entryA10')

        const log2 = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), data.heads, { length: 4 })
        expect(log2.heads[0].gid).toEqual(data.heads[0].gid)
        expect(log2.length).toEqual(4)
        expect(log2.values[0].payload.value).toEqual('entryC0')
        expect(log2.values[1].payload.value).toEqual('entryA8')
        expect(log2.values[2].payload.value).toEqual('entryA9')
        expect(log2.values[3].payload.value).toEqual('entryA10')

        const log3 = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), data.heads, { length: 7 })
        expect(log3.heads[0].gid).toEqual(data.heads[0].gid)
        expect(log3.length).toEqual(7)
        expect(log3.values[0].payload.value).toEqual('entryB5')
        expect(log3.values[1].payload.value).toEqual('entryA6')
        expect(log3.values[2].payload.value).toEqual('entryC0')
        expect(log3.values[3].payload.value).toEqual('entryA7')
        expect(log3.values[4].payload.value).toEqual('entryA8')
        expect(log3.values[5].payload.value).toEqual('entryA9')
        expect(log3.values[6].payload.value).toEqual('entryA10')
      })

      it('onProgress callback is fired for each entry', async () => {
        const items1: Entry<string>[] = []
        const amount = 100
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const n1 = await Entry.create({
            ipfs, publicKey: new Ed25519PublicKey({
              publicKey: signKey.publicKey
            }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryA' + i, next: prev1 ? [prev1] : undefined
          })
          items1.push(n1)
        }

        let i = 0
        const callback = (entry: Entry<string>) => {
          entry.init({ encoding: JSON_ENCODING_OPTIONS });
          assert.notStrictEqual(entry, null)
          expect(entry.hash).toEqual(items1[items1.length - i - 1].hash)
          expect(entry.payload.value).toEqual(items1[items1.length - i - 1].payload.value)
          i++
        }

        await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), last(items1),
          { length: -1, exclude: [], onProgressCallback: callback })
      })

      it('retrieves partial log from an entry hash', async () => {
        const log1 = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        const log2 = new Log<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })
        const log3 = new Log<string>(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 100
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          const n1 = await Entry.create({ ipfs, publicKey: log1._publicKey, sign: (data) => Keystore.sign(data, signKey), gidSeed: 'X', data: 'entryA' + i, next: prev1 ? [prev1] : undefined })
          const n2 = await Entry.create({ ipfs, publicKey: log2._publicKey, sign: (data) => Keystore.sign(data, signKey2), gidSeed: 'X', data: 'entryB' + i, next: prev2 ? [prev2, n1] : [n1] })
          const n3 = await Entry.create({ ipfs, publicKey: log3._publicKey, sign: (data) => Keystore.sign(data, signKey3), gidSeed: 'X', data: 'entryC' + i, next: prev3 ? [prev3, n1, n2] : [n1, n2] })
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        // limit to 10 entries
        const a = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), last(items1), { length: 10 })
        expect(a.length).toEqual(10)

        // limit to 42 entries
        const b = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), last(items1), { length: 42 })
        expect(b.length).toEqual(42)
      })

      it('retrieves full log from an entry hash', async () => {
        const log1 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        const log2 = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })
        const log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 10
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          const n1 = await Entry.create({ ipfs, publicKey: log1._publicKey, sign: (data) => Keystore.sign(data, signKey), gidSeed: 'X', data: 'entryA' + i, next: prev1 ? [prev1] : undefined })
          const n2 = await Entry.create({ ipfs, publicKey: log2._publicKey, sign: (data) => Keystore.sign(data, signKey2), gidSeed: 'X', data: 'entryB' + i, next: prev2 ? [prev2, n1] : [n1] })
          const n3 = await Entry.create({ ipfs, publicKey: log3._publicKey, sign: (data) => Keystore.sign(data, signKey3), gidSeed: 'X', data: 'entryC' + i, next: prev3 ? [prev3, n1, n2] : [n1, n2] })
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        const a = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), [last(items1)],
          { length: amount })
        expect(a.length).toEqual(amount)

        const b = await Log.fromEntry<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), [last(items2)],
          { length: amount * 2 })
        expect(b.length).toEqual(amount * 2)

        const c = await Log.fromEntry<string>(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), [last(items3)],
          { length: amount * 3 })
        expect(c.length).toEqual(amount * 3)
      })

      it('retrieves full log from an entry hash 2', async () => {
        const log1 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        const log2 = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })
        const log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 10
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          const n1 = await Entry.create({ ipfs, publicKey: log1._publicKey, sign: (data) => Keystore.sign(data, signKey), gidSeed: 'X', data: 'entryA' + i, next: prev1 ? [prev1] : undefined })
          const n2 = await Entry.create({ ipfs, publicKey: log2._publicKey, sign: (data) => Keystore.sign(data, signKey2), gidSeed: 'X', data: 'entryB' + i, next: prev2 ? [prev2, n1] : [n1] })
          const n3 = await Entry.create({ ipfs, publicKey: log3._publicKey, sign: (data) => Keystore.sign(data, signKey3), gidSeed: 'X', data: 'entryC' + i, next: prev3 ? [prev3, n1, n2] : [n1, n2] })
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        const a = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), last(items1),
          { length: amount })
        expect(a.length).toEqual(amount)

        const b = await Log.fromEntry<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), last(items2),
          { length: amount * 2 })
        expect(b.length).toEqual(amount * 2)

        const c = await Log.fromEntry<string>(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), last(items3),
          { length: amount * 3 })
        expect(c.length).toEqual(amount * 3)
      })

      it('retrieves full log from an entry hash 3', async () => {
        const log1 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        const log2 = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })
        const log3 = new Log(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey4), { logId: 'X' })
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 10
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          /*        log1.tickClock()
                 log2.tickClock()
                 log3.tickClock() */
          const n1 = await Entry.create({ ipfs, publicKey: log1._publicKey, sign: (data) => Keystore.sign(data, signKey), gidSeed: 'X', data: 'entryA' + i, next: prev1 ? [prev1] : undefined, clock: items1.length > 0 ? items1[items1.length - 1].clock.advance() : undefined })
          const n2 = await Entry.create({ ipfs, publicKey: log2._publicKey, sign: (data) => Keystore.sign(data, signKey2), gidSeed: 'X', data: 'entryB' + i, next: prev2 ? [prev2, n1] : [n1], clock: items2.length > 0 ? items2[items2.length - 1].clock.advance() : undefined })
          const n3 = await Entry.create({ ipfs, publicKey: log3._publicKey, sign: (data) => Keystore.sign(data, signKey3), gidSeed: 'X', data: 'entryC' + i, next: prev3 ? [prev3, n1, n2] : [n1, n2], clock: items3.length > 0 ? items3[items3.length - 1].clock.advance() : undefined })
          /*        log1.mergeClock(log2.clock)
                 log1.mergeClock(log3.clock)
                 log2.mergeClock(log1.clock)
                 log2.mergeClock(log3.clock)
                 log3.mergeClock(log1.clock)
                 log3.mergeClock(log2.clock) */
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        const a = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), last(items1),
          { length: amount })
        expect(a.length).toEqual(amount)

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

        const b = await Log.fromEntry<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), last(items2),
          { length: amount * 2 })
        expect(b.length).toEqual(amount * 2)
        assert.deepStrictEqual(b.values.map((e) => e.payload.value), itemsInB)

        const c = await Log.fromEntry<string>(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey4), last(items3),
          { length: amount * 3 })
        await c.append('EOF')
        expect(c.length).toEqual(amount * 3 + 1)

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

        assert.deepStrictEqual(c.values.map(e => e.payload.value), tmp)

        // make sure logX comes after A, B and C
        const logX = new Log<string>(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey4), { logId: 'X' })
        await logX.append('1')
        await logX.append('2')
        await logX.append('3')
        const d = await Log.fromEntry<string>(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), last(logX.values),
          { length: -1 })

        await c.join(d)
        await d.join(c)

        await c.append('DONE')
        await d.append('DONE')
        const f = await Log.fromEntry<string>(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), last(c.values),
          { length: -1, exclude: [] })
        const g = await Log.fromEntry<string>(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), last(d.values),
          { length: -1, exclude: [] })

        expect(f.toString()).toEqual(bigLogString)
        expect(g.toString()).toEqual(bigLogString)
      })

      it('retrieves full log of randomly joined log', async () => {
        const log1 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        const log2 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
        const log3 = new Log(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey4), { logId: 'X' })

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

        assert.deepStrictEqual(log1.values.map(e => e.payload.value), expectedData)
      })

      it('retrieves randomly joined log deterministically', async () => {
        const logA = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        const logB = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
        const log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey4), { logId: 'X' })
        const log = new Log(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })

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

        expect(log.values.map(e => e.payload.value)).toStrictEqual(expectedData)
      })

      it('sorts', async () => {
        const testLog = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
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
        assert.deepStrictEqual(fetchOrder.map(e => e.payload.value), expectedData)

        const reverseOrder = log.values.slice().reverse().sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder, reverseOrder)

        const hashOrder = log.values.slice().sort((a, b) => a.hash.localeCompare(b.hash)).sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder, hashOrder)

        const randomOrder2 = log.values.slice().sort((a, b) => 0.5 - Math.random()).sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder, randomOrder2)

        // partial data
        const partialLog = log.values.filter(e => e.payload.value !== 'entryC0').sort(Entry.compare)
        assert.deepStrictEqual(partialLog.map(e => e.payload.value), expectedData2)

        const partialLog2 = log.values.filter(e => e.payload.value !== 'entryA10').sort(Entry.compare)
        assert.deepStrictEqual(partialLog2.map(e => e.payload.value), expectedData3)

        const partialLog3 = log.values.filter(e => e.payload.value !== 'entryB5').sort(Entry.compare)
        assert.deepStrictEqual(partialLog3.map(e => e.payload.value), expectedData4)
      })

      it('sorts deterministically from random order', async () => {
        const testLog = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const log = testLog.log
        const expectedData = testLog.expectedData

        const fetchOrder = log.values.slice().sort(Entry.compare)
        assert.deepStrictEqual(fetchOrder.map(e => e.payload.value), expectedData)

        let sorted
        for (let i = 0; i < 1000; i++) {
          const randomOrder = log.values.slice().sort((a, b) => 0.5 - Math.random())
          sorted = randomOrder.sort(Entry.compare)
          assert.deepStrictEqual(sorted.map(e => e.payload.value), expectedData)
        }
      })

      it('sorts entries correctly', async () => {
        const testLog = await LogCreator.createLogWithTwoHundredEntries(ipfs, signKeys)
        const log = testLog.log
        const expectedData = testLog.expectedData
        assert.deepStrictEqual(log.values.map(e => e.payload.value), expectedData)
      })

      it('sorts entries according to custom tiebreaker function', async () => {
        const testLog = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)

        const firstWriteWinsLog =
          new Log(ipfs, signKeys[0].publicKey, (data) => Keystore.sign(data, signKeys[0]), { logId: 'X', sortFn: FirstWriteWins })
        await firstWriteWinsLog.join(testLog.log)
        assert.deepStrictEqual(firstWriteWinsLog.values.map(e => e.payload.value),
          firstWriteExpectedData)
      })

      it('throws an error if the tiebreaker returns zero', async () => {
        const testLog = await LogCreator.createLogWithSixteenEntries(ipfs, signKeys)
        const firstWriteWinsLog =
          new Log(ipfs, signKeys[0].publicKey, (data) => Keystore.sign(data, signKeys[0]), { logId: 'X', sortFn: BadComparatorReturnsZero })
        await firstWriteWinsLog.join(testLog.log)
        assert.throws(() => firstWriteWinsLog.values, Error, 'Error Thrown')
      })

      it('retrieves partially joined log deterministically - single next pointer', async () => {
        const nextPointerAmount = 1

        const logA = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        const logB = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
        const log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey4), { logId: 'X' })
        const log = new Log(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })

        for (let i = 1; i <= 5; i++) {
          await logA.append('entryA' + i, { nexts: logA.heads  /* logA.getPow2Refs(nextPointersAmount) */ })
        }

        for (let i = 1; i <= 5; i++) {
          await logB.append('entryB' + i, { nexts: logB.heads /* logB.getPow2Refs(nextPointersAmount) */ })
        }

        await log3.join(logA)
        await log3.join(logB)

        for (let i = 6; i <= 10; i++) {
          await logA.append('entryA' + i, { nexts: logA.heads /*  logA.getPow2Refs(nextPointersAmount) */ })
        }

        await log.join(log3)
        await log.append('entryC0', { nexts: logB.heads /* log.getPow2Refs(nextPointersAmount) */ })

        await log.join(logA)

        const hash = await log.toMultihash()

        // First 5
        let res = await Log.fromMultihash(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), hash, { length: 5 })

        const first5 = [
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.payload.value), first5)

        // First 11
        res = await Log.fromMultihash(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), hash, { length: 11 })

        const first11 = [
          'entryB3', 'entryA4', 'entryB4',
          'entryA5', 'entryB5',
          'entryA6',
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.payload.value), first11)

        // All but one
        res = await Log.fromMultihash(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), hash, { length: 16 - 1 })

        const all = [
          /* excl */ 'entryB1', 'entryA2', 'entryB2', 'entryA3', 'entryB3',
          'entryA4', 'entryB4', 'entryA5', 'entryB5',
          'entryA6',
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.payload.value), all)
      })

      it('retrieves partially joined log deterministically - multiple next pointers', async () => {
        /*         const nextPointersAmount = 64
         */
        const logA = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        const logB = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
        const log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey4), { logId: 'X' })
        const log = new Log(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })

        for (let i = 1; i <= 5; i++) {
          await logA.append('entryA' + i, { nexts: logA.heads  /* logA.getPow2Refs(nextPointersAmount) */ })
        }

        for (let i = 1; i <= 5; i++) {
          await logB.append('entryB' + i, { nexts: logB.heads /* logB.getPow2Refs(nextPointersAmount) */ })
        }

        await log3.join(logA)
        await log3.join(logB)

        for (let i = 6; i <= 10; i++) {
          await logA.append('entryA' + i, { nexts: logA.heads /*  logA.getPow2Refs(nextPointersAmount) */ })
        }

        await log.join(log3)
        await log.append('entryC0', { nexts: logB.heads /* log.getPow2Refs(nextPointersAmount) */ })

        await log.join(logA)

        const hash = await log.toMultihash()

        // First 5
        let res = await Log.fromMultihash(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), hash, { length: 5 })

        const first5 = [
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.payload.value), first5)

        // First 11
        res = await Log.fromMultihash(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), hash, { length: 11 })

        const first11 = [
          'entryB3', 'entryA4', 'entryB4', 'entryA5',
          'entryB5', 'entryA6',
          'entryC0',
          'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.payload.value), first11)

        // All but one
        res = await Log.fromMultihash(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), hash, { length: 16 - 1 })

        const all = [
          /* excl */ 'entryB1', 'entryA2', 'entryB2', 'entryA3', 'entryB3',
          'entryA4', 'entryB4', 'entryA5', 'entryB5',
          'entryA6',
          'entryC0', 'entryA7', 'entryA8', 'entryA9', 'entryA10'
        ]

        assert.deepStrictEqual(res.values.map(e => e.payload.value), all)
      })


      describe('fetches a log', () => {
        const amount = 100
        let items1: Entry<string>[] = []
        let items2: Entry<string>[] = []
        let items3: Entry<string>[] = []
        let log1: Log<any>, log2: Log<any>, log3: Log<any>

        beforeEach(async () => {
          const ts = new Date().getTime()
          log1 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
          log2 = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })
          log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
          items1 = []
          items2 = []
          items3 = []
          for (let i = 1; i <= amount; i++) {
            const prev1 = last(items1)
            const prev2 = last(items2)
            const prev3 = last(items3)
            const n1 = await Entry.create({ ipfs, publicKey: log1._publicKey, sign: (data) => Keystore.sign(data, signKey), gidSeed: 'X', data: 'entryA' + i, next: prev1 ? [prev1] : undefined, clock: items1.length > 0 ? items1[items1.length - 1].clock.advance() : undefined })
            const n2 = await Entry.create({ ipfs, publicKey: log2._publicKey, sign: (data) => Keystore.sign(data, signKey2), gidSeed: 'X', data: 'entryB' + i, next: prev2 ? [prev2, n1] : [n1], clock: items2.length > 0 ? items2[items2.length - 1].clock.advance() : undefined })
            const n3 = await Entry.create({ ipfs, publicKey: log3._publicKey, sign: (data) => Keystore.sign(data, signKey3), gidSeed: 'X', data: 'entryC' + i, next: prev3 ? [prev3, n1, n2] : [n1, n2], clock: items3.length > 0 ? items3[items3.length - 1].clock.advance() : undefined })

            /*      log1.tickClock()
                 log2.tickClock()
                 log3.tickClock()
                 log1.mergeClock(log2.clock)
                 log1.mergeClock(log3.clock)
                 log2.mergeClock(log1.clock)
                 log2.mergeClock(log3.clock)
                 log3.mergeClock(log1.clock)
                 log3.mergeClock(log2.clock) */
            items1.push(n1)
            items2.push(n2)
            items3.push(n3)
          }
        })

        it('returns all entries - no excluded entries', async () => {
          const a = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), last(items1),
            { length: -1 })
          expect(a.length).toEqual(amount)
          expect(a.values[0].hash).toEqual(items1[0].hash)
        })

        it('returns all entries - including excluded entries', async () => {
          // One entry
          const a = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), last(items1),
            { length: -1, exclude: [items1[0]] })
          expect(a.length).toEqual(amount)
          expect(a.values[0].hash).toEqual(items1[0].hash)

          // All entries
          const b = await Log.fromEntry<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), last(items1),
            { length: -1, exclude: items1 })
          expect(b.length).toEqual(amount)
          expect(b.values[0].hash).toEqual(items1[0].hash)
        })
      })
    })
  })
})
