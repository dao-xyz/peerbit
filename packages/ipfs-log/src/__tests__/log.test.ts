
const assert = require('assert')
const rmrf = require('rimraf')
const { CID } = require('multiformats/cid')
const { base58btc } = require('multiformats/bases/base58')
import { LamportClock as Clock } from '../lamport-clock'
import { Entry } from '../entry'
import { Log } from '../log'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
const Keystore = require('orbit-db-keystore')
const fs = require('fs-extra')
const io = require('orbit-db-io')

// For tiebreaker testing
import { LastWriteWins } from '../log-sorting';
import { assertPayload } from './utils/assert'
const FirstWriteWins = (a, b) => LastWriteWins(a, b) * -1

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity, testIdentity2, testIdentity3

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log (' + IPFS + ')', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore, signingKeystore

    beforeAll(async () => {
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
      rmrf.sync(signingKeysPath)
      rmrf.sync(identityKeysPath)

      await keystore?.close()
      await signingKeystore?.close()
    })

    describe('constructor', () => {
      test('creates an empty log with default params', () => {
        const log = new Log(ipfs, testIdentity, undefined)
        assert.notStrictEqual(log._entryIndex, null)
        assert.notStrictEqual(log._headsIndex, null)
        assert.notStrictEqual(log._id, null)
        assert.notStrictEqual(log.id, null)
        assert.notStrictEqual(log.clock, null)
        assert.notStrictEqual(log.values, null)
        assert.notStrictEqual(log.heads, null)
        assert.notStrictEqual(log.tails, null)
        // assert.notStrictEqual(log.tailCids, null)
        assert.deepStrictEqual(log.values, [])
        assert.deepStrictEqual(log.heads, [])
        assert.deepStrictEqual(log.tails, [])
      })

      test('throws an error if IPFS instance is not passed as an argument', () => {
        let err
        try {
          const log = new Log(undefined as any, undefined, undefined as any) // eslint-disable-line no-unused-vars
        } catch (e) {
          err = e
        }
        assert.strictEqual(err.message, 'IPFS instance not defined')
      })

      test('sets an id', () => {
        const log = new Log(ipfs, testIdentity, { logId: 'ABC' })
        assert.strictEqual(log.id, 'ABC')
      })

      test('sets the clock id', () => {
        const log = new Log(ipfs, testIdentity, { logId: 'ABC' })
        assert.strictEqual(log.id, 'ABC')
        assert.strictEqual(log.clock.id, testIdentity.publicKey)
      })

      test('generates id string if id is not passed as an argument', () => {
        const log = new Log(ipfs, testIdentity, undefined)
        assert.strictEqual(typeof log.id === 'string', true)
      })

      test('sets items if given as params', async () => {
        const one = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryA')), [], new Clock('A', 0))
        const two = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryB')), [], new Clock('B', 0))
        const three = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryC')), [], new Clock('C', 0))
        const log = new Log(ipfs, testIdentity,
          { logId: 'A', entries: [one, two, three] })
        assert.strictEqual(log.length, 3)
        assertPayload(log.values[0].data.payload, 'entryA')
        assertPayload(log.values[1].data.payload, 'entryB')
        assertPayload(log.values[2].data.payload, 'entryC')
      })

      test('sets heads if given as params', async () => {
        const one = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryA')), [])
        const two = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryB')), [])
        const three = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryC')), [])
        const log = new Log(ipfs, testIdentity,
          { logId: 'B', entries: [one, two, three], heads: [three] })
        assert.strictEqual(log.heads.length, 1)
        assert.strictEqual(log.heads[0].hash, three.hash)
      })

      test('finds heads if heads not given as params', async () => {
        const one = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryA')), [])
        const two = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryB')), [])
        const three = await Entry.create(ipfs, testIdentity, 'A', new Uint8Array(Buffer.from('entryC')), [])
        const log = new Log(ipfs, testIdentity,
          { logId: 'A', entries: [one, two, three] })
        assert.strictEqual(log.heads.length, 3)
        assert.strictEqual(log.heads[2].hash, one.hash)
        assert.strictEqual(log.heads[1].hash, two.hash)
        assert.strictEqual(log.heads[0].hash, three.hash)
      })

      test('throws an error if entries is not an array', () => {
        let err
        try {
          const log = new Log(ipfs, testIdentity, { logId: 'A', entries: {} as any }) // eslint-disable-line no-unused-vars
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, undefined)
        assert.strictEqual(err.message, '\'entries\' argument must be an array of Entry instances')
      })

      test('throws an error if heads is not an array', () => {
        let err
        try {
          const log = new Log(ipfs, testIdentity, { logId: 'A', entries: [], heads: {} }) // eslint-disable-line no-unused-vars
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, undefined)
        assert.strictEqual(err.message, '\'heads\' argument must be an array')
      })

      test('creates default public AccessController if not defined', async () => {
        const log = new Log(ipfs, testIdentity)
        const anyoneCanAppend = await log._access.canAppend('any' as any, undefined)
        assert.notStrictEqual(log._access, undefined)
        assert.strictEqual(anyoneCanAppend, true)
      })

      test('throws an error if identity is not defined', () => {
        let err
        try {
          const log = new Log(ipfs, undefined)
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, undefined)
        assert.strictEqual(err.message, 'Identity is required')
      })
    })

    describe('toString', () => {
      let log
      const expectedData = 'five\n└─four\n  └─three\n    └─two\n      └─one'

      beforeEach(async () => {
        log = new Log(ipfs, testIdentity, { logId: 'A' })
        await log.append('one')
        await log.append('two')
        await log.append('three')
        await log.append('four')
        await log.append('five')
      })

      test('returns a nicely formatted string', () => {
        assert.strictEqual(log.toString(), expectedData)
      })
    })

    describe('get', () => {
      let log

      beforeEach(async () => {
        log = new Log(ipfs, testIdentity, { logId: 'AAA' })
        await log.append('one')
      })

      test('returns an Entry', () => {
        const entry = log.get(log.values[0].hash)
        assert.deepStrictEqual(entry.hash, 'zdpuAoFzNYcuuQHk1gLcB8fomHGrqT9k1uQeAvewZJ1cSYrms')
      })

      test('returns undefined when Entry is not in the log', () => {
        const entry = log.get('QmFoo')
        assert.deepStrictEqual(entry, undefined)
      })
    })

    describe('setIdentity', () => {
      let log

      beforeEach(async () => {
        log = new Log(ipfs, testIdentity, { logId: 'AAA' })
        await log.append('one')
      })

      test('changes identity', async () => {
        assert.strictEqual(log.values[0].data.clock.id, testIdentity.publicKey)
        assert.strictEqual(log.values[0].data.clock.time, 1)
        log.setIdentity(testIdentity2)
        await log.append('two')
        assert.strictEqual(log.values[1].data.clock.id, testIdentity2.publicKey)
        assert.strictEqual(log.values[1].data.clock.time, 2)
        log.setIdentity(testIdentity3)
        await log.append('three')
        assert.strictEqual(log.values[2].data.clock.id, testIdentity3.publicKey)
        assert.strictEqual(log.values[2].data.clock.time, 3)
      })
    })

    describe('has', () => {
      let log, expectedData

      beforeAll(async () => {
        expectedData = {
          hash: 'zdpuAoFzNYcuuQHk1gLcB8fomHGrqT9k1uQeAvewZJ1cSYrms',
          id: 'AAA',
          payload: 'one',
          next: [],
          v: 1,
          clock: new Clock(testIdentity.publicKey, 1),
          key: testIdentity.toSerializable()
        }

        const sig = await testIdentity.provider.sign(testIdentity, Buffer.from(JSON.stringify(expectedData)))
        Object.assign(expectedData, { sig })
      })

      beforeEach(async () => {
        log = new Log(ipfs, testIdentity, { logId: 'AAA' })
        await log.append('one')
      })

      test('returns true if it has an Entry', () => {
        assert.strictEqual(log.has(expectedData), true)
      })

      test('returns true if it has an Entry, hash lookup', () => {
        assert.strictEqual(log.has(expectedData.hash), true)
      })

      test('returns false if it doesn\'t have the Entry', () => {
        assert.strictEqual(log.has('zdFoo'), false)
      })
    })

    describe('serialize', () => {
      let log
      const expectedData = {
        id: 'AAA',
        heads: ['zdpuApASvEM59JKWn7Y39JWVSoiQ2CoJWpWseNTzqWvX1dRtC']
      }

      beforeEach(async () => {
        log = new Log(ipfs, testIdentity, { logId: 'AAA' })
        await log.append('one')
        await log.append('two')
        await log.append('three')
      })

      describe('toJSON', () => {
        test('returns the log in JSON format', () => {
          assert.strictEqual(JSON.stringify(log.toJSON()), JSON.stringify(expectedData))
        })
      })

      describe('toSnapshot', () => {
        const expectedData = {
          id: 'AAA',
          heads: ['zdpuApASvEM59JKWn7Y39JWVSoiQ2CoJWpWseNTzqWvX1dRtC'],
          values: [
            'zdpuAoFzNYcuuQHk1gLcB8fomHGrqT9k1uQeAvewZJ1cSYrms',
            'zdpuAo5DjP7XfnJqe8v8RTedi44Xg2w49Wb9xwRBdzf3LNJCV',
            'zdpuApASvEM59JKWn7Y39JWVSoiQ2CoJWpWseNTzqWvX1dRtC'
          ]
        }

        test('returns the log snapshot', () => {
          const snapshot = log.toSnapshot()
          assert.strictEqual(snapshot.id, expectedData.id)
          assert.strictEqual(snapshot.heads.length, expectedData.heads.length)
          assert.strictEqual(snapshot.heads[0].hash, expectedData.heads[0])
          assert.strictEqual(snapshot.values.length, expectedData.values.length)
          assert.strictEqual(snapshot.values[0].hash, expectedData.values[0])
          assert.strictEqual(snapshot.values[1].hash, expectedData.values[1])
          assert.strictEqual(snapshot.values[2].hash, expectedData.values[2])
        })
      })

      describe('toBuffer', () => {
        test('returns the log as a Buffer', () => {
          assert.deepStrictEqual(log.toBuffer(), Buffer.from(JSON.stringify(expectedData)))
        })
      })

      describe('toMultihash - cbor', () => {
        test('returns the log as ipfs CID', async () => {
          const expectedCid = 'zdpuAwC43AQmYEPAnmidtfuUdBQSWuK95z1446UntBMhrqdto'
          const log = new Log(ipfs, testIdentity, { logId: 'A' })
          await log.append('one')
          const hash = await log.toMultihash()
          assert.strictEqual(hash, expectedCid)
        })

        test('log serialized to ipfs contains the correct data', async () => {
          const expectedData = {
            id: 'A',
            heads: ['zdpuAky58cAEgNyxPGotdCZny1sfk7ima9FJVtPTydDgrCFZw']
          }
          const expectedCid = 'zdpuAwC43AQmYEPAnmidtfuUdBQSWuK95z1446UntBMhrqdto'
          const log = new Log(ipfs, testIdentity, { logId: 'A' })
          await log.append('one')
          const hash = await log.toMultihash()
          assert.strictEqual(hash, expectedCid)
          const result = await io.read(ipfs, hash)
          const heads = result.heads.map(head => head.toString(base58btc))
          assert.deepStrictEqual(heads, expectedData.heads)
        })

        test('throws an error if log items is empty', async () => {
          const emptyLog = new Log(ipfs, testIdentity)
          let err
          try {
            await emptyLog.toMultihash()
          } catch (e) {
            err = e
          }
          assert.notStrictEqual(err, null)
          assert.strictEqual(err.message, 'Can\'t serialize an empty log')
        })
      })

      describe('toMultihash - pb', () => {
        test('returns the log as ipfs multihash', async () => {
          const expectedMultihash = 'QmSgYrc2cbLghngrBWJtNvmh282BrUgtGxzjYhEuPgC7Sj'
          const log = new Log(ipfs, testIdentity, { logId: 'A' })
          await log.append('one')
          const multihash = await log.toMultihash({ format: 'dag-pb' })
          assert.strictEqual(multihash, expectedMultihash)
        })

        test('log serialized to ipfs contains the correct data', async () => {
          const expectedData = {
            id: 'A',
            heads: ['zdpuAky58cAEgNyxPGotdCZny1sfk7ima9FJVtPTydDgrCFZw']
          }
          const expectedMultihash = 'QmSgYrc2cbLghngrBWJtNvmh282BrUgtGxzjYhEuPgC7Sj'
          const log = new Log(ipfs, testIdentity, { logId: 'A' })
          await log.append('one')
          const multihash = await log.toMultihash({ format: 'dag-pb' })
          assert.strictEqual(multihash, expectedMultihash)
          const result = await ipfs.object.get(CID.parse(multihash))
          const res = JSON.parse(Buffer.from(result.Data).toString())
          assert.deepStrictEqual(res.heads, expectedData.heads)
        })

        test('throws an error if log items is empty', async () => {
          const emptyLog = new Log(ipfs, testIdentity)
          let err
          try {
            await emptyLog.toMultihash()
          } catch (e) {
            err = e
          }
          assert.notStrictEqual(err, null)
          assert.strictEqual(err.message, 'Can\'t serialize an empty log')
        })
      })

      describe('fromMultihash', () => {
        test('creates a log from ipfs CID - one entry', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuB23XC5xJBJbfk5d9EfpWjX56VqTu3z4CUzVf2mxVURnEy']
          }
          const log = new Log(ipfs, testIdentity, { logId: 'X' })
          await log.append('one')
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, hash, { length: -1 })
          assert.strictEqual(JSON.stringify(res.toJSON()), JSON.stringify(expectedData))
          assert.strictEqual(res.length, 1)
          assertPayload(res.values[0].data.payload, 'one')
          assert.strictEqual(res.values[0].data.clock.id, testIdentity.publicKey)
          assert.strictEqual(res.values[0].data.clock.time, 1)
        })

        test('creates a log from ipfs CID - three entries', async () => {
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, hash, { length: -1 })
          assert.strictEqual(res.length, 3)
          assertPayload(res.values[0].data.payload, 'one')
          assert.strictEqual(res.values[0].data.clock.time, 1)
          assertPayload(res.values[1].data.payload, 'two')
          assert.strictEqual(res.values[1].data.clock.time, 2)
          assertPayload(res.values[2].data.payload, 'three')
          assert.strictEqual(res.values[2].data.clock.time, 3)
        })

        test('creates a log from ipfs multihash (backwards compat)', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuB23XC5xJBJbfk5d9EfpWjX56VqTu3z4CUzVf2mxVURnEy']
          }
          const log = new Log(ipfs, testIdentity, { logId: 'X' })
          await log.append('one')
          const multihash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, multihash, { length: -1 })
          assert.strictEqual(JSON.stringify(res.toJSON()), JSON.stringify(expectedData))
          assert.strictEqual(res.length, 1)
          assertPayload(res.values[0].data.payload, 'one')
          assert.strictEqual(res.values[0].data.clock.id, testIdentity.publicKey)
          assert.strictEqual(res.values[0].data.clock.time, 1)
        })

        test('has the right sequence number after creation and appending', async () => {
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, hash, { length: -1 })
          assert.strictEqual(res.length, 3)
          await res.append('four')
          assert.strictEqual(res.length, 4)
          assertPayload(res.values[3].data.payload, 'four')
          assert.strictEqual(res.values[3].data.clock.time, 4)
        })

        test('creates a log from ipfs CID that has three heads', async () => {
          const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
          const log2 = new Log(ipfs, testIdentity2, { logId: 'A' })
          const log3 = new Log(ipfs, testIdentity3, { logId: 'A' })
          await log1.append('one') // order is determined by the identity's publicKey
          await log2.append('two')
          await log3.append('three')
          await log1.join(log2)
          await log1.join(log3)
          const hash = await log1.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, hash, { length: -1 })
          assert.strictEqual(res.length, 3)
          assert.strictEqual(res.heads.length, 3)
          assertPayload(res.heads[2].data.payload, 'three')
          assertPayload(res.heads[1].data.payload, 'two') // order is determined by the identity's publicKey
          assertPayload(res.heads[0].data.payload, 'one')
        })

        test('creates a log from ipfs CID that has three heads w/ custom tiebreaker', async () => {
          const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
          const log2 = new Log(ipfs, testIdentity2, { logId: 'A' })
          const log3 = new Log(ipfs, testIdentity3, { logId: 'A' })
          await log1.append('one') // order is determined by the identity's publicKey
          await log2.append('two')
          await log3.append('three')
          await log1.join(log2)
          await log1.join(log3)
          const hash = await log1.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, hash,
            { sortFn: FirstWriteWins })
          assert.strictEqual(res.length, 3)
          assert.strictEqual(res.heads.length, 3)
          assertPayload(res.heads[2].data.payload, 'one')
          assertPayload(res.heads[1].data.payload, 'two') // order is determined by the identity's publicKey
          assertPayload(res.heads[0].data.payload, 'three')
        })

        test('creates a log from ipfs CID up to a size limit', async () => {
          const amount = 100
          const size = amount / 2
          const log = new Log(ipfs, testIdentity, { logId: 'A' })
          for (let i = 0; i < amount; i++) {
            await log.append(i.toString())
          }
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, hash, { length: size })
          assert.strictEqual(res.length, size)
        })

        test('creates a log from ipfs CID up without size limit', async () => {
          const amount = 100
          const log = new Log(ipfs, testIdentity, { logId: 'A' })
          for (let i = 0; i < amount; i++) {
            await log.append(i.toString())
          }
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, hash, { length: -1 })
          assert.strictEqual(res.length, amount)
        })

        test('throws an error if ipfs is not defined', async () => {
          let err
          try {
            await Log.fromMultihash(undefined as any, undefined, undefined, undefined as any)
          } catch (e) {
            err = e
          }
          assert.notStrictEqual(err, null)
          assert.strictEqual(err.message, 'IPFS instance not defined')
        })

        test('throws an error if hash is not defined', async () => {
          let err
          try {
            await Log.fromMultihash(ipfs, undefined, undefined, undefined as any)
          } catch (e) {
            err = e
          }
          assert.notStrictEqual(err, null)
          assert.strictEqual(err.message, 'Invalid hash: undefined')
        })

        test('throws an error if data from hash is not valid JSON', async () => {
          const value = 'hello'
          const cid = CID.parse(await io.write(ipfs, 'dag-pb', value))
          let err
          try {
            const hash = cid.toString(base58btc)
            await Log.fromMultihash(ipfs, testIdentity, hash, undefined as any)
          } catch (e) {
            err = e
          }
          assert.strictEqual(err.message, 'Unexpected token h in JSON at position 0')
        })

        test('throws an error when data from CID is not instance of Log', async () => {
          const hash = await ipfs.dag.put({})
          let err
          try {
            await Log.fromMultihash(ipfs, testIdentity, hash, undefined as any)
          } catch (e) {
            err = e
          }
          assert.strictEqual(err.message, 'Given argument is not an instance of Log')
        })

        test('onProgress callback is fired for each entry', async () => {
          const amount = 100
          const log = new Log(ipfs, testIdentity, { logId: 'A' })
          for (let i = 0; i < amount; i++) {
            await log.append(i.toString())
          }

          const items = log.values
          let i = 0
          const loadProgressCallback = (entry) => {
            assert.notStrictEqual(entry, null)
            assert.strictEqual(entry.hash, items[items.length - i - 1].hash)
            assertPayload(entry.data.payload, items[items.length - i - 1].data.payload)
            i++
          }

          const hash = await log.toMultihash()
          const result = await Log.fromMultihash(ipfs, testIdentity, hash,
            { length: -1, exclude: [], onProgressCallback: loadProgressCallback })

          // Make sure the onProgress callback was called for each entry
          assert.strictEqual(i, amount)
          // Make sure the log entries are correct ones
          assert.strictEqual(result.values[0].data.clock.time, 1)
          assertPayload(result.values[0].data.payload, '0')
          assert.strictEqual(result.values[result.length - 1].data.clock.time, 100)
          assert.strictEqual(result.values[result.length - 1].data.payload, '99')
        })
      })

      describe('fromEntryHash', () => {
        afterEach(() => {
          if (Log.fromEntryHash["restore"]) {
            Log.fromEntryHash["restore"]()
          }
        })

        test('calls fromEntryHash', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuB23XC5xJBJbfk5d9EfpWjX56VqTu3z4CUzVf2mxVURnEy']
          }
          const log = new Log(ipfs, testIdentity, { logId: 'X' })
          await log.append('one')
          const res = await Log.fromEntryHash(ipfs, testIdentity, expectedData.heads[0],
            { logId: log.id, length: -1 })
          assert.strictEqual(JSON.stringify(res.toJSON()), JSON.stringify(expectedData))
        })
      })

      describe('fromMultihash', () => {
        afterEach(() => {
          if (Log.fromMultihash["restore"]) {
            Log.fromMultihash["restore"]()
          }
        })

        test('calls fromMultihash', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuB23XC5xJBJbfk5d9EfpWjX56VqTu3z4CUzVf2mxVURnEy']
          }
          const log = new Log(ipfs, testIdentity, { logId: 'X' })
          await log.append('one')
          const multihash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, multihash, { length: -1 })
          assert.strictEqual(JSON.stringify(res.toJSON()), JSON.stringify(expectedData))
        })

        test('calls fromMultihash with custom tiebreaker', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuB23XC5xJBJbfk5d9EfpWjX56VqTu3z4CUzVf2mxVURnEy']
          }
          const log = new Log(ipfs, testIdentity, { logId: 'X' })
          await log.append('one')
          const multihash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, testIdentity, multihash,
            { length: -1, sortFn: FirstWriteWins })
          assert.strictEqual(JSON.stringify(res.toJSON()), JSON.stringify(expectedData))
        })
      })
    })

    describe('values', () => {
      test('returns all entries in the log', async () => {
        const log = new Log(ipfs, testIdentity)
        assert.strictEqual(log.values instanceof Array, true)
        assert.strictEqual(log.length, 0)
        await log.append('hello1')
        await log.append('hello2')
        await log.append('hello3')
        assert.strictEqual(log.values instanceof Array, true)
        assert.strictEqual(log.length, 3)
        assertPayload(log.values[0].data.payload, 'hello1')
        assertPayload(log.values[1].data.payload, 'hello2')
        assertPayload(log.values[2].data.payload, 'hello3')
      })
    })
  })
})
