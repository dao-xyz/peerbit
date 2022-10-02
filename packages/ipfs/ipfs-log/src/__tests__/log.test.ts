
import assert from 'assert'
import rmrf from 'rimraf'
const { CID } = require('multiformats/cid')
const { base58btc } = require('multiformats/bases/base58')
import { Entry, getPeerID, LamportClock as Clock, Payload, Signature } from '@dao-xyz/ipfs-log-entry';
import { Log } from '../log.js'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import fs from 'fs-extra'
import io from '@dao-xyz/io-utils'

// For tiebreaker testing
import { LastWriteWins } from '../log-sorting.js';
import { DecryptedThing } from '@dao-xyz/encryption-utils';
import { serialize } from '@dao-xyz/borsh';
import { Ed25519PublicKey, PublicKey } from '@dao-xyz/identity';
const FirstWriteWins = (a, b) => LastWriteWins(a, b) * -1

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs
} from '@dao-xyz/orbit-db-test-utils'

let ipfsd, ipfs, signKey: SignKeyWithMeta, signKey2: SignKeyWithMeta, signKey3: SignKeyWithMeta

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore: Keystore, signingKeystore: Keystore

    beforeAll(async () => {
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      signKey = await keystore.getKeyByPath(new Uint8Array([0]), SignKeyWithMeta);
      signKey2 = await keystore.getKeyByPath(new Uint8Array([1]), SignKeyWithMeta);
      signKey3 = await keystore.getKeyByPath(new Uint8Array([2]), SignKeyWithMeta);
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
      it('creates an empty log with default params', () => {
        const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), undefined)
        assert.notStrictEqual(log._entryIndex, null)
        assert.notStrictEqual(log._headsIndex, null)
        assert.notStrictEqual(log._id, null)
        assert.notStrictEqual(log._id, null)
        assert.notStrictEqual(log.values, null)
        assert.notStrictEqual(log.heads, null)
        assert.notStrictEqual(log.tails, null)
        // assert.notStrictEqual(log.tailCids, null)
        assert.deepStrictEqual(log.values, [])
        assert.deepStrictEqual(log.heads, [])
        assert.deepStrictEqual(log.tails, [])
      })

      it('throws an error if IPFS instance is not passed as an argument', () => {
        let err
        try {
          const log = new Log(undefined as any, undefined, undefined as any) // eslint-disable-line no-unused-vars
        } catch (e) {
          err = e
        }
        expect(err.message).toEqual('IPFS instance not defined')
      })

      it('sets an id', async () => {
        const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'ABC' })
        expect(log._id).toEqual('ABC')
      })

      it('generates id string if id is not passed as an argument', () => {
        const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), undefined)
        assert.strictEqual(typeof log._id === 'string', true)
      })

      it('sets items if given as params', async () => {
        const one = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryA', next: [], clock: new Clock(new Uint8Array([0]), 0)
        })
        const two = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryB', next: [], clock: new Clock(new Uint8Array([1]), 0)
        })
        const three = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryC', next: [], clock: new Clock(new Uint8Array([2]), 0)
        })
        const log = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey),
          { logId: 'A', entries: [one, two, three] })
        expect(log.length).toEqual(3)
        expect(log.values[0].payload.value).toEqual('entryA')
        expect(log.values[1].payload.value).toEqual('entryB')
        expect(log.values[2].payload.value).toEqual('entryC')
      })

      it('sets heads if given as params', async () => {
        const one = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryA', next: []
        })
        const two = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryB', next: []
        })
        const three = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryC', next: []
        })
        const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey),
          { logId: 'B', entries: [one, two, three], heads: [three] })
        expect(log.heads.length).toEqual(1)
        expect(log.heads[0].hash).toEqual(three.hash)
      })

      it('finds heads if heads not given as params', async () => {
        const one = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryA', next: []
        })
        const two = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryB', next: []
        })
        const three = await Entry.create({
          ipfs, publicKey: new Ed25519PublicKey({
            publicKey: signKey.publicKey
          }), sign: (data) => Keystore.sign(data, signKey), gidSeed: 'A', data: 'entryC', next: []
        })
        const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey),
          { logId: 'A', entries: [one, two, three] })
        expect(log.heads.length).toEqual(3)
        expect(log.heads[2].hash).toEqual(one.hash)
        expect(log.heads[1].hash).toEqual(two.hash)
        expect(log.heads[0].hash).toEqual(three.hash)
      })

      it('throws an error if entries is not an array', () => {
        let err
        try {
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A', entries: {} as any }) // eslint-disable-line no-unused-vars
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, undefined)
        expect(err.message).toEqual('\'entries\' argument must be an array of Entry instances')
      })

      it('throws an error if heads is not an array', () => {
        let err
        try {
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A', entries: [], heads: {} }) // eslint-disable-line no-unused-vars
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, undefined)
        expect(err.message).toEqual('\'heads\' argument must be an array')
      })

      it('creates default public AccessController if not defined', async () => {
        const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey),)
        const anyoneCanAppend = await log._access.canAppend('any' as any, new DecryptedThing({
          data: serialize(signKey)
        }))
        assert.notStrictEqual(log._access, undefined)
        expect(anyoneCanAppend).toEqual(true)
      })

      it('throws an error if identity is not defined', () => {
        let err
        try {
          const log = new Log(ipfs, undefined, undefined)
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, undefined, undefined)
        expect(err.message).toEqual('Identity is required')
      })
    })

    describe('toString', () => {
      let log
      const expectedData = 'five\n└─four\n  └─three\n    └─two\n      └─one'

      beforeEach(async () => {
        log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
        await log.append('one')
        await log.append('two')
        await log.append('three')
        await log.append('four')
        await log.append('five')
      })

      it('returns a nicely formatted string', () => {
        expect(log.toString()).toEqual(expectedData)
      })
    })

    describe('get', () => {
      let log: Log<any>

      beforeEach(async () => {
        log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'AAA' })
        await log.append('one')
      })

      it('returns an Entry', () => {
        const entry = log.get(log.values[0].hash)
        assert.deepStrictEqual(entry.hash, 'zdpuAyzqV8JmwffQ3hydKY3jqJdN81u4DNYduFjtZf92CMmto')
      })

      it('returns undefined when Entry is not in the log', () => {
        const entry = log.get('QmFoo')
        assert.deepStrictEqual(entry, undefined)
      })
    })

    describe('setIdentity', () => {
      let log

      beforeEach(async () => {
        log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'AAA' })
        await log.append('one')
      })

      it('changes identity', async () => {
        assert.deepStrictEqual(log.values[0].clock.id, signKey.publicKey)
        expect(log.values[0].clock.time).toEqual(1)
        log.setIdentity(signKey2)
        await log.append('two')
        assert.deepStrictEqual(log.values[1].clock.id, signKey2.publicKey)
        expect(log.values[1].clock.time).toEqual(2)
        log.setIdentity(signKey3)
        await log.append('three')
        assert.deepStrictEqual(log.values[2].clock.id, signKey3.publicKey)
        expect(log.values[2].clock.time).toEqual(3)
      })
    })

    describe('has', () => {
      let log: Log<string>, expectedData: Entry<string>

      beforeAll(async () => {
        const clock = new Clock(new Uint8Array(signKey.publicKey.getBuffer()), 1)
        const clockDecrypted = new DecryptedThing<Clock>({ data: serialize(clock) });
        const payload = new DecryptedThing<Payload<string>>({
          data: serialize(new Payload<string>({
            data: new Uint8Array(Buffer.from('one'))
          }))
        });
        const gid = 'aaa';
        const publicKey = new DecryptedThing<PublicKey>({
          data: serialize(new Ed25519PublicKey({ publicKey: signKey.publicKey }))
        })
        expectedData = new Entry<string>({
          hash: 'zdpuAozwfaZEdTCimGoLbXrz3hsJdCQZATpVgyVDMJLVrACqw',
          payload,
          clock: clockDecrypted,
          gid,
          publicKey,
          signature: new DecryptedThing({
            data: serialize(new Signature({
              signature: await Keystore.sign(Entry.createDataToSign(gid, payload, clockDecrypted, [], [], 0, 0), signKey)
            }))
          }),
          next: []
        });
      })

      beforeEach(async () => {
        log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'AAA' })
        await log.append('one')
      })

      it('returns true if it has an Entry', () => {
        assert(log.has(expectedData))
      })

      it('returns true if it has an Entry, hash lookup', () => {
        assert(log.has(expectedData.hash))
      })

      it('returns false if it doesn\'t have the Entry', () => {
        assert.strictEqual(log.has('zdFoo'), false)
      })
    })

    describe('serialize', () => {
      let log
      const expectedData = {
        id: 'AAA',
        heads: ['zdpuAwowDaJLXfghq6SoDNZBX1Q1x2vQXG8QPHbBUUGcAaVdJ']
      }

      beforeEach(async () => {
        log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'AAA' })
        await log.append('one')
        await log.append('two')
        await log.append('three')
      })

      describe('toJSON', () => {
        it('returns the log in JSON format', () => {
          expect(JSON.stringify(log.toJSON())).toEqual(JSON.stringify(expectedData))
        })
      })

      describe('toSnapshot', () => {
        const expectedData = {
          id: 'AAA',
          heads: ['zdpuAwowDaJLXfghq6SoDNZBX1Q1x2vQXG8QPHbBUUGcAaVdJ'],
          values: [
            'zdpuAoPjdySyxksiVoK72NbVrg498d4kSXD2inKpNehzoHNfx',
            'zdpuArwikbBGXzxsbR9VpMoFnXjHCa1kdBZeemdkrdTXfw2CM',
            'zdpuAwowDaJLXfghq6SoDNZBX1Q1x2vQXG8QPHbBUUGcAaVdJ'
          ]
        }

        it('returns the log snapshot', () => {
          const snapshot = log.toSnapshot()
          expect(snapshot.id).toEqual(expectedData.id)
          expect(snapshot.heads.length).toEqual(expectedData.heads.length)
          expect(snapshot.heads[0].hash).toEqual(expectedData.heads[0])
          expect(snapshot.values.length).toEqual(expectedData.values.length)
          expect(snapshot.values[0].hash).toEqual(expectedData.values[0])
          expect(snapshot.values[1].hash).toEqual(expectedData.values[1])
          expect(snapshot.values[2].hash).toEqual(expectedData.values[2])
        })
      })

      describe('toBuffer', () => {
        it('returns the log as a Buffer', () => {
          assert.deepStrictEqual(log.toBuffer(), Buffer.from(JSON.stringify(expectedData)))
        })
      })

      describe('toMultihash - cbor', () => {
        it('returns the log as ipfs CID', async () => {
          const expectedCid = 'zdpuB21fX4YEWXmwUtMpLXKztbneMrFN3VMJowxkECJG9sbph'
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          await log.append('one')
          const hash = await log.toMultihash()
          expect(hash).toEqual(expectedCid)
        })

        it('log serialized to ipfs contains the correct data', async () => {
          const expectedData = {
            id: 'A',
            heads: ['zdpuB1BLzntnfJFoMsxfi74ZUJZnbF235RffCR2JAD6oYQmmD']
          }
          const expectedCid = 'zdpuB21fX4YEWXmwUtMpLXKztbneMrFN3VMJowxkECJG9sbph'
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          await log.append('one')
          const hash = await log.toMultihash()
          expect(hash).toEqual(expectedCid)
          const result = await io.read(ipfs, hash)
          const heads = result.heads.map(head => head.toString(base58btc))
          assert.deepStrictEqual(heads, expectedData.heads)
        })

        it('throws an error if log items is empty', async () => {
          const emptyLog = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey))
          let err
          try {
            await emptyLog.toMultihash()
          } catch (e) {
            err = e
          }
          assert.notStrictEqual(err, null)
          expect(err.message).toEqual('Can\'t serialize an empty log')
        })
      })

      describe('toMultihash - pb', () => {
        it('returns the log as ipfs multihash', async () => {
          const expectedMultihash = 'QmcGjfa5fw91TTxP8cp3Jt96r2vt74NmdYmXzYoHs1v9n9'
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          await log.append('one')
          const multihash = await log.toMultihash({ format: 'dag-pb' })
          expect(multihash).toEqual(expectedMultihash)
        })

        it('log serialized to ipfs contains the correct data', async () => {
          const expectedData = {
            id: 'A',
            heads: ['zdpuB1BLzntnfJFoMsxfi74ZUJZnbF235RffCR2JAD6oYQmmD']
          }
          const expectedMultihash = 'QmTKjw1mRCkJcZFPo6QQEixgr1ewsmvL8mkDhcWcMauaWD'
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          await log.append('one')
          const multihash = await log.toMultihash({ format: 'dag-pb' })
          expect(multihash).toEqual(expectedMultihash)
          const result = await ipfs.object.get(CID.parse(multihash))
          const res = JSON.parse(Buffer.from(result.Data).toString())
          assert.deepStrictEqual(res.heads, expectedData.heads)
        })

        it('throws an error if log items is empty', async () => {
          const emptyLog = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey))
          let err
          try {
            await emptyLog.toMultihash()
          } catch (e) {
            err = e
          }
          assert.notStrictEqual(err, null)
          expect(err.message).toEqual('Can\'t serialize an empty log')
        })
      })

      describe('fromMultihash', () => {
        it('creates a log from ipfs CID - one entry', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuAx5CqSNpCRGRhU1oZ8vEZ66pYVgUvBCRCAYXLJ3Sg6Vto']
          }
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
          await log.append('one')
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: -1 })
          expect(JSON.stringify(res.toJSON())).toEqual(JSON.stringify(expectedData))
          expect(res.length).toEqual(1)
          expect(res.values[0].payload.value).toEqual('one')
          assert.deepStrictEqual(res.values[0].clock.id, signKey.publicKey)
          expect(res.values[0].clock.time).toEqual(1)
        })

        it('creates a log from ipfs CID - three entries', async () => {
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: -1 })
          expect(res.length).toEqual(3)
          expect(res.values[0].payload.value).toEqual('one')
          expect(res.values[0].clock.time).toEqual(1)
          expect(res.values[1].payload.value).toEqual('two')
          expect(res.values[1].clock.time).toEqual(2)
          expect(res.values[2].payload.value).toEqual('three')
          expect(res.values[2].clock.time).toEqual(3)
        })

        it('creates a log from ipfs multihash (backwards compat)', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuAwNbitN5qJ6qxNWTxRssx1ai7M9TwFHAVaRa3uFgawZMk']
          }
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
          await log.append('one')
          const multihash = await log.toMultihash()
          const res = await Log.fromMultihash<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), multihash, { length: -1 })
          expect(JSON.stringify(res.toJSON())).toEqual(JSON.stringify(expectedData))
          expect(res.length).toEqual(1)
          expect(res.values[0].payload.value).toEqual('one')
          expect(res.values[0].clock.id).toEqual(signKey.publicKey)
          expect(res.values[0].clock.time).toEqual(1)
        })

        it('has the right sequence number after creation and appending', async () => {
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: -1 })
          expect(res.length).toEqual(3)
          await res.append('four')
          expect(res.length).toEqual(4)
          expect(res.values[3].payload.value).toEqual('four')
          expect(res.values[3].clock.time).toEqual(4)
        })

        it('creates a log from ipfs CID that has three heads', async () => {
          const log1 = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          const log2 = new Log<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'A' })
          const log3 = new Log<string>(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'A' })
          await log1.append('one') // order is determined by the identity's publicKey
          await log2.append('two')
          await log3.append('three')
          await log1.join(log2)
          await log1.join(log3)
          const hash = await log1.toMultihash()
          const res = await Log.fromMultihash<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: -1 })
          expect(res.length).toEqual(3)
          expect(res.heads.length).toEqual(3)
          expect(res.heads[2].payload.value).toEqual('three')
          expect(res.heads[1].payload.value).toEqual('two') // order is determined by the identity's publicKey
          expect(res.heads[0].payload.value).toEqual('one')
        })

        it('creates a log from ipfs CID that has three heads w/ custom tiebreaker', async () => {
          const log1 = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          const log2 = new Log<string>(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'A' })
          const log3 = new Log<string>(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'A' })
          await log1.append('one') // order is determined by the identity's publicKey
          await log2.append('two')
          await log3.append('three')
          await log1.join(log2)
          await log1.join(log3)
          const hash = await log1.toMultihash()
          const res = await Log.fromMultihash<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash,
            { sortFn: FirstWriteWins })
          expect(res.length).toEqual(3)
          expect(res.heads.length).toEqual(3)
          expect(res.heads[2].payload.value).toEqual('one')
          expect(res.heads[1].payload.value).toEqual('two') // order is determined by the identity's publicKey
          expect(res.heads[0].payload.value).toEqual('three')
        })

        it('creates a log from ipfs CID up to a size limit', async () => {
          const amount = 100
          const size = amount / 2
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          for (let i = 0; i < amount; i++) {
            await log.append(i.toString())
          }
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: size })
          expect(res.length).toEqual(size)
        })

        it('creates a log from ipfs CID up without size limit', async () => {
          const amount = 100
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          for (let i = 0; i < amount; i++) {
            await log.append(i.toString())
          }
          const hash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, { length: -1 })
          expect(res.length).toEqual(amount)
        })

        it('throws an error if ipfs is not defined', async () => {
          let err
          try {
            await Log.fromMultihash(undefined as any, undefined, undefined, undefined as any)
          } catch (e) {
            err = e
          }
          assert.notStrictEqual(err, null)
          expect(err.message).toEqual('IPFS instance not defined')
        })

        it('throws an error if hash is not defined', async () => {
          let err
          try {
            await Log.fromMultihash(ipfs, undefined, undefined, undefined as any)
          } catch (e) {
            err = e
          }
          assert.notStrictEqual(err, null)
          expect(err.message).toEqual('Invalid hash: undefined')
        })

        it('throws an error if data from hash is not valid JSON', async () => {
          const value = 'hello'
          const cid = CID.parse(await io.write(ipfs, 'dag-pb', value))
          let err
          try {
            const hash = cid.toString(base58btc)
            await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, undefined as any)
          } catch (e) {
            err = e
          }
          expect(err.message).toEqual('Unexpected token h in JSON at position 0')
        })

        it('throws an error when data from CID is not instance of Log', async () => {
          const hash = await ipfs.dag.put({})
          let err
          try {
            await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash, undefined as any)
          } catch (e) {
            err = e
          }
          expect(err.message).toEqual('Given argument is not an instance of Log')
        })

        it('onProgress callback is fired for each entry', async () => {
          const amount = 100
          const log = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'A' })
          for (let i = 0; i < amount; i++) {
            await log.append(i.toString())
          }

          const items = log.values
          let i = 0
          const loadProgressCallback = (entry: Entry<string>) => {
            assert.notStrictEqual(entry, null)
            expect(entry.hash).toEqual(items[items.length - i - 1].hash)
            expect(entry.payload.value).toEqual(items[items.length - i - 1].payload.value)
            i++
          }

          const hash = await log.toMultihash()
          const result = await Log.fromMultihash<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), hash,
            { length: -1, exclude: [], onProgressCallback: loadProgressCallback })

          // Make sure the onProgress callback was called for each entry
          expect(i).toEqual(amount)
          // Make sure the log entries are correct ones
          expect(result.values[0].clock.time).toEqual(1)
          expect(result.values[0].payload.value).toEqual('0')
          expect(result.values[result.length - 1].clock.time).toEqual(100)
          expect(result.values[result.length - 1].payload.value).toEqual('99')
        })
      })

      describe('fromEntryHash', () => {
        afterEach(() => {
          if (Log.fromEntryHash["restore"]) {
            Log.fromEntryHash["restore"]()
          }
        })

        it('calls fromEntryHash', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuAwNbitN5qJ6qxNWTxRssx1ai7M9TwFHAVaRa3uFgawZMk']
          }
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
          await log.append('one')
          const res = await Log.fromEntryHash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), expectedData.heads[0],
            { logId: log._id, length: -1 })
          expect(JSON.stringify(res.toJSON())).toEqual(JSON.stringify(expectedData))
        })
      })

      describe('fromMultihash', () => {
        afterEach(() => {
          if (Log.fromMultihash["restore"]) {
            Log.fromMultihash["restore"]()
          }
        })

        it('calls fromMultihash', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuAwNbitN5qJ6qxNWTxRssx1ai7M9TwFHAVaRa3uFgawZMk']
          }
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
          await log.append('one')
          const multihash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), multihash, { length: -1 })
          expect(JSON.stringify(res.toJSON())).toEqual(JSON.stringify(expectedData))
        })

        it('calls fromMultihash with custom tiebreaker', async () => {
          const expectedData = {
            id: 'X',
            heads: ['zdpuAwNbitN5qJ6qxNWTxRssx1ai7M9TwFHAVaRa3uFgawZMk']
          }
          const log = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
          await log.append('one')
          const multihash = await log.toMultihash()
          const res = await Log.fromMultihash(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), multihash,
            { length: -1, sortFn: FirstWriteWins })
          expect(JSON.stringify(res.toJSON())).toEqual(JSON.stringify(expectedData))
        })
      })
    })

    describe('values', () => {
      it('returns all entries in the log', async () => {
        const log = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey))
        expect(log.values instanceof Array).toEqual(true)
        expect(log.length).toEqual(0)
        await log.append('hello1')
        await log.append('hello2')
        await log.append('hello3')
        expect(log.values instanceof Array).toEqual(true)
        expect(log.length).toEqual(3)
        expect(log.values[0].payload.value).toEqual('hello1')
        expect(log.values[1].payload.value).toEqual('hello2')
        expect(log.values[2].payload.value).toEqual('hello3')
      })
    })
  })
})
