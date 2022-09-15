const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Entry, LamportClock as Clock } from '@dao-xyz/ipfs-log-entry';
import { Log } from '../log'
import { assertPayload } from './utils/assert'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { Ed25519PublicKeyData } from '@dao-xyz/identity';

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, signKey: SignKeyWithMeta, signKey2: SignKeyWithMeta, signKey3: SignKeyWithMeta, signKey4: SignKeyWithMeta

const last = (arr) => {
  return arr[arr.length - 1]
}

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Join', function () {
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

      // The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
      signKey = await keystore.createKey(new Uint8Array([0]), SignKeyWithMeta);
      signKey2 = await keystore.createKey(new Uint8Array([1]), SignKeyWithMeta);
      signKey3 = await keystore.createKey(new Uint8Array([2]), SignKeyWithMeta);
      signKey4 = await keystore.createKey(new Uint8Array([3]), SignKeyWithMeta);
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

    describe('join', () => {
      let log1: Log<string>, log2: Log<string>, log3: Log<string>, log4: Log<string>

      beforeEach(async () => {
        log1 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), { logId: 'X' })
        log2 = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), { logId: 'X' })
        log3 = new Log(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), { logId: 'X' })
        log4 = new Log(ipfs, signKey4.publicKey, (data) => Keystore.sign(data, signKey4), { logId: 'X' })
      })


      it('joins logs', async () => {
        const items1: Entry<string>[] = []
        const items2: Entry<string>[] = []
        const items3: Entry<string>[] = []
        const amount = 100
        for (let i = 1; i <= amount; i++) {
          const prev1 = last(items1)
          const prev2 = last(items2)
          const prev3 = last(items3)
          const n1 = await Entry.create({
            ipfs, publicKey: new Ed25519PublicKeyData({
              publicKey: signKey.publicKey
            }), sign: (data) => Keystore.sign(data, signKey), logId: 'X', data: 'entryA' + i, next: [prev1]
          })
          const n2 = await Entry.create({
            ipfs, publicKey: new Ed25519PublicKeyData({
              publicKey: signKey2.publicKey
            }), sign: (data) => Keystore.sign(data, signKey2), logId: 'X', data: 'entryB' + i, next: [prev2, n1]
          })
          const n3 = await Entry.create({
            ipfs, publicKey: new Ed25519PublicKeyData({
              publicKey: signKey3.publicKey
            }), sign: (data) => Keystore.sign(data, signKey3), logId: 'X', data: 'entryC' + i, next: [prev3, n1, n2]
          })
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        // Here we're creating a log from entries signed by A and B
        // but we accept entries from C too
        const logA = await Log.fromEntry(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), last(items2), { length: -1 })
        // Here we're creating a log from entries signed by peer A, B and C
        // "logA" accepts entries from peer C so we can join logs A and B
        const logB = await Log.fromEntry(ipfs, signKey3.publicKey, (data) => Keystore.sign(data, signKey3), last(items3), { length: -1 })
        assert.strictEqual(logA.length, items2.length + items1.length)
        assert.strictEqual(logB.length, items3.length + items2.length + items1.length)

        await logA.join(logB)

        assert.strictEqual(logA.length, items3.length + items2.length + items1.length)
        // The last Entry<T>, 'entryC100', should be the only head
        // (it points to entryB100, entryB100 and entryC99)
        assert.strictEqual(logA.heads.length, 1)
      })

      it('throws an error if first log is not defined', async () => {
        let err
        try {
          await log1.join(undefined)
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, null)
        assert.strictEqual(err.message, 'Log instance not defined')
      })

      it('throws an error if passed argument is not an instance of Log', async () => {
        let err
        try {
          await log1.join({} as any)
        } catch (e) {
          err = e
        }
        assert.notStrictEqual(err, null)
        assert.strictEqual(err.message, 'Given argument is not an instance of Log')
      })

      it('joins only unique items', async () => {
        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2)
        await log1.join(log2)

        const expectedData = [
          'helloA1', 'helloB1', 'helloA2', 'helloB2'
        ]

        assert.strictEqual(log1.length, 4)
        assert.deepStrictEqual(log1.values.map((e) => e.payload.value), expectedData)

        const item = last(log1.values)
        assert.strictEqual(item.next.length, 1)
      })

      it('joins logs two ways', async () => {
        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2)
        await log2.join(log1)

        const expectedData = [
          'helloA1', 'helloB1', 'helloA2', 'helloB2'
        ]

        assert.deepStrictEqual(log1.values.map((e) => e.hash), log2.values.map((e) => e.hash))
        assert.deepStrictEqual(log1.values.map((e) => e.payload.value), expectedData)
        assert.deepStrictEqual(log2.values.map((e) => e.payload.value), expectedData)
      })

      it('joins logs twice', async () => {
        await log1.append('helloA1')
        await log2.append('helloB1')
        await log2.join(log1)

        await log1.append('helloA2')
        await log2.append('helloB2')
        await log2.join(log1)

        const expectedData = [
          'helloA1', 'helloB1', 'helloA2', 'helloB2'
        ]

        assert.strictEqual(log2.length, 4)
        assert.deepStrictEqual(log2.values.map((e) => e.payload.value), expectedData)
      })

      it('joins 2 logs two ways', async () => {
        await log1.append('helloA1')
        await log2.append('helloB1')
        await log2.join(log1)
        await log1.join(log2)
        await log1.append('helloA2')
        await log2.append('helloB2')
        await log2.join(log1)

        const expectedData = [
          'helloA1', 'helloB1', 'helloA2', 'helloB2'
        ]

        assert.strictEqual(log2.length, 4)
        assert.deepStrictEqual(log2.values.map((e) => e.payload.value), expectedData)
      })

      it('joins 2 logs two ways and has the right heads at every step', async () => {
        await log1.append('helloA1')
        assert.strictEqual(log1.heads.length, 1)
        assertPayload(log1.heads[0].payload.value, 'helloA1')

        await log2.append('helloB1')
        assert.strictEqual(log2.heads.length, 1)
        assertPayload(log2.heads[0].payload.value, 'helloB1')

        await log2.join(log1)
        assert.strictEqual(log2.heads.length, 2)
        assertPayload(log2.heads[0].payload.value, 'helloB1')
        assertPayload(log2.heads[1].payload.value, 'helloA1')

        await log1.join(log2)
        assert.strictEqual(log1.heads.length, 2)
        assertPayload(log1.heads[0].payload.value, 'helloB1')
        assertPayload(log1.heads[1].payload.value, 'helloA1')

        await log1.append('helloA2')
        assert.strictEqual(log1.heads.length, 1)
        assertPayload(log1.heads[0].payload.value, 'helloA2')

        await log2.append('helloB2')
        assert.strictEqual(log2.heads.length, 1)
        assertPayload(log2.heads[0].payload.value, 'helloB2')

        await log2.join(log1)
        assert.strictEqual(log2.heads.length, 2)
        assertPayload(log2.heads[0].payload.value, 'helloB2')
        assertPayload(log2.heads[1].payload.value, 'helloA2')
      })

      it('joins 4 logs to one', async () => {
        // order determined by identity's publicKey
        await log1.append('helloA1')
        await log1.append('helloA2')

        await log2.append('helloB1')
        await log2.append('helloB2')

        await log3.append('helloC1')
        await log3.append('helloC2')

        await log4.append('helloD1')
        await log4.append('helloD2')
        await log1.join(log2)
        await log1.join(log3)
        await log1.join(log4)

        const expectedData = [
          'helloA1',
          'helloB1',
          'helloC1',
          'helloD1',
          'helloA2',
          'helloB2',
          'helloC2',
          'helloD2'
        ]

        assert.strictEqual(log1.length, 8)
        assert.deepStrictEqual(log1.values.map(e => e.payload.value), expectedData)
      })

      it('joins 4 logs to one is commutative', async () => {
        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log3.append('helloC1')
        await log3.append('helloC2')
        await log4.append('helloD1')
        await log4.append('helloD2')
        await log1.join(log2)
        await log1.join(log3)
        await log1.join(log4)
        await log2.join(log1)
        await log2.join(log3)
        await log2.join(log4)

        assert.strictEqual(log1.length, 8)
        assert.deepStrictEqual(log1.values.map(e => e.payload.value), log2.values.map(e => e.payload.value))
      })

      it('joins logs and updates clocks', async () => {
        await log1.append('helloA1')
        await log2.append('helloB1')
        await log2.join(log1)
        await log1.append('helloA2')
        await log2.append('helloB2')

        assert.strictEqual(log1.clock.id, signKey.publicKey)
        assert.strictEqual(log2.clock.id, signKey2.publicKey)
        assert.strictEqual(log1.clock.time, 2)
        assert.strictEqual(log2.clock.time, 2)

        await log3.join(log1)
        assert.strictEqual(log3.id, 'X')
        assert.strictEqual(log3.clock.id, signKey3.publicKey)
        assert.strictEqual(log3.clock.time, 2)

        await log3.append('helloC1')
        await log3.append('helloC2')
        await log1.join(log3)
        await log1.join(log2)
        await log4.append('helloD1')
        await log4.append('helloD2')
        await log4.join(log2)
        await log4.join(log1)
        await log4.join(log3)
        await log4.append('helloD3')
        await log4.append('helloD4')

        await log1.join(log4)
        await log4.join(log1)
        await log4.append('helloD5')
        await log1.append('helloA5')
        await log4.join(log1)
        assert.deepStrictEqual(log4.clock.id, signKey4.publicKey)
        assert.deepStrictEqual(log4.clock.time, 7)

        await log4.append('helloD6')
        assert.deepStrictEqual(log4.clock.time, 8)

        const expectedData = [
          { payload: 'helloA1', id: 'X', clock: new Clock(new Uint8Array(signKey.publicKey.getBuffer()), 1) },
          { payload: 'helloB1', id: 'X', clock: new Clock(new Uint8Array(signKey2.publicKey.getBuffer()), 1) },
          { payload: 'helloD1', id: 'X', clock: new Clock(new Uint8Array(signKey4.publicKey.getBuffer()), 1) },
          { payload: 'helloA2', id: 'X', clock: new Clock(new Uint8Array(signKey.publicKey.getBuffer()), 2) },
          { payload: 'helloB2', id: 'X', clock: new Clock(new Uint8Array(signKey2.publicKey.getBuffer()), 2) },
          { payload: 'helloD2', id: 'X', clock: new Clock(new Uint8Array(signKey4.publicKey.getBuffer()), 2) },
          { payload: 'helloC1', id: 'X', clock: new Clock(new Uint8Array(signKey3.publicKey.getBuffer()), 3) },
          { payload: 'helloC2', id: 'X', clock: new Clock(new Uint8Array(signKey3.publicKey.getBuffer()), 4) },
          { payload: 'helloD3', id: 'X', clock: new Clock(new Uint8Array(signKey4.publicKey.getBuffer()), 5) },
          { payload: 'helloD4', id: 'X', clock: new Clock(new Uint8Array(signKey4.publicKey.getBuffer()), 6) },
          { payload: 'helloA5', id: 'X', clock: new Clock(new Uint8Array(signKey.publicKey.getBuffer()), 7) },
          { payload: 'helloD5', id: 'X', clock: new Clock(new Uint8Array(signKey4.publicKey.getBuffer()), 7) },
          { payload: 'helloD6', id: 'X', clock: new Clock(new Uint8Array(signKey4.publicKey.getBuffer()), 8) }
        ]

        const transformed = log4.values.map((e) => {
          return { payload: e.payload.value, id: e.id, clock: e.clock }
        })

        assert.strictEqual(log4.length, 13)
        assert.deepStrictEqual(transformed, expectedData)
      })

      it('joins logs from 4 logs', async () => {
        await log1.append('helloA1')
        await log1.join(log2)
        await log2.append('helloB1')
        await log2.join(log1)
        await log1.append('helloA2')
        await log2.append('helloB2')

        await log1.join(log3)
        assert.strictEqual(log1.id, 'X')
        assert.strictEqual(log1.clock.id, new Uint8Array(signKey.publicKey.getBuffer()))
        assert.strictEqual(log1.clock.time, 2)

        await log3.join(log1)
        assert.strictEqual(log3.id, 'X')
        assert.strictEqual(log3.clock.id, new Uint8Array(signKey3.publicKey.getBuffer()))
        assert.strictEqual(log3.clock.time, 2)

        await log3.append('helloC1')
        await log3.append('helloC2')
        await log1.join(log3)
        await log1.join(log2)
        await log4.append('helloD1')
        await log4.append('helloD2')
        await log4.join(log2)
        await log4.join(log1)
        await log4.join(log3)
        await log4.append('helloD3')
        await log4.append('helloD4')

        assert.strictEqual(log4.clock.id, signKey4.publicKey)
        assert.strictEqual(log4.clock.time, 6)

        const expectedData = [
          'helloA1',
          'helloB1',
          'helloD1',
          'helloA2',
          'helloB2',
          'helloD2',
          'helloC1',
          'helloC2',
          'helloD3',
          'helloD4'
        ]

        assert.strictEqual(log4.length, 10)
        assert.deepStrictEqual(log4.values.map((e) => e.payload.value), expectedData)
      })

      describe('takes length as an argument', () => {
        beforeEach(async () => {
          await log1.append('helloA1')
          await log1.append('helloA2')
          await log2.append('helloB1')
          await log2.append('helloB2')
        })

        it('joins only specified amount of entries - one entry', async () => {
          await log1.join(log2, 1)

          const expectedData = [
            'helloB2'
          ]
          const lastEntry = last(log1.values)

          assert.strictEqual(log1.length, 1)
          assert.deepStrictEqual(log1.values.map((e) => e.payload.value), expectedData)
          assert.strictEqual(lastEntry.next.length, 1)
        })

        it('joins only specified amount of entries - two entries', async () => {
          await log1.join(log2, 2)

          const expectedData = [
            'helloA2', 'helloB2'
          ]
          const lastEntry = last(log1.values)

          assert.strictEqual(log1.length, 2)
          assert.deepStrictEqual(log1.values.map((e) => e.payload.value), expectedData)
          assert.strictEqual(lastEntry.next.length, 1)
        })

        it('joins only specified amount of entries - three entries', async () => {
          await log1.join(log2, 3)

          const expectedData = [
            'helloB1', 'helloA2', 'helloB2'
          ]
          const lastEntry = last(log1.values)

          assert.strictEqual(log1.length, 3)
          assert.deepStrictEqual(log1.values.map((e) => e.payload.value), expectedData)
          assert.strictEqual(lastEntry.next.length, 1)
        })

        it('joins only specified amount of entries - (all) four entries', async () => {
          await log1.join(log2, 4)

          const expectedData = [
            'helloA1', 'helloB1', 'helloA2', 'helloB2'
          ]
          const lastEntry = last(log1.values)

          assert.strictEqual(log1.length, 4)
          assert.deepStrictEqual(log1.values.map((e) => e.payload.value), expectedData)
          assert.strictEqual(lastEntry.next.length, 1)
        })
      })
    })
  })
})
