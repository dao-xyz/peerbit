import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Entry } from '../entry.js';
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { Ed25519PublicKey } from '@dao-xyz/peerbit-crypto';
import { arraysCompare, arraysEqual } from '@dao-xyz/borsh-utils';
import { LamportClock as Clock } from '../lamport-clock.js';
import { jest } from '@jest/globals';

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs,
  connectPeers
} from '@dao-xyz/orbit-db-test-utils'

import { Controller } from 'ipfsd-ctl'
import { IPFS } from 'ipfs-core-types'
import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto'
import { dirname } from 'path';
import { fileURLToPath } from 'url';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);


let ipfsd: Controller, ipfs: IPFS, ipfs2: IPFS, ipfsd2: Controller, signKey: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>, signKey3: KeyWithMeta<Ed25519Keypair>, signKey4: KeyWithMeta<Ed25519Keypair>

const last = (arr: any[]) => {
  return arr[arr.length - 1]
}

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Join', function () {
    jest.setTimeout(config.timeout)

    const { signingKeyFixtures, signingKeysPath } = config

    let keystore: Keystore

    beforeAll(async () => {

      rmrf.sync(signingKeysPath(__filenameBase))

      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

      keystore = new Keystore(await createStore(signingKeysPath(__filenameBase)))

      // The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
      const keys: KeyWithMeta<Ed25519Keypair>[] = []
      for (let i = 0; i < 4; i++) {
        keys.push(await keystore.getKey(new Uint8Array([i])) as KeyWithMeta<Ed25519Keypair>)
      };
      keys.sort((a, b) => {
        return arraysCompare(a.keypair.publicKey.publicKey, b.keypair.publicKey.publicKey)
      });
      signKey = keys[0];
      signKey2 = keys[1];
      signKey3 = keys[2];
      signKey4 = keys[3];
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api

      ipfsd2 = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs2 = ipfsd2.api

      const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
      await connectPeers(ipfs, ipfs2, { filter: isLocalhostAddress })

    })

    afterAll(async () => {
      await stopIpfs(ipfsd)
      await stopIpfs(ipfsd2)


      rmrf.sync(signingKeysPath(__filenameBase))

      await keystore?.close()

    })

    describe('join', () => {
      let log1: Log<string>, log2: Log<string>, log3: Log<string>, log4: Log<string>

      beforeEach(async () => {
        log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'X' })
        log2 = new Log(ipfs, {
          publicKey: signKey2.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
        }, { logId: 'X' })
        log3 = new Log(ipfs2, {
          publicKey: signKey3.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey3.keypair.sign(data))
        }, { logId: 'X' })
        log4 = new Log(ipfs2, {
          publicKey: signKey4.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey4.keypair.sign(data))
        }, { logId: 'X' })
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
            ipfs, identity: {
              publicKey: signKey.keypair.publicKey,
              sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
            }, gidSeed: 'X' + i, data: 'entryA' + i, next: prev1 ? [prev1] : undefined
          })
          const n2 = await Entry.create({
            ipfs, identity: {
              publicKey: signKey2.keypair.publicKey,
              sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
            }, data: 'entryB' + i, next: prev2 ? [prev2, n1] : [n1]
          })
          const n3 = await Entry.create({
            ipfs: ipfs2, identity: {
              publicKey: signKey3.keypair.publicKey,
              sign: async (data: Uint8Array) => (await signKey3.keypair.sign(data))
            }, data: 'entryC' + i, next: prev3 ? [prev3, n1, n2] : [n1, n2]
          })
          items1.push(n1)
          items2.push(n2)
          items3.push(n3)
        }

        // Here we're creating a log from entries signed by A and B
        // but we accept entries from C too
        const logA = await Log.fromEntry(ipfs, {
          publicKey: signKey3.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey3.keypair.sign(data))
        }, last(items2), { length: -1 })


        // Here we're creating a log from entries signed by peer A, B and C
        // "logA" accepts entries from peer C so we can join logs A and B
        const logB = await Log.fromEntry(ipfs2, {
          publicKey: signKey3.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey3.keypair.sign(data))
        }, last(items3), { length: -1 })
        expect(logA.length).toEqual(items2.length + items1.length)
        expect(logB.length).toEqual(items3.length + items2.length + items1.length)

        await logA.join(logB)

        expect(logA.length).toEqual(items3.length + items2.length + items1.length)
        // The last Entry<T>, 'entryC100', should be the only head
        // (it points to entryB100, entryB100 and entryC99)
        expect(logA.heads.length).toEqual(1)

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

        expect(log1.length).toEqual(4)
        assert.deepStrictEqual(log1.values.map((e) => e.payload.getValue()), expectedData)

        const item = last(log1.values)
        expect(item.next.length).toEqual(1)
      })


      it('joins logs two ways', async () => {
        const a1 = await log1.append('helloA1')
        const a2 = await log1.append('helloA2')
        const b1 = await log2.append('helloB1')
        const b2 = await log2.append('helloB2')
        await log1.join(log2)
        await log2.join(log1)

        const expectedData = [
          'helloA1', 'helloB1', 'helloA2', 'helloB2'
        ]

        expect(log1.heads).toContainValues([a2, b2]);
        expect(log2.heads).toContainValues([a2, b2]);
        expect(a2.next).toContainValues([a1.hash]);
        expect(b2.next).toContainValues([b1.hash]);

        assert.deepStrictEqual(log1.values.map((e) => e.hash), log2.values.map((e) => e.hash))
        assert.deepStrictEqual(log1.values.map((e) => e.payload.getValue()), expectedData)
        assert.deepStrictEqual(log2.values.map((e) => e.payload.getValue()), expectedData)
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

        expect(log2.length).toEqual(4)
        assert.deepStrictEqual(log2.values.map((e) => e.payload.getValue()), expectedData)
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

        expect(log2.length).toEqual(4)
        assert.deepStrictEqual(log2.values.map((e) => e.payload.getValue()), expectedData)
      })

      it('joins 2 logs two ways and has the right heads at every step', async () => {
        await log1.append('helloA1')
        expect(log1.heads.length).toEqual(1)
        expect(log1.heads[0].payload.getValue()).toEqual('helloA1')

        await log2.append('helloB1')
        expect(log2.heads.length).toEqual(1)
        expect(log2.heads[0].payload.getValue()).toEqual('helloB1')

        await log2.join(log1)
        expect(log2.heads.length).toEqual(2)
        expect(log2.heads[0].payload.getValue()).toEqual('helloB1')
        expect(log2.heads[1].payload.getValue()).toEqual('helloA1')

        await log1.join(log2)
        expect(log1.heads.length).toEqual(2)
        expect(log1.heads[0].payload.getValue()).toEqual('helloB1')
        expect(log1.heads[1].payload.getValue()).toEqual('helloA1')

        await log1.append('helloA2')
        expect(log1.heads.length).toEqual(1)
        expect(log1.heads[0].payload.getValue()).toEqual('helloA2')

        await log2.append('helloB2')
        expect(log2.heads.length).toEqual(1)
        expect(log2.heads[0].payload.getValue()).toEqual('helloB2')

        await log2.join(log1)
        expect(log2.heads.length).toEqual(2)
        expect(log2.heads[0].payload.getValue()).toEqual('helloB2')
        expect(log2.heads[1].payload.getValue()).toEqual('helloA2')
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

        expect(log1.length).toEqual(8)
        assert.deepStrictEqual(log1.values.map(e => e.payload.getValue()), expectedData)
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

        expect(log1.length).toEqual(8)
        assert.deepStrictEqual(log1.values.map(e => e.payload.getValue()), log2.values.map(e => e.payload.getValue()))
      })

      it('joins logs and updates clocks', async () => {
        const a1 = await log1.append('helloA1')
        const b1 = await log2.append('helloB1')
        await log2.join(log1)
        const a2 = await log1.append('helloA2')
        const b2 = await log2.append('helloB2')

        expect(a2.clock.id).toEqual(signKey.keypair.publicKey.bytes)
        expect(b2.clock.id).toEqual(signKey2.keypair.publicKey.bytes)
        expect(a2.clock.time).toEqual(1n)
        expect(b2.clock.time).toEqual(1n)

        await log3.join(log1)
        /*   assert.deepStrictEqual(log3.clock.id, signKey3.keypair.publicKey.bytes)
          expect(log3.clock.time).toEqual(2n)
    */
        await log3.append('helloC1')
        const c2 = await log3.append('helloC2')
        await log1.join(log3)
        await log1.join(log2)
        await log4.append('helloD1')
        const d2 = await log4.append('helloD2')
        await log4.join(log2)
        await log4.join(log1)
        await log4.join(log3)
        const d3 = await log4.append('helloD3')
        expect(d3.gid).toEqual([b2.gid, c2.gid, d2.gid].sort()[0]);
        await log4.append('helloD4')
        await log1.join(log4)
        await log4.join(log1)
        const d5 = await log4.append('helloD5')
        expect(d5.gid).toEqual([d2.gid, b2.gid, c2.gid].sort()[0]);

        const a5 = await log1.append('helloA5')
        expect(a5.gid).toEqual([d2.gid, b2.gid, c2.gid].sort()[0]);

        await log4.join(log1)
        const d6 = await log4.append('helloD6')
        expect(d5.gid).toEqual(a5.gid);
        expect(d6.gid).toEqual(a5.gid);

        const expectedData = [
          { payload: 'helloA1', gid: a1.gid, clock: new Clock(signKey.keypair.publicKey.bytes, 0) },
          { payload: 'helloB1', gid: b1.gid, clock: new Clock(signKey2.keypair.publicKey.bytes, 0) },
          { payload: 'helloD1', gid: d2.gid, clock: new Clock(signKey4.keypair.publicKey.bytes, 0) },
          { payload: 'helloA2', gid: a2.gid, clock: new Clock(signKey.keypair.publicKey.bytes, 1) },
          { payload: 'helloB2', gid: b2.gid, clock: new Clock(signKey2.keypair.publicKey.bytes, 1) },
          { payload: 'helloD2', gid: d2.gid, clock: new Clock(signKey4.keypair.publicKey.bytes, 1) },
          { payload: 'helloC1', gid: a1.gid, clock: new Clock(signKey3.keypair.publicKey.bytes, 2) },
          { payload: 'helloC2', gid: c2.gid, clock: new Clock(signKey3.keypair.publicKey.bytes, 3) },
          { payload: 'helloD3', gid: d3.gid, clock: new Clock(signKey4.keypair.publicKey.bytes, 4) },
          { payload: 'helloD4', gid: d3.gid, clock: new Clock(signKey4.keypair.publicKey.bytes, 5) },
          { payload: 'helloA5', gid: a5.gid, clock: new Clock(signKey.keypair.publicKey.bytes, 6) },
          { payload: 'helloD5', gid: d5.gid, clock: new Clock(signKey4.keypair.publicKey.bytes, 6) },
          { payload: 'helloD6', gid: d6.gid, clock: new Clock(signKey4.keypair.publicKey.bytes, 7) }
        ]

        const transformed = log4.values.map((e) => {
          return { payload: e.payload.getValue(), gid: e.gid, clock: e.clock }
        })

        expect(log4.length).toEqual(13)
        assert.deepStrictEqual(transformed, expectedData)
      })

      it('joins logs from 4 logs', async () => {
        const a1 = await log1.append('helloA1')
        await log1.join(log2)
        const b1 = await log2.append('helloB1')
        await log2.join(log1)
        const a2 = await log1.append('helloA2')
        const b2 = await log2.append('helloB2')

        await log1.join(log3)
        // Sometimes failes because of clock ids are random TODO Fix
        expect(log1.heads[log1.heads.length - 1].gid).toEqual([b2.gid].sort()[0])
        expect(a2.clock.id).toEqual(signKey.keypair.publicKey.bytes)
        expect(a2.clock.time).toEqual(1n)

        await log3.join(log1)
        expect(log3.heads[log3.heads.length - 1].gid).toEqual([b2.gid].sort()[0])
        /*   assert.deepStrictEqual(log3.clock.id, signKey3.keypair.publicKey.bytes)
          expect(log3.clock.time).toEqual(2n) */

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
        const d4 = await log4.append('helloD4')

        expect(d4.clock.id).toEqual(signKey4.keypair.publicKey.bytes)
        expect(d4.clock.time).toEqual(5n)

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

        expect(log4.length).toEqual(10)
        assert.deepStrictEqual(log4.values.map((e) => e.payload.getValue()), expectedData)
      })

      describe('gid shadow callback', () => {
        it('it emits callback when gid is shadowed, triangle shape', async () => {

          /*  
           Either A or B shaded
           ┌─┐┌─┐  
           │a││b│  
           └┬┘└┬┘  
           ┌▽──▽──┐
           │a or b│
           └──────┘
           */

          const a1 = await log1.append('helloA1', { nexts: [] })
          const b1 = await log1.append('helloB1', { nexts: [] })
          let callbackValue: string[] = undefined as any;
          const ab1 = await log1.append('helloAB1', { nexts: [a1, b1], onGidsShadowed: (gids) => callbackValue = gids })
          expect(callbackValue).toHaveLength(1);
          expect(callbackValue[0]).toEqual(ab1.gid === a1.gid ? b1.gid : a1.gid); // if ab1 has gid a then b will be shadowed

        })

        it('it emits callback when gid is shadowed, N shape', async () => {

          /*  
           No shadows
            ┌──┐┌───┐ 
            │a0││b1 │ 
            └┬─┘└┬─┬┘ 
            ┌▽─┐ │┌▽─┐
            │a1│ ││b2│
            └┬─┘ │└──┘
            ┌▽───▽┐   
            │a2   │   
            └─────┘   
           */

          const a0 = await log1.append('helloA0', { nexts: [] })
          const a1 = await log1.append('helloA1', { nexts: [a0] })
          const b1 = await log1.append('helloB1', { nexts: [] })
          const b2 = await log1.append('helloB2', { nexts: [b1] })

          let callbackValue: any;
          // make sure gid is choosen from 1 bs

          const a2 = await log1.append('helloA2', { nexts: [a1, b1], onGidsShadowed: (gids) => callbackValue = gids })
          expect(callbackValue).toBeUndefined();
        })
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

          expect(log1.length).toEqual(1)
          assert.deepStrictEqual(log1.values.map((e) => e.payload.getValue()), expectedData)
          expect(lastEntry.next.length).toEqual(1)
        })

        it('joins only specified amount of entries - two entries', async () => {
          await log1.join(log2, 2)

          const expectedData = [
            'helloA2', 'helloB2'
          ]
          const lastEntry = last(log1.values)

          expect(log1.length).toEqual(2)
          assert.deepStrictEqual(log1.values.map((e) => e.payload.getValue()), expectedData)
          expect(lastEntry.next.length).toEqual(1)
        })

        it('joins only specified amount of entries - three entries', async () => {
          await log1.join(log2, 3)

          const expectedData = [
            'helloB1', 'helloA2', 'helloB2'
          ]
          const lastEntry = last(log1.values)

          expect(log1.length).toEqual(3)
          assert.deepStrictEqual(log1.values.map((e) => e.payload.getValue()), expectedData)
          expect(lastEntry.next.length).toEqual(1)
        })

        it('joins only specified amount of entries - (all) four entries', async () => {
          await log1.join(log2, 4)

          const expectedData = [
            'helloA1', 'helloB1', 'helloA2', 'helloB2'
          ]
          const lastEntry = last(log1.values)

          expect(log1.length).toEqual(4)
          assert.deepStrictEqual(log1.values.map((e) => e.payload.getValue()), expectedData)
          expect(lastEntry.next.length).toEqual(1)
        })
      })
    })
  })
})
