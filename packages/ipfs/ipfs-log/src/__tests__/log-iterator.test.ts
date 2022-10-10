import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { LogCreator } from './utils/log-creator.js'
import { jest } from '@jest/globals';

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs
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

let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>, signKey3: KeyWithMeta<Ed25519Keypair>

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Iterator', function () {
    jest.setTimeout(config.timeout)

    const { signingKeyFixtures, signingKeysPath } = config

    let keystore: Keystore

    beforeAll(async () => {

      rmrf.sync(signingKeysPath(__filenameBase))

      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

      keystore = new Keystore(await createStore(signingKeysPath(__filenameBase)))

      //@ts-ignore
      signKey = await keystore.getKey(new Uint8Array([3]));
      //@ts-ignore
      signKey2 = await keystore.getKey(new Uint8Array([2]));
      //@ts-ignore
      signKey3 = await keystore.getKey(new Uint8Array([1]));
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api
    })

    afterAll(async () => {
      await stopIpfs(ipfsd)

      rmrf.sync(signingKeysPath(__filenameBase))

      await keystore?.close()

    })

    describe('Basic iterator functionality', () => {
      let log1: Log<string>

      beforeEach(async () => {
        log1 = new Log(ipfs, {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'X' })

        for (let i = 0; i <= 100; i++) {
          await log1.append('entry' + i)
        }
      })

      it('returns a Symbol.iterator object', async () => {
        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: 0
        })

        expect(typeof it[Symbol.iterator]).toEqual('function')
        assert.deepStrictEqual(it.next(), { value: undefined, done: true })
      })

      it('returns length with lte and amount', async () => {
        const amount = 10
        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: amount
        })
        const length = [...it].length;
        expect(length).toEqual(10)
      })

      it('returns entries with lte and amount and payload', async () => {
        const amount = 10

        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: amount
        })

        let i = 0
        for (const entry of it) {
          expect(entry.payload.getValue()).toEqual('entry' + (67 - i++))
        }
        expect(i).toEqual(amount)

      })



      it('returns correct length with gt and amount', async () => {
        const amount = 5
        const it = log1.iterator({
          gt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: amount
        })

        let i = 0
        for (const entry of it) {
          expect(entry.payload.getValue()).toEqual('entry' + (72 - i++))
        }
        expect(i).toEqual(amount)
      })



      it('returns entries with gte and amount and payload', async () => {
        const amount = 12

        const it = log1.iterator({
          gt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc',
          amount: amount
        })

        let i = 0
        for (const entry of it) {
          expect(entry.payload.getValue()).toEqual('entry' + (79 - i++))
        }
        expect(i).toEqual(amount);
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
        expect(hashes.length).toEqual(10)
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
        expect(hashes.length).toEqual(25)
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
        expect(hashes.length).toEqual(4)
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
        expect(hashes.length).toEqual(10)
      })

      it('returns length with gt and default amount', async () => {
        const it = log1.iterator({
          gt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        expect([...it].length).toEqual(33)
      })

      it('returns entries with gt and default amount', async () => {
        const it = log1.iterator({
          gt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        let i = 0
        for (const entry of it) {
          expect(entry.payload.getValue()).toEqual('entry' + (100 - i++))
        }
      })

      it('returns length with gte and default amount', async () => {
        const it = log1.iterator({
          gte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        expect([...it].length).toEqual(34)
      })

      it('returns entries with gte and default amount', async () => {
        const it = log1.iterator({
          gte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        let i = 0
        for (const entry of it) {
          expect(entry.payload.getValue()).toEqual('entry' + (100 - i++))
        }
      })

      it('returns length with lt and default amount value', async () => {
        const it = log1.iterator({
          lt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        expect([...it].length).toEqual(67)
      })

      it('returns entries with lt and default amount value', async () => {
        const it = log1.iterator({
          lt: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        let i = 0
        for (const entry of it) {
          expect(entry.payload.getValue()).toEqual('entry' + (66 - i++))
        }
      })

      it('returns length with lte and default amount value', async () => {
        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        expect([...it].length).toEqual(68)
      })

      it('returns entries with lte and default amount value', async () => {
        const it = log1.iterator({
          lte: 'zdpuApRErChG8jJptFerzSgfFSj89z49dFanFt9XtPMujVtKc'
        })

        let i = 0
        for (const entry of it) {
          expect(entry.payload.getValue()).toEqual('entry' + (67 - i++))
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
        identities = [signKey3, signKey2, signKey3, signKey]
        fixture = await LogCreator.createLogWithSixteenEntries(ipfs, identities)
      })

      it('returns the full length from all heads', async () => {
        const it = fixture.log.iterator({
          lte: fixture.log.heads
        })

        expect([...it].length).toEqual(16)
      })

      it('returns partial entries from all heads', async () => {
        const it = fixture.log.iterator({
          lte: fixture.log.heads,
          amount: 6
        })

        assert.deepStrictEqual([...it].map(e => e.payload.getValue()),
          ['entryA10', 'entryA9', 'entryA8', 'entryA7', 'entryC0', 'entryA6'])
      })

      it('returns partial logs from single heads #1', async () => {
        const it = fixture.log.iterator({
          lte: [fixture.log.heads[0]]
        })

        expect([...it].length).toEqual(10)
      })

      it('returns partial logs from single heads #2', async () => {
        const it = fixture.log.iterator({
          lte: [fixture.log.heads[1]]
        })

        expect([...it].length).toEqual(11)
      })

    })
  })
})
