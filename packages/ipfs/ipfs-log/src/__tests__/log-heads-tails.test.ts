import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Entry } from '../entry';
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'

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
import { jest } from '@jest/globals';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);


let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>, signKey3: KeyWithMeta<Ed25519Keypair>, signKey4: KeyWithMeta<Ed25519Keypair>

const last = (arr: any[]) => {
  return arr[arr.length - 1]
}

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Heads and Tails', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore: Keystore, signingKeystore: Keystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(await createStore(identityKeysPath))
      signingKeystore = new Keystore(await createStore(signingKeysPath))
      // @ts-ignore
      signKey = await keystore.getKey(new Uint8Array([0]));
      // @ts-ignore
      signKey2 = await keystore.getKey(new Uint8Array([1]));
      // @ts-ignore
      signKey3 = await keystore.getKey(new Uint8Array([2]));
      // @ts-ignore
      signKey4 = await keystore.getKey(new Uint8Array([4]));

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

    describe('heads', () => {
      it('finds one head after one entry', async () => {
        const log1 = new Log<string>(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        expect(log1.heads.length).toEqual(1)
      })

      it('finds one head after two entries', async () => {
        const log1 = new Log<string>(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        await log1.append('helloA2')
        expect(log1.heads.length).toEqual(1)
      })

      it('log contains the head entry', async () => {
        const log1 = new Log<string>(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        await log1.append('helloA2')
        assert.deepStrictEqual(log1.get(log1.heads[0].hash), log1.heads[0])
      })

      it('finds head after a join and append', async () => {
        const log1 = new Log<string>(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log2 = new Log<string>(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')

        await log2.join(log1)
        await log2.append('helloB2')
        const expectedHead = last(log2.values)

        expect(log2.heads.length).toEqual(1)
        assert.deepStrictEqual(log2.heads[0].hash, expectedHead.hash)
      })

      it('finds two heads after a join', async () => {
        const log2 = new Log<string>(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log1 = new Log<string>(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')
        const expectedHead1 = last(log1.values)

        await log2.append('helloB1')
        await log2.append('helloB2')
        const expectedHead2 = last(log2.values)

        await log1.join(log2)

        const heads = log1.heads
        expect(heads.length).toEqual(2)
        expect(heads[0].hash).toEqual(expectedHead2.hash)
        expect(heads[1].hash).toEqual(expectedHead1.hash)
      })

      it('finds two heads after two joins', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log2 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')

        await log2.append('helloB1')
        await log2.append('helloB2')

        await log1.join(log2)

        await log2.append('helloB3')

        await log1.append('helloA3')
        await log1.append('helloA4')
        const expectedHead2 = last(log2.values)
        const expectedHead1 = last(log1.values)

        await log1.join(log2)

        const heads = log1.heads
        expect(heads.length).toEqual(2)
        expect(heads[0].hash).toEqual(expectedHead1.hash)
        expect(heads[1].hash).toEqual(expectedHead2.hash)
      })

      it('finds two heads after three joins', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log2 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log3 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2)
        await log1.append('helloA3')
        await log1.append('helloA4')
        const expectedHead1 = last(log1.values)
        await log3.append('helloC1')
        await log3.append('helloC2')
        await log2.join(log3)
        await log2.append('helloB3')
        const expectedHead2 = last(log2.values)
        await log1.join(log2)

        const heads = log1.heads
        expect(heads.length).toEqual(2)
        expect(heads[0].hash).toEqual(expectedHead1.hash)
        expect(heads[1].hash).toEqual(expectedHead2.hash)
      })

      it('finds three heads after three joins', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log2 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log3 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })

        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2)
        await log1.append('helloA3')
        await log1.append('helloA4')
        const expectedHead1 = last(log1.values)
        await log3.append('helloC1')
        await log2.append('helloB3')
        await log3.append('helloC2')
        const expectedHead2 = last(log2.values)
        const expectedHead3 = last(log3.values)
        await log1.join(log2)
        await log1.join(log3)

        const heads = log1.heads
        expect(heads.length).toEqual(3)
        assert.deepStrictEqual(heads[0].hash, expectedHead1.hash)
        assert.deepStrictEqual(heads[1].hash, expectedHead2.hash)
        assert.deepStrictEqual(heads[2].hash, expectedHead3.hash)
      })
    })

    describe('tails', () => {
      it('returns a tail', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        expect(log1.tails.length).toEqual(1)
      })

      it('tail is a Entry', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        expect(Entry.isEntry(log1.tails[0])).toEqual(true)
      })

      it('returns tail entries', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log2 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        await log2.append('helloB1')
        await log1.join(log2)
        expect(log1.tails.length).toEqual(2)
        expect(Entry.isEntry(log1.tails[0])).toEqual(true)
        expect(Entry.isEntry(log1.tails[1])).toEqual(true)
      })

      it('returns tail hashes', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log2 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2, 2)
        expect(log1.tailHashes.length).toEqual(2)
      })

      it('returns no tail hashes if all entries point to empty nexts', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log2 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        await log2.append('helloB1')
        await log1.join(log2)
        expect(log1.tailHashes.length).toEqual(0)
      })

      it('returns tails after loading a partial log', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'A' })
        const log2 = new Log(ipfs, {
          publicKey: signKey2.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
        }, { logId: 'A' })
        await log1.append('helloA1')
        await log1.append('helloA2')
        await log2.append('helloB1')
        await log2.append('helloB2')
        await log1.join(log2)
        const log4 = await Log.fromEntry(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, log1.heads, { length: 2 })
        expect(log4.length).toEqual(2)
        expect(log4.tails.length).toEqual(2)
        expect(log4.tails[0].hash).toEqual(log4.values[0].hash)
        expect(log4.tails[1].hash).toEqual(log4.values[1].hash)
      })

      it('returns tails sorted by public key', async () => {
        const log1 = new Log(ipfs, {
          publicKey: signKey.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
        }, { logId: 'XX' })
        const log2 = new Log(ipfs, {
          publicKey: signKey2.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
        }, { logId: 'XX' })
        const log3 = new Log(ipfs, {
          publicKey: signKey3.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey3.keypair.sign(data))
        }, { logId: 'XX' })
        const log4 = new Log(ipfs, {
          publicKey: signKey4.keypair.publicKey,
          sign: async (data: Uint8Array) => (await signKey4.keypair.sign(data))
        }, { logId: 'XX' })
        await log1.append('helloX1')
        await log2.append('helloB1')
        await log3.append('helloA1')
        await log3.join(log1)
        await log3.join(log2)
        await log4.join(log3)
        expect(log4.tails.length).toEqual(3)
        const log4Id = (await log4.tails[0].gid);
        expect(log4Id).toEqual('XX')
        assert.deepStrictEqual(log4.tails[0].clock.id, signKey3.keypair.publicKey)
        assert.deepStrictEqual(log4.tails[1].clock.id, signKey2.keypair.publicKey)
        assert.deepStrictEqual(log4.tails[2].clock.id, signKey.keypair.publicKey)
        assert.deepStrictEqual(log4.heads[0].clock.id, signKey4.keypair.publicKey)
      })
    })
  })
})
