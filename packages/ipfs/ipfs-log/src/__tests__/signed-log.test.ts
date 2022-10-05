import assert from 'assert'
import rmrf from 'rimraf'
import fs from 'fs-extra'
import { CanAppendAccessController } from '../default-access-controller.js'
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { Entry } from '../entry.js'
import { SignatureWithKey } from '@dao-xyz/peerbit-crypto'
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto"

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
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);



let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Signed Log', function () {
    jest.setTimeout(config.timeout)

    let { signingKeyFixtures, signingKeysPath } = config
    let keystore: Keystore


    beforeAll(async () => {
      rmrf.sync(signingKeysPath(__filenameBase))

      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

      keystore = new Keystore(await createStore(signingKeysPath(__filenameBase)))

      // @ts-ignore
      signKey = await keystore.getKey(new Uint8Array([0]));
      // @ts-ignore
      signKey2 = await keystore.getKey(new Uint8Array([1]));
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api
    })

    afterAll(async () => {
      await stopIpfs(ipfsd)

      rmrf.sync(signingKeysPath(__filenameBase))
      await keystore?.close()

    })



    it('has the correct identity', () => {
      const log = new Log(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A' })
      expect(log._identity.publicKey).toMatchSnapshot('publicKeyFromLog');
    })

    it('has the correct public key', () => {
      const log = new Log(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A' })
      expect(log._identity.publicKey).toEqual(signKey.keypair.publicKey)
    })

    it('has the correct pkSignature', () => {
      const log = new Log(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A' })
      expect(log._identity.publicKey).toEqual(signKey.keypair.publicKey)
    })

    it('has the correct signature', () => {
      const log = new Log(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A' })
      expect(log._identity.publicKey).toEqual(signKey.keypair.publicKey)
    })

    it('entries contain an identity', async () => {
      const log = new Log(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A' })
      await log.append('one')
      assert.notStrictEqual(await log.values[0].signature, null)
      assert.deepStrictEqual(await log.values[0].publicKey, signKey.keypair.publicKey)
    })

    it('doesn\'t join logs with different IDs ', async () => {
      const log1 = new Log<string>(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A' })
      const log2 = new Log<string>(ipfs, {
        publicKey: signKey2.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
      }, { logId: 'B' })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        await log2.append('three')
        await log1.join(log2)
      } catch (e: any) {
        err = e.toString()
        throw e
      }

      expect(err).toEqual(undefined)
      expect(log1._id).toEqual('A')
      expect(log1.values.length).toEqual(1)
      expect(log1.values[0].payload.value).toEqual('one')
    })



    it('throws an error if log is signed but the signature doesn\'t verify', async () => {
      const log1 = new Log<string>(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A' })
      const log2 = new Log<string>(ipfs, {
        publicKey: signKey2.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
      }, { logId: 'A' })
      let err

      try {
        await log1.append('one');
        await log2.append('two');
        let entry: Entry<string> = log2.values[0]
        entry._signature = await log1.values[0]._signature;
        await log1.join(log2)
      } catch (e: any) {
        err = e.toString()
      }

      const entry = log2.values[0]
      expect(err).toEqual(`Error: Could not validate signature "${await entry.signature}" for entry "${entry.hash}" and key "${(await entry.publicKey)}"`)
      expect(log1.values.length).toEqual(1)
      expect(log1.values[0].payload.value).toEqual('one')
    })

    it('throws an error if entry doesn\'t have append access', async () => {
      const denyAccess = { canAppend: (_, __) => Promise.resolve(false) } as CanAppendAccessController<string>
      const log1 = new Log<string>(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A' })
      const log2 = new Log(ipfs, {
        publicKey: signKey2.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
      }, { logId: 'A', access: denyAccess })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        await log1.join(log2)
      } catch (e: any) {
        err = e.toString()
      }

      expect(err).toEqual(`Error: Could not append entry, key "${signKey2.keypair.publicKey}" is not allowed to write to the log`)
    })

    it('throws an error upon join if entry doesn\'t have append access', async () => {
      const testACL = {
        canAppend: async (_entry: any, signature: MaybeEncrypted<SignatureWithKey>) => signature.decrypted.getValue(SignatureWithKey).publicKey.equals(signKey.keypair.publicKey)
      } as CanAppendAccessController<string>;
      const log1 = new Log<string>(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A', access: testACL })
      const log2 = new Log<string>(ipfs, {
        publicKey: signKey2.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
      }, { logId: 'A' })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        await log1.join(log2)
      } catch (e: any) {
        err = e.toString()
      }

      expect(err).toEqual(`Error: Could not append Entry<T>, key "${signKey2.keypair.publicKey}" is not allowed to write to the log`)
    })
  })
})
