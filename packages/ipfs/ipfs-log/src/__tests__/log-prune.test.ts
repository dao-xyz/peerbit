import rmrf from 'rimraf'
import fs from 'fs-extra'
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


let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Cut', function () {
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

      //@ts-ignore
      signKey = await keystore.getKey(new Uint8Array([0]));

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


    it('cut back to max oplog length', async () => {
      const log = new Log<string>(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A', prune: { maxLength: 1, cutToLength: 1 } })
      await log.append('hello1')
      await log.append('hello2')
      await log.append('hello3')
      expect(log.length).toEqual(1);
      expect(log.values[0].payload.value).toEqual('hello3');
    })

    it('cut back to cut length', async () => {
      const log = new Log<string>(ipfs, {
        publicKey: signKey.keypair.publicKey,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { logId: 'A', prune: { maxLength: 3, cutToLength: 1 } })
      await log.append('hello1')
      await log.append('hello2')
      await log.append('hello3')
      expect(log.length).toEqual(3);
      await log.append('hello4')
      expect(log.length).toEqual(1); // We exceed 'maxLength' and cut back to 'cutToLength'
      expect(log.values[0].payload.value).toEqual('hello4');
    })
  })
})
