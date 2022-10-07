import rmrf from 'rimraf'
import fs from 'fs-extra'
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
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


let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Cut', function () {
    jest.setTimeout(config.timeout)

    const { signingKeyFixtures, signingKeysPath } = config

    let keystore: Keystore

    beforeAll(async () => {

      rmrf.sync(signingKeysPath(__filenameBase))

      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

      keystore = new Keystore(await createStore(signingKeysPath(__filenameBase)))

      //@ts-ignore
      signKey = await keystore.getKey(new Uint8Array([0]));

      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
      ipfs = ipfsd.api
    })

    afterAll(async () => {
      await stopIpfs(ipfsd)

      rmrf.sync(signingKeysPath(__filenameBase))

      await keystore?.close()

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
      expect(log.values[0].payload.getValue()).toEqual('hello3');
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
      expect(log.values[0].payload.getValue()).toEqual('hello4');
    })
  })
})
