
import rmrf from 'rimraf'
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/peerbit-keystore'
import fs from 'fs-extra'
import { jest } from '@jest/globals';

// Test utils
import {
    nodeConfig as config,
    testAPIs,
    startIpfs,
    stopIpfs
} from '@dao-xyz/peerbit-test-utils'
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
    describe('Log remove', function () {
        jest.setTimeout(config.timeout)

        const { signingKeyFixtures, signingKeysPath } = config
        let keystore: Keystore

        beforeAll(async () => {

            await fs.copy(signingKeyFixtures(__dirname), signingKeysPath(__filenameBase))

            keystore = new Keystore(await createStore(signingKeysPath(__filenameBase)))

            signKey = await await keystore.getKey(new Uint8Array([0])) as KeyWithMeta<Ed25519Keypair>;
            ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig)
            ipfs = ipfsd.api
        })

        afterAll(async () => {
            await stopIpfs(ipfsd)
            rmrf.sync(signingKeysPath(__filenameBase))


            await keystore?.close()

        })
        describe('remove', () => {
            it('removes by next', async () => {
                const log = new Log<string>(ipfs, { ...signKey.keypair, sign: (data) => signKey.keypair.sign(data) });
                expect(log.values instanceof Array).toEqual(true)
                expect(log.length).toEqual(0)
                await log.append('hello1')
                await log.append('hello2')
                const h3 = await log.append('hello3')
                expect(log.values instanceof Array).toEqual(true)
                expect(log.length).toEqual(3)
                expect(log.values[0].payload.getValue()).toEqual('hello1')
                expect(log.values[1].payload.getValue()).toEqual('hello2')
                expect(log.values[2].payload.getValue()).toEqual('hello3')
                log.removeAll([h3]);
                expect(log.length).toEqual(0)
                expect(log.values.length).toEqual(0)
            })
        })
    })
})
