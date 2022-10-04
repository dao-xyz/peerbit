
import assert from 'assert'
import rmrf from 'rimraf'
import { Log } from '../log.js'
import { createStore, Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import fs from 'fs-extra'

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
    describe('Log remove', function () {
        jest.setTimeout(config.timeout)

        const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config
        let keystore: Keystore, signingKeystore: Keystore

        beforeAll(async () => {
            await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
            await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

            keystore = new Keystore(await createStore(identityKeysPath))
            signingKeystore = new Keystore(await createStore(signingKeysPath))

            signKey = await await keystore.createKey(await Ed25519Keypair.create(), { id: new Uint8Array([0]), overwrite: true });
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
        describe('remove', () => {
            it('removes by next', async () => {
                const log = new Log<string>(ipfs, { publicKey: signKey.keypair.publicKey, sign: (data) => signKey.keypair.sign(data) });
                expect(log.values instanceof Array).toEqual(true)
                expect(log.length).toEqual(0)
                await log.append('hello1')
                await log.append('hello2')
                const h3 = await log.append('hello3')
                expect(log.values instanceof Array).toEqual(true)
                expect(log.length).toEqual(3)
                expect(log.values[0].payload.value).toEqual('hello1')
                expect(log.values[1].payload.value).toEqual('hello2')
                expect(log.values[2].payload.value).toEqual('hello3')
                log.removeAll([h3]);
                expect(log.length).toEqual(0)
                expect(log.values.length).toEqual(0)
            })
        })
    })
})
