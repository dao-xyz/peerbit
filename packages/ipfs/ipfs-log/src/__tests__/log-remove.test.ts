
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
import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { serialize } from '@dao-xyz/borsh';
import { Id } from '@dao-xyz/ipfs-log-entry';
import { Ed25519PublicKey, Identity, PublicKey } from '@dao-xyz/peerbit-crypto';
const FirstWriteWins = (a, b) => LastWriteWins(a, b) * -1

// Test utils
const {
    config,
    testAPIs,
    startIpfs,
    stopIpfs
} = require('@dao-xyz/orbit-db-test-utils')

let ipfsd, ipfs, signKey: SignKeyWithMeta

Object.keys(testAPIs).forEach((IPFS) => {
    describe('Log remove', function () {
        jest.setTimeout(config.timeout)

        const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config
        let keystore: Keystore, signingKeystore: Keystore

        beforeAll(async () => {
            await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
            await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

            keystore = new Keystore(await createStore(identityKeysPath)))
        signingKeystore = new Keystore(await createStore(signingKeysPath)))

    signKey = await keystore.createKey(new Uint8Array([0]), SignKeyWithMeta, undefined, { overwrite: true });
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
        const log = new Log<string>(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey))
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
