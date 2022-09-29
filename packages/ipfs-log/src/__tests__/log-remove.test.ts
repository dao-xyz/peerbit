
const assert = require('assert')
const rmrf = require('rimraf')
const { CID } = require('multiformats/cid')
const { base58btc } = require('multiformats/bases/base58')
import { Entry, getPeerID, LamportClock as Clock, Payload, Signature } from '@dao-xyz/ipfs-log-entry';
import { Log } from '../log'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
const fs = require('fs-extra')
import io from '@dao-xyz/orbit-db-io'

// For tiebreaker testing
import { LastWriteWins } from '../log-sorting';
import { assertPayload } from './utils/assert'
import { DecryptedThing } from '@dao-xyz/encryption-utils';
import { serialize } from '@dao-xyz/borsh';
import { Id } from '@dao-xyz/ipfs-log-entry';
import { Ed25519PublicKeyData, Identity, PublicKey } from '@dao-xyz/identity';
const FirstWriteWins = (a, b) => LastWriteWins(a, b) * -1

// Test utils
const {
    config,
    testAPIs,
    startIpfs,
    stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, signKey: SignKeyWithMeta

Object.keys(testAPIs).forEach((IPFS) => {
    describe('Log remove', function () {
        jest.setTimeout(config.timeout)

        const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config
        let keystore: Keystore, signingKeystore: Keystore

        beforeAll(async () => {
            await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
            await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

            keystore = new Keystore(identityKeysPath)
            signingKeystore = new Keystore(signingKeysPath)

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
                assertPayload(log.values[0].payload.value, 'hello1')
                assertPayload(log.values[1].payload.value, 'hello2')
                assertPayload(log.values[2].payload.value, 'hello3')
                log.removeAll([h3]);
                expect(log.length).toEqual(0)
                expect(log.values.length).toEqual(0)
            })
        })
    })
})
