const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Entry, LamportClock as Clock } from '@dao-xyz/ipfs-log-entry';
import { Log } from '../log'
import { Identities, Identity } from '@dao-xyz/orbit-db-identity-provider'
import { assertPayload } from './utils/assert'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { X25519PublicKey } from 'sodium-plus'
// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity: Identity, testIdentity2: Identity, testIdentity3: Identity, testIdentity4: Identity

const last = (arr) => {
  return arr[arr.length - 1]
}

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Encryption', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore: Keystore, signingKeystore: Keystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      // The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the testIdentity suffix
      testIdentity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore, signingKeystore })
      testIdentity2 = await Identities.createIdentity({ id: new Uint8Array([1]), keystore, signingKeystore })
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

    describe('join', () => {
      let log1: Log<string>, log2: Log<string>, log3: Log<string>, log4: Log<string>

      beforeEach(async () => {
        const logOptions = {
          logId: 'X', encryption: {
            decrypt: (data, _senderPublicKey) => Promise.resolve(data),
            encrypt: (data, _recieverPublicKey) => Promise.resolve({
              data,
              senderPublicKey: new X25519PublicKey(Buffer.from(new Array(32).fill(9)))
            })
          }
        };
        log1 = new Log(ipfs, testIdentity, logOptions)
        log2 = new Log(ipfs, testIdentity2, logOptions)
      })

      it('join encrypted identities', async () => {
        const nullKey = new X25519PublicKey(Buffer.from(new Array(32).fill(0)))
        await log1.append('helloA1', { recieverClock: nullKey, recieverIdentity: nullKey, recieverPayload: nullKey, pointerCount: 1 })
        await log1.append('helloA2', { recieverClock: nullKey, recieverIdentity: nullKey, recieverPayload: nullKey, pointerCount: 1 })
        await log2.append('helloB1', { recieverClock: nullKey, recieverIdentity: nullKey, recieverPayload: nullKey, pointerCount: 1 })
        await log2.append('helloB2', { recieverClock: nullKey, recieverIdentity: nullKey, recieverPayload: nullKey, pointerCount: 1 })

        // Remove decrypted caches of the log2 values
        log2.values.forEach((value) => {
          value.metadata._metadata.clear();
          value._clock.clear();
          value.payload._data.clear();
        })

        await log1.join(log2)

        const expectedData = [
          'helloA1', 'helloB1', 'helloA2', 'helloB2'
        ]

        assert.strictEqual(log1.length, 4)
        assert.deepStrictEqual(log1.values.map((e) => e.payload.value), expectedData)

        const item = last(log1.values)
        assert.strictEqual(item.next.length, 1)
      })
    })
  })
})
