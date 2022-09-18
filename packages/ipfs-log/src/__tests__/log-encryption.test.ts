const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { Entry, LamportClock as Clock } from '@dao-xyz/ipfs-log-entry';
import { Log } from '../log'
import { assertPayload } from './utils/assert'
import { BoxKeyWithMeta, Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { X25519PublicKey } from 'sodium-plus'
// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, signKey: SignKeyWithMeta, signKey2: SignKeyWithMeta, signKey3: SignKeyWithMeta, signKey4: SignKeyWithMeta

const last = (arr) => {
  return arr[arr.length - 1]
}

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Log - Encryption', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore: Keystore, signingKeystore: Keystore,
      senderKey: BoxKeyWithMeta, recieverKey: BoxKeyWithMeta

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)

      senderKey = await keystore.createKey('sender', BoxKeyWithMeta, undefined, { overwrite: true });
      recieverKey = await keystore.createKey('reciever', BoxKeyWithMeta, undefined, { overwrite: true });

      // The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
      signKey = await keystore.createKey(new Uint8Array([0]), SignKeyWithMeta);
      signKey2 = await keystore.createKey(new Uint8Array([1]), SignKeyWithMeta);
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
            getEncryptionKey: () => Promise.resolve(senderKey.secretKey),
            getAnySecret: async (publicKeys: X25519PublicKey[]) => {
              for (let i = 0; i < publicKeys.length; i++) {
                if (Buffer.compare(publicKeys[i].getBuffer(), senderKey.secretKey.getBuffer()) === 0) {
                  return {
                    index: i,
                    secretKey: senderKey.secretKey
                  }
                }
                if (Buffer.compare(publicKeys[i].getBuffer(), recieverKey.secretKey.getBuffer()) === 0) {
                  return {
                    index: i,
                    secretKey: recieverKey.secretKey
                  }
                }

              }
            }
          }
        };
        log1 = new Log(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), logOptions)
        log2 = new Log(ipfs, signKey2.publicKey, (data) => Keystore.sign(data, signKey2), logOptions)
      })

      it('join encrypted identities only with knowledge of id and clock', async () => {
        await log1.append('helloA1', { reciever: { id: undefined, clock: undefined, publicKey: recieverKey.publicKey, payload: recieverKey.publicKey, signature: recieverKey.publicKey } })
        await log1.append('helloA2', { reciever: { id: undefined, clock: undefined, publicKey: recieverKey.publicKey, payload: recieverKey.publicKey, signature: recieverKey.publicKey } })
        await log2.append('helloB1', { reciever: { id: undefined, clock: undefined, publicKey: recieverKey.publicKey, payload: recieverKey.publicKey, signature: recieverKey.publicKey } })
        await log2.append('helloB2', { reciever: { id: undefined, clock: undefined, publicKey: recieverKey.publicKey, payload: recieverKey.publicKey, signature: recieverKey.publicKey } })

        // Remove decrypted caches of the log2 values
        log2.values.forEach((value) => {
          value._id.clear();
          value._publicKey.clear();
          value._clock.clear();
          value._payload.clear();
          value._signature.clear();
          value._clock.clear();

        })

        await log1.join(log2)
        assert.strictEqual(log1.length, 4)
        const item = last(log1.values)
        assert.strictEqual(item.next.length, 1)
      })
    })
  })
})
