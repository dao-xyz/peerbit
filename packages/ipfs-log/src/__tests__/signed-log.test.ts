const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { AccessController } from '../default-access-controller'
import { Log } from '../log'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import { assertPayload } from './utils/assert'
import { EntryDataDecrypted } from '@dao-xyz/ipfs-log-entry'
import { Keystore } from '@dao-xyz/orbit-db-keystore'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity, testIdentity2

Object.keys(testAPIs).forEach((IPFS) => {
  describe('Signed Log', function () {
    jest.setTimeout(config.timeout)

    const { identityKeyFixtures, signingKeyFixtures, identityKeysPath, signingKeysPath } = config

    let keystore, signingKeystore

    beforeAll(async () => {
      rmrf.sync(identityKeysPath)
      rmrf.sync(signingKeysPath)
      await fs.copy(identityKeyFixtures(__dirname), identityKeysPath)
      await fs.copy(signingKeyFixtures(__dirname), signingKeysPath)

      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)


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

    it('creates a signed log', () => {
      const logId = 'A'
      const log = new Log(ipfs, testIdentity, { logId })
      assert.notStrictEqual(log.id, null)
      assert.strictEqual(log.id, logId)
    })

    it('has the correct identity', () => {
      const log = new Log(ipfs, testIdentity, { logId: 'A' })
      assert.notStrictEqual(log.id, null)
      assert.deepStrictEqual(log._identity.id, new Uint8Array([142, 77, 162, 36, 240, 138, 190, 8, 68, 7, 85, 219, 60, 66, 84, 40, 10, 114, 139, 141, 185, 144, 70, 51, 71, 38, 16, 216, 169, 95, 44, 10]))
      assert.deepStrictEqual(log._identity.publicKey, new Uint8Array([255, 175, 77, 50, 85, 231, 150, 224, 187, 183, 32, 179, 123, 47, 244, 109, 152, 79, 144, 143, 77, 230, 39, 92, 230, 45, 82, 105, 53, 99, 12, 119]))
      assert.deepStrictEqual(log._identity.signatures.id, new Uint8Array([235, 205, 152, 103, 111, 24, 200, 48, 188, 2, 194, 146, 95, 91, 175, 37, 166, 109, 146, 142, 245, 228, 245, 118, 100, 83, 116, 199, 34, 35, 114, 214, 8, 253, 18, 223, 79, 82, 146, 204, 77, 33, 156, 226, 153, 56, 61, 10, 114, 240, 205, 219, 190, 172, 73, 78, 89, 14, 43, 217, 83, 118, 10, 8, 142, 77, 162, 36, 240, 138, 190, 8, 68, 7, 85, 219, 60, 66, 84, 40, 10, 114, 139, 141, 185, 144, 70, 51, 71, 38, 16, 216, 169, 95, 44, 10]))
      assert.deepStrictEqual(log._identity.signatures.publicKey, new Uint8Array([211, 131, 69, 12, 171, 132, 229, 14, 56, 130, 219, 110, 210, 98, 236, 74, 45, 72, 52, 175, 61, 72, 149, 167, 217, 159, 217, 181, 60, 106, 18, 230, 112, 146, 196, 53, 55, 17, 162, 74, 93, 79, 227, 141, 72, 4, 156, 254, 34, 149, 193, 38, 212, 23, 215, 159, 156, 112, 198, 40, 51, 40, 142, 13, 255, 175, 77, 50, 85, 231, 150, 224, 187, 183, 32, 179, 123, 47, 244, 109, 152, 79, 144, 143, 77, 230, 39, 92, 230, 45, 82, 105, 53, 99, 12, 119, 235, 205, 152, 103, 111, 24, 200, 48, 188, 2, 194, 146, 95, 91, 175, 37, 166, 109, 146, 142, 245, 228, 245, 118, 100, 83, 116, 199, 34, 35, 114, 214, 8, 253, 18, 223, 79, 82, 146, 204, 77, 33, 156, 226, 153, 56, 61, 10, 114, 240, 205, 219, 190, 172, 73, 78, 89, 14, 43, 217, 83, 118, 10, 8, 142, 77, 162, 36, 240, 138, 190, 8, 68, 7, 85, 219, 60, 66, 84, 40, 10, 114, 139, 141, 185, 144, 70, 51, 71, 38, 16, 216, 169, 95, 44, 10]))
    })

    it('has the correct public key', () => {
      const log = new Log(ipfs, testIdentity, { logId: 'A' })
      assert.strictEqual(log._identity.publicKey, testIdentity.publicKey)
    })

    it('has the correct pkSignature', () => {
      const log = new Log(ipfs, testIdentity, { logId: 'A' })
      assert.strictEqual(log._identity.signatures.id, testIdentity.signatures.id)
    })

    it('has the correct signature', () => {
      const log = new Log(ipfs, testIdentity, { logId: 'A' })
      assert.strictEqual(log._identity.signatures.publicKey, testIdentity.signatures.publicKey)
    })

    it('entries contain an identity', async () => {
      const log = new Log(ipfs, testIdentity, { logId: 'A' })
      await log.append('one')
      assert.notStrictEqual(log.values[0].data.sig, null)
      assert.deepStrictEqual(log.values[0].data.identity, testIdentity.toSerializable())
    })

    it('doesn\'t sign entries when identity is not defined', async () => {
      let err
      try {
        const log = new Log(ipfs, undefined, undefined) // eslint-disable-line no-unused-vars
      } catch (e) {
        err = e
      }
      assert.strictEqual(err.message, 'Identity is required')
    })

    it('doesn\'t join logs with different IDs ', async () => {
      const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
      const log2 = new Log<string>(ipfs, testIdentity2, { logId: 'B' })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        await log2.append('three')
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
        throw e
      }

      assert.strictEqual(err, undefined)
      assert.strictEqual(log1.id, 'A')
      assert.strictEqual(log1.values.length, 1)
      assertPayload(log1.values[0].data.payload, 'one')
    })

    it('throws an error if log is signed but trying to merge with an entry that doesn\'t have public signing key', async () => {
      const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
      const log2 = new Log<string>(ipfs, testIdentity2, { logId: 'A' })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        delete (log2.values[0].data as EntryDataDecrypted<string>)._key
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Entry doesn\'t have a key')
    })

    it('throws an error if log is signed but trying to merge an entry that doesn\'t have a signature', async () => {
      const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
      const log2 = new Log<string>(ipfs, testIdentity2, { logId: 'A' })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        delete (log2.values[0].data as EntryDataDecrypted<string>)._sig
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Entry doesn\'t have a signature')
    })

    it('throws an error if log is signed but the signature doesn\'t verify', async () => {
      const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
      const log2 = new Log<string>(ipfs, testIdentity2, { logId: 'A' })
      let err

      try {
        await log1.append('one');
        await log2.append('two');
        (log2.values[0].data as EntryDataDecrypted<string>)._sig = log1.values[0].data.sig
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
      }

      const entry = log2.values[0]
      assert.strictEqual(err, `Error: Could not validate signature "${entry.data.sig}" for entry "${entry.hash}" and key "${entry.data.key}"`)
      assert.strictEqual(log1.values.length, 1)
      assertPayload(log1.values[0].data.payload, 'one')
    })

    it('throws an error if entry doesn\'t have append access', async () => {
      const denyAccess = { canAppend: (_, __) => false } as AccessController<string>
      const log1 = new Log(ipfs, testIdentity, { logId: 'A' })
      const log2 = new Log(ipfs, testIdentity2, { logId: 'A', access: denyAccess })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
      }

      assert.strictEqual(err, `Error: Could not append Entry<T>, key "${testIdentity2.id}" is not allowed to write to the log`)
    })

    it('throws an error upon join if entry doesn\'t have append access', async () => {
      const testACL = {
        canAppend: (entry, _) => entry.data.identity.id !== testIdentity2.id
      } as AccessController<string>;
      const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A', access: testACL })
      const log2 = new Log<string>(ipfs, testIdentity2, { logId: 'A' })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
      }

      assert.strictEqual(err, `Error: Could not append Entry<T>, key "${testIdentity2.id}" is not allowed to write to the log`)
    })
  })
})
