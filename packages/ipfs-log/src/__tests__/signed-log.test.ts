const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { AccessController } from '../default-access-controller'
import { Log } from '../log'
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import { assertPayload } from './utils/assert'
import { Metadata } from '@dao-xyz/ipfs-log-entry'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { serialize } from '@dao-xyz/borsh'
import { DecryptedThing } from '@dao-xyz/encryption-utils'

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
      assert.deepStrictEqual(log._identity.id, new Uint8Array([111, 6, 85, 222, 160, 121, 220, 218, 205, 255, 21, 34, 65, 120, 252, 76, 2, 241, 182, 50, 29, 37, 245, 30, 202, 21, 214, 10, 252, 73, 167, 230]))
      assert.deepStrictEqual(log._identity.publicKey, new Uint8Array([240, 177, 8, 169, 97, 21, 230, 138, 138, 99, 69, 70, 172, 19, 253, 191, 14, 57, 119, 130, 161, 179, 40, 29, 220, 145, 162, 221, 100, 1, 22, 179]))
      assert.deepStrictEqual(log._identity.signatures.id, new Uint8Array([94, 213, 221, 3, 129, 109, 111, 5, 154, 104, 176, 108, 180, 52, 238, 236, 10, 230, 234, 134, 144, 254, 20, 195, 247, 167, 95, 192, 115, 179, 155, 99, 190, 83, 56, 134, 146, 126, 234, 5, 19, 7, 126, 98, 146, 161, 241, 49, 39, 203, 178, 137, 109, 252, 67, 60, 49, 250, 171, 198, 124, 166, 181, 9, 111, 6, 85, 222, 160, 121, 220, 218, 205, 255, 21, 34, 65, 120, 252, 76, 2, 241, 182, 50, 29, 37, 245, 30, 202, 21, 214, 10, 252, 73, 167, 230]))
      assert.deepStrictEqual(log._identity.signatures.publicKey, new Uint8Array([153, 57, 78, 75, 239, 78, 27, 30, 51, 49, 242, 47, 42, 58, 0, 11, 178, 119, 33, 87, 95, 117, 202, 43, 245, 66, 173, 242, 165, 174, 239, 28, 221, 138, 0, 237, 169, 64, 221, 11, 141, 24, 82, 111, 77, 207, 229, 71, 214, 19, 151, 51, 213, 193, 109, 51, 145, 190, 145, 168, 255, 130, 252, 4, 249, 145, 187, 86, 63, 149, 199, 198, 57, 245, 252, 154, 48, 16, 105, 118, 136, 151, 13, 190, 204, 44, 32, 250, 144, 165, 173, 217, 231, 38, 141, 155, 98, 24, 169, 199, 9, 58, 88, 45, 228, 51, 81, 54, 20, 223, 115, 33, 122, 47, 222, 172, 161, 205, 149, 197, 148, 251, 49, 19, 142, 74, 10, 255, 164, 233, 204, 87, 0, 229, 53, 111, 219, 92, 111, 14, 149, 94, 23, 252, 246, 33, 26, 193, 17, 233, 87, 247, 89, 134, 240, 134, 43, 34, 155, 12, 93, 201, 5, 196, 210, 158, 82, 212, 109, 104, 191, 137, 168, 231, 184, 220, 225, 250, 242, 158, 33, 48, 197, 90, 87, 8, 234, 41, 227, 127, 18, 251]))
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
      assert.notStrictEqual(await log.values[0].metadata.signature, null)
      assert.deepStrictEqual(await log.values[0].metadata.identity, testIdentity.toSerializable())
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
      assertPayload(log1.values[0].payload.value, 'one')
    })



    it('throws an error if log is signed but trying to merge an entry that doesn\'t have a signature', async () => {
      const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
      const log2 = new Log<string>(ipfs, testIdentity2, { logId: 'A' })

      let err
      try {
        await log1.append('one')
        await log2.append('two')
        delete (log2.values[0].metadata)._metadata
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(err, 'Error: Unsupported')
    })

    it('throws an error if log is signed but the signature doesn\'t verify', async () => {
      const log1 = new Log<string>(ipfs, testIdentity, { logId: 'A' })
      const log2 = new Log<string>(ipfs, testIdentity2, { logId: 'A' })
      let err

      try {
        await log1.append('one');
        await log2.append('two');
        (log2.values[0].metadata._metadata as DecryptedThing<Metadata>)._data = serialize(new Metadata({
          id: await log2.values[0].metadata.id,
          clock: await log2.values[0].metadata.clock,
          signature: await log1.values[0].metadata.signature,
          identity: await log2.values[0].metadata.identity
        }))
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
      }

      const entry = log2.values[0]
      assert.strictEqual(err, `Error: Could not validate signature "${await entry.metadata.signature}" for entry "${entry.hash}" and key "${(await entry.metadata.identity).publicKey}"`)
      assert.strictEqual(log1.values.length, 1)
      assertPayload(log1.values[0].payload.value, 'one')
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
        canAppend: (entry, identity, _) => Buffer.compare(identity.id, testIdentity.id) === 0
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
