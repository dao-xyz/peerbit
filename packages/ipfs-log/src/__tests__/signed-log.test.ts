const assert = require('assert')
const rmrf = require('rimraf')
const fs = require('fs-extra')
import { AccessController } from '../default-access-controller'
import { Log } from '../log'
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider'
import { assertPayload } from './utils/assert'
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { Entry } from '@dao-xyz/ipfs-log-entry'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

let ipfsd, ipfs, testIdentity: Identity, testIdentity2: Identity

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
      expect(log._identity.toSerializable()).toMatchSnapshot('identity');
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
      assert.notStrictEqual(await log.values[0].signature, null)
      assert.deepStrictEqual(await log.values[0].identity, testIdentity.toSerializable())
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
        delete (log2.values[0]._signature)
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
        let entry: Entry<string> = log2.values[0]
        entry._signature = await log1.values[0]._signature;
        await log1.join(log2)
      } catch (e) {
        err = e.toString()
      }

      const entry = log2.values[0]
      assert.strictEqual(err, `Error: Could not validate signature "${await entry.signature}" for entry "${entry.hash}" and key "${(await entry.identity).publicKey}"`)
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
        canAppend: async (_entry, identity, _) => Buffer.compare(Buffer.from(identity.decrypted.getValue(IdentitySerializable).id), Buffer.from(testIdentity.id)) === 0
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
