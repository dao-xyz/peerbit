import { Identities } from "../identities"
import { Identity, Signatures } from "../identity"

const assert = require('assert')
const path = require('path')
const rmrf = require('rimraf')
import { Keystore } from '@dao-xyz/orbit-db-keystore'
const fixturesPath = path.resolve(__dirname, 'fixtures/keys')
const savedKeysPath = path.resolve(__dirname, 'fixtures/savedKeys')
const signingKeysPath = path.resolve(__dirname, 'signingKeys')
const identityKeysPath = path.resolve(__dirname, 'identityKeys')
const migrate = require('localstorage-level-migration')
import fs from 'fs-extra';
import { joinUint8Arrays } from "../utils"
import { Ed25519PublicKey } from "sodium-plus"
const type = 'orbitdb'

describe('Identity Provider', function () {
  beforeAll(async () => {
    rmrf.sync(signingKeysPath)
    rmrf.sync(identityKeysPath)
  })

  afterAll(async () => {
    rmrf.sync(signingKeysPath)
    rmrf.sync(identityKeysPath)
  })

  describe('Creating Identities', () => {
    const id = new Uint8Array([1])
    let identity: Identity

    it('identityKeysPath only - has the correct id', async () => {
      identity = await Identities.createIdentity({ id, identityKeysPath })
      const key = await identity.provider.keystore.getKey(id)
      const externalId = new Uint8Array((await Keystore.getPublicSign(key)).getBuffer());
      assert.deepStrictEqual(identity.id, externalId)
    })

    it('identityKeysPath and signingKeysPath - has a different id', async () => {
      identity = await Identities.createIdentity({ id, identityKeysPath, signingKeysPath })
      const key = await identity.provider.keystore.getKey(id)
      const externalId = await Keystore.getPublicSign(key);
      try {
        assert.deepStrictEqual(identity.id, new Uint8Array(externalId.getBuffer()))
        assert(false); // expected to not be deep equal
      } catch (error) {
        // Expected
      }
    })

    afterEach(async () => {
      await identity?.provider.keystore.close()
      await identity?.provider.signingKeystore.close()
    })
  })

  describe('Passing in custom keystore', () => {
    const id = new Uint8Array([1]); let identity: Identity; let keystore: Keystore; let signingKeystore: Keystore

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
    })

    it('has the correct id', async () => {
      identity = await Identities.createIdentity({ id, keystore })
      keystore = identity.provider._keystore
      const key = await keystore.getKey(id)
      const externalId = await Keystore.getPublicSign(key);
      assert.deepStrictEqual(identity.id, new Uint8Array(externalId.getBuffer()))
    })

    it('created a key for id in identity-keystore', async () => {
      const key = await keystore.getKey(id)
      assert.notStrictEqual(key, undefined)
    })

    it('has the correct public key', async () => {
      const key = await keystore.getKey(id)
      const externalId = (await Keystore.getPublicSign(key)).toString('base64');
      const signingKey = await keystore.getKey(externalId)
      assert.notStrictEqual(signingKey, undefined)
      assert.deepStrictEqual(identity.publicKey, new Uint8Array((await Keystore.getPublicSign(signingKey)).getBuffer()))
    })

    it('has a signature for the id', async () => {
      const key = await keystore.getKey(id)
      const externalId = await Keystore.getPublicSign(key);
      const signingKey = await keystore.getKey(externalId.toString('base64'))
      const idSignature = await keystore.sign(signingKey, new Uint8Array(externalId.getBuffer()))
      const publicKey = await Keystore.getPublicSign(signingKey);
      const verifies = await Keystore.verify(idSignature, publicKey, new Uint8Array(externalId.getBuffer()))
      assert.strictEqual(verifies, true)
      assert.deepStrictEqual(identity.signatures.id, idSignature)
    })

    it('has a signature for the publicKey', async () => {
      const key = await keystore.getKey(id)
      const externalId = await Keystore.getPublicSign(key);
      const signingKey = await keystore.getKey(externalId.toString('base64'))
      const idSignature = await keystore.sign(signingKey, new Uint8Array(externalId.getBuffer()))
      const externalKey = await keystore.getKey(id)
      const publicKeyAndIdSignature = await keystore.sign(externalKey, joinUint8Arrays([identity.publicKey, idSignature]))
      assert.deepStrictEqual(identity.signatures.publicKey, publicKeyAndIdSignature)
    })

    afterAll(async () => {
      await keystore.close()
      await signingKeystore.close()
    })
  })

  describe('create an identity with saved keys', () => {
    let keystore: Keystore, signingKeystore: Keystore
    let savedKeysKeystore: Keystore, identity: Identity
    const id = new Uint8Array(Buffer.from('id'))
    const expectedPublicKey = new Uint8Array([151, 15, 154, 152, 163, 12, 235, 93, 47, 90, 49, 208, 165, 112, 247, 71, 132, 114, 205, 172, 92, 204, 115, 105, 138, 93, 10, 39, 225, 77, 120, 11])
    const expectedIdSignature = new Uint8Array([32, 79, 37, 236, 119, 230, 153, 106, 52, 233, 154, 211, 158, 194, 210, 175, 233, 39, 119, 103, 166, 98, 142, 198, 240, 246, 171, 63, 117, 25, 124, 13, 140, 169, 155, 240, 174, 31, 198, 172, 217, 203, 61, 21, 37, 7, 20, 239, 170, 18, 28, 120, 42, 115, 224, 55, 60, 217, 83, 215, 94, 243, 169, 6, 81, 88, 146, 112, 141, 42, 119, 208, 163, 50, 101, 22, 118, 86, 202, 59, 109, 75, 175, 47, 66, 46, 200, 240, 170, 144, 161, 24, 38, 178, 46, 50])
    const expectedPkIdSignature = new Uint8Array([32, 64, 152, 210, 238, 235, 182, 100, 141, 164, 232, 110, 213, 74, 176, 177, 62, 172, 169, 39, 150, 172, 174, 16, 130, 76, 49, 99, 200, 230, 27, 79, 81, 3, 117, 156, 88, 195, 106, 53, 91, 241, 139, 109, 196, 85, 217, 127, 167, 8, 12, 123, 80, 204, 63, 77, 134, 109, 222, 142, 64, 232, 213, 7, 151, 15, 154, 152, 163, 12, 235, 93, 47, 90, 49, 208, 165, 112, 247, 71, 132, 114, 205, 172, 92, 204, 115, 105, 138, 93, 10, 39, 225, 77, 120, 11, 32, 79, 37, 236, 119, 230, 153, 106, 52, 233, 154, 211, 158, 194, 210, 175, 233, 39, 119, 103, 166, 98, 142, 198, 240, 246, 171, 63, 117, 25, 124, 13, 140, 169, 155, 240, 174, 31, 198, 172, 217, 203, 61, 21, 37, 7, 20, 239, 170, 18, 28, 120, 42, 115, 224, 55, 60, 217, 83, 215, 94, 243, 169, 6, 81, 88, 146, 112, 141, 42, 119, 208, 163, 50, 101, 22, 118, 86, 202, 59, 109, 75, 175, 47, 66, 46, 200, 240, 170, 144, 161, 24, 38, 178, 46, 50])

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
      await fs.copy(fixturesPath, savedKeysPath)
      savedKeysKeystore = new Keystore(savedKeysPath)
      //await savedKeysKeystore.createKey(id, 'sign');
      /* */
      identity = await Identities.createIdentity({ id, keystore: savedKeysKeystore })
      /*  */
      const x = 132;
    })



    afterAll(async () => {
      rmrf.sync(savedKeysPath)
    })

    it('has the correct id', async () => {
      const key = await savedKeysKeystore.getKey(id)
      assert.deepStrictEqual(identity.id, new Uint8Array((await Keystore.getPublicSign(key)).getBuffer()));
    })

    it('has the correct public key', async () => {
      assert.deepStrictEqual(identity.publicKey, expectedPublicKey)
    })

    it('has the correct identity type', async () => {
      assert.strictEqual(identity.type, type)
    })

    it('has the correct idSignature', async () => {
      assert.deepStrictEqual(identity.signatures.id, expectedIdSignature)
    })

    it('has a pubKeyIdSignature for the publicKey', async () => {
      assert.deepStrictEqual(identity.signatures.publicKey, expectedPkIdSignature)
    })

    it('has the correct signatures', async () => {
      const internalSigningKey = await savedKeysKeystore.getKey(identity.id)
      const externalSigningKey = await savedKeysKeystore.getKey(id)
      const idSignature = await savedKeysKeystore.sign(internalSigningKey, identity.id)
      const pubKeyIdSignature = await savedKeysKeystore.sign(externalSigningKey, joinUint8Arrays([identity.publicKey, idSignature]))
      const expectedSignature = { id: idSignature, publicKey: pubKeyIdSignature }
      assert.deepStrictEqual({ ...identity.signatures }, expectedSignature)
    })

    afterAll(async () => {
      await keystore.close()
      await signingKeystore.close()
    })
  })

  describe('verify identity\'s signature', () => {
    const id = new Uint8Array(Buffer.from('QmFoo'))
    let identity: Identity, keystore: Keystore, signingKeystore

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
    })

    it('identity pkSignature verifies', async () => {
      identity = await Identities.createIdentity({ id, type, keystore, signingKeystore })
      const verified = await Keystore.verify(identity.signatures.id, new Ed25519PublicKey(Buffer.from(identity.publicKey)), identity.id)
      assert.strictEqual(verified, true)
    })

    it('identity signature verifies', async () => {
      identity = await Identities.createIdentity({ id, type, keystore, signingKeystore })
      const verified = await Keystore.verify(identity.signatures.publicKey, new Ed25519PublicKey(Buffer.from(identity.id)), joinUint8Arrays([identity.publicKey, identity.signatures.id]))
      assert.strictEqual(verified, true)
    })

    it('false signature doesn\'t verify', async () => {
      class IP {
        async getId() { return 'pubKey' }

        async sign(data) { return `false signature '${data}'` }

        static async verifyIdentity(data) { return false }

        static get type() { return 'fake' }
      }

      Identities.addIdentityProvider(IP)
      identity = await Identities.createIdentity({ type: IP.type, keystore, signingKeystore })
      const verified = await Identities.verifyIdentity(identity)
      assert.strictEqual(verified, false)
    })

    afterAll(async () => {
      await keystore.close()
      await signingKeystore.close()
    })
  })

  describe('verify identity', () => {
    const id = new Uint8Array(Buffer.from('QmFoo'))
    let identity: Identity, keystore, signingKeystore

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
    })

    it('identity verifies', async () => {
      identity = await Identities.createIdentity({ id, type, keystore, signingKeystore })
      const verified = await identity.provider.verifyIdentity(identity)
      assert.strictEqual(verified, true)
    })

    afterAll(async () => {
      await keystore.close()
      await signingKeystore.close()
    })
  })

  describe('sign data with an identity', () => {
    const id = new Uint8Array(Buffer.from('0x01234567890abcdefghijklmnopqrstuvwxyz'))
    const data = new Uint8Array([1, 2, 3])
    let identity: Identity, keystore: Keystore, signingKeystore: Keystore

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
      identity = await Identities.createIdentity({ id, keystore, signingKeystore })
    })

    it('sign data', async () => {
      const signingKey = await keystore.getKey(identity.id)
      const expectedSignature = await keystore.sign(signingKey, data)
      const signature = await identity.provider.sign(identity, data)
      assert.deepStrictEqual(signature, expectedSignature)
    })

    it('throws an error if private key is not found from keystore', async () => {
      // Remove the key from the keystore (we're using a mock storage in these tests)
      const modifiedIdentity = new Identity({
        id: new Uint8Array([123]), publicKey: identity.publicKey, signatures: new Signatures({ id: new Uint8Array([0]), publicKey: identity.signatures.publicKey }), type: identity.type, provider: identity.provider
      })
      let signature
      let err
      try {
        signature = await identity.provider.sign(modifiedIdentity, data)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(signature, undefined)
      assert.strictEqual(err, 'Error: Private signing key not found from Keystore')
    })

    afterAll(async () => {
      await keystore.close()
      await signingKeystore.close()
    })
  })

  describe('verify data signed by an identity', () => {
    const id = new Uint8Array(Buffer.from('03602a3da3eb35f1148e8028f141ec415ef7f6d4103443edbfec2a0711d716f53f'))
    const data = new Uint8Array(Buffer.from('hello friend'))
    let identity: Identity, keystore: Keystore, signingKeystore: Keystore
    let signature

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
    })

    beforeEach(async () => {
      identity = await Identities.createIdentity({ id, type, keystore, signingKeystore })
      signature = await identity.provider.sign(identity, data)
    })

    it('verifies that the signature is valid', async () => {
      const verified = await identity.provider.verify(signature, new Ed25519PublicKey(Buffer.from(identity.publicKey)), data)
      assert.strictEqual(verified, true)
    })

    it('doesn\'t verify invalid signature', async () => {
      const verified = await identity.provider.verify(new Uint8Array([0, 0, 0]), new Ed25519PublicKey(Buffer.from(identity.publicKey)), data)
      assert.strictEqual(verified, false)
    })

    afterAll(async () => {
      await keystore.close()
      await signingKeystore.close()
    })
  })

  // TODO fix failing (bad test)
  describe('create identity from existing keys', () => {
    const source = fixturesPath + '/existing'
    const publicKey = '045756c20f03ec494d07e8dd8456f67d6bd97ca175e6c4882435fe364392f131406db3a37eebe1d634b105a57b55e4f17247c1ec8ffe04d6a95d1e0ee8bed7cfbd'
    let identity: Identity, keystore: Keystore, signingKeystore: Keystore

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
      identity = await Identities.createIdentity({ id: new Uint8Array(Buffer.from('A')), migrate: migrate(source) as any, keystore, signingKeystore })
    })

    it('creates identity with correct public key', async () => {
      assert.strictEqual(identity.publicKey, publicKey)
    })

    it('verifies signatures signed by existing key', async () => {
      const sig = new Uint8Array([1, 2, 3])
      const ver = await identity.provider.verify(sig, new Ed25519PublicKey(Buffer.from(identity.publicKey)), 'signme')
      assert.strictEqual(ver, true)
    })

    afterAll(async () => {
      await keystore.close()
      await signingKeystore.close()
    })
  })
})
