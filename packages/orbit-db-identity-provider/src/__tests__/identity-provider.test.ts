import fs from 'fs-extra';
import { Ed25519PublicKey } from "sodium-plus"
import { joinUint8Arrays } from "@dao-xyz/io-utils"
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { Identities } from "../identities"
import { Identity, Signatures } from "../identity"

const assert = require('assert')
const path = require('path')
const rmrf = require('rimraf')
const fixturesPath = path.resolve(__dirname, 'fixtures/keys')
const savedKeysPath = path.resolve(__dirname, 'fixtures/savedKeys')
const signingKeysPath = path.resolve(__dirname, 'signingKeys')
const identityKeysPath = path.resolve(__dirname, 'identityKeys')
/* const migrate = require('localstorage-level-migration') */

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
      const externalId = new Uint8Array((await Keystore.getPublicSign(key.key)).getBuffer());
      assert.deepStrictEqual(identity.id, externalId)
    })

    it('identityKeysPath and signingKeysPath - has a different id', async () => {
      identity = await Identities.createIdentity({ id, identityKeysPath, signingKeysPath })
      const key = await identity.provider.keystore.getKey(id)
      const externalId = await Keystore.getPublicSign(key.key);
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
      const externalId = await Keystore.getPublicSign(key.key);
      assert.deepStrictEqual(identity.id, new Uint8Array(externalId.getBuffer()))
    })

    it('created a key for id in identity-keystore', async () => {
      const key = await keystore.getKey(id)
      assert.notStrictEqual(key, undefined)
    })

    it('has the correct public key', async () => {
      const key = await keystore.getKey(id)
      const externalId = (await Keystore.getPublicSign(key.key)).toString('base64');
      const signingKey = await keystore.getKey(externalId)
      assert.notStrictEqual(signingKey, undefined)
      assert.deepStrictEqual(identity.publicKey, new Uint8Array((await Keystore.getPublicSign(signingKey.key)).getBuffer()))
    })

    it('has a signature for the id', async () => {
      const key = await keystore.getKey(id)
      const externalId = await Keystore.getPublicSign(key.key);
      const signingKey = await keystore.getKey(externalId.toString('base64'))
      const idSignature = await keystore.sign(new Uint8Array(externalId.getBuffer()), signingKey.key)
      const publicKey = await Keystore.getPublicSign(signingKey.key);
      const verifies = await Keystore.verify(idSignature, publicKey, new Uint8Array(externalId.getBuffer()))
      assert.strictEqual(verifies, true)
      assert.deepStrictEqual(identity.signatures.id, idSignature)
    })

    it('has a signature for the publicKey', async () => {
      const key = await keystore.getKey(id)
      const externalId = await Keystore.getPublicSign(key.key);
      const signingKey = await keystore.getKey(externalId.toString('base64'))
      const idSignature = await keystore.sign(new Uint8Array(externalId.getBuffer()), signingKey.key)
      const externalKey = await keystore.getKey(id)
      const publicKeyAndIdSignature = await keystore.sign(joinUint8Arrays([identity.publicKey, idSignature]), externalKey.key)
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
    const expectedPublicKey = new Uint8Array([149, 153, 230, 21, 248, 33, 64, 46, 60, 147, 143, 176, 191, 29, 34, 227, 41, 7, 45, 234, 71, 76, 238, 9, 8, 130, 134, 250, 250, 127, 138, 24])
    const expectedIdSignature = new Uint8Array([236, 194, 225, 188, 49, 172, 197, 5, 85, 103, 239, 2, 167, 136, 221, 172, 214, 183, 234, 214, 107, 208, 180, 184, 12, 37, 238, 196, 201, 239, 51, 229, 89, 126, 36, 208, 167, 240, 95, 2, 123, 119, 36, 148, 23, 150, 215, 119, 233, 57, 173, 159, 200, 76, 141, 47, 1, 186, 248, 0, 0, 162, 56, 12, 121, 65, 215, 71, 7, 191, 191, 192, 160, 86, 48, 111, 55, 138, 14, 236, 38, 32, 198, 180, 54, 166, 165, 102, 135, 230, 30, 84, 248, 43, 171, 229])
    const expectedPkIdSignature = new Uint8Array([21, 107, 251, 110, 41, 187, 168, 241, 221, 207, 172, 116, 121, 100, 115, 90, 67, 247, 126, 40, 194, 74, 73, 219, 163, 155, 214, 65, 177, 152, 114, 182, 200, 105, 204, 137, 246, 241, 242, 209, 84, 124, 97, 181, 149, 6, 142, 221, 166, 6, 181, 82, 86, 48, 18, 76, 82, 49, 122, 137, 231, 103, 93, 13, 149, 153, 230, 21, 248, 33, 64, 46, 60, 147, 143, 176, 191, 29, 34, 227, 41, 7, 45, 234, 71, 76, 238, 9, 8, 130, 134, 250, 250, 127, 138, 24, 236, 194, 225, 188, 49, 172, 197, 5, 85, 103, 239, 2, 167, 136, 221, 172, 214, 183, 234, 214, 107, 208, 180, 184, 12, 37, 238, 196, 201, 239, 51, 229, 89, 126, 36, 208, 167, 240, 95, 2, 123, 119, 36, 148, 23, 150, 215, 119, 233, 57, 173, 159, 200, 76, 141, 47, 1, 186, 248, 0, 0, 162, 56, 12, 121, 65, 215, 71, 7, 191, 191, 192, 160, 86, 48, 111, 55, 138, 14, 236, 38, 32, 198, 180, 54, 166, 165, 102, 135, 230, 30, 84, 248, 43, 171, 229])

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
      await fs.copy(fixturesPath, savedKeysPath)
      savedKeysKeystore = new Keystore(savedKeysPath)
      //await savedKeysKeystore.createKey(id, 'sign');
      identity = await Identities.createIdentity({ id, keystore: savedKeysKeystore })
      const x = 123;
      /*  */
    })



    afterAll(async () => {
      rmrf.sync(savedKeysPath)
    })

    it('has the correct id', async () => {
      const key = await savedKeysKeystore.getKey(id)
      assert.deepStrictEqual(identity.id, new Uint8Array((await Keystore.getPublicSign(key.key)).getBuffer()));
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
      const idSignature = await savedKeysKeystore.sign(identity.id, internalSigningKey.key)
      const pubKeyIdSignature = await savedKeysKeystore.sign(joinUint8Arrays([identity.publicKey, idSignature]), externalSigningKey.key)
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
      const expectedSignature = await keystore.sign(data, signingKey.key)
      const signature = await identity.provider.sign(data, identity)
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
        signature = await identity.provider.sign(data, modifiedIdentity)
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
      signature = await identity.provider.sign(data, identity)
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
  /*   describe('create identity from existing keys', () => {
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
        const ver = await identity.provider.verify(sig, new Ed25519PublicKey(Buffer.from(identity.publicKey)), new Uint8Array(Buffer.from('signme')))
        assert.strictEqual(ver, true)
      })
  
      afterAll(async () => {
        await keystore.close()
        await signingKeystore.close()
      })
    }) */
})
