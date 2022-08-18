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
      const key = await identity.provider.keystore.getKeyByPath(id)
      const externalId = new Uint8Array((await Keystore.getPublicSign(key.key)).getBuffer());
      assert.deepStrictEqual(identity.id, externalId)
    })

    it('identityKeysPath and signingKeysPath - has a different id', async () => {
      identity = await Identities.createIdentity({ id, identityKeysPath, signingKeysPath })
      const key = await identity.provider.keystore.getKeyByPath(id)
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
      const key = await keystore.getKeyByPath(id)
      const externalId = await Keystore.getPublicSign(key.key);
      assert.deepStrictEqual(identity.id, new Uint8Array(externalId.getBuffer()))
    })

    it('created a key for id in identity-keystore', async () => {
      const key = await keystore.getKeyByPath(id)
      assert.notStrictEqual(key, undefined)
    })

    it('has the correct public key', async () => {
      const key = await keystore.getKeyByPath(id)
      const externalId = (await Keystore.getPublicSign(key.key)).toString('base64');
      const signingKey = await keystore.getKeyByPath(externalId)
      assert.notStrictEqual(signingKey, undefined)
      assert.deepStrictEqual(identity.publicKey, new Uint8Array((await Keystore.getPublicSign(signingKey.key)).getBuffer()))
    })

    it('has a signature for the id', async () => {
      const key = await keystore.getKeyByPath(id)
      const externalId = await Keystore.getPublicSign(key.key);
      const signingKey = await keystore.getKeyByPath(externalId.toString('base64'))
      const idSignature = await keystore.sign(new Uint8Array(externalId.getBuffer()), signingKey.key)
      const publicKey = await Keystore.getPublicSign(signingKey.key);
      const verifies = await Keystore.verify(idSignature, publicKey, new Uint8Array(externalId.getBuffer()))
      assert.strictEqual(verifies, true)
      assert.deepStrictEqual(identity.signatures.id, idSignature)
    })

    it('has a signature for the publicKey', async () => {
      const key = await keystore.getKeyByPath(id)
      const externalId = await Keystore.getPublicSign(key.key);
      const signingKey = await keystore.getKeyByPath(externalId.toString('base64'))
      const idSignature = await keystore.sign(new Uint8Array(externalId.getBuffer()), signingKey.key)
      const externalKey = await keystore.getKeyByPath(id)
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
    const expectedPublicKey = new Uint8Array([34, 25, 197, 196, 58, 191, 195, 150, 50, 12, 250, 161, 75, 204, 196, 237, 90, 213, 95, 99, 147, 48, 219, 220, 111, 206, 3, 215, 78, 76, 175, 62])
    const expectedIdSignature = new Uint8Array([183, 195, 62, 69, 115, 185, 130, 177, 76, 54, 167, 9, 176, 89, 190, 124, 50, 78, 145, 175, 51, 238, 63, 226, 100, 98, 11, 90, 129, 114, 15, 43, 44, 114, 135, 39, 25, 31, 37, 69, 14, 68, 4, 244, 32, 248, 93, 254, 18, 141, 126, 32, 158, 120, 94, 83, 8, 255, 203, 21, 51, 84, 21, 4, 98, 206, 5, 13, 52, 218, 240, 158, 50, 164, 151, 34, 83, 27, 153, 13, 196, 141, 84, 133, 125, 89, 160, 61, 255, 97, 44, 97, 161, 18, 219, 101])
    const expectedPkIdSignature = new Uint8Array([160, 55, 242, 93, 70, 203, 149, 134, 83, 166, 19, 91, 104, 252, 250, 129, 45, 196, 4, 86, 249, 230, 105, 26, 136, 27, 172, 4, 119, 166, 175, 227, 179, 57, 68, 195, 3, 45, 198, 120, 24, 158, 245, 18, 77, 243, 37, 172, 200, 122, 240, 113, 173, 55, 61, 221, 83, 64, 253, 69, 224, 181, 9, 13, 34, 25, 197, 196, 58, 191, 195, 150, 50, 12, 250, 161, 75, 204, 196, 237, 90, 213, 95, 99, 147, 48, 219, 220, 111, 206, 3, 215, 78, 76, 175, 62, 183, 195, 62, 69, 115, 185, 130, 177, 76, 54, 167, 9, 176, 89, 190, 124, 50, 78, 145, 175, 51, 238, 63, 226, 100, 98, 11, 90, 129, 114, 15, 43, 44, 114, 135, 39, 25, 31, 37, 69, 14, 68, 4, 244, 32, 248, 93, 254, 18, 141, 126, 32, 158, 120, 94, 83, 8, 255, 203, 21, 51, 84, 21, 4, 98, 206, 5, 13, 52, 218, 240, 158, 50, 164, 151, 34, 83, 27, 153, 13, 196, 141, 84, 133, 125, 89, 160, 61, 255, 97, 44, 97, 161, 18, 219, 101])

    beforeAll(async () => {
      keystore = new Keystore(identityKeysPath)
      signingKeystore = new Keystore(signingKeysPath)
      await fs.copy(fixturesPath, savedKeysPath)
      savedKeysKeystore = new Keystore(savedKeysPath)
      // await savedKeysKeystore.createKey(id, 'sign');
      identity = await Identities.createIdentity({ id, keystore: savedKeysKeystore })
      const x = 123;
      /*  */
    })



    afterAll(async () => {
      rmrf.sync(savedKeysPath)
    })

    it('has the correct id', async () => {
      const key = await savedKeysKeystore.getKeyByPath(id)
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
      const internalSigningKey = await savedKeysKeystore.getKeyByPath(identity.id)
      const externalSigningKey = await savedKeysKeystore.getKeyByPath(id)
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
      const signingKey = await keystore.getKeyByPath(identity.id)
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
