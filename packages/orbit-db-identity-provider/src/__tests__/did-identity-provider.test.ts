import { DIDIdentityProvider } from "../did-identity-provider"
import { Identities } from "../identities"
import { Identity, Signatures } from "../identity"

const assert = require('assert')
const path = require('path')
const rmrf = require('rimraf')
import { Ed25519Provider } from 'key-did-provider-ed25519'
import { Ed25519PublicKey } from 'sodium-plus'
const { default: KeyResolver } = require('key-did-resolver')
import { Keystore } from '@dao-xyz/orbit-db-keystore'
const keypath = path.resolve(__dirname, 'keys')

let keystore: Keystore

const seed = new Uint8Array([157, 94, 116, 198, 19, 248, 93, 239, 173, 82, 245, 222, 199, 7, 183, 177, 123, 238, 83, 240, 143, 188, 87, 191, 33, 95, 58, 136, 46, 218, 219, 245])
const didStr = new Uint8Array([100, 105, 100, 58, 107, 101, 121, 58, 122, 54, 77, 107, 112, 110, 84, 74, 119, 114, 114, 86, 117, 112, 104, 78, 104, 49, 117, 75, 98, 53, 68, 66, 55, 101, 82, 120, 118, 113, 110, 105, 86, 97, 83, 68, 85, 72, 85, 54, 106, 116, 71, 86, 109, 110, 51, 114]);

const type = DIDIdentityProvider.type
describe('DID Identity Provider', function () {
  beforeAll(async () => {
    rmrf.sync(keypath)
    DIDIdentityProvider.setDIDResolver(KeyResolver.getResolver())
    Identities.addIdentityProvider(DIDIdentityProvider)
    keystore = new Keystore(keypath)
  })

  afterAll(async () => {
    await keystore.close()
    rmrf.sync(keypath)
  })

  describe('create an DID identity', () => {
    let identity: Identity

    beforeAll(async () => {
      const didProvider = new Ed25519Provider(seed)
      identity = await Identities.createIdentity({ type, keystore, didProvider })
    })

    it('has the correct id', async () => {
      assert.deepStrictEqual(identity.id, didStr)
    })

    it('created a key for id in keystore', async () => {
      const key = await keystore.getKey(Buffer.from(didStr).toString())
      assert.notStrictEqual(key, undefined)
    })

    it('has the correct public key', async () => {
      const signingKey = await keystore.getKey(Buffer.from(didStr).toString())
      assert.notStrictEqual(signingKey, undefined)
      assert.deepStrictEqual(identity.publicKey, new Uint8Array((await Keystore.getPublicSign(signingKey)).getBuffer()))
    })

    it('has a signature for the id', async () => {
      const signingKey = await keystore.getKey(Buffer.from(didStr).toString())
      const idSignature = await keystore.sign(signingKey, didStr)
      const verifies = await Keystore.verify(idSignature, new Ed25519PublicKey(Buffer.from(identity.publicKey)), new Uint8Array(Buffer.from(didStr)))
      assert.strictEqual(verifies, true)
      assert.deepStrictEqual(identity.signatures.id, idSignature)
    })

    it('has a signature for the publicKey', async () => {
      const signingKey = await keystore.getKey(Buffer.from(didStr).toString())
      const idSignature = await keystore.sign(signingKey, didStr)
      assert.notStrictEqual(idSignature, undefined)
    })
  })

  describe('verify identity', () => {
    let identity: Identity

    beforeAll(async () => {
      const didProvider = new Ed25519Provider(seed)
      identity = await Identities.createIdentity({ type, keystore, didProvider })
    })

    it('DID identity verifies', async () => {
      const verified = await Identities.verifyIdentity(identity)
      assert.strictEqual(verified, true)
    })

    it('DID identity with incorrect id does not verify', async () => {
      const identity2 = new Identity({
        id: new Uint8Array([1, 1, 1]), publicKey: identity.publicKey, signatures: identity.signatures, type: identity.type, provider: identity.provider
      })
      const verified = await Identities.verifyIdentity(identity2)
      assert.strictEqual(verified, false)
    })
  })

  describe('sign data with an identity', () => {
    let identity: Identity
    const data = new Uint8Array(Buffer.from('hello friend'))

    beforeAll(async () => {
      const didProvider = new Ed25519Provider(seed)
      identity = await Identities.createIdentity({ type, keystore, didProvider })
    })

    it('sign data', async () => {
      const signingKey = await keystore.getKey(Buffer.from(identity.id).toString())
      const expectedSignature = await keystore.sign(signingKey, data)
      const signature = await identity.provider.sign(identity, data)
      assert.deepStrictEqual(signature, expectedSignature)
    })

    it('throws an error if private key is not found from keystore', async () => {
      // Remove the key from the keystore (we're using a mock storage in these tests)
      const modifiedIdentity = new Identity({
        id: new Uint8Array([1, 1, 1]), publicKey: identity.publicKey, signatures: new Signatures({ id: new Uint8Array([1, 1, 1]), publicKey: identity.signatures.publicKey }), type: identity.type, provider: identity.provider
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

    describe('verify data signed by an identity', () => {
      const data = new Uint8Array(Buffer.from('hello friend'))
      let identity: Identity
      let signature: Uint8Array

      beforeAll(async () => {
        const didProvider = new Ed25519Provider(seed)
        identity = await Identities.createIdentity({ type, keystore, didProvider })
        signature = await identity.provider.sign(identity, data)
      })

      it('verifies that the signature is valid', async () => {
        const verified = await identity.provider.verify(signature, new Ed25519PublicKey(Buffer.from(identity.publicKey)), data)
        assert.strictEqual(verified, true)
      })

      it('doesn\'t verify invalid signature', async () => {
        const verified = await identity.provider.verify(new Uint8Array([1, 1, 1]), new Ed25519PublicKey(Buffer.from(identity.publicKey)), data)
        assert.strictEqual(verified, false)
      })
    })
  })
})
