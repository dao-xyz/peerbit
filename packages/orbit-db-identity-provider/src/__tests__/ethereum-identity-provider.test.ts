import { EthIdentityProvider } from "../ethereum-identity-provider"
import { Identities } from "../identities"
import { Identity, Signatures } from "../identity"

const assert = require('assert')
const path = require('path')
const rmrf = require('rimraf')
const Keystore = require('orbit-db-keystore')
const keypath = path.resolve(__dirname, 'keys')

let keystore

const type = EthIdentityProvider.type
describe('Ethereum Identity Provider', function () {
  beforeAll(async () => {
    rmrf.sync(keypath)
    Identities.addIdentityProvider(EthIdentityProvider)
    keystore = new Keystore(keypath)
  })

  afterAll(async () => {
    await keystore.close()
    rmrf.sync(keypath)
  })

  describe('create an ethereum identity', () => {
    let identity
    let wallet

    beforeAll(async () => {
      const ethIdentityProvider = new EthIdentityProvider()
      wallet = await ethIdentityProvider._createWallet()
      identity = await Identities.createIdentity({ type, keystore, wallet })
    })

    test('has the correct id', async () => {
      assert.strictEqual(identity.id, wallet.address)
    })

    test('created a key for id in keystore', async () => {
      const key = await keystore.getKey(wallet.address)
      assert.notStrictEqual(key, undefined)
    })

    test('has the correct public key', async () => {
      const signingKey = await keystore.getKey(wallet.address)
      assert.notStrictEqual(signingKey, undefined)
      assert.strictEqual(identity.publicKey, keystore.getPublic(signingKey))
    })

    test('has a signature for the id', async () => {
      const signingKey = await keystore.getKey(wallet.address)
      const idSignature = await keystore.sign(signingKey, wallet.address)
      const verifies = await Keystore.verify(idSignature, Buffer.from(signingKey.public.marshal()).toString('hex'), wallet.address)
      assert.strictEqual(verifies, true)
      assert.strictEqual(identity.signatures.id, idSignature)
    })

    test('has a signature for the publicKey', async () => {
      const signingKey = await keystore.getKey(wallet.address)
      const idSignature = await keystore.sign(signingKey, wallet.address)
      const publicKeyAndIdSignature = await wallet.signMessage(identity.publicKey + idSignature)
      assert.strictEqual(identity.signatures.publicKey, publicKeyAndIdSignature)
    })
  })

  describe('verify identity', () => {
    let identity

    beforeAll(async () => {
      identity = await Identities.createIdentity({ keystore, type })
    })

    test('ethereum identity verifies', async () => {
      const verified = await Identities.verifyIdentity(identity)
      assert.strictEqual(verified, true)
    })

    test('ethereum identity with incorrect id does not verify', async () => {
      const identity2 = new Identity({
        ...identity.toSerializable(),
        provider: identity.provider,
        id: 'NotAnId',
      })
      const verified = await Identities.verifyIdentity(identity2)
      assert.strictEqual(verified, false)
    })
  })

  describe('sign data with an identity', () => {
    let identity
    const data = 'hello friend'

    beforeAll(async () => {
      identity = await Identities.createIdentity({ keystore, type })
    })

    test('sign data', async () => {
      const signingKey = await keystore.getKey(identity.id)
      const expectedSignature = await keystore.sign(signingKey, data)
      const signature = await identity.provider.sign(identity, data, keystore)
      assert.strictEqual(signature, expectedSignature)
    })

    test('throws an error if private key is not found from keystore', async () => {
      // Remove the key from the keystore (we're using a mock storage in these tests)
      const modifiedIdentity = new Identity({
        id: 'this id does not exist', publicKey: identity.publicKey, signatures: new Signatures({ id: '<sig>', publicKey: identity.signatures }), type: identity.type, provider: identity.provider
      })
      let signature
      let err
      try {
        signature = await identity.provider.sign(modifiedIdentity, data, keystore)
      } catch (e) {
        err = e.toString()
      }
      assert.strictEqual(signature, undefined)
      assert.strictEqual(err, 'Error: Private signing key not found from Keystore')
    })

    describe('verify data signed by an identity', () => {
      const data = 'hello friend'
      let identity
      let signature

      beforeAll(async () => {
        identity = await Identities.createIdentity({ type, keystore })
        signature = await identity.provider.sign(identity, data, keystore)
      })

      test('verifies that the signature is valid', async () => {
        const verified = await identity.provider.verify(signature, identity.publicKey, data)
        assert.strictEqual(verified, true)
      })

      test('doesn\'t verify invalid signature', async () => {
        const verified = await identity.provider.verify('invalid', identity.publicKey, data)
        assert.strictEqual(verified, false)
      })
    })
  })
})
