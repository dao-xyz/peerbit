import { EthIdentityProvider } from "../ethereum-identity-provider"
import { Identities } from "../identities"
import { Identity, Signatures } from "../identity"
import { Ed25519PublicKey } from 'sodium-plus';
import { Wallet } from '@ethersproject/wallet'

const assert = require('assert')
const path = require('path')
const rmrf = require('rimraf')
import { Keystore } from '@dao-xyz/orbit-db-keystore'
import { joinUint8Arrays } from "@dao-xyz/io-utils";
const keypath = path.resolve(__dirname, 'keys')

let keystore: Keystore

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
    let identity: Identity
    let wallet: Wallet

    beforeAll(async () => {
      const ethIdentityProvider = new EthIdentityProvider()
      wallet = await ethIdentityProvider._createWallet()
      identity = await Identities.createIdentity({ type, keystore, wallet })
    })

    it('has the correct id', async () => {
      assert.deepStrictEqual(identity.id, new Uint8Array(Buffer.from(wallet.address)))
    })

    it('created a key for id in keystore', async () => {
      const key = await keystore.getKey(Buffer.from(wallet.address).toString('base64'))
      assert(!!key)
    })

    it('has the correct public key', async () => {
      const signingKey = await keystore.getKey(Buffer.from(wallet.address).toString('base64'))
      assert.notStrictEqual(signingKey, undefined)
      assert.deepStrictEqual(identity.publicKey, new Uint8Array((await Keystore.getPublicSign(signingKey.key)).getBuffer()))
    })

    it('has a signature for the id', async () => {
      const signingKey = (await keystore.getKey(Buffer.from(wallet.address).toString('base64'))).key
      const idSignature = await keystore.sign(wallet.address, signingKey)
      const verifies = await Keystore.verify(idSignature, await Keystore.getPublicSign(signingKey), new Uint8Array(Buffer.from(wallet.address)))
      assert.strictEqual(verifies, true)
      assert.deepStrictEqual(identity.signatures.id, idSignature)
    })

    it('has a signature for the publicKey', async () => {
      const signingKey = await keystore.getKey(Buffer.from(wallet.address).toString('base64'))
      const idSignature = await keystore.sign(wallet.address, signingKey.key)
      const publicKeyAndIdSignature = await wallet.signMessage(joinUint8Arrays([identity.publicKey, idSignature]))
      assert.deepStrictEqual(identity.signatures.publicKey, new Uint8Array(Buffer.from(publicKeyAndIdSignature)))
    })
  })

  describe('verify identity', () => {
    let identity: Identity

    beforeAll(async () => {
      identity = await Identities.createIdentity({ keystore, type })
    })

    it('ethereum identity verifies', async () => {
      const verified = await Identities.verifyIdentity(identity)
      assert.strictEqual(verified, true)
    })

    it('ethereum identity with incorrect id does not verify', async () => {
      const identity2 = new Identity({
        ...identity.toSerializable(),
        provider: identity.provider,
        id: new Uint8Array([1, 1, 1]),
      })
      const verified = await Identities.verifyIdentity(identity2)
      assert.strictEqual(verified, false)
    })
  })

  describe('sign data with an identity', () => {
    let identity: Identity
    const data = new Uint8Array(Buffer.from('hello friend'))

    beforeAll(async () => {
      identity = await Identities.createIdentity({ keystore, type })
    })

    it('sign data', async () => {
      const signingKey = await keystore.getKey(identity.id)
      const expectedSignature = await keystore.sign(Buffer.from(data), signingKey.key)
      const signature = await identity.provider.sign(data, identity)
      assert.deepStrictEqual(signature, expectedSignature)
    })

    it('throws an error if private key is not found from keystore', async () => {
      // Remove the key from the keystore (we're using a mock storage in these tests)
      const modifiedIdentity = new Identity({
        id: new Uint8Array([1, 2, 3]), publicKey: identity.publicKey, signatures: new Signatures({ id: new Uint8Array([0]), publicKey: identity.signatures.publicKey }), type: identity.type, provider: identity.provider
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

    describe('verify data signed by an identity', () => {
      const data = new Uint8Array(Buffer.from('hello friend'));
      let identity: Identity
      let signature

      beforeAll(async () => {
        identity = await Identities.createIdentity({ type, keystore })
        signature = await identity.provider.sign(data, identity)
      })

      it('verifies that the signature is valid', async () => {
        const verified = await identity.provider.verify(signature, new Ed25519PublicKey(Buffer.from(identity.publicKey)), data)
        assert.strictEqual(verified, true)
      })

      it('doesn\'t verify invalid signature', async () => {
        const verified = await identity.provider.verify(new Uint8Array(Buffer.from('invalid')), new Ed25519PublicKey(Buffer.from(identity.publicKey)), data)
        assert.strictEqual(verified, false)
      })
    })
  })
})
