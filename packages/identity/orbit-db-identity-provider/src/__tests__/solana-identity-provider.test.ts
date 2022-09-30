import { Identity, Signatures } from "../identity"
import { PublicKey as SPublicKey, Keypair } from '@solana/web3.js';
import assert from 'assert'
const path = require('path')
import rmrf from 'rimraf'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { Identities } from "../identities"
const keypath = path.resolve(__dirname, 'keys')
import nacl from "tweetnacl";
import { Ed25519PublicKey } from 'sodium-plus';
import { SolanaIdentityProvider } from "../solana-identity-provider";
import { joinUint8Arrays } from "@dao-xyz/borsh-utils";
let keystore: Keystore

/** Tests have to run in order */
const type = SolanaIdentityProvider.type
describe('Solana Identity Provider', function () {
    beforeAll(async () => {
        rmrf.sync(keypath)
        Identities.addIdentityProvider(SolanaIdentityProvider)
        keystore = new Keystore(keypath)
    })

    afterAll(async () => {
        await keystore.close()
        rmrf.sync(keypath)
    })

    describe('create an solana identity', () => {
        let identity: Identity
        let keypair: Keypair

        beforeAll(async () => {
            keypair = Keypair.generate();
            identity = await Identities.createIdentity({ type, keystore, keypair: keypair })
        })

        it('has the correct id', async () => {
            assert.deepStrictEqual(identity.id, keypair.publicKey.toBytes())
        })

        it('created a key for id in keystore', async () => {
            const key = await keystore.getKeyByPath(keypair.publicKey.toBuffer().toString('base64'))
            assert(!!key)
        })

        it('has the correct public key', async () => {
            const signingKey = await keystore.getKeyByPath<SignKeyWithMeta>(keypair.publicKey.toBuffer().toString('base64'))
            assert.notStrictEqual(signingKey, undefined)
            assert.deepStrictEqual(identity.publicKey, signingKey.publicKey)
        })

        it('has a signature for the id', async () => {
            const signingKey = await keystore.getKeyByPath<SignKeyWithMeta>(keypair.publicKey.toBuffer().toString('base64'))
            const idSignature = await Keystore.sign(keypair.publicKey.toBytes(), signingKey)
            const verifies = await Keystore.verify(idSignature, signingKey.publicKey, keypair.publicKey.toBytes())
            expect(verifies).toEqual(true)
            assert.deepStrictEqual(identity.signatures.id, idSignature)
        })

        it('has a signature for the publicKey', async () => {
            const signingKey = await keystore.getKeyByPath<SignKeyWithMeta>(keypair.publicKey.toBuffer().toString('base64'))
            const idSignature = await Keystore.sign(keypair.publicKey.toBytes(), signingKey)
            const publicKeyAndIdSignature = await nacl.sign(Buffer.concat([identity.publicKey.getBuffer(), idSignature]), keypair.secretKey)
            assert.deepStrictEqual(identity.signatures.publicKey, new Uint8Array(Buffer.from(publicKeyAndIdSignature)))
        })
    })

    describe('verify identity', () => {
        let identity: Identity

        beforeAll(async () => {
            identity = await Identities.createIdentity({ keystore, type })
        })

        it('solana identity verifies', async () => {
            const verified = await Identities.verifyIdentity(identity)
            expect(verified).toEqual(true)
        })

        it('solana identity with incorrect id does not verify', async () => {
            const identity2 = new Identity({
                ...identity.toSerializable(),
                provider: identity.provider,
                id: new Uint8Array([1, 1, 1]),
            })
            const verified = await Identities.verifyIdentity(identity2)
            expect(verified).toEqual(false)
        })
    })

    describe('sign data with an identity', () => {
        let identity: Identity
        const data = new Uint8Array(Buffer.from('hello friend'))

        beforeAll(async () => {
            identity = await Identities.createIdentity({ keystore, type })
        })

        it('sign data', async () => {
            const signingKey = await keystore.getKeyByPath<SignKeyWithMeta>(identity.id)
            const expectedSignature = await Keystore.sign(Buffer.from(data), signingKey)
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
            expect(signature).toEqual(undefined)
            expect(err).toEqual('Error: Private signing key not found from Keystore')
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
                const verified = await identity.provider.verify(signature, identity.publicKey, data)
                expect(verified).toEqual(true)
            })

            it('doesn\'t verify invalid signature', async () => {
                const verified = await identity.provider.verify(new Uint8Array(Buffer.from('invalid')), identity.publicKey, data)
                expect(verified).toEqual(false)
            })
        })
    })
})
