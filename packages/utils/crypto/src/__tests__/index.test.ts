import { Ed25519Keypair, X25519Keypair, verifySignatureSecp256k1, verifySignatureEd25519, Secp256k1PublicKey } from '../index.js'
import sodium from 'libsodium-wrappers';
import { deserialize, serialize } from '@dao-xyz/borsh';
import { Wallet } from '@ethersproject/wallet';
import { Session } from '@dao-xyz/orbit-db-test-utils';
import { IPFSAddress } from '../ipfs.js';

describe('Ed25519', () => {

    it('ser/der', async () => {

        const keypair = await Ed25519Keypair.create();
        const derser = deserialize(serialize(keypair), Ed25519Keypair);
        expect(derser.publicKey.publicKey).toEqual(keypair.publicKey.publicKey);
    })

    /*   it('size', async () => {
          const kp = await Ed25519Keypair.create();
          expect(serialize(kp.publicKey)).toHaveLength(PUBLIC_KEY_WIDTH);
      }) */

    it('verify native', async () => {
        await sodium.ready
        const keypair = sodium.crypto_sign_keypair();
        const data = new Uint8Array([1, 2, 3])
        const signature = sodium.crypto_sign_detached(data, keypair.privateKey);
        const isVerified = await verifySignatureEd25519(signature, keypair.publicKey, data);
        expect(isVerified).toBeTrue()
    })


    it('verify primitve', async () => {
        await sodium.ready
        const keypair = await Ed25519Keypair.create();
        const data = new Uint8Array([1, 2, 3])
        const signature = await keypair.sign(data);
        const isVerified = await verifySignatureEd25519(signature, keypair.publicKey, data);
        expect(isVerified).toBeTrue()
    })


})



describe('X25519', () => {

    it('ser/der', async () => {

        const keypair = await X25519Keypair.create();
        const derser = deserialize(serialize(keypair), X25519Keypair);
        expect(derser.publicKey.publicKey).toEqual(keypair.publicKey.publicKey);
    })

    /*  it('size', async () => {
         const kp = await X25519Keypair.create();
         expect(serialize(kp.publicKey)).toHaveLength(PUBLIC_KEY_WIDTH);
     }) */
})

describe('Sepck2561k1', () => {
    it('verify', async () => {
        const wallet = await Wallet.createRandom()
        const data = new Uint8Array([1, 2, 3])
        const pk = new Secp256k1PublicKey({
            address: await wallet.getAddress()
        });
        const signature = await wallet.signMessage(data);
        const isVerified = await verifySignatureSecp256k1(new Uint8Array(Buffer.from(signature)), pk, data);
        expect(isVerified).toBeTrue()
    })

    it('ser/der', async () => {
        const wallet = await Wallet.createRandom()
        const pk = new Secp256k1PublicKey({
            address: await wallet.getAddress()
        });
        const derser = deserialize(serialize(pk), Secp256k1PublicKey);
        expect(derser.address).toEqual(pk.address);
    })


    /*  it('size', async () => {
         expect(serialize(new Secp256k1PublicKey({
             address: await Wallet.createRandom().getAddress()
         })
         )).toHaveLength(PUBLIC_KEY_WIDTH);
     }) */
})

describe('IPFS', () => {
    let session: Session
    beforeAll(async () => {
        session = await Session.connected(1);
    })

    afterAll(async () => {
        await session.stop();
    })

    it('ser/der', async () => {
        const pk = new IPFSAddress({
            address: session.peers[0].id.toString()
        });
        const derser = deserialize(serialize(pk), IPFSAddress);
        expect(derser.address).toEqual(pk.address);
    })

    /*  it('size', async () => {
         const pk = new IPFSAddress({
             address: session.peers[0].id.toString()
         });
         expect(serialize(pk)).toHaveLength(PUBLIC_KEY_WIDTH);
     }) */
})