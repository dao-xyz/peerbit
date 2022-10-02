import { verifySignatureEd25519 } from '../index.js'
import sodium from 'libsodium-wrappers';
describe('Ed25519PublicKey', () => {
    it('verify', async () => {
        await sodium.ready
        const keypair = sodium.crypto_sign_keypair();
        const data = new Uint8Array([1, 2, 3])
        const signature = sodium.crypto_sign_detached(data, keypair.privateKey);
        const isVerified = await verifySignatureEd25519(signature, keypair.publicKey, data);
        expect(isVerified)
    })
})