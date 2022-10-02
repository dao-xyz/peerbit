import { Keypair, PublicKey as SPublicKey } from '@solana/web3.js';
import nacl from "tweetnacl";
import { IdentityProvider } from './identity-provider-interface';
import { Ed25519PublicKey, verifySignatureEd25519 } from '@dao-xyz/identity';
import { Identity } from './identity';

type Signer = (data: Uint8Array) => Uint8Array;
export type SolanaIdentityProviderOptions = { signer?: Signer, keypair?: Keypair, publicKey?: SPublicKey };
export class SolanaIdentityProvider extends IdentityProvider {

    signer: (data: Uint8Array) => Promise<Uint8Array> | Uint8Array
    publicKey: SPublicKey
    constructor(options: SolanaIdentityProviderOptions) {
        super()
        this.signer = options.signer;
        let keypair = options.keypair;
        if (!this.signer) {
            keypair = options.keypair ? options.keypair : Keypair.generate();
            this.signer = (data: Uint8Array) => nacl.sign(data, keypair.secretKey)
        }
        if (!options.publicKey && !keypair) {
            throw new Error("PublicKey or Keypair has to be provided")
        }
        this.publicKey = options.publicKey ? options.publicKey : keypair.publicKey
    }

    // Returns the type of the identity provider
    static get type(): string {
        return 'solana'
    }

    // Returns the signer's id
    async getId(): Promise<Uint8Array> {
        return this.publicKey.toBytes();
    }


    // Returns a signature of pubkeysignature
    async sign(data: Uint8Array, options = {}) {
        if (!this.signer) { throw new Error('Signing function is required') }
        return await this.signer(data);
    }

    /*  static async verify(signature: Uint8Array, data: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
         const signedData = nacl.sign.open(signature, publicKey);
         let verified = nacl.verify(signedData, data);
         return verified
     } */

    static async verifyIdentity(identity: Identity) {

        // Verify that identity was signed by the id
        /* const signedKey = nacl.sign.open(Uint8Array.from(Buffer.from(identity.signatures.publicKey)), bs58.decode(identity.id));
        let verified = nacl.verify(signedKey, Uint8Array.from(Buffer.from(identity.publicKey + identity.signatures.id)));
        return verified */
        if (identity.id instanceof Ed25519PublicKey) {
            return verifySignatureEd25519(identity.signatures.publicKey, identity.id, Buffer.concat([identity.publicKey.getBuffer(), identity.signatures.id]));
        }
        return false;
    }
}
