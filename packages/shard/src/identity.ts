import IdentityProvider from "orbit-db-identity-provider/src/identity-provider-interface";

import { Keypair, PublicKey as SPublicKey } from '@solana/web3.js';
import { Identity } from "@dao-xyz/orbit-db-identity-provider";
import nacl from "tweetnacl";
import bs58 from 'bs58';



export class SolanaIdentityProvider extends IdentityProvider {

    wallet: SPublicKey;
    keypair: Keypair | undefined;
    signer: ((data: Uint8Array) => any) | undefined
    constructor(options: { wallet: SPublicKey, keypair?: Keypair, signer?: (data: Uint8Array) => any }) {
        super()
        this.wallet = options.wallet;
        this.keypair = options.keypair;
        this.signer = options.signer;
    }

    // Returns the type of the identity provider
    static get type(): string {
        return 'solana'
    }

    // Returns the signer's id
    async getId(): Promise<string> {
        return this.wallet.toBase58();
    }

    // Returns a signature of pubkeysignature
    async signIdentity(data: string) {
        if (!this.signer && !this.keypair) {
            throw new Error("This identity to not support signing")
        }

        let array = Uint8Array.from(Buffer.from(data));
        return this.signer ? this.signer(array) : nacl.sign(array, this.keypair.secretKey)
    }

    static async verifyIdentity(identity: Identity) {

        // Verify that identity was signed by the id
        /* const signedKey = nacl.sign.open(Uint8Array.from(Buffer.from(identity.signatures.publicKey)), bs58.decode(identity.id));
        let verified = nacl.verify(signedKey, Uint8Array.from(Buffer.from(identity.publicKey + identity.signatures.id)));
        return verified */
        return false;
    }
}
