import { field, variant } from "@dao-xyz/borsh";
import { PrivateSignKey, PublicSignKey, SignKey, Keypair } from './key.js';
import { arraysCompare } from '@dao-xyz/borsh-utils';
import sodium from 'libsodium-wrappers';
import { U8IntArraySerializer } from '@dao-xyz/borsh-utils';
import { Signer } from "./signer.js";
import { K } from "./encryption.js";

const NONCE_LENGTH = 24;



@variant(0)
export class Ed25519PublicKey extends PublicSignKey {

    @field(U8IntArraySerializer)
    publicKey: Uint8Array;

    constructor(properties?: { publicKey: Uint8Array }) {
        super();
        if (properties) {
            this.publicKey = properties.publicKey;
        }
    }

    equals(other: SignKey): boolean {
        if (other instanceof Ed25519PublicKey) {
            return arraysCompare(this.publicKey, other.publicKey) === 0
        }
        return false;
    }
    toString(): string {
        return "ed25119/" + Buffer.from(this.publicKey).toString('hex');
    }

}

@variant(0)
export class Ed25519PrivateKey extends PrivateSignKey {

    @field(U8IntArraySerializer)
    privateKey: Uint8Array;

    constructor(properties?: { privateKey: Uint8Array }) {
        super();
        if (properties) {
            this.privateKey = properties.privateKey;
        }
    }

    equals(other: SignKey): boolean {
        if (other instanceof Ed25519PrivateKey) {
            return arraysCompare(this.privateKey, other.privateKey) === 0
        }
        return false;
    }
    toString(): string {
        return "ed25119/" + Buffer.from(this.privateKey).toString('hex');
    }
}


@variant(0)
export class Ed25519Keypair extends Keypair implements Signer {

    publicKey: Ed25519PublicKey;
    privateKey: Ed25519PrivateKey;

    static async create(): Promise<Ed25519Keypair> {

        await sodium.ready;
        const generated = sodium.crypto_sign_keypair();
        const kp = new Ed25519Keypair();
        kp.publicKey = new Ed25519PublicKey({
            publicKey: generated.publicKey
        });
        kp.privateKey = new Ed25519PrivateKey({
            privateKey: generated.privateKey
        });
        return kp;
    }
    async sign(data) {
        const signature = await sign(data, this.privateKey)
        return {
            signature,
            publicKey: this.publicKey
        }
    }
}

const sign = async (data: Uint8Array, privateKey: Ed25519PrivateKey, signedHash = false) => {
    await sodium.ready;
    let signedData = signedHash ? await sodium.crypto_generichash(32, data) : data;
    const signature = await sodium.crypto_sign_detached(signedData, privateKey.privateKey);
    return signature;
}


export const verifySignatureEd25519 = async (signature: Uint8Array, publicKey: Ed25519PublicKey | Uint8Array, data: Uint8Array, signedHash = false) => {
    await sodium.ready;
    let res = false
    try {
        let signedData = signedHash ? await sodium.crypto_generichash(32, data) : data;
        const verified = await sodium.crypto_sign_verify_detached(signature, signedData, publicKey instanceof Ed25519PublicKey ? publicKey.publicKey : publicKey);
        res = verified
    } catch (error) {
        return false;
    }
    return res
}