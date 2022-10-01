import { field, variant } from "@dao-xyz/borsh";
import { SignKey } from './key.js';
import { arraysCompare } from '@dao-xyz/borsh-utils';
import sodium from 'libsodium-wrappers';
import { U8IntArraySerializer } from '@dao-xyz/borsh-utils';

const NONCE_LENGTH = 24;

@variant(0)
export class Ed25519PublicKey extends SignKey {

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