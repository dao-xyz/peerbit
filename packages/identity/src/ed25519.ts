import { field, variant } from "@dao-xyz/borsh";
import { PublicKey } from "./key";
import { Ed25519PublicKey } from 'sodium-plus';
import { SodiumPlus } from 'sodium-plus';
import { bufferSerializer } from '@dao-xyz/io-utils';


const NONCE_LENGTH = 24;

const _crypto = SodiumPlus.auto();

@variant(0)
export class Ed25519PublicKeyData extends PublicKey {

    @field({ type: bufferSerializer(Ed25519PublicKey) })
    publicKey: Ed25519PublicKey;

    constructor(properties?: { publicKey: Ed25519PublicKey }) {
        super();
        if (properties) {
            this.publicKey = properties.publicKey;
        }
    }

    equals(other: PublicKey): boolean {
        if (other instanceof Ed25519PublicKeyData) {
            return Buffer.compare(this.publicKey.getBuffer(), other.publicKey.getBuffer()) === 0;
        }
        return false;
    }
    toString(): string {
        return "ed25119/" + this.publicKey.toString('hex');
    }

}


export const verifySignatureEd25519 = async (signature: Uint8Array, publicKey: Ed25519PublicKeyData | Ed25519PublicKey, data: Uint8Array, signedHash = false) => {
    let res = false
    const crypto = await _crypto;
    try {
        const signedData = await crypto.crypto_sign_open(Buffer.from(signature), publicKey instanceof Ed25519PublicKeyData ? publicKey.publicKey : publicKey);
        const verified = Buffer.compare(signedData, signedHash ? await crypto.crypto_generichash(Buffer.from(data)) : Buffer.from(data)) === 0;
        res = verified
    } catch (error) {
        return false;
    }
    return res
}