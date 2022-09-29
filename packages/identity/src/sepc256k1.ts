import { field, variant } from "@dao-xyz/borsh";
import { arraysEqual, U8IntArraySerializer } from "@dao-xyz/io-utils";
import { PublicKey } from "./key";
import { verifyMessage } from '@ethersproject/wallet'
import { SodiumPlus } from 'sodium-plus';

const _crypto = SodiumPlus.auto();

@variant(1)
export class Secp256k1PublicKeyData extends PublicKey {

    @field({ type: 'string' })
    address: string;

    constructor(properties?: { address: string }) {
        super();
        if (properties) {
            this.address = properties.address;
        }
    }

    equals(other: PublicKey): boolean {
        if (other instanceof Secp256k1PublicKeyData) {
            return this.address === other.address;
        }
        return false;
    }
    toString(): string {
        return "secpt256k1/" + this.address
    }
}


export const verifySignatureSecp256k1 = async (signature: Uint8Array, publicKey: Secp256k1PublicKeyData, data: Uint8Array, signedHash = false): Promise<boolean> => {
    const crypto = await _crypto;
    let signedData = signedHash ? await crypto.crypto_generichash(Buffer.from(data)) : data;
    const signerAddress = verifyMessage(signedData, Buffer.from(signature).toString());
    return (signerAddress === publicKey.address)
}