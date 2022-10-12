import { field, fixedArray, variant, vec } from "@dao-xyz/borsh";
import { PublicSignKey, SignKey } from './key.js';
import { verifyMessage } from '@ethersproject/wallet'
import sodium from 'libsodium-wrappers';
import { fixedUint8Array } from "@dao-xyz/borsh-utils";


@variant(2)
export class Secp256k1PublicKey extends PublicSignKey {

    @field({ type: fixedUint8Array(20) })
    address: Uint8Array; // this is really an ethereum variant of the publickey, that is calculated by hashing the publickey


    @field({ type: fixedUint8Array(44) })
    padding: Uint8Array = new Uint8Array((new Array(44)).fill(0))  // we do padding because we want all publicsignkeys to have same size (64 bytes) excluding descriminators. This allows us to efficiently index keys and use byte search to find them we predetermined offsets

    constructor(properties?: { address: string }) {
        super();
        if (properties) {
            // remove 0x and decode
            this.address = new Uint8Array(Buffer.from(properties.address.slice(2), 'hex'));
        }
    }

    equals(other: SignKey): boolean {
        if (other instanceof Secp256k1PublicKey) {
            return this.address === other.address;
        }
        return false;
    }
    toString(): string {
        return "secpt256k1/" + this.address
    }
}


export const verifySignatureSecp256k1 = async (signature: Uint8Array, publicKey: Secp256k1PublicKey, data: Uint8Array, signedHash = false): Promise<boolean> => {
    await sodium.ready;
    let signedData = signedHash ? await sodium.crypto_generichash(32, Buffer.from(data)) : data;
    const signerAddress = verifyMessage(signedData, Buffer.from(signature).toString());
    return (Buffer.compare(Buffer.from(signerAddress.slice(2), 'hex'), Buffer.from(publicKey.address)) === 0)
}