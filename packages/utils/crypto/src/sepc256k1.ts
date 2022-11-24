import { field, variant } from "@dao-xyz/borsh";
import { PublicSignKey } from "./key.js";
import { verifyMessage } from "@ethersproject/wallet";
import sodium from "libsodium-wrappers";
import { arraysEqual, fixedUint8Array } from "@dao-xyz/peerbit-borsh-utils";
import { fromHexString } from "./utils.js";

@variant(1)
export class Secp256k1PublicKey extends PublicSignKey {
    @field({ type: fixedUint8Array(20) })
    address: Uint8Array; // this is really an ethereum variant of the publickey, that is calculated by hashing the publickey

    constructor(properties?: { address: string }) {
        super();
        if (properties) {
            // remove 0x and decode
            this.address = fromHexString(properties.address.slice(2));
        }
    }

    equals(other: PublicSignKey): boolean {
        if (other instanceof Secp256k1PublicKey) {
            return this.address === other.address;
        }
        return false;
    }
    toString(): string {
        return "sepc256k1/" + new TextDecoder().decode(this.address);
    }
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const verifySignatureSecp256k1 = async (
    signature: Uint8Array,
    publicKey: Secp256k1PublicKey,
    data: Uint8Array,
    signedHash = false
): Promise<boolean> => {
    await sodium.ready;
    const signedData = signedHash
        ? await sodium.crypto_generichash(32, data)
        : data;
    const signerAddress = verifyMessage(signedData, decoder.decode(signature));
    return arraysEqual(
        fromHexString(signerAddress.slice(2)),
        publicKey.address
    );
};
