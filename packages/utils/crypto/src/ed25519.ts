import { field, variant } from "@dao-xyz/borsh";
import { PrivateSignKey, PublicSignKey, Keypair } from "./key.js";
import { arraysCompare, fixedUint8Array } from "@dao-xyz/peerbit-borsh-utils";
import sodium from "libsodium-wrappers";
import { Signer, SignWithKey } from "./signer.js";
import { SignatureWithKey } from "./signature.js";
import { toHexString } from "./utils.js";

@variant(0)
export class Ed25519PublicKey extends PublicSignKey {
    @field({ type: fixedUint8Array(32) })
    publicKey: Uint8Array;

    constructor(properties?: { publicKey: Uint8Array }) {
        super();
        if (properties) {
            this.publicKey = properties.publicKey;
        }
    }

    equals(other: PublicSignKey): boolean {
        if (other instanceof Ed25519PublicKey) {
            return arraysCompare(this.publicKey, other.publicKey) === 0;
        }
        return false;
    }
    toString(): string {
        return "ed25119p/" + toHexString(this.publicKey);
    }
}

@variant(0)
export class Ed25519PrivateKey extends PrivateSignKey {
    @field({ type: fixedUint8Array(64) })
    privateKey: Uint8Array;

    constructor(properties?: { privateKey: Uint8Array }) {
        super();
        if (properties) {
            this.privateKey = properties.privateKey;
        }
    }

    equals(other: PublicSignKey): boolean {
        if (other instanceof Ed25519PrivateKey) {
            return arraysCompare(this.privateKey, other.privateKey) === 0;
        }
        return false;
    }
    toString(): string {
        return "ed25119s/" + toHexString(this.privateKey);
    }
}

@variant(0)
export class Ed25519Keypair extends Keypair implements Signer {
    @field({ type: Ed25519PublicKey })
    publicKey: Ed25519PublicKey;

    @field({ type: Ed25519PrivateKey })
    privateKey: Ed25519PrivateKey;

    constructor(properties?: {
        publicKey: Ed25519PublicKey;
        privateKey: Ed25519PrivateKey;
    }) {
        super();
        if (properties) {
            this.privateKey = properties.privateKey;
            this.publicKey = properties.publicKey;
        }
    }

    static async create(): Promise<Ed25519Keypair> {
        await sodium.ready;
        const generated = sodium.crypto_sign_keypair();
        const kp = new Ed25519Keypair();
        kp.publicKey = new Ed25519PublicKey({
            publicKey: generated.publicKey,
        });
        kp.privateKey = new Ed25519PrivateKey({
            privateKey: generated.privateKey,
        });
        return kp;
    }
    async sign(data: Uint8Array): Promise<Uint8Array> {
        return sign(data, this.privateKey);
    }

    signer(): SignWithKey {
        return async (data: Uint8Array) => {
            return new SignatureWithKey({
                publicKey: this.publicKey,
                signature: await this.sign(data),
            });
        };
    }

    equals(other: Keypair) {
        if (other instanceof Ed25519Keypair) {
            return (
                this.publicKey.equals(other.publicKey) &&
                this.privateKey.equals(other.privateKey)
            );
        }
        return false;
    }
}

const sign = async (
    data: Uint8Array,
    privateKey: Ed25519PrivateKey,
    signedHash = false
) => {
    await sodium.ready;
    const signedData = signedHash
        ? await sodium.crypto_generichash(32, data)
        : data;
    const signature = await sodium.crypto_sign_detached(
        signedData,
        privateKey.privateKey
    );
    return signature;
};

export const verifySignatureEd25519 = async (
    signature: Uint8Array,
    publicKey: Ed25519PublicKey | Uint8Array,
    data: Uint8Array,
    signedHash = false
) => {
    await sodium.ready;
    let res = false;
    try {
        const signedData = signedHash
            ? await sodium.crypto_generichash(32, data)
            : data;
        const verified = await sodium.crypto_sign_verify_detached(
            signature,
            signedData,
            publicKey instanceof Ed25519PublicKey
                ? publicKey.publicKey
                : publicKey
        );
        res = verified;
    } catch (error) {
        return false;
    }
    return res;
};
