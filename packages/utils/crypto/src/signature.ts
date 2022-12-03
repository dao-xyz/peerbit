import {
    deserialize,
    variant,
    field,
    option,
    serialize,
    AbstractType,
} from "@dao-xyz/borsh";
import { arraysEqual } from "@dao-xyz/peerbit-borsh-utils";
import { Ed25519PublicKey, verifySignatureEd25519 } from "./ed25519";
import { PublicSignKey } from "./key";
import { Secp256k1PublicKey, verifySignatureSecp256k1 } from "./sepc256k1.js";
import { SignWithKey } from "./signer.js";

@variant(0)
export class SignatureWithKey {
    @field({ type: Uint8Array })
    signature: Uint8Array;

    @field({ type: PublicSignKey })
    publicKey: PublicSignKey;

    constructor(props?: { signature: Uint8Array; publicKey: PublicSignKey }) {
        if (props) {
            this.signature = props.signature;
            this.publicKey = props.publicKey;
        }
    }

    equals(other: SignatureWithKey): boolean {
        if (!arraysEqual(this.signature, other.signature)) {
            return false;
        }
        return (
            Buffer.compare(
                serialize(this.publicKey),
                serialize(other.publicKey)
            ) === 0
        );
    }
}

@variant(0)
export class MaybeSigned<T> {
    @field({ type: Uint8Array })
    data: Uint8Array;

    @field({ type: option(SignatureWithKey) })
    signature?: SignatureWithKey;

    constructor(props?: {
        data: Uint8Array;
        value?: T;
        signature?: SignatureWithKey;
    }) {
        if (props) {
            this.data = props.data;
            this.signature = props.signature;
            this._value = props.value;
        }
    }

    _value?: T;

    getValue(constructor: AbstractType<T>): T {
        return deserialize(this.data, constructor);
    }

    async verify(): Promise<boolean> {
        if (!this.signature) {
            return true;
        }
        return verify(
            this.signature.signature,
            this.signature.publicKey,
            this.data
        );
    }

    equals(other: MaybeSigned<T>): boolean {
        if (!arraysEqual(this.data, other.data)) {
            return false;
        }
        if (!this.signature !== !other.signature) {
            return false;
        }
        if (this.signature && other.signature) {
            return this.signature.equals(other.signature);
        }
        return true;
    }

    /**
     * In place
     * @param signer
     */
    async sign(signer: SignWithKey): Promise<MaybeSigned<T>> {
        const signatureResult = await signer(this.data);
        this.signature = new SignatureWithKey({
            publicKey: signatureResult.publicKey,
            signature: signatureResult.signature,
        });
        return this;
    }
}

export const verify = async (
    signature: Uint8Array,
    publicKey: PublicSignKey,
    data: Uint8Array
) => {
    if (!signature) {
        return true;
    }
    if (publicKey instanceof Ed25519PublicKey) {
        return await verifySignatureEd25519(signature, publicKey, data);
    } else if (publicKey instanceof Secp256k1PublicKey) {
        return await verifySignatureSecp256k1(signature, publicKey, data);
    }
    return false;
};
