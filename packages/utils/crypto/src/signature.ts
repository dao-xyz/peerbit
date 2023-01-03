import {
    deserialize,
    variant,
    field,
    option,
    serialize,
    AbstractType,
} from "@dao-xyz/borsh";
import { arraysEqual } from "@dao-xyz/peerbit-borsh-utils";
import { Ed25519PublicKey, verifySignatureEd25519 } from "./ed25519.js";
import { PublicSignKey } from "./key.js";
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

export const verify = (
    signature: Uint8Array,
    publicKey: PublicSignKey,
    data: Uint8Array
) => {
    if (!signature) {
        return true;
    }
    if (publicKey instanceof Ed25519PublicKey) {
        return verifySignatureEd25519(signature, publicKey, data);
    } else if (publicKey instanceof Secp256k1PublicKey) {
        return verifySignatureSecp256k1(signature, publicKey, data);
    }
    return false;
};

/* 

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
export abstract class MaybeSigned<T> {

    @field({ type: Uint8Array })
    data: Uint8Array;

    constructor(props: {
        data: Uint8Array;
        value?: T;
    }) {
        this.data = props.data;
        this._value = props.value;
    }

    _value?: T;

    getValue(constructor: AbstractType<T>): T {
        return deserialize(this.data, constructor);
    }

    abstract verify(): Promise<boolean>

    equals(other: MaybeSigned<T>): boolean {
        if (!arraysEqual(this.data, other.data)) {
            return false;
        }
        return true;
    }

}

@variant(0)
export class Signed<T> extends MaybeSigned<T> {

    @field({ type: option(SignatureWithKey) })
    signature: SignatureWithKey;

    constructor(props: {
        data: Uint8Array;
        value?: T;
        signature: SignatureWithKey;
    }) {
        super(props)
        this.signature = props.signature;

    }

    async verify(): Promise<boolean> {
        return verify(
            this.signature.signature,
            this.signature.publicKey,
            this.data
        );
    }

    equals(other: MaybeSigned<T>): boolean {
        if (other instanceof Signed<T>) {
            if (!this.signature !== !other.signature) {
                return false;
            }
            if (this.signature && other.signature) {
                return this.signature.equals(other.signature);
            }
            return super.equals(other)
        }
        else {
            return false;
        }
    }


    async sign(signer: SignWithKey): Promise<MaybeSigned<T>> {
        const signatureResult = await signer(this.data);
        this.signature = new SignatureWithKey({
            publicKey: signatureResult.publicKey,
            signature: signatureResult.signature,
        });
        return this;
    }
}


@variant(0)
export class Unsigned<T> extends MaybeSigned<T> {


    constructor(props: {
        data: Uint8Array;
        value?: T;
    }) {
        super(props)
    }

    equals(other: MaybeSigned<T>): boolean {
        if (other instanceof Unsigned) {
            return super.equals(other)
        }
        else {
            return false;
        }
    }

   
    async sign(signer: SignWithKey): Promise<Signed<T>> {
        const signatureResult = await signer(this.data);
        return new Signed({
            data: this.data,
            signature: new SignatureWithKey({
                publicKey: signatureResult.publicKey,
                signature: signatureResult.signature,
            }),
            value: this._value
        });
    }

    async verify(): Promise<boolean> {
        return true;
    }
}


export const verify = (
    signature: Uint8Array,
    publicKey: PublicSignKey,
    data: Uint8Array
) => {
    if (!signature) {
        return true;
    }
    if (publicKey instanceof Ed25519PublicKey) {
        return verifySignatureEd25519(signature, publicKey, data);
    } else if (publicKey instanceof Secp256k1PublicKey) {
        return verifySignatureSecp256k1(signature, publicKey, data);
    }
    return false;
};

*/
