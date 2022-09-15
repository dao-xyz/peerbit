import { deserialize, Constructor, variant, field, option, serialize } from "@dao-xyz/borsh";
import { arraysEqual, U8IntArraySerializer } from "@dao-xyz/io-utils";
import { PublicKey, } from ".";
import { Ed25519PublicKeyData, verifySignatureEd25519 } from './ed25519';
import { Secp256k1PublicKeyData, verifySignatureSecp256k1 } from './sepc256k1';

@variant(0)
export class SignatureWithKey {

    @field(U8IntArraySerializer)
    signature: Uint8Array

    @field({ type: PublicKey })
    publicKey: PublicKey

    constructor(props?: {
        signature: Uint8Array,
        publicKey: PublicKey
    }) {
        if (props) {
            this.signature = props.signature;
            this.publicKey = props.publicKey
        }
    }

    equals(other: SignatureWithKey): boolean {
        if (!arraysEqual(this.signature, other.signature)) {
            return false;
        }
        return Buffer.compare(serialize(this.publicKey), serialize(other.publicKey)) === 0;
    }
}


@variant(0)
export class MaybeSigned<T>  {

    @field(U8IntArraySerializer)
    data: Uint8Array

    @field({ type: option(SignatureWithKey) })
    signature?: SignatureWithKey

    constructor(props?: {
        data?: Uint8Array,
        value?: T,
        signature?: SignatureWithKey
    }) {
        if (props) {
            this.data = props.data;
            this.signature = props.signature;
            this._value = props.value;
        }
    }

    _value: T

    getValue(constructor: Constructor<T>): T {
        return deserialize(this.data, constructor)
    }

    async verify(): Promise<boolean> {
        if (!this.signature) {
            return true;
        }
        return verify(this.signature.signature, this.signature.publicKey, this.data);
    }


    equals(other: MaybeSigned<T>): boolean {
        if (!arraysEqual(this.data, other.data)) {
            return false;
        }
        if (!this.signature !== !other.signature) {
            return false;
        }
        if (this.signature && other.signature) {
            return this.signature.equals(other.signature)
        }
        return true;
    }


    /**
     * In place
     * @param signer 
     */
    async sign(signer: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicKey }>): Promise<MaybeSigned<T>> {
        const signatureResult = await signer(this.data)
        this.signature = new SignatureWithKey({
            publicKey: signatureResult.publicKey,
            signature: signatureResult.signature
        })
        return this;
    }

}


export const verify = async (signature: Uint8Array, publicKey: PublicKey, data: Uint8Array) => {
    if (!signature) {
        return true;
    }
    if (publicKey instanceof Ed25519PublicKeyData) {
        return await verifySignatureEd25519(signature, publicKey, data)
    }
    else if (publicKey instanceof Secp256k1PublicKeyData) {
        return await verifySignatureSecp256k1(signature, publicKey, data)
    }
    return false
}