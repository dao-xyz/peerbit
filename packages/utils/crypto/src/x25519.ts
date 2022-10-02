export * from './errors.js';
import { Constructor, deserialize, field, serialize, variant, vec } from '@dao-xyz/borsh';
import { arraysCompare, bufferSerializer, U8IntArraySerializer } from '@dao-xyz/borsh-utils';
import { arraysEqual } from '@dao-xyz/borsh-utils'
import { AccessError } from './errors.js';
import sodium from 'libsodium-wrappers';
import { Keypair, PrivateEncryptionKey, PublicKeyEncryptionKey } from './key.js';
import { Ed25519Keypair, Ed25519PublicKey, Ed25519PrivateKey } from './ed25519.js';



@variant(0)
export class X25519PublicKey extends PublicKeyEncryptionKey {

    @field(U8IntArraySerializer)
    publicKey: Uint8Array;

    constructor(properties?: { publicKey: Uint8Array }) {
        super();
        if (properties) {
            this.publicKey = properties.publicKey;
        }
    }

    equals(other: PublicKeyEncryptionKey): boolean {
        if (other instanceof X25519PublicKey) {
            return arraysCompare(this.publicKey, other.publicKey) === 0
        }
        return false;
    }
    toString(): string {
        return "x25519public/" + Buffer.from(this.publicKey).toString('hex');
    }

    static async from(ed25119PublicKey: Ed25519PublicKey): Promise<X25519PublicKey> {
        await sodium.ready;
        return new X25519PublicKey({
            publicKey: sodium.crypto_sign_ed25519_pk_to_curve25519(ed25119PublicKey.publicKey)
        })
    }

    static async create(): Promise<X25519PublicKey> {
        await sodium.ready;
        return new X25519PublicKey({
            publicKey: sodium.crypto_box_keypair().publicKey
        })
    }
}


@variant(0)
export class X25519SecretKey extends PrivateEncryptionKey {

    @field(U8IntArraySerializer)
    secretKey: Uint8Array;

    constructor(properties?: { secretKey: Uint8Array }) {
        super();
        if (properties) {
            this.secretKey = properties.secretKey;
        }
    }

    equals(other: PublicKeyEncryptionKey): boolean {
        if (other instanceof X25519SecretKey) {
            return arraysCompare(this.secretKey, other.secretKey) === 0
        }
        return false;
    }
    toString(): string {
        return "x25519secret" + Buffer.from(this.secretKey).toString('hex');
    }

    async publicKey(): Promise<X25519PublicKey> {
        await sodium.ready;
        return new X25519PublicKey({
            publicKey: sodium.crypto_scalarmult_base(this.secretKey)
        })
    }
    static async from(ed25119SecretKey: Ed25519PrivateKey): Promise<X25519SecretKey> {
        await sodium.ready;
        return new X25519SecretKey({
            secretKey: sodium.crypto_sign_ed25519_sk_to_curve25519(ed25119SecretKey.privateKey)
        })
    }

    static async create(): Promise<X25519SecretKey> {
        await sodium.ready;
        return new X25519SecretKey({
            secretKey: sodium.crypto_box_keypair().privateKey
        })
    }

}


@variant(1)
export class X25519Keypair extends Keypair {

    @field({ type: X25519PublicKey })
    publicKey: X25519PublicKey;

    @field({ type: X25519SecretKey })
    secretKey: X25519SecretKey;

    static async create(): Promise<X25519Keypair> {

        await sodium.ready;
        const generated = sodium.crypto_box_keypair();
        const kp = new X25519Keypair();
        kp.publicKey = new X25519PublicKey({
            publicKey: generated.publicKey
        });
        kp.secretKey = new X25519SecretKey({
            secretKey: generated.privateKey
        });
        return kp;
    }

    static async from(ed25119Keypair: Ed25519Keypair): Promise<X25519Keypair> {
        const pk = await X25519PublicKey.from(ed25119Keypair.publicKey);
        const sk = await X25519SecretKey.from(ed25119Keypair.privateKey);
        const kp = new X25519Keypair()
        kp.publicKey = pk;
        kp.secretKey = sk;
        return kp;
    }
}



/* 

export const verifySignature = async (signature: Uint8Array, publicKey: PublicKey, data: Uint8Array, signedHash = false) => {
    let res = false
    const crypto = await _crypto;
    try {
        const signedData = await crypto.crypto_sign_open(Buffer.from(signature), publicKey);
        const verified = Buffer.compare(signedData, signedHash ? await crypto.crypto_generichash(Buffer.from(data)) : Buffer.from(data)) === 0;
        res = verified
    } catch (error) {
        return false;
    }
    return res
}

*/

/* @variant(0)
export class MaybeSigned<T>  {

    async open(opener: (bytes: Uint8Array, key: Ed25519PublicKey) => Promise<Uint8Array>, constructor: Constructor<T> | Uint8ArrayConstructor): Promise<T> {
        throw new Error("Not implemented")
    }

    equals(other: MaybeEncrypted<T>): boolean {
        throw new Error("Not implemented")
    }


} */


/* @variant(0)
export class UnsignedMessage<T> extends MaybeSigned<T> {

    @field(U8IntArraySerializer)
    data: Uint8Array

    constructor(props?: {
        data: Uint8Array
    }) {
        super();
        if (props) {
            this.data = props.data;
        }
    }

    async sign(signer: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: Ed25519PublicKey }>): Promise<SignedMessage<T>> {
        const signatureResult = await signer(this.data)
        return new SignedMessage({
            key: signatureResult.publicKey,
            signature: signatureResult.signature
        });
    }


    async open(_opener: (bytes: Uint8Array, key: Ed25519PublicKey) => Promise<Uint8Array>, constructor: Constructor<T>): Promise<T> {
        return deserialize(this.data, constructor)
    }
}


@variant(1)
export class SignedMessage<T> extends MaybeSigned<T> {

    @field(U8IntArraySerializer)
    signature: Uint8Array

    @field(bufferSerializer(Ed25519PublicKey))
    key: Ed25519PublicKey

    constructor(props?: {
        signature?: Uint8Array,
        key: Ed25519PublicKey
    }) {
        super();
        if (props) {
            this.signature = props.signature;
            this.key = props.key;
        }
    }

    async open(opener: (bytes: Uint8Array, key: Ed25519PublicKey) => Promise<Uint8Array>, constructor: Constructor<T> | Uint8ArrayConstructor): Promise<T> {
        const data = Buffer.from(await opener(this.signature, this.key));
        if (constructor === Uint8Array) {
            return data as any as T
        }
        return deserialize(data, constructor as Constructor<T>);
    }
} */