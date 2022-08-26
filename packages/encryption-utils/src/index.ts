export * from './errors';
import { BinaryReader, BinaryWriter, Constructor, deserialize, field, option, serialize, variant } from '@dao-xyz/borsh';
import { U8IntArraySerializer } from '@dao-xyz/io-utils';
import { X25519PublicKey, Ed25519PublicKey, X25519SecretKey, Ed25519SecretKey, CryptographyKey } from 'sodium-plus';
import { arraysEqual } from '@dao-xyz/io-utils'

export interface PublicKeyEncryption {
    encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ data: Uint8Array, senderPublicKey: X25519PublicKey }>,
    decrypt: (data: Uint8Array, senderPublicKey: X25519PublicKey, recieverPublicKey: X25519PublicKey) => Promise<Uint8Array>
}


export type GetBuffer = {
    getBuffer(): Buffer
}
export const bufferSerializer = (clazz: Constructor<GetBuffer>) => {
    return {
        serialize: (obj: GetBuffer, writer: BinaryWriter) => {
            const buffer = obj.getBuffer();
            writer.writeU32(buffer.length);
            for (let i = 0; i < buffer.length; i++) {
                writer.writeU8(buffer[i])
            }
        },
        deserialize: (reader: BinaryReader) => {
            const len = reader.readU32();
            const arr = new Uint8Array(len);
            for (let i = 0; i < len; i++) {
                arr[i] = reader.readU8();
            }
            return new clazz(Buffer.from(arr));
        }
    }
}


@variant(0)
export class MaybeEncrypted<T>  {

    _encryption: PublicKeyEncryption
    init(encryption?: PublicKeyEncryption) {
        this._encryption = encryption;
        return this;
    }
    constructor() {

    }

    /**
     * Will throw error if not decrypted
     */
    get decrypted(): DecryptedThing<T> {
        throw new Error("Not implented")
    }

    async decrypt(): Promise<DecryptedThing<T>> {
        throw new Error("Not implemented")
    }
    equals(other: MaybeEncrypted<T>): boolean {
        throw new Error("Not implemented")
    }

    /**
     * Clear cached data
     */
    clear() {
        throw new Error("Not implemented")
    }


}

@variant(0)
export class DecryptedThing<T> extends MaybeEncrypted<T> {

    @field(U8IntArraySerializer)
    _data: Uint8Array;

    constructor(props?: { data?: Uint8Array, value?: T }) {
        super();
        if (props) {
            this._data = props.data;
            this._value = props.value
        }
    }

    _value?: T;
    getValue(clazz: Constructor<T>): T {
        if (this._value) {
            return this._value;
        }
        return deserialize(Buffer.from(this._data), clazz)
    }

    async encrypt(recieverPublicKey: X25519PublicKey): Promise<EncryptedThing<T>> {
        const bytes = serialize(this)
        const { data, senderPublicKey } = await this._encryption.encrypt(Buffer.from(bytes), recieverPublicKey);
        const enc = new EncryptedThing<T>({ encrypted: data, senderPublicKey, recieverPublicKey })
        enc._decrypted = this;
        return enc;
    }

    get decrypted(): DecryptedThing<T> {
        return this;
    }

    async decrypt(): Promise<DecryptedThing<T>> {
        return this;
    }

    equals(other: MaybeEncrypted<T>) {
        if (other instanceof DecryptedThing) {
            return arraysEqual(this._data, other._data)
        }
        else {
            return false;
        }
    }

    clear() {
        this._value = undefined;
    }
}

@variant(1)
export class EncryptedThing<T> extends MaybeEncrypted<T> {

    _encryption: PublicKeyEncryption

    @field(U8IntArraySerializer)
    _encrypted: Uint8Array;

    @field(bufferSerializer(X25519PublicKey))
    _senderPublicKey: X25519PublicKey

    @field(bufferSerializer(X25519PublicKey))
    _recieverPublicKey: X25519PublicKey


    constructor(obj?: {
        encrypted: Uint8Array;
        senderPublicKey: X25519PublicKey;
        recieverPublicKey: X25519PublicKey;

    }) {
        super();
        if (obj) {
            this._encrypted = obj.encrypted;
            this._senderPublicKey = obj.senderPublicKey;
            this._recieverPublicKey = obj.recieverPublicKey
        }
    }



    _decrypted: DecryptedThing<T>
    get decrypted(): DecryptedThing<T> {
        if (!this._decrypted) {
            throw new Error("Entry has not been decrypted, invoke decrypt method before")
        }
        return this._decrypted;
    }


    async decrypt(): Promise<DecryptedThing<T>> {
        if (this._decrypted) {
            return this._decrypted
        }

        if (!this._encryption) {
            throw new Error("Not initialized");
        }
        if (!this._encrypted || !this._senderPublicKey || !this._recieverPublicKey) {
            throw new Error("X");
        }

        let der: any = this;
        let counter = 0;
        while (der instanceof EncryptedThing) {
            const decrypted = await this._encryption.decrypt(this._encrypted, this._senderPublicKey, this._recieverPublicKey);
            if (!decrypted) {
                throw new Error("Y");
            }
            der = deserialize(Buffer.from(decrypted), DecryptedThing)
            counter += 1;
            if (counter >= 10) {
                throw new Error("Unexpected decryption behaviour, data seems to always be in encrypted state")
            }
        }
        this._decrypted = der as DecryptedThing<T>
        return this._decrypted;
    }


    equals(other: MaybeEncrypted<T>): boolean {
        if (other instanceof EncryptedThing) {
            return arraysEqual(this._encrypted, other._encrypted) && Buffer.compare(this._senderPublicKey.getBuffer(), other._senderPublicKey.getBuffer()) === 0 && Buffer.compare(this._recieverPublicKey.getBuffer(), other._recieverPublicKey.getBuffer()) === 0
        }
        else {
            return false;
        }
    }


    clear() {
        this._decrypted = undefined;
    }
}

@variant(0)
export class SignatureWithKey {

    @field(U8IntArraySerializer)
    signature: Uint8Array

    @field(bufferSerializer(Ed25519PublicKey))
    publicKey: Ed25519PublicKey

    constructor(props?: {
        signature: Uint8Array,
        publicKey: Ed25519PublicKey
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
        return Buffer.compare(this.publicKey.getBuffer(), other.publicKey.getBuffer()) === 0;
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
        return deserialize(Buffer.from(this.data), constructor)
    }

    async verify(verifier: (signature: Uint8Array, key: Ed25519PublicKey, data: Uint8Array) => Promise<boolean>): Promise<boolean> {
        if (!this.signature) {
            return true;
        }
        return verifier(this.signature.signature, this.signature.publicKey, this.data)
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
    async sign(signer: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: Ed25519PublicKey }>): Promise<MaybeSigned<T>> {
        const signatureResult = await signer(this.data)
        this.signature = new SignatureWithKey({
            publicKey: signatureResult.publicKey,
            signature: signatureResult.signature
        })
        return this;
    }

}

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
        return deserialize(Buffer.from(this.data), constructor)
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