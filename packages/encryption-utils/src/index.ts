import { BinaryReader, BinaryWriter, Constructor, deserialize, field, serialize, variant } from '@dao-xyz/borsh';
import { U8IntArraySerializer } from '@dao-xyz/io-utils';
import { X25519PublicKey, Ed25519PublicKey, CryptographyKey } from 'sodium-plus';
import { arraysEqual } from '@dao-xyz/io-utils'

export interface PublicKeyEncryption {
    encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ data: Uint8Array, senderPublicKey: X25519PublicKey }>,
    decrypt: (data: Uint8Array, senderPublicKey: X25519PublicKey, recieverPublicKey: X25519PublicKey) => Promise<Uint8Array>
}

export interface Encryption {
    recieverPayload: X25519PublicKey,
    recieverIdentity: X25519PublicKey,
    options: PublicKeyEncryption
}

export const Ed25519PublicKeySerializer = {
    serialize: (obj: Ed25519PublicKey, writer: BinaryWriter) => {
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
        return new Ed25519PublicKey(Buffer.from(arr));
    }
}

export const X25519PublicKeySerializer = {
    serialize: (obj: X25519PublicKey, writer: BinaryWriter) => {
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
        return new X25519PublicKey(Buffer.from(arr));
    }
}

export const CryptographyKeySerializer = {
    serialize: (obj: CryptographyKey, writer) => {
        const buffer = obj.getBuffer();
        writer.writeU32(buffer.length);
        buffer.forEach((value) => {
            writer.writeU8(value)
        })
    },
    deserialize: (reader) => {
        const len = reader.readU32();
        const arr = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            arr[i] = reader.readU8();
        }
        return new CryptographyKey(Buffer.from(arr));
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


    async decrypt(): Promise<DecryptedThing<T>> {
        throw new Error("Not implemented")
    }
    equals(other: MaybeEncrypted<T>): boolean {
        throw new Error("Not implemented")
    }


}

@variant(0)
export class DecryptedThing<T> extends MaybeEncrypted<T> {

    @field(U8IntArraySerializer)
    _data: Uint8Array;

    constructor(props?: { data: Uint8Array }) {
        super();
        if (props) {
            this._data = props.data;

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
}

@variant(1)
export class EncryptedThing<T> extends MaybeEncrypted<T> {

    _encryption: PublicKeyEncryption

    @field(U8IntArraySerializer)
    _encrypted: Uint8Array;

    @field(X25519PublicKeySerializer)
    _senderPublicKey: X25519PublicKey

    @field(X25519PublicKeySerializer)
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
        let der: any = this;
        let counter = 0;
        while (der instanceof EncryptedThing) {
            der = deserialize(Buffer.from(await this._encryption.decrypt(this._encrypted, this._senderPublicKey, this._recieverPublicKey)), DecryptedThing)
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
}


@variant(0)
export class MaybeSigned<T>  {

    async open(opener: (bytes: Uint8Array, key: Ed25519PublicKey) => Promise<Uint8Array>, constructor: Constructor<T> | Uint8ArrayConstructor): Promise<T> {
        throw new Error("Not implemented")
    }

    equals(other: MaybeEncrypted<T>): boolean {
        throw new Error("Not implemented")
    }


}


@variant(0)
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

    @field(Ed25519PublicKeySerializer)
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
}