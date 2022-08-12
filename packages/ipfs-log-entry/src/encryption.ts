import { BinaryReader, BinaryWriter, Constructor, deserialize, field, serialize, variant } from '@dao-xyz/borsh';
import { U8IntArraySerializer } from '@dao-xyz/io-utils';
import { X25519PublicKey } from 'sodium-plus';
import { arraysEqual } from '@dao-xyz/io-utils'
export interface CryptOptions {
    encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ data: Uint8Array, senderPublicKey: X25519PublicKey }>,
    decrypt: (data: Uint8Array, senderPublicKey: X25519PublicKey, recieverPublicKey: X25519PublicKey) => Promise<Uint8Array>
}

export interface Encryption {
    recieverPayload: X25519PublicKey,
    recieverIdentity: X25519PublicKey,
    options: CryptOptions
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

@variant(0)
export class MaybeEncrypted<T>  {

    _crypt: CryptOptions
    init(crypt?: CryptOptions) {
        this._crypt = crypt;
        return this;
    }
    constructor() {

    }

    async encrypt(_recieverPublicKey: X25519PublicKey): Promise<EncryptedThing<T>> {
        throw new Error("Not implemented")

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
        const { data, senderPublicKey } = await this._crypt.encrypt(Buffer.from(bytes), recieverPublicKey);
        const enc = new EncryptedThing<T>({ data, senderPublicKey, recieverPublicKey })
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

    _crypt: CryptOptions

    @field(U8IntArraySerializer)
    _data: Uint8Array;

    @field(X25519PublicKeySerializer)
    _senderPublicKey: X25519PublicKey

    @field(X25519PublicKeySerializer)
    _recieverPublicKey: X25519PublicKey


    constructor(obj?: {
        data: Uint8Array;
        senderPublicKey: X25519PublicKey;
        recieverPublicKey: X25519PublicKey;

    }) {
        super();
        if (obj) {
            this._data = obj.data;
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

    async encrypt(recieverPublicKey: X25519PublicKey) {
        if (this._recieverPublicKey !== recieverPublicKey) {
            throw new Error("Re-encrypt not supported, please decrypt and decrypt manually")
        }
        return this;
    }

    async decrypt(): Promise<DecryptedThing<T>> {
        if (this._decrypted) {
            return this._decrypted
        }
        let der: any = this;
        let counter = 0;
        while (der instanceof EncryptedThing) {
            der = deserialize(Buffer.from(await this._crypt.decrypt(this._data, this._senderPublicKey, this._recieverPublicKey)), DecryptedThing)
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
            return arraysEqual(this._data, other._data) && Buffer.compare(this._senderPublicKey.getBuffer(), other._senderPublicKey.getBuffer()) === 0 && Buffer.compare(this._recieverPublicKey.getBuffer(), other._recieverPublicKey.getBuffer()) === 0
        }
        else {
            return false;
        }
    }
}