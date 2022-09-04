export * from './errors';
import { BinaryReader, BinaryWriter, Constructor, deserialize, field, option, serialize, variant, vec } from '@dao-xyz/borsh';
import { U8IntArraySerializer } from '@dao-xyz/io-utils';
import { X25519PublicKey, Ed25519PublicKey, SodiumPlus, CryptographyKey } from 'sodium-plus';
import { arraysEqual } from '@dao-xyz/io-utils'
import { X25519SecretKey } from 'sodium-plus';
const NONCE_LENGTH = 24;
const _crypto = SodiumPlus.auto();
export interface PublicKeyEncryption {
    getEncryptionKey: () => Promise<X25519SecretKey>
    getAnySecret: (publicKey: X25519PublicKey[]) => Promise<{ index: number, secretKey: X25519SecretKey } | undefined>

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

    async encrypt(...recieverPublicKeys: X25519PublicKey[]): Promise<EncryptedThing<T>> {
        const bytes = serialize(this)
        const crypto = await _crypto;
        const epheremalKey = await crypto.crypto_secretbox_keygen();
        const nonce = new Uint8Array(await crypto.randombytes_buf(NONCE_LENGTH));
        const cipher = await crypto.crypto_secretbox(Buffer.from(bytes), Buffer.from(nonce), epheremalKey);
        const encryptionKey = await this._encryption.getEncryptionKey();
        const ks = await Promise.all(recieverPublicKeys.map(async recieverPublicKey => {
            const kNonce = new Uint8Array(await crypto.randombytes_buf(NONCE_LENGTH));
            return new K({
                encryptedKey: new CipherWithNonce({
                    cipher: await crypto.crypto_box(epheremalKey.getBuffer(), Buffer.from(kNonce), encryptionKey, recieverPublicKey),
                    nonce: kNonce
                }), recieverPublicKey
            })
        }))
        const enc = new EncryptedThing<T>({
            encrypted: new Uint8Array(cipher), nonce, envelope: new Envelope({
                senderPublicKey: await crypto.crypto_box_publickey_from_secretkey(encryptionKey), ks
            })
        })
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

@variant(0)
export class CipherWithNonce {


    @field(U8IntArraySerializer)
    nonce: Uint8Array

    @field(U8IntArraySerializer)
    cipher: Uint8Array

    constructor(props?: {
        nonce: Uint8Array
        cipher: Uint8Array

    }) {
        if (props) {
            this.nonce = props.nonce;
            this.cipher = props.cipher;
        }
    }

    equals(other: CipherWithNonce): boolean {
        if (other instanceof CipherWithNonce) {
            return arraysEqual(this.nonce, other.nonce) && arraysEqual(this.cipher, other.cipher);
        }
        else {
            return false;
        }
    }
}


@variant(0)
export class K {

    @field({ type: CipherWithNonce })
    _encryptedKey: CipherWithNonce;

    @field(bufferSerializer(X25519PublicKey))
    _recieverPublicKey: X25519PublicKey

    constructor(props?: {
        encryptedKey: CipherWithNonce,
        recieverPublicKey: X25519PublicKey;
    }) {
        if (props) {
            this._encryptedKey = props.encryptedKey
            this._recieverPublicKey = props.recieverPublicKey

        }
    }


    equals(other: K): boolean {
        if (other instanceof K) {
            return this._encryptedKey.equals(other._encryptedKey) && Buffer.compare(this._recieverPublicKey.getBuffer(), other._recieverPublicKey.getBuffer()) === 0
        }
        else {
            return false;
        }
    }

}

@variant(0)
export class Envelope {
    @field(bufferSerializer(X25519PublicKey))
    _senderPublicKey: X25519PublicKey

    @field({ type: vec(K) })
    _ks: K[];


    constructor(props?: {
        senderPublicKey: X25519PublicKey
        ks: K[]
    }) {
        if (props) {
            this._senderPublicKey = props.senderPublicKey;
            this._ks = props.ks;
        }
    }

    equals(other: Envelope): boolean {
        if (other instanceof Envelope) {
            if (Buffer.compare(this._senderPublicKey.getBuffer(), other._senderPublicKey.getBuffer()) !== 0) {
                return false;
            }

            if (this._ks.length != other._ks.length) {
                return false;
            }
            for (let i = 0; i < this._ks.length; i++) {
                if (!this._ks[i].equals(other._ks[i])) {
                    return false;
                }

            }
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

    @field(U8IntArraySerializer)
    _nonce: Uint8Array;

    @field({ type: Envelope })
    _envelope: Envelope

    constructor(props?: {
        encrypted: Uint8Array;
        nonce: Uint8Array;
        envelope: Envelope
    }) {
        super();
        if (props) {
            this._encrypted = props.encrypted;
            this._nonce = props.nonce;
            this._envelope = props.envelope;

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
        const crypto = await _crypto;
        // We only need to open with one of the keys
        const key = await this._encryption.getAnySecret(this._envelope._ks.map(k => k._recieverPublicKey))
        if (key) {
            const k = this._envelope._ks[key.index];
            const epheremalKey = new CryptographyKey(await crypto.crypto_box_open(Buffer.from(k._encryptedKey.cipher), Buffer.from(k._encryptedKey.nonce), key.secretKey, this._envelope._senderPublicKey));
            let der: any = this;
            let counter = 0;
            while (der instanceof EncryptedThing) {
                const decrypted = await crypto.crypto_secretbox_open(Buffer.from(this._encrypted), Buffer.from(this._nonce), epheremalKey);
                der = deserialize(Buffer.from(decrypted), DecryptedThing)
                counter += 1;
                if (counter >= 10) {
                    throw new Error("Unexpected decryption behaviour, data seems to always be in encrypted state")
                }
            }
            this._decrypted = der as DecryptedThing<T>
        }
        return this._decrypted;
    }


    equals(other: MaybeEncrypted<T>): boolean {
        if (other instanceof EncryptedThing) {
            if (!arraysEqual(this._encrypted, other._encrypted)) {
                return false;
            }
            if (!arraysEqual(this._nonce, other._nonce)) {
                return false;
            }

            if (!this._envelope.equals(other._envelope)) {
                return false;
            }
        }
        else {
            return false;
        }
    }


    clear() {
        this._decrypted = undefined;
    }
}


export const verifySignature = async (signature: Uint8Array, publicKey: Ed25519PublicKey, data: Uint8Array, signedHash = false) => {
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

    async verify(verifier?: (signature: Uint8Array, key: Ed25519PublicKey, data: Uint8Array) => Promise<boolean>): Promise<boolean> {
        if (!this.signature) {
            return true;
        }
        return verifier ? verifier(this.signature.signature, this.signature.publicKey, this.data) : verifySignature(this.signature.signature, this.signature.publicKey, this.data)
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
export const decryptVerifyInto = async <T>(data: Uint8Array, clazz: Constructor<T>, encryption?: PublicKeyEncryption, options: { isTrusted?: (key: Ed25519PublicKey) => Promise<boolean> } = {}) => {
    const maybeEncrypted = deserialize<MaybeEncrypted<MaybeSigned<any>>>(Buffer.from(data), MaybeEncrypted);
    const decrypted = await (encryption ? maybeEncrypted.init(encryption) : maybeEncrypted).decrypt();
    const maybeSigned = decrypted.getValue(MaybeSigned);
    if (!await maybeSigned.verify()) {
        return;
    }

    if (maybeSigned.signature && options.isTrusted) {
        if (!await options.isTrusted(maybeSigned.signature.publicKey)) {
            return;
        }
    }
    return deserialize(Buffer.from(maybeSigned.data), clazz);
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