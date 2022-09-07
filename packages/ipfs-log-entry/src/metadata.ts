import { LamportClock as Clock, LamportClock } from './lamport-clock'
import { variant, field, serialize } from '@dao-xyz/borsh';

import { IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider'
import { arraysEqual } from '@dao-xyz/io-utils';
import { X25519PublicKey } from 'sodium-plus';
import { PublicKeyEncryption, DecryptedThing, EncryptedThing, MaybeEncrypted } from '@dao-xyz/encryption-utils';
/* 
@variant(0)
export class IdentitySecure {

    @field({ type: MaybeEncrypted })
    _identity: MaybeEncrypted<IdentitySerializable>;

    constructor(props?: {
        identity: MaybeEncrypted<IdentitySerializable>
    }) {
        if (props) {
            this._identity = props.identity;
        }
    }

    init(encryption?: PublicKeyEncryption) {
        this._identity.init(encryption);
        return this;
    }
    get identity(): Promise<MaybeEncrypted<IdentitySerializable>> {
        return this._identity.decrypt()
    }
}
 */

/* 
@variant(0)
export class Metadata {

    @field({ type: 'string' })
    _id: string // For determining a unique chain

    @field({ type: IdentitySerializable })
    _identity: IdentitySerializable

    @field(U8IntArraySerializerOptional)
    _signature: Uint8Array; // Signing some data

    constructor(props: {
        id: string
        identity: IdentitySerializable,
        signature: Uint8Array
    }) {
        if (props) {
            this._id = props.id
            this._identity = props.identity;
            this._signature = props.signature
        }
    }

    equals(other: Metadata): boolean {
        return this._id === other._id && this._identity.equals(other._identity) && arraysEqual(this._signature, other._signature)
    }
}

@variant(0)
export class MetadataSecure {

    @field({ type: MaybeEncrypted })
    _metadata: MaybeEncrypted<Metadata>;

    constructor(props?: {
        metadata: MaybeEncrypted<Metadata> | Metadata
    }) {
        if (props) {
            if (props.metadata instanceof Metadata) {
                this._metadata = new DecryptedThing({
                    data: serialize(props.metadata)
                })
            }
            else {
                this._metadata = props.metadata;
            }
        }
    }

    init(encryption?: PublicKeyEncryption) {
        this._metadata.init(encryption);
        return this;
    }

    get id(): Promise<string> {
        return this._metadata.decrypt().then(x => x.getValue(Metadata)._id)
    }

    get decrypted(): Metadata {
        if (this._metadata instanceof DecryptedThing) {
            return this._metadata.getValue(Metadata);
        }
        else if (this._metadata instanceof EncryptedThing) {
            return (this._metadata as EncryptedThing<Metadata>).decrypted.getValue(Metadata);
        }
        throw new Error("Unsupported")

    }

    get idDecrypted(): string {
        return this.decrypted._id;
    }


    get identity(): Promise<IdentitySerializable> {
        return this._metadata.decrypt().then(x => x.getValue(Metadata)._identity)
    }

    get signature(): Promise<Uint8Array> {
        return this._metadata.decrypt().then(x => x.getValue(Metadata)._signature)
    }

    async encrypt(recieverPublicKey: X25519PublicKey) {
        if (this._metadata instanceof EncryptedThing) {
            return;
        }
        else if (this._metadata instanceof DecryptedThing) {
            this._metadata = await this._metadata.encrypt(recieverPublicKey)

        }
        else {
            throw new Error("Unsupported")
        }
    }
    equals(other: MetadataSecure) {
        return this._metadata.equals(other._metadata);
    }
} */