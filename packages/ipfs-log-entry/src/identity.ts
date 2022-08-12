import { LamportClock as Clock, LamportClock } from './lamport-clock'
import { isDefined } from './is-defined'
import { variant, field, vec, option, serialize, deserialize, BinaryReader } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io';
import { IPFS } from 'ipfs-core-types/src/'
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider'
import { arraysEqual, U8IntArraySerializer, U8IntArraySerializerOptional } from '@dao-xyz/io-utils';
import { X25519PublicKey, Ed25519PublicKey } from 'sodium-plus';
import { CryptOptions, DecryptedThing, EncryptedThing, MaybeEncrypted } from './encryption';
import { IOOptions } from './entry';
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

    init(crypt?: CryptOptions) {
        this._identity.init(crypt);
        return this;
    }
    get identity(): Promise<MaybeEncrypted<IdentitySerializable>> {
        return this._identity.decrypt()
    }
}
 */
@variant(0)
export class IdentityWithSignature {

    @field({ type: IdentitySerializable })
    _identity: IdentitySerializable

    @field(U8IntArraySerializerOptional)
    _signature: Uint8Array; // Signing some data

    constructor(props: {
        identity: IdentitySerializable,
        signature: Uint8Array;
    }) {
        if (props) {
            this._identity = props.identity;
            this._signature = props.signature;
        }
    }

    equals(other: IdentityWithSignature): boolean {
        return this._identity.equals(other._identity) && arraysEqual(this._signature, other._signature)
    }
}

@variant(0)
export class IdentityWithSignatureSecure {

    @field({ type: MaybeEncrypted })
    _identityWithSignature: MaybeEncrypted<IdentityWithSignature>;

    constructor(props?: {
        identityWithSignature: MaybeEncrypted<IdentityWithSignature> | IdentityWithSignature
    }) {
        if (props) {
            if (props.identityWithSignature instanceof IdentityWithSignature) {
                this._identityWithSignature = new DecryptedThing({
                    data: serialize(props.identityWithSignature)
                })
            }
            else {
                this._identityWithSignature = props.identityWithSignature;
            }
        }
    }

    init(crypt?: CryptOptions) {
        this._identityWithSignature.init(crypt);
        return this;
    }

    get identity(): Promise<IdentitySerializable> {
        return this._identityWithSignature.decrypt().then(x => x.getValue(IdentityWithSignature)._identity)
    }

    get signature(): Promise<Uint8Array> {
        return this._identityWithSignature.decrypt().then(x => x.getValue(IdentityWithSignature)._signature)
    }

    async encrypt(recieverPublicKey: X25519PublicKey) {
        if (this._identityWithSignature instanceof EncryptedThing) {
            return;
        }
        else if (this._identityWithSignature instanceof DecryptedThing) {
            this._identityWithSignature = await this._identityWithSignature.encrypt(recieverPublicKey)

        }
        else {
            throw new Error("Unsupported")
        }
    }
    equals(other: IdentityWithSignatureSecure) {
        return this._identityWithSignature.equals(other._identityWithSignature);
    }
}