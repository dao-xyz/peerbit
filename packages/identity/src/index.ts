import { field, serialize, variant } from "@dao-xyz/borsh";
import { createHash } from "crypto";
import { BinaryPayload } from "@dao-xyz/bpayload";
import { U8IntArraySerializer, arraysEqual } from "@dao-xyz/io-utils";
export type IdentityProviderType = 'orbitdb' | 'ethereum' | 'solana' | string;

@variant("trust")
export class TrustData extends BinaryPayload {
}

@variant(0)
export class PublicKey extends TrustData {

    @field(U8IntArraySerializer)
    id: Uint8Array;

    @field({ type: 'String' })
    type: string;


    constructor(properties?: {
        id: Uint8Array;
        type: IdentityProviderType;
    }) {
        super();
        if (properties) {
            this.id = properties.id;
            this.type = properties.type;
        }
    }

    static from(identity: PublicKey | { type: string, id: Uint8Array }): PublicKey {
        if (identity instanceof PublicKey)
            return identity;
        /*  else if (typeof identity === 'string') {
             const splitIndex = identity.indexOf("/")
             if (splitIndex == -1) {
                 throw new Error("When parsing PublicKey from string, identity is expected to be in the form [CHAIN TYPE]/[PUBLICKEY], got: " + identity)
             }
             const type = identity.substring(0, splitIndex);
             const id = identity.substring(splitIndex + 1);
             return new PublicKey({
                 id: new Uint8Array(Buffer.from(id)), type
             })
         } */
        return new PublicKey({
            id: identity.id,
            type: identity.type
        })
    }

    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex');
    }

    equals(other: { type: string, id: Uint8Array }) {
        return this.type === other.type && arraysEqual(this.id, other.id)
    }

    toString() {
        return this.type + '/' + Buffer.from(this.id).toString('base64');
    }
}

