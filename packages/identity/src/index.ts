import { field, serialize, variant } from "@dao-xyz/borsh";
import { createHash } from "crypto";
import { BinaryPayload } from "@dao-xyz/bpayload";
export type IdentityProviderType = 'orbitdb' | 'ethereum' | 'solana' | string;

@variant("trust")
export class TrustData extends BinaryPayload {
}

@variant(0)
export class PublicKey extends TrustData {

    @field({ type: 'String' })
    id: string;

    @field({ type: 'String' })
    type: string;


    constructor(properties?: {
        id: string;
        type: IdentityProviderType;
    }) {
        super();
        if (properties) {
            this.id = properties.id;
            this.type = properties.type;
        }
    }

    static from(identity: PublicKey | { type: string, id: string }): PublicKey {
        if (identity instanceof PublicKey)
            return identity;
        return new PublicKey({
            id: identity.id,
            type: identity.type
        })
    }

    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex');
    }
    toString() {
        return this.type + '/' + this.id;
    }
}

