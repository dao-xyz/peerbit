import { serialize, variant } from "@dao-xyz/borsh";
import { SystemBinaryPayload } from "@dao-xyz/bpayload";

export type IdentityProviderType = 'orbitdb' | 'ethereum' | 'solana';

@variant(0)
export class Identity extends SystemBinaryPayload {

}



@variant(0)
export class PublicKey extends Identity {

    equals(other: PublicKey): boolean {
        throw new Error("Not implemented");
    }

    getBuffer(): Buffer {
        return Buffer.from(serialize(this))
    }
    hashCode(): string {
        return this.getBuffer().toString('base64');
    }

    toString(): string {
        throw new Error("Not implemented");
    }

}



/* 
@variant(1)
export class PublicKey extends TrustData {

    @field(U8IntArraySerializer)
    id: Uint8Array;

    @field({ type: 'string' })
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

    static from(identity: PublicKey | { type: IdentityProviderType, id: Uint8Array }): PublicKey {
        if (identity instanceof PublicKey) {
            return identity;
        }

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

 */