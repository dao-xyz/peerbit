import { field } from "@dao-xyz/borsh";
import { IdentityAsJson } from "@dao-xyz/orbit-db-identity-provider";

export class PublicKey {
    @field({ type: 'String' })
    type: string;

    @field({ type: 'String' })
    address: string;
    constructor(props?: {
        type: string;
        address: string;
    }) {
        if (props) {
            Object.assign(this, props);
        }
    }

    public equals(other: PublicKey): boolean {
        return this.type == other.type && this.address == other.address;
    }

    public toString(): string {
        return this.type + "/" + this.address;
    }

    static from(identity: IdentityAsJson): PublicKey {
        return new PublicKey({
            type: identity.type,
            address: identity.publicKey
        })
    }
}
/* import { field, variant } from "@dao-xyz/borsh";


export class PublicKey {

    public equals(_other: PublicKey): boolean {
        throw new Error("Not implemented")
    }

    public toString(): string {
        throw new Error("Not implemented")
    }

    static from(type: string, address: string): PublicKey {
        switch (type) {
            case 'orbitdb':
                return new OrbitDBPublicKey({
                    address
                })
            case 'ether':
                return new EtherPublicKey({
                    address
                })
            default:
                throw new Error("Unknown identity type: " + type)
        }
    }
}

@variant(0)
export class OrbitDBPublicKey extends PublicKey {

    @field({ type: 'String' })
    address: string;

    constructor(props?: {
        address: string
    }) {
        super();
        if (props) {
            Object.assign(this, props);
        }

    }

    public equals(other: PublicKey): boolean {
        if (other instanceof EtherPublicKey) {
            return other.address == this.address
        }
        return false;
    }

    public toString(): string {
        return this.address;
    }
}


@variant(1)
export class EtherPublicKey extends PublicKey {

    @field({ type: 'String' })
    address: string;

    constructor(props?: {
        address: string
    }) {
        super();
        if (props) {
            Object.assign(this, props);
        }

    }

    public equals(other: PublicKey): boolean {
        if (other instanceof EtherPublicKey) {
            return other.address == this.address
        }
        return false;
    }

    public toString(): string {
        return this.address;
    }
} */