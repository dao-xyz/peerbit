import { field, variant } from "@dao-xyz/borsh";
import { IdentityAsJson } from "orbit-db-identity-provider";
import type { IdentityProviderType } from "orbit-db-identity-provider";
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

@variant(0)
export class BSignatures {

    @field({ type: 'String' })
    id: string;

    @field({ type: 'String' })
    publicKey: string;

    constructor(options?: {
        id: string;
        publicKey: string;
    }) {
        if (options) {
            Object.assign(this, options);
        }
    }

}

@variant(0)
export class BIdentity {
    @field({ type: 'String' })
    id: string;

    @field({ type: 'String' })
    publicKey: string;

    @field({ type: BSignatures })
    signatures: BSignatures;

    @field({ type: 'String' })
    type: IdentityProviderType;

    constructor(options?: {
        id: string;
        publicKey: string;
        signatures: BSignatures,
        type: String;
    }) {
        if (options) {
            Object.assign(this, options);
        }
    }

    toIdentityJSON(): IdentityAsJson {
        return this; // the same!
    }

    static from(identity: IdentityAsJson): BIdentity {
        return new BIdentity({
            id: identity.id,
            publicKey: identity.publicKey,
            signatures: new BSignatures({
                id: identity.signatures.id,
                publicKey: identity.signatures.publicKey
            }),
            type: identity.type
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