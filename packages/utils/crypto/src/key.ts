import { serialize, variant } from "@dao-xyz/borsh";
import { SystemBinaryPayload } from "@dao-xyz/bpayload";

export type IdentityProviderType = 'orbitdb' | 'ethereum' | 'solana';


@variant(2)
export class Crypto extends SystemBinaryPayload {

}




@variant(0)
export class Key extends Crypto {

    equals(other: Key): boolean {
        throw new Error("Not implemented");
    }

    get bytes(): Uint8Array {
        return serialize(this)
    }

    hashCode(): string {
        return Buffer.from(this.bytes).toString('base64');
    }

    toString(): string {
        throw new Error("Not implemented");
    }
}

@variant(1)
export class Keypair extends Crypto {

    publicKey: PublicSignKey | PublicKeyEncryptionKey

    static async create(): Promise<Keypair> {
        throw new Error("Not implemented")
    }

    equals(other: Keypair): boolean {
        throw new Error("Not implemented");
    }
}



// ---- SIGNATURE KEYS -----

@variant(0)
export class SignKey extends Key {

}
@variant(0)
export class PublicSignKey extends SignKey {

}
@variant(1)
export class PrivateSignKey extends SignKey {

}


// ---- PUBLIC KEY ENCRYPTION -----
@variant(1)
export class PublicKeyEncryption extends Key {

}
@variant(0)
export class PublicKeyEncryptionKey extends PublicKeyEncryption {

}

@variant(1)
export class PrivateEncryptionKey extends PublicKeyEncryption {

}

// ---- OTHER KEYS ----
@variant(2)
export class OtherKey extends Key { }

@variant(0)
export class PlainKey extends OtherKey { }


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