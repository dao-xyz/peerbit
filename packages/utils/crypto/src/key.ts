import { serialize } from "@dao-xyz/borsh";

export type IdentityProviderType = "orbitdb" | "ethereum" | "solana";

interface Key {
    equals(other: Key): boolean;
    get bytes(): Uint8Array;
    hashCode(): string;
    toString(): string;
}

export abstract class Keypair {
    publicKey: PublicSignKey | PublicKeyEncryptionKey;

    static async create(): Promise<Keypair> {
        throw new Error("Not implemented");
    }

    equals(other: Keypair): boolean {
        throw new Error("Not implemented");
    }
}

// ---- SIGNATURE KEYS -----
export interface PublicSignKey extends Key {}
export abstract class PublicSignKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashCode(): string {
        return Buffer.from(this.bytes).toString("base64");
    }
}

export interface PrivateSignKey extends Key {}
export abstract class PrivateSignKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashCode(): string {
        return Buffer.from(this.bytes).toString("base64");
    }
}

// ---- PUBLIC KEY ENCRYPTION -----
export interface PublicKeyEncryptionKey extends Key {}
export abstract class PublicKeyEncryptionKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashCode(): string {
        return Buffer.from(this.bytes).toString("base64");
    }
}
export interface PrivateEncryptionKey extends Key {}
export abstract class PrivateEncryptionKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashCode(): string {
        return Buffer.from(this.bytes).toString("base64");
    }
}

// ---- OTHER KEYS ----
export interface PlainKey extends Key {}
export abstract class PlainKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashCode(): string {
        return Buffer.from(this.bytes).toString("base64");
    }
}
