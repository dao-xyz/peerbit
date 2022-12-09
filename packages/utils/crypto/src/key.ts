import { serialize } from "@dao-xyz/borsh";
import { toBase64 } from "./utils.js";

interface Key {
    equals(other: Key): boolean;
    get bytes(): Uint8Array;
    hashcode(): string;
    toString(): string;
}

export abstract class Keypair {
    abstract get publicKey(): PublicSignKey | PublicKeyEncryptionKey;

    static create(): Keypair {
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

    hashcode(): string {
        return toBase64(this.bytes);
    }
}

export interface PrivateSignKey extends Key {}
export abstract class PrivateSignKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashcode(): string {
        return toBase64(this.bytes);
    }
}

// ---- PUBLIC KEY ENCRYPTION -----
export interface PublicKeyEncryptionKey extends Key {}
export abstract class PublicKeyEncryptionKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashcode(): string {
        return toBase64(this.bytes);
    }
}
export interface PrivateEncryptionKey extends Key {}
export abstract class PrivateEncryptionKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashcode(): string {
        return toBase64(this.bytes);
    }
}

// ---- OTHER KEYS ----
export interface PlainKey extends Key {}
export abstract class PlainKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashcode(): string {
        return toBase64(this.bytes);
    }
}
