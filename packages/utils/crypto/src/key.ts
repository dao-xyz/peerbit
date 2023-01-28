import { serialize } from "@dao-xyz/borsh";
import crypto from "crypto";

interface Key {
    equals(other: Key): boolean;
    get bytes(): Uint8Array;
    hashcode(): string;
    toString(): string;
    _hashcode: string;
}

export abstract class Keypair {
    abstract get publicKey(): PublicSignKey | PublicKeyEncryptionKey;

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
        return (
            this._hashcode ||
            (this._hashcode = crypto
                .createHash("sha256")
                .update(this.bytes)
                .digest("base64"))
        );
    }
}

export interface PrivateSignKey extends Key {}
export abstract class PrivateSignKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashcode(): string {
        return (
            this._hashcode ||
            (this._hashcode = crypto
                .createHash("sha256")
                .update(this.bytes)
                .digest("base64"))
        );
    }
}

// ---- PUBLIC KEY ENCRYPTION -----
export interface PublicKeyEncryptionKey extends Key {}
export abstract class PublicKeyEncryptionKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashcode(): string {
        return (
            this._hashcode ||
            (this._hashcode = crypto
                .createHash("sha256")
                .update(this.bytes)
                .digest("base64"))
        );
    }
}
export interface PrivateEncryptionKey extends Key {}
export abstract class PrivateEncryptionKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashcode(): string {
        return (
            this._hashcode ||
            (this._hashcode = crypto
                .createHash("sha256")
                .update(this.bytes)
                .digest("base64"))
        );
    }
}

// ---- OTHER KEYS ----
export interface PlainKey extends Key {}
export abstract class PlainKey implements Key {
    get bytes(): Uint8Array {
        return serialize(this);
    }

    hashcode(): string {
        return (
            this._hashcode ||
            (this._hashcode = crypto
                .createHash("sha256")
                .update(this.bytes)
                .digest("base64"))
        );
    }
}
