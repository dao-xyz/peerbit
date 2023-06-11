import { serialize } from "@dao-xyz/borsh";
import { sha256Base64Sync } from "./hash.js";
import { PeerId } from "@libp2p/interface-peer-id";

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

	toPeerId(): Promise<PeerId> {
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
		return this._hashcode || (this._hashcode = sha256Base64Sync(this.bytes));
	}
}

export interface PrivateSignKey extends Key {}
export abstract class PrivateSignKey implements Key {
	get bytes(): Uint8Array {
		return serialize(this);
	}

	hashcode(): string {
		return this._hashcode || (this._hashcode = sha256Base64Sync(this.bytes));
	}
}

// ---- PUBLIC KEY ENCRYPTION -----
export interface PublicKeyEncryptionKey extends Key {}
export abstract class PublicKeyEncryptionKey implements Key {
	get bytes(): Uint8Array {
		return serialize(this);
	}

	hashcode(): string {
		return this._hashcode || (this._hashcode = sha256Base64Sync(this.bytes));
	}
}
export interface PrivateEncryptionKey extends Key {}
export abstract class PrivateEncryptionKey implements Key {
	get bytes(): Uint8Array {
		return serialize(this);
	}

	hashcode(): string {
		return this._hashcode || (this._hashcode = sha256Base64Sync(this.bytes));
	}
}

// ---- OTHER KEYS ----
export interface PlainKey extends Key {}
export abstract class PlainKey implements Key {
	get bytes(): Uint8Array {
		return serialize(this);
	}

	hashcode(): string {
		return this._hashcode || (this._hashcode = sha256Base64Sync(this.bytes));
	}
}
