import { field, serialize } from "@dao-xyz/borsh";
import { sha256Base64Sync } from "./hash.js";
import { PeerId } from "@libp2p/interface/peer-id";
import { compare } from "@peerbit/uint8arrays";
import { toHexString } from "./utils";

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

	// TODO: Should we add not implemented errors for .create and and .from as well?
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

export class ByteKey extends PlainKey {
	@field({ type: Uint8Array })
	key: Uint8Array;

	constructor(properties: { key: Uint8Array }) {
		super();
		this.key = properties.key;
	}

	equals(other: ByteKey) {
		return compare(this.key, other.key) === 0;
	}

	// TODO: What should be preprended to this string here?
	toString(): string {
		return "bytekey/" + toHexString(this.key);
	}
}
