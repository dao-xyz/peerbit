import {
	Ed25519Keypair,
	X25519Keypair,
	Keypair,
	XSalsa20Poly1305,
	Ed25519PublicKey,
	X25519PublicKey,
	ByteKey,
	Secp256k1Keypair,
	Secp256k1PublicKey,
	PublicKeyEncryptionKey,
	PublicSignKey
} from "@peerbit/crypto";

export type KeypairFromPublicKey<T> = T extends X25519PublicKey
	? X25519Keypair
	: T extends Ed25519PublicKey
	? Ed25519Keypair
	: T extends Secp256k1PublicKey
	? Secp256k1Keypair
	: T extends PublicSignKey | PublicKeyEncryptionKey
	? Keypair
	: never;

// Should perhaps be un crypto package
export type Keypairs =
	| Ed25519Keypair
	| Keypair
	| X25519Keypair
	| Secp256k1Keypair;

// Should perhaps be un crypto package
export type PublicKeys =
	| Ed25519PublicKey
	| X25519PublicKey
	| Secp256k1PublicKey
	| PublicSignKey
	| PublicKeyEncryptionKey;

export interface Keychain {
	// Add a key to the keychain.
	import(
		parameters: (
			| { keypair: Keypairs }
			| { key: XSalsa20Poly1305 | ByteKey }
		) & { id: Uint8Array }
	): Promise<void>;

	// This is only really relevant for asymmetric keys? -> No changes
	exportByPublicKey<T extends PublicKeys, Q = KeypairFromPublicKey<T>>(
		publicKey: T
	): Promise<Q | undefined>;

	// Export any key by their hashcode.
	// If Key is PublicKey/PrivateKey keypair. The hashcode should be of the publickey
	/* 
		key = new ByteKey({key: new Uint8Array(32)})
		keychain.exportByHash(key.hashcode())  // returns key
	*/
	exportByHash<T extends PublicKeys, Q = KeypairFromPublicKey<T>>(
		hash: string
	): Promise<Q | undefined>;

	// ID's are the sha256base are user defined ids. Anyone can store any key with a specific  id
	exportById<
		T =
			| "ed25519"
			| "x25519"
			| "secp256k1"
			| "xsalsa20poly1305"
			| "bytekey"
			| "keypair",
		Q = T extends "ed25519"
			? Ed25519Keypair
			: T extends "x25519"
			? X25519Keypair
			: T extends "secp256k1"
			? Secp256k1Keypair
			: T extends "keypair"
			? Keypair
			: T extends "xsalsa20poly1305"
			? XSalsa20Poly1305
			: ByteKey
	>(
		id: string,
		type: T
	): Promise<Q | undefined>;
}
