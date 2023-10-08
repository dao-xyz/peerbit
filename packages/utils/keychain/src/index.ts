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

export interface Keychain {
	// Add a key to the keychain.
	import(
		parameters: (
			| { keypair: Ed25519Keypair | X25519Keypair | Secp256k1Keypair | Keypair }
			| { key: XSalsa20Poly1305 | ByteKey }
		) & { id: Uint8Array }
	): Promise<void>;

	// This is only really relevant for asymmetric keys? -> No changes
	exportByKey<
		T extends
			| Ed25519PublicKey
			| X25519PublicKey
			| Secp256k1PublicKey
			| PublicSignKey
			| PublicKeyEncryptionKey,
		Q = KeypairFromPublicKey<T>
	>(
		publicKey: T
	): Promise<Q | undefined>;

	// ID's are the sha256base hashes of the public key (or the symmetric key itself)
	// Throws if no key can be found of type `type` with id `id`?
	// If type is undefined, just return any bytekey.
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
