import {
	Ed25519Keypair,
	X25519Keypair,
	Aes256Key,
	Ed25519PublicKey,
	X25519PublicKey
} from "@peerbit/crypto";

export type KeypairFromPublicKey<T> = T extends X25519PublicKey
	? X25519PublicKey extends T
		? X25519Keypair
		: Ed25519Keypair
	: Ed25519Keypair;

export interface Keychain {
	// Add a key to the keychain.
	// Represents keys internally as X25519 and Aes256.
	// Transforms Ed25519 keys to X25519 keys internally?
	import(
		parameters: (
			| { keypair: Ed25519Keypair | X25519Keypair }
			| { key: Aes256Key }
		) & { id: Uint8Array }
	): Promise<void>;

	// This is only really relevant for asymmetric keys? -> No changes
	exportByKey<
		T extends Ed25519PublicKey | X25519PublicKey,
		Q = KeypairFromPublicKey<T>
	>(
		publicKey: T
	): Promise<Q | undefined>;

	// ID's are the sha256 hashes of the public key (or the symmetric key itself)
	// Throws if no key can be found of type `type` with id `id`?
	exportById<
		T = "ed25519" | "x25519" | "aes256",
		Q = T extends "ed25519"
			? Ed25519Keypair
			: T extends "x25519"
			? X25519Keypair
			: Aes256Key
	>(
		id: Uint8Array,
		type: T
	): Promise<Q | undefined>;
}
