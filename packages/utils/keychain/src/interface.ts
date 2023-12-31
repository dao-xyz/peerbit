import {
	Ed25519Keypair,
	X25519Keypair,
	Keypair,
	Ed25519PublicKey,
	X25519PublicKey,
	ByteKey,
	Secp256k1Keypair,
	Secp256k1PublicKey,
	PublicKeyEncryptionKey,
	PublicSignKey
} from "@peerbit/crypto";
import { AbstractType } from "@dao-xyz/borsh";

export type KeypairFromPublicKey<T> = T extends X25519PublicKey
	? X25519Keypair
	: T extends Ed25519PublicKey
		? Ed25519Keypair
		: T extends Secp256k1PublicKey
			? Secp256k1Keypair
			: T extends PublicSignKey | PublicKeyEncryptionKey
				? Keypair
				: never;

export type KeypairParameters = {
	keypair: Ed25519Keypair | X25519Keypair | Secp256k1Keypair | Keypair;
};
export type KeyParameters = { key: ByteKey };

export interface Keychain {
	// Add a key to the keychain.
	import(
		parameters: (KeypairParameters | KeyParameters) & { id: Uint8Array }
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
		T extends Ed25519Keypair | Secp256k1Keypair | X25519Keypair | ByteKey
	>(
		id: Uint8Array,
		type: AbstractType<T>
	): Promise<T | undefined>;
}
