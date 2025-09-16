import { type AbstractType } from "@dao-xyz/borsh";
import { type Keychain as ILibp2pKeychain } from "@libp2p/keychain";
import type {
	ByteKey,
	Ed25519Keypair,
	Ed25519PublicKey,
	Keypair,
	PublicKeyEncryptionKey,
	PublicSignKey,
	Secp256k1Keypair,
	Secp256k1PublicKey,
	X25519Keypair,
	X25519PublicKey,
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

export type KeypairParameters = {
	keypair: Ed25519Keypair | X25519Keypair | Secp256k1Keypair | Keypair;
};
export type KeyParameters = { key: ByteKey };

export interface CryptoKeychain {
	// Add a key to the keychain.
	import(
		parameters: (KeypairParameters | KeyParameters) & { id: Uint8Array },
	): Promise<void>;

	// This is only really relevant for asymmetric keys? -> No changes
	exportByKey<
		T extends
			| Ed25519PublicKey
			| X25519PublicKey
			| Secp256k1PublicKey
			| PublicSignKey
			| PublicKeyEncryptionKey,
		Q = KeypairFromPublicKey<T>,
	>(
		publicKey: T,
	): Promise<Q | undefined>;

	// ID's are the sha256base hashes of the public key (or the symmetric key itself)
	// Throws if no key can be found of type `type` with id `id`?
	// If type is undefined, just return any bytekey.
	exportById<
		T extends Ed25519Keypair | Secp256k1Keypair | X25519Keypair | ByteKey,
	>(
		id: Uint8Array,
		type: AbstractType<T>,
	): Promise<T | undefined>;

	start(): Promise<void>;
	stop(): Promise<void>;
}

export type IPeerbitKeychain = CryptoKeychain & ILibp2pKeychain;
