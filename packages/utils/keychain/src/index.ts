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
	PublicSignKey,
	toBase64
} from "@peerbit/crypto";
import { type KeyParameters, type Keychain, type KeypairParameters } from "./interface.js";
import { type AnyStore } from "@peerbit/any-store";
import { serialize, deserialize, type Constructor } from "@dao-xyz/borsh";
import { createStore } from "@peerbit/any-store";

export type { Keychain };

export type KeypairFromPublicKey<T> = T extends X25519PublicKey
	? X25519Keypair
	: T extends Ed25519PublicKey
	? Ed25519Keypair
	: T extends Secp256k1PublicKey
	? Secp256k1Keypair
	: T extends PublicSignKey | PublicKeyEncryptionKey
	? Keypair
	: never;

export class DefaultKeychain implements Keychain {
	constructor(
		readonly properties: { store: AnyStore } = { store: createStore() }
	) { }
	async import(
		parameters: (KeypairParameters | KeyParameters) & { id?: Uint8Array }
	): Promise<void> {
		let bytes: Uint8Array;
		let publicKey: Uint8Array | undefined;
		if ((parameters as KeypairParameters).keypair) {
			const kp = (parameters as KeypairParameters).keypair;
			bytes = serialize(kp);
			publicKey = serialize(kp.publicKey);

			if (kp instanceof Ed25519Keypair) {
				// also import as X25519Keypair for convenience
				const xkp = await X25519Keypair.from(kp);
				await this.import({ keypair: xkp });
			}
		} else {
			bytes = serialize((parameters as KeyParameters).key);
		}

		if (parameters.id) {
			await this.properties.store.put(toBase64(parameters.id), bytes);
		}

		if (publicKey) {
			await this.properties.store.put(toBase64(publicKey), bytes);
		}
	}
	async exportByKey<
		T extends
		| X25519PublicKey
		| Ed25519PublicKey
		| Secp256k1PublicKey
		| PublicSignKey
		| PublicKeyEncryptionKey,
		Q = KeypairFromPublicKey<T>
	>(publicKey: T): Promise<Q | undefined> {
		const key = await this.properties.store.get(toBase64(serialize(publicKey)));
		if (key) {
			return deserialize(key, Keypair) as Q;
		}
		return undefined;
	}

	async exportById<
		T = Ed25519Keypair | Secp256k1Keypair | X25519Keypair | ByteKey
	>(id: Uint8Array, type: Constructor<T>): Promise<T | undefined> {
		const key = await this.properties.store.get(toBase64(id));
		if (key) {
			const maybeConvert = (type as any) === X25519Keypair;
			const exported = deserialize(
				key,
				maybeConvert ? (Keypair as Constructor<T>) : type
			);

			if (maybeConvert && exported instanceof Ed25519Keypair) {
				return X25519Keypair.from(exported) as T;
			}
			return exported as T;
		}
		return undefined;
	}

	async start() {
		await this.properties.store.open();
	}

	async stop() {
		await this.properties.store.close();
	}
}
