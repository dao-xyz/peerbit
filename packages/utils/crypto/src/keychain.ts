import { KeyChain as InternalKeychain } from "@libp2p/interface/keychain";
import { keysPBM } from "@libp2p/crypto/keys";
import { identity } from "multiformats/hashes/identity";
import { base58btc } from "multiformats/bases/base58";
import { Cache } from "@peerbit/cache";
import { Ed25519Keypair, Ed25519PublicKey } from "./ed25519.js";
import { Keypair, PublicSignKey } from "./key.js";

import { KeyInfo } from "@libp2p/interface/keychain";
import { AccessError, X25519Keypair, X25519PublicKey } from "./x25519.js";

export type KeypairFromPublicKey<T> = T extends X25519PublicKey
	? X25519PublicKey extends T
		? X25519Keypair
		: Ed25519Keypair
	: Ed25519Keypair;

export interface Keychain {
	import(keypair: Ed25519Keypair, id: Uint8Array): Promise<void>;

	exportByKey<
		T extends Ed25519PublicKey | X25519PublicKey,
		Q = KeypairFromPublicKey<T>
	>(
		publicKey: T
	): Promise<Q | undefined>;

	exportById<
		T = "ed25519" | "x25519",
		Q = T extends "ed25519" ? Ed25519Keypair : X25519Keypair
	>(
		id: Uint8Array,
		type: T
	): Promise<Q | undefined>;
}

export class Libp2pKeychain implements Keychain {
	constructor(
		readonly keychain: InternalKeychain,
		readonly options?: { cache?: Cache<X25519Keypair | Ed25519Keypair | null> }
	) {}

	keychainKeyIdFromPublicKey(publicKey: X25519PublicKey) {
		const bytes = keysPBM.PublicKey.encode({
			Type: keysPBM.KeyType.Ed25519,
			Data: publicKey.publicKey
		}).subarray();

		const encoding = identity.digest(bytes);
		return base58btc.encode(encoding.bytes).substring(1);
	}

	private cacheKey(key: Ed25519Keypair | X25519Keypair, id?: Uint8Array) {
		this.options?.cache?.add(base58btc.encode(key.publicKey.bytes), key);
		id && this.options?.cache?.add(base58btc.encode(id), key);
	}

	private getCachedById(id: Uint8Array): Ed25519Keypair | null | undefined {
		const key = base58btc.encode(id instanceof PublicSignKey ? id.bytes : id);
		const cached = this.options?.cache?.get(key);
		if (cached === null) {
			return null;
		} else if (!cached) {
			return undefined;
		} else if (cached instanceof Ed25519Keypair) {
			return cached;
		}
		throw new Error("Unexpected cached keypair type: " + key?.constructor.name);
	}

	private getCachedByKey<
		T extends X25519PublicKey | Ed25519PublicKey,
		Q = KeypairFromPublicKey<T>
	>(publicKey: T): Q | null | undefined {
		const key = base58btc.encode(publicKey.bytes);
		const cached = this.options?.cache?.get(key);
		if (cached === null) {
			return null;
		} else if (!cached) {
			return undefined;
		} else if (cached instanceof Keypair) {
			return cached as Q;
		}
		throw new Error("Unexpected cached keypair type: " + key?.constructor.name);
	}

	exportByKey = async <
		T extends X25519PublicKey | Ed25519PublicKey,
		Q = KeypairFromPublicKey<T>
	>(
		publicKey: T
	): Promise<Q | undefined> => {
		const cached = this.getCachedByKey<T, Q>(publicKey);
		if (cached !== undefined) {
			// if  null, means key is deleted
			return cached ? cached : undefined;
		}

		let keyInfo: KeyInfo | undefined = undefined;
		if (publicKey instanceof Ed25519PublicKey) {
			try {
				keyInfo = await this.keychain.findKeyById(
					(await publicKey.toPeerId()).toString()
				);
			} catch (e: any) {
				if (e.code !== "ERR_KEY_NOT_FOUND") {
					throw e;
				}
			}
		}

		try {
			keyInfo = await this.keychain.findKeyByName(
				base58btc.encode(publicKey.bytes)
			);
		} catch (e: any) {
			if (e.code !== "ERR_KEY_NOT_FOUND") {
				throw e;
			}
		}

		if (!keyInfo) {
			return undefined;
		}

		const peerId = await this.keychain.exportPeerId(keyInfo.name);

		return (
			publicKey instanceof X25519PublicKey
				? X25519Keypair.fromPeerId(peerId)
				: Ed25519Keypair.fromPeerId(peerId)
		) as Q;
	};

	async exportById<
		T = "ed25519" | "x25519",
		Q = T extends "ed25519" ? Ed25519Keypair : X25519Keypair
	>(id: Uint8Array, type: T): Promise<Q | undefined> {
		const cached = this.getCachedById(id) as Ed25519Keypair | undefined | null;
		if (cached !== undefined) {
			// if  null, means key is deleted
			if (type === "x25519" && cached instanceof Ed25519Keypair) {
				return X25519Keypair.from(cached) as Q; // TODO perf, don't do this all the time
			}
			return cached ? (cached as Q) : undefined;
		}
		try {
			const keyInfo = await this.keychain.findKeyByName(base58btc.encode(id));
			const peerId = await this.keychain.exportPeerId(keyInfo.name);
			if (type === "x25519") {
				return X25519Keypair.fromPeerId(peerId) as Q;
			}
			return Ed25519Keypair.fromPeerId(peerId) as Q;
		} catch (e: any) {
			if (e.code !== "ERR_KEY_NOT_FOUND") {
				throw e;
			}
		}
	}

	import = async (keypair: Ed25519Keypair, id: Uint8Array) => {
		const receiverKeyPeerId = await keypair.toPeerId();
		this.cacheKey(keypair, id);

		// import as ed
		await this.keychain.importPeer(base58btc.encode(id), receiverKeyPeerId);

		// import as x so we can decrypt messages with this public key (if received any)
		const xKeypair = await X25519Keypair.from(keypair);
		this.cacheKey(xKeypair);
		await this.keychain.importPeer(
			base58btc.encode(xKeypair.publicKey.bytes),
			receiverKeyPeerId
		);
	};

	// Arrow function is used so we can reference this function and use 'this' without .bind(self)
	getAnyKeypair = async (publicKeys) => {
		for (let i = 0; i < publicKeys.length; i++) {
			try {
				const key = await this.exportByKey(publicKeys[i]);
				if (key && key instanceof X25519Keypair) {
					return {
						index: i,
						keypair: key as X25519Keypair
					};
				}
			} catch (error: any) {
				// Key missing
				if (error.code !== "ERR_NOT_FOUND") {
					throw error;
				}
			}
		}
		throw new AccessError("Failed to access key");
	};
}
