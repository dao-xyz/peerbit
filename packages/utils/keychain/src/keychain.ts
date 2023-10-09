/**
 * L0 Keychain implementation using AnyStore
 */

import { createStore, AnyStore } from "@peerbit/any-store";
import {
	Keychain as IKeychain,
	KeypairFromPublicKey,
	Keypairs,
	PublicKeys
} from "./interface.js";
import {
	Ed25519Keypair,
	Keypair,
	X25519Keypair,
	Secp256k1Keypair,
	XSalsa20Poly1305,
	ByteKey,
	sha256Base64Sync
} from "@peerbit/crypto";
import { serialize } from "@dao-xyz/borsh";

class Keychain implements IKeychain {
	store: AnyStore;
	constructor(directory?: string | undefined) {
		this.store = createStore(directory);
	}
	import(
		parameters: (
			| { keypair: Keypairs }
			| { key: XSalsa20Poly1305 | ByteKey }
		) & { id?: Uint8Array }
	): Promise<void> {
		let hashcode: string;
		let bytes: Uint8Array;
		if ((parameters as { keypair: Keypairs }).keypair) {
			const kp = (parameters as { keypair: Keypairs }).keypair;
			hashcode = kp.publicKey.hashcode();
			bytes = serialize(kp);
		} else {
			const key = (parameters as { key: XSalsa20Poly1305 | ByteKey }).key;
			hashcode = key.hashcode();
			bytes = serialize(key);
		}
		this.store.put(hashcode, bytes);

		if ((parameters as { id: Uint8Array }).id) {
			this.store.put(
				sha256Base64Sync((parameters as { id: Uint8Array }).id),
				bytes
			);
		}
	}

	exportByPublicKey<T extends PublicKeys, Q = KeypairFromPublicKey<T>>(
		publicKey: T
	): Promise<Q | undefined> {
		// anystore.get by publicKey.hashcode() -> deserialize(bytes, T) -> return
		return this.exportByHash(publicKey.hashcode());
	}
	exportByHash<T extends PublicKeys, Q = KeypairFromPublicKey<T>>(
		hash: string
	): Promise<Q | undefined> {
		// anystore.get by hash -> deserialize(bytes, T) -> return
		throw new Error("Method not implemented.");
	}
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
	>(id: string, type: T): Promise<Q | undefined> {
		// anystore.get by sha256Base64Sync(id) -> deserialize(bytes, T) -> return
		throw new Error("Method not implemented.");
	}
}
