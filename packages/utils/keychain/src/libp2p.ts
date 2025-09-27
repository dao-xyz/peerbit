import type { Constructor } from "@dao-xyz/borsh";
import type { PrivateKey } from "@libp2p/interface";
import {
	type Keychain as ILibp2pKeychain,
	type KeyInfo,
	type KeychainComponents,
	type KeychainInit,
	keychain as libp2pKeyChain,
} from "@libp2p/keychain";
import { type AnyStore, createStore } from "@peerbit/any-store";
import type {
	ByteKey,
	Ed25519Keypair,
	Ed25519PublicKey,
	PublicKeyEncryptionKey,
	PublicSignKey,
	Secp256k1Keypair,
	Secp256k1PublicKey,
	X25519Keypair,
	X25519PublicKey,
} from "@peerbit/crypto/dist/src";
import { DefaultCryptoKeychain } from "./crypto.js";
import type {
	CryptoKeychain,
	KeyParameters,
	KeypairFromPublicKey,
	KeypairParameters,
} from "./interface.js";

export class PeerbitKeychain implements CryptoKeychain, ILibp2pKeychain {
	private libp2p: ILibp2pKeychain;
	private crypto: CryptoKeychain;

	constructor(
		components: KeychainComponents,
		init: {
			libp2p?: KeychainInit;
			crypto?: { store: AnyStore } | CryptoKeychain;
		},
	) {
		this.libp2p = libp2pKeyChain(init.libp2p)(components);
		this.crypto =
			init.crypto instanceof DefaultCryptoKeychain
				? init.crypto
				: new DefaultCryptoKeychain({ store: createStore() });
	}
	import(
		parameters: (KeypairParameters | KeyParameters) & { id: Uint8Array },
	): Promise<void> {
		return this.crypto.import(parameters);
	}
	exportByKey<
		T extends
			| Ed25519PublicKey
			| X25519PublicKey
			| Secp256k1PublicKey
			| PublicSignKey
			| PublicKeyEncryptionKey,
		Q = KeypairFromPublicKey<T>,
	>(publicKey: T): Promise<Q | undefined> {
		return this.crypto.exportByKey<T, Q>(publicKey);
	}
	exportById<
		T extends Ed25519Keypair | Secp256k1Keypair | X25519Keypair | ByteKey,
	>(id: Uint8Array, type: Constructor<T>): Promise<T | undefined> {
		return this.crypto.exportById<T>(id, type);
	}
	findKeyByName(name: string): Promise<KeyInfo> {
		return this.libp2p.findKeyByName(name);
	}
	findKeyById(id: string): Promise<KeyInfo> {
		return this.libp2p.findKeyById(id);
	}
	importKey(name: string, key: PrivateKey): Promise<KeyInfo> {
		return this.libp2p.importKey(name, key);
	}
	exportKey(name: string): Promise<PrivateKey> {
		return this.libp2p.exportKey(name);
	}
	removeKey(name: string): Promise<KeyInfo> {
		return this.libp2p.removeKey(name);
	}
	renameKey(oldName: string, newName: string): Promise<KeyInfo> {
		return this.libp2p.renameKey(oldName, newName);
	}
	listKeys(): Promise<KeyInfo[]> {
		return this.libp2p.listKeys();
	}
	rotateKeychainPass(oldPass: string, newPass: string): Promise<void> {
		return this.libp2p.rotateKeychainPass(oldPass, newPass);
	}
	async start(): Promise<void> {
		await this.crypto.start();
	}
	async stop(): Promise<void> {
		await this.crypto.stop();
	}
}

export function keychain(
	init: {
		libp2p: KeychainInit;
		crypto?: CryptoKeychain | { store: AnyStore };
	} = { libp2p: {}, crypto: { store: createStore() } },
): (components: KeychainComponents) => PeerbitKeychain {
	return (components: KeychainComponents) => {
		return new PeerbitKeychain(components, init);
	};
}
