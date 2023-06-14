import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Keypair,
	X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import {
	AccessError,
	PublicKeyEncryptionResolver,
	X25519Keypair,
} from "@dao-xyz/peerbit-crypto";
import { KeyChain } from "@libp2p/interface-keychain";
import { keysPBM } from "@libp2p/crypto/keys";
import { identity } from "multiformats/hashes/identity";
import { base58btc } from "multiformats/bases/base58";
import { Cache } from "@dao-xyz/cache";

import createError from "err-code";

export type StorePublicKeyEncryption = (
	replicationTopic: string
) => PublicKeyEncryptionResolver;

export class FastKeychain implements PublicKeyEncryptionResolver {
	keychainCache: Cache<X25519Keypair | null> = new Cache({ max: 1000 });
	constructor(
		readonly identityKeypair: Ed25519Keypair,
		private defaultEncryptionKeypair: X25519Keypair,
		readonly keychain: KeyChain
	) {}
	static async create(identityKeypair: Ed25519Keypair, keychain: KeyChain) {
		const fk = new FastKeychain(
			identityKeypair,
			await X25519Keypair.from(identityKeypair),
			keychain
		);

		// Import peerId to keychain so it can be easily accessible
		try {
			await fk.importKeypair(fk.identityKeypair);
		} catch (error: any) {
			if (error.code === "ERR_KEY_ALREADY_EXISTS") {
				return fk;
			}
			throw error;
		}

		return fk;
	}

	keychainKeyIdFromPublicKey(publicKey: X25519PublicKey) {
		const bytes = keysPBM.PublicKey.encode({
			Type: keysPBM.KeyType.Ed25519,
			Data: publicKey.publicKey,
		}).subarray();

		const encoding = identity.digest(bytes);
		return base58btc.encode(encoding.bytes).substring(1);
	}

	exportKeypair = async <
		T extends X25519PublicKey | Ed25519PublicKey,
		Q = T extends X25519PublicKey
			? X25519PublicKey extends T
				? X25519Keypair
				: Ed25519Keypair
			: Ed25519Keypair
	>(
		publicKey: T
	): Promise<Q> => {
		/* const id = keychainKeyIdFromPublicKey(publicKey);
		const key = await keychain.findKeyById(id);
		const password = "default-password";
		const pem = await keychain.exportKey(key.name, password);
		const privateKey = await importKey(pem, password);
		return new X25519Keypair({
			publicKey: new X25519PublicKey({ publicKey: privateKey.public.bytes }),
			secretKey: new X25519SecretKey({
				secretKey: privateKey.bytes.slice(0, 32),
			}),
		}); */

		const key = base58btc.encode(publicKey.bytes);
		const cached = this.keychainCache.get(key);
		if (cached === null) {
			throw createError(
				new Error("Key declared null in cache"),
				"ERR_NOT_FOUND"
			);
		} else if (cached instanceof Keypair) {
			return cached as Q;
		}

		const peerId = await this.keychain.exportPeerId(key);
		return (
			publicKey instanceof X25519PublicKey
				? X25519Keypair.fromPeerId(peerId)
				: Ed25519Keypair.fromPeerId(peerId)
		) as Q;
	};

	importKeypair = async (keypair: Ed25519Keypair) => {
		const receiverKeyPeerId = await keypair.toPeerId();

		const edKey = base58btc.encode(keypair.publicKey.bytes);
		const xKeypair = await X25519Keypair.from(keypair);
		const xKey = base58btc.encode(xKeypair.publicKey.bytes);

		this.keychainCache.add(edKey, xKeypair);
		this.keychainCache.add(xKey, xKeypair);

		// import as ed
		await this.keychain.importPeer(edKey, receiverKeyPeerId);

		// import as x so we can decrypt messages with this public key (if recieved any)
		await this.keychain.importPeer(xKey, receiverKeyPeerId);
	};

	// Arrow function is used so we can reference this function and use 'this' without .bind(self)
	getAnyKeypair = async (publicKeys) => {
		for (let i = 0; i < publicKeys.length; i++) {
			try {
				const key = await this.exportKeypair(publicKeys[i]);
				if (key && key instanceof X25519Keypair) {
					return {
						index: i,
						keypair: key as X25519Keypair,
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

	// Arrow function is used so we can reference this function and use 'this' without .bind(self)
	getEncryptionKeypair = () => {
		return this.defaultEncryptionKeypair;
	};
}
