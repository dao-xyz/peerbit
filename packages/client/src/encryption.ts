import {
	Ed25519Keypair,
	Ed25519PublicKey,
	X25519PublicKey,
	X25519SecretKey,
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

export type StorePublicKeyEncryption = (
	replicationTopic: string
) => PublicKeyEncryptionResolver;

export const defaultKeypair = async (
	keychain: KeyChain
): Promise<X25519Keypair> => {
	try {
		const defaultEncryptionKeyPeerId = await keychain.exportPeerId("default");
		if (defaultEncryptionKeyPeerId.type !== "Ed25519") {
			throw new Error("Expected default key to be of type Ed25519");
		}

		return X25519Keypair.fromPeerId(defaultEncryptionKeyPeerId);
		// TODO key rotation
	} catch (error) {
		await keychain.createKey("default", "Ed25519");
		return X25519Keypair.fromPeerId(await keychain.exportPeerId("default"));
	}
};

export const keychainKeyIdFromPublicKey = (publicKey: X25519PublicKey) => {
	const bytes = keysPBM.PublicKey.encode({
		Type: keysPBM.KeyType.Ed25519,
		Data: publicKey.publicKey,
	}).subarray();

	const encoding = identity.digest(bytes);
	return base58btc.encode(encoding.bytes).substring(1);
};

export const exportKeypair = async <
	T extends X25519PublicKey | Ed25519PublicKey,
	Q = T extends X25519PublicKey
		? X25519PublicKey extends T
			? X25519Keypair
			: Ed25519Keypair
		: Ed25519Keypair
>(
	keychain: KeyChain,
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

	const peerId = await keychain.exportPeerId(base58btc.encode(publicKey.bytes));
	return (
		publicKey instanceof X25519PublicKey
			? X25519Keypair.fromPeerId(peerId)
			: Ed25519Keypair.fromPeerId(peerId)
	) as Q;
};

export const importKeypair = async (
	keychain: KeyChain,
	keypair: Ed25519Keypair
) => {
	const receiverKeyPeerId = await keypair.toPeerId();

	// import as ed
	await keychain.importPeer(
		base58btc.encode(keypair.publicKey.bytes),
		receiverKeyPeerId
	);

	// import as x so we can decrypt messages with this public key (if recieved any)
	await keychain.importPeer(
		base58btc.encode((await X25519Keypair.from(keypair)).publicKey.bytes),
		receiverKeyPeerId
	);
};

export const encryptionWithRequestKey = async (
	keystore: KeyChain
): Promise<PublicKeyEncryptionResolver> => {
	const defaultEncryptionKey = await defaultKeypair(keystore);
	return {
		getAnyKeypair: async (publicKeys) => {
			for (let i = 0; i < publicKeys.length; i++) {
				try {
					const key = await exportKeypair(keystore, publicKeys[i]);
					if (key) {
						return {
							index: i,
							keypair: key,
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
		},

		getEncryptionKeypair: () => {
			return defaultEncryptionKey as X25519Keypair;
		},
	};
};
