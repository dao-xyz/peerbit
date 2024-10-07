import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { type PeerId } from "@libp2p/interface";
import sodium from "libsodium-wrappers";
import { compare } from "uint8arrays";
import { Ed25519Keypair, Ed25519PublicKey } from "./ed25519.js";
import {
	Keypair,
	PrivateEncryptionKey,
	PublicKeyEncryptionKey,
} from "./key.js";
import { toHexString } from "./utils.js";

export * from "./errors.js";

@variant(0)
export class X25519PublicKey extends PublicKeyEncryptionKey {
	@field({ type: fixedArray("u8", 32) })
	publicKey: Uint8Array;

	constructor(properties: { publicKey: Uint8Array }) {
		super();
		if (properties.publicKey.length !== 32) {
			throw new Error("Expecting key to have length 32");
		}
		this.publicKey = properties.publicKey;
	}

	equals(other: PublicKeyEncryptionKey): boolean {
		if (other instanceof X25519PublicKey) {
			return compare(this.publicKey, other.publicKey) === 0;
		}
		return false;
	}

	toString(): string {
		return "x25519p/" + toHexString(this.publicKey);
	}

	static async from(
		ed25119PublicKey: Ed25519PublicKey,
	): Promise<X25519PublicKey> {
		await sodium.ready;
		return new X25519PublicKey({
			publicKey: sodium.crypto_sign_ed25519_pk_to_curve25519(
				ed25119PublicKey.publicKey,
			),
		});
	}

	static async fromPeerId(peerId: PeerId): Promise<X25519PublicKey> {
		await sodium.ready;
		const ed = Ed25519PublicKey.fromPeerId(peerId);
		return X25519PublicKey.from(ed);
	}

	static async create(): Promise<X25519PublicKey> {
		await sodium.ready;
		return new X25519PublicKey({
			publicKey: sodium.crypto_box_keypair().publicKey,
		});
	}
}

@variant(0)
export class X25519SecretKey extends PrivateEncryptionKey {
	@field({ type: fixedArray("u8", 32) })
	secretKey: Uint8Array;

	constructor(properties: { secretKey: Uint8Array }) {
		super();
		if (properties.secretKey.length !== 32) {
			throw new Error("Expecting key to have length 32");
		}
		this.secretKey = properties.secretKey;
	}

	equals(other: PublicKeyEncryptionKey): boolean {
		if (other instanceof X25519SecretKey) {
			return compare(this.secretKey, other.secretKey) === 0;
		}
		return false;
	}

	toString(): string {
		return "x25519s" + toHexString(this.secretKey);
	}

	async publicKey(): Promise<X25519PublicKey> {
		return new X25519PublicKey({
			publicKey: sodium.crypto_scalarmult_base(this.secretKey),
		});
	}

	static async from(ed25119Keypair: Ed25519Keypair): Promise<X25519SecretKey> {
		await sodium.ready;
		return new X25519SecretKey({
			secretKey: sodium.crypto_sign_ed25519_sk_to_curve25519(
				ed25119Keypair.privateKeyPublicKey,
			),
		});
	}

	static async create(): Promise<X25519SecretKey> {
		await sodium.ready;
		return new X25519SecretKey({
			secretKey: sodium.crypto_box_keypair().privateKey,
		});
	}
}

@variant(1)
export class X25519Keypair extends Keypair {
	@field({ type: X25519PublicKey })
	publicKey: X25519PublicKey;

	@field({ type: X25519SecretKey })
	secretKey: X25519SecretKey;

	constructor(properties: {
		publicKey: X25519PublicKey;
		secretKey: X25519SecretKey;
	}) {
		super();
		this.publicKey = properties.publicKey;
		this.secretKey = properties.secretKey;
	}

	static async create(): Promise<X25519Keypair> {
		await sodium.ready;
		const generated = sodium.crypto_box_keypair();
		const kp = new X25519Keypair({
			publicKey: new X25519PublicKey({
				publicKey: generated.publicKey,
			}),
			secretKey: new X25519SecretKey({
				secretKey: generated.privateKey,
			}),
		});

		return kp;
	}

	static async from(ed25119Keypair: Ed25519Keypair): Promise<X25519Keypair> {
		const kp = new X25519Keypair({
			publicKey: await X25519PublicKey.from(ed25119Keypair.publicKey),
			secretKey: await X25519SecretKey.from(ed25119Keypair),
		});
		return kp;
	}

	equals(other: Keypair) {
		if (other instanceof X25519Keypair) {
			return (
				this.publicKey.equals(other.publicKey) &&
				this.secretKey.equals(other.secretKey)
			);
		}
		return false;
	}
}
