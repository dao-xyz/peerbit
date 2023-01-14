export * from "./errors.js";
import { field, variant } from "@dao-xyz/borsh";
import { arraysCompare, fixedUint8Array } from "@dao-xyz/peerbit-borsh-utils";
import sodium from "libsodium-wrappers";
import {
	Keypair,
	PrivateEncryptionKey,
	PublicKeyEncryptionKey,
} from "./key.js";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Ed25519PrivateKey,
} from "./ed25519.js";
import { toHexString } from "./utils.js";
await sodium.ready;

@variant(0)
export class X25519PublicKey extends PublicKeyEncryptionKey {
	@field({ type: fixedUint8Array(32) })
	publicKey: Uint8Array;

	constructor(properties?: { publicKey: Uint8Array }) {
		super();
		if (properties) {
			this.publicKey = properties.publicKey;
		}
	}

	equals(other: PublicKeyEncryptionKey): boolean {
		if (other instanceof X25519PublicKey) {
			return arraysCompare(this.publicKey, other.publicKey) === 0;
		}
		return false;
	}
	toString(): string {
		return "x25519p/" + toHexString(this.publicKey);
	}

	static from(ed25119PublicKey: Ed25519PublicKey): X25519PublicKey {
		return new X25519PublicKey({
			publicKey: sodium.crypto_sign_ed25519_pk_to_curve25519(
				ed25119PublicKey.publicKey
			),
		});
	}

	static create(): X25519PublicKey {
		return new X25519PublicKey({
			publicKey: sodium.crypto_box_keypair().publicKey,
		});
	}
}

@variant(0)
export class X25519SecretKey extends PrivateEncryptionKey {
	@field({ type: fixedUint8Array(32) })
	secretKey: Uint8Array;

	constructor(properties?: { secretKey: Uint8Array }) {
		super();
		if (properties) {
			this.secretKey = properties.secretKey;
		}
	}

	equals(other: PublicKeyEncryptionKey): boolean {
		if (other instanceof X25519SecretKey) {
			return arraysCompare(this.secretKey, (other as X25519SecretKey).secretKey) === 0;
		}
		return false;
	}
	toString(): string {
		return "x25519s" + toHexString(this.secretKey);
	}

	async publicKey(): Promise<X25519PublicKey> {
		await sodium.ready;
		return new X25519PublicKey({
			publicKey: sodium.crypto_scalarmult_base(this.secretKey),
		});
	}
	static from(ed25119SecretKey: Ed25519PrivateKey): X25519SecretKey {
		return new X25519SecretKey({
			secretKey: sodium.crypto_sign_ed25519_sk_to_curve25519(
				ed25119SecretKey.privateKey
			),
		});
	}

	static create(): X25519SecretKey {
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

	static create(): X25519Keypair {
		const generated = sodium.crypto_box_keypair();
		const kp = new X25519Keypair();
		kp.publicKey = new X25519PublicKey({
			publicKey: generated.publicKey,
		});
		kp.secretKey = new X25519SecretKey({
			secretKey: generated.privateKey,
		});
		return kp;
	}

	static from(ed25119Keypair: Ed25519Keypair): X25519Keypair {
		const pk = X25519PublicKey.from(ed25119Keypair.publicKey);
		const sk = X25519SecretKey.from(ed25119Keypair.privateKey);
		const kp = new X25519Keypair();
		kp.publicKey = pk;
		kp.secretKey = sk;
		return kp;
	}

	equals(other: Keypair) {
		if (other instanceof X25519Keypair) {
			return (
				this.publicKey.equals(other.publicKey) &&
				this.secretKey.equals((other as X25519Keypair).secretKey as X25519SecretKey as any)
			);
		}
		return false;
	}
}
