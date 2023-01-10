import { field, variant } from "@dao-xyz/borsh";
import { PrivateSignKey, PublicSignKey, Keypair } from "./key.js";
import { arraysCompare, fixedUint8Array } from "@dao-xyz/peerbit-borsh-utils";
import sodium from "libsodium-wrappers";
import { Signer, SignWithKey } from "./signer.js";
import { SignatureWithKey } from "./signature.js";
import { toHexString } from "./utils.js";
import { peerIdFromKeys } from "@libp2p/peer-id";
import { supportedKeys } from "@libp2p/crypto/keys";
import { coerce } from "./bytes.js";
import { PeerId } from "@libp2p/interface-peer-id";
import crypto from 'crypto'
await sodium.ready;

/* 
 TODO add native crypto sign support
const signFn = (): (data: Uint8Array, privateKey: Ed25519PrivateKey) => Uint8Array => {
	if ((globalThis as any).Buffer) {
		return (globalThis as any).Buffer.allocUnsafe
	}
	return (len) => new Uint8Array(len);
}
const allocUnsafe = allocUnsafeFn();

const alg = { name: 'Ed25519' };
crypto.sign(alg, new Uint8Array([1, 2, 3]), new Uint8Array([1, 2, 3])) 
*/

const sign = (
	data: Uint8Array,
	privateKey: Ed25519PrivateKey,
	signedHash = false
) => {
	const signedData = signedHash ? sodium.crypto_generichash(32, data) : data;
	const signature = sodium.crypto_sign_detached(
		signedData,
		privateKey.privateKey
	);
	return signature;
};

@variant(0)
export class Ed25519PublicKey extends PublicSignKey {
	@field({ type: fixedUint8Array(32) })
	publicKey: Uint8Array;

	constructor(properties?: { publicKey: Uint8Array }) {
		super();
		if (properties) {
			this.publicKey = properties.publicKey;
		}
	}

	equals(other: PublicSignKey): boolean {
		if (other instanceof Ed25519PublicKey) {
			return arraysCompare(this.publicKey, other.publicKey) === 0;
		}
		return false;
	}
	toString(): string {
		return "ed25119p/" + toHexString(this.publicKey);
	}

	toPeerId() {
		return peerIdFromKeys(
			new supportedKeys["ed25519"].Ed25519PublicKey(this.publicKey).bytes
		);
	}
	static from(id: PeerId) {
		if (!id.publicKey) {
			throw new Error("Missing public key");
		}
		if (id.type === "Ed25519") {
			return new Ed25519PublicKey({
				publicKey: coerce(id.publicKey!.slice(4)),
			});
		}
		throw new Error("Unsupported key type: " + id.type);
	}
}

@variant(0)
export class Ed25519PrivateKey extends PrivateSignKey {
	@field({ type: fixedUint8Array(64) })
	privateKey: Uint8Array;


	constructor(properties?: { privateKey: Uint8Array }) {
		super();
		if (properties) {
			this.privateKey = properties.privateKey;
		}
	}

	equals(other: PublicSignKey): boolean {
		if (other instanceof Ed25519PrivateKey) {
			return arraysCompare(this.privateKey, other.privateKey) === 0;
		}
		return false;
	}

	toString(): string {
		return "ed25119s/" + toHexString(this.privateKey);
	}

	static from(id: PeerId) {
		if (!id.privateKey) {
			throw new Error("Missing privateKey key");
		}
		if (id.type === "Ed25519") {
			return new Ed25519PrivateKey({
				privateKey: coerce(id.privateKey!.slice(4)),
			});
		}
		throw new Error("Unsupported key type: " + id.type);
	}
}

@variant(0)
export class Ed25519Keypair extends Keypair implements Signer {
	@field({ type: Ed25519PublicKey })
	publicKey: Ed25519PublicKey;

	@field({ type: Ed25519PrivateKey })
	privateKey: Ed25519PrivateKey;

	constructor(properties?: {
		publicKey: Ed25519PublicKey;
		privateKey: Ed25519PrivateKey;
	}) {
		super();
		if (properties) {
			this.privateKey = properties.privateKey;
			this.publicKey = properties.publicKey;
		}
	}

	static create(): Ed25519Keypair {
		const generated = sodium.crypto_sign_keypair();
		const kp = new Ed25519Keypair();
		kp.publicKey = new Ed25519PublicKey({
			publicKey: generated.publicKey,
		});
		kp.privateKey = new Ed25519PrivateKey({
			privateKey: generated.privateKey,
		});
		return kp;
	}

	sign(data: Uint8Array): Uint8Array {
		return sign(data, this.privateKey);
	}

	signer(): SignWithKey {
		return async (data: Uint8Array) => {
			return new SignatureWithKey({
				publicKey: this.publicKey,
				signature: await this.sign(data),
			});
		};
	}

	equals(other: Keypair) {
		if (other instanceof Ed25519Keypair) {
			return (
				this.publicKey.equals(other.publicKey) &&
				this.privateKey.equals(other.privateKey)
			);
		}
		return false;
	}

	static from(peerId: PeerId) {
		return new Ed25519Keypair({
			privateKey: Ed25519PrivateKey.from(peerId),
			publicKey: Ed25519PublicKey.from(peerId)
		})
	}
}


export const verifySignatureEd25519 = (
	signature: Uint8Array,
	publicKey: Ed25519PublicKey | Uint8Array,
	data: Uint8Array,
	signedHash = false
) => {
	let res = false;
	try {
		const hashedData = signedHash ? crypto.createHash('sha256').update(data).digest() : data;
		const verified = sodium.crypto_sign_verify_detached(
			signature,
			hashedData,
			publicKey instanceof Ed25519PublicKey
				? publicKey.publicKey
				: publicKey
		);
		res = verified;
	} catch (error) {
		return false;
	}
	return res;
};
