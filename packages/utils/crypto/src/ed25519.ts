import { field, variant } from "@dao-xyz/borsh";
import { PrivateSignKey, PublicSignKey, Keypair } from "./key.js";
import { arraysCompare, fixedUint8Array } from "@dao-xyz/peerbit-borsh-utils";
import { Signer, SignWithKey } from "./signer.js";
import { SignatureWithKey } from "./signature.js";
import { toHexString } from "./utils.js";
import { peerIdFromKeys } from "@libp2p/peer-id";
import { supportedKeys } from "@libp2p/crypto/keys";
import { coerce } from "./bytes.js";
import sodium from "libsodium-wrappers";
import type { Ed25519PeerId, PeerId } from "@libp2p/interface-peer-id";
import { sign } from "./ed25519-sign.js";

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

	toPeerId(): Promise<PeerId> {
		return peerIdFromKeys(
			new supportedKeys["ed25519"].Ed25519PublicKey(this.publicKey).bytes
		);
	}

	/* Don't use keyobject for publicKeys becuse it takes longer time to derive it compare to verifying with sodium
	private keyObject: any;
	get keyObject() {
		return (
			this._keyObject ||
			(this._keyObject = crypto.createPublicKey({
				format: "der",
				type: "spki",
				key: toDER(this.publicKey),
			}))
		);
	} */
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

	equals(other: Ed25519PrivateKey): boolean {
		if (other instanceof Ed25519PrivateKey) {
			return arraysCompare(this.privateKey, other.privateKey) === 0;
		}
		return false;
	}

	toString(): string {
		return "ed25119s/" + toHexString(this.privateKey);
	}

	keyObject: any; // crypto.KeyObject;

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

	static async create(): Promise<Ed25519Keypair> {
		await sodium.ready;
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

	sign(data: Uint8Array, hash = false): Promise<Uint8Array> {
		return sign(data, this.privateKey, hash);
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

	static from(peerId: PeerId | Ed25519PeerId) {
		return new Ed25519Keypair({
			privateKey: Ed25519PrivateKey.from(peerId),
			publicKey: Ed25519PublicKey.from(peerId),
		});
	}
}
