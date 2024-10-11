import { field, fixedArray, variant } from "@dao-xyz/borsh";
import {
	type SignatureLike,
	arrayify,
	joinSignature,
	splitSignature,
} from "@ethersproject/bytes";
import { Wallet } from "@ethersproject/wallet";
import { generateKeyPair, publicKeyFromRaw } from "@libp2p/crypto/keys";
import { type PeerId } from "@libp2p/interface";
import { peerIdFromPublicKey } from "@libp2p/peer-id";
import utf8 from "@protobufjs/utf8";
import _ec from "elliptic";
import { equals } from "uint8arrays";
import { Keypair, PrivateSignKey, PublicSignKey } from "./key.js";
import { PreHash, prehashFn } from "./prehash.js";
import { SignatureWithKey } from "./signature.js";
import { type Identity } from "./signer.js";
import { toHexString } from "./utils.js";

import EC = _ec.ec;
let _curve: EC;

@variant(1)
export class Secp256k1PublicKey extends PublicSignKey {
	@field({ type: fixedArray("u8", 33) })
	publicKey: Uint8Array;

	constructor(properties: { publicKey: Uint8Array }) {
		super();
		if (properties.publicKey.length !== 33) {
			throw new Error("Expecting key to have length 33");
		}
		this.publicKey = properties.publicKey;
	}

	static async recover(wallet: {
		signMessage(message: string | Uint8Array): Promise<string> | string;
	}) {
		// Signa message
		const toSign = new Uint8Array([0]);
		const signature = await wallet.signMessage(toSign);

		// So we can recover the public key
		const publicKey = recoverPublicKeyFromSignature(
			await prehashFn(toSign, PreHash.ETH_KECCAK_256),
			signature,
		);

		return new Secp256k1PublicKey({ publicKey });
	}

	equals(other: PublicSignKey): boolean {
		if (other instanceof Secp256k1PublicKey) {
			return equals(this.publicKey, other.publicKey);
		}
		return false;
	}

	toString(): string {
		return "sepc256k1/" + toHexString(this.publicKey);
	}

	toPeerId(): PeerId {
		return peerIdFromPublicKey(publicKeyFromRaw(this.publicKey));
	}

	static fromPeerId(id: PeerId) {
		if (!id.publicKey) {
			throw new Error("Missing public key");
		}
		if (id.type === "secp256k1") {
			return new Secp256k1PublicKey({
				publicKey: id.publicKey.raw,
			});
		}
		throw new Error("Unsupported key type: " + id.type);
	}
}

@variant(1)
export class Secp256k1PrivateKey extends PrivateSignKey {
	@field({ type: Uint8Array })
	privateKey: Uint8Array;

	constructor(properties: { privateKey: Uint8Array }) {
		super();
		if (properties.privateKey.length !== 32) {
			throw new Error("Expecting key to have length 32");
		}

		this.privateKey = properties.privateKey;
	}

	equals(other: Secp256k1PrivateKey): boolean {
		if (other instanceof Secp256k1PrivateKey) {
			return equals(this.privateKey, other.privateKey);
		}
		return false;
	}

	toString(): string {
		return "secp256k1s/" + toHexString(this.privateKey);
	}
}

@variant(2)
export class Secp256k1Keypair extends Keypair implements Identity {
	@field({ type: Secp256k1PublicKey })
	publicKey: Secp256k1PublicKey;

	@field({ type: Secp256k1PrivateKey })
	privateKey: Secp256k1PrivateKey;

	_wallet!: Wallet;
	constructor(properties: {
		publicKey: Secp256k1PublicKey;
		privateKey: Secp256k1PrivateKey;
	}) {
		super();
		this.privateKey = properties.privateKey;
		this.publicKey = properties.publicKey;
	}

	static async create(): Promise<Secp256k1Keypair> {
		const generated = await generateKeyPair("secp256k1");
		const kp = new Secp256k1Keypair({
			publicKey: new Secp256k1PublicKey({
				publicKey: generated.publicKey.raw,
			}),
			privateKey: new Secp256k1PrivateKey({
				privateKey: generated.raw,
			}),
		});

		return kp;
	}

	async sign(
		data: Uint8Array,
		prehash: PreHash = PreHash.ETH_KECCAK_256,
	): Promise<SignatureWithKey> {
		const maybeHashed = await prehashFn(data, prehash);

		const signature = joinSignature(
			(this._wallet || (this._wallet = new Wallet(this.privateKey.privateKey)))
				._signingKey()
				.signDigest(maybeHashed),
		);
		const signatureBytes = new Uint8Array(utf8.length(signature)); // TODO utilize Buffer allocUnsafe
		utf8.write(signature, signatureBytes, 0);

		return new SignatureWithKey({
			prehash,
			publicKey: this.publicKey,
			signature: signatureBytes,
		});
	}

	equals(other: Keypair) {
		if (other instanceof Secp256k1Keypair) {
			return (
				this.publicKey.equals(other.publicKey) &&
				this.privateKey.equals(other.privateKey)
			);
		}
		return false;
	}

	toPeerId(): PeerId {
		return peerIdFromPublicKey(publicKeyFromRaw(this.publicKey.publicKey));
	}
}

const decoder = new TextDecoder();

function getCurve() {
	if (!_curve) {
		_curve = new EC("secp256k1");
	}
	return _curve;
}

export const recoverPublicKeyFromSignature = (
	digest: Uint8Array,
	signature: SignatureLike,
): Uint8Array => {
	const sig = splitSignature(signature);
	const rs = { r: arrayify(sig.r), s: arrayify(sig.s) };
	return new Uint8Array(
		getCurve()
			.recoverPubKey(arrayify(digest), rs, sig.recoveryParam)
			.encodeCompressed(),
	);
};

export const verifySignatureSecp256k1 = async (
	signature: SignatureWithKey,
	data: Uint8Array,
): Promise<boolean> => {
	const hashedData = await prehashFn(data, signature.prehash);
	const signerKey = recoverPublicKeyFromSignature(
		arrayify(hashedData),
		decoder.decode(signature.signature),
	);
	return equals(
		signerKey,
		(signature.publicKey as Secp256k1PublicKey).publicKey,
	);
};
