import { field, fixedArray, variant, vec } from "@dao-xyz/borsh";
import { Keypair, PrivateSignKey, PublicSignKey } from "./key.js";
import { Wallet } from "@ethersproject/wallet";
import { arrayify } from "@ethersproject/bytes";
import { joinSignature } from "@ethersproject/bytes";
import { SignatureLike, splitSignature } from "@ethersproject/bytes";
import _ec from "elliptic";
import EC = _ec.ec;
let _curve: EC;

import { equals } from "@peerbit/uint8arrays";
import { toHexString } from "./utils.js";
import { PeerId } from "@libp2p/interface/peer-id";
import { Identity, Signer } from "./signer.js";
import { coerce } from "./bytes.js";
import { generateKeyPair, supportedKeys } from "@libp2p/crypto/keys";
import utf8 from "@protobufjs/utf8";
import { SignatureWithKey } from "./signature.js";
import { PreHash, prehashFn } from "./prehash.js";
import { peerIdFromKeys } from "@libp2p/peer-id";

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

	static async recover(wallet: Wallet) {
		// Signa message
		const toSign = new Uint8Array([0]);
		const signature = await wallet.signMessage(toSign);

		// So we can recover the public key
		const publicKey = recoverPublicKeyFromSignature(
			await prehashFn(toSign, PreHash.ETH_KECCAK_256),
			signature
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

	static from(id: PeerId) {
		if (!id.publicKey) {
			throw new Error("Missing public key");
		}
		if (id.type === "secp256k1") {
			return new Secp256k1PublicKey({
				publicKey: id.publicKey.slice(4), // computeAddress(!.slice(4)),
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

	static from(id: PeerId) {
		if (!id.privateKey) {
			throw new Error("Missing privateKey key");
		}
		if (id.type === "secp256k1") {
			return new Secp256k1PrivateKey({
				privateKey: coerce(id.privateKey!.slice(4)),
			});
		}
		throw new Error("Unsupported key type: " + id.type);
	}
}

@variant(2)
export class Secp256k1Keypair extends Keypair implements Identity {
	@field({ type: Secp256k1PublicKey })
	publicKey: Secp256k1PublicKey;

	@field({ type: Secp256k1PrivateKey })
	privateKey: Secp256k1PrivateKey;

	_wallet: Wallet;
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
				publicKey: generated.public.marshal(),
			}),
			privateKey: new Secp256k1PrivateKey({
				privateKey: generated.marshal(),
			}),
		});

		return kp;
	}

	async sign(
		data: Uint8Array,
		prehash: PreHash = PreHash.ETH_KECCAK_256
	): Promise<SignatureWithKey> {
		const maybeHashed = await prehashFn(data, prehash);

		const signature = joinSignature(
			(this._wallet || (this._wallet = new Wallet(this.privateKey.privateKey)))
				._signingKey()
				.signDigest(maybeHashed)
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

	static fromPeerId(peerId: PeerId) {
		return new Secp256k1Keypair({
			privateKey: Secp256k1PrivateKey.from(peerId),
			publicKey: Secp256k1PublicKey.from(peerId),
		});
	}

	toPeerId(): Promise<PeerId> {
		return peerIdFromKeys(
			new supportedKeys["secp256k1"].Secp256k1PublicKey(
				this.publicKey.publicKey
			).bytes,
			new supportedKeys["secp256k1"].Secp256k1PrivateKey(
				this.privateKey.privateKey,
				this.publicKey.publicKey
			).bytes
		);
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
	signature: SignatureLike
): Uint8Array => {
	const sig = splitSignature(signature);
	const rs = { r: arrayify(sig.r), s: arrayify(sig.s) };
	return new Uint8Array(
		getCurve()
			.recoverPubKey(arrayify(digest), rs, sig.recoveryParam)
			.encodeCompressed()
	);
};

export const verifySignatureSecp256k1 = async (
	signature: SignatureWithKey,
	data: Uint8Array
): Promise<boolean> => {
	const hashedData = await prehashFn(data, signature.prehash);
	const signerKey = recoverPublicKeyFromSignature(
		arrayify(hashedData),
		decoder.decode(signature.signature)
	);
	return equals(
		signerKey,
		(signature.publicKey as Secp256k1PublicKey).publicKey
	);
};
