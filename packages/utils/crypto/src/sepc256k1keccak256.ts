import { field, variant } from "@dao-xyz/borsh";
import { Keypair, PrivateSignKey, PublicSignKey } from "./key.js";
import { verifyMessage, Wallet } from "@ethersproject/wallet";
import { joinSignature, arrayify } from "@ethersproject/bytes";
import { hashMessage } from "@ethersproject/hash";

import { arraysCompare, arraysEqual, fixedUint8Array } from "@dao-xyz/peerbit-borsh-utils";
import { fromHexString, toHexString } from "./utils.js";
import { computeAddress } from "@ethersproject/transactions";
import { PeerId } from "@libp2p/interface-peer-id";
import crypto from 'crypto';
import { Signer } from "./signer.js";
import { coerce } from "./bytes.js";
import { generateKeyPair } from '@libp2p/crypto/keys'
import utf8 from '@protobufjs/utf8'

@variant(1)
export class Secp256k1Keccak256PublicKey extends PublicSignKey {

	@field({ type: fixedUint8Array(20) })
	address: Uint8Array; // keccak256, we do this because we want to be able to use web wallets

	constructor(properties: { address: string }) {
		super();
		this.address = fromHexString(properties.address.startsWith("0x") ? properties.address.slice(2) : properties.address);
	}

	equals(other: PublicSignKey): boolean {
		if (other instanceof Secp256k1Keccak256PublicKey) {
			return this.address === other.address;
		}
		return false;
	}

	toString(): string {
		return "sepc256k1/" + new TextDecoder().decode(this.address);
	}

	static from(id: PeerId) {
		if (!id.publicKey) {
			throw new Error("Missing public key");
		}
		if (id.type === "secp256k1") {
			return new Secp256k1Keccak256PublicKey({
				address: computeAddress(id.publicKey!.slice(4)),
			});
		}
		throw new Error("Unsupported key type: " + id.type);
	}
}

@variant(1)
export class Secp256k1Keccak256PrivateKey extends PrivateSignKey {

	@field({ type: Uint8Array })
	privateKey: Uint8Array;

	constructor(properties: { privateKey: Uint8Array }) {
		super();
		this.privateKey = properties.privateKey;
	}

	equals(other: Secp256k1Keccak256PrivateKey): boolean {
		if (other instanceof Secp256k1Keccak256PrivateKey) {
			return arraysCompare(this.privateKey, other.privateKey) === 0;
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
		if (id.type === 'secp256k1') {
			return new Secp256k1Keccak256PrivateKey({
				privateKey: coerce(id.privateKey!.slice(4)),
			});
		}
		throw new Error("Unsupported key type: " + id.type);
	}
}


@variant(2)
export class Sec256k1Keccak256Keypair extends Keypair implements Signer {
	@field({ type: Secp256k1Keccak256PublicKey })
	publicKey: Secp256k1Keccak256PublicKey;

	@field({ type: Secp256k1Keccak256PrivateKey })
	privateKey: Secp256k1Keccak256PrivateKey;

	_wallet: Wallet;
	constructor(properties: {
		publicKey: Secp256k1Keccak256PublicKey;
		privateKey: Secp256k1Keccak256PrivateKey;
	}) {
		super();
		if (properties) {
			this.privateKey = properties.privateKey;
			this.publicKey = properties.publicKey;
		}
	}

	static async create(): Promise<Sec256k1Keccak256Keypair> {
		const generated = await generateKeyPair('secp256k1');
		const kp = new Sec256k1Keccak256Keypair({
			publicKey: new Secp256k1Keccak256PublicKey({
				address: computeAddress(generated.public.bytes),
			}),
			privateKey: new Secp256k1Keccak256PrivateKey({
				privateKey: generated.bytes,
			})
		});

		return kp;
	}


	sign(data: Uint8Array): Uint8Array {
		const signature = joinSignature((this._wallet || (this._wallet = new Wallet(this.privateKey.privateKey)))._signingKey().signDigest(hashMessage(data)));
		const ret = new Uint8Array(utf8.length(signature));
		utf8.write(signature, ret, 0)
		return ret;
	}


	equals(other: Keypair) {
		if (other instanceof Sec256k1Keccak256Keypair) {
			return (
				this.publicKey.equals(other.publicKey) &&
				this.privateKey.equals(other.privateKey)
			);
		}
		return false;
	}

	static from(peerId: PeerId) {
		return new Sec256k1Keccak256Keypair({
			privateKey: Secp256k1Keccak256PrivateKey.from(peerId),
			publicKey: Secp256k1Keccak256PublicKey.from(peerId)
		})
	}
}


const decoder = new TextDecoder();

export const verifySignatureSecp256k1 = (
	signature: Uint8Array,
	publicKey: Secp256k1Keccak256PublicKey,
	data: Uint8Array,
	signedHash = false
): boolean => {
	const hashedData = signedHash ? crypto.createHash('sha256').update(data).digest() : data;
	const signerAddress = verifyMessage(hashedData, decoder.decode(signature));
	return arraysEqual(
		fromHexString(signerAddress.slice(2)),
		publicKey.address
	);
};
