import {
	type AbstractType,
	deserialize,
	field,
	option,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { compare, equals } from "uint8arrays";
import { verifySignatureEd25519 } from "./ed25519-sign.js";
import { Ed25519PublicKey } from "./ed25519.js";
import { PublicSignKey } from "./key.js";
import { PreHash } from "./prehash.js";
import { Secp256k1PublicKey, verifySignatureSecp256k1 } from "./sepc256k1.js";
import { type SignWithKey } from "./signer.js";

@variant(0)
export class SignatureWithKey {
	@field({ type: Uint8Array })
	signature: Uint8Array;

	@field({ type: PublicSignKey })
	publicKey: PublicSignKey;

	@field({ type: "u8" })
	prehash: PreHash = PreHash.NONE; // 0 no prehash, 1 sha256, 2 blake3?

	constructor(props: {
		signature: Uint8Array;
		publicKey: PublicSignKey;
		prehash: PreHash;
	}) {
		this.signature = props.signature;
		this.publicKey = props.publicKey;
		this.prehash = props.prehash;
	}

	equals(other: SignatureWithKey): boolean {
		if (!equals(this.signature, other.signature)) {
			return false;
		}
		return (
			compare(serialize(this.publicKey), serialize(other.publicKey)) === 0 &&
			this.prehash === other.prehash
		);
	}
}

@variant(0)
export class MaybeSigned<T> {
	@field({ type: Uint8Array })
	data: Uint8Array;

	@field({ type: option(SignatureWithKey) })
	signature?: SignatureWithKey;

	constructor(props: {
		data: Uint8Array;
		value?: T;
		signature?: SignatureWithKey;
	}) {
		this.data = props.data;
		this.signature = props.signature;
		this._value = props.value;
	}

	_value?: T;

	getValue(constructor: AbstractType<T>): T {
		return deserialize(this.data, constructor);
	}

	async verify(): Promise<boolean> {
		if (!this.signature) {
			return true;
		}
		return verify(this.signature, this.data);
	}

	equals(other: MaybeSigned<T>): boolean {
		if (!equals(this.data, other.data)) {
			return false;
		}
		if (!this.signature !== !other.signature) {
			return false;
		}
		if (this.signature && other.signature) {
			return this.signature.equals(other.signature);
		}
		return true;
	}

	/*
	 In place signing
	*/
	async sign(signer: SignWithKey): Promise<MaybeSigned<T>> {
		const signatureResult = await signer(this.data);
		this.signature = signatureResult;
		return this;
	}
}

export const verify = async (signature: SignatureWithKey, data: Uint8Array) => {
	if (signature.publicKey instanceof Ed25519PublicKey) {
		return verifySignatureEd25519(signature, data);
	} else if (signature.publicKey instanceof Secp256k1PublicKey) {
		return verifySignatureSecp256k1(signature, data);
	}
	return false;
};
