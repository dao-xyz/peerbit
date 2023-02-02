import { Ed25519Keypair, Ed25519PublicKey } from "./ed25519.js";
import { sha256 } from "./hash.js";
import sodium from "libsodium-wrappers";
import { PreHash, prehashFn } from "./prehash.js";
import { SignatureWithKey } from "./signature.js";

export const sign = async (
	data: Uint8Array,
	keypair: Ed25519Keypair,
	prehash: PreHash
) => {
	const hashedData = await prehashFn(data, prehash);

	return new SignatureWithKey({
		prehash,
		publicKey: keypair.publicKey,
		signature: sodium.crypto_sign_detached(
			hashedData,
			keypair.privateKey.privateKey
		),
	});
};

export const verifySignatureEd25519 = async (
	data: Uint8Array,
	signature: SignatureWithKey
) => {
	let res = false;
	try {
		const hashedData = await prehashFn(data, signature.prehash);
		const verified = sodium.crypto_sign_verify_detached(
			signature.signature,
			hashedData,
			(signature.publicKey as Ed25519PublicKey).publicKey
		);
		res = verified;
	} catch (error) {
		return false;
	}
	return res;
};
