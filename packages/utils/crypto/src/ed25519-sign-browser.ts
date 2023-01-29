import { Ed25519PrivateKey, Ed25519PublicKey } from "./ed25519.js";
import { sha256 } from "./hash.js";
import sodium from "libsodium-wrappers";

export const sign = async (
	data: Uint8Array,
	privateKey: Ed25519PrivateKey,
	signedHash = false
) => {
	const signedData = signedHash ? await sha256(data) : data;
	return sodium.crypto_sign_detached(signedData, privateKey.privateKey);
};

export const verifySignatureEd25519 = async (
	signature: Uint8Array,
	publicKey: Ed25519PublicKey,
	data: Uint8Array,
	signedHash = false
) => {
	let res = false;
	try {
		const hashedData = signedHash ? await sha256(data) : data;
		const verified = sodium.crypto_sign_verify_detached(
			signature,
			hashedData,
			publicKey instanceof Ed25519PublicKey ? publicKey.publicKey : publicKey
		);
		res = verified;
	} catch (error) {
		return false;
	}
	return res;
};
