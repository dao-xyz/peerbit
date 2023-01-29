import { Ed25519PrivateKey, Ed25519PublicKey } from "./ed25519.js";
import { sha256 } from "./hash.js";
import sodium from "libsodium-wrappers";
import crypto from "crypto";

export const sign = async (
	data: Uint8Array,
	privateKey: Ed25519PrivateKey,
	signedHash = false
) => {
	const signedData = signedHash ? await sha256(data) : data;

	if (!privateKey.keyObject) {
		privateKey.keyObject = crypto.createPrivateKey({
			format: "der",
			type: "pkcs8",
			key: toDER(privateKey.privateKey, true),
		});
	}
	return crypto.sign(null, signedData, privateKey.keyObject);
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

		/* 	return crypto.verify(null, hashedData, publicKey.keyObject, signature); */
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

function toDER(key: Uint8Array, p = false) {
	if (p) {
		return Buffer.concat([
			Buffer.from([48, 46, 2, 1, 0, 48, 5, 6, 3, 43, 101, 112, 4, 34, 4, 32]),
			key,
		]);
	}

	// Ed25519's OID
	const oid = Buffer.from([0x06, 0x03, 0x2b, 0x65, 0x70]);

	// Create a byte sequence containing the OID and key
	const elements = Buffer.concat([
		Buffer.concat([
			Buffer.from([0x30]), // Sequence tag
			Buffer.from([oid.length]),
			oid,
		]),
		Buffer.concat([
			Buffer.from([0x03]), // Bit tag
			Buffer.from([key.length + 1]),
			Buffer.from([0x00]), // Zero bit
			key,
		]),
	]);

	// Wrap up by creating a sequence of elements
	const der = Buffer.concat([
		Buffer.from([0x30]), // Sequence tag
		Buffer.from([elements.length]),
		elements,
	]);

	return der;
}
