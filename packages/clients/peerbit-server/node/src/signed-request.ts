import {
	type Identity,
	PublicSignKey,
	SignatureWithKey,
	fromBase64,
	toBase64,
	verify,
	Ed25519PublicKey
} from "@peerbit/crypto";
import { deserialize, serialize, BinaryWriter } from "@dao-xyz/borsh";
import http from "http";

const SIGNATURE_KEY = "X-Peerbit-Signature";
const SIGNATURE_TIME_KEY = "X-Peerbit-Signature-Time";

export const signRequest = async (
	headers: Record<string, string>,
	method: string,
	path: string,
	data: string | undefined,
	keypair: Identity<Ed25519PublicKey>
) => {
	const sigTimestamp = Math.round(new Date().getTime() / 1000).toString();
	const write = new BinaryWriter();
	if (!method) {
		throw new Error("Expecting method");
	}
	if (!path) {
		throw new Error("Expecting path");
	}

	write.string(
		method.toLowerCase() + path.toLowerCase() + sigTimestamp + (data || "")
	);
	const signature = await keypair.sign(write.finalize());
	headers[SIGNATURE_TIME_KEY] = sigTimestamp;
	headers[SIGNATURE_KEY] = toBase64(serialize(signature));
};

export const getBody = (req: http.IncomingMessage): Promise<string> => {
	return new Promise((resolve, reject) => {
		let body = "";
		req.on("data", (d) => {
			body += d;
		});
		req.on("end", () => {
			resolve(body);
		});
		req.on("error", (e) => reject(e));
	});
};

export const verifyRequest = async (
	headers: Record<string, string | string[] | undefined>,
	method: string,
	path: string,
	body = ""
): Promise<PublicSignKey> => {
	const timestamp =
		headers[SIGNATURE_TIME_KEY] || headers[SIGNATURE_TIME_KEY.toLowerCase()];
	if (typeof timestamp !== "string") {
		throw new Error("Unexpected timestamp type: " + typeof timestamp);
	}

	const write = new BinaryWriter();
	if (!method) {
		throw new Error("Expecting method");
	}
	if (!path) {
		throw new Error("Expecting path");
	}

	write.string(method.toLowerCase() + path.toLowerCase() + timestamp + body);
	const signature =
		headers[SIGNATURE_KEY] || headers[SIGNATURE_KEY.toLowerCase()];
	if (typeof signature !== "string") {
		throw new Error("Unexpected signature type: " + typeof signature);
	}
	const signatureWithKey = deserialize(fromBase64(signature), SignatureWithKey);
	if (await verify(signatureWithKey, write.finalize())) {
		return signatureWithKey.publicKey;
	}
	throw new Error("Invalid signature");
};
