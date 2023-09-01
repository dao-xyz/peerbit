import { sha256 } from "./hash.js";
import sha3 from "js-sha3";
import { toUtf8Bytes } from "@ethersproject/strings";
import { concat } from "uint8arrays";

const messagePrefix = "\x19Ethereum Signed Message:\n";
const ethKeccak256Hash = (message: Uint8Array) =>
	new Uint8Array(
		sha3.keccak256
			.update(
				concat([
					toUtf8Bytes(messagePrefix),
					toUtf8Bytes(String(message.length)),
					message
				])
			)
			.arrayBuffer()
	);

export enum PreHash {
	NONE = 0,
	SHA_256 = 1,
	//BLAKE3 = 2,
	ETH_KECCAK_256 = 3
}

export const prehashFn = (
	data: Uint8Array,
	prehash: PreHash
): Promise<Uint8Array> | Uint8Array => {
	if (prehash === PreHash.NONE) {
		return data;
	}
	if (prehash === PreHash.SHA_256) {
		return sha256(data);
	}
	if (prehash === PreHash.ETH_KECCAK_256) {
		return ethKeccak256Hash(data);
	}

	throw new Error("Unsupported");
};
