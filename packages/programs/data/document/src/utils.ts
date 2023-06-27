import { toBase64 } from "@peerbit/crypto";

export type Keyable = string | Uint8Array;
export const asString = (obj: Keyable) =>
	typeof obj === "string" ? obj : toBase64(obj);
export const checkKeyable = (obj: Keyable) => {
	if (obj == null) {
		throw new Error(
			`The provided key value is null or undefined, expecting string or Uint8array`
		);
	}
	if (typeof obj === "string" || obj instanceof Uint8Array) {
		return;
	}

	throw new Error("Key is not string or Uint8array, key value: " + typeof obj);
};
