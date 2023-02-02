import { sha256 } from "./hash.js";

export enum PreHash {
	NONE = 0,
	SHA_256 = 1,
}

export const prehashFn = (
	data: Uint8Array,
	prehash: PreHash
): Promise<Uint8Array> | Uint8Array => {
	if (prehash === PreHash.NONE) {
		return data;
	}
	return sha256(data);
};
