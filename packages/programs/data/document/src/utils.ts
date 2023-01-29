import { toBase64 } from "@dao-xyz/peerbit-crypto";

export type Keyable = string | { hashCode: () => string } | Uint8Array;
export const asString = (obj: Keyable) => {
	if (obj instanceof Uint8Array) {
		return toBase64(obj);
	}
	return typeof obj === "string" ? obj : obj.hashCode();
};
