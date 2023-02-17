import { toBase64 } from "@dao-xyz/peerbit-crypto";

export type Keyable = string | Uint8Array;
export const asString = (obj: Keyable) =>
	typeof obj === "string" ? obj : toBase64(obj);
