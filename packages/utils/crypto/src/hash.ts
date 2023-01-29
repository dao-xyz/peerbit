import crypto from "crypto";

export const sha256Base64Sync = (bytes: Uint8Array) =>
	crypto.createHash("sha256").update(bytes).digest("base64");
export const sha256Base64 = async (bytes: Uint8Array) =>
	crypto.createHash("sha256").update(bytes).digest("base64");
export const sha256 = async (bytes: Uint8Array) =>
	crypto.createHash("sha256").update(bytes).digest();
export const sha256Sync = (bytes: Uint8Array) =>
	crypto.createHash("sha256").update(bytes).digest();
