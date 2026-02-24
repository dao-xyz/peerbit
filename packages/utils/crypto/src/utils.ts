import sodium from "libsodium-wrappers";
import { base58btc } from "multiformats/bases/base58";

export const fromHexString = (hexString: string) => sodium.from_hex(hexString);

// Normalize to a "local realm" Uint8Array.
// This avoids libsodium rejecting cross-realm typed arrays (e.g. jsdom/happy-dom environments).
const asU8 = (bytes: ArrayBufferView) => {
	// Fast-path: in normal runtimes `bytes` is already a local Uint8Array.
	// Cross-realm typed arrays (iframes/jsdom/happy-dom) fail `instanceof` checks in libsodium-wrappers.
	return bytes instanceof Uint8Array
		? bytes
		: new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
};

export const toHexString = (bytes: Uint8Array) => sodium.to_hex(asU8(bytes));

export const toBase64 = (arr: Uint8Array) => {
	return sodium.to_base64(asU8(arr), sodium.base64_variants.ORIGINAL);
};
export const fromBase64 = (base64: string) => {
	return sodium.from_base64(base64, sodium.base64_variants.ORIGINAL);
};

export const toBase64URL = (arr: Uint8Array) => {
	return sodium.to_base64(asU8(arr), sodium.base64_variants.URLSAFE);
};
export const fromBase64URL = (base64: string) => {
	return sodium.from_base64(base64, sodium.base64_variants.URLSAFE);
};

export const toBase58 = (arr: Uint8Array) => {
	return base58btc.baseEncode(asU8(arr));
};
