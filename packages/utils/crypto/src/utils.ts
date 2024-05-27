import sodium from "libsodium-wrappers";
import { base58btc } from 'multiformats/bases/base58'

export const fromHexString = (hexString: string) => sodium.from_hex(hexString);

export const toHexString = (bytes: Uint8Array) => sodium.to_hex(bytes);

export const toBase64 = (arr: Uint8Array) => {
	return sodium.to_base64(arr, sodium.base64_variants.ORIGINAL);
};
export const fromBase64 = (base64: string) => {
	return sodium.from_base64(base64, sodium.base64_variants.ORIGINAL);
};

export const toBase64URL = (arr: Uint8Array) => {
	return sodium.to_base64(arr, sodium.base64_variants.URLSAFE);
};
export const fromBase64URL = (base64: string) => {
	return sodium.from_base64(base64, sodium.base64_variants.URLSAFE);
};

export const toBase58 = (arr: Uint8Array) => {
	return base58btc.baseEncode(arr)
}