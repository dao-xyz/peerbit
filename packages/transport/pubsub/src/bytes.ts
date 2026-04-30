import type { Uint8ArrayList } from "uint8arraylist";

export type ByteListLike = {
	byteLength: number;
	subarray(start?: number, end?: number): Uint8Array;
};

export const normalizeUint8Array = (bytes: unknown): Uint8Array | undefined => {
	if (bytes instanceof Uint8Array) {
		return bytes;
	}
	if (ArrayBuffer.isView(bytes)) {
		return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	}
	if (
		bytes &&
		typeof bytes === "object" &&
		typeof (bytes as ByteListLike).subarray === "function"
	) {
		return normalizeUint8Array((bytes as ByteListLike).subarray());
	}
	return undefined;
};

export const toUint8Array = (
	arr: Uint8Array | Uint8ArrayList | ByteListLike,
): Uint8Array => {
	const normalized = normalizeUint8Array(arr);
	if (!normalized) {
		throw new Error("Expected byte source");
	}
	return normalized;
};
