import { Uint8ArrayList } from 'uint8arraylist'

export interface Uint8ArrayView {
	get(i: number): number
	subarray(from?: number, to?: number): Uint8Array,
	length: number,
	byteLength: number
}

export const viewFromBytes = (bytes: Uint8ArrayList | Uint8Array | Uint8ArrayView, offset: number, length: number): Uint8ArrayView => {
	if (bytes instanceof Uint8ArrayList) {
		return {
			subarray: (from?: number, to?: number) => bytes.subarray(offset, offset + length).subarray(from, to),
			length: length,
			byteLength: length,
			get: (i) => bytes.get(i + offset)
		}
	}
	if (bytes instanceof Uint8Array) {
		let subarray: Uint8Array | undefined = undefined;
		return {
			subarray: (from?: number, to?: number) => subarray || (subarray = bytes.subarray(offset, offset + length).subarray(from, to)),
			length: length,
			byteLength: length,
			get: (i) => bytes[i + offset]
		}
	}
	return {
		subarray: (from?: number, to?: number) => bytes.subarray(offset, offset + length).subarray(from, to),
		length: length,
		byteLength: length,
		get: (i) => bytes.get(i + offset)
	}
}

export const viewAsArray = (bytes: Uint8Array | Uint8ArrayView | Uint8ArrayList) => {
	if (bytes instanceof Uint8Array) {
		return bytes;
	}
	return bytes.subarray();
}
