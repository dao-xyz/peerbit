import { type AbstractType, deserialize, serialize } from "@dao-xyz/borsh";

export interface Encoding<T> {
	encoder: (data: T) => Uint8Array;
	decoder: (bytes: Uint8Array) => T;
}
export const NO_ENCODING: Encoding<any> = {
	encoder: (obj: Uint8Array) => {
		if (obj instanceof Uint8Array === false) {
			throw new Error(
				"With NO_ENCODING only Uint8arrays are allowed, received: " +
					(obj?.["constructor"]?.["name"] || typeof obj),
			);
		}
		return obj;
	},
	decoder: (bytes: Uint8Array) => {
		return bytes;
	},
};

export const BORSH_ENCODING = <T>(clazz: AbstractType<T>): Encoding<T> => {
	return {
		decoder: (bytes: Uint8Array) => deserialize(bytes, clazz),
		encoder: (data: any) => serialize(data),
	};
};
