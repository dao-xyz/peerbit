import { type AbstractType, deserialize, serialize } from "@dao-xyz/borsh";
import { type Encoding } from "@peerbit/log";
import stringify from "json-stringify-deterministic";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const JSON_ENCODING: Encoding<any> = {
	encoder: (obj: any) => {
		return new Uint8Array(encoder.encode(stringify(obj)));
	},
	decoder: (bytes: Uint8Array) => {
		return JSON.parse(decoder.decode(bytes).toString());
	}
};

export const BORSH_ENCODING = <T>(clazz: AbstractType<T>): Encoding<T> => {
	return {
		decoder: (bytes: Uint8Array) => deserialize(bytes, clazz),
		encoder: (data: any) => serialize(data)
	};
};
