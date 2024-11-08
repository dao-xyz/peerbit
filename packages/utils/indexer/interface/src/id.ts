import { field, variant } from "@dao-xyz/borsh";
import { toBase64 } from "@peerbit/crypto";
import {
	decodeUint8Array,
	encodeUint8Array,
	encodingLength,
} from "uint8-varint";

export abstract class PrimitiveValue {}

@variant(0)
export class StringValue extends PrimitiveValue {
	@field({ type: "string" })
	string: string;

	constructor(string: string) {
		super();
		this.string = string;
	}
}

@variant(1)
export abstract class NumberValue extends PrimitiveValue {
	abstract get value(): number | bigint;
}

@variant(0)
export abstract class IntegerValue extends NumberValue {}

@variant(0)
export class UnsignedIntegerValue extends IntegerValue {
	@field({ type: "u32" })
	number: number;

	constructor(number: number) {
		super();
		if (!Number.isInteger(number) || number > 4294967295 || number < 0) {
			throw new Error("Number is not u32");
		}
		this.number = number;
	}

	get value() {
		return this.number;
	}
}

@variant(1)
export class BigUnsignedIntegerValue extends IntegerValue {
	@field({ type: "u64" })
	number: bigint;

	constructor(number: bigint) {
		super();
		if (number > 18446744073709551615n || number < 0n) {
			throw new Error("Number is not u64");
		}
		this.number = number;
	}
	get value() {
		return this.number;
	}
}

export type IdPrimitive = string | number | bigint;

export abstract class IdKey {
	abstract get key(): string | bigint | number | Uint8Array;
	abstract get primitive(): IdPrimitive;
}

@variant(0)
export class StringKey extends IdKey {
	@field({ type: "string" })
	key: string;

	constructor(key: string) {
		super();
		this.key = key;
	}
	get primitive() {
		return this.key;
	}
}

@variant(1)
export class Uint8ArrayKey extends IdKey {
	@field({ type: Uint8Array })
	key: Uint8Array;

	constructor(key: Uint8Array) {
		super();
		this.key = key;
	}

	private _keyString!: string;
	get primitive(): string {
		return this._keyString || (this._keyString = toBase64(this.key));
	}
}

const varint53 = {
	deserialize: (reader: any) => {
		const number = decodeUint8Array(reader._buf, reader._offset);
		const len = encodingLength(number);
		reader._offset += len;
		return number;
	},
	serialize: (value: any, writer: any) => {
		const offset = writer.totalSize;
		writer["_writes"] = writer["_writes"].next = () =>
			encodeUint8Array(value, writer["_buf"], offset);
		writer.totalSize += encodingLength(value);
	},
};

@variant(2)
export class IntegerKey extends IdKey {
	@field(varint53) // max value is 2^53 - 1 (9007199254740991)
	key: number;

	constructor(key: number) {
		super();
		this.key = key;
	}

	get primitive() {
		return this.key;
	}
}

export type Ideable = string | number | bigint | Uint8Array;

const idKeyTypes = new Set(["string", "number", "bigint"]);

export const toId = (obj: Ideable): IdKey => {
	if (typeof obj === "string") {
		return new StringKey(obj);
	}
	if (typeof obj === "number") {
		return new IntegerKey(obj);
	}
	if (typeof obj === "bigint") {
		if (obj <= Number.MAX_SAFE_INTEGER && obj >= 0n) {
			return new IntegerKey(Number(obj));
		}
		throw new Error(
			"BigInt is not less than 2^53. Max value is 9007199254740991",
		);
	}
	if (obj instanceof Uint8Array) {
		return new Uint8ArrayKey(obj);
	}
	throw new Error(
		"Unexpected index key: " +
			typeof obj +
			", expected: string, number, bigint or Uint8Array",
	);
};

export const toIdeable = (
	key: IdKey | Ideable,
): string | number | bigint | Uint8Array => {
	if (key instanceof IdKey) {
		return key.key;
	}
	return key;
};

export const checkId = (obj: Ideable) => {
	if (obj == null) {
		throw new Error(
			`The provided key value is null or undefined, expecting string, number, bigint, or Uint8array`,
		);
	}
	const type = typeof obj;

	if (type === "number") {
		if (!Number.isInteger(obj)) {
			throw new Error(`The provided key number value is not an integer`);
		}
	}

	if (idKeyTypes.has(type) || obj instanceof Uint8Array) {
		return;
	}

	throw new Error(
		`Key is not ${[...idKeyTypes]}, provided key value type: ${typeof obj}`,
	);
};
