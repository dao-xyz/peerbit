import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { toBase64 } from "@peerbit/crypto";

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
		if (
			Number.isInteger(number) === false ||
			number > 4294967295 ||
			number < 0
		) {
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
		if (number > 18446744073709551615n || number < 0) {
			throw new Error("Number is not u32");
		}
		this.number = number;
	}
	get value() {
		return this.number;
	}
}

export abstract class IndexKey {
	abstract get string();
}

@variant(0)
export class StringKey extends IndexKey {
	@field({ type: "string" })
	key: string;

	constructor(key: string) {
		super();
		this.key = key;
	}
	get string() {
		return this.key;
	}
}

@variant(1)
export class Uint8ArrayKey extends IndexKey {
	@field({ type: Uint8Array })
	key: Uint8Array;

	constructor(key: Uint8Array) {
		super();
		this.key = key;
	}

	private _keyString: string;
	get string(): string {
		return this._keyString || (this._keyString = toBase64(this.key));
	}
}

@variant(2)
export class IntegerKey extends IndexKey {
	@field({ type: IntegerValue })
	private key: IntegerValue;

	constructor(key: IntegerValue) {
		super();
		this.key = key;
	}

	get string() {
		return this.key.value.toString();
	}
}

export type Keyable = string | number | bigint | Uint8Array;

const idKeyTypes = new Set(["string", "number", "bigint"]);

export const asKey = (obj: Keyable): IndexKey => {
	if (typeof obj === "string") {
		return new StringKey(obj);
	}
	if (typeof obj === "number") {
		return new IntegerKey(new UnsignedIntegerValue(obj));
	}
	if (typeof obj === "bigint") {
		return new IntegerKey(new BigUnsignedIntegerValue(obj));
	}
	if (obj instanceof Uint8Array) {
		return new Uint8ArrayKey(obj);
	}
	throw new Error(
		"Unexpected index key: " +
			typeof obj +
			", expected: string, number, bigint or Uint8Array"
	);
};

export const keyAsString = (key: IndexKey | Keyable) => {
	if (key instanceof IndexKey) {
		return key.string;
	}

	if (typeof key === "string") {
		return key;
	}

	if (typeof key === "number") {
		return key.toString();
	}

	if (typeof key === "bigint") {
		return key.toString();
	}

	if (key instanceof Uint8Array) {
		return toBase64(key);
	}
};

export const checkKeyable = (obj: Keyable) => {
	if (obj == null) {
		throw new Error(
			`The provided key value is null or undefined, expecting string or Uint8array`
		);
	}
	const type = typeof obj;

	if (type === "number") {
		if (Number.isInteger(obj) === false) {
			throw new Error(
				`The provided key value is not an integer, expecting string or Uint8array`
			);
		}
	}

	if (idKeyTypes.has(type) || obj instanceof Uint8Array) {
		return;
	}

	throw new Error(
		`Key is not ${[...idKeyTypes]}, provided key value type: ${typeof obj}`
	);
};
