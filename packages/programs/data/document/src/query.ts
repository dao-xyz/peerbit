import { AbstractType, deserialize, field, variant, vec } from "@dao-xyz/borsh";
import { PublicSignKey, X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { EntryEncryptionTemplate } from "@dao-xyz/peerbit-log";

export enum Compare {
	Equal = 0,
	Greater = 1,
	GreaterOrEqual = 2,
	Less = 3,
	LessOrEqual = 4,
}
export const compare = (
	test: bigint | number,
	compare: Compare,
	value: bigint | number
) => {
	switch (compare) {
		case Compare.Equal:
			return test == value; // == because with want bigint == number at some cases
		case Compare.Greater:
			return test > value;
		case Compare.GreaterOrEqual:
			return test >= value;
		case Compare.Less:
			return test < value;
		case Compare.LessOrEqual:
			return test <= value;
		default:
			console.warn("Unexpected compare");
			return false;
	}
};

@variant(0)
export class U64Compare {
	@field({ type: "u8" })
	compare: Compare;

	@field({ type: "u64" })
	value: bigint;

	constructor(props?: { value: bigint; compare: Compare }) {
		if (props) {
			this.compare = props.compare;
			this.value = props.value;
		}
	}
}

/// ----- QUERY -----

export abstract class Query {}

@variant(0)
export class DocumentQuery {
	@field({ type: vec(Query) })
	queries!: Query[];

	constructor(props?: { queries: Query[] }) {
		if (props) {
			this.queries = props.queries;
		}
	}
}

/* @variant(1)
export abstract class ContextQuery extends Query { }

@variant(0)
export class CreatedAtQuery extends ContextQuery {
	@field({ type: vec(U64Compare) })
	created: U64Compare[];

	constructor(props?: { created: U64Compare[] }) {
		super();
		if (props) {
			this.created = props.created;
		}
	}
}

@variant(1)
export class ModifiedAtQuery extends ContextQuery {
	@field({ type: vec(U64Compare) })
	modified: U64Compare[];

	constructor(props?: { modified: U64Compare[] }) {
		super();
		if (props) {
			this.modified = props.modified;
		}
	}
}
 */

@variant(1)
export abstract class LogicalQuery extends Query {}

@variant(0)
export class And extends LogicalQuery {
	@field({ type: vec(Query) })
	and: Query[];

	constructor(and: Query[]) {
		super();
		this.and = and;
	}
}

@variant(1)
export class Or extends LogicalQuery {
	@field({ type: vec(Query) })
	or: Query[];

	constructor(or: Query[]) {
		super();
		this.or = or;
	}
}

@variant(2)
export abstract class StateQuery extends Query {}

@variant(1)
export class StateFieldQuery extends StateQuery {
	@field({ type: vec("string") })
	key: string[];

	constructor(props: { key: string[] | string }) {
		super();
		this.key = Array.isArray(props.key) ? props.key : [props.key];
	}
}

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
abstract class NumberValue extends PrimitiveValue {
	abstract get value(): number | bigint;
}

@variant(0)
abstract class IntegerValue extends NumberValue {}

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

@variant(1)
export class ByteMatchQuery extends StateFieldQuery {
	@field({ type: Uint8Array })
	value: Uint8Array;

	@field({ type: "u8" })
	private _reserved: number; // Replcate MemoryCompare query with this?

	constructor(props: { key: string[] | string; value: Uint8Array }) {
		super(props);
		this.value = props.value;
		this._reserved = 0;
	}
}

export enum StringMatchMethod {
	"exact" = 0,
	"prefix" = 1,
	"contains" = 2,
}

@variant(2)
export class StringMatch extends StateFieldQuery {
	@field({ type: "string" })
	value: string;

	@field({ type: "u8" })
	method: StringMatchMethod;

	@field({ type: "bool" })
	caseInsensitive: boolean;

	constructor(props: {
		key: string[] | string;
		value: string;
		method?: StringMatchMethod;
		caseInsensitive?: boolean;
	}) {
		super(props);
		this.value = props.value;
		this.method = props.method ?? StringMatchMethod.exact;
		this.caseInsensitive = props.caseInsensitive ?? false;
	}
}

@variant(3)
export class IntegerCompare extends StateFieldQuery {
	@field({ type: "u8" })
	compare: Compare;

	@field({ type: IntegerValue })
	value: IntegerValue;

	constructor(props: {
		key: string[] | string;
		value: bigint | number | IntegerValue;
		compare: Compare;
	}) {
		super(props);
		if (props.value instanceof IntegerValue) {
			this.value = props.value;
		} else {
			if (typeof props.value === "bigint") {
				this.value = new BigUnsignedIntegerValue(props.value);
			} else {
				this.value = new UnsignedIntegerValue(props.value);
			}
		}

		this.compare = props.compare;
	}
}

@variant(4)
export class MissingField extends StateFieldQuery {
	constructor(props: { key: string[] | string }) {
		super(props);
	}
}

@variant(5)
export class BoolQuery extends StateFieldQuery {
	@field({ type: "bool" })
	value: boolean;

	constructor(props: { key: string[] | string; value: boolean }) {
		super(props);
		this.value = props.value;
	}
}

// TODO MemoryCompareQuery can be replaces with ByteMatchQuery? Or Nesteed Queries + ByteMatchQuery?
/* @variant(0)
export class MemoryCompare {
	@field({ type: Uint8Array })
	bytes: Uint8Array;

	@field({ type: "u64" })
	offset: bigint;

	constructor(opts?: { bytes: Uint8Array; offset: bigint }) {
		if (opts) {
			this.bytes = opts.bytes;
			this.offset = opts.offset;
		}
	}
}

@variant(4)
export class MemoryCompareQuery extends Query {
	@field({ type: vec(MemoryCompare) })
	compares: MemoryCompare[];

	constructor(opts?: { compares: MemoryCompare[] }) {
		super();
		if (opts) {
			this.compares = opts.compares;
		}
	}
} */

/// ----- RESULTS -----

export abstract class Result {}

@variant(0)
export class Context {
	@field({ type: "u64" })
	created: bigint;

	@field({ type: "u64" })
	modified: bigint;

	@field({ type: "string" })
	head: string;

	constructor(properties?: {
		created: bigint;
		modified: bigint;
		head: string;
	}) {
		if (properties) {
			this.created = properties.created;
			this.modified = properties.modified;
			this.head = properties.head;
		}
	}
}

@variant(0)
export class ResultWithSource<T> extends Result {
	@field({ type: Uint8Array })
	_source: Uint8Array;

	@field({ type: Context })
	context: Context;

	_type: AbstractType<T>;
	constructor(opts: { source: Uint8Array; context: Context; value?: T }) {
		super();
		this._source = opts.source;
		this.context = opts.context;
		this._value = opts.value;
	}

	init(type: AbstractType<T>) {
		this._type = type;
	}

	_value?: T;
	get value(): T {
		if (this._value) {
			return this._value;
		}
		if (!this._source) {
			throw new Error("Missing source binary");
		}
		this._value = deserialize(this._source, this._type);
		return this._value;
	}
}

@variant(0)
export class Results<T> {
	@field({ type: vec(ResultWithSource) })
	results: ResultWithSource<T>[];

	constructor(properties?: { results: ResultWithSource<T>[] }) {
		if (properties) {
			this.results = properties.results;
		}
	}
}

/* @variant(5)
export class LogQuery extends Query { } */

/**
 * Find logs that can be decrypted by certain keys
 */
/* 
@variant(0)
export class EntryEncryptedByQuery
	extends LogQuery
	implements
	EntryEncryptionTemplate<
		X25519PublicKey[],
		X25519PublicKey[],
		X25519PublicKey[],
		X25519PublicKey[]
	>
{
	@field({ type: vec(X25519PublicKey) })
	metadata: X25519PublicKey[];

	@field({ type: vec(X25519PublicKey) })
	payload: X25519PublicKey[];

	@field({ type: vec(X25519PublicKey) })
	next: X25519PublicKey[];

	@field({ type: vec(X25519PublicKey) })
	signatures: X25519PublicKey[];

	constructor(properties?: {
		metadata: X25519PublicKey[];
		next: X25519PublicKey[];
		payload: X25519PublicKey[];
		signatures: X25519PublicKey[];
	}) {
		super();
		if (properties) {
			this.metadata = properties.metadata;
			this.payload = properties.payload;
			this.next = properties.next;
			this.signatures = properties.signatures;
		}
	}
}

@variant(1)
export class SignedByQuery extends LogQuery {
	@field({ type: vec(PublicSignKey) })
	_publicKeys: PublicSignKey[];

	constructor(properties: { publicKeys: PublicSignKey[] }) {
		super();
		if (properties) {
			this._publicKeys = properties.publicKeys;
		}
	}

	get publicKeys() {
		return this._publicKeys;
	}
}
 */
