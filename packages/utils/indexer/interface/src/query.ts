import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { toBase64 } from "@peerbit/crypto";
import { v4 as uuid } from "uuid";
import {
	BigUnsignedIntegerValue,
	IntegerValue,
	UnsignedIntegerValue,
} from "./id.js";

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
	value: bigint | number,
) => {
	switch (compare) {
		case Compare.Equal:
			// eslint-disable-next-line eqeqeq
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
			// eslint-disable-next-line no-console
			console.warn("Unexpected compare");
			return false;
	}
};

/// ----- QUERY -----

export abstract class Query {
	clone() {
		return deserialize(serialize(this), this.constructor) as this;
	}
}

export enum SortDirection {
	ASC = 0,
	DESC = 1,
}

@variant(0)
export class Sort {
	@field({ type: vec("string") })
	key: string[];

	@field({ type: "u8" })
	direction: SortDirection;

	constructor(properties: {
		key: string[] | string;
		direction?: SortDirection | "asc" | "desc";
	}) {
		this.key = Array.isArray(properties.key)
			? properties.key
			: [properties.key];
		if (properties.direction) {
			if (properties.direction === "asc") {
				this.direction = SortDirection.ASC;
			} else if (properties.direction === "desc") {
				this.direction = SortDirection.DESC;
			} else {
				this.direction = properties.direction;
			}
		} else {
			this.direction = SortDirection.ASC;
		}
	}
}

@variant(1)
export abstract class LogicalQuery extends Query {}

@variant(0)
export class And extends LogicalQuery {
	@field({ type: vec(Query) })
	and: Query[];

	constructor(and: Query[]) {
		super();
		this.and = and;

		if (this.and.length === 0) {
			throw new Error("And query must have at least one query");
		}
	}
}

@variant(1)
export class Or extends LogicalQuery {
	@field({ type: vec(Query) })
	or: Query[];

	constructor(or: Query[]) {
		super();
		this.or = or;

		if (this.or.length === 0) {
			throw new Error("Or query must have at least one query");
		}
	}
}

@variant(2)
export class Not extends LogicalQuery {
	@field({ type: Query })
	not: Query;

	constructor(not: Query) {
		super();
		this.not = not;
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

@variant(1)
export class ByteMatchQuery extends StateFieldQuery {
	@field({ type: Uint8Array })
	value: Uint8Array;

	@field({ type: "u8" })
	// @ts-expect-error: unused
	private _reserved: number; // Replicate MemoryCompare query with this?

	constructor(props: { key: string[] | string; value: Uint8Array }) {
		super(props);
		this.value = props.value;
		this._reserved = 0;
	}

	_valueString!: string;
	/**
	 * value `asString`
	 */
	get valueString() {
		return this._valueString ?? (this._valueString = toBase64(this.value));
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
		compare: "eq" | "gt" | "gte" | "lt" | "lte" | Compare;
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

		if (typeof props.compare === "string") {
			if (props.compare === "eq") {
				this.compare = Compare.Equal;
			} else if (props.compare === "gt") {
				this.compare = Compare.Greater;
			} else if (props.compare === "gte") {
				this.compare = Compare.GreaterOrEqual;
			} else if (props.compare === "lt") {
				this.compare = Compare.Less;
			} else if (props.compare === "lte") {
				this.compare = Compare.LessOrEqual;
			} else {
				throw new Error("Invalid compare string");
			}
		} else {
			this.compare = props.compare;
		}
	}
}

@variant(4)
export class IsNull extends StateFieldQuery {}

@variant(5)
export class BoolQuery extends StateFieldQuery {
	@field({ type: "bool" })
	value: boolean;

	constructor(props: { key: string[] | string; value: boolean }) {
		super(props);
		this.value = props.value;
	}
}

// @experimental not supported by all implementations
@variant(2)
export class Nested extends Query {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	path: string;

	@field({ type: vec(Query) })
	query: Query[];

	constructor(props: {
		id?: string;
		path: string;
		query:
			| Query[]
			| Query
			| Record<
					string,
					string | number | bigint | Uint8Array | boolean | null | undefined
			  >;
	}) {
		super();
		this.path = props.path;
		this.id = props.id ?? uuid();
		this.query = toQuery(props.query);
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

export type QueryLike =
	| Query[]
	| Query
	| Record<
			string,
			string | number | bigint | Uint8Array | boolean | null | undefined
	  >;
export const toQuery = (query?: QueryLike): Query[] => {
	if (!query) {
		return [];
	}
	if (Array.isArray(query)) {
		return query;
	}
	if (query instanceof Query) {
		return [query];
	}

	return convertQueryRecordToObject(query);
};

const convertQueryRecordToObject = (
	obj: Record<
		string,
		| string
		| number
		| bigint
		| Uint8Array
		| boolean
		| null
		| undefined
		| Record<string, any>
	>,
	queries: Query[] = [],
	path?: string[],
) => {
	for (const [k, v] of Object.entries(obj)) {
		let mergedKey = path ? [...path, k] : [k];
		if (typeof v === "object" && v instanceof Uint8Array === false) {
			convertQueryRecordToObject(v!, queries, mergedKey);
		} else {
			const matcher = getMatcher(mergedKey, v);
			queries.push(matcher);
		}
	}
	return queries;
};

export const getMatcher = (
	key: string[],
	value: string | number | bigint | Uint8Array | boolean | null | undefined,
) => {
	if (typeof value === "string") {
		return new StringMatch({ key, value });
	} else if (typeof value === "bigint" || typeof value === "number") {
		return new IntegerCompare({ key, value, compare: Compare.Equal });
	} else if (typeof value === "boolean") {
		return new BoolQuery({ key, value });
	} else if (value == null) {
		return new IsNull({ key });
	} else if (value instanceof Uint8Array) {
		return new ByteMatchQuery({ key, value });
	}

	throw new Error("Invalid query value");
};
