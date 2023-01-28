import { AbstractType, deserialize, field, variant, vec } from "@dao-xyz/borsh";

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
export class DocumentQueryRequest {
	@field({ type: vec(Query) })
	queries!: Query[];

	constructor(props?: { queries: Query[] }) {
		if (props) {
			this.queries = props.queries;
		}
	}
}

@variant(1)
export abstract class ContextQuery extends Query {}

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

@variant(2)
export abstract class StateQuery extends Query {}

@variant(1)
export class StateFieldQuery extends StateQuery {
	@field({ type: vec("string") })
	key: string[];

	constructor(props?: { key: string[] | string }) {
		super();
		if (props) {
			this.key = Array.isArray(props.key) ? props.key : [props.key];
		}
	}
}

@variant(1)
export class FieldByteMatchQuery extends StateFieldQuery {
	@field({ type: Uint8Array })
	value: Uint8Array;

	constructor(props?: { key: string[]; value: Uint8Array }) {
		super(props);
		if (props) {
			this.value = props.value;
		}
	}
}

@variant(2)
export class FieldStringMatchQuery extends StateFieldQuery {
	@field({ type: "string" })
	value: string;

	constructor(props?: { key: string[] | string; value: string }) {
		super(props);
		if (props) {
			this.value = props.value;
		}
	}
}

@variant(3)
export class FieldBigIntCompareQuery extends StateFieldQuery {
	@field({ type: "u8" })
	compare: Compare;

	@field({ type: "u64" })
	value: bigint;

	constructor(props?: {
		key: string[] | string;
		value: bigint;
		compare: Compare;
	}) {
		super(props);
		if (props) {
			this.value = props.value;
			this.compare = props.compare;
		}
	}
}

@variant(4)
export class FieldMissingQuery extends StateFieldQuery {
	constructor(props?: { key: string[] | string }) {
		super(props);
	}
}

@variant(0)
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
}

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
	_source?: Uint8Array;

	@field({ type: Context })
	context: Context;

	_type: AbstractType<T>;
	constructor(opts: { source?: Uint8Array; context: Context; value?: T }) {
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
