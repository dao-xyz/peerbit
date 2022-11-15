import {
    AbstractType,
    deserialize,
    field,
    option,
    variant,
    vec,
} from "@dao-xyz/borsh";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";

/// ----- QUERY -----

@variant(0)
export class Query {}

@variant(0)
export class MultipleQueries {
    @field({ type: vec(Query) })
    queries!: Query[];

    constructor(props?: { queries: Query[] }) {
        if (props) {
            this.queries = props.queries;
        }
    }
}

export enum SortDirection {
    Ascending = 0,
    Descending = 1,
}

@variant(0)
export class DocumentQueryRequest extends MultipleQueries {
    @field({ type: option("u64") })
    offset: bigint | undefined;

    @field({ type: option("u64") })
    size: bigint | undefined;

    @field({ type: "u8" })
    sort: 0 = 0;

    constructor(props?: { offset?: bigint; size?: bigint; queries: Query[] }) {
        super(
            props
                ? {
                      queries: props.queries,
                  }
                : undefined
        );

        if (props) {
            this.offset = props.offset;
            this.size = props.size;
        }
    }
}

@variant(2)
export class StateQuery extends Query {}

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
    @field(UInt8ArraySerializer)
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
export enum Compare {
    Equal = 0,
    Greater = 1,
    GreaterOrEqual = 2,
    Less = 3,
    LessOrEqual = 4,
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

@variant(0)
export class MemoryCompare {
    @field(UInt8ArraySerializer)
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

export class ResultContext {}
export class Result {}

@variant(0)
export class ResultWithSource<T> extends Result {
    @field(UInt8ArraySerializer)
    _source: Uint8Array;

    @field({ type: option(ResultContext) })
    context: ResultContext | undefined;

    _type: AbstractType<T>;
    constructor(opts?: { source: Uint8Array; context?: ResultContext }) {
        super();
        if (opts) {
            this._source = opts.source;
            this.context = opts.context;
        }
    }

    init(type: AbstractType<T>) {
        this._type = type;
    }

    _value: T;
    get value(): T {
        if (this._value) {
            return this._value;
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
