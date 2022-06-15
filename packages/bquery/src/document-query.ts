import { field, option, variant, vec } from "@dao-xyz/borsh";
import BN from 'bn.js';
import { QueryType } from "./query-type";

export enum SortDirection {
    Ascending = 0,
    Descending = 1
}

export class FieldSort {

    @field({ type: vec('String') })
    fieldPath: string[]

    @field({ type: 'u8' })
    direction: SortDirection

    constructor(opts: {
        fieldPath: string[],
        direction: SortDirection
    }) {
        if (opts) {
            Object.assign(this, opts);
        }
    }
}



export class FieldQuery {

    public apply(doc: any): boolean {
        throw new Error("Not implemented")
    }
}

@variant(0)
export class FieldFilterQuery extends FieldQuery {


    @field({ type: 'String' })
    key: string

    @field({ type: vec('u8') })
    value: Uint8Array

    constructor(opts?: FieldFilterQuery) {
        super();
        if (opts) {
            Object.assign(this, opts)
        }
    }

    public apply(doc: any): boolean {
        return doc[this.key] === this.value
    }
}

@variant(1)
export class FieldStringMatchQuery extends FieldQuery {


    @field({ type: 'String' })
    key: string

    @field({ type: 'String' })
    value: string

    constructor(opts?: {
        key: string
        value: string
    }) {
        super();
        if (opts) {
            Object.assign(this, opts)
        }
    }

    public apply(doc: any): boolean {
        return (doc[this.key] as string).toLowerCase().indexOf(this.value.toLowerCase()) != -1;
    }
}
export enum Compare {
    Equal = 0,
    Greater = 1,
    GreaterOrEqual = 2,
    Less = 3,
    LessOrEqual = 4
}

@variant(2)
export class FieldCompareQuery extends FieldQuery {

    @field({ type: 'u8' })
    compare: Compare

    @field({ type: 'String' })
    key: string

    @field({ type: 'u64' })
    value: BN


    constructor(opts?: {
        key: string
        value: BN,
        compare: Compare
    }) {
        super();
        if (opts) {
            Object.assign(this, opts)
        }
    }

    apply(doc: any): boolean {
        let value = doc[this.key];
        if (value instanceof BN == false) {
            value = new BN(value);
        }
        switch (this.compare) {
            case Compare.Equal:
                return value.eq(this.value);
            case Compare.Greater:
                return value.gt(this.value);
            case Compare.GreaterOrEqual:
                return value.gte(this.value);
            case Compare.Less:
                return value.lt(this.value);
            case Compare.LessOrEqual:
                return value.lte(this.value);
            default:
                console.warn("Unexpected compare");
                return false;
        }
    }
}





@variant(0)
export class DocumentQueryRequest extends QueryType {

    @field({ type: option('u64') })
    offset: BN | undefined;

    @field({ type: option('u64') })
    size: BN | undefined;

    @field({ type: vec(FieldQuery) })
    queries: FieldQuery[]

    @field({ type: option(FieldSort) })
    sort: FieldSort | undefined;

    constructor(obj?: {
        offset?: BN
        size?: BN
        queries: FieldQuery[]
        sort?: FieldSort

    }) {
        super();
        if (obj) {
            Object.assign(this, obj);
        }
    }

}