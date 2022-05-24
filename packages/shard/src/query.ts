import { Constructor, deserialize, field, variant, vec } from "@dao-xyz/borsh";
import { generateUUID } from "./id";
import bs58 from "bs58";
import BN from "bn.js";


export class Query {

    public apply(doc: any): boolean {
        throw new Error("Not implemented")
    }
}

@variant(0)
export class FilterQuery extends Query {


    @field({ type: 'String' })
    key: string

    @field({ type: vec('u8') })
    value: Uint8Array

    constructor(opts?: FilterQuery) {
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
export class StringMatchQuery extends Query {


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
export class CompareQuery extends Query {

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
export class QueryRequestV0 {

    @field({ type: 'String' })
    id: string

    @field({ type: vec(Query) })
    queries: Query[]

    constructor(obj?: {
        id?: string,
        queries: Query[]

    }) {
        if (obj) {
            Object.assign(this, obj);
            if (!this.id) {
                this.id = generateUUID();
            }
        }
    }

    getResponseTopic(topic: string): string {
        return topic + '/' + this.id
    }

}


@variant(1)
export class EncodedQueryResponse {

    @field({ type: vec('String') })
    results: string[] // base58 encoded

    constructor(obj?: {
        results: string[]

    }) {
        if (obj) {
            Object.assign(this, obj);
        }
    }
}

export class QueryResponse<T> {

    results: T[] // base58 encoded
    constructor(obj?: {
        results: T[]

    }) {
        if (obj) {
            Object.assign(this, obj);
        }
    }
    static from<T>(encoded: EncodedQueryResponse, clazz: Constructor<T>): QueryResponse<T> {
        let results = encoded.results.map(x => deserialize(bs58.decode(x), clazz))
        return new QueryResponse({
            results
        })
    }
}