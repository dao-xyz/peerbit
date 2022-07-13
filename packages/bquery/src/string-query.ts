import { field, option, variant, vec } from "@dao-xyz/borsh";
import { ResultCoordinates } from "./result";
import BN from 'bn.js';
import { MultipleQueriesType, Query, QueryType } from "./query-interface";
import { ContextMatchQuery } from "./context";


@variant(2)
export class StringMatchQuery extends Query {

    @field({ type: 'String' })
    value: string

    @field({ type: 'u8' })
    exactMatch: boolean

    constructor(opts?: {
        value: string
        exactMatch: boolean
    }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
    preprocess(string: string): string {
        if (this.exactMatch) {
            return string.toLowerCase();
        }
        return string;
    }
}

@variant(0)
export class RangeCoordinate {

    @field({ type: 'u64' })
    offset: BN;

    @field({ type: 'u64' })
    length: BN;

    constructor(opts?: {
        offset: BN;
        length: BN;
    }) {
        if (opts) {
            Object.assign(this, opts);
        }
    }

}
@variant(0)
export class RangeCoordinates extends ResultCoordinates {

    @field({ type: vec(RangeCoordinate) })
    coordinates: RangeCoordinate[]

    constructor(opts?: {
        coordinates: RangeCoordinate[];
    }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }

}


@variant(1)
export class StringQueryRequest extends MultipleQueriesType {



    constructor(obj?: {
        queries: Query[]
    }) {
        super();
        if (obj) {
            Object.assign(this, obj);
        }
    }

}
