import { field, option, variant, vec } from "@dao-xyz/borsh";
import { ResultCoordinates, ResultSource } from "./result";
import BN from 'bn.js';
import { QueryType } from "./query-type";


@variant(0)
export class StringMatchQuery {

    @field({ type: 'String' })
    value: string

    @field({ type: 'u8' })
    exactMatch: boolean

    constructor(opts?: {
        value: string
        exactMatch: boolean
    }) {
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
export class StringQueryRequest extends QueryType {

    @field({ type: vec(StringMatchQuery) })
    queries: StringMatchQuery[];

    constructor(obj?: {
        queries: StringMatchQuery[]
    }) {
        super();
        if (obj) {
            Object.assign(this, obj);
        }
    }

}



@variant(0)
export class StringResultSource extends ResultSource {

    @field({ type: 'String' })
    string: string

    constructor(obj?: {
        string: string;
    }) {
        super();
        if (obj) {
            Object.assign(this, obj);
        }
    }
}

