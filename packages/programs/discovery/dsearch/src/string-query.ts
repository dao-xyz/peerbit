import { field, variant } from "@dao-xyz/borsh";
import { MultipleQueriesType, Query } from './query-interface.js';


@variant(3)
export class StringMatchQuery extends Query {

    @field({ type: 'string' })
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
