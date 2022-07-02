import { field, variant } from "@dao-xyz/borsh";
import { Query } from "./query-interface";

@variant(0)
export class ContextMatchQuery extends Query {

}

@variant(0)
export class ShardMatchQuery extends ContextMatchQuery {

    @field({ type: 'String' })
    cid: string

    constructor(opts?: {
        cid: string
    }) {
        super();
        if (opts) {
            this.cid = opts.cid;
        }
    }
}

