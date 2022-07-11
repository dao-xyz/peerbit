import { field, variant } from "@dao-xyz/borsh";
import { Query } from "./query-interface";

@variant(0)
export class ContextMatchQuery extends Query {

}

@variant(0)
export class StoreAddressMatchQuery extends ContextMatchQuery {

    @field({ type: 'String' })
    address: string

    constructor(opts?: {
        address: string
    }) {
        super();
        if (opts) {
            this.address = opts.address;
        }
    }
}

