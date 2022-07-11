import { field, variant, vec } from "@dao-xyz/borsh";


@variant(0)
export class Query {

}

export class QueryType {

}


@variant(0)
export class MultipleQueriesType extends QueryType {
    @field({ type: vec(Query) })
    queries: Query[]
}
