
import { field, option, variant, vec } from "@dao-xyz/borsh";

@variant(0)
export class Query {

}

export class QueryType {

}


@variant(0)
export class MultipleQueriesType extends QueryType {

    @field({ type: vec(Query) })
    queries!: Query[]

    constructor(props?: {
        queries: Query[]
    }) {
        super();
        if (props) {
            this.queries = props.queries;
        }
    }
}

export enum SortDirection {
    Ascending = 0,
    Descending = 1
}



@variant(0)
export class FieldSort {

    @field({ type: vec('string') })
    key: string[]

    @field({ type: 'u8' })
    direction: SortDirection

    constructor(props: {
        key: string[] | string,
        direction: SortDirection
    }) {
        if (props) {
            this.key = Array.isArray(props.key) ? props.key : [props.key];
            this.direction = props.direction;
        }
    }
}

@variant(0)
export class PageQueryRequest extends MultipleQueriesType {

    @field({ type: option('u64') })
    offset: bigint | undefined;

    @field({ type: option('u64') })
    size: bigint | undefined;

    @field({ type: 'u8' })
    sort: 0 = 0;

    constructor(props?: {
        offset?: bigint
        size?: bigint
        queries: Query[]

    }) {
        super(props ? {
            queries: props.queries
        } : undefined);

        if (props) {
            this.offset = props.offset;
            this.size = props.size;
        }
    }
}