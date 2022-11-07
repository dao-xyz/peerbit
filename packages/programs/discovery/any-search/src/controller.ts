import { field, variant } from '@dao-xyz/borsh';
import { SignKey } from "@dao-xyz/peerbit-crypto";
import { QueryOptions, DQuery, CanRead, QueryTopicOption } from '@dao-xyz/peerbit-query';
import { AbstractProgram, Address, ComposableProgram } from '@dao-xyz/peerbit-program'
import { MultipleQueriesType, QueryType } from './query-interface';
import { Result, Results } from './result';
import { ProgramMatchQuery } from './context';


export type SearchContext = (() => Address) | AbstractProgram;
export type AnySearchInitializationOptions<T> = { canRead?: CanRead, context: SearchContext, queryHandler: (query: QueryType) => Promise<Result[]> };

@variant("anysearch")
export class AnySearch<T> extends ComposableProgram {

    @field({ type: DQuery })
    _query: DQuery<QueryType, Results>

    _queryHandler: (query: QueryType) => Promise<Result[]>
    _context: SearchContext;

    _setup: boolean = false;
    constructor(properties: { query: DQuery<QueryType, Results> }) {
        super()
        if (properties) {
            this._query = properties.query;
        }
    }


    public async setup(options: AnySearchInitializationOptions<T> & { queryTopic?: QueryTopicOption }) {
        this._setup = true;
        this._queryHandler = options.queryHandler;
        this._context = options.context;
        await this._query.setup({ canRead: options.canRead, responseHandler: this._onQueryMessage.bind(this), queryType: QueryType, responseType: Results, queryTopic: options.queryTopic })
    }

    async _onQueryMessage(query: QueryType, from?: SignKey): Promise<Results | undefined> {
        if (!this._setup) {
            throw new Error(".setup(...) needs to be invoked before use")
        }
        if (query instanceof MultipleQueriesType) {
            // Handle context queries
            for (const q of query.queries) {
                if (q instanceof ProgramMatchQuery) {
                    const context = this._context instanceof AbstractProgram ? this._context.address : this._context();
                    if (q.program !== context.toString()) {
                        // This query is not for me!
                        return;
                    }
                }
            }

            // Handle non context queries
            const results = await this._queryHandler(query);
            if (!results || results.length == 0) {
                return;
            }
            let response = new Results({
                results
            });
            return response;
        }
    }

    public query(queryRequest: QueryType, responseHandler: (results: Results) => void, options?: QueryOptions): Promise<void> {
        return this._query.query(queryRequest, responseHandler, options);
    }

}
