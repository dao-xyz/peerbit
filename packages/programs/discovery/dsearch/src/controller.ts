import { field, variant } from '@dao-xyz/borsh';
import { SignatureWithKey, SignKey } from "@dao-xyz/peerbit-crypto";
import { IPFS } from 'ipfs-core-types';
import { QueryOptions, DQuery } from '@dao-xyz/peerbit-dquery';
import { Identity } from '@dao-xyz/ipfs-log';
import { Program, ProgramInitializationOptions } from '@dao-xyz/peerbit-program'
import { Address, IInitializationOptions } from '@dao-xyz/peerbit-dstore';
import { MultipleQueriesType, QueryType } from './query-interface';
import { Result, Results } from './result';
import { StoreAddressMatchQuery } from './context';

export const getQueryTopic = (region: string): string => {
    return region + '/query';
}
/* export type IQueryStoreOptions<T> = IStoreOptions<T> & { queryRegion?: string, subscribeToQueries: boolean };
 */

export type SearchContext = { address: Address };
export type DSearchInitializationOptions<T> = { canRead?(signature: SignatureWithKey | undefined): Promise<boolean>, context: SearchContext, queryHandler: (query: QueryType) => Promise<Result[]> };

@variant([0, 2])
export class DSearch<T> extends Program {

    @field({ type: DQuery })
    _query: DQuery<QueryType, Results>

    _queryHandler: (query: QueryType) => Promise<Result[]>
    _context: SearchContext;
    constructor(properties: { query: DQuery<QueryType, Results>, name?: string }) {
        super(properties)
        if (properties) {
            this._query = properties.query;
        }
    }


    public async setup(options: DSearchInitializationOptions<T>) {
        this._queryHandler = options.queryHandler;
        this._context = options.context;
        await this._query.setup({ canRead: options.canRead, responseHandler: this._onQueryMessage.bind(this), queryType: QueryType, responseType: Results })
    }

    async _onQueryMessage(query: QueryType, from?: SignKey): Promise<Results | undefined> {

        if (query instanceof MultipleQueriesType) {
            // Handle context queries
            for (const q of query.queries) {
                if (q instanceof StoreAddressMatchQuery) {
                    if (q.address != this._context.address.toString()) {
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
