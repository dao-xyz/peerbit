import { deserialize, field, serialize, variant } from '@dao-xyz/borsh';
import type { Message } from '@libp2p/interface-pubsub'
import { AccessError, decryptVerifyInto, SignatureWithKey, SignKey, X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { IPFS } from 'ipfs-core-types';
import { QueryRequestV0, QueryResponseV0, query, QueryOptions, DQueryInitializationOptions, DQuery } from '@dao-xyz/peerbit-dquery';
import { Identity } from '@dao-xyz/ipfs-log';
import { Program } from '@dao-xyz/peerbit-program'
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
export type DSearchInitializationOptions<T> = IInitializationOptions<T> & { canRead?(signature: SignatureWithKey | undefined): Promise<boolean>, context: SearchContext, queryHandler: (query: QueryType) => Promise<Result[]> };

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

    public async init(ipfs: IPFS, identity: Identity, options: DSearchInitializationOptions<T>) {
        await super.init(ipfs, identity, options)
        this._queryHandler = options.queryHandler;
        this._context = options.context;
        await this._query.init(ipfs, identity, { ...options, responseHandler: this._onQueryMessage.bind(this), queryType: QueryType, responseType: Results })
        return this;
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
