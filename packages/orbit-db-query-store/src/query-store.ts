import { IStoreOptions, Index, Store } from '@dao-xyz/orbit-db-store'
import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { deserialize, serialize } from '@dao-xyz/borsh';
import { Message } from 'ipfs-core-types/types/src/pubsub'
import { QueryRequestV0, QueryResponseV0, Result, query, MultipleQueriesType, StoreAddressMatchQuery } from '@dao-xyz/bquery';
import { IPFS as IPFSInstance } from "ipfs-core-types";

export const getQueryTopic = (region: string): string => {
    return region + '/query';
}
export type IQueryStoreOptions<T, X, I extends Index<T, X>> = IStoreOptions<T, X, I> & { queryRegion?: string, subscribeToQueries: boolean };

export class QueryStore<T, X, I extends Index<T, X>, O extends IQueryStoreOptions<T, X, I>> extends Store<T, X, I, O> {

    _subscribed: boolean = false
    queryRegion?: string;
    subscribeToQueries: boolean;
    _initializationPromise?: Promise<void>;

    constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: O) {
        super(ipfs, id, dbname, options)
        this.queryRegion = options.queryRegion;
        this.subscribeToQueries = options.subscribeToQueries;
        if (this.subscribeToQueries) {
            this._subscribeToQueries();
        }
    }



    public async close(): Promise<void> {
        await this._initializationPromise;
        await this._ipfs.pubsub.unsubscribe(this.queryTopic, this._onQueryMessage);
        this._subscribed = false;
        await super.close();
    }

    public async load(amount?: number, opts?: {}): Promise<void> {
        await super.load(amount, opts);
    }

    async queryHandler(_query: QueryRequestV0): Promise<Result[]> {
        throw new Error("Not implemented");
    }

    async _subscribeToQueries(): Promise<void> {
        this._initializationPromise = null;
        if (this._subscribed) {
            return
        }

        this._initializationPromise = this._ipfs.pubsub.subscribe(this.queryTopic, this._onQueryMessage)
        await this._initializationPromise;
        this._subscribed = true;
    }

    async _onQueryMessage(msg: Message): Promise<void> {

        try {
            // TODO try catch deserialize parse to properly handle migrations (prevent old clients to break)
            let query = deserialize(Buffer.from(msg.data), QueryRequestV0);
            if (query.type instanceof MultipleQueriesType) {
                // Handle context queries
                for (const q of query.type.queries) {
                    if (q instanceof StoreAddressMatchQuery) {
                        if (q.address != this.address.toString()) {
                            // This query is not for me!
                            return;
                        }
                    }
                }

                // Handle non context queries
                const results = await this.queryHandler(query);
                if (!results || results.length == 0) {
                    return;
                }
                let response = new QueryResponseV0({
                    results
                });

                let bytes = serialize(response);
                await this._ipfs.pubsub.publish(
                    query.getResponseTopic(this.queryTopic),
                    bytes
                )
            }
            else {
                // Unsupported query type
                return;
            }

        } catch (error) {
            console.error(error)
        }
    }

    public query(queryRequest: QueryRequestV0, responseHandler: (response: QueryResponseV0,) => void, waitForAmount?: number, maxAggregationTime?: number): Promise<void> {
        return query(this._ipfs.pubsub, this.queryTopic, queryRequest, responseHandler, waitForAmount, maxAggregationTime);
    }

    public get queryTopic(): string {
        if (!this.address) {
            throw new Error("Not initialized");
        }
        if (this.queryRegion)
            return getQueryTopic(this.queryRegion); // this store is accessed through some shared query group
        else {
            return getQueryTopic(this.address.toString()); // this tore is accessed by querying the store directly
        }
    }
}

