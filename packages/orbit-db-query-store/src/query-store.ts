import { IStoreOptions, Index, Store } from '@dao-xyz/orbit-db-store'
import { Identity } from 'orbit-db-identity-provider';
import { deserialize, serialize } from '@dao-xyz/borsh';
import { Message } from 'ipfs-core-types/types/src/pubsub'
import { QueryRequestV0, QueryResponseV0, Result, query, MultipleQueriesType, ShardMatchQuery } from '@dao-xyz/bquery';
import { IPFS as IPFSInstance } from "ipfs-core-types";

export const getQueryTopic = (region: string): string => {
  return region + '/query';
}
export type IQueryStoreOptions<X extends Index> = IStoreOptions<X> & { queryRegion: string };

export class QueryStore<X extends Index, O extends IQueryStoreOptions<X>> extends Store<X, O> {

  _subscribed: boolean = false
  queryRegion: string;
  context: { cid: string };

  _initializationPromise?: Promise<void>;
  constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: O) {
    super(ipfs, id, dbname, options)
    this.queryRegion = options.queryRegion;
  }

  public async subscribeToQueries(context: { cid: string }) {
    await this._initializationPromise;
    this.context = context;
    this._initializationPromise = this._subscribeToQueries();
  }

  public async close(): Promise<void> {
    await this._initializationPromise;
    await this._ipfs.pubsub.unsubscribe(this.queryTopic);
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
    if (!this.context.cid) {
      throw new Error("Not initialized");
    }

    if (this._subscribed) {
      return
    }

    await this._ipfs.pubsub.subscribe(this.queryTopic, async (msg: Message) => {
      try {
        let query = deserialize(Buffer.from(msg.data), QueryRequestV0);
        if (query.type instanceof MultipleQueriesType) {
          // Handle context queries
          for (const q of query.type.queries) {
            if (q instanceof ShardMatchQuery) {
              if (q.cid != this.context.cid) {
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
    })
    this._subscribed = true;
  }

  public query(queryRequest: QueryRequestV0, responseHandler: (response: QueryResponseV0,) => void, waitForAmount?: number, maxAggregationTime?: number): Promise<void> {
    return query(this._ipfs.pubsub, this.queryTopic, queryRequest, responseHandler, waitForAmount, maxAggregationTime);
  }

  public get queryTopic(): string {
    if (!this.address || !this.queryRegion) {
      throw new Error("Not initialized");
    }

    return getQueryTopic(this.queryRegion);
  }
}

