import { IStoreOptions, Store, Address, IInitializationOptions } from '@dao-xyz/orbit-db-store'
import { field, option } from '@dao-xyz/borsh';
import type { Message } from '@libp2p/interface-pubsub'
import { QueryRequestV0, QueryResponseV0, Result, MultipleQueriesType, StoreAddressMatchQuery } from '@dao-xyz/query-protocol';
import { X25519PublicKey } from 'sodium-plus';
import { AccessError, decryptVerifyInto } from "@dao-xyz/peerbit-crypto";
import { AccessController } from '@dao-xyz/orbit-db-store';
import { ReadWriteAccessController } from './read-write-access-controller';
import { IPFS } from 'ipfs-core-types';
import { PublicKey } from '@dao-xyz/peerbit-crypto';
import { query, respond } from './io.js';

export const getQueryTopic = (region: string): string => {
    return region + '/query';
}
/* export type IQueryStoreOptions<T> = IStoreOptions<T> & { queryRegion?: string, subscribeToQueries: boolean };
 */
export class QueryStore<T> extends Store<T> {

    @field({ type: option('string') })

    queryRegion?: string;
    subscribeToQueries: boolean = true;

    _subscribed: boolean = false;
    _initializationPromise?: Promise<void>;
    _onQueryMessageBinded: any = undefined;

    constructor(properties: { queryRegion?: string, accessController: AccessController<T> }) {
        super(properties)
        if (properties) {
            this.queryRegion = properties.queryRegion;
            // is this props ser or not??? 

            if (properties.accessController && properties.accessController instanceof ReadWriteAccessController === false) {
                throw new Error("Expected ReadWriteAccessController for a store that accepts queries");
            }
        }
    }

    public async init(ipfs: IPFS, publicKey: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<T>) {
        await super.init(ipfs, publicKey, sign, options)
        if (this.subscribeToQueries) {
            this._subscribeToQueries();
        }
        return this;
    }


    public async close(): Promise<void> {
        await this._initializationPromise;
        await this._ipfs.pubsub.unsubscribe(this.queryTopic, this._onQueryMessageBinded);
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

        this._onQueryMessageBinded = this._onQueryMessage.bind(this);
        this._initializationPromise = this._ipfs.pubsub.subscribe(this.queryTopic, this._onQueryMessageBinded)
        await this._initializationPromise;
        this._subscribed = true;
    }

    async _onQueryMessage(msg: Message): Promise<void> {

        try {
            // TODO try catch deserialize parse to properly handle migrations (prevent old clients to break)



            let query: QueryRequestV0 = undefined;
            try {
                query = await decryptVerifyInto(msg.data, QueryRequestV0, this._oplog._encryption, {
                    isTrusted: (this.accessController || this.fallbackAccessController).allowAll ? undefined : async (key) => {
                        const accessController = (this.accessController || this.fallbackAccessController) as ReadWriteAccessController<any>;
                        return (accessController).canRead(key)
                    }
                })
            } catch (error) {
                if (error instanceof AccessError) {
                    return;
                }
                throw error;
            }


            // ACL
            /*           if (this.access && !await this.accessController.canRead(xyz, k)) {
                          return;
                      } */


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

                await respond(this._ipfs.pubsub, this.queryTopic, query, response, {
                    encryption: this._oplog._encryption, signer: async (bytes) => {
                        return {
                            signature: await this._oplog._sign(bytes),
                            publicKey: this._oplog._publicKey
                        }
                    }
                })
            }
            else {
                // Unsupported query type
                return;
            }

        } catch (error) {
            console.error(error)
        }
    }

    public query(queryRequest: QueryRequestV0, responseHandler: (response: QueryResponseV0) => void, options: {
        signer?: (bytes: Uint8Array) => Promise<{
            signature: Uint8Array;
            publicKey: PublicKey;
        }>
        waitForAmount?: number,
        maxAggregationTime?: number,
        recievers?: X25519PublicKey[]

    }): Promise<void> {
        return query(this._ipfs.pubsub, this.queryTopic, queryRequest, responseHandler, options);
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
