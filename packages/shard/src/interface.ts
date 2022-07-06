

import { field, variant } from "@dao-xyz/borsh";
import { IStoreOptions, Store } from '@dao-xyz/orbit-db-store';
import { BStoreOptions } from "@dao-xyz/orbit-db-bstores";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { DEFAULT_QUERY_REGION, Shard } from "./shard";
import * as events from 'events';
import { waitForReplicationEvents } from "./utils";
import { query } from "@dao-xyz/bquery";
import { getQueryTopic, QueryStore } from "@dao-xyz/orbit-db-query-store";
import { QueryRequestV0 } from "@dao-xyz/bquery";
import { QueryResponseV0 } from "@dao-xyz/bquery";
import { AnyPeer } from "./node";
import { BPayload } from '@dao-xyz/bgenerics';

// Extends results source in order to be queried
@variant([0, 1])
export class DBInterface extends BPayload {

    get initialized(): boolean {
        throw new Error("Not implemented")
    }


    get loaded(): boolean {
        throw new Error("Not implemented")
    }

    close() {
        throw new Error("Not implemented")

    }

    async init(_peer: AnyPeer, _dbNameResolver: (name: string) => string, _options: IStoreOptions<any, any>): Promise<void> {
        throw new Error("Not implemented")
    }

    async load(): Promise<void> {
        throw new Error("Not implemented")
    }

}

// Every interface has to have its own variant, else DBInterface can not be
// used as a deserialization target.
@variant([0, 1])
export class SingleDBInterface<T, B extends Store<T, any, any>> extends DBInterface {

    @field({ type: 'String' })
    name: string;

    @field({ type: 'String' })
    address: string;

    @field({ type: BStoreOptions })
    storeOptions: BStoreOptions<B>;

    db: B;
    _peer: AnyPeer
    _options: IStoreOptions<T, any>
    _dbNameResolver: (key: string) => string;

    constructor(opts?: {
        name: string;
        address?: string;
        storeOptions: BStoreOptions<B>;
    }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }

    get options(): IStoreOptions<T, any> {
        return this._options;
    }

    async init(peer: AnyPeer, dbNameResolver: (name: string) => string, options: IStoreOptions<T, any>): Promise<void> {
        this.db = undefined;
        this._options = options;
        this._dbNameResolver = dbNameResolver;
        this._peer = peer;
        if (!this.address) {
            this.address = (await this._peer.orbitDB.determineAddress(this.getDBName(), this.storeOptions.identifier, this.options)).toString();
        }
    }


    async newStore(): Promise<B> {
        if (!this._peer) {
            throw new Error("Not initialized")
        }

        this.db = await this.storeOptions.newStore(this.address ? this.address : this.getDBName(), this._peer.orbitDB, this._peer.options.behaviours.typeMap, this.options);
        this.address = this.db.address.toString();
        return this.db;
    }

    async load(waitForReplicationEventsCount: number = 0): Promise<void> {
        if (!this._peer || !this.initialized) {
            throw new Error("Not initialized")
        }

        if (!this.db) {
            await this.newStore() //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        }
        await this.db.load(waitForReplicationEventsCount);

        if (this._peer.options.isServer) {
            await waitForReplicationEvents(this.db, waitForReplicationEventsCount);
        }
    }


    /**
     * Write to DB without fully loadung it
     * @param write 
     * @param obj 
     * @param unsubscribe 
     * @returns 
     */
    async write(write: (obj: T) => Promise<any>, obj: T, unsubscribe: boolean = true): Promise<B> {
        let topic = this.address.toString();
        let subscribed = !!this._peer.orbitDB._pubsub._subscriptions[topic];
        let directConnectionsFromWrite = {};
        if (!subscribed) {
            await this._peer.orbitDB._pubsub.subscribe(topic, this._peer.orbitDB._onMessage.bind(this._peer.orbitDB), (address: string, peer: any) => {
                this._peer.orbitDB._onPeerConnected(address, peer);
                directConnectionsFromWrite[peer] = address;
            })
        }
        await write(obj);
        if (!subscribed && unsubscribe) {
            // TODO: could cause sideeffects if there is another write that wants to access the topic
            await this._peer.orbitDB._pubsub.unsubscribe(topic);

            const removeDirectConnect = e => {
                const conn = this._peer.orbitDB._directConnections[e];
                if (conn) {
                    this._peer.orbitDB._directConnections[e].close()
                    delete this._peer.orbitDB._directConnections[e]
                }

            }

            // Close all direct connections to peers
            Object.keys(directConnectionsFromWrite).forEach(removeDirectConnect)

            // unbind?
        }
        return this.db
    }

    async query(queryRequest: QueryRequestV0, responseHandler: (response: QueryResponseV0) => void, region: string = DEFAULT_QUERY_REGION, waitForAmount?: number, maxAggregationTime?: number) {
        if (!this.address) {
            throw new Error("Can not query because DB address is unknown")
        }
        return query(this._peer.node.pubsub, getQueryTopic(region), queryRequest, responseHandler, waitForAmount, maxAggregationTime)
    }




    getDBName(): string {
        return this._dbNameResolver(this.name);
    }
    close() {
        this.db = undefined;
        this._peer = undefined;
    }

    get initialized(): boolean {
        return !!this.address;
    }

    get loaded(): boolean {
        return !!this.db;
    }

}


@variant([0, 0])
export class RecursiveShardDBInterface<T extends DBInterface> extends DBInterface {

    @field({ type: SingleDBInterface })
    db: SingleDBInterface<Shard<T>, BinaryDocumentStore<Shard<T>>>;

    constructor(opts?: { db: SingleDBInterface<Shard<T>, BinaryDocumentStore<Shard<T>>> }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }

    get initialized(): boolean {
        return this.db.initialized
    }

    close() {
        this.db.close();
    }

    async init(peer: AnyPeer, dbNameResolver: (name: string) => string, options: IStoreOptions<Shard<T>, any>): Promise<void> {
        await this.db.init(peer, dbNameResolver, options);
    }


    async load(waitForReplicationEventsCount = 0): Promise<void> {
        await this.db.load(waitForReplicationEventsCount);
    }


    async loadShard(cid: string): Promise<Shard<T>> {
        // Get the latest shard that have non empty peer
        let shard = this.db.db.get(cid)[0]
        await shard.init(this.db._peer);
        return shard;
    }
    get loaded(): boolean {
        return !!this.db?.loaded;
    }

}

