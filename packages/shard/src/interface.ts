

import { field, variant } from "@dao-xyz/borsh";
import { Store } from '@dao-xyz/orbit-db-store';
import { BStoreOptions } from "@dao-xyz/orbit-db-bstores";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { Shard } from "./shard";
import * as events from 'events';
import { waitForReplicationEvents } from "./utils";
import { ResultSource, query } from "@dao-xyz/bquery";
import { getQueryTopic } from "@dao-xyz/orbit-db-query-store";
import { QueryRequestV0 } from "@dao-xyz/bquery";
import { QueryResponseV0 } from "@dao-xyz/bquery";

// Extends results source in order to be queried
@variant([0, 1])
export class DBInterface extends ResultSource {

    get initialized(): boolean {
        throw new Error("Not implemented")
    }


    get loaded(): boolean {
        throw new Error("Not implemented")
    }

    close() {
        throw new Error("Not implemented")

    }

    async init(_shard: Shard<any>): Promise<void> {
        throw new Error("Not implemented")
    }

    async load(): Promise<void> {
        throw new Error("Not implemented")
    }

}

// Every interface has to have its own variant, else DBInterface can not be
// used as a deserialization target.
@variant([0, 1])
export class SingleDBInterface<T, B extends Store<any, any>> extends DBInterface {

    @field({ type: 'String' })
    name: string;

    @field({ type: 'String' })
    address: string;

    @field({ type: BStoreOptions })
    storeOptions: BStoreOptions<B>;

    db: B;
    _shard: Shard<any>

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

    async init(shard: Shard<any>): Promise<void> {
        this._shard = shard;
        this.db = undefined;
        if (!this.address) {
            this.address = (await this._shard.peer.orbitDB.determineAddress(this.getDBName(), this.storeOptions.identifier, this._shard.peer.options.defaultOptions)).toString();
        }
    }


    async newStore(): Promise<B> {
        if (!this._shard) {
            throw new Error("Not initialized")
        }

        this.db = await this.storeOptions.newStore(this.address ? this.address : this.getDBName(), this._shard.peer.orbitDB, this._shard.peer.options.behaviours.typeMap, this._shard.peer.options.defaultOptions);
        onReplicationMark(this.db);
        this.address = this.db.address.toString();
        return this.db;
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
        let subscribed = !!this._shard.peer.orbitDB._pubsub._subscriptions[topic];
        let directConnectionsFromWrite = {};
        if (!subscribed) {
            await this._shard.peer.orbitDB._pubsub.subscribe(topic, this._shard.peer.orbitDB._onMessage.bind(this._shard.peer.orbitDB), (address: string, peer: any) => {
                this._shard.peer.orbitDB._onPeerConnected(address, peer);
                directConnectionsFromWrite[peer] = address;
            })
        }
        await write(obj);
        if (!subscribed && unsubscribe) {
            // TODO: could cause sideeffects if there is another write that wants to access the topic
            await this._shard.peer.orbitDB._pubsub.unsubscribe(topic);

            const removeDirectConnect = e => {
                const conn = this._shard.peer.orbitDB._directConnections[e];
                if (conn) {
                    this._shard.peer.orbitDB._directConnections[e].close()
                    delete this._shard.peer.orbitDB._directConnections[e]
                }

            }

            // Close all direct connections to peers
            Object.keys(directConnectionsFromWrite).forEach(removeDirectConnect)

            // unbind?
        }
        return this.db
    }

    async query(queryRequest: QueryRequestV0, responseHandler: (response: QueryResponseV0) => void, waitForAmount?: number, maxAggregationTime?: number) {
        if (!this.address) {
            throw new Error("Can not query because DB address is unknown")
        }
        return query(this._shard.peer.node.pubsub, getQueryTopic(this.address), queryRequest, responseHandler, waitForAmount, maxAggregationTime)
    }


    async load(waitForReplicationEventsCount: number = 0): Promise<void> {
        if (!this._shard || !this.initialized) {
            throw new Error("Not initialized")
        }

        if (!this.db) {
            await this.newStore() //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        }
        await this.db.load();
        if (this._shard.peer.options.isServer) {
            await waitForReplicationEvents(this.db, waitForReplicationEventsCount);
        }
    }

    getDBName(): string {
        return this._shard.getDBName(this.name);
    }
    close() {
        this.db = undefined;
        this._shard = undefined;
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

    async init(shard: Shard<any>): Promise<void> {
        await this.db.init(shard);
    }


    async load(waitForReplicationEventsCount = 0): Promise<void> {
        await this.db.load(waitForReplicationEventsCount);
    }


    async loadShard(cid: string, options: { expectedPeerReplicationEvents?: number } = { expectedPeerReplicationEvents: 0 }): Promise<Shard<T>> {
        // Get the latest shard that have non empty peer
        let shard = this.db.db.get(cid)[0]
        await shard.init(this.db._shard.peer);
        await shard.loadPeers(options.expectedPeerReplicationEvents);
        return shard;
    }
    get loaded(): boolean {
        return !!this.db?.loaded;
    }

}

export const onReplicationMark = (store: Store<any, any>) => store.events.on('replicated', () => {
    store["replicated"] = true // replicated once
});
