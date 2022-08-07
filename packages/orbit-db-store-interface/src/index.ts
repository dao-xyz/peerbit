

import { field, variant } from "@dao-xyz/borsh";
import { IStoreOptions, Store } from '@dao-xyz/orbit-db-store';
import { BStoreOptions } from "@dao-xyz/orbit-db-bstores";
import * as events from 'events';
import { waitForReplicationEvents } from "./utils";
import { BinaryPayload } from "@dao-xyz/bpayload";
import { OrbitDB } from "@dao-xyz/orbit-db";

// Extends results source in order to be queried
//@variant([0, 1])
@variant("interface")
export class DBInterface extends BinaryPayload {

    get initialized(): boolean {
        throw new Error("Not implemented")
    }


    get loaded(): boolean {
        throw new Error("Not implemented")
    }

    close() {
        throw new Error("Not implemented")

    }

    async init(_orbitDB: OrbitDB, _options: IStoreOptions<any, any, any>): Promise<void> {
        throw new Error("Not implemented")
    }

    async load(): Promise<void> {
        throw new Error("Not implemented")
    }

    clone(): DBInterface {
        throw new Error("Not implemented")
    }

}

// Every interface has to have its own variant, else DBInterface can not be
// used as a deserialization target.
@variant([0, 0])
export abstract class SingleDBInterface<T, B extends Store<any, any, any, any>> extends DBInterface {

    @field({ type: 'String' })
    name: string;

    @field({ type: 'String' })
    address: string;

    @field({ type: BStoreOptions })
    storeOptions: BStoreOptions<B>;

    db: B;
    _orbitDB: OrbitDB
    _options: IStoreOptions<T, T, any>

    constructor(opts?: {
        name: string;
        address?: string;
        storeOptions: BStoreOptions<B>;
    }) {
        super();
        if (opts) {
            this.name = opts.name;
            this.address = opts.address;
            this.storeOptions = opts.storeOptions;
        }
    }

    get options(): IStoreOptions<T, T, any> {
        return this._options;
    }

    async init(orbitDB: OrbitDB, options: IStoreOptions<T, T, any>): Promise<void> {
        this.db = undefined;
        this._options = options;
        this._orbitDB = orbitDB;
        if (!this.address) {
            this.address = (await this._orbitDB.determineAddress(this.name, this.storeOptions.identifier, this.options)).toString();
        }
    }


    async newStore(): Promise<B> {
        if (!this._orbitDB) {
            throw new Error("Not initialized")
        }

        this.db = await this.storeOptions.newStore(this.address ? this.address : this.name, this._orbitDB, this.options);
        this.address = this.db.address.toString();
        return this.db;
    }

    async load(waitForReplicationEventsCount: number = 0): Promise<void> {
        if (!this._orbitDB || !this.initialized) {
            throw new Error("Not initialized")
        }

        if (!this.options.replicate && waitForReplicationEventsCount > 0) {
            throw new Error("Replicate is set to false, but loading expects replication events to happen")
        }

        if (!this.db) {
            await this.newStore() //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        }
        await this.db.load(waitForReplicationEventsCount);

        if (this.options.replicate && waitForReplicationEventsCount > 0) {
            await waitForReplicationEvents(this.db, waitForReplicationEventsCount);
        }
    }


    // TODO this function shopuld perhaps live in the "orbit-db" package and be renamed to something appropiate
    /**
     * Write to DB without fully loading it
     * @param write 
     * @param obj 
     * @param unsubscribe 
     * @returns 
     */
    async write(write: (obj: T) => Promise<any>, obj: T, unsubscribe: boolean = true): Promise<B> {
        let topic = Store.getReplicationTopic(this.address, this._options);
        let subscribed = !!this._orbitDB._pubsub._subscriptions[topic];
        let directConnectionsFromWrite: { [peer: string]: string } = {};
        let preExistingConnections = new Set();
        if (!subscribed) {
            await this._orbitDB._pubsub.subscribe(topic, this._orbitDB._onMessage.bind(this._orbitDB), (address: string, peer: string) => {
                if (this._orbitDB._directConnections[peer]) {
                    preExistingConnections.add(peer);
                }
                this._orbitDB.getChannel(peer, topic)
                //this._orbitDB._onPeerConnected(topic, peer);
                directConnectionsFromWrite[peer] = address;
            })
        }
        await write(obj);
        if (!subscribed && unsubscribe) {
            // TODO: could cause sideeffects if there is another write that wants to access the topic
            await this._orbitDB._pubsub.unsubscribe(topic);

            const removeDirectConnect = peer => {
                const conn = this._orbitDB._directConnections[peer];
                if (conn && !preExistingConnections.has(peer)) {
                    this._orbitDB._directConnections[peer].close()
                    delete this._orbitDB._directConnections[peer]
                }

            }

            // Close all direct connections to peers
            Object.keys(directConnectionsFromWrite).forEach(removeDirectConnect)

            // unbind?
        }
        return this.db
    }

    /* async query(queryRequest: QueryRequestV0, responseHandler: (response: QueryResponseV0) => void, region: string, waitForAmount?: number, maxAggregationTime?: number) {
        if (!this.address) {
            throw new Error("Can not query because DB address is unknown")
        }
        return query(this._peer.node.pubsub, getQueryTopic(region), queryRequest, responseHandler, waitForAmount, maxAggregationTime)
    }
    */



    close() {
        this.db = undefined;
        this._orbitDB = undefined;
    }

    get initialized(): boolean {
        return !!this.address;
    }

    get loaded(): boolean {
        return !!this.db;
    }

    clone(): DBInterface {
        return Reflect.construct(this.constructor, [{
            name: this.name,
            storeOptions: this.storeOptions
        }])
    }

}
