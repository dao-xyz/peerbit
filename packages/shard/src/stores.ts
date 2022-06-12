/**
 * We have to provide all stores implementations in a sharded compatible form, so that
 * peers can replicate stores upon request (on demand).
 * This is why we are creating an serializable version of the store options.
 * (Store options are passed in the replication request)
 */

import { field, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore, DocumentStoreOptions, BINARY_DOCUMENT_STORE_TYPE } from "@dao-xyz/orbit-db-bdocstore";
import { BinaryFeedStore, BINARY_FEED_STORE_TYPE } from "@dao-xyz/orbit-db-bfeedstore";
import { BinaryKeyValueStore, BINARY_KEYVALUE_STORE_TYPE } from '@dao-xyz/orbit-db-bkvstore';
import OrbitDB from "orbit-db";
import FeedStore from "orbit-db-feedstore";
import Store from "orbit-db-store";
import { TypedBehaviours } from "./shard";
import { delay, waitFor } from "./utils";

OrbitDB.addDatabaseType(BINARY_DOCUMENT_STORE_TYPE, BinaryDocumentStore as any)
OrbitDB.addDatabaseType(BINARY_KEYVALUE_STORE_TYPE, BinaryKeyValueStore as any)
OrbitDB.addDatabaseType(BINARY_FEED_STORE_TYPE, BinaryFeedStore as any)

export class StoreOptions<B extends Store<any, any>> {

    constructor() {

    }

    async newStore(_address: string, _orbitDB: OrbitDB, _defaultOptions: IStoreOptions, _behaviours: TypedBehaviours): Promise<B> {
        throw new Error("Not implemented")
    }
    get identifier(): string {
        throw new Error("Not implemented")
    }


}

@variant(0)
export class FeedStoreOptions<T> extends StoreOptions<FeedStore<T>> {

    constructor() {
        super();
    }

    async newStore(address: string, orbitDB: OrbitDB, defaultOptions: IStoreOptions, _behaviours: TypedBehaviours): Promise<FeedStore<T>> {
        return orbitDB.feed(address, defaultOptions)
    }

    get identifier(): string {
        return 'feed'
    }

}

@variant(1)
export class BinaryDocumentStoreOptions<T> extends StoreOptions<BinaryDocumentStore<T>> {

    @field({ type: 'String' })
    indexBy: string;

    @field({ type: 'String' })
    objectType: string;

    constructor(opts: {
        indexBy: string;
        objectType: string;

    }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
    async newStore(address: string, orbitDB: OrbitDB, defaultOptions: IStoreOptions, behaviours: TypedBehaviours): Promise<BinaryDocumentStore<T>> {
        let clazz = behaviours.typeMap[this.objectType];
        if (!clazz) {
            throw new Error(`Undefined type: ${this.objectType}`);
        }
        return orbitDB.open<BinaryDocumentStore<T>>(address, { ...defaultOptions, ...{ clazz, create: true, type: BINARY_DOCUMENT_STORE_TYPE, indexBy: this.indexBy, subscribeToQueries: defaultOptions.replicate } } as DocumentStoreOptions<T>)
    }

    get identifier(): string {
        return BINARY_DOCUMENT_STORE_TYPE
    }


}


@variant(2)
export class BinaryKeyValueStoreOptions<T> extends StoreOptions<BinaryKeyValueStore<T>> {


    @field({ type: 'String' })
    objectType: string;

    constructor(opts: {
        objectType: string;

    }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
    async newStore(address: string, orbitDB: OrbitDB, defaultOptions: IStoreOptions, behaviours: TypedBehaviours): Promise<BinaryKeyValueStore<T>> {
        let clazz = behaviours.typeMap[this.objectType];
        if (!clazz) {
            throw new Error(`Undefined type: ${this.objectType}`);
        }

        return orbitDB.open<BinaryKeyValueStore<T>>(address, { ...defaultOptions, ...{ clazz, create: true, type: BINARY_KEYVALUE_STORE_TYPE } } as any)
    }

    get identifier(): string {
        return BINARY_KEYVALUE_STORE_TYPE
    }

}


@variant(3)
export class BinaryFeedStoreOptions<T> extends StoreOptions<BinaryFeedStore<T>> {


    @field({ type: 'String' })
    objectType: string;

    constructor(opts: {
        objectType: string;

    }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
    async newStore(address: string, orbitDB: OrbitDB, defaultOptions: IStoreOptions, behaviours: TypedBehaviours): Promise<BinaryFeedStore<T>> {
        let clazz = behaviours.typeMap[this.objectType];
        if (!clazz) {
            throw new Error(`Undefined type: ${this.objectType}`);
        }

        return orbitDB.open<BinaryFeedStore<T>>(address, { ...defaultOptions, ...{ clazz, create: true, type: BINARY_FEED_STORE_TYPE } } as any)
    }

    get identifier(): string {
        return BINARY_FEED_STORE_TYPE
    }

}

const MAX_REPLICATION_WAIT_TIME = 15 * 1000;

export const waitForReplicationEvents = async (store: Store<any, any>, waitForReplicationEventsCount: number) => {
    /**
     * This method is flaky
     * First we check the progress of replicatoin
     * then we check a custom replicated boolean, as the replicationStatus
     * is not actually tracking whether the store is loaded
     */

    if (!waitForReplicationEventsCount)
        return

    await waitFor(() => !!store.replicationStatus && waitForReplicationEventsCount <= store.replicationStatus.max)

    let startTime = +new Date;
    while (store.replicationStatus.progress < store.replicationStatus.max) {
        await delay(50);
        if (+new Date - startTime > MAX_REPLICATION_WAIT_TIME) {
            console.warn("Max replication time, aborting wait for")
            return;
        }
    }

    // await waitFor(() => store["replicated"])
    return;
}
