/**
 * We have to provide all stores implementations in a sharded compatible form, so that
 * peers can replicate stores upon request (on demand).
 * This is why we are creating an serializable version of the store options.
 * (Store options are passed in the replication request)
 */

import { field, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore, BINARY_DOCUMENT_STORE_TYPE } from "@dao-xyz/orbit-db-bdocstore";
import { BinaryKeyValueStore, BINARY_KEYVALUE_STORE_TYPE } from '@dao-xyz/orbit-db-bkvstore';
import OrbitDB from "orbit-db";
import FeedStore from "orbit-db-feedstore";
import Store from "orbit-db-store";
import { TypedBehaviours } from "./shard";

OrbitDB.addDatabaseType(BINARY_DOCUMENT_STORE_TYPE, BinaryDocumentStore as any)
OrbitDB.addDatabaseType(BINARY_KEYVALUE_STORE_TYPE, BinaryKeyValueStore as any)



export class StoreOptions<B extends Store> {

    constructor() {

    }

    async newStore(_address: string, _orbitDB: OrbitDB, _defaultOptions: IStoreOptions, _behaviours: TypedBehaviours): Promise<B> {
        throw new Error("Not implemented")
    }
    get identifier(): string {
        throw new Error("Not implemented")
    }

    get queriable(): boolean {
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
    get queriable(): boolean {
        return false;
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
        return orbitDB.open<BinaryDocumentStore<T>>(address, { ...defaultOptions, ...{ clazz, create: true, type: BINARY_DOCUMENT_STORE_TYPE, indexBy: this.indexBy } } as any)
    }

    get identifier(): string {
        return BINARY_DOCUMENT_STORE_TYPE
    }
    get queriable(): boolean {
        return true;
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

    get queriable(): boolean {
        return true;
    }

}
