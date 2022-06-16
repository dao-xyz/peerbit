import { Constructor } from '@dao-xyz/borsh';
import Store from 'orbit-db-store';
import { KeyValueIndex } from './key-value-index';
import OrbitDB from 'orbit-db';
import { StoreOptions, IQueryStoreOptions } from '@dao-xyz/orbit-db-bstores';
export declare const BINARY_KEYVALUE_STORE_TYPE = "bkv_store";
export declare class BinaryKeyValueStoreOptions<T> extends StoreOptions<BinaryKeyValueStore<T>> {
    objectType: string;
    constructor(opts: {
        objectType: string;
    });
    newStore(address: string, orbitDB: OrbitDB, typeMap: {
        [key: string]: Constructor<any>;
    }, options: IQueryStoreOptions): Promise<BinaryKeyValueStore<T>>;
    get identifier(): string;
}
export declare class BinaryKeyValueStore<T> extends Store<T, KeyValueIndex<T>> {
    _type: string;
    constructor(ipfs: any, id: any, dbname: any, options: IStoreOptions & {
        clazz: Constructor<T>;
    });
    get(key: string): T;
    set(key: string, data: T, options?: {}): Promise<string>;
    put(key: string, data: T, options?: {}): Promise<string>;
    del(key: string, options?: {}): Promise<string>;
}
