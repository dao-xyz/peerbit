import { Constructor } from '@dao-xyz/borsh';
import { Store } from '@dao-xyz/orbit-db-store';
import { KeyValueIndex } from './key-value-index';
import OrbitDB from 'orbit-db';
import { IQueryStoreOptions } from '@dao-xyz/orbit-db-query-store';
import { BStoreOptions } from '@dao-xyz/orbit-db-bstores';
export declare type IKeyValueStoreOptions<T> = IQueryStoreOptions<KeyValueIndex<T>> & {
    clazz: Constructor<T>;
};
export declare const BINARY_KEYVALUE_STORE_TYPE = "bkv_store";
export declare class BinaryKeyValueStoreOptions<T> extends BStoreOptions<BinaryKeyValueStore<T>> {
    objectType: string;
    constructor(opts: {
        objectType: string;
    });
    newStore(address: string, orbitDB: OrbitDB, typeMap: {
        [key: string]: Constructor<any>;
    }, options: IKeyValueStoreOptions<T>): Promise<BinaryKeyValueStore<T>>;
    get identifier(): string;
}
export declare class BinaryKeyValueStore<T> extends Store<KeyValueIndex<T>, IKeyValueStoreOptions<T>> {
    _type: string;
    constructor(ipfs: any, id: any, dbname: any, options: IKeyValueStoreOptions<T>);
    get(key: string): T;
    set(key: string, data: T, options?: {}): Promise<unknown>;
    put(key: string, data: T, options?: {}): Promise<unknown>;
    del(key: string, options?: {}): Promise<unknown>;
    get size(): number;
}
