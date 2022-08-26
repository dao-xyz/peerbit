import { DocumentIndex, IndexedValue, Operation } from './document-index';
import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { Constructor } from '@dao-xyz/borsh';
import { QueryRequestV0, Result } from '@dao-xyz/bquery';
import { IPFS as IPFSInstance } from "ipfs-core-types";
import { QueryStore } from '@dao-xyz/orbit-db-query-store';
import { IQueryStoreOptions } from '@dao-xyz/orbit-db-query-store';
import { BStoreOptions } from '@dao-xyz/orbit-db-bstores';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { BinaryPayload } from '@dao-xyz/bpayload';
export declare const BINARY_DOCUMENT_STORE_TYPE = "bdoc_store";
export declare type DocumentStoreOptions<T> = IQueryStoreOptions<Operation, IndexedValue<T>, DocumentIndex<T>> & {
    indexBy?: string;
    clazz: Constructor<T>;
};
export declare type IBinaryDocumentStoreOptions<T> = IQueryStoreOptions<Operation, IndexedValue<T>, DocumentIndex<T>> & {
    indexBy?: string;
    clazz: Constructor<T>;
};
export declare class BinaryDocumentStoreOptions<T extends BinaryPayload> extends BStoreOptions<BinaryDocumentStore<T>> {
    indexBy: string;
    objectType: string;
    constructor(opts: {
        indexBy: string;
        objectType: string;
    });
    newStore(address: string, orbitDB: OrbitDB, options: IBinaryDocumentStoreOptions<T>): Promise<BinaryDocumentStore<T>>;
    get identifier(): string;
}
export declare class BinaryDocumentStore<T extends BinaryPayload> extends QueryStore<Operation, IndexedValue<T>, DocumentIndex<T>, IBinaryDocumentStoreOptions<T>> {
    _type: string;
    constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: IBinaryDocumentStoreOptions<T>);
    get index(): DocumentIndex<T>;
    get(key: any, caseSensitive?: boolean): IndexedValue<T>[];
    load(amount?: number, opts?: {}): Promise<void>;
    close(): Promise<void>;
    queryDocuments(mapper: ((doc: IndexedValue<T>) => boolean)): IndexedValue<T>[];
    queryHandler(query: QueryRequestV0): Promise<Result[]>;
    batchPut(docs: T[], onProgressCallback: any): Promise<import("ipfs-core-types/src/root").AddResult[]>;
    put(doc: T, options?: {}): Promise<unknown>;
    putAll(docs: T[], options?: {}): Promise<unknown>;
    del(key: any, options?: {}): Promise<unknown>;
    get size(): number;
}
