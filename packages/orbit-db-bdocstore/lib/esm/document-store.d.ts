/// <reference types="orbit-db" />
import { DocumentIndex } from './document-index';
import { Identity } from 'orbit-db-identity-provider';
import { Constructor } from '@dao-xyz/borsh';
import { QueryRequestV0, Result } from '@dao-xyz/bquery';
import { IPFS as IPFSInstance } from "ipfs-core-types";
import { QueryStore, QueryStoreOptions } from '@dao-xyz/orbit-db-query-store';
export declare const BINARY_DOCUMENT_STORE_TYPE = "bdocstore";
export declare type DocumentStoreOptions<T> = IStoreOptions & QueryStoreOptions & {
    indexBy?: string;
    clazz: Constructor<T>;
};
export declare class BinaryDocumentStore<T> extends QueryStore<T, DocumentIndex<T>> {
    _type: string;
    _subscribed: boolean;
    subscribeToQueries: boolean;
    constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: DocumentStoreOptions<T>);
    get index(): DocumentIndex<T>;
    get(key: any, caseSensitive?: boolean): T[];
    load(amount?: number, opts?: {}): Promise<void>;
    close(): Promise<void>;
    queryDocuments(mapper: ((doc: T) => boolean), options?: {
        fullOp?: boolean;
    }): T[] | {
        payload: Payload<T>;
    }[];
    queryHandler(query: QueryRequestV0): Promise<Result[]>;
    batchPut(docs: T[], onProgressCallback: any): Promise<any>;
    put(doc: T, options?: {}): Promise<string>;
    putAll(docs: T[], options?: {}): Promise<string>;
    del(key: any, options?: {}): Promise<string>;
}
