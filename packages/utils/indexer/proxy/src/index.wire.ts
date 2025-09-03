import type { IdKey } from "@peerbit/indexer-interface";
import type { Query, Sort } from "@peerbit/indexer-interface";

// Wire-level RPC interface for IndexContract (serialized payloads)
export interface IndexWire<T extends Record<string, any> = Record<string, any>, N = any> {
    // lifecycle
    start(): Promise<void>;
    stop(): Promise<void>;
    drop(): Promise<void>;

    // CRUD (payloads are serialized Uint8Array)
    get(args: { id: IdKey }): Promise<Uint8Array | undefined>;
    put(args: { value: Uint8Array; schema: new (...a: any[]) => any; id?: IdKey }): Promise<void>;
    del(args: { query: Query[] }): Promise<IdKey[]>;
    sum(args: { key: string[]; query?: Query[] }): Promise<string>;
    count(args: { query?: Query[] }): Promise<number>;
    getSize(): Promise<number>;

    // iterate (handle-based)
    iterateOpen(args: { query?: Query[]; sort?: Sort[] }): Promise<string>;
    iterateNext(args: { iterator: string; amount: number }): Promise<Array<{ id: IdKey; value: Uint8Array }>>;
    iterateAll(args: { iterator: string }): Promise<Array<{ id: IdKey; value: Uint8Array }>>;
    iterateDone(args: { iterator: string }): Promise<boolean>;
    iteratePending(args: { iterator: string }): Promise<number>;
    iterateClose(args: { iterator: string }): Promise<void>;
}


