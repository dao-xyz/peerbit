import { Constructor } from "@dao-xyz/borsh";
import { Hashable } from "./utils";
import { Entry } from "@dao-xyz/ipfs-log-entry";
import { Log } from "@dao-xyz/ipfs-log";
export declare class Operation {
}
export declare class PutOperation extends Operation {
    key: string;
    value: Uint8Array;
    constructor(props?: {
        key: string;
        value: Uint8Array;
    });
}
export declare class PutAllOperation extends Operation {
    docs: PutOperation[];
    constructor(props?: {
        docs: PutOperation[];
    });
}
export declare class DeleteOperation extends Operation {
    key: string;
    constructor(props?: {
        key: string;
    });
}
export interface IndexedValue<T> {
    key: string;
    value: T;
    entry: Entry<Operation>;
}
export declare class DocumentIndex<T> {
    _index: {
        [key: string]: IndexedValue<T>;
    };
    clazz: Constructor<T>;
    constructor();
    init(clazz: Constructor<T>): void;
    get(key: Hashable): IndexedValue<T>;
    updateIndex(oplog: Log<IndexedValue<T>>): Promise<void>;
    deserializeOrPass(value: Uint8Array | T): T;
    deserializeOrItem(entry: Entry<Operation>, operation: PutOperation): IndexedValue<T>;
}
