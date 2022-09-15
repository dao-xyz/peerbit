import { Constructor } from "@dao-xyz/borsh";
import { Hashable } from "./utils";
import { Entry } from "@dao-xyz/ipfs-log-entry";
import { Log } from "@dao-xyz/ipfs-log";
export declare class Operation<T> {
}
export declare class PutOperation<T> extends Operation<T> {
    key: string;
    data: Uint8Array;
    _value: T;
    constructor(props?: {
        key: string;
        data: Uint8Array;
        value?: T;
    });
    get value(): T | undefined;
}
export declare class PutAllOperation<T> extends Operation<T> {
    docs: PutOperation<T>[];
    constructor(props?: {
        docs: PutOperation<T>[];
    });
}
export declare class DeleteOperation extends Operation<any> {
    key: string;
    constructor(props?: {
        key: string;
    });
}
export interface IndexedValue<T> {
    key: string;
    value: T;
    entry: Entry<Operation<T>>;
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
    deserializeOrPass(value: PutOperation<T>): T;
    deserializeOrItem(entry: Entry<Operation<T>>, operation: PutOperation<T>): IndexedValue<T>;
}
