/// <reference types="orbit-db" />
import { Constructor } from "@dao-xyz/borsh";
import { ToStringable } from "./utils";
export declare class DocumentIndex<T> {
    _index: {
        [key: string]: {
            payload: Payload<T>;
        };
    };
    clazz: Constructor<T>;
    constructor();
    init(clazz: Constructor<T>): void;
    get(key: ToStringable, fullOp?: boolean): ({
        payload: Payload<T>;
    } | T);
    updateIndex(oplog: any, onProgressCallback: any): void;
    deserializeOrPass(value: string | T): T;
    deserializeOrItem(item: LogEntry<T | string>): LogEntry<T>;
}
