import { Constructor } from "@dao-xyz/borsh";
import { IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";
import { Hashable } from "./utils";
export interface LogEntry<T> {
    identity: IdentitySerializable;
    payload: Payload<T>;
}
export interface Payload<T> {
    op?: string;
    key?: string;
    value: T;
}
export declare class DocumentIndex<T> {
    _index: {
        [key: string]: LogEntry<T>;
    };
    clazz: Constructor<T>;
    constructor();
    init(clazz: Constructor<T>): void;
    get(key: Hashable, fullOp?: boolean): (LogEntry<T> | T);
    updateIndex(oplog: any): Promise<void>;
    deserializeOrPass(value: string | T): T;
    deserializeOrItem(item: LogEntry<T | string>): LogEntry<T>;
}
