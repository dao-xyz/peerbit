/// <reference types="orbit-db" />
import { Constructor } from "@dao-xyz/borsh";
import { IdentityAsJson } from "orbit-db-identity-provider";
import { ToStringable } from "./utils";
export interface LogEntry<T> {
    identity: IdentityAsJson;
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
    get(key: ToStringable, fullOp?: boolean): (LogEntry<T> | T);
    updateIndex(oplog: any): Promise<void>;
    deserializeOrPass(value: string | T): T;
    deserializeOrItem(item: LogEntry<T | string>): LogEntry<T>;
}
