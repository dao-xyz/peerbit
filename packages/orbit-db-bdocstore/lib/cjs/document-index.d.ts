import { Constructor } from "@dao-xyz/borsh";
import { Payload } from "./Payload";
export declare class DocumentIndex<T> {
    _index: {
        [key: string]: {
            payload: Payload;
        };
    };
    clazz: Constructor<T>;
    constructor();
    init(clazz: Constructor<T>): void;
    get(key: any, fullOp?: boolean): {
        payload: Payload;
    };
    updateIndex(oplog: any, onProgressCallback: any): void;
    deserializeOrPass(value: string | T): T;
}
