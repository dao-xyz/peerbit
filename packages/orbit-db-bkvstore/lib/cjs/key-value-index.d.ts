import { Constructor } from "@dao-xyz/borsh";
export declare class KeyValueIndex<T> {
    _index: {
        [key: string]: T;
    };
    clazz: Constructor<T>;
    constructor();
    init(clazz: Constructor<T>): void;
    get(key: any): T;
    updateIndex(oplog: any): void;
}
