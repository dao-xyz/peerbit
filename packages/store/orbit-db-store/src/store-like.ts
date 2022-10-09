import { Address, Addressable } from "./io.js";
import { IPFS } from 'ipfs-core-types'
import { IInitializationOptions } from "./store.js";
import { Identity, Log } from "@dao-xyz/ipfs-log";
import Cache from '@dao-xyz/orbit-db-cache';
import { Entry } from "@dao-xyz/ipfs-log";
import { EntryWithRefs } from "./entry-with-refs.js";

export interface Initiable<T> {
    init?(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<Initiable<T>>;
}



export interface StoreLike<T> extends Addressable, Initiable<T> {
    close?(): Promise<void>;
    drop?(): Promise<void>;
    load?(): Promise<void>;
    close?(): Promise<void>;
    save(ipfs: any, options?: {
        format?: string;
        pin?: boolean;
        timeout?: number;
    }): Promise<Address>
    sync(heads: (Entry<T> | EntryWithRefs<T>)[]): Promise<void>

    get replicationTopic(): string;
    /*     get events(): EventEmitter;
     */
    get address(): Address
    get oplog(): Log<T>
    get cache(): Cache<any>
    get id(): string;
    get replicate(): boolean;
    /*   get allowForks(): boolean; */

}

