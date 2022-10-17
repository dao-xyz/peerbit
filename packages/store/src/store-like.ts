import { Address, Addressable } from "./io.js";
import { IPFS } from 'ipfs-core-types'
import { IInitializationOptions } from "./store.js";
import { Identity, Log, Payload } from "@dao-xyz/ipfs-log";

export interface Initiable<T> {
    init?(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<this>;
}
export interface Saveable {
    save(ipfs: any, options?: {
        format?: string;
        pin?: boolean;
        timeout?: number;
    }): Promise<Address>
}

/* export interface StoreLike<T> extends Addressable, Initiable<T>, Saveable {
    close?(): Promise<void>;
    drop?(): Promise<void>;
    load?(): Promise<void>;
    close?(): Promise<void>;
    sync(heads: (Entry<T> | EntryWithRefs<T>)[]): Promise<void>
    get address(): Address
    get oplog(): Log<T>
    get id(): string;
    get replicate(): boolean;
    canAppend?(payload: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<SignatureWithKey>): Promise<boolean>
} */