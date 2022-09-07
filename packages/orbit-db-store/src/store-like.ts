import { Addressable } from "./io";
import { IPFS } from 'ipfs-core-types/src/'
import { IInitializationOptions } from "./store";
import { Identity } from "@dao-xyz/orbit-db-identity-provider";

export interface StoreLike extends Addressable {
    init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<void>;
    close?(): Promise<void>;
    drop?(): Promise<void>;
    load?(): Promise<void>;

}

