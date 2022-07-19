/**
 * We have to provide all stores implementations in a sharded compatible form, so that
 * peers can replicate stores upon request (on demand).
 * This is why we are creating an serializable version of the store options.
 * (Store options are passed in the replication request)
 */

import { OrbitDB } from "@dao-xyz/orbit-db";
import { IStoreOptions, Store } from '@dao-xyz/orbit-db-store';


export class BStoreOptions<B extends Store<any, any, any>> {

    constructor() {

    }

    async newStore(_address: string, _orbitDB: OrbitDB, _options: IStoreOptions<any, any>): Promise<B> {
        throw new Error("Not implemented")
    }


    get identifier(): string {
        throw new Error("Not implemented")
    }
}