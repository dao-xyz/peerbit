/**
 * We have to provide all stores implementations in a sharded compatible form, so that
 * peers can replicate stores upon request (on demand).
 * This is why we are creating an serializable version of the store options.
 * (Store options are passed in the replication request)
 */

import { Constructor } from "@dao-xyz/borsh";
import OrbitDB from "orbit-db";
import Store from "orbit-db-store";

export type IQueryStoreOptions = IStoreOptions & { subscribeToQueries: boolean };

export class StoreOptions<B extends Store<any, any>> {

    constructor() {

    }

    async newStore(_address: string, _orbitDB: OrbitDB, _typeMap: { [key: string]: Constructor<any> }, _options: IQueryStoreOptions): Promise<B> {
        throw new Error("Not implemented")
    }

    get identifier(): string {
        throw new Error("Not implemented")
    }
}