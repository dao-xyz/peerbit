

import { field, variant } from "@dao-xyz/borsh";
import { IStoreOptions } from '@dao-xyz/orbit-db-store';
import { BinaryDocumentStore, BinaryDocumentStoreOptions } from "@dao-xyz/orbit-db-bdocstore";
import { Shard } from "./shard";
import { SingleDBInterface, DBInterface } from '@dao-xyz/orbit-db-store-interface';
import { BStoreOptions } from "@dao-xyz/orbit-db-bstores";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { AnyPeer } from "./peer";

// Extends results source in order to be queried

@variant([0, 1])
export class RecursiveShardDBInterface<T extends DBInterface> extends SingleDBInterface<Shard<T>, BinaryDocumentStore<Shard<T>>> {

    constructor(opts?: { name: string; address?: string }) {
        super({
            name: opts?.name,
            address: opts?.address,
            storeOptions: new BinaryDocumentStoreOptions<Shard<T>>({
                indexBy: 'cid',
                objectType: Shard.name
            })
        })
    }

    async init(orbitDB: OrbitDB, options: IStoreOptions<T, any>): Promise<void> {
        options.typeMap[Shard.name] = Shard;
        return await super.init(orbitDB, options);
    }
    /*  @field({ type: SingleDBInterface })
     db: SingleDBInterface<Shard<T>, BinaryDocumentStore<Shard<T>>>;
 
     constructor(opts?: { db: SingleDBInterface<Shard<T>, BinaryDocumentStore<Shard<T>>> }) {
         super();
         if (opts) {
             Object.assign(this, opts);
         }
     }
 
     get initialized(): boolean {
         return this.db.initialized
     }
 
     close() {
         this.db.close();
     }
 
     async init(peer: AnyPeer,  options: IStoreOptions<Shard<T>, any>): Promise<void> {
         await this.db.init(peer, options);
     }
 
 
     async load(waitForReplicationEventsCount = 0): Promise<void> {
         await this.db.load(waitForReplicationEventsCount);
     } */


    async loadShard(cid: string, peer: AnyPeer): Promise<Shard<T>> {
        // Get the latest shard that have non empty peer
        let shard = this.db.get(cid)[0]
        await shard.init(peer);
        return shard;
    }
    /* get loaded(): boolean {
        return !!this.db?.loaded;
    } */

}


