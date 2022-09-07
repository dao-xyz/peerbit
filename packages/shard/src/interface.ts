

import { variant } from "@dao-xyz/borsh";
import { IStoreOptions } from '@dao-xyz/orbit-db-store';
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { Shard } from "./shard";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { AnyPeer } from "./peer";

// Extends results source in order to be queried

/* @variant([0, 1])
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

    async init(orbitDB: OrbitDB, options: IStoreOptions<Shard<T>, any, any>): Promise<void> {
        options.typeMap[Shard.name] = Shard;
        return await super.init(orbitDB, options);
    }



    async loadShard(cid: string, peer: AnyPeer): Promise<Shard<T>> {
        // Get the latest shard that have non empty peer
        let shard = this.db.get(cid)[0]
        await shard.value.init(peer);
        return shard.value;
    }
}


 */