/**
 * A decentralized storage of DAO meta data info. 
 * The nodes governing this storage are part of a "official" set of nodes
 * These nodes are trusted by the dao.xyz.dao to act truthfully to serve 
 * the wider community a way of creating, modifying, deleting and searching 
 * DAOs (organizations/communities/groups)
 */

import { field, variant } from '@dao-xyz/borsh';
import { AnyPeer, BinaryDocumentStoreOptions, Shard, RecursiveShard, ServerOptions } from '@dao-xyz/node';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import { IPFSInstanceExtended } from '@dao-xyz/node';
import { Identity } from 'orbit-db-identity-provider';
import OrbitDB from 'orbit-db';


@variant(0)
export class DAO {

    @field({ type: 'String' })
    name: string;

    @field({ type: Shard })
    shard: Shard<BinaryDocumentStore<DAO>>
}

/* class RecursiveShardOrDao {

    constructor() {

    }
}

@variant(0)
class RecursiveShardShard extends RecursiveShardOrDao {

    @field({ type: RecursiveShard })
    shard: RecursiveShard<any>
    constructor() {
        super();
    }
}

variant(1)
class DaoShard extends RecursiveShardOrDao {

    @field({ type: Shard })
    shard: Shard<BinaryDocumentStore<DAO>>
    constructor() {
        super();
    }
} */

export class DaoDB {

    peer: AnyPeer
    genesis: RecursiveShard<BinaryDocumentStore<DAO>>;
    constructor(genesis: RecursiveShard<BinaryDocumentStore<DAO>>) {
        this.genesis = genesis;
    }

    public async create(args: { id: string, rootAddress: string; orbitDB: OrbitDB, identity?: Identity; }): Promise<void> {
        this.peer = new AnyPeer();
        let options = new ServerOptions({
            behaviours: {
                typeMap: {
                    [DAO.name]: DAO
                }
            },
            id: args.id,
            replicationCapacity: 500 * 1000
        })
        await this.peer.create({
            options,
            orbitDB: args.orbitDB,
            rootAddress: args.rootAddress
        });

        let firstNode = !this.genesis.address;
        await this.genesis.init(this.peer);

        //  --- Create
        if (firstNode) {
            console.log('... is genesis ...');
            // Support shard
            await this.genesis.replicate(); // Only necessary if firstNode
            let shard = new Shard<BinaryDocumentStore<DAO>>({
                shardSize: this.genesis.shardSize, // Assumptions?
                cluster: 'daos',
                storeOptions: new BinaryDocumentStoreOptions({
                    indexBy: 'name',
                    objectType: DAO.name
                })
            });
            await shard.init(this.peer);
            await this.genesis.blocks.put(shard)
        }
    }
}

