/**
 * A decentralized storage of DAO meta data info. 
 * The nodes governing this storage are part of a "official" set of nodes
 * These nodes are trusted by the dao.xyz.dao to act truthfully to serve 
 * the wider community a way of creating, modifying, deleting and searching 
 * DAOs (organizations/communities/groups)
 */

import { variant } from '@dao-xyz/borsh';
import { ShardedDB, ShardChain, BinaryDocumentStoreOptions } from '@dao-xyz/node';
import { BinaryDocumentStore } from '@dao-xyz/orbit-db-bdocstore';
import { Identity } from 'orbit-db-identity-provider';

@variant(0)
export class DAO {
    name: string;
}
export class DaoDB extends ShardedDB {

    db: ShardedDB
    daos: ShardChain<BinaryDocumentStore<DAO>>

    constructor() {
        super();
    }

    public async create(options?: { rootId: string; local: boolean; identity?: Identity; }): Promise<void> {
        await super.create({
            ...options, ...{
                behaviours: {
                    typeMap: {
                        [DAO.name]: DAO
                    }
                },
                repo: './ipfs',
                replicationCapacity: 512 * 1000,
            }
        });


        //  --- Create
        let rootChains = this.shardChainChain;

        // Create Root shard
        await rootChains.addPeerToShards();

        // Create/Load DAO store

        let daoStoreOptions = new BinaryDocumentStoreOptions<DAO>({
            indexBy: "name",
            objectType: DAO.name
        });

        this.daos = await this.loadShardChain("dao", daoStoreOptions);

    }

    public async support() {

        await this.daos.addPeerToShards(
            {
                peersLimit: 1,
                startIndex: 0,
                supportAmountOfShards: 1
            }
        );

    }





}

export const getDAOs = () => { }
