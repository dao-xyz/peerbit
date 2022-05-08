import { field, serialize, variant } from "@dao-xyz/borsh";
import OrbitDB from "orbit-db";
import FeedStore from "orbit-db-feedstore";
import KeyValueStore from "orbit-db-kvstore";
import BN from 'bn.js';
import CounterStore from "orbit-db-counterstore";
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import DocumentStore from "orbit-db-docstore";
import * as IPFS from "ipfs";
import { ShardedDB } from ".";
import AccessController from "orbit-db-access-controllers/src/access-controller-interface";
import { CONTRACT_ACCESS_CONTROLLER } from "./acl";

@variant(2)
export class Peer {

    @field({ type: 'String' })
    id: string // peerInfo id

    @field({ type: 'u64' })
    capacity: BN // bytes

    constructor(obj?: Peer) {
        if (obj) {
            Object.assign(this, obj);
        }
    }
}

@variant(1)
export class ReplicationRequest {
    constructor(obj?: ReplicationRequest) {
        if (obj) {
            Object.assign(this, obj);
        }
    }

    @field({ type: 'String' })
    shard: string

    @field({ type: 'u64' })
    index: BN
}


export const SHARD_INDEX = 0;
const MAX_SHARD_SIZE = 1024 * 500 * 1000;
const MAX_SHARDING_WAIT_TIME = 60 * 1000;
export class Shard<B> {

    shardName: string;
    index: BN;
    maxShardSize: number;
    defaultOptions: IStoreOptions;

    /*     peersDBName: string;
    
        blocksDBName: string;
     */
    // Initializable
    peers: KeyValueStore<string> | undefined
    blocks: FeedStore<B> | undefined;
    memoryAdded: CounterStore | undefined;
    memoryRemoved: CounterStore | undefined;

    children: FeedStore<Shard<B>> | undefined

    constructor(from: { shardName: string, index: BN, maxShardSize?: number, defaultOptions: IStoreOptions } = { index: new BN(0), shardName: 'root', maxShardSize: MAX_SHARD_SIZE, defaultOptions: {} }) {
        if (from) {
            this.shardName = from.shardName;
            this.index = new BN(from.index);
            this.maxShardSize = typeof from.maxShardSize === 'number' ? from.maxShardSize : MAX_SHARD_SIZE;
            this.defaultOptions = from.defaultOptions;
            //this.childrenShardsDBName = this.getDBName('children');
        }
    }

    getDBName(name: string) {
        return this.shardName + "-" + name + "-" + this.index.toNumber()
    }

    async loadPeers(db: OrbitDB) {
        this.peers = await db.keyvalue(this.getDBName('peers'), this.defaultOptions);

        this.peers.events.on('replicated', () => {

            console.log('SOME REPL');
        })
        await this.peers.load();

    }

    async loadBlocks(db: OrbitDB) {
        this.blocks = await db.feed(this.getDBName('blocks'), this.defaultOptions);
        await this.blocks.load();
    }

    async addBlock(block: B, sizeBytes: number, db: ShardedDB<any>, requestSharding: boolean = true): Promise<string> {

        if (sizeBytes > MAX_SHARD_SIZE) {
            throw new Error("Block too large");
        }

        if (!this.memoryAdded) {
            await this.loadMemorySize(db.orbitDB);
        }

        if (!this.blocks) {
            await this.loadBlocks(db.orbitDB);
        }
        // This is not a perfect memory check since, there could
        // be a parallel peer that also wants to add memory at the same  time
        // However though, we will not overshoot greatly 

        // Improvement: Make this synchronized across peers
        if (this.memoryAdded.value - this.memoryRemoved.value + sizeBytes > this.maxShardSize) {
            console.log('Max shard size achieved, request new shard');
            if (!db.shardingTopic) {
                throw new Error("No sharding topic");
            }
            if (requestSharding) {
                await Shard.requestReplicatedShard(new Shard({
                    index: this.index.addn(1),
                    shardName: this.shardName,
                    defaultOptions: this.defaultOptions
                }), db);
                await this.addBlock(block, sizeBytes, db, requestSharding);
            }
        }

        await this.memoryAdded.inc(sizeBytes);
        let added = await this.blocks.add(block);
        return added;
    }

    static async requestReplicatedShard(shard: Shard<any>, db: ShardedDB<any>): Promise<Shard<any> | undefined> {
        if (Object.keys(shard.peers.all).length == 0) {
            await db.node.pubsub.publish(db.shardingTopic, serialize(new ReplicationRequest({
                index: shard.index,
                shard: shard.shardName
            })));
        }
        let startTime = new Date().getTime();

        while (Object.keys(shard.peers.all).length == 0 && new Date().getTime() - startTime < MAX_SHARDING_WAIT_TIME) {
            await delay(1000);
        }

        if (Object.keys(shard.peers.all).length == 0) {
            throw new Error("Fail to perform sharding");
        }
        return shard;
    }

    async removeBlock(hash: string, ipfs: IPFSInstance): Promise<void> {
        let rem = await this.blocks.remove(hash)
        // let toRemove = await this.blocks.get(rem);

        if (rem) {
            let result = await ipfs.files.stat(hash);
            await this.memoryRemoved.inc(result.sizeLocal);

        }
    }

    async loadMemorySize(db: OrbitDB) {
        this.memoryAdded = await db.counter(this.getDBName('memory_added'), this.defaultOptions);
        this.memoryRemoved = await db.counter(this.getDBName('memory_removed'), this.defaultOptions);
    }

    async replicate(db: ShardedDB<any>) {
        let id = (await db.node.id()).id;
        if (!this.peers) {
            await this.loadPeers(db.orbitDB);
        }
        await this.peers.set(id, "123");
        /*

        serialize(new Peer({
            capacity: new BN(db.replicationCapacity),
            id
        }))
        */
        console.log('set shard peers', id, this.peers.all)

        await this.loadBlocks(db.orbitDB);
    }


}

const delay = ms => new Promise(res => setTimeout(res, ms));
