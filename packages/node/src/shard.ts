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
const MAX_SHARDING_WAIT_TIME = 30 * 1000;

export class ShardChain<B> {
    shardChainName: string;
    defaultOptions: IStoreOptions;
    db: ShardedDB

    shardCounter: CounterStore | undefined = undefined;
    constructor(opts?: {
        shardChainName: string;
        defaultOptions: IStoreOptions;
        db: ShardedDB
    }) {
        if (opts) {
            this.shardChainName = opts.shardChainName;
            this.defaultOptions = opts.defaultOptions;
            this.db = opts.db;
        }
    }

    async getShardCounter(): Promise<CounterStore> {
        if (this.shardCounter) {
            return this.shardCounter;
        }
        this.shardCounter = await this.db.orbitDB.counter(this.shardChainName, this.defaultOptions);
        await this.shardCounter.load();
        return this.shardCounter;
    }

    async getWritableShard(): Promise<Shard<B> | undefined> {
        // Get the latest shard that have non empty peer
        let index = 0;
        let lastShard = undefined;
        while (true) {
            const shard = new Shard({ chain: this, index: new BN(index), defaultOptions: this.defaultOptions })
            await shard.loadPeers(this.db.orbitDB);
            console.log('load shard peers: ', shard.peers.id, shard.peers.all)
            if (Object.keys(shard.peers.all).length > 0) {
                lastShard = shard;
            }
            else {
                if (index == 0) {
                    await shard.requestReplicatedShard(this.db);
                    return shard;
                }
                return lastShard;
            }
            index += 1;
        }
    }
    async loadShard(index: BN): Promise<Shard<B>> {


        const shard = new Shard<B>({ chain: this, index, defaultOptions: this.defaultOptions })
        await shard.loadPeers(this.db.orbitDB);
        await shard.loadBlocks(this.db.orbitDB);
        return shard;
    }



    async addPeerToShards(startIndex: number, peersLimit: number, supportAmountOfShards: number): Promise<Shard<any>[]> {
        let index = startIndex;
        let supportedShards = 0;
        let shards: Shard<any>[] = [];
        while (supportedShards < supportAmountOfShards) {

            const shard = new Shard({ chain: this, index: new BN(index), defaultOptions: this.defaultOptions })
            await shard.loadPeers(this.db.orbitDB);
            let peersCount = Object.keys(shard.peers.all).length;
            if (peersCount == 0 && startIndex != index) {
                return shards; // dont create a new shard (yet)
            }

            if (Object.keys(shard.peers.all).length < peersLimit) {

                // Replicate (i.e. support)
                // const peerInfo = await this.node.id();
                await shard.replicate(this.db);
                supportedShards += 1;
                shards.push(shard);

            }

            console.log('set shard peers: ', shard.peers.id, shard.peers.all)


            index += 1;
        }
        return shards;
    }




}

export class Shard<B> {

    chain: ShardChain<B>;
    index: BN;
    maxShardSize: number;

    /*     peersDBName: string;
    
        blocksDBName: string;
     */
    // Initializable
    peers: KeyValueStore<string> | undefined
    blocks: FeedStore<B> | undefined;
    memoryAdded: CounterStore | undefined;
    memoryRemoved: CounterStore | undefined;

    children: FeedStore<Shard<B>> | undefined

    constructor(from: { chain: ShardChain<B>, index: BN, maxShardSize?: number, defaultOptions: IStoreOptions }) {
        this.chain = from.chain;
        this.index = new BN(from.index);
        this.maxShardSize = typeof from.maxShardSize === 'number' ? from.maxShardSize : MAX_SHARD_SIZE;
    }

    getDBName(name: string) {
        return this.chain.shardChainName + "-" + name + "-" + this.index.toNumber()
    }

    async loadPeers(db: OrbitDB) {
        this.peers = await db.keyvalue(this.getDBName('peers'), this.chain.defaultOptions);

        this.peers.events.on('replicated', () => {

            console.log('SOME REPL');
        })
        await this.peers.load();

    }

    async loadBlocks(db: OrbitDB) {
        this.blocks = await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        await this.blocks.load();
    }

    async addBlock(block: B, sizeBytes: number, db: ShardedDB): Promise<string> {

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
            throw new Error("Please perform sharding for chain: " + this.chain.shardChainName)
            /*  if (requestSharding) {
                 await Shard.requestReplicatedShard(new Shard({
                     index: this.index.addn(1),
                     shardChainName: this.chain.shardChainName,
                     defaultOptions: this.chain.defaultOptions
                 }), db);
                 await this.addBlock(block, sizeBytes, db, requestSharding);
             } */
        }

        await this.memoryAdded.inc(sizeBytes);
        let added = await this.blocks.add(block);
        return added;
    }

    async requestReplicatedShard(db: ShardedDB): Promise<void> {
        let shardCounter = await this.chain.getShardCounter();
        if (shardCounter.value < this.index.toNumber()) {
            throw new Error(`Expecting shard counter to be less than the new index ${shardCounter} !< ${this.index}`)
        }

        if (Object.keys(this.peers.all).length == 0) {
            await db.node.pubsub.publish(db.shardingTopic, serialize(new ReplicationRequest({
                index: this.index,
                shard: this.chain.shardChainName
            })));
        }

        let startTime = new Date().getTime();
        while (Object.keys(this.peers.all).length == 0 && new Date().getTime() - startTime < MAX_SHARDING_WAIT_TIME) {
            console.log('Waiting for sharding ...')
            await delay(1000);
        }

        if (Object.keys(this.peers.all).length == 0) {
            throw new Error("Fail to perform sharding");
        }

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
        this.memoryAdded = await db.counter(this.getDBName('memory_added'), this.chain.defaultOptions);
        this.memoryRemoved = await db.counter(this.getDBName('memory_removed'), this.chain.defaultOptions);
    }

    async replicate(db: ShardedDB) {
        /// Shard counter might be wrong because someone else could request sharding at the same time
        let shardCounter = await this.chain.getShardCounter();
        if (shardCounter.value <= this.index.toNumber()) {
            await shardCounter.inc(1);
        }

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
