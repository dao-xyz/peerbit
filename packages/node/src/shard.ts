import { Constructor, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import OrbitDB from "orbit-db";
import FeedStore from "orbit-db-feedstore";
import KeyValueStore from "orbit-db-kvstore";
import BN from 'bn.js';
import Store from "orbit-db-store";
import CounterStore from "orbit-db-counterstore";
import { ShardedDB } from ".";
import DocumentStore from "orbit-db-docstore";

export const SHARD_INDEX = 0;
const MAX_SHARD_SIZE = 1024 * 500 * 1000;
const MAX_SHARDING_WAIT_TIME = 30 * 1000;
export const SHARD_NAME_FIELD = "name";
export const SHARD_STORE_TYPE_FIELD = "storeType";
export const SHARD_STORE_OBJECT_TYPE_FIELD = "objectType";

// io

@variant(1)
export class ReplicationRequest {
    constructor(obj?: ReplicationRequest) {
        if (obj) {
            Object.assign(this, obj);
        }
    }

    @field({ type: 'String' })
    [SHARD_NAME_FIELD]: string

    @field({ type: 'u64' })
    index: BN

    @field({ type: 'String' })
    [SHARD_STORE_TYPE_FIELD]: string

    @field({ type: option('String') })
    [SHARD_STORE_OBJECT_TYPE_FIELD]: string | undefined


}

export class Query {

}

@variant(0)
export class FilterQuery extends Query {
    @field({ type: 'String' })
    key: string

    @field({ type: vec('u8') })
    value: Uint8Array

    constructor(opts?: FilterQuery) {
        super();
        if (opts) {
            Object.assign(this, opts)
        }
    }
}


@variant(2)
export class QueryRequest {

    @field({ type: vec(Query) })
    filters: Query[]

    constructor(obj?: QueryRequest) {
        if (obj) {
            Object.assign(this, obj);
        }
    }

}

// data

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






/* abstract class XYZStoreBuilder<T extends XYZStore> {
    newOrLoad(name: string, chain: ShardChain<T>): Promise<T> {
        throw new Error("Not implemented")
    }
}

abstract class XYZStore extends Store {
    load(): Promise<void> {
        throw new Error("Not implemented")
    }
}

export class FeedXYZSore extends XYZStore 
{
    
}
 */


export type StoreBuilder<B> = (name: string, defaultOptions: IStoreOptions, orbitdDB: OrbitDB) => Promise<B>

// patch behaviours in a big MAP ? 
// type -> 
/* 
{
    - query() => 
    - new => 
    - 
} 
*/
export type Behaviours<St> = {
    newStore: StoreBuilder<St>
}

export type TypedBehaviours = {
    stores: {
        [key: string]: Behaviours<any>;
    }
    typeMap: {
        [key: string]: Constructor<any>
    }
};


@variant(0)
export class ShardChain<B extends Store> {

    @field({ type: 'String' })
    [SHARD_NAME_FIELD]: string;

    @field({ type: 'String' })
    [SHARD_STORE_TYPE_FIELD]: string;

    @field({ type: option('String') })
    [SHARD_STORE_OBJECT_TYPE_FIELD]: string | undefined;

    defaultOptions: IStoreOptions;
    db: ShardedDB
    shardCounter: CounterStore | undefined = undefined;
    behaviours: TypedBehaviours;

    constructor(opts?: {
        shardChainName: string;
        storeType: string;
        objectType?: string;
    }) {

        if (opts) {
            this.name = opts.shardChainName;
            this.storeType = opts.storeType;
            this.objectType = opts.objectType;
        }

    }
    init(opts: {

        defaultOptions: IStoreOptions;
        db: ShardedDB;
        behaviours: TypedBehaviours;
    }) {
        this.defaultOptions = opts.defaultOptions;
        this.db = opts.db;
        this.behaviours = opts.behaviours;
    }

    async newStore(name: string): Promise<B> {

        let newStore = await this.behaviours[this.storeType].newStore(name, { ... this.defaultOptions, clazz: this.objectType ? this.behaviours.typeMap[this.objectType] : undefined }, this.db.orbitDB);
        return newStore;
    }

    async getShardCounter(): Promise<CounterStore> {

        if (this.shardCounter) {
            return this.shardCounter;
        }
        this.shardCounter = await this.db.orbitDB.counter(this.name, this.defaultOptions);
        await this.shardCounter.load();
        return this.shardCounter;

    }

    async getWritableShard(): Promise<Shard<B> | undefined> {
        // Get the latest shard that have non empty peer
        let index = 0;
        let lastShard = undefined;
        while (true) {
            const shard = new Shard({ chain: this, index: new BN(index), defaultOptions: this.defaultOptions })
            await shard.loadPeers();
            console.log('load shard peers: ', shard.peers.id, shard.peers.all)
            if (Object.keys(shard.peers.all).length > 0) {
                lastShard = shard;
            }
            else {
                if (index == 0) {
                    await shard.requestReplicatedShard();
                    return shard;
                }
                return lastShard;
            }
            index += 1;
        }
    }
    async getShard(index: number): Promise<Shard<B> | undefined> {
        // Get the latest shard that have non empty peer
        const shard = new Shard({ chain: this, index: new BN(index), defaultOptions: this.defaultOptions })
        return shard;
    }




    async loadShard(index: BN): Promise<Shard<B>> {

        const shard = new Shard<B>({ chain: this, index, defaultOptions: this.defaultOptions })
        await shard.loadPeers();
        await shard.loadBlocks();
        return shard;
    }



    async addPeerToShards(startIndex: number, peersLimit: number, supportAmountOfShards: number): Promise<Shard<any>[]> {

        let index = startIndex;
        let supportedShards = 0;
        let shards: Shard<any>[] = [];
        while (supportedShards < supportAmountOfShards) {

            const shard = new Shard({ chain: this, index: new BN(index), defaultOptions: this.defaultOptions })
            await shard.loadPeers();
            let peersCount = Object.keys(shard.peers.all).length;
            if (peersCount == 0 && startIndex != index) {
                return shards; // dont create a new shard (yet)
            }

            if (Object.keys(shard.peers.all).length < peersLimit) {

                // Replicate (i.e. support)
                // const peerInfo = await this.node.id();
                await shard.replicate();
                supportedShards += 1;
                shards.push(shard);

            }

            console.log('set shard peers: ', shard.peers.id, shard.peers.all)


            index += 1;
        }
        return shards;
    }




}

export class Shard<B extends Store> {

    chain: ShardChain<B>;
    index: BN;
    maxShardSize: number;

    /*     peersDBName: string;
    
        blocksDBName: string;
     */
    // Initializable
    peers: KeyValueStore<string> | undefined

    blocks: B | undefined;
    memoryAdded: CounterStore | undefined;
    memoryRemoved: CounterStore | undefined;

    children: FeedStore<Shard<B>> | undefined

    constructor(from: { chain: ShardChain<B>, index: BN, maxShardSize?: number, defaultOptions: IStoreOptions }) {
        this.chain = from.chain;
        this.index = new BN(from.index);
        this.maxShardSize = typeof from.maxShardSize === 'number' ? from.maxShardSize : MAX_SHARD_SIZE;
    }

    getDBName(name: string) {
        return "chain-" + this.chain.name + "-" + this.chain.storeType + "-" + this.chain.objectType ? this.chain.objectType : '_' + "-" + name + "-" + this.index.toNumber()
    }

    async loadPeers() {
        this.peers = await this.chain.db.orbitDB.keyvalue(this.getDBName('peers'), this.chain.defaultOptions);

        this.peers.events.on('replicated', () => {

            console.log('SOME REPL');
        })
        await this.peers.load();

    }

    async loadBlocks(): Promise<B> {
        this.blocks = await this.chain.newStore(this.getDBName('blocks')) //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        await this.blocks.load();
        return this.blocks;
    }

    async makeSpace(sizeBytes: number): Promise<void> {

        if (sizeBytes > MAX_SHARD_SIZE) {
            throw new Error("Block too large");
        }

        if (!this.memoryAdded) {
            await this.loadMemorySize();
        }

        if (!this.blocks) {
            await this.loadBlocks();
        }
        // This is not a perfect memory check since, there could
        // be a parallel peer that also wants to add memory at the same  time
        // However though, we will not overshoot greatly 

        // Improvement: Make this synchronized across peers
        if (this.memoryAdded.value - this.memoryRemoved.value + sizeBytes > this.maxShardSize) {
            console.log('Max shard size achieved, request new shard');
            throw new Error("Please perform sharding for chain: " + this.chain.name)
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
        /*  let added = await this.blocks.add(block);
         return added; */
    }

    async requestReplicatedShard(): Promise<void> {
        let shardCounter = await this.chain.getShardCounter();
        if (shardCounter.value < this.index.toNumber()) {
            throw new Error(`Expecting shard counter to be less than the new index ${shardCounter} !< ${this.index}`)
        }

        if (Object.keys(this.peers.all).length == 0) {
            await this.chain.db.node.pubsub.publish(this.chain.db.getShardingTopic(), serialize(new ReplicationRequest({
                index: this.index,
                name: this.chain.name,
                storeType: this.chain.storeType,
                objectType: this.chain.objectType
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

    async query(query: QueryRequest): Promise<any[]> {
        // query
        if (this.blocks instanceof DocumentStore) {
            let result = this.blocks.query(
                doc =>
                    query.filters.map(f => {
                        if (f instanceof FilterQuery) {
                            let docValue = doc[f.key];
                            return docValue == f.value
                        }
                    }).reduce((prev, current) => prev && current)
            )

            // publish response
            return result

        }
        else if (this.blocks instanceof FeedStore) {
            let result = this.blocks.iterator().collect().map(x => x.payload.value).filter(
                doc =>
                    query.filters.map(f => {
                        if (f instanceof FilterQuery) {
                            let docValue = doc[f.key];
                            return docValue == f.value
                        }
                    }).reduce((prev, current) => prev && current)
            )

            // publish response
            return result
        }

        else {
            throw new Error("Querying not supported")
        }

    }



    async removeAndFreeMemory(amount: number, remove: () => Promise<string>): Promise<void> {
        let rem = await remove();
        if (rem) {
            await this.memoryRemoved.inc(amount);

        }
    }

    async loadMemorySize() {
        this.memoryAdded = await this.chain.db.orbitDB.counter(this.getDBName('memory_added'), this.chain.defaultOptions);
        this.memoryRemoved = await this.chain.db.orbitDB.counter(this.getDBName('memory_removed'), this.chain.defaultOptions);
    }

    async replicate() {
        /// Shard counter might be wrong because someone else could request sharding at the same time
        let shardCounter = await this.chain.getShardCounter();
        if (shardCounter.value <= this.index.toNumber()) {
            await shardCounter.inc(1);
        }

        let id = (await this.chain.db.node.id()).id;
        if (!this.peers) {
            await this.loadPeers();
        }
        await this.peers.set(id, id);
        /*

        serialize(new Peer({
            capacity: new BN(db.replicationCapacity),
            id
        }))
        */
        console.log('set shard peers', id, this.peers.all)

        await this.loadBlocks();
    }


}

const delay = ms => new Promise(res => setTimeout(res, ms));
