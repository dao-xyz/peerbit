import { Constructor, deserialize, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import OrbitDB from "orbit-db";
import FeedStore from "orbit-db-feedstore";
import KeyValueStore from "orbit-db-kvstore";
import BN from 'bn.js';
import Store from "orbit-db-store";
import CounterStore from "orbit-db-counterstore";
import { ShardedDB } from ".";
import DocumentStore from "orbit-db-docstore";
import { BinaryKeyValueStore } from '@dao-xyz/orbit-db-bkvstore';
import { BinaryKeyValueStoreOptions, StoreOptions } from "./stores";
import { generateUUID } from "./id";
import { Message } from 'ipfs-core-types/types/src/pubsub'
import { EncodedQueryResponse, FilterQuery, Query, QueryRequestV0, QueryResponse, StringMatchQuery } from "./query";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import base58 from "bs58";
import { waitFor } from "./utils";

export const SHARD_INDEX = 0;
const MAX_SHARD_SIZE = 1024 * 500 * 1000;
const MAX_SHARDING_WAIT_TIME = 30 * 1000;
const MAX_REPLICATION_WAIT_TIME = 30 * 1000;

export const SHARD_CHAIN_ID_FIELD = "id";
export const SHARD_NAME_FIELD = "name";




// io


@variant(1)
export class ReplicationRequest {


    @field({ type: 'String' })
    shardChainName: string

    @field({ type: 'u64' })
    index: BN

    @field({ type: option(StoreOptions) })
    storeOptions: StoreOptions<any> | undefined;

    @field({ type: 'u64' })
    shardSize: BN

    constructor(obj?: ReplicationRequest) {
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
    typeMap: {
        [key: string]: Constructor<any>
    }
};


/* const waitForReplicationEvents = async (waitForReplicationEventsCount: number, storeEvents: { on: (event: string, cb: () => void) => void }) => {
    if (waitForReplicationEventsCount <= 0)
        return;

    let replications = 0;
    storeEvents.on('replicated', () => {
        replications += 1;
    });

    let startTime = +new Date;
    while (+new Date - startTime < MAX_REPLICATION_WAIT_TIME && replications != waitForReplicationEventsCount) {
        await delay(30);
    }
    if (replications < waitForReplicationEventsCount) {
        // Could potentially be an issue where replication events already happened before this call, 
        // hence we just warn if result is not the expected
        console.warn("Failed to find all replication events");
    }
} */

const waitForReplicationEvents = async (store: Store, waitForReplicationEventsCount: number) => {
    if (!waitForReplicationEventsCount)
        return

    await waitFor(() => !!store.replicationStatus && waitForReplicationEventsCount <= store.replicationStatus.max)

    let startTime = +new Date;
    while (store.replicationStatus.progress < store.replicationStatus.max) {
        await delay(50);
        if (+new Date - startTime > MAX_REPLICATION_WAIT_TIME) {
            console.warn("Max replication time, aborting wait for")
            return;
        }
    }
    return;
}


@variant(0)
export class ShardChain<B extends Store> {

    @field({ type: 'String' })
    _id: string;

    @field({ type: 'String' })
    remoteAddress: string;

    @field({ type: 'String' })
    name: string;

    @field({ type: option(StoreOptions) })
    storeOptions: StoreOptions<B> | undefined;

    @field({ type: 'u64' })
    shardSize: BN

    defaultOptions: IStoreOptions;
    db: ShardedDB
    shardCounter: CounterStore | undefined = undefined;
    behaviours: TypedBehaviours;

    constructor(opts?: {
        id?: string;
        remoteAddress: string; // orbitdb/123abc123abc
        name: string;
        storeOptions: StoreOptions<any> | undefined;
        shardSize: BN
    }) {
        if (opts) {
            Object.assign(this, opts);
            if (!this._id) {
                this._id = this.id;
            }
        }
    }

    get id(): string {
        // TODO, id should contain path, or be unique
        /* (this.remoteAddress ? (this.remoteAddress + '/') : '') + */
        return this.name + "-" + this.storeOptions.identifier;
        /* return (this.remoteAddress ? (this.remoteAddress + '/') : '') + this.name + "-" + this.storeOptions.identifier; */
    }

    get queryTopic(): string {
        return this.id + "/query"
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
        return this.storeOptions.newStore(name, this.db.orbitDB, this.defaultOptions, this.behaviours);
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




    async loadShard(index: number, options: { expectedPeerReplicationEvents?: number, expectedBlockReplicationEvents?: number } = { expectedPeerReplicationEvents: 0, expectedBlockReplicationEvents: 0 }): Promise<Shard<B>> {

        const shard = new Shard<B>({ chain: this, index: new BN(index), defaultOptions: this.defaultOptions })
        await shard.loadPeers(options.expectedPeerReplicationEvents);
        await shard.loadBlocks(options.expectedBlockReplicationEvents);
        return shard;
    }



    async addPeerToShards(opts: {
        startIndex: number,
        peersLimit: number,
        supportAmountOfShards: number
    } = {
            peersLimit: 1,
            startIndex: 0,
            supportAmountOfShards: 1
        }): Promise<Shard<B>[]> {

        let index = opts.startIndex;
        let supportedShards = 0;
        let shards: Shard<B>[] = [];
        while (supportedShards < opts.supportAmountOfShards) {

            const shard = new Shard<B>({ chain: this, index: new BN(index), defaultOptions: this.defaultOptions })
            await shard.loadPeers();
            let peersCount = Object.keys(shard.peers.all).length;
            if (peersCount == 0 && opts.startIndex != index) {
                return shards; // dont create a new shard (yet)
            }

            if (Object.keys(shard.peers.all).length < opts.peersLimit) {

                // Replicate (i.e. support)
                // const peerInfo = await this.node.id();
                await shard.replicate({
                    capacity: this.shardSize
                });
                supportedShards += 1;
                shards.push(shard);

            }

            /*             
            console.log('set shard peers: ', shard.peers.id, shard.peers.all)
             */

            index += 1;
        }
        return shards;
    }






}

export class Shard<B extends Store> {

    chain: ShardChain<B>;
    index: BN;
    maxShardSize: number;
    querable: boolean = true;

    /*     peersDBName: string;
    
        blocksDBName: string;
     */
    // Initializable
    peers: BinaryKeyValueStore<Peer> | undefined

    blocks: B;
    memoryAdded: CounterStore | undefined;
    memoryRemoved: CounterStore | undefined;

    children: FeedStore<Shard<B>> | undefined

    constructor(from: { chain: ShardChain<B>, index: BN, maxShardSize?: number, defaultOptions: IStoreOptions }) {
        this.chain = from.chain;
        this.index = new BN(from.index);
        this.maxShardSize = typeof from.maxShardSize === 'number' ? from.maxShardSize : MAX_SHARD_SIZE;
    }

    getDBName(name: string): string {
        return this.chain.id + "-" + name + "-" + this.index.toNumber()
    }

    async loadPeers(waitForReplicationEventsCount: number = 0) {
        if (this.peers) {
            return this.peers;
        }

        // Second argument 
        this.peers = await new BinaryKeyValueStoreOptions<Peer>({ objectType: Peer.name }).newStore(this.getDBName('peers'), this.chain.db.orbitDB, this.chain.defaultOptions, this.chain.behaviours);
        /*       await Promise.all([
                  waitForReplicationEvents(waitForReplicationEventsCount, this.peers.events),
                  this.peers.load()
              ])
       */
        await this.peers.load();
        await waitForReplicationEvents(this.peers, waitForReplicationEventsCount);
        return this.peers;
    }

    async isSupported(peersCount: number = 1) {
        await this.loadPeers(peersCount);
        return Object.keys(this.peers.all).length >= peersCount
    }

    async loadBlocks(waitForReplicationEventsCount: number = 0): Promise<B> {
        if (this.blocks) {
            return this.blocks;
        }
        this.blocks = await this.chain.newStore(this.getDBName('blocks')) //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        this.blocks.events.on('replicated', (e) => {
            console.log('Replicated', e)
        })

        await this.blocks.load();
        await waitForReplicationEvents(this.blocks, waitForReplicationEventsCount);
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
                shardChainName: this.chain.name,
                storeOptions: this.chain.storeOptions,
                shardSize: this.chain.shardSize
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

    async query(query: QueryRequestV0): Promise<any[]> {
        // query
        if (this.blocks instanceof DocumentStore || this.blocks instanceof BinaryDocumentStore) {

            /*     if (query.filters.length == 0) {
                    return this.blocks.all;
                } */
            let filters: (Query | ((v: any) => boolean))[] = query.queries;
            if (filters.length == 0) {
                filters = [(v?) => true];
            }
            let result = this.blocks.query(
                doc =>
                    filters.map(f => {
                        if (f instanceof Query) {
                            return f.apply(doc)
                        }
                        else {
                            return (f as ((v: any) => boolean))(doc)
                        }
                    }).reduce((prev, current) => prev && current)
            )

            // publish response
            return result

        }
        else if (this.blocks instanceof FeedStore) {
            let result = this.blocks.iterator().collect().map(x => x.payload.value).filter(
                doc =>
                    query.queries.map(f => {
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

    async replicate(opts: { capacity: BN }) {
        /// Shard counter might be wrong because someone else could request sharding at the same time
        let shardCounter = await this.chain.getShardCounter();
        if (shardCounter.value <= this.index.toNumber()) {
            await shardCounter.inc(1);
        }

        let id = (await this.chain.db.node.id()).id;
        if (!this.peers) {
            await this.loadPeers();
        }
        await this.peers.set(id, new Peer({
            id: id,
            capacity: opts.capacity
        }));

        await this.chain.db.node.pubsub.subscribe(this.chain.queryTopic, async (msg: Message) => {
            try {
                await this.blocks.load();
                let query = deserialize(Buffer.from(msg.data), QueryRequestV0);
                let results = await this.query(query);
                let response = new EncodedQueryResponse({
                    results: results.map(r => base58.encode(serialize(r)))
                });

                let bytes = serialize(response);
                await this.chain.db.node.pubsub.publish(
                    query.getResponseTopic(this.chain.queryTopic),
                    bytes
                )
            } catch (error) {
                console.error(JSON.stringify(error))
            }
        })
        /*
        
        serialize(new Peer({
            capacity: new BN(db.replicationCapacity),
            id
        }))
        */

        await this.loadBlocks();
    }


}

const delay = ms => new Promise(res => setTimeout(res, ms));
