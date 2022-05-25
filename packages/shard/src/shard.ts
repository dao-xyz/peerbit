import { Constructor, deserialize, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import OrbitDB from "orbit-db";
import FeedStore from "orbit-db-feedstore";
import KeyValueStore from "orbit-db-kvstore";
import BN from 'bn.js';
import Store from "orbit-db-store";
import CounterStore from "orbit-db-counterstore";
import DocumentStore from "orbit-db-docstore";
import { BinaryKeyValueStore } from '@dao-xyz/orbit-db-bkvstore';
import { BinaryDocumentStoreOptions, BinaryKeyValueStoreOptions, StoreOptions } from "./stores";
import { generateUUID } from "./id";
import { Message } from 'ipfs-core-types/types/src/pubsub'
import { EncodedQueryResponse, FilterQuery, Query, QueryRequestV0, QueryResponse, StringMatchQuery } from "./query";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import base58 from "bs58";
import { delay, waitFor } from "./utils";
import { AnyPeer, IPFSInstanceExtended } from "./node";
import { CID } from "ipfs";

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

    @field({ type: vec('String') })
    addresses: string[] // address

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
    /**
     * This method is flaky
     * First we check the progress of replicatoin
     * then we check a custom replicated boolean, as the replicationStatus
     * is not actually tracking whether the store is loaded
     */

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

    // await waitFor(() => store["replicated"])
    return;
}

const onReplicationMark = (store: Store) => store.events.on('replicated', () => {
    store["replicated"] = true // replicated once
});


@variant(1)
export class Shard<B extends Store> {

    @field({ type: 'String' })
    id: string

    @field({ type: 'String' })
    cluster: string

    @field({ type: option(StoreOptions) })
    storeOptions: StoreOptions<B> | undefined;

    @field({ type: 'u64' })
    shardSize: BN

    @field({ type: 'String' })
    address: string; // the actual data db

    @field({ type: 'String' })
    memoryAddedAddress: string; // Track memory added to data db

    @field({ type: 'String' })
    memoryRemovedAddress: string; // Track memory removed to data db

    @field({ type: 'String' })
    peersAddress: string; // peers data db

    @field({ type: option('String') })
    parentShardCID: string | undefined;



    _peers?: BinaryKeyValueStore<Peer>
    blocks?: B;
    memoryAdded?: CounterStore;
    memoryRemoved?: CounterStore;
    peer: AnyPeer;

    cid: string;
    constructor(from?: {
        id: string
        cluster: string
        storeOptions: StoreOptions<B> | undefined;
        shardSize: BN
        address: string;
        peersAddress: string;
        memoryAddedAddress: string;
        memoryRemovedAddress: string;
        parentShardCID: string;
    } | {
        cluster: string
        storeOptions: StoreOptions<B> | undefined;
        shardSize: BN
    }) {
        if (from) {
            Object.assign(this, from);
        }
        if (!this.id) {
            this.id = generateUUID();
        }
    }

    async init(from: AnyPeer, parent?: RecursiveShard<B>): Promise<Shard<B>> {
        await this.close();
        from.options.behaviours.typeMap[Peer.name] = Peer;
        this.peer = from;
        let isInitialized = this.initialized;
        await this.loadStores();

        if (parent) {
            this.parentShardCID = parent.cid;
            await parent.blocks.put(this);
        }


        if (!isInitialized) {
            await this.save(from.node);
        }
        return this;
    }

    async close() {
        /*  if (this._peers) {
             await this._peers.close();
         }
         if (this.blocks) {
             await this.blocks.close();
         }
         if (this.memoryAdded) {
             await this.memoryAdded.close();
         }
         if (this.memoryRemoved) {
             await this.memoryRemoved.close();
         } */
        this._peers = undefined;
        this.blocks = undefined;
        this.memoryAdded = undefined;
        this.memoryRemoved = undefined;
    }

    async loadStores() {
        this.blocks = await this.newStore(this.address ? this.address : this.getDBName('blocks')) //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        onReplicationMark(this.blocks);


        this.address = this.blocks.address.toString();
        this._peers = await new BinaryKeyValueStoreOptions<Peer>({ objectType: Peer.name }).newStore(this.peersAddress ? this.peersAddress : this.getDBName("peers"), this.peer.orbitDB, this.peer.options.defaultOptions, this.peer.options.behaviours);
        onReplicationMark(this._peers);

        this.peersAddress = this._peers.address.toString();
        await this.loadMemorySize();
        this.memoryAddedAddress = this.memoryAdded.address.toString();
        this.memoryRemovedAddress = this.memoryRemoved.address.toString();

    }

    async loadMemorySize() {
        this.memoryAdded = await this.peer.orbitDB.counter(this.memoryAddedAddress ? this.memoryAddedAddress : this.getDBName('memory_added'), this.peer.options.defaultOptions);
        onReplicationMark(this.memoryAdded);


        this.memoryRemoved = await this.peer.orbitDB.counter(this.memoryRemovedAddress ? this.memoryRemovedAddress : this.getDBName('memory_removed'), this.peer.options.defaultOptions);
        onReplicationMark(this.memoryRemoved);

    }


    getDBName(name: string): string {
        return this.id + '-' + name;
    }

    get queryTopic(): string {
        return this.cluster;
    }


    async loadPeers(waitForReplicationEventsCount: number = 0) {
        if (!this._peers) {
            this._peers = await new BinaryKeyValueStoreOptions<Peer>({ objectType: Peer.name }).newStore(this.peersAddress ? this.peersAddress : this.getDBName("peers"), this.peer.orbitDB, this.peer.options.defaultOptions, this.peer.options.behaviours);
        }

        // Second argument 
        await this._peers.load();
        await waitForReplicationEvents(this._peers, waitForReplicationEventsCount);
        return this._peers;
    }

    async isSupported(peersCount: number = 1) {
        await this.loadPeers(peersCount);
        return Object.keys(this._peers.all).length >= peersCount
    }
    async newStore(name: string): Promise<B> {
        return this.storeOptions.newStore(name, this.peer.orbitDB, this.peer.options.defaultOptions, this.peer.options.behaviours);
    }

    async loadBlocks(waitForReplicationEventsCount: number = 0): Promise<B> {
        if (!this.blocks) {
            this.blocks = await this.newStore(this.address ? this.address : this.getDBName('blocks')) //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        }
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
        if (this.memoryAdded.value - this.memoryRemoved.value + sizeBytes > this.shardSize.toNumber()) {
            console.log('Max shard size achieved, request new shard');
            throw new Error("Please perform sharding for chain: " + this.cluster)
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

    async requestReplicate(): Promise<void> {
        /*   let shardCounter = await this.chain.getShardCounter();
          if (shardCounter.value < this.index.toNumber()) {
              throw new Error(`Expecting shard counter to be less than the new index ${shardCounter} !< ${this.index}`)
          } */

        if (Object.keys(this._peers.all).length == 0) {
            // Send message that we need peers for this shard
            // The recieved of the message should be the DB that contains this shard,
            let thisSerialized = serialize(this);
            await this.peer.node.pubsub.publish(this.peer.replicationTopic, thisSerialized);
        }

        await waitFor(() => Object.keys(this._peers.all).length > 0)

        if (Object.keys(this._peers.all).length == 0) {
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

    public async addPeer() {

        let thisPeer = new Peer({
            addresses: (await this.peer.node.id()).addresses.map(x => x.toString())
        });
        await this._peers.set(this.id, thisPeer);

        // Connect to parent shard, and connects to its peers 

        if (this.parentShardCID) {
            let parentShard = await Shard.loadFromCID(this.parentShardCID, this.peer.node);
            await parentShard.init(this.peer);
            let parentPeers = await parentShard.loadPeers(1); // Expect at least 1 peer from parent
            let thisAddressSet = new Set(thisPeer.addresses);
            const isSelfDial = (other: Peer) => {
                for (const addr of other.addresses) {
                    if (thisAddressSet.has(addr))
                        return true;
                }
                return false;
            }
            // Connect to all parent peers, we could do better (cherry pick), but ok for now
            await Promise.all(Object.values(parentPeers.all).filter(peer => !isSelfDial(peer)).map((peer) => this.peer.node.swarm.connect(peer.addresses[0])))
        }
    }


    async replicate() {
        /// Shard counter might be wrong because someone else could request sharding at the same time
        let id = (await this.peer.node.id()).id;
        await this.loadPeers();
        await this.loadBlocks();
        await this.loadMemorySize();
        await this.addPeer();


        await this.peer.node.pubsub.subscribe(this.queryTopic, async (msg: Message) => {
            try {
                await this.blocks.load();
                let query = deserialize(Buffer.from(msg.data), QueryRequestV0);
                let results = await this.query(query);
                let response = new EncodedQueryResponse({
                    results: results.map(r => base58.encode(serialize(r)))
                });

                let bytes = serialize(response);
                await this.peer.node.pubsub.publish(
                    query.getResponseTopic(this.queryTopic),
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

    get initialized(): boolean {
        return !!this.address && !!this.peersAddress && !!this.memoryAddedAddress && !!this.memoryRemovedAddress
    };

    async save(node: IPFSInstanceExtended): Promise<string> {
        if (!this.initialized) {
            throw new Error("Not initialized");
        }

        let arr = serialize(this);
        let addResult = await node.add(arr)
        let pinResult = await node.pin.add(addResult.cid)
        this.cid = pinResult.toString();
        return this.cid;
    }


    static async loadFromCID<B extends Store>(cid: string, node: IPFSInstanceExtended) {
        let arr = await node.cat(cid);
        let first = undefined;
        for await (const obj of arr) {
            return deserialize<Shard<B>>(Buffer.from(obj), Shard)

        }

    }



}

// TODO verify serialization work with inheritance
@variant(2)
export class RecursiveShard<T extends Store> extends Shard<BinaryDocumentStore<Shard<T>>> {



    /* @field({ type: 'u64' })
    XYZshards: BN

    defaultOptions: IStoreOptions;
    db: ShardedDB
    shardCounter: CounterStore | undefined = undefined;
    behaviours: TypedBehaviours; */

    /* constructor(opts?: {
        id?: string;
        name: string;
        remoteAddress?: string;
        storeOptions: StoreOptions<any> | undefined;
        shardSize: BN
    }) {
        if (opts) {
            Object.assign(this, opts);
            if (!this._id) {
                this._id = this.id;
            }
        }
    } */
    /* 
        get queryTopic(): string {
            return this.id + "/query"
        }
     */

    /* 
        init(opts: {
    
            defaultOptions: IStoreOptions;
            db: ShardedDB;
            behaviours: TypedBehaviours;
        }) {
            this.defaultOptions = opts.defaultOptions;
            this.db = opts.db;
            this.behaviours = opts.behaviours;
        } */



    /* async getShardCounter(): Promise<CounterStore> {

        if (this.shardCounter) {
            return this.shardCounter;
        }
        this.shardCounter = await this.db.orbitDB.counter(this.name, this.defaultOptions);
        await this.shardCounter.load();
        return this.shardCounter;

    } */

    constructor(from?: {
        id: string
        cluster: string
        storeOptions: StoreOptions<BinaryKeyValueStore<Shard<T>>> | undefined;
        shardSize: BN
        address: string;
        peersAddress: string;
        memoryAddedAddress: string;
        memoryRemovedAddress: string;
        parentShardCID: string;
    } | {
        cluster: string
        shardSize: BN
    }) {
        const storeOptions = new BinaryDocumentStoreOptions<Shard<T>>({
            objectType: RecursiveShard.name,
            indexBy: 'id'
        })
        super({ ...from, storeOptions });
    }

    async init(from: AnyPeer, parent?: RecursiveShard<BinaryDocumentStore<Shard<T>>>): Promise<RecursiveShard<T>> {
        from.options.behaviours.typeMap[RecursiveShard.name] = RecursiveShard;
        await super.init(from, parent);
        return this;
    }

    /*  async addShard(shard: Shard<T>) {
         if (shard.cid != undefined) {
             throw new Error("Already initialized. `addShard` requires an uniniialized shard")
         }
 
         shard.parentShardCID = this.cid;
         await this.blocks.put(shard);
     } */

    async getWritableShard(storeOptions: StoreOptions<T>, peer: AnyPeer): Promise<Shard<T>> {
        // Get the latest shard that have non empty peer
        let index = 0;
        let lastShard = undefined;
        while (true) {

            const shard = new Shard<T>({
                id: generateUUID(),
                cluster: this.cluster + "/" + this.cluster,
                shardSize: this.shardSize,
                storeOptions,
                address: undefined,
                memoryAddedAddress: undefined,
                memoryRemovedAddress: undefined,
                peersAddress: undefined,
                parentShardCID: this.cid
            });

            await shard.init(peer);
            await shard.loadPeers();

            if (Object.keys(shard._peers.all).length > 0) {
                lastShard = shard;
            }
            else {
                if (index == 0) {
                    await shard.requestReplicate();
                    this.blocks.put(shard);
                    return shard;
                }
                return lastShard;
            }
            index += 1;
        }
    }
    async loadShard(index: number, options: { expectedPeerReplicationEvents?: number, expectedBlockReplicationEvents?: number } = { expectedPeerReplicationEvents: 0, expectedBlockReplicationEvents: 0 }): Promise<Shard<T>> {
        // Get the latest shard that have non empty peer
        let block: Shard<T> = await (await this.loadBlocks(index + 1)).get(index.toString())[0];
        await block.init(this.peer);
        await block.loadPeers(options.expectedPeerReplicationEvents);
        await block.loadBlocks(options.expectedBlockReplicationEvents);
        return block;
    }


    /* async addPeerToShards(opts: {
        startIndex: number,
        peersLimit: number,
        supportAmountOfShards: number
    } = {
            peersLimit: 1,
            startIndex: 0,
            supportAmountOfShards: 1
        }): Promise<Shard<T>[]> {

        let index = opts.startIndex;
        let supportedShards = 0;
        let shards: Shard<T>[] = [];
        while (supportedShards < opts.supportAmountOfShards) {

            const results = await (await this.loadBlocks(index + 1)).get(index.toString());
            const shard: Shard<T> = results[0]
            await shard.init(this.peer);
            await shard.loadPeers();
            let peersCount = Object.keys(shard._peers.all).length;
            if (peersCount == 0 && opts.startIndex != index) {
                return shards; // dont create a new shard (yet)
            }

            if (Object.keys(shard._peers.all).length < opts.peersLimit) {

                // Replicate (i.e. support)
                // const peerInfo = await this.node.id();
                await shard.replicate();
                supportedShards += 1;
                let serialized = serialize(shard);
                await this.makeSpace(serialized.length);
                shards.push(shard);
            }

       
            index += 1;
        }
        return shards;
    }
 */

}

