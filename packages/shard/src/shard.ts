import { Constructor, deserialize, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import OrbitDB from "orbit-db";
import BN from 'bn.js';
import Store from "orbit-db-store";
import CounterStore from "orbit-db-counterstore";
import { BinaryKeyValueStore, BinaryKeyValueStoreOptions } from '@dao-xyz/orbit-db-bkvstore';
/* import {  waitForReplicationEvents } from "./stores"; */
import { delay, waitFor, waitForReplicationEvents } from "./utils";
import { AnyPeer, IPFSInstanceExtended } from "./node";
import { Peer } from "./peer";
import { PublicKey } from "./key";
import { P2PTrust } from "./trust";
import { DBInterface, onReplicationMark } from "./interface";
import { BinaryDocumentStoreOptions } from "@dao-xyz/orbit-db-bdocstore";
import { StoreOptions } from '@dao-xyz/orbit-db-bstores';
export const SHARD_INDEX = 0;
const MAX_SHARD_SIZE = 1024 * 500 * 1000;

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




@variant([0, 0])
export class Shard<T extends DBInterface> {

    @field({ type: 'String' })
    cluster: string

    @field({ type: P2PTrust })
    trust: P2PTrust; // Infrastructure trust region, i.e. what signers can we trust for data for

    @field({ type: 'u64' })
    shardSize: BN

    @field({ type: DBInterface })
    interface: T; // the actual data dbs, all governed by the shard

    @field({ type: 'String' })
    memoryAddedAddress: string; // Track memory added to data db

    @field({ type: 'String' })
    memoryRemovedAddress: string; // Track memory removed to data db

    @field({ type: 'String' })
    peersAddress: string; // peers data db

    @field({ type: option('String') })
    parentShardCID: string | undefined; // one of the shards in the parent cluster

    @field({ type: 'u64' })
    shardIndex: BN // 0, 1, 2... this index will change the IFPS hash of this shard serialized. This means we can iterate shards without actually saving them in a DB

    _peers: BinaryKeyValueStore<Peer>
    memoryAdded?: CounterStore;
    memoryRemoved?: CounterStore;
    peer: AnyPeer;

    cid: string;
    constructor(props?: {
        cluster: string
        interface: T
        shardSize: BN
        address: string
        peersAddress: string
        memoryAddedAddress: string
        memoryRemovedAddress: string
        parentShardCID: string
        trust: P2PTrust
        shardIndex: BN
    } | {
        cluster: string
        interface: T
        shardSize: BN
        shardIndex?: BN
        trust?: P2PTrust
    }) {


        if (props) {
            Object.assign(this, props);
        }

        if (!this.shardIndex) {
            this.shardIndex = new BN(0);
        }

    }

    async init(from: AnyPeer, parentShardCID?: string): Promise<Shard<T>> {
        // TODO: this is ugly but ok for now
        from.options.behaviours.typeMap[Peer.name] = Peer;

        await this.close();

        this.peer = from;
        let isInitialized = this.initialized;

        if (!this.trust) {
            this.trust = new P2PTrust({
                rootTrust: PublicKey.from(from.orbitDB.identity),
            })
        }


        this.interface.init(this);

        if (parentShardCID) {
            this.parentShardCID = parentShardCID;
        }

        if (!isInitialized) {
            // only needed for write, not needed to be loaded automatically
            await this.interface.load(); // TODO we should just try to predict DB addresses, no need to LOAD them into memory
            await this.trust.init(this);
            await this.loadMetaStores();
            await this.save(from.node);
        }
        return this;
    }

    async close() {

        this._peers = undefined;
        //this.dbs.forEach(db => { db.db = undefined });
        this.interface.close();
        this.trust?.close();
        this.memoryAdded = undefined;
        this.memoryRemoved = undefined;
    }

    async loadMetaStores() {
        //this.blocks = await this.newStore(this.address ? this.address : this.getDBName('blocks')) //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
        //await Promise.all(this.dbs.map(db => db.newStore(this)));
        this._peers = await new BinaryKeyValueStoreOptions<Peer>({ objectType: Peer.name }).newStore(this.peersAddress ? this.peersAddress : this.getDBName("peers"), this.peer.orbitDB, this.peer.options.behaviours.typeMap, this.peer.options.defaultOptions);
        onReplicationMark(this._peers);
        this.peersAddress = this._peers.address.toString();
        await this.loadMemorySize();
        this.memoryAddedAddress = this.memoryAdded.address.toString();
        this.memoryRemovedAddress = this.memoryRemoved.address.toString();
        await this.trust.load();

    }


    async loadMemorySize() {
        this.memoryAdded = await this.peer.orbitDB.counter(this.memoryAddedAddress ? this.memoryAddedAddress : this.getDBName('memory_added'), this.peer.options.defaultOptions);
        onReplicationMark(this.memoryAdded);


        this.memoryRemoved = await this.peer.orbitDB.counter(this.memoryRemovedAddress ? this.memoryRemovedAddress : this.getDBName('memory_removed'), this.peer.options.defaultOptions);
        onReplicationMark(this.memoryRemoved);

    }

    get queryTopic(): string {
        return this.cluster;
    }

    get peers(): BinaryKeyValueStore<Peer> {
        if (!this._peers) {
            throw new Error("Peers not loaded");
        }
        return this._peers;
    }

    async loadPeers(waitForReplicationEventsCount: number = 0) {
        if (!this._peers) {
            this._peers = await new BinaryKeyValueStoreOptions<Peer>({ objectType: Peer.name }).newStore(this.peersAddress ? this.peersAddress : this.getDBName("peers"), this.peer.orbitDB, this.peer.options.behaviours.typeMap, this.peer.options.defaultOptions);
        }

        // Second argument 
        await this._peers.load();
        if (this.peer.options.isServer) {
            await waitForReplicationEvents(this._peers, waitForReplicationEventsCount);

        }
        return this._peers;
    }

    async isSupported(peersCount: number = 1) {
        await this.loadPeers(peersCount);
        return Object.keys(this._peers.all).length >= peersCount
    }

    /* async loadDBs(): Promise<DB<any>[]> {
        await Promise.all(this.dbs.map(db => db.load(this)))
        return this.dbs;
    }

    async loadDB<T extends Store<any, any>>(name: string): Promise<DB<T>> {
        let db = this.dbs.find(x => x.name == name);
        await db.load(this);
        return db;
    }
    async loadShardDB<B extends Store<Shard, any>>(name: string): Promise<B> {
        let db = await this.loadDB(name);
        return db.db as B;
    }
 */


    async makeSpace(sizeBytes: number): Promise<void> {

        if (sizeBytes > MAX_SHARD_SIZE) {
            throw new Error("Block too large");
        }

        if (!this.memoryAdded) {
            await this.loadMemorySize();
        }

        if (!this.interface.initialized) {
            await this.interface.load();
        }
        /*  if (this.dbs.find(db => !db.db)) {
             await this.loadDBs();
         } */
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



    async removeAndFreeMemory(amount: number, remove: () => Promise<string>): Promise<void> {
        let rem = await remove();
        if (rem) {
            await this.memoryRemoved.inc(amount);

        }
    }

    public async addPeer() {

        // Connect to parent shard, and connects to its peers 
        let parentPeers: BinaryKeyValueStore<Peer> = undefined;
        if (this.parentShardCID) {
            let parentShard = await Shard.loadFromCID(this.parentShardCID, this.peer.node);
            // TODO:  fix to work if parent is a cluster
            await parentShard.init(this.peer);
            parentPeers = await parentShard.loadPeers(1); // Expect at least 1 peer from parent

        }
        const task = async () => {
            let thisPeer = new Peer({
                key: PublicKey.from(this.peer.orbitDB.identity),
                addresses: (await this.peer.node.id()).addresses.map(x => x.toString()),
                timestamp: new BN(+new Date)
            });
            await this._peers.set(thisPeer.key.toString(), thisPeer);

            if (parentPeers) {
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

        await task();
        /*  const cron = async () => {
             while (this.peer.node.isOnline()) {
                 await task();
                 await delay(10000)
             }
 
         }
         cron(); */
    }

    async requestReplicate(): Promise<void> {
        /*   let shardCounter = await this.chain.getShardCounter();
          if (shardCounter.value < this.index.toNumber()) {
              throw new Error(`Expecting shard counter to be less than the new index ${shardCounter} !< ${this.index}`)
          } */

        if (Object.keys(this._peers.all).length == 0) {
            // Send message that we need peers for this shard
            // The recieved of the message should be the DB that contains this shard,
            let ser = serialize(this);
            await this.peer.node.pubsub.publish(this.trust.replicationTopic, ser);
        }

        await waitFor(() => Object.keys(this._peers.all).length > 0)

        if (Object.keys(this._peers.all).length == 0) {
            throw new Error("Fail to perform sharding");
        }

    }


    async replicate() {
        /// Shard counter might be wrong because someone else could request sharding at the same time

        //let id = (await this.peer.node.id()).id;

        /*  await Promise.all([
             this.loadPeers(),
             this.interface.load(),
             this.loadMemorySize()
         ]); */

        await this.addPeer();

        /* await this.peer.node.pubsub.subscribe(this.queryTopic, async (msg: Message) => {
            try {
                let query = deserialize(Buffer.from(msg.data), QueryRequestV0);
                let results = await this.interface.query(query);
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
        }) */
        /*
         
        serialize(new Peer({
            capacity: new BN(db.replicationCapacity),
            id
        }))
        */

        //await this.loadBlocks();
    }

    get initialized(): boolean {
        let metaStoresInitialized = !this.peer.options.isServer || (!!this.peersAddress && !!this.memoryAddedAddress && !!this.memoryRemovedAddress)
        return this.interface.initialized && metaStoresInitialized;
    };

    getDBName(name: string): string {
        return (this.parentShardCID ? this.parentShardCID : '-') + '-' + name;
    }

    async save(node: IPFSInstanceExtended): Promise<string> {
        if (!this.initialized) {
            throw new Error("Not initialized");
        }

        let arr = serialize(this);
        let addResult = await node.add(arr)
        let pinResult = await node.pin.add(addResult.cid)
        this.cid = pinResult.toString();

        await this.trust.save(node);

        return this.cid;
    }


    static async loadFromCID<T extends DBInterface>(cid: string, node: IPFSInstanceExtended) {
        let arr = await node.cat(cid);
        for await (const obj of arr) {
            let der = deserialize<Shard<T>>(Buffer.from(obj), Shard);
            der.cid = cid;
            return der;

        }
    }


    static get recursiveStoreOption() {
        return new BinaryDocumentStoreOptions<Shard<any>>({
            objectType: Shard.name,
            indexBy: 'cid'
        })

    }
}





// TODO verify serialization work with inheritance
/* @variant(2)
export class RecursiveShard<T extends Store<any, any>> extends Shard<BinaryDocumentStore<Shard<T>>> {
 */


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

/* constructor(props?: {
    id: string
    cluster: string
    storeOptions: StoreOptions<BinaryKeyValueStore<Shard<T>>> | undefined
    shardSize: BN
    address: string
    peersAddress: string
    memoryAddedAddress: string
    memoryRemovedAddress: string
    parentShardCID: string
    createdAt: number
    trust: P2PTrust,
    acl: ACL
} | {
    cluster: string
    shardSize: BN,
    trust?: P2PTrust,
    acl?: ACL
}) {
    const storeOptions = new BinaryDocumentStoreOptions<Shard<T>>({
        objectType: RecursiveShard.name,
        indexBy: 'id'
    })

    super({ ...props, storeOptions });
}

async init(from: AnyPeer, parent?: RecursiveShard<BinaryDocumentStore<Shard<T>>>): Promise<RecursiveShard<T>> {
    from.options.behaviours.typeMap[RecursiveShard.name] = RecursiveShard;
    await super.init(from, parent);
    return this;
} */

/*  async addShard(shard: Shard<T>) {
     if (shard.cid != undefined) {
         throw new Error("Already initialized. `addShard` requires an uniniialized shard")
     }
 
     shard.parentShardCID = this.cid;
     await this.blocks.put(shard);
 } */
/*
async getWritableShard(storeOptions: StoreOptions<T>, peer: AnyPeer): Promise<Shard<T>> {
    // Get the latest shard that have non empty peer
    let lastShard = undefined;

    // get "latest" shard


    const shard = new Shard<T>({
        id: randomUUID(),
        cluster: this.cluster,
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
        await shard.requestReplicate();
        this.blocks.put(shard);
        return shard;
    }
    return lastShard;


} 
*/

/*  async loadShard(index: number, options: { expectedPeerReplicationEvents?: number, expectedBlockReplicationEvents?: number } = { expectedPeerReplicationEvents: 0, expectedBlockReplicationEvents: 0 }): Promise<Shard<T>> {
     // Get the latest shard that have non empty peer
     let block: Shard<T> = await (await this.loadBlocks(index + 1)).get(index.toString())[0];
     await block.init(this.peer);
     await block.loadPeers(options.expectedPeerReplicationEvents);
     await block.loadBlocks(options.expectedBlockReplicationEvents);
     return block;
 } */


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

/* }
 */
