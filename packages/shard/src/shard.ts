import { Constructor, deserialize, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import OrbitDB from "orbit-db";
import BN from 'bn.js';
import { AnyPeer, IPFSInstanceExtended } from "./node";
import { PublicKey } from "@dao-xyz/identity";
import { P2PTrust } from "./trust";
import { DBInterface } from "./interface";
import { BinaryDocumentStoreOptions } from "@dao-xyz/orbit-db-bdocstore";
import { BStoreOptions } from '@dao-xyz/orbit-db-bstores';
import { IStoreOptions } from '@dao-xyz/orbit-db-store'
import { BPayload } from '@dao-xyz/bgenerics';
import { waitForAsync } from "@dao-xyz/time";
import { delay } from "@dao-xyz/time";
import { EMIT_HEALTHCHECK_INTERVAL, PeerInfo, ShardPeerInfo } from "./peer";
import { IQueryStoreOptions } from "@dao-xyz/orbit-db-query-store";
import { TRUST_REGION_ACCESS_CONTROLLER } from "./identity";
export const SHARD_INDEX = 0;
const MAX_SHARD_SIZE = 1024 * 500 * 1000;
export const DEFAULT_QUERY_REGION = 'world';
// io


@variant(1)
export class ReplicationRequest {


    @field({ type: 'String' })
    shardChainName: string

    @field({ type: 'u64' })
    index: BN

    @field({ type: option(BStoreOptions) })
    storeOptions: BStoreOptions<any> | undefined;

    @field({ type: 'u64' })
    shardSize: BN

    constructor(obj?: ReplicationRequest) {
        if (obj) {
            Object.assign(this, obj);
        }
    }

}





export type StoreBuilder<B> = (name: string, defaultOptions: IStoreOptions<any, any>, orbitdDB: OrbitDB) => Promise<B>


export type Behaviours<St> = {
    newStore: StoreBuilder<St>
}

export type TypedBehaviours = {
    typeMap: {
        [key: string]: Constructor<any>
    }
};


@variant([0, 0])
export class Shard<T extends DBInterface> extends BPayload {

    @field({ type: 'String' })
    id: string

    @field({ type: 'String' })
    cluster: string

    @field({ type: P2PTrust })
    trust: P2PTrust; // Infrastructure trust region, i.e. what signers can we trust for data for

    @field({ type: 'u64' })
    shardSize: BN

    @field({ type: DBInterface })
    interface: T; // the actual data dbs, all governed by the shard

    /* @field({ type: SingleDBInterface })
    memoryAdded: SingleDBInterface<number, CounterStore>;

    @field({ type: SingleDBInterface })
    memoryRemoved: SingleDBInterface<number, CounterStore>; */

    /*     @field({ type: SingleDBInterface })
        peers: SingleDBInterface<Peer, BinaryDocumentStore<Peer>>
    
     */
    @field({ type: option('String') })
    parentShardCID: string | undefined; // one of the shards in the parent cluster

    @field({ type: 'u64' })
    shardIndex: BN // 0, 1, 2... this index will change the IFPS hash of this shard serialized. This means we can iterate shards without actually saving them in a DB

    shardPeerInfo: ShardPeerInfo | undefined;

    peer: AnyPeer;

    cid: string;

    defaultStoreOptions: IQueryStoreOptions<any, any>;

    constructor(props?: {
        id: string,
        cluster: string
        interface: T
        shardSize: BN
        address: string
        parentShardCID: string
        trust: P2PTrust
        shardIndex: BN
    } | {
        id: string,
        cluster: string
        interface: T
        shardSize: BN
        shardIndex?: BN
        trust?: P2PTrust
    }) {

        super();
        if (props) {
            Object.assign(this, props);
        }

        if (!this.shardIndex) {
            this.shardIndex = new BN(0);
        }

    }

    async init(from: AnyPeer, parentShardCID?: string): Promise<Shard<T>> {
        // TODO: this is ugly but ok for now



        await this.close();
        this.peer = from;

        this.defaultStoreOptions = {
            queryRegion: DEFAULT_QUERY_REGION,
            subscribeToQueries: this.peer.options.isServer,
            accessController: {
                //write: [this.orbitDB.identity.id],
                /*  trustRegionResolver: () => this.trust, */
                type: TRUST_REGION_ACCESS_CONTROLLER
            } as any,
            replicate: this.peer.options.isServer,
            directory: this.peer.options.storeDirectory
        }

        if (parentShardCID) {
            this.parentShardCID = parentShardCID;
        }


        if (!this.trust) {
            this.trust = new P2PTrust({
                rootTrust: PublicKey.from(from.orbitDB.identity),
            })
        }
        await this.trust.init(this.peer, (name) => this.getDBName(name), this.defaultStoreOptions);


        if (!this.shardPeerInfo) {
            this.shardPeerInfo = new ShardPeerInfo(this);
        }
        /* if (!this.peers) {
            this.peers = new SingleDBInterface({
                name: "_peers",
                storeOptions: new BinaryDocumentStoreOptions({
                    indexBy: 'key',
                    objectType: Peer.name
                })
            });
        }

        await this.peers.init(this, {
            recycle: this.peer.options.peersRecycle
        }); */

        /* 
                if (!this.memoryAdded) {
                    this.memoryAdded = new SingleDBInterface({
                        name: "_memoryAdded",
                        storeOptions: new CounterStoreOptions()
                    });
                }
                await this.memoryAdded.init(this);
        
                if (!this.memoryRemoved) {
                    this.memoryRemoved = new SingleDBInterface({
                        name: "_memoryRemoved",
                        storeOptions: new CounterStoreOptions()
                    });
                }
                await this.memoryRemoved.init(this); */

        await this.interface.init(this.peer, (key: string) => this.getDBName(key), this.defaultStoreOptions);



        if (!this.cid) {
            // only needed for write, not needed to be loaded automatically
            await this.save(from.node);
        }

        return this;
    }

    async close() {

        this.shardPeerInfo?.close();
        //this.dbs.forEach(db => { db.db = undefined });
        this.interface.close();
        this.trust?.close();/* 
        this.memoryAdded?.close();
        this.memoryAdded?.close(); */
    }

    //this.blocks = await this.newStore(this.address ? this.address : this.getDBName('blocks')) //await db.feed(this.getDBName('blocks'), this.chain.defaultOptions);
    //await Promise.all(this.dbs.map(db => db.newStore(this)));
    //this.peers = await new BinaryKeyValueStoreOptions<Peer>({ objectType: Peer.name }).newStore(this.peersAddress ? this.peersAddress : this.getDBName("peers"), this.peer.orbitDB, this.peer.options.behaviours.typeMap, this.peer.options.defaultOptions);

    /* async initMetaStores() {

        this.peers.init(this);
        await this.loadMemorySize();
        await this.trust.load();

    } */


    /*  async loadMemorySize() {
         await this.memoryAdded.load();
         await this.memoryRemoved.load();
 
     }
  */
    getQueryTopic(topic: string): string {
        return this.id + "-" + this.cluster + "-" + topic;
    }


    /* async getRemotePeersSize(waitOnlyForOne: boolean = false, maxAggregationTime: number = 10000): Promise<number> {
        const db = this.peers;
        let size: number = undefined;
        const queryPromise = db.query(new QueryRequestV0({
            type: new DocumentQueryRequest({
                queries: [
                    new FieldCompareQuery(
                        {
                            key: 'timestamp',
                            compare: Compare.GreaterOrEqual,
                            value: new BN(+new Date - this.peer.options.peerHealtcheckInterval - this.peer.options.expectedPingDelay) // last offseate is for to compensate for expected IO delays

                        }
                    )

                ]
            }),
        }), (resp) => { size = size ? Math.max(resp.results.length, size) : resp.results.length }, waitOnlyForOne ? 1 : undefined, maxAggregationTime)
        if (waitOnlyForOne) {
            await waitFor(() => size !== undefined, maxAggregationTime)
            // will cause mem leak for a while (max aggregation time)
        }
        else {
            await queryPromise;
        }
        if (size == undefined) { // No peers
            size = 0;
        }
        return size;
    } */

    /*  async loadPeers(waitForReplicationEventsCount: number = 0) {
         if (!this.peers.db) {
             await this.peers.newStore();
         }
 
         // Second argument 
         await this.peers.load();
         if (this.peer.options.isServer) {
             await waitForReplicationEvents(this.peers.db, waitForReplicationEventsCount);
 
         }
         return this.peers;
     } */

    /*  async isSupported(peersCount: number = 1) {
         await this.loadPeers(peersCount);
         return this.peers.db.size >= peersCount
     } */

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

        /*  if (!this.memoryAdded.db) {
             await this.loadMemorySize();
         }
  */
        if (!this.interface.loaded) {
            await this.interface.load();
        }
        /*  if (this.dbs.find(db => !db.db)) {
             await this.loadDBs();
         } */
        // This is not a perfect memory check since, there could
        // be a parallel peer that also wants to add memory at the same  time
        // However though, we will not overshoot greatly 

        // Improvement: Make this synchronized across peers
        // TODOadd proper memory check
        /*   if (this.memoryAdded.db.value - this.memoryRemoved.db.value + sizeBytes > this.shardSize.toNumber()) {
              console.log('Max shard size achieved, request new shard');
              throw new Error("Please perform sharding for chain: " + this.cluster)
              
          }
          await this.memoryAdded.db.inc(sizeBytes); */
        /*  let added = await this.blocks.add(block);
         return added; */
    }



    /* async removeAndFreeMemory(amount: number, remove: () => Promise<string>): Promise<void> {
        let rem = await remove();
        if (rem) {
            await this.memoryRemoved.db.inc(amount);

        }
    } */

    public async startSupportPeer() {

        // This method createsa continous job that performs two things. 
        // 1. Pings the peers data base with peer statistics (such as memory left)
        // 2. Dials parents 

        let parentShard: Shard<any> | undefined = undefined;
        if (this.parentShardCID) {
            parentShard = await Shard.loadFromCID(this.parentShardCID, this.peer.node); //WE CANT LOAD TS IF NOT CONNECTED
            // TODO:  fix to work if parent is a cluster
            await parentShard.init(this.peer);

        }


        /* if (!this.peers.db) {
            await this.peers.newStore();
        } */

        const peerIsSupportingParent = !!this.parentShardCID && this.peer.supportJobs.find((job) => job.shardCID === this.parentShardCID)
        const connectToParentShard = !peerIsSupportingParent && !!parentShard && !this.peer.supportJobs.find((job) => job.connectingToParentShardCID == this.parentShardCID)
        const controller = new AbortController();
        const newJob = {
            shardCID: this.shardPeerInfo._shard.cid,
            controller,
            connectingToParentShardCID: connectToParentShard ? this.parentShardCID : undefined
        }

        this.peer.supportJobs.push(newJob);

        const task = async () => {
            await this.shardPeerInfo.emitHealthcheck();

            // Connect to parent shard, and connects to its peers 
            if (connectToParentShard) {
                let parentPeers = await parentShard.shardPeerInfo.getPeers()
                if (parentPeers.length == 0) {
                    console.error("Failed to swarm connect to parent");
                    //throw new Error("Expected to find at least 1 parent peer, got 0")
                }
                let myAddresses = (await this.peer.node.id()).addresses.map(x => x.toString());
                if (parentPeers?.length > 0) {
                    let thisAddressSet = new Set(myAddresses);
                    const isSelfDial = (peer: PeerInfo) => {
                        for (const addr of peer.addresses) {
                            if (thisAddressSet.has(addr))
                                return true;
                        }
                        return false;
                    }
                    const mySwarmAddresses = new Set((await this.peer.node.swarm.addrs()).map(x => x.addrs).flat(1).map(x => x.toString()));
                    const isAlreadyDialed = (peer: PeerInfo) => {
                        for (const addr of peer.addresses) {
                            if (mySwarmAddresses.has(addr))
                                return true;
                        }
                        return false;
                    }

                    // Connect to all parent peers, we could do better (cherry pick), but ok for now
                    const connectPromises = parentPeers.filter(peer => !isSelfDial(peer) && !isAlreadyDialed(peer)).map((peer) => this.peer.node.swarm.connect(peer.addresses[0]));
                    await Promise.all(connectPromises)
                }
            }
        }

        const cron = async () => {
            let stop = false;
            let promise: Promise<any> = undefined;
            let delayStopper: () => void | undefined = undefined;
            controller.signal.addEventListener("abort", async () => {
                stop = true;
                if (delayStopper)
                    delayStopper();
                await promise;
            });
            while (this.peer.node.isOnline() && !stop) {
                promise = task();
                await promise;
                await delay(EMIT_HEALTHCHECK_INTERVAL, (stopper) => { delayStopper = stopper }); // some delay
            }
        }
        cron();
    }
    /* public async stopSupportPeer() {
        // ??? 
    }
 */
    static async subscribeForReplication(me: AnyPeer, trust: P2PTrust): Promise<void> {
        await me.node.pubsub.subscribe(trust.replicationTopic, async (msg: any) => {
            try {
                let shard = deserialize(Buffer.from(msg.data), Shard);

                // check if enough memory 
                if (shard.shardSize.toNumber() > me.options.replicationCapacity) {
                    console.log(`Can not replicate shard size ${shard.shardSize.toNumber()} with peer capacity ${me.options.replicationCapacity}`)
                    return;
                }
                await shard.init(me);
                // check if is trusted,

                /*    
                WE CAN NOT HAVE THIS CHECK; BECAUSE WE CAN NOT KNOW WHETHER WE HAVE LOADED THE TRUST DB FULLY (WE NEED TO WAIT TM)
                
                if (!shard.trust.isTrusted(PublicKey.from(this.orbitDB.identity))) { 
                      //if not no point replicating
                      console.log(`Can not replicate since not trusted`)
                      return;
                  }
                 */
                await shard.replicate();

            } catch (error) {
                console.error('Invalid replication request', error.toString());
                throw error;
            }
        })
    }

    async requestReplicate(): Promise<void> {
        /*   let shardCounter = await this.chain.getShardCounter();
          if (shardCounter.value < this.index.toNumber()) {
              throw new Error(`Expecting shard counter to be less than the new index ${shardCounter} !< ${this.index}`)
          } */

        const currentPeersCount = async () => (await this.shardPeerInfo.getPeers()).length
     /*    if (await currentPeersCount() == 0) */ {
            // Send message that we need peers for this shard
            // The recieved of the message should be the DB that contains this shard,

        }
        let ser = serialize(this);
        await this.peer.node.pubsub.publish(this.trust.replicationTopic, ser);
        await waitForAsync(async () => await currentPeersCount() > 0, 60000)

    }


    async replicate() {
        /// Shard counter might be wrong because someone else could request sharding at the same time

        //let id = (await this.peer.node.id()).id;

        /*  await Promise.all([
            
         ); */
        await this.interface.load();
        /*      await this.loadMemorySize(); */
        await this.startSupportPeer();
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

    /* get initialized(): boolean {
        if (!this.peer) {
            return false;
        }
        let metaStoresInitialized = !this.peer.options.isServer || (!!this.peers && !!this.memoryAddedAddress && !!this.memoryRemovedAddress)
        return this.interface.initialized && metaStoresInitialized;
    }; */

    getDBName(name: string): string {
        return (this.parentShardCID ? this.parentShardCID : '') + '-' + this.id + '-' + name;
    }

    async save(node: IPFSInstanceExtended): Promise<string> {

        let arr = serialize(this);
        let addResult = await node.add(arr)
        let pinResult = await node.pin.add(addResult.cid)
        this.cid = pinResult.toString();
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

    if (Object.keys(shard.peers.all).length > 0) {
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
        let peersCount = Object.keys(shard.peers.all).length;
        if (peersCount == 0 && opts.startIndex != index) {
            return shards; // dont create a new shard (yet)
        }

        if (Object.keys(shard.peers.all).length < opts.peersLimit) {

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
