import { deserialize, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { Address, IInitializationOptions, IStoreOptions, load, save, Store, StoreLike } from '@dao-xyz/orbit-db-store'
import { IPFS as IPFSInstance } from 'ipfs-core-types';
import { delay, waitForAsync } from "@dao-xyz/time";
import { AnyPeer } from "./peer";
import { BinaryPayload, SystemBinaryPayload } from '@dao-xyz/bpayload';

export const SHARD_INDEX = 0;
const MAX_SHARD_SIZE = 1024 * 500 * 1000;
export const DEFAULT_QUERY_REGION = 'world';
export const MIN_REPLICATION_AMOUNT = 1;
import { MemoryLimitExceededError } from "./errors";
import Logger from 'logplease';
import isNode from 'is-node';
import { RegionAccessController } from "@dao-xyz/orbit-db-trust-web";
import { PublicKey } from "@dao-xyz/identity";
import { Log } from "@dao-xyz/ipfs-log";
import { Entry } from "@dao-xyz/ipfs-log-entry";
import esm from "@dao-xyz/orbit-db-cache";

let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}

const logger = Logger.create('shard', { color: Logger.Colors.Blue })
Logger.setLogLevel('ERROR')
// io
@variant(1)
export class ReplicationRequest {


    @field({ type: 'string' })
    shardChainName: string

    @field({ type: 'u64' })
    index: bigint

    @field({ type: option(Store) })
    store: Store<any>

    @field({ type: 'u64' })
    shardSize: bigint

    constructor(obj?: ReplicationRequest) {
        if (obj) {
            Object.assign(this, obj);
        }
    }

}

export type StoreBuilder<B> = (name: string, defaultOptions: IStoreOptions<any>, orbitdDB: OrbitDB) => Promise<B>

export type Behaviours<St> = {
    newStore: StoreBuilder<St>
}


/* @variant([0, 0]) */

@variant(2)
export class Shard<S extends Store<any>> extends SystemBinaryPayload implements StoreLike<S> {

    @field({ type: 'string' })
    id: string

    @field({ type: 'string' })
    cluster: string

    @field({ type: RegionAccessController })
    trust: RegionAccessController; // Infrastructure trust region, i.e. what signers can we trust for data for

    @field({ type: Store })
    store: S; // the actual data dbs, all governed by the shard

    @field({ type: option('string') })
    parentAddress?: string; // one of the shards in the parent cluster

    @field({ type: 'u64' })
    shardIndex: bigint // 0, 1, 2... this index will change the IFPS hash of this shard serialized. This means we can iterate shards without actually saving them in a DB


    /*     peer: AnyPeer; */

    address: Address;

    /* storeOptions: IQueryStoreOptions<T, T, any>
    */
    constructor(props?: {
        id: string,
        cluster: string
        store: S
        parentAddress: Address | string
        trust: RegionAccessController
        shardIndex: bigint
    } | {
        id: string,
        cluster: string
        store: S
        shardIndex?: bigint
        trust?: RegionAccessController

    }) {

        super();
        if (props) {

            this.id = props.id;
            this.cluster = props.cluster;
            this.store = props.store;
            this.shardIndex = props.shardIndex;
            this.trust = props.trust;
            this.parentAddress = props["parentAddress"]?.toString();
        }

        if (!this.shardIndex) {
            this.shardIndex = 0n;
        }

    }

    async init(ipfs: IPFSInstance<{}>, key: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<any>): Promise<void> {
        await this.trust.init(ipfs, key, sign, options);
        await this.store.init(ipfs, key, sign, options);

        // TODO: this is ugly but ok for now
        /*   if (this.peer && this.peer !== from) {
              throw new Error("Reinitialization with different peer might lead to unexpected behaviours. Create a new instance instead")
          } */
        //await this.close();
        /*   this.peer = from;

        const result = await this.peer.getCachedTrustOrSet(this.trust, this);
        this.trust = result.trust;*/

        /*    if (!this.shardPeerInfo) {
               this.shardPeerInfo = new ShardPeerInfo(this);
           }
    */

        /* await this.peer.orbitDB.open(this.store); */ // this.storeOptions

        /*  if (this.trust.afterShardSave) {
             this.trust.afterShardSave();
         }
  */
        return;
    }

    drop(): Promise<void> {
        return this.store.drop();
    }
    sync(heads: Entry<S>[]): Promise<void> {
        return this.sync(heads);
    }
    get replicationTopic(): string {
        return this.store.replicationTopic;
    }
    get events(): import("events") {
        return this.store.events;
    }

    get oplog(): Log<S> {
        return this.store.oplog;
    }
    get cache(): esm {
        return this.store.cache;
    }
    get replicate(): boolean {
        return this.store.replicate;
    }
    getHeads(): Promise<Entry<S>[]> {
        return this.store.getHeads();
    }
    get name(): string {
        return this.store.name;
    }

    async load() {
        await this.trust.load();
        await this.store.load();
    }


    async save(ipfs: any, options?: {
        format?: string;
        pin?: boolean;
        timeout?: number;
    }): Promise<Address> {
        const address = await save(ipfs, this, options)
        this.address = address;
        return address;
    }

    static load(ipfs: any, address: Address, options?: {
        timeout?: number;
    }) {
        return load(ipfs, address, Shard, options)
    }


    /*  async open(from: AnyPeer): Promise<Shard<S>> {
         // TODO: this is ugly but ok for now
         if (this.peer && this.peer !== from) {
             throw new Error("Reinitialization with different peer might lead to unexpected behaviours. Create a new instance instead")
         }
         //await this.close();
         this.peer = from;
 
 
 
         if (!this.trust) {
             this.trust = new RegionAccessController({
                 rootTrust: from.orbitDB.identity
             })
             await this.peer.orbitDB.open(this.trust) // this.storeOptions
 
         }
 
         const result = await this.peer.getCachedTrustOrSet(this.trust, this);
         this.trust = result.trust;
 
         if (!this.shardPeerInfo) {
             this.shardPeerInfo = new ShardPeerInfo(this);
         }
 
 
         await this.peer.orbitDB.open(this.store); // this.storeOptions
 
         if (result.afterShardSave) {
             result.afterShardSave();
         }
         return this;
     }
  */
    async close() {
        /* 
                await this.peer?.removeAndCloseCachedTrust(this.trust, this);
                await this.shardPeerInfo?.close(); */
        await this.store.close();
    }

    getQueryTopic(topic: string): string {
        return this.id + "-" + this.cluster + "-" + topic;
    }


    public async startSupportPeer() {

        // This method createsa continous job that performs two things. 
        // 1. Pings the peers data base with peer statistics (such as memory left)
        // 2. Dials parents 

        let parentShard: Shard<any> | undefined = undefined;
        if (this.parentAddress) {
            parentShard = await Shard.load(this.peer.node, Address.parse(this.parentAddress)); //WE CANT LOAD TS IF NOT CONNECTED
            // TODO:  fix to work if parent is a cluster
            // await parentShard.open(this.peer);
        }
        /*
        
                const peerIsSupportingParent = !!this.parentAddress && this.peer.supportJobs.has(this.parentAddress)
        
                // TODO make more efficient (this.peer.supportJobs.values()...)
                const connectToParentShard = !peerIsSupportingParent && !!parentShard && ![...this.peer.supportJobs.values()].find((job) => job.connectingToParentShardCID == this.parentAddress)
                const controller = new AbortController();
                const newJob = {
                    shard: this.shardPeerInfo._shard,
                    controller,
                    connectingToParentShardCID: connectToParentShard ? this.parentAddress : undefined
                }
        
                 this.peer.supportJobs.set(this.shardPeerInfo._shard.address.toString(), newJob);
        
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
                    while (this.peer.node.isOnline() && !stop) { // 
                        promise = task();
                        await promise;
                        await delay(EMIT_HEALTHCHECK_INTERVAL, { stopperCallback: (stopper) => { delayStopper = stopper } }); // some delay
                    }
                }
                cron();*/
    }
    /* public async stopSupportPeer() {
        // ??? 
    }
 */
    /* static async subscribeForReplication(me: AnyPeer, trust: RegionAccessController, onReplication?: (shard: Shard<any>) => void): Promise<void> {
        await me.node.pubsub.subscribe(trust.replicationTopic, async (msg: any) => {
            try {
                let shard = deserialize(msg.data, Shard);
                if (me.supportJobs.has(shard.address.toString())) {
                    return; // Already replicated
                }

               
                const trustResult = (await me.getCachedTrustOrSet(trust, shard));
                shard.trust = trustResult.trust  // this is necessary, else the shard with initialize with a new trust region
                await shard.support(me);
                if (trustResult.afterShardSave) {
                    trustResult.afterShardSave();
                }

                if (onReplication)
                    onReplication(shard);

            } catch (error) {
                if (error instanceof MemoryLimitExceededError) {
                    logger.info(error.message);
                    return;
                }
                logger.error('Invalid replication request', error.toString());
                throw error;
            }
        })
    } */

    /*  _requestingReplicationPromise: Promise<void>;
     async requestReplicate(shardIndex?: bigint): Promise<void> {
         let shard = this as Shard<S>;
         if (shardIndex !== undefined) {
             shard = await this.createShardWithIndex(shardIndex);
         }
 
         await this._requestingReplicationPromise;
         this._requestingReplicationPromise = new Promise(async (resolve, reject) => {
             const currentPeersCount = async () => (await this.shardPeerInfo.getPeers()).length
             let ser = serialize(shard);
             await this.peer.node.pubsub.publish(this.trust.replicationTopic, ser);
             await waitForAsync(async () => await currentPeersCount() >= MIN_REPLICATION_AMOUNT, {
                 timeout: 60000,
                 delayInterval: 50
             })
             resolve();
         })
         await this._requestingReplicationPromise;
 
     } */


    /*  async support(peer: AnyPeer) {
         /// Shard counter might be wrong because someone else could request sharding at the same time
         if (!v8) {
             throw new Error("Can not replicate outside a Node environment");
         }
         // check if enough memory 
         const usedHeap = v8.getHeapStatistics().used_heap_size;
         if (usedHeap > peer.options.heapSizeLimit) {
             throw new MemoryLimitExceededError(`Can not replicate with peer heap size limit: ${peer.options.heapSizeLimit} when used heap is: ${usedHeap}`);
         }
 
         await peer.orbitDB.open(this);
         await this.startSupportPeer();
 
     }
  */


    getDBName(name: string): string {
        return (this.parentAddress ? this.parentAddress : '') + '-' + this.id + '-' + this.shardIndex + "-" + name;
    }

    async requestNewShard(): Promise<void> {

        return this.requestReplicate(this.shardIndex + 1n)
    }

    async createShardWithIndex(shardIndex: bigint, peer: AnyPeer = this.peer): Promise<Shard<S>> {
        const shard = new Shard<S>({
            shardIndex,
            id: this.id,
            cluster: this.cluster,
            parentAddress: this.parentAddress,
            store: this.store.clone(this.cluster + shardIndex) as S,
            resourceRequirements: this.resourceRequirements,
            trust: this.trust,
        })
        await peer.orbitDB.open
        return shard;
    }



}

/* static get recursiveStoreOption() {
       return new BinaryDocumentStoreOptions<Shard<any>>({
           objectType: Shard.name,
           indexBy: 'cid'
       })

   } */


/* get defaultStoreOptions(): IQueryStoreOptions<T> {
       if (!this.peer) {
           throw new Error("Not initialized")
       }
       return {
           queryRegion: DEFAULT_QUERY_REGION,
           replicationTopic: () => this.trust.address,
           accessController: {
               type: DYNAMIC_ACCESS_CONTROLER,
               trustResolver: () => this.trust,
               heapSizeLimit: v8 ? () => Math.min(v8.getHeapStatistics().total_heap_size, this.peer.options.heapSizeLimit) : () => this.peer.options.heapSizeLimit,
               onMemoryExceeded: (_entry: Entry<T>) => {
                   if (this._requestingReplicationPromise) {
                       // Already replicating
                       logger.info("Memory exceeded heap, but replication is already in process")
                   }
                   else {
                       this.requestNewShard();
                   }
               },
               allowAll: !this.peer.options.isServer, // because, if we are not a "server" we don't care about our own ACL, we just want to append everything we write (we never replicate others)
               storeOptions: {
                   subscribeToQueries: this.peer.options.isServer,
                   replicate: this.peer.options.isServer,
                   directory: this.peer.options.storeDirectory,
                   queryRegion: DEFAULT_QUERY_REGION, // the acl has a DB that you also can query
                   replicationTopic: () => this.trust.address,
               }
           } as any,
           subscribeToQueries: this.peer.options.isServer,
           replicate: this.peer.options.isServer,
           directory: this.peer.options.storeDirectory,
           typeMap: {},
           nameResolver: (name: string) => this.getDBName(name)
       }
   } */