import { Constructor, deserialize, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import { OrbitDB } from "@dao-xyz/orbit-db";
import BN from 'bn.js';
import { DBInterface } from "@dao-xyz/orbit-db-store-interface";
import { BinaryDocumentStoreOptions } from "@dao-xyz/orbit-db-bdocstore";
import { BStoreOptions } from '@dao-xyz/orbit-db-bstores';
import { IStoreOptions } from '@dao-xyz/orbit-db-store'
import { ResultSource } from '@dao-xyz/bquery';
import { waitForAsync } from "@dao-xyz/time";
import { IPFS as IPFSInstance } from 'ipfs-core-types';
import { delay } from "@dao-xyz/time";
import { AnyPeer, EMIT_HEALTHCHECK_INTERVAL, PeerInfo, ShardPeerInfo } from "./peer";
import { IQueryStoreOptions } from "@dao-xyz/orbit-db-query-store";
import { DYNAMIC_ACCESS_CONTROLER } from "@dao-xyz/orbit-db-dynamic-access-controller";
import { P2PTrust } from '@dao-xyz/orbit-db-trust-web'

export const SHARD_INDEX = 0;
const MAX_SHARD_SIZE = 1024 * 500 * 1000;
export const DEFAULT_QUERY_REGION = 'world';
export const MIN_REPLICATION_AMOUNT = 1;
import v8 from 'v8';
import { MemoryLimitExceededError } from "./errors";
import Logger from 'logplease';
import { Entry } from "@dao-xyz/ipfs-log";
const logger = Logger.create('shard', { color: Logger.Colors.Blue })
Logger.setLogLevel('ERROR')
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

export class ResourceRequirements { }

@variant(0)
export class NoResourceRequirements extends ResourceRequirements { }

@variant([0, 0])
export class Shard<T extends DBInterface> extends ResultSource {

    @field({ type: 'String' })
    id: string

    @field({ type: 'String' })
    cluster: string

    @field({ type: P2PTrust })
    trust: P2PTrust; // Infrastructure trust region, i.e. what signers can we trust for data for

    @field({ type: ResourceRequirements })
    resourceRequirements: ResourceRequirements

    @field({ type: DBInterface })
    interface: T; // the actual data dbs, all governed by the shard

    @field({ type: option('String') })
    parentShardCID: string | undefined; // one of the shards in the parent cluster

    @field({ type: 'u64' })
    shardIndex: BN // 0, 1, 2... this index will change the IFPS hash of this shard serialized. This means we can iterate shards without actually saving them in a DB

    shardPeerInfo: ShardPeerInfo | undefined;

    peer: AnyPeer;

    cid: string;

    storeOptions: IQueryStoreOptions<T, any>

    constructor(props?: {
        id: string,
        cluster: string
        interface: T
        resourceRequirements: ResourceRequirements
        address: string
        parentShardCID: string
        trust: P2PTrust
        shardIndex: BN
    } | {
        id: string,
        cluster: string
        interface: T
        resourceRequirements: ResourceRequirements
        shardIndex?: BN
        trust?: P2PTrust

    }) {

        super();
        if (props) {

            Object.assign(this, props); // TODO fix types, storeOPtions are only partially intialized at best
        }

        if (!this.shardIndex) {
            this.shardIndex = new BN(0);
        }

    }
    get defaultStoreOptions(): IQueryStoreOptions<T, any> {
        if (!this.peer) {
            throw new Error("Not initialized")
        }
        return {
            queryRegion: DEFAULT_QUERY_REGION,
            subscribeToQueries: this.peer.options.isServer,
            accessController: {
                type: DYNAMIC_ACCESS_CONTROLER,
                trustResolver: () => this.trust,
                heapSizeLimit: v8 ? () => Math.min(v8.getHeapStatistics().total_heap_size, this.peer.options.heapSizeLimit) : () => this.peer.options.heapSizeLimit,
                onMemoryExceeded: (entry: Entry<any>) => {
                    if (this._requestingReplicationPromise) {
                        // Already replicating
                        logger.info("Memory exceeded heap, but replication is already in process")
                    }
                    else {
                        this.requestNewShard();
                    }
                },
                appendAll: !this.peer.options.isServer // because, if we are not a "server" we don't care about our own ACL, we just want to append everything we write (we never replicate others)
            } as any,
            replicate: this.peer.options.isServer,
            directory: this.peer.options.storeDirectory,
            typeMap: {},
            nameResolver: (name: string) => this.getDBName(name)
        }
    }

    async init(from: AnyPeer, parentShardCID?: string): Promise<Shard<T>> {
        // TODO: this is ugly but ok for now

        await this.close();
        this.peer = from;
        this.storeOptions = this.defaultStoreOptions;


        if (parentShardCID) {
            this.parentShardCID = parentShardCID;
        }

        if (!this.trust) {
            this.trust = new P2PTrust({
                rootTrust: from.orbitDB.identity.toSerializable()
            })
        }
        await this.trust.init(this.peer.orbitDB, this.storeOptions);
        this.trust = await this.peer.getCachedTrustOrSet(this.trust, this);


        if (!this.shardPeerInfo) {
            this.shardPeerInfo = new ShardPeerInfo(this);
        }

        await this.interface.init(this.peer.orbitDB, this.storeOptions);



        if (!this.cid) {
            // only needed for write, not needed to be loaded automatically
            await this.save(from.node);
        }

        return this;
    }

    async close() {

        await this.peer?.removeAndCloseCachedTrust(this.trust, this);
        await this.shardPeerInfo?.close();
        //this.dbs.forEach(db => { db.db = undefined });
        await this.interface.close();
        /* this.trust?.close();
        this.memoryAdded?.close();
        this.memoryAdded?.close(); */
    }

    getQueryTopic(topic: string): string {
        return this.id + "-" + this.cluster + "-" + topic;
    }


    async makeSpace(sizeBytes: number): Promise<void> {
        if (sizeBytes > MAX_SHARD_SIZE) {
            throw new Error("Block too large");
        }
        if (!this.interface.loaded) {
            await this.interface.load();
        }
    }

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

        const peerIsSupportingParent = !!this.parentShardCID && this.peer.supportJobs.find((job) => job.shard.cid === this.parentShardCID)
        const connectToParentShard = !peerIsSupportingParent && !!parentShard && !this.peer.supportJobs.find((job) => job.connectingToParentShardCID == this.parentShardCID)
        const controller = new AbortController();
        const newJob = {
            shard: this.shardPeerInfo._shard,
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

                // check if is trusted,

                /*    
                WE CAN NOT HAVE THIS CHECK; BECAUSE WE CAN NOT KNOW WHETHER WE HAVE LOADED THE TRUST DB FULLY (WE NEED TO WAIT TM)
                
                if (!shard.trust.isTrusted(PublicKey.from(this.orbitDB.identity))) { 
                      //if not no point replicating
                      console.log(`Can not replicate since not trusted`)
                      return;
                  }
                 */
                await shard.replicate(me);

            } catch (error) {
                if (error instanceof MemoryLimitExceededError) {
                    logger.info(error.message);
                    return;
                }
                logger.error('Invalid replication request', error.toString());
                throw error;
            }
        })
    }
    _requestingReplicationPromise: Promise<void>;
    async requestReplicate(shardIndex?: BN): Promise<void> {
        let shard = this as Shard<T>;
        if (shardIndex !== undefined) {
            shard = new Shard<T>({
                shardIndex,
                id: this.id,
                cluster: this.cluster,
                parentShardCID: this.parentShardCID,
                interface: this.interface.clone() as T,
                resourceRequirements: this.resourceRequirements,
                trust: this.trust
            })
            await shard.init(this.peer, this.parentShardCID);
        }
        /*   let shardCounter = await this.chain.getShardCounter();
          if (shardCounter.value < this.index.toNumber()) {
              throw new Error(`Expecting shard counter to be less than the new index ${shardCounter} !< ${this.index}`)
          } */
        await this._requestingReplicationPromise;
        this._requestingReplicationPromise = new Promise(async (resolve, reject) => {
            const currentPeersCount = async () => (await this.shardPeerInfo.getPeers()).length
            let ser = serialize(shard);
            await this.peer.node.pubsub.publish(this.trust.replicationTopic, ser);
            await waitForAsync(async () => await currentPeersCount() >= MIN_REPLICATION_AMOUNT, 60000)
            resolve();
        })
        await this._requestingReplicationPromise;

    }

    async requestNewShard(): Promise<void> {

        return this.requestReplicate(this.shardIndex.addn(1))
    }


    async replicate(peer: AnyPeer) {
        /// Shard counter might be wrong because someone else could request sharding at the same time
        if (!v8) {
            throw new Error("Can not replicate outside a Node environment");
        }
        // check if enough memory 
        const usedHeap = v8.getHeapStatistics().used_heap_size;
        if (usedHeap > peer.options.heapSizeLimit) {
            throw new MemoryLimitExceededError(`Can not replicate with peer heap size limit: ${peer.options.heapSizeLimit} when used heap is: ${usedHeap}`);
        }

        await this.init(peer);
        await this.interface.load();
        await this.startSupportPeer();

    }

    getDBName(name: string): string {
        return (this.parentShardCID ? this.parentShardCID : '') + '-' + this.id + '-' + this.shardIndex.toNumber() + "-" + name;
    }

    async save(node: IPFSInstance): Promise<string> {

        let arr = serialize(this);
        let addResult = await node.add(arr)
        let pinResult = await node.pin.add(addResult.cid)
        this.cid = pinResult.toString();
        return this.cid;
    }


    static async loadFromCID<T extends DBInterface>(cid: string, node: IPFSInstance) {
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