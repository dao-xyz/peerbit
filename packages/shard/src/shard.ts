import { deserialize, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { IStoreOptions, Store } from '@dao-xyz/orbit-db-store'
import { IPFS as IPFSInstance } from 'ipfs-core-types';
import { delay, waitForAsync } from "@dao-xyz/time";
import { AnyPeer, EMIT_HEALTHCHECK_INTERVAL, PeerInfo, ShardPeerInfo } from "./peer";
import { BinaryPayload, SystemBinaryPayload } from '@dao-xyz/bpayload';

export const SHARD_INDEX = 0;
const MAX_SHARD_SIZE = 1024 * 500 * 1000;
export const DEFAULT_QUERY_REGION = 'world';
export const MIN_REPLICATION_AMOUNT = 1;
import { MemoryLimitExceededError } from "./errors";
import Logger from 'logplease';
import isNode from 'is-node';
import { RegionAccessController } from "@dao-xyz/orbit-db-trust-web";

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

export class ResourceRequirements { }

@variant(0)
export class NoResourceRequirements extends ResourceRequirements { }

/* @variant([0, 0]) */

@variant(2)
export class Shard<S extends Store<any>> extends SystemBinaryPayload {

    @field({ type: 'string' })
    id: string

    @field({ type: 'string' })
    cluster: string

    @field({ type: RegionAccessController })
    trust: RegionAccessController; // Infrastructure trust region, i.e. what signers can we trust for data for

    @field({ type: ResourceRequirements })
    resourceRequirements: ResourceRequirements

    @field({ type: Store })
    store: S; // the actual data dbs, all governed by the shard

    @field({ type: option('string') })
    parentShardCID: string | undefined; // one of the shards in the parent cluster

    @field({ type: 'u64' })
    shardIndex: bigint // 0, 1, 2... this index will change the IFPS hash of this shard serialized. This means we can iterate shards without actually saving them in a DB

    shardPeerInfo: ShardPeerInfo | undefined;

    peer: AnyPeer;

    cid: string;

    /* storeOptions: IQueryStoreOptions<T, T, any>
 */
    constructor(props?: {
        id: string,
        cluster: string
        store: S
        resourceRequirements: ResourceRequirements
        address: string
        parentShardCID: string
        trust: RegionAccessController
        shardIndex: bigint
    } | {
        id: string,
        cluster: string
        store: S
        resourceRequirements: ResourceRequirements
        shardIndex?: bigint
        trust?: RegionAccessController

    }) {

        super();
        if (props) {

            this.id = props.id;
            this.cluster = props.cluster;
            this.store = props.store;
            this.resourceRequirements = props.resourceRequirements;
            this.shardIndex = props.shardIndex;
            this.trust = props.trust;
        }

        if (!this.shardIndex) {
            this.shardIndex = 0n;
        }

    }
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

    async open(from: AnyPeer): Promise<Shard<S>> {
        // TODO: this is ugly but ok for now
        if (this.peer && this.peer !== from) {
            throw new Error("Reinitialization with different peer might lead to unexpected behaviours. Create a new instance instead")
        }
        //await this.close();
        this.peer = from;
        /*  this.storeOptions = this.defaultStoreOptions; */




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

        if (!this.cid) {
            // only needed for write, not needed to be loaded automatically
            await this.save(from.node);
        }

        if (result.afterShardSave) {
            result.afterShardSave();
        }
        return this;
    }

    async close() {

        await this.peer?.removeAndCloseCachedTrust(this.trust, this);
        await this.shardPeerInfo?.close();
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
        if (this.parentShardCID) {
            parentShard = await Shard.loadFromCID(this.parentShardCID, this.peer.node); //WE CANT LOAD TS IF NOT CONNECTED
            // TODO:  fix to work if parent is a cluster
            // await parentShard.open(this.peer);
        }

        const peerIsSupportingParent = !!this.parentShardCID && this.peer.supportJobs.has(this.parentShardCID)

        // TODO make more efficient (this.peer.supportJobs.values()...)
        const connectToParentShard = !peerIsSupportingParent && !!parentShard && ![...this.peer.supportJobs.values()].find((job) => job.connectingToParentShardCID == this.parentShardCID)
        const controller = new AbortController();
        const newJob = {
            shard: this.shardPeerInfo._shard,
            controller,
            connectingToParentShardCID: connectToParentShard ? this.parentShardCID : undefined
        }

        this.peer.supportJobs.set(this.shardPeerInfo._shard.cid, newJob);

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
        cron();
    }
    /* public async stopSupportPeer() {
        // ??? 
    }
 */
    static async subscribeForReplication(me: AnyPeer, trust: RegionAccessController, onReplication?: (shard: Shard<any>) => void): Promise<void> {
        await me.node.pubsub.subscribe(trust.replicationTopic, async (msg: any) => {
            try {
                let shard = deserialize(Buffer.from(msg.data), Shard);
                if (me.supportJobs.has(shard.cid)) {
                    return; // Already replicated
                }

                // check if is trusted,

                /*    
                WE CAN NOT HAVE THIS CHECK; BECAUSE WE CAN NOT KNOW WHETHER WE HAVE LOADED THE TRUST DB FULLY (WE NEED TO WAIT TM)
                
                if (!shard.trust.isTrusted(PublicKey.from(this.orbitDB.identity))) { 
                      //if not no point replicating
                      console.log(`Can not replicate since not trusted`)
                      return;
                  }
                 */
                const trustResult = (await me.getCachedTrustOrSet(trust, shard));
                shard.trust = trustResult.trust  // this is necessary, else the shard with initialize with a new trust region
                await shard.replicate(me);
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
    }

    _requestingReplicationPromise: Promise<void>;
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

    }

    async requestNewShard(): Promise<void> {

        return this.requestReplicate(this.shardIndex + 1n)
    }

    async createShardWithIndex(shardIndex: bigint, peer: AnyPeer = this.peer): Promise<Shard<S>> {
        const shard = new Shard<S>({
            shardIndex,
            id: this.id,
            cluster: this.cluster,
            parentShardCID: this.parentShardCID,
            store: this.store.clone(this.cluster + shardIndex) as S,
            resourceRequirements: this.resourceRequirements,
            trust: this.trust,
        })
        await shard.open(peer);
        return shard;
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

        await this.open(peer);
        /*     await this.load(); */
        await this.startSupportPeer();

    }
    async load() {
        if (!this.trust.store.initialized) { // Since the trust is shared between shards, we dont want to reinitialize already loaded trust
            await this.trust.load();
        }
        await this.store.load();
    }

    getDBName(name: string): string {
        return (this.parentShardCID ? this.parentShardCID : '') + '-' + this.id + '-' + this.shardIndex + "-" + name;
    }

    async save(node: IPFSInstance): Promise<string> {

        let arr = serialize(this);
        let addResult = await node.add(arr)
        let pinResult = await node.pin.add(addResult.cid)
        this.cid = pinResult.toString();
        return this.cid;
    }


    static async loadFromCID<T extends Store<any>>(cid: string, node: IPFSInstance) {
        let arr = await node.cat(cid);
        for await (const obj of arr) {
            let der = deserialize<Shard<T>>(Buffer.from(obj), Shard);
            der.cid = cid;
            return der;
        }
    }


    /* static get recursiveStoreOption() {
        return new BinaryDocumentStoreOptions<Shard<any>>({
            objectType: Shard.name,
            indexBy: 'cid'
        })

    } */
}