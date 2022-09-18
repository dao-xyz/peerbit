import { field, variant } from "@dao-xyz/borsh";
import { Shard } from "./shard";
import { OrbitDB } from '@dao-xyz/orbit-db';
import { v4 as uuid } from 'uuid';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import { RegionAccessController } from "@dao-xyz/orbit-db-trust-web";
import isNode from 'is-node';

let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}

export const EMIT_HEALTHCHECK_INTERVAL = 5000;
export const ROOT_CHAIN_SHARD_SIZE = 100;
const EXPECTED_PING_DELAY = 10 * 1000; // expected pubsub hello ping delay (two way)



export class PeerOptions {

    heapSizeLimit: number;
    // todo add disc size limit

    isServer: boolean;
    expectedPingDelay: number = EXPECTED_PING_DELAY;
    storeDirectory: string;

    constructor(options: {
        directoryId?: string;
        heapSizeLimit: number;
        isServer: boolean
    }) {
        Object.assign(this, options);
        this.heapSizeLimit = options.heapSizeLimit;


        // Static behaviours
        //this.behaviours.typeMap[Shard.name] = Shard;
        this.isServer = options.isServer;
        this.storeDirectory = './orbit-db-stores/' + (options.directoryId ? options.directoryId : uuid());
    }
}



export class AnyPeer {

    public orbitDB: OrbitDB = undefined;
    public options: PeerOptions;
    public id: string;

    public supportJobs: Map<string, {
        shard: Shard<any>,
        connectingToParentShardCID?: string
        controller: AbortController
    }> = new Map();


    // trust regions that are currently replicated by the peer
    public trustWebs: Map<string, { trust: RegionAccessController, shards: Map<string, Shard<any>> }> = new Map(); // key is the hash of P2PTrust

    // to know whether we should treat the peer as long lasting or temporary with web restrictions

    constructor(id?: string) {
        this.id = id;
    }

    async create(options: { orbitDB: OrbitDB, options: PeerOptions }): Promise<void> {
        this.orbitDB = options.orbitDB;
        this.options = options.options;

    }
    get node(): IPFSInstance {
        return this.orbitDB._ipfs;
    }

    _getCachedTrustOrSetPromise: Promise<{ trust: RegionAccessController, afterShardSave?: () => void }> = undefined;
    async getCachedTrustOrSet(from: RegionAccessController, shard: Shard<any>): Promise<{ trust: RegionAccessController, afterShardSave?: () => void }> {
        await this._getCachedTrustOrSetPromise; // prevent concurrency sideffects
        this._getCachedTrustOrSetPromise = new Promise(async (resolve, reject) => {
            const hashCode = from.hashCode();
            let value = this.trustWebs.get(hashCode);
            if (!value) {
                value = {
                    shards: new Map(),
                    trust: from
                }
            }
            else {
                if (from !== value.trust) { // Only close trust if different (instance wise)
                    await from.close();
                }
            }
            let afterShardSave = undefined;
            if (!shard.address) {
                afterShardSave = () => value.shards.set(shard.address.toString(), shard);
            }
            else {
                value.shards.set(shard.address.toString(), shard);
            }

            this.trustWebs.set(hashCode, value)
            resolve({
                trust: value.trust,
                afterShardSave
            })
        })
        return this._getCachedTrustOrSetPromise;

    }

    _removeAndCloseCachedTrust: Promise<void> = undefined;
    async removeAndCloseCachedTrust(from: RegionAccessController, shard: Shard<any>): Promise<void> {
        await this._getCachedTrustOrSetPromise; // prevent concurrency sideffects 
        this._removeAndCloseCachedTrust = new Promise(async (resolve, reject) => {
            const hashCode = from.hashCode();
            let value = this.trustWebs.get(hashCode);
            if (!value)
                return;
            value.shards.delete(shard.address.toString())
            if (value.shards.size === 0) {
                await value.trust.close();
                this.trustWebs.delete(hashCode);
            }
            resolve();
        });
        await this._getCachedTrustOrSetPromise;
    }



    async disconnect(): Promise<void> {
        try {

            for (const jobs of this.supportJobs.values()) {
                jobs.controller.abort();
            }
            await this.orbitDB.disconnect();


        } catch (error) {

        }
    }
}

/*   await this.orbitDB.disconnect(); */
/*  let p = (await this.node.pubsub.ls()).map(topic => this.node.pubsub.unsubscribe(topic))
 await Promise.all(p); */


@variant("check")
export class PeerCheck {

    @field({ type: 'string' })
    responseTopic: string

    constructor(obj?: { responseTopic: string }) {
        if (obj) {
            Object.assign(this, obj);
        }

    }

}

// we create a dummy shard chain just to be able to create a new Shard
/* let chain = new ShardChain<any>({
    name: request.shardChainName,
    remoteAddress: this.rootAddress,
    storeOptions: request.storeOptions,
    shardSize: request.shardSize // Does not have any effect

});


chain.init({
    defaultOptions: this.defaultOptions,
    db: this,
    behaviours: this.behaviours,
})

let shardToReplicate = new Shard({
    index: request.index,
    chain,
    defaultOptions: this.defaultOptions
});

await shardToReplicate.replicate({
    capacity: request.shardSize
});
 */
// this.handleMessageReceived.bind(this)
/*  
    this.node.libp2p.connectionManager.on('peer:connect', this.handlePeerConnected.bind(this))
    
    await this.node.pubsub.subscribe(peerInfo.id, (msg: any) => {
            this.latestMessages.set(peerInfo.id, msg);
            console.log('Got msg')
            this.handleMessageReceived(msg)
        }) // this.handleMessageReceived.bind(this)
    */

/*  handlePeerConnected(ipfsPeer) {
     const ipfsId = ipfsPeer.id
     if (this["onpeerconnect"]) (this as any).onpeerconnect(ipfsId)
 }
*/
/*  async sendMessage(topic: string, message: any) {
     try {
         const msgString = JSON.stringify(message)
         const messageBuffer = Buffer.from(msgString)
         await this.node.pubsub.publish(topic, messageBuffer)
     } catch (e) {
         throw (e)
     }
 } */

/* const Libp2p = require('libp2p')
const TCP = require('libp2p-tcp')
const Websockets = require('libp2p-websockets')
const WebrtcStar = require('libp2p-webrtc-star')
const wrtc = require('wrtc')
const Mplex = require('libp2p-mplex')
const { NOISE } = require('libp2p-noise')
const Secio = require('libp2p-secio')
const Bootstrap = require('libp2p-bootstrap')
const MDNS = require('libp2p-mdns')
const KadDHT = require('libp2p-kad-dht')
const Gossipsub = require('libp2p-gossipsub') */
