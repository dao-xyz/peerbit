import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { Shard } from "./shard";
import { Message } from 'ipfs-core-types/types/src/pubsub'
import { delay } from "@dao-xyz/time";
import { createHash } from "crypto";
import { IdentitySerializable } from "@dao-xyz/orbit-db-identity-provider";

export const EMIT_HEALTHCHECK_INTERVAL = 5000;

import { OrbitDB } from '@dao-xyz/orbit-db';
import { v4 as uuid } from 'uuid';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import { P2PTrust } from "@dao-xyz/orbit-db-trust-web";
import isNode from 'is-node';
let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}
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

    public supportJobs: {
        shard: Shard<any>,
        connectingToParentShardCID?: string
        controller: AbortController
    }[] = [];

    // trust regions that are currently replicated by the peer
    public trustWebs: Map<string, { trust: P2PTrust, shards: Shard<any>[] }> = new Map(); // key is the hash of P2PTrust

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

    _getCachedTrustOrSetPromise: Promise<P2PTrust> = undefined;
    async getCachedTrustOrSet(from: P2PTrust, shard: Shard<any>): Promise<P2PTrust> {
        await this._getCachedTrustOrSetPromise; // prevent concurrency sideffects
        this._getCachedTrustOrSetPromise = new Promise(async (resolve, reject) => {
            const hashCode = from.hashCode();
            let value = this.trustWebs.get(hashCode);
            if (!value) {
                value = {
                    shards: [shard],
                    trust: from
                }
            }
            else {
                if (from != value.trust) { // Only close trust if different (instance wise)
                    await from.close();
                }
                value.shards.push(shard)
            }
            this.trustWebs.set(hashCode, value)
            resolve(value.trust)
        })
        return this._getCachedTrustOrSetPromise;

    }

    _removeAndCloseCachedTrust: Promise<void> = undefined;
    async removeAndCloseCachedTrust(from: P2PTrust, shard: Shard<any>): Promise<void> {
        await this._getCachedTrustOrSetPromise; // prevent concurrency sideffects 
        this._removeAndCloseCachedTrust = new Promise(async (resolve, reject) => {
            const hashCode = from.hashCode();
            let value = this.trustWebs.get(hashCode);
            if (!value)
                return;
            value.shards = value.shards.filter(s => s !== shard);
            if (value.shards.length == 0) {
                await value.trust.close();
                this.trustWebs.delete(hashCode);
            }
            resolve();
        });
        await this._getCachedTrustOrSetPromise;
    }



    async disconnect(): Promise<void> {
        try {
            /*   await this.orbitDB.disconnect(); */
            /*  let p = (await this.node.pubsub.ls()).map(topic => this.node.pubsub.unsubscribe(topic))
             await Promise.all(p); */
            for (const jobs of this.supportJobs) {
                jobs.controller.abort();
            }
            await this.orbitDB.disconnect();

            /*            
             */
        } catch (error) {

        }
    }
}



@variant("check")
export class PeerCheck {

    @field({ type: 'String' })
    responseTopic: string

    constructor(obj?: { responseTopic: string }) {
        if (obj) {
            Object.assign(this, obj);
        }

    }

}

@variant("info")
export class PeerInfo {

    @field({ type: IdentitySerializable })
    key: IdentitySerializable

    @field({ type: vec('String') })
    addresses: string[] // address

    @field({
        serialize: (value, writer) => {
            writer.writeU64(value);
        },
        deserialize: (reader) => {
            return reader.readU64().toNumber();
        }
    })
    memoryLeft: number

    constructor(obj?: {
        key: IdentitySerializable,
        addresses: string[],
        memoryLeft: number
    }) {
        if (obj) {
            Object.assign(this, obj);
        }
    }

}

export class ShardPeerInfo {
    _shard: Shard<any>
    constructor(shard: Shard<any>) {
        this._shard = shard;
    }


    async getShardPeerInfo(): Promise<PeerInfo> {

        return new PeerInfo({
            key: this._shard.peer.orbitDB.identity.toSerializable(),
            addresses: (await this._shard.peer.node.id()).addresses.map(x => x.toString()),
            memoryLeft: v8.getHeapStatistics().total_available_size//v8
        })
    }

    /**
     * 
     * Start to "support" the shard
     * by responding to peer healthcheck requests
     */
    async emitHealthcheck(): Promise<void> {
        if (this._shard.peer.options.isServer) {
            this._shard.peer.node.pubsub.publish(this.peerHealthTopic, serialize(await this.getShardPeerInfo()))
        }
    }
    get peerHealthTopic(): string {
        return this._shard.getQueryTopic('health')
    }


    async getPeers(): Promise<PeerInfo[]> {
        let peers: Map<string, PeerInfo> = new Map();
        const ids = new Set();
        await this._shard.peer.node.pubsub.subscribe(this.peerHealthTopic, (message: Message) => {
            const p = deserialize(Buffer.from(message.data), PeerInfo);
            peers.set(p.key.toString(), p); // TODO check verify responses are valid
        })

        await delay(EMIT_HEALTHCHECK_INTERVAL + 3000); // add some extra padding to make sure all nodes have emitted
        return [...peers.values()]
    }

    /**
     * An intentionally imperfect leader rotation routine
     * @param slot, some time measure
     * @returns 
     */
    async isLeader(slot: number): Promise<boolean> {
        // Hash the time, and find the closest peer id to this hash
        const h = (h: string) => createHash('sha1').update(h).digest('hex');
        const slotHash = h(slot.toString())


        const hashToPeer: Map<string, PeerInfo> = new Map();
        const peers: PeerInfo[] = [...await this.getPeers()];
        if (peers.length == 0) {
            return false;
        }

        const peerHashed: string[] = [];
        peers.forEach((peer) => {
            const peerHash = h(peer.key.toString());
            hashToPeer.set(peerHash, peer);
            peerHashed.push(peerHash);
        })
        peerHashed.push(slotHash);
        // TODO make more efficient
        peerHashed.sort((a, b) => a.localeCompare(b)) // sort is needed, since "getPeers" order is not deterministic
        let slotIndex = peerHashed.findIndex(x => x === slotHash);
        // we only step forward 1 step (ignoring that step backward 1 could be 'closer')
        // This does not matter, we only have to make sure all nodes running the code comes to somewhat the 
        // same conclusion (are running the same leader selection algorithm)
        let nextIndex = slotIndex + 1;
        if (nextIndex >= peerHashed.length)
            nextIndex = 0;
        return hashToPeer.get(peerHashed[nextIndex]).key.id === this._shard.peer.orbitDB.identity.id

        // better alg, 
        // convert slot into hash, find most "probable peer" 
    }

    close(): Promise<void> {
        return this._shard.peer.node.pubsub.unsubscribe(this._shard.getQueryTopic('peer'))
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
