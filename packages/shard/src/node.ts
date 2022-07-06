
import OrbitDB from 'orbit-db';
import { TRUST_REGION_ACCESS_CONTROLLER } from './identity';
import { Shard, TypedBehaviours } from './shard';
import { v4 as uuid } from 'uuid';
import { P2PTrust } from './trust';
import { deserialize } from '@dao-xyz/borsh';
import { PublicKey } from "@dao-xyz/identity";
import { IQueryStoreOptions } from '@dao-xyz/orbit-db-query-store';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import { RecycleOptions } from '@dao-xyz/ipfs-log';

export interface IPFSInstanceExtended extends IPFSInstance {
    libp2p: any
}

export const ROOT_CHAIN_SHARD_SIZE = 100;
const PEER_HEALTH_CHECK_INTERVAL = 2000;
const EXPECTED_PING_DELAY = 10 * 1000; // expected pubsub hello ping delay (two way)


export class PeerOptions {

    behaviours: TypedBehaviours
    replicationCapacity: number;
    isServer: boolean;
    expectedPingDelay: number = EXPECTED_PING_DELAY;
    storeDirectory: string;

    constructor(options: {
        directoryId?: string;
        behaviours: TypedBehaviours;
        replicationCapacity: number;
        isServer: boolean;
    }) {
        Object.assign(this, options);
        this.replicationCapacity = options.replicationCapacity;
        this.behaviours = options.behaviours;
        if (!this.behaviours) {
            throw new Error("Expecting behaviours");
        }

        // Static behaviours
        this.behaviours.typeMap[Shard.name] = Shard;
        this.isServer = options.isServer;
        this.storeDirectory = './orbit-db-stores/' + (options.directoryId ? options.directoryId : uuid());
    }
}



export class AnyPeer {

    public node: IPFSInstanceExtended = undefined;
    public orbitDB: OrbitDB = undefined;
    public options: PeerOptions;
    public id: string;

    public supportJobs: {
        shardCID: string,
        connectingToParentShardCID?: string
        controller: AbortController
    }[] = [];

    // to know whether we should treat the peer as long lasting or temporary with web restrictions

    constructor(id?: string) {
        this.id = id;
    }

    async create(options: { orbitDB: OrbitDB, options: PeerOptions }): Promise<void> {
        this.orbitDB = options.orbitDB;
        this.options = options.options;
        this.node = options.orbitDB._ipfs;
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
            await this.node.stop();
            /*            
             */
        } catch (error) {

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
