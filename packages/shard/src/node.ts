
import OrbitDB from 'orbit-db';
import { CONTRACT_ACCESS_CONTROLLER } from './identity';
import { Shard, TypedBehaviours } from './shard';
import { Peer } from './peer';
import { v4 as uuid } from 'uuid';
import { P2PTrust } from './trust';
import { deserialize } from '@dao-xyz/borsh';
import { PublicKey } from './key';
import { IQueryStoreOptions } from '@dao-xyz/orbit-db-bstores';
import { IPFS as IPFSInstance } from 'ipfs-core-types'

export interface IPFSInstanceExtended extends IPFSInstance {
    libp2p: any
}

export const ROOT_CHAIN_SHARD_SIZE = 100;


export class PeerOptions {

    defaultOptions: IQueryStoreOptions;
    behaviours: TypedBehaviours
    replicationCapacity: number;
    isServer: boolean;

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
        this.behaviours.typeMap[Peer.name] = Peer;
        this.isServer = options.isServer;

        this.defaultOptions = {
            subscribeToQueries: this.isServer,
            accessController: {
                //write: [this.orbitDB.identity.id],
                type: CONTRACT_ACCESS_CONTROLLER
            } as any,
            replicate: this.isServer,
            directory: './orbit-db-stores/' + (options.directoryId ? options.directoryId : uuid())
        }

    }


}



export class AnyPeer {

    public rootAddress: string;

    public node: IPFSInstanceExtended = undefined;

    public orbitDB: OrbitDB = undefined;

    public options: PeerOptions;

    public id: string;

    // to know whether we should treat the peer as long lasting or temporary with web restrictions

    constructor(id?: string) {
        this.id = id;
    }

    async create(options: { orbitDB: OrbitDB, options: PeerOptions }): Promise<void> {
        this.orbitDB = options.orbitDB;
        this.options = options.options;
        this.node = options.orbitDB._ipfs;

        /*  if (this["onready"]) (this as any).onready(); */
    }

    async subscribeForReplication(trust: P2PTrust): Promise<void> {
        await this.node.pubsub.subscribe(trust.replicationTopic, async (msg: any) => {
            try {
                let shard = deserialize(Buffer.from(msg.data), Shard);

                // check if enough memory 
                if (shard.shardSize.toNumber() > this.options.replicationCapacity) {
                    console.log(`Can not replicate shard size ${shard.shardSize.toNumber()} with peer capacity ${this.options.replicationCapacity}`)
                    return;
                }
                await shard.init(this);
                // check if is trusted,

                /*    
                WE CAN NOT HAVE THIS CHECK; BECAUSE WE CAN NOT KNOW WHETHER WE HAVE LOADED THE TRUST DB FULLY (WE NEED TO WAIT TM)
                
                if (!shard.trust.isTrusted(PublicKey.from(this.orbitDB.identity))) { 
                      //if not no point replicating
                      console.log(`Can not replicate since not trusted`)
                      return;
                  }
   */
                this.options.replicationCapacity -= shard.shardSize.toNumber();
                await shard.replicate();

            } catch (error) {
                console.error('Invalid replication request', error.toString());
                throw error;
            }
        })
    }

    /*   
    async query<T>(topic: string, query: QueryRequestV0, clazz: Constructor<T>, responseHandler: (response: QueryResponse<T>) => void, maxAggregationTime: number = 30 * 1000) {
        // send query and wait for replies in a generator like behaviour
        let responseTopic = query.getResponseTopic(topic);
        await this.node.pubsub.subscribe(responseTopic, (msg: Message) => {
            const encoded = deserialize(Buffer.from(msg.data), EncodedQueryResponse);
            let result = QueryResponse.from(encoded, clazz);
            responseHandler(result);
        })
        await this.node.pubsub.publish(topic, serialize(query));
    } 
  
    
  
  */

    async disconnect(): Promise<void> {
        try {
            /*   await this.orbitDB.disconnect(); */
            /*  let p = (await this.node.pubsub.ls()).map(topic => this.node.pubsub.unsubscribe(topic))
             await Promise.all(p); */
            await this.orbitDB.disconnect();
            await this.node.stop();
            /*            
             */
        } catch (error) {

        }
        /*  
        /*  try {
             let p = (await this.node.pubsub.ls()).map(topic => this.node.pubsub.unsubscribe(topic))
             await Promise.all(p);
         } catch (error) {
 
         } */
        /*    */
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
