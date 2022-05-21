
import OrbitDB from 'orbit-db';
import { Identity } from 'orbit-db-identity-provider';
import { TrustResolver } from './trust';
import { CONTRACT_ACCESS_CONTROLLER } from './acl';
import { Peer, ReplicationRequest, Shard, ShardChain, SHARD_CHAIN_ID_FIELD, SHARD_NAME_FIELD, StoreBuilder, TypedBehaviours } from './shard';
import * as IPFS from 'ipfs';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import { Constructor, deserialize, serialize } from '@dao-xyz/borsh';
import BN from 'bn.js'
import Store from 'orbit-db-store';
import FeedStore from 'orbit-db-feedstore';
import DocumentStore from 'orbit-db-docstore';
import { BinaryDocumentStore, BINARY_DOCUMENT_STORE_TYPE } from '@dao-xyz/orbit-db-bdocstore';
import { BinaryKeyValueStore, BINARY_KEYVALUE_STORE_TYPE } from '@dao-xyz/orbit-db-bkvstore';
import { BinaryDocumentStoreOptions, StoreOptions } from './stores';
import { EncodedQueryResponse, QueryRequestV0, QueryResponse } from './query';
import { Message } from 'ipfs-core-types/types/src/pubsub'




interface IPFSInstanceExtended extends IPFSInstance {
    libp2p: any
}

export const ROOT_CHAIN_SHARD_SIZE = 100;

export class ShardedDB {
    public node: IPFSInstanceExtended = undefined;
    public orbitDB: OrbitDB = undefined;
    public defaultOptions: ICreateOptions = undefined;
    /*   public pieces: DocumentStore<any> = undefined; */
    public IPFS: typeof IPFS = undefined;

    public latestMessages: Map<string, any> = new Map() // by topic
    public rootAddress: string;
    public behaviours: TypedBehaviours
    public replicationCapacity: number;
    constructor() {
        this.IPFS = IPFS;
    }
    async create(options: { rootAddress: string, local: boolean, repo: string, identity?: Identity, trustProvider?: TrustResolver, behaviours: TypedBehaviours, replicationCapacity: number } = {
        local: false, repo: './ipfs', rootAddress: 'root', behaviours: {
            typeMap: {}
        },
        replicationCapacity: 0
    }): Promise<void> {
        this.replicationCapacity = options.replicationCapacity;
        this.behaviours = options.behaviours;
        if (!this.behaviours) {
            throw new Error("Expecting behaviours");
        }

        // Static behaviours
        this.behaviours.typeMap[ShardChain.name] = ShardChain;
        this.behaviours.typeMap[Peer.name] = Peer;

        // Create IPFS instance
        const ipfsOptions = options.local ? {
            preload: { enabled: false },
            repo: options.repo,
            EXPERIMENTAL: { pubsub: true },
            config: {
                Bootstrap: [],
                Addresses: { Swarm: [] }
            }
        } : {
            relay: { enabled: true, hop: { enabled: true, active: true } },
            repo: options.repo,
            EXPERIMENTAL: { pubsub: true },
            config: {
                Addresses: {
                    Swarm: [
                        `/ip4/0.0.0.0/tcp/0`,
                        `/ip4/127.0.0.1/tcp/0/ws`
                        //'/dns4/secure-beyond-12878.herokuapp.com/tcp/443/wss/p2p-webrtc-star/',
                        // '/dns4/secure-beyond-12878.herokuapp.com/tcp/80/ws/p2p-webrtc-star/'
                        /*      '/dns4/wrtc-star1.par.dwebops.pub/tcp/443/wss/p2p-webrtc-star',
                             '/dns4/wrtc-star2.sjc.dwebops.pub/tcp/443/wss/p2p-webrtc-star',
                             '/ip4/127.0.0.1/tcp/13579/wss/p2p-webrtc-star' */
                        //'/dns4/ws-star.discovery.libp2p.io/tcp/443/wss/p2p-websocket-star'
                        /*  '/dns4/secure-beyond-12878.herokuapp.com/tcp/443/wss/p2p-webrtc-star/' */
                    ]
                }
                /*      Addresses: {
                       Swarm: [ */

                /*  `/ip4/0.0.0.0/tcp/0`,
                 `/ip4/127.0.0.1/tcp/0/ws` */
                /*  '/dns4/wrtc-star1.par.dwebops.pub/tcp/443/wss/p2p-webrtc-star/',
                 '/dns4/wrtc-star2.sjc.dwebops.pub/tcp/443/wss/p2p-webrtc-star/',
                 '/dns4/webrtc-star.discovery.libp2p.io/tcp/443/wss/p2p-webrtc-star/', */
                /*   ], */
                /*  API: `/ip4/127.0.0.1/tcp/0`,
                 Gateway: `/ip4/127.0.0.1/tcp/0`,
                 RPC: `/ip4/127.0.0.1/tcp/0` */
                /*   }*/
            }
        }

        /* '/ip4/0.0.0.0/tcp/0',
     '/ip4/0.0.0.0/tcp/0/ws',
     `/ip4/127.0.0.1/tcp/15555/ws/p2p-webrtc-star/` */
        /*    
     const libp2p = await Libp2p.create({
       addresses: {
         listen: [
     
           '/dns4/secure-beyond-12878.herokuapp.com/tcp/443/wss/p2p-webrtc-star/'
         ]
       },
       modules: {
         transport: [TCP, Websockets, WebrtcStar],
         streamMuxer: [Mplex],
         connEncryption: [NOISE, Secio],
         peerDiscovery: [Bootstrap, MDNS],
         dht: KadDHT,
         pubsub: Gossipsub
       },
       config: {
         transport: {
           [WebrtcStar.prototype[Symbol.toStringTag]]: {
             wrtc
           }
         },
         peerDiscovery: {
           bootstrap: {
             list: ['/ip4/127.0.0.1/tcp/63785/ipfs/12D3KooWLzqGooZH35FaFQokh5xyc4P6HEn3aWpoXjQkieytTDdL']
           }
         },
         dht: {
           enabled: true,
           randomWalk: {
             enabled: true
           }
         }
       }
     })
     await libp2p.start();
     this.node = libp2p; */
        this.node = await IPFS.create(ipfsOptions)
        this.rootAddress = options.rootAddress;
        await this._init({
            identity: options.identity,
        });

    }


    async _init(options: { identity?: Identity } = {}): Promise<void> {
        const peerInfo = await this.node.id()
        this.orbitDB = await OrbitDB.createInstance(this.node,
            {
                identity: options.identity,
                directory: './orbit-db/' + peerInfo.id
            })
        this.defaultOptions = {
            accessController: {
                //write: [this.orbitDB.identity.id],
                type: CONTRACT_ACCESS_CONTROLLER
            } as any,
            replicate: true,
            directory: './orbit-db-stores/' + peerInfo.id
        }


        /*  this.pieces = await this.orbitDB.docstore('pieces', docStoreOptions)
         await this.pieces.load();
     
         */

        /* 
         await this.loadFixtureData({
           'username': Math.floor(Math.random() * 1000000),
           'posts': this.posts.id,
           'nodeId': peerInfo.id
         }) */

        this.node.libp2p.connectionManager.on('peer:connect', this.handlePeerConnected.bind(this))
        await this.node.pubsub.subscribe(peerInfo.id, (msg: any) => {
            this.latestMessages.set(peerInfo.id, msg);
            console.log('Got msg')
            this.handleMessageReceived(msg)
        }) // this.handleMessageReceived.bind(this)

        if (this["onready"]) (this as any).onready();
    }

    getShardingTopic(): string {
        return this.rootAddress + "-" + "sharding";
    }

    get replicationTopic() {
        return this.rootAddress + "_replication"
    }
    get shardChainChain(/* xyzAddress: string */): ShardChain<BinaryDocumentStore<ShardChain<any>>> {

        // the shard of shards
        let shardChain = new ShardChain<BinaryDocumentStore<ShardChain<any>>>({
            remoteAddress: this.rootAddress,
            name: "_genisis",
            storeOptions: new BinaryDocumentStoreOptions({
                indexBy: SHARD_CHAIN_ID_FIELD,
                objectType: ShardChain.name
            }),
            shardSize: new BN(ROOT_CHAIN_SHARD_SIZE)
        });
        shardChain.init({
            behaviours: this.behaviours,
            db: this,
            defaultOptions: this.defaultOptions
        })
        return shardChain;
    }


    _createShardChain<B extends Store>(name: string, options: StoreOptions<B>, shardSize: BN): ShardChain<B> {

        let newShardChain = new ShardChain<any>({
            name: name,
            storeOptions: options,
            shardSize,
            remoteAddress: this.rootAddress
        });

        newShardChain.init({
            behaviours: this.behaviours,
            db: this,
            defaultOptions: this.defaultOptions
        });

        return newShardChain;
    }

    async findRootShardContainingShardChain<B extends Store>(chardShain: ShardChain<B>, shardChains: ShardChain<BinaryDocumentStore<ShardChain<any>>>): Promise<ShardChain<B> | undefined> {
        let expectedId = chardShain.id;

        let counter = await shardChains.getShardCounter();
        for (let i = 0; i < counter.value; i++) { // root shard iterator
            let shard = await shardChains.getShard(i);
            await shard.loadBlocks();

            // Every root shard contains shards, check if our wanted shard is inside here
            let results = await shard.blocks.get(expectedId);
            if (results?.length == 1) {
                let chain = results[0] as any as ShardChain<B>;
                chain.init({
                    behaviours: this.behaviours,
                    db: this,
                    defaultOptions: this.defaultOptions
                });
                return chain as any; // TODO FIX
            }
            else if (results?.length > 1) {
                throw new Error("Expecting only one result but got: " + results.length);
            }
        }

    }

    // seperate create and load sharchain? because size settings?
    async loadShardChain<B extends Store>(name: string, options: StoreOptions<B>): Promise<ShardChain<B>> {

        const newShardChain = this._createShardChain(name, options, undefined);
        let shardChains = await this.shardChainChain;
        let existingChard = await this.findRootShardContainingShardChain(newShardChain, shardChains);
        return existingChard;
    }

    async createShardChain<B extends Store>(name: string, options: StoreOptions<B>, shardSize: BN = new BN(10 * 1000000)): Promise<ShardChain<B>> {

        const newShardChain = this._createShardChain(name, options, shardSize);
        let shardChains = await this.shardChainChain;
        let existingChard = await this.findRootShardContainingShardChain(newShardChain, shardChains);
        if (existingChard)
            return existingChard;

        // From all the root shards, find the wanted shard
        // Create new shard, as it is not found
        let root = await shardChains.getWritableShard();
        let bytes = serialize(newShardChain);
        await root.loadBlocks()
        await root.makeSpace(bytes.length);
        await root.blocks.put(newShardChain);
        return newShardChain;
    }




    /*  getFeedStoreShardChain<T>(shardChainName: string, type: string): ShardChain<FeedStore<T>> {
   
       let chain = new ShardChain<FeedStore<T>>({
         shardChainName: shardChainName,
         defaultOptions: this.defaultOptions,
         db: this,
         behaviours: this.behaviours,
         type: ''
         //storeBuilder: (a, b, c) => c.feed(a, b)
       });
       return chain;
     } */


    async subscribeForReplication(): Promise<void> {
        await this.node.pubsub.subscribe(this.replicationTopic, async (msg: any) => {
            try {
                let request = deserialize(msg.data, ReplicationRequest);
                if (request.shardSize.toNumber() > this.replicationCapacity) {
                    console.log(`Can not replicate shard size ${request.shardSize.toNumber()} with peer capacity ${this.replicationCapacity}`)
                    return;
                }
                this.replicationCapacity -= request.shardSize.toNumber();

                // we create a dummy shard chain just to be able to create a new Shard
                let chain = new ShardChain<any>({
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

            } catch (error) {
                console.error('Invalid replication request');
            }
        }) // this.handleMessageReceived.bind(this)
    }

    async query<T>(topic: string, query: QueryRequestV0, clazz: Constructor<T>, responseHandler: (response: QueryResponse<T>) => void, maxAggregationTime: number = 30 * 1000) {
        // send query and wait for replies in a generator like behaviour
        let responseTopic = query.getResponseTopic(topic);
        await this.node.pubsub.subscribe(responseTopic, (msg: Message) => {
            const encoded = deserialize(Buffer.from(msg.data), EncodedQueryResponse);
            let result = QueryResponse.from(encoded, clazz);
            responseHandler(result);
        })
        await this.node.pubsub.publish(topic, serialize(query));

        // Unsubscrice after a while
        setTimeout(() => {
            this.node.pubsub.unsubscribe(responseTopic);
        }, maxAggregationTime);
    }



    async disconnect(): Promise<void> {
        await this.orbitDB.disconnect();
        await this.node.stop({
            timeout: 0
        });
        const t = 123;

    }
    // async addNewPiece(hash, instrument = 'Piano') {
    // const existingPiece = this.getPieceByHash(hash)
    // if (existingPiece)
    // {
    // await this.updatePieceByHash(hash, instrument)
    //return
    /*  }
     const cid = await this.pieces.put({ hash, instrument })
     return cid
   } */

    /*  async addNewPost(post: Post): Promise<string> {
       // const existingPiece = this.getPieceByHash(hash)
       // if (existingPiece)
       {
         // await this.updatePieceByHash(hash, instrument)
         //return
       }
       const cid = await this.posts.add(post)
       return cid
     } */
    /* getAllPieces() {
      const pieces = this.pieces.get('')
      return pieces
    }
  
    getPieceByInstrument(instrument) {
      return this.pieces.query((piece) => piece["instrument"] === instrument)
    } */

    async getIpfsPeers() {
        const peers = await this.node.swarm.peers()
        return peers
    }

    async connectToPeer(multiaddr, protocol = '/p2p-circuit/ipfs/') {
        try {
            await this.node.swarm.connect(protocol + multiaddr)
        } catch (e) {
            throw (e)
        }
    }

    /* async loadFixtureData(fixtureData) {
      const fixtureKeys = Object.keys(fixtureData)
      for (let i in fixtureKeys) {
        let key = fixtureKeys[i]
        if (!this.user.get(key)) await this.user.set(key, fixtureData[key])
      }
    } */

    /* getAllProfileFields() {
      return this.user.all;
    } */

    handlePeerConnected(ipfsPeer) {
        const ipfsId = ipfsPeer.id
        if (this["onpeerconnect"]) (this as any).onpeerconnect(ipfsId)
    }

    async sendMessage(topic: string, message: any) {
        try {
            const msgString = JSON.stringify(message)
            const messageBuffer = Buffer.from(msgString)
            await this.node.pubsub.publish(topic, messageBuffer)
        } catch (e) {
            throw (e)
        }
    }

    handleMessageReceived(msg: any) {
        // 
    }
}


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
