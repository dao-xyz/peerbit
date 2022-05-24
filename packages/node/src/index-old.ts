
import OrbitDB from 'orbit-db';
import DocumentStore from 'orbit-db-docstore';
import FeedStore from 'orbit-db-feedstore';
import { Identity } from 'orbit-db-identity-provider';
import KeyValueStore from 'orbit-db-kvstore';
import Store from 'orbit-db-store';
import { TrustResolver } from './trust';
import io from 'orbit-db-io';
import { CONTRACT_ACCESS_CONTROLLER } from './acl';
import CounterStore from 'orbit-db-counterstore';
import { Shard } from './shard';
import * as IPFS from 'ipfs';
import { IPFS as IPFSInstance } from 'ipfs-core-types'
import { serialize } from '@dao-xyz/borsh';

interface Post {
  content: string;
}

interface Block {
  index: number,

  // Post DB
  posts: string

}
interface Root {
  blocks: string
}


export interface IPFSInstanceExtended extends IPFSInstance {
  libp2p: any
}


export class Blobby {
  public node: IPFSInstanceExtended = undefined;
  public orbitDB: OrbitDB = undefined;
  public defaultOptions: any = undefined;
  public explorer: KeyValueStore<Root> = undefined;
  public blocks: FeedStore<Block> = undefined;
  public posts: FeedStore<Post> = undefined;
  /*   public pieces: DocumentStore<any> = undefined; */
  public user: KeyValueStore<any> = undefined;
  public IPFS: typeof IPFS = undefined;

  public latestMessages: Map<string, any> = new Map() // by topic

  constructor() {
    this.IPFS = IPFS;
  }
  async create(options: { rootDB?: string, local: boolean, repo: string, identity?: Identity, trustProvider?: TrustResolver } = {
    local: false, repo: './ipfs', rootDB: undefined
  }): Promise<void> {

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
            '/dns4/secure-beyond-12878.herokuapp.com/tcp/443/wss/p2p-webrtc-star/'
            /*            
             `/ip4/0.0.0.0/tcp/0`,
                        `/ip4/127.0.0.1/tcp/0/ws` */
            /*  '/dns4/wrtc-star1.par.dwebops.pub/tcp/443/wss/p2p-webrtc-star/',
             '/dns4/wrtc-star2.sjc.dwebops.pub/tcp/443/wss/p2p-webrtc-star/',
             '/dns4/webrtc-star.discovery.libp2p.io/tcp/443/wss/p2p-webrtc-star/', */
          ],
          /*  API: `/ip4/127.0.0.1/tcp/0`,
           Gateway: `/ip4/127.0.0.1/tcp/0`,
           RPC: `/ip4/127.0.0.1/tcp/0` */
        }
      }
    }

    this.node = await IPFS.create(ipfsOptions)
    await this._init({
      identity: options.identity,
      rootDB: options.rootDB,
      trustProvider: options.trustProvider
    });

  }


  async _init(options: { identity?: Identity, trustProvider?: TrustResolver, rootDB?: string } = {}): Promise<void> {
    const peerInfo = await this.node.id()
    this.orbitDB = await OrbitDB.createInstance(this.node,
      {
        identity: options.identity,
      })
    this.defaultOptions = {
      accessController: {
        write: [this.orbitDB.identity.id],
        //type: CONTRACT_ACCESS_CONTROLLER,
        trustProvider: options.trustProvider
      },

    }

    const docStoreOptions = {
      ...this.defaultOptions,
      indexBy: 'hash',
    }
    /*  this.pieces = await this.orbitDB.docstore('pieces', docStoreOptions)
     await this.pieces.load();
 
     */

    this.user = await this.orbitDB.kvstore('user', this.defaultOptions)
    await this.user.load()


    this.posts = await this.orbitDB.feed<Post>('posts', docStoreOptions);
    await this.posts.load()

    await this.loadFixtureData({
      'username': Math.floor(Math.random() * 1000000),
      'posts': this.posts.id,
      'nodeId': peerInfo.id
    })

    this.node.libp2p.connectionManager.on('peer:connect', this.handlePeerConnected.bind(this))
    await this.node.pubsub.subscribe(peerInfo.id, (msg: any) => {
      this.latestMessages.set(peerInfo.id, msg);
      this.handleMessageReceived(msg)
    }) // this.handleMessageReceived.bind(this)

    if (this["onready"]) (this as any).onready();
  }



  async disconnect(): Promise<void> {
    await this.orbitDB.disconnect();
    await this.node.stop();

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

  async addNewPost(post: Post): Promise<string> {
    // const existingPiece = this.getPieceByHash(hash)
    // if (existingPiece)
    {
      // await this.updatePieceByHash(hash, instrument)
      //return
    }
    const cid = await this.posts.add(post)
    return cid
  }
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

  async loadFixtureData(fixtureData) {
    const fixtureKeys = Object.keys(fixtureData)
    for (let i in fixtureKeys) {
      let key = fixtureKeys[i]
      if (!this.user.get(key)) await this.user.set(key, fixtureData[key])
    }
  }

  getAllProfileFields() {
    return this.user.all;
  }

  handlePeerConnected(ipfsPeer) {
    const ipfsId = ipfsPeer.id
    if (this["onpeerconnect"]) (this as any).onpeerconnect(ipfsId)
  }

  async sendMessage(topic, message) {
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
