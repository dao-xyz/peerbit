import path from 'path'
import { Address, IStoreOptions, Store, StoreLike } from '@dao-xyz/orbit-db-store'
// @ts-ignore
import Logger from 'logplease'
import { IPFS, IPFS as IPFSInstance } from 'ipfs-core-types';
import Cache from '@dao-xyz/orbit-db-cache'
import { Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { isDefined } from './is-defined.js'
import { Level } from 'level';
import { exchangeHeads, ExchangeHeadsMessage } from './exchange-heads.js'
import { Entry, Identity, toBase64 } from '@dao-xyz/ipfs-log'
import { serialize, deserialize } from '@dao-xyz/borsh'
import { ProtocolMessage } from './message.js'
import type { Message as PubSubMessage, SignedMessage as SignedPubSubMessage } from '@libp2p/interface-pubsub';
import { SharedChannel, SharedIPFSChannel } from './channel.js'
import { exchangeKeys, KeyResponseMessage, KeyAccessCondition, recieveKeys, requestAndWaitForKeys, RequestKeyMessage, RequestKeyCondition, RequestKeysByKey, RequestKeysByReplicationTopic } from './exchange-keys.js'
import { DecryptedThing, Ed25519Keypair, EncryptedThing, MaybeEncrypted, PublicKeyEncryptionResolver, SignKey, X25519Keypair } from "@dao-xyz/peerbit-crypto"
import { X25519PublicKey, IPFSAddress } from '@dao-xyz/peerbit-crypto'
import LRU from 'lru-cache';
import { DirectChannel } from '@dao-xyz/ipfs-pubsub-direct-channel'
import { encryptionWithRequestKey } from './encryption.js'
import { MaybeSigned } from '@dao-xyz/peerbit-crypto';
import { WAIT_FOR_PEERS_TIME, exchangePeerInfo, ReplicatorInfo, PeerInfoWithMeta, RequestReplicatorInfo, requestPeerInfo } from './exchange-replication.js'
import { createHash } from 'crypto'
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';
import { multiaddr } from '@multiformats/multiaddr'

// @ts-ignore
import { delay, waitFor } from '@dao-xyz/time'
import { LRUCounter } from './lru-counter.js'
import { IpfsPubsubPeerMonitor } from '@dao-xyz/ipfs-pubsub-peer-monitor';
import type { PeerId } from '@libp2p/interface-peer-id';
import { EntryWithRefs } from '@dao-xyz/orbit-db-store';
import { exchangeSwarmAddresses, ExchangeSwarmMessage } from './exchange-network.js';
/* let v8 = undefined;
if (isNode) {
  v8 = require('v8');
}
 */
/* let AccessControllersModule = AccessControllers;
 */
const logger = Logger.create('orbit-db')
Logger.setLogLevel('ERROR')

const defaultTimeout = 30000 // 30 seconds
const STORE_MIN_HEAP_SIZE = 50 * 1000;

const MIN_REPLICAS = 2;

export type StoreOperations = 'write' | 'all'
export type Storage = { createStore: (string: string) => Level }
export type OptionalCreateOptions = { minReplicas?: number, waitForKeysTimout?: number }
export type CreateOptions = { keystore: Keystore, identity: Identity, directory: string, peerId: PeerId, storage: Storage, cache: Cache<any>, localNetwork: boolean } & OptionalCreateOptions;
export type CreateInstanceOptions = { storage?: Storage, directory?: string, keystore?: Keystore, peerId?: PeerId, identity?: Identity, cache?: Cache<any>, localNetwork?: boolean } & OptionalCreateOptions;
export type OpenStoreOptions = {
  identity?: Identity,
  directory?: string,
  timeout?: number,
  rejectIfAlreadyOpen?: boolean
} & IStoreOptions<any>;
const groupByGid = <T extends (Entry<any> | EntryWithRefs<any>)>(entries: T[]) => {
  const groupByGid: Map<string, T[]> = new Map()
  for (const head of entries) {
    const gid = head instanceof Entry ? head.gid : head.entry.gid;
    let value = groupByGid.get(gid);
    if (!value) {
      value = []
      groupByGid.set(gid, value)
    }
    value.push(head);
  }
  return groupByGid;
}

export class OrbitDB {

  _ipfs: IPFSInstance;
  /* 
    _pubsub: PubSub; */
  _directConnections: Map<string, SharedChannel<DirectChannel>>;
  _replicationTopicSubscriptions: Map<string, SharedChannel<SharedIPFSChannel>>;

  identity: Identity;
  id: PeerId;
  directory: string;
  storage: Storage;
  caches: { [key: string]: { cache: Cache<any>, handlers: Set<string> } };
  keystore: Keystore;
  minReplicas: number;
  stores: { [topic: string]: { [address: string]: StoreLike<any> } };
  localNetwork: boolean;
  _trustedNetwork: Map<string, TrustedNetwork>
  /*  heapsizeLimitForForks: number = 1000 * 1000 * 1000; */

  _gidPeersHistory: Map<string, Set<string>> = new Map()
  _waitForKeysTimeout = 10000;
  _keysInflightMap: Map<string, Promise<any>> = new Map(); // TODO fix types
  _keyRequestsLRU: LRU<string, KeyWithMeta<Ed25519Keypair | X25519Keypair>[] | null> = new LRU({ max: 100, ttl: 10000 });
  _peerInfoLRU: Map<string, PeerInfoWithMeta> = new Map();// LRU = new LRU({ max: 1000, ttl:  EMIT_HEALTHCHECK_INTERVAL * 4 });
  _supportedHashesLRU: LRUCounter = new LRUCounter(new LRU({ ttl: 60000 }))
  _peerInfoResponseCounter: LRUCounter = new LRUCounter(new LRU({ ttl: 100000 }))

  //_peerInfoMap: Map<string, Map<string, Set<string>>> // peer -> store -> heads
  /*   _replicationTopicJobs: Map<string, { controller: AbortController }> = new Map(); */


  /*   canAccessKeys: KeyAccessCondition
   */

  constructor(ipfs: IPFSInstance, identity: Identity, options: CreateOptions) {
    if (!isDefined(ipfs)) { throw new Error('IPFS required') }
    if (!isDefined(identity)) { throw new Error('identity key required') }

    this._ipfs = ipfs
    this.identity = identity
    this.id = options.peerId

    this.directory = options.directory || './orbitdb'
    this.storage = options.storage
    this._directConnections = new Map();
    this._trustedNetwork = new Map();
    this.stores = {}
    this.caches = {}
    this.minReplicas = options.minReplicas || MIN_REPLICAS;
    this.localNetwork = options.localNetwork;
    this.caches[this.directory] = { cache: options.cache, handlers: new Set() }
    this.keystore = options.keystore
    if (options.waitForKeysTimout) {
      this._waitForKeysTimeout = options.waitForKeysTimout;
    }
    /* this.heapsizeLimitForForks = options.heapsizeLimitForForks; */
    this._ipfs.pubsub.subscribe(DirectChannel.getTopic([this.id.toString()]), this._onMessage.bind(this));

    // AccessControllers module can be passed in to enable
    // testing with orbit-db-access-controller
    /*     AccessControllersModule = options.AccessControllers || AccessControllers
     */
    this._replicationTopicSubscriptions = new Map();
  }

  get cache() { return this.caches[this.directory].cache }

  get encryption(): PublicKeyEncryptionResolver {
    return encryptionWithRequestKey(this.identity, this.keystore)
  }

  async requestAndWaitForKeys<T extends (Ed25519Keypair | X25519Keypair)>(replicationTopic: string, condition: RequestKeyCondition<T>): Promise<KeyWithMeta<T>[] | undefined> {
    const promiseKey = condition.hashcode;
    const existingPromise = this._keysInflightMap.get(promiseKey);
    if (existingPromise) {
      return existingPromise
    }

    let lruCache = this._keyRequestsLRU.get(promiseKey);
    if (lruCache !== undefined) {
      return lruCache as KeyWithMeta<T>[];
    }

    const promise = new Promise<KeyWithMeta<T>[] | undefined>((resolve, reject) => {
      const send = (message: Uint8Array) => this._ipfs.pubsub.publish(replicationTopic, message)
      requestAndWaitForKeys(condition, send, this.keystore, this.identity, this._waitForKeysTimeout).then((results) => {
        if (results && results?.length > 0) {
          resolve(results);
        }
        else {
          resolve(undefined);
        }
      }).catch((error) => {
        reject(error);
      })
    })
    this._keysInflightMap.set(promiseKey, promise);
    const result = await promise;
    this._keyRequestsLRU.set(promiseKey, result ? result : null);
    this._keysInflightMap.delete(promiseKey);
    return result;
  }

  async decryptedSignedThing(data: Uint8Array): Promise<DecryptedThing<MaybeSigned<Uint8Array>>> {
    const signedMessage = await (new MaybeSigned({ data })).sign(await this.getSigner());
    return new DecryptedThing({
      data: serialize(signedMessage)
    })
  }

  async enryptedSignedThing(data: Uint8Array, reciever: X25519PublicKey): Promise<EncryptedThing<MaybeSigned<Uint8Array>>> {
    const signedMessage = await (new MaybeSigned({ data })).sign(await this.getSigner());
    return new DecryptedThing<MaybeSigned<Uint8Array>>({
      data: serialize(signedMessage)
    }).encrypt(this.encryption.getEncryptionKeypair, reciever)
  }

  /*   getReplicationTopicEncryption(): PublicKeyEncryptionResolver {
      return replicationTopicEncryptionWithRequestKey(this.identity, this.keystore, (key, replicationTopic) => this.requestAndWaitForKeys(replicationTopic, new RequestKeysByKey<(Ed25519Keypair | X25519Keypair)>({
        key
      })))
    } */


  async getEncryptionKey(replicationTopic: string): Promise<KeyWithMeta<Ed25519Keypair | X25519Keypair> | undefined> {
    // v0 take some recent
    const keys = (await this.keystore.getKeys<Ed25519Keypair | X25519Keypair>(replicationTopic));
    let key = keys?.[0];
    if (!key) {
      const keys = await this.requestAndWaitForKeys(replicationTopic, new RequestKeysByReplicationTopic({
        replicationTopic
      }))
      key = keys ? keys[0] : undefined;
    }
    return key;
  }


  static async createInstance(ipfs: IPFS, options: CreateInstanceOptions = {}) {
    let id: PeerId = (await ipfs.id()).id;
    const directory = options.directory || './orbitdb'

    const storage = options.storage || {
      createStore: (path): Level => {
        return new Level(path)
      }
    };


    /* if (options.identity && options.identity.provider.keystore) {
      options.keystore = options.identity.provider.keystore
    } */
    const keystore: Keystore = options.keystore || new Keystore(await storage.createStore(path.join(directory, id.toString(), '/keystore')))
    let identity: Identity;
    if (options.identity) {
      identity = options.identity;
    }
    else {
      const signKey = await keystore.createEd25519Key({ id: id.toString() });

      identity = {
        ...signKey.keypair,
        sign: (data) => signKey.keypair.sign(data)

      }
    }

    /* const signKey = options.signKey || await options.keystore.createKey(Buffer.from(id), KeyWithMeta<Ed25519Keypair>); */
    /* if (!options.identity) {
      options.identity = await Identities.createIdentity({
        id: new Uint8Array(Buffer.from(id)),
        keystore: options.keystore
      })
    } */

    const cache = options.cache || new Cache(await storage.createStore(path.join(directory, id.toString(), '/cache')));
    const localNetwork = options.localNetwork || false;
    const finalOptions = Object.assign({}, options, { peerId: id, keystore, identity, directory, storage, cache, localNetwork })
    return new OrbitDB(ipfs, identity, finalOptions)
  }


  async disconnect() {
    // Close a direct connection and remove it from internal state

    for (const [_topic, channel] of this._replicationTopicSubscriptions) {
      await channel.close();
    }


    await this._ipfs.pubsub.unsubscribe(DirectChannel.getTopic([this.id.toString()]));
    const removeDirectConnect = (value: any, e: string) => {
      this._directConnections.get(e)?.close()
      this._directConnections.delete(e);
    }

    // Close all direct connections to peers
    this._directConnections.forEach(removeDirectConnect);


    // Disconnect from pubsub
    /*   if (this._pubsub) {
        await this._ipfs.pubsub.disconnect()
      } */


    // close keystore
    await this.keystore.close()

    // Close all open databases
    for (const [key, dbs] of Object.entries(this.stores)) {
      await Promise.all(Object.values(dbs).map(db => db.close && db.close()));
      delete this.stores[key]
    }

    const caches = Object.keys(this.caches)
    for (const directory of caches) {
      await this.caches[directory].cache.close()
      delete this.caches[directory]
    }

    // Remove all databases from the state
    this.stores = {}
  }

  // Alias for disconnect()
  async stop() {
    await this.disconnect()
  }

  async _createCache(directory: string) {
    const cacheStorage = await this.storage.createStore(directory)
    return new Cache(cacheStorage)
  }



  // Callback for local writes to the database. We the update to pubsub.
  _onWrite<T>(store: Store<any>, entry: Entry<T>, replicationTopic: string) {
    const sendAll = (data: Uint8Array): Promise<void> => this._ipfs.pubsub.publish(replicationTopic, data);
    let send = sendAll;
    if (store.replicate) {
      // send to peers directly
      send = async (data: Uint8Array) => {
        const replicators = await this.findReplicators(replicationTopic, store.replicate, entry.gid);
        const channels: SharedChannel<DirectChannel>[] = [];
        for (const replicator of replicators) {
          if (replicator === this.id.toString()) {
            continue;
          }
          let channel = this._directConnections.get(replicator);
          if (!channel) { // we are missing a channel, send to all instead as fallback
            return sendAll(data);
          }
          else {
            channels.push(channel);
          }
        }
        await Promise.all(channels.map(channel => channel.channel.send(data)));
        return;
      }
    }
    exchangeHeads(send, store, this.identity, [entry], replicationTopic, true)
  }

  // Callback for receiving a message from the network
  async _onMessage(message: PubSubMessage) {
    try {
      const peer = message.type === 'signed' ? (message as SignedPubSubMessage).from : undefined;
      const maybeEncryptedMessage = deserialize(message.data, MaybeEncrypted) as MaybeEncrypted<MaybeSigned<ProtocolMessage>>
      const decrypted = await maybeEncryptedMessage.decrypt(this.encryption.getAnyKeypair)
      const signedMessage = decrypted.getValue(MaybeSigned);
      await signedMessage.verify();
      const msg = signedMessage.getValue(ProtocolMessage);
      const sender: SignKey | undefined = signedMessage.signature?.publicKey;
      const checkTrustedSender = async (replicationTopic: string): Promise<boolean> => {
        let isTrusted = false;
        if (sender) {
          isTrusted = !!(await this.getNetwork(replicationTopic)?.isTrusted(sender))
        }
        if (!isTrusted) {
          logger.info("Recieved message from untrusted peer")
          return false
        }
        return true
      }

      if (msg instanceof ExchangeHeadsMessage) {
        /**
         * I have recieved heads from someone else. 
         * I can use them to load associated logs and join/sync them with the data stores I own
         */

        const { replicationTopic, address, heads } = msg
        const senderIsTrusted = await checkTrustedSender(replicationTopic);
        // replication topic === trustedNetwork address

        let stores = this.stores[replicationTopic]
        if (!stores) {
          if (!senderIsTrusted) {
            return; // dont allow untrusted senders to open stores
          }

          stores = {};
          this.stores[replicationTopic] = stores
        }
        const isReplicating = this._replicationTopicSubscriptions.has(replicationTopic); // TODO should be TrustedNetwork has my ipfs id

        if (heads && stores) {

          /*   let isLeaderResolver: () => Promise<{ leaders: string[], isLeader: boolean }> = async () => {
              const leaders = await this.findLeaders(replicationTopic, address, Buffer.from(signedMessage.signature.signature).toString('base64'), this.minReplicas);
              return {
                leaders,
                isLeader: this.isLeader(leaders)
              }
            } */

          /**
           * Filter our heads that we should not care about
           */
          // We should sync heads if 
          // - We already support next
          // or 
          // - Or we are a leader
          // DO CHECKS here and dont pass isLeader check to sync methodd

          const leaderCache: Map<string, string[]> = new Map();
          if (!stores[address]) {
            // open store if is leader and sender is trusted

            if (!senderIsTrusted) {
              return;
            }

            for (const [gid, value] of groupByGid(heads)) {
              // Check if root, if so, we check if we should open the store
              const leaders = await this.findLeaders(replicationTopic, isReplicating, gid, this.minReplicas); // Todo reuse calculations
              leaderCache.set(gid, leaders);
              if (leaders.find(x => x === this.id.toString())) {
                await this.open(Address.parse(address), replicationTopic, { directory: this.directory, identity: this.identity })
              }
            }
            if (!stores[address]) {
              return;
            }
          }

          const store = stores[address];
          /*           heads.sort(store.oplog._sortFn); */
          /*    const mergeable = [];
             const newItems: Map<string, Entry<any>> = new Map(); */

          const toMerge: EntryWithRefs<any>[] = [];
          for (const [gid, value] of groupByGid(heads)) {
            const leaders = leaderCache.get(gid) || await this.findLeaders(replicationTopic, isReplicating, gid, this.minReplicas);
            const isLeader = leaders.find((l) => l === this.id.toString());
            if (!isLeader) {
              continue;
            }
            value.forEach((head) => {
              toMerge.push(head);
            })

          }
          if (toMerge.length > 0) {
            await store.sync(toMerge);
          }
          /*   for (const head of heads) {
              if (store.oplog._peersByGid.has(head.gid) || head.next.find(n => store.oplog.has(n))) {
                mergeable.push(head);
              }
              else { */
          // if new root, then check if we should merge this

          /*  if (head.next.length === 0) {
             if (leaders.find((l) => l === this.id)) {
               // is leader
               newItems.set(head.hash, head);
               store.oplog.setPeersByGid(head.gid, new Set(leaders))
             }
             else {
               // Safely ignore item, since its a not root element, and we are not the leader
             }
           }
           else {
             // Unexpected if not new items contains nexts
             head.next.forEach((next) => {
               if (!newItems.has(next) && !store.oplog.has(next)) {
                 logger.error("Failed to sync item with next elements that are unknown")
               }
             })
             mergeable.push(head);
             newItems.set(head.hash, head);
           } */
          /*  }
         } */



          /* 
                    for (const [storeAddress, store] of Object.entries(stores)) {
                      if (store) {
                        if (storeAddress !== address) {
                          continue // this messages was intended for another store
                        }
                        if (heads.length > 0) { */
          /*    if (!store.replicate) {
               // if we are only to write, then only care about others clock
               for (const head of heads) {
                 head.init({
                   encoding: store.oplog._encoding,
                   encryption: store.oplog._encryption
                 })
                 const clock = await head.getClock();
                 store.oplog.mergeClock(clock)
               }
             }
             else {
               // Full sync
               
 
             } */

          /*      }
           
             }
           } */
        }

        logger.debug(`Received ${heads.length} heads for '${address}':\n`, JSON.stringify(heads.map(e => e.entry.hash), null, 2))
      }
      /*  else if (msg instanceof RequestHeadsMessage) {
          // I have recieved a message urging me to share my heads
          // so that another peer can clone my log and join with theirs
         const { replicationTopic, address } = msg
         if (!(await checkTrustedSender(replicationTopic))) {
           return;
         }
 
         const stores = this.stores[replicationTopic];  // Send the heads if we have any
         if (stores) {
           for (const [storeAddress, store] of Object.entries(stores)) {
             if (store.replicate) {
               await exchangeHeads(async (peer, msg) => {
                 const channel = await this.getChannel(peer, replicationTopic);
                 return channel.send(Buffer.from(msg));
               }, store, (hash) => this.findLeaders(replicationTopic, store.address.toString(), hash, this.minReplicas), await this.getSigner());
             }
             else {
               // Ignore for now (dont share headss)
             }
           }
         }
         logger.debug(`Received exchange heades request for topic: ${replicationTopic}, address: ${address}`)
       }*/
      else if (msg instanceof KeyResponseMessage) {
        await recieveKeys(msg, (keys) => {
          return Promise.all(keys.map((key) => this.keystore.saveKey(key)))
        })

      }
      else if (msg instanceof ExchangeSwarmMessage) {
        let hasAll = true;
        for (const i of msg.info) {
          if (!this._directConnections.has(i.id)) {
            hasAll = false;
            break;
          }
        }
        if (hasAll) {
          return;
        }

        if (!await checkTrustedSender(message.topic)) {
          return;
        }

        msg.info.forEach(async (info) => {
          if (info.id === this.id.toString()) {
            return;
          }
          const suffix = '/p2p/' + info.id;
          this._ipfs.swarm.connect(multiaddr(info.address.toString() + (info.address.indexOf(suffix) === -1 ? suffix : '')));
        })
      }
      else if (msg instanceof RequestKeyMessage) {

        if (!peer) {
          logger.error("Execting a sigmed pubsub message")
          return;
        }

        if (!sender) {
          logger.info("Expecing sender when recieving key info")
          return;
        }

        if (msg.condition instanceof RequestKeysByReplicationTopic) {
          if (!await checkTrustedSender(msg.condition.replicationTopic)) {
            return;
          }
          const canExchangeKey: KeyAccessCondition = (key) => key.group === (msg.condition as RequestKeysByReplicationTopic<any>).replicationTopic;

          /**
           * Someone is requesting X25519 Secret keys for me so that they can open encrypted messages (and encrypt)
           * 
           */

          const send = (data: Uint8Array) => this._ipfs.pubsub.publish(DirectChannel.getTopic([peer.toString()]), data);
          await exchangeKeys(send, msg, canExchangeKey, this.keystore, this.identity, this.encryption)
          logger.debug(`Exchanged keys`)
        }

      }
      else if (msg instanceof RequestReplicatorInfo) {

        if (!peer) {
          logger.error("Execting a sigmed pubsub message")
          return;
        }

        const store = this.stores[msg.replicationTopic]?.[msg.address];
        if (!store || !store.replicate) {
          return;
        }

        if (!(await checkTrustedSender(msg.replicationTopic))) {
          return;
        }

        // if supports store, return resp
        if (store) {
          const send = this._directConnections.has(peer.toString()) ? (message: Uint8Array) => (this._directConnections.get(peer.toString()) as SharedChannel<DirectChannel>).channel.send(message) : (message: Uint8Array) => this._ipfs.pubsub.publish(msg.replicationTopic, message);
          if (msg.heads) {
            let ownedHeads = msg.heads.filter(h => !!store.oplog._entryIndex.get(h));
            if (ownedHeads.length > 0) {
              await exchangePeerInfo(msg.id, msg.replicationTopic, store, ownedHeads, send, await this.getSigner())
            }
          }
          else {
            await exchangePeerInfo(msg.id, msg.replicationTopic, store, undefined, send, await this.getSigner())
          }
        }
      }
      else if (msg instanceof ReplicatorInfo) {

        if (!(await checkTrustedSender(msg.replicationTopic))) {
          return;
        }

        if (!sender) {
          logger.info("Expecing sender when recieving replicatio info")
          return;
        }

        // TODO singleton
        const hashcode = sender.hashCode();

        this._peerInfoLRU.set(hashcode, {
          peerInfo: msg,
          publicKey: sender
        } as PeerInfoWithMeta)

        if (msg.fromId) {
          await this._peerInfoResponseCounter.increment(msg.fromId);
        }
        msg.heads?.forEach((h) => {
          this._supportedHashesLRU.increment(h);
        })

      }

      /*  else if (msg instanceof RequestReplication) {
 
         if (!this._subscribeForReplication.has(msg.replicationTopic)) {
           return;
         }
 
         if (!(await checkTrustedSender(msg.replicationTopic))) {
           return;
         }
         for (const r of msg.resourceRequirements) {
           if (!await r.ok(this)) {
             return; // does not fulfill criteria
           }
         }
         // TODO only leader open?
         await this.open(msg.store, { replicationTopic: msg.replicationTopic });
         if (msg.heads.length > 0) {
           await msg.store.sync(msg.heads, () => this.isLeader(msg.store, Buffer.from(data).toString('base64'), XXX))
         }
       } */

      else {
        throw new Error("Unexpected message")
      }
    } catch (e: any) {
      logger.error(e)
    }
  }


  async _onPeerConnected(replicationTopic: string, peer: string) {
    logger.debug(`New peer '${peer}' connected to '${replicationTopic}'`)


    // determine if we should open a channel (we are replicating a store on the topic + a weak check the peer is trusted)
    const stores = this.stores[replicationTopic];
    if (stores) {
      for (const [_storeAddress, store] of Object.entries(stores)) {
        if (store.replicate) {
          // create a channel for sending/receiving messages
          /*  const channel = await this.getChannel(peer, replicationTopic); */
          /*  await exchangeHeads((msg) => channel.send(Buffer.from(msg)), store, (key) => this._supportedHashesLRU.get(key) >= this.minReplicas, await this.getSigner()); */
          /*    await exchangeHeads(this.id, async (peer, msg) => {
               const channel = await this.getChannel(peer, replicationTopic);
               return channel.send(Buffer.from(msg));
             }, store, (hash) => this.findLeaders(replicationTopic, store.address.toString(), hash, this.minReplicas), await this.getSigner()); */

          /*  let openChannel: boolean;
           const network = this.getNetwork(replicationTopic);
           if (network) { // network could be undefined because we are just to create it, in that case we will allow all connections and assume they are limited
             openChannel = await network.isTrusted(new IPFSAddress({ address: peer })) // this could be false if even if it should be true because network info is not update (yet)
           }
           else {
             openChannel = true;
           }
           if (openChannel) {
             await this.getChannel(peer, replicationTopic);
           } */
          await exchangeSwarmAddresses((data) => this._ipfs.pubsub.publish(replicationTopic, data), this.identity, peer, await this._ipfs.swarm.peers(), this.getNetwork(replicationTopic), this.localNetwork)
          await this.getChannel(peer, replicationTopic); // always open a channel, and drop channels if necessary (not trusted) (TODO)
          return; // we return because we have know opened a channel to this peer

          // Creation of this channel here, will make sure it is created even though a head might not be exchanged

          /*       await exchangeHeads(this.id, async (peer, msg) => {
                  const channel = await this.getChannel(peer, replicationTopic);
                  return channel.send(Buffer.from(msg));
                }, store, (hash) => this.findLeaders(replicationTopic, store.address.toString(), hash, this.minReplicas), await this.getSigner()); */

        }
        else {
          const x = 123;
          // If replicate false, we are in write mode. Means we should exchange all heads 
          /*   await exchangeHeads((message) => this._ipfs.pubsub.publish(replicationTopic, message), store, () => false, await this.getSigner()) */
        }


      }
    }
  }

  /* async _onPeerDisconnected(topic: string, peer: string) {

    // get all topics for this peer
    if (this._directConnections.has(peer)) {
      for (const replicationTopic of this._directConnections.get(peer).dependencies) {
        for (const store of Object.values(this.stores[replicationTopic])) {
          const heads = await store.getHeads();
          const groupedByGid = groupByGid(heads);
          for (const [gid, entries] of groupedByGid) {
            const peers = this.findReplicators(store, gid); // would not work if peer got disconnected?
            const index = peers.findIndex(p => p === peer);
            if (index !== -1) { //
              // We lost an important peer,
              if (peers[(index + 1) & peers.length] === this.id) {

                // is should tell the others that we need one more replicator
                //const
              }
            }
          }
        }
      }
    }

  } */


  /**
   * When a peers join the networkk and want to participate the leaders for particular log subgraphs might change, hence some might start replicating, might some stop
   * This method will go through my owned entries, and see whether I should share them with a new leader, and/or I should stop care about specific entries
   * @param channel
   */
  async replicationReorganization(modifiedChannel: DirectChannel) {
    const connections = this._directConnections.get(modifiedChannel.recieverId.toString());
    if (!connections) {
      logger.error("Missing direct connection to: " + modifiedChannel.recieverId.toString());
      return;
    }

    for (const replicationTopic of connections.dependencies) {
      const stores = this.stores[replicationTopic];
      if (stores) {
        for (const store of Object.values(stores)) {
          const heads = store.oplog.heads;
          const groupedByGid = groupByGid(heads);
          for (const [gid, entries] of groupedByGid) {
            if (entries.length === 0) {
              continue; // TODO maybe close store?
            }
            /*     const oldPeers = this.findReplicators(store, gid, [channel.recieverId]);
                const oldPeersSet = new Set(this.findReplicators(store, gid, [channel.recieverId])); */
            const oldPeersSet = this._gidPeersHistory.get(gid);
            const newPeers = await this.findReplicators(replicationTopic, store.replicate, gid);
            /* const index = oldPeers.findIndex(p => p === channel.recieverId); */
            for (const newPeer of newPeers) {
              if (!oldPeersSet?.has(newPeer) && newPeer !== this.id.toString()) { // second condition means that if the new peer is us, we should not do anything, since we are expecting to recieve heads, not send

                // send heads to the new peer
                const channel = this._directConnections.get(newPeer)?.channel;
                if (!channel) {

                  logger.error("Missing channel when reorg to peer: " + newPeer.toString())
                  continue
                }

                await exchangeHeads(async (message) => {
                  await channel.send(message);
                }, store, this.identity, entries, replicationTopic, true)
              }
            }

            if (!newPeers.find(x => x === this.id.toString())) {
              // delete entries since we are not suppose to replicate this anymore
              // TODO add delay? freeze time? (to ensure resiliance for bad io)
              store.oplog.removeAll(entries);

              // TODO if length === 0 maybe close store? 
            }
            this._gidPeersHistory.set(gid, new Set(newPeers))
            /* if (index !== -1)  */{ //
              // We lost an replicating peer,
              // find diff

              /* if (peers[(index + 1) & peers.length] === this.id) { */

              // is should tell the others that we need one more replicator
              //const
              /* } */
            }
          }
        }
      }

    }

  }



  async getSigner() {
    return async (bytes: Uint8Array) => {
      return {
        signature: await this.identity.sign(bytes),
        publicKey: this.identity.publicKey
      }
    }
  }


  async getChannel(peer: string, fromTopic: string): Promise<DirectChannel | undefined> {

    // TODO what happens if disconnect and connection to direct connection is happening
    // simultaneously
    const getDirectConnection = (peer: string) => this._directConnections.get(peer)?._channel

    let channel = getDirectConnection(peer)
    if (!channel) {
      try {
        logger.debug(`Create a channel to ${peer}`)
        channel = await DirectChannel.open(this._ipfs, peer, this._onMessage.bind(this), {
          onPeerLeaveCallback: (channel) => {

            // First modify direct connections
            this._directConnections.get(channel.recieverId.toString())?.close(channel.recieverId.toString())

            // Then perform replication reorg
            this.replicationReorganization(channel);
          },
          onNewPeerCallback: (channel) => {

            // First modify direct connections
            if (!this._directConnections.has(channel.recieverId.toString())) {
              this._directConnections.set(channel.recieverId.toString(), new SharedChannel(channel, new Set([fromTopic])));
            }
            else {
              this._directConnections.get(channel.recieverId.toString())?.dependencies.add(fromTopic);
            }

            // Then perform replication reorg
            this.replicationReorganization(channel);
          }
        })
        logger.debug(`Channel created to ${peer}`)
      } catch (e: any) {
        logger.error(e)
        return undefined;
      }
    }

    // Wait for the direct channel to be fully connected
    await channel.connect()
    logger.debug(`Connected to ${peer}`)

    return channel;
  }



  // Callback when a database was closed
  async _onClose(db: Store<any>, replicationTopic: string) { // TODO Can we really close a this.stores, either we close all stores in the replication topic or none
    const address = db.address.toString()
    logger.debug(`Close ${address}`)

    // Unsubscribe from pubsub
    await this._replicationTopicSubscriptions.get(replicationTopic)?.close(db.id);


    const dir = db && db.options.directory ? db.options.directory : this.directory
    const cache = this.caches[dir]

    if (cache && cache.handlers.has(address)) {
      cache.handlers.delete(address)
      if (!cache.handlers.size) {
        await cache.cache.close()
      }
    }

    delete this.stores[replicationTopic][address]

    const otherStoresUsingSameReplicationTopic = this.stores[replicationTopic]

    // close all connections with this repplication topic if this is the last dependency
    const isLastStoreForReplicationTopic = Object.keys(otherStoresUsingSameReplicationTopic).length === 0;
    if (isLastStoreForReplicationTopic) {

      /*   const cron = this._replicationTopicJobs.get(db.replicationTopic);
        if (cron) {
          cron.controller.abort();
          this._replicationTopicJobs.delete(db.replicationTopic);
        }
   */
      for (const [key, connection] of this._directConnections) {
        await connection.close(replicationTopic);
        // Delete connection from thing

        // TODO what happens if we close a store, but not its direct connection? we should open direct connections and make it dependenct on the replciation topic
      }
    }



  }

  async _onDrop(db: Store<any>) {
    const address = db.address.toString()
    const dir = db && db.options.directory ? db.options.directory : this.directory
    await this._requestCache(address, dir, db._cache)
  }

  async _onLoad(db: Store<any>) {
    const address = db.address.toString()
    const dir = db && db.options.directory ? db.options.directory : this.directory
    await this._requestCache(address, dir, db._cache)
    /*   this.addStore(db); */
  }


  /* addStore(store: Store<any>) {
    const storeAddress = store.address.toString();
    if (!storeAddress) { throw new Error("Address undefined") }
   
    const existingStore = this.stores[storeAddress];
    if (!!existingStore && existingStore !== store) { // second condition only makes this throw error if we are to add a new instance with the same address
      throw new Error(`Store at ${storeAddress} is already created`)
    }
    this.stores[storeAddress] = store;
  }
  */
  async addStore(store: StoreLike<any>, replicationTopic: string) {
    if (!this.stores[replicationTopic]) {
      this.stores[replicationTopic] = {};
    }
    if (this.stores[replicationTopic] === undefined) {
      throw new Error("Unexpected behaviour")
    }

    const storeAddress = store.address.toString();
    const existingStore = this.stores[replicationTopic][storeAddress];
    if (!!existingStore && existingStore !== store) { // second condition only makes this throw error if we are to add a new instance with the same address
      throw new Error(`Store at ${replicationTopic}/${storeAddress} is already created`)
    }
    this.stores[replicationTopic][storeAddress] = store;

    /* if (!this._replicationTopicJobs.has(replicationTopic) && store.replicate) {
      const controller = new AbortController();
      const job = await createEmitHealthCheckJob({
        stores: () => Object.keys(this.stores[replicationTopic]),
        subscribingForReplication: (topic) => this._subscribeForReplication.has(topic)
      }, replicationTopic, (r, d) => this._ipfs.pubsub.publish(r, d), () => this._ipfs.isOnline(), controller, await this.getSigner(), this.encryption);
      job();
      this._replicationTopicJobs.set(replicationTopic, {
        controller
      })
    } */
  }

  _getPeersLRU: LRU<string, Promise<PeerInfoWithMeta[]>> = new LRU({ max: 500, ttl: WAIT_FOR_PEERS_TIME })

  getPeersOnTopic(topic: string): string[] {
    const ret: string[] = [];
    for (const [k, v] of this._directConnections) {
      if (v.dependencies.has(topic)) {
        ret.push(k);
      }
    }
    return ret;
  }


  async getPeers(request: RequestReplicatorInfo, options: { ignoreCache?: boolean, waitForPeersTime?: number } = {}): Promise<PeerInfoWithMeta[]> {
    const serializedRequest = serialize(request);
    const hashKey = Buffer.from(serializedRequest).toString('base64');
    if (!options.ignoreCache) {
      const promise = this._getPeersLRU.get(hashKey);
      if (promise) {
        return promise;
      }
    }

    const promise = new Promise<PeerInfoWithMeta[]>(async (r, c) => {
      await this.subscribeToReplicationTopic(request.replicationTopic);
      await requestPeerInfo(serializedRequest, request.replicationTopic, (topic, message) => this._ipfs.pubsub.publish(topic, message), await this.getSigner())
      const directConnectionsOnTopic = this.getPeersOnTopic(request.replicationTopic).length
      const timeout = options?.waitForPeersTime || WAIT_FOR_PEERS_TIME * 3;
      if (directConnectionsOnTopic) {
        // Assume that all peers are connected
        // TODO What happens if directConnectionsOnTopic changes?
        try {
          await waitFor(() => this._peerInfoResponseCounter.get(request.id) as number >= directConnectionsOnTopic, { timeout, delayInterval: 400 })
        } catch (error) {
        }
      }
      else {
        await delay(timeout);
      }

      /* const caches: { value: PeerInfoWithMeta }[] = Object.values(this._peerInfoLRU); */
      const peersSupportingAddress: PeerInfoWithMeta[] = [];
      this._peerInfoLRU.forEach((v, k) => {
        if (v.peerInfo.store === request.address) {
          peersSupportingAddress.push(v)
        }
      })
      r(peersSupportingAddress)
    })
    this._getPeersLRU.set(hashKey, promise);
    return promise
  }

  /**
  * An intentionally imperfect leader rotation routine
  * @param slot, some time measure
  * @returns 
  */
  isLeader(leaders: string[]): boolean {
    return !!(leaders.find(id => id === this.id.toString()))
  }

  findReplicators(replicationTopic: string, replicating: boolean, gid: string/* , addPeers: string[] = [], removePeers: string[] = [] */): Promise<string[]> {
    return this.findLeaders(replicationTopic, replicating, gid, this.minReplicas);
  }


  async findLeaders(replicationTopic: string, replicating: boolean, slot: { toString(): string }, numberOfLeaders: number/* , addPeers: string[] = [], removePeers: string[] = [] */): Promise<string[]> {
    // Hash the time, and find the closest peer id to this hash
    const h = (h: string) => createHash('sha1').update(h).digest('hex');
    const slotHash = h(slot.toString())

    // Assumption: All peers wanting to replicate on topic has direct connections with me (Fully connected network)
    let peers: string[] = this.getPeersOnTopic(replicationTopic);

    // Assumption: Network specification is accurate
    // Replication topic is not an address we assume that the network allows all participants
    const isTrusted = (peer: string | PeerId) => Address.isValid(replicationTopic) ? this.getNetwork(replicationTopic)?.isTrusted(new IPFSAddress({ address: peer.toString() })) : true
    peers = await Promise.all(peers.map(isTrusted))
      .then((results) => peers.filter((_v, index) => results[index]))

    const hashToPeer: Map<string, string> = new Map();
    const peerHashed: string[] = [];

    if (peers.length === 0) {
      return [this.id.toString()];
    }

    // Add self
    if (replicating) {
      peers.push(this.id.toString())
    }


    // Hash step
    peers.forEach((peer) => {
      const peerHash = h(peer + slotHash); // we do peer + slotHash because we want peerHashed.sort() to be different for each slot, (so that uniformly random pick leaders). You can see this as seed
      hashToPeer.set(peerHash, peer);
      peerHashed.push(peerHash);
    })
    numberOfLeaders = Math.min(numberOfLeaders, peerHashed.length);
    peerHashed.push(slotHash);

    // Choice step

    // TODO make more efficient
    peerHashed.sort((a, b) => a.localeCompare(b)) // sort is needed, since "getPeers" order is not deterministic
    let slotIndex = peerHashed.findIndex(x => x === slotHash);
    // we only step forward 1 step (ignoring that step backward 1 could be 'closer')
    // This does not matter, we only have to make sure all nodes running the code comes to somewhat the 
    // same conclusion (are running the same leader selection algorithm)
    const leaders: string[] = [];
    let offset = 0;
    for (let i = 0; i < numberOfLeaders; i++) {
      let nextIndex = (slotIndex + 1 + i + offset) % peerHashed.length;
      if (nextIndex === slotIndex) {
        offset += 1;
        nextIndex = (nextIndex + 1) % peerHashed.length;
      }
      leaders.push(hashToPeer.get(peerHashed[nextIndex]) as string);
    }
    return leaders;
  }



  /*  _requestingReplicationPromise: Promise<void>;
   async requestReplication(store: Store<any>, options: { heads?: Entry<any>[], replicationTopic?: string, waitForPeersTime?: number } = {}) {
     const replicationTopic = options?.replicationTopic || store.replicationTopic;
     if (!replicationTopic) {
       throw new Error("Missing replication topic for replication");
     }
     await this._requestingReplicationPromise;
     if (!store.address) {
       await store.save(this._ipfs);
     }
     const currentPeersCountFn = async () => (await this.getPeers(replicationTopic, store.address, options)).length
     const currentPeersCount = await currentPeersCountFn();
     this._requestingReplicationPromise = new Promise(async (resolve, reject) => {
       const signedThing = new DecryptedThing({
         data: await serialize(await (new MaybeSigned({
           data: serialize(new RequestReplication({
             replicationTopic,
             store,
             heads: options?.heads,
             resourceRequirements: [new HeapSizeRequirement({
               heapSize: BigInt(STORE_MIN_HEAP_SIZE)
             })]
           }))
         })).sign(await this.getSigner()))
       }) /// TODO add encryption?
   
       await this._ipfs.pubsub.publish(replicationTopic, serialize(signedThing));
       await waitForAsync(async () => await currentPeersCountFn() >= currentPeersCount + 1, {
         timeout: (options?.waitForPeersTime || 5000) * 2,
         delayInterval: 50
       })
       resolve();
   
     })
     await this._requestingReplicationPromise;
   } */


  async subscribeToReplicationTopic(topic: string): Promise<void> {
    if (!this.stores[topic]) {
      this.stores[topic] = {};
    }
    if (!this._replicationTopicSubscriptions.has(topic)) {
      const topicMonitor = new IpfsPubsubPeerMonitor(this._ipfs.pubsub, topic, {
        onJoin: (peer) => {
          logger.debug(`Peer joined ${topic}:`)
          logger.debug(peer)
          this._onPeerConnected(topic, peer);
        },
        onLeave: (peer) => {
          logger.debug(`Peer ${peer} left ${topic}`)
          /*    this._onPeerDisconnected(topic, peer); */
        },
        onError: (e) => {
          logger.error(e)
        }

      })
      this._replicationTopicSubscriptions.set(topic, new SharedChannel(await new SharedIPFSChannel(this._ipfs, this.id, topic, this._onMessage.bind(this), topicMonitor).start()));

    }

    /* if (!this._ipfs.pubsub._subscriptions[topic]) {
      
    } */

  }
  hasSubscribedToReplicationTopic(topic: string): boolean {
    return !!this.stores[topic]
  }
  unsubscribeToReplicationTopic(topic: string | TrustedNetwork, id: string = '_'): Promise<boolean> | undefined {
    if (typeof topic !== 'string') {
      topic = topic.address.toString();
    }

    /* if (this._ipfs.pubsub._subscriptions[topic]) { */
    return this._replicationTopicSubscriptions.get(topic)?.close(id);
    /* } */
  }



  /* Create and Open databases */

  /*
    options = {
      accessController: { write: [] } // array of keys that can write to this database
      overwrite: false, // whether we should overwrite the existing database if it exists
    }
  */


  /*  directory?: string,
   onlyHash?: boolean,
   overwrite?: boolean,
   accessController?: any,
   create?: boolean,
   type?: string,
   localOnly?: boolean,
   replicationConcurrency?: number,
   replicate?: boolean,
   replicationTopic?: string | (() => string),
 
   encoding?: Encoding<any>;
   encryption?: (keystore: Keystore) => StorePublicKeyEncryption; */
  /* async create<S extends StoreLike<any>>(store: S, options: {
    timeout?: number,
    identity?: Identity,
    cache?: Cache,



  } & IStoreOptions<any> = {}): Promise<S> {

    logger.debug('create()')
    logger.debug(`Creating database '${store.name}' as ${store.constructor.name}`)

    // Create the database address

    // TODO prevent double save (store is also saved on init)
    const dbAddress = await store.save(this._ipfs, { pin: true });

    if (!options.cache)
      options.cache = await this._requestCache(dbAddress.toString(), options.directory)

    // Check if we have the database locally
    const haveDB = await this._haveLocalData(options.cache, dbAddress)
    if (haveDB) { throw new Error(`Database '${dbAddress}' already exists!`) }

    // Save the database locally
    await this._addManifestToCache(options.cache, dbAddress)

    logger.debug(`Created database '${dbAddress}'`)

    // Open the database
    return this.open<S>(store, options)
  } */

  async _requestCache(address: string, directory: string, existingCache?: Cache<any>) {
    const dir = directory || this.directory
    if (!this.caches[dir]) {
      const newCache = existingCache || await this._createCache(dir)
      this.caches[dir] = { cache: newCache, handlers: new Set() }
    }
    this.caches[dir].handlers.add(address)
    const cache = this.caches[dir].cache

    // "Wake up" the caches if they need it
    if (cache) await cache.open()

    return cache
  }


  _openStorePromise: Promise<StoreLike<any> | undefined>

  /**
   * Default behaviour of a store is only to accept heads that are forks (new roots) with some probability
   * and to replicate heads (and updates) which is requested by another peer
   * @param store 
   * @param options 
   * @returns 
   */
  async open<S extends StoreLike<any>>(storeOrAddress: /* string | Address |  */S | Address | string, replicationTopic: string, options: OpenStoreOptions = {}): Promise<S> {


    // TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?
    await this._openStorePromise;
    this._openStorePromise = new Promise<S | undefined>(async (resolve, reject) => {
      let store = storeOrAddress as S;
      if (typeof storeOrAddress === 'string') {
        storeOrAddress = Address.parse(storeOrAddress);
      }
      if (storeOrAddress instanceof Address) {
        try {
          store = await Store.load(this._ipfs, storeOrAddress as any as Address) as any as S // TODO fix typings
        } catch (error) {
          logger.error("Failed to load store with address: " + storeOrAddress.toString());
          reject(error);
        }
      }

      try {
        logger.debug('open()')

        options = Object.assign({ localOnly: false, create: false }, options)
        logger.debug(`Open database '${store}'`)

        const resolveCache = async (address: Address) => {
          const cache = await this._requestCache(address.toString(), options.directory || this.directory)
          const haveDB = await this._haveLocalData(cache, address)
          logger.debug((haveDB ? 'Found' : 'Didn\'t find') + ` database '${address}'`)
          if (options.localOnly && !haveDB) {
            logger.warn(`Database '${address}' doesn't exist!`)
            throw new Error(`Database '${address}' doesn't exist!`)
          }

          if (!haveDB) {
            await this._addManifestToCache(cache, address)
          }
          return cache;
        }

        if (!options.encryption) {
          options.encryption = encryptionWithRequestKey(this.identity, this.keystore, (key) => this.requestAndWaitForKeys(replicationTopic, new RequestKeysByKey<(Ed25519Keypair | X25519Keypair)>({
            key
          })))
        }

        // Open the the database
        store.init && await store.init(this._ipfs, options.identity || this.identity, {
          replicate: true, ...options, ...{
            resolveCache,
            saveAndResolveStore: async (ipfs: IPFS, store: StoreLike<any>) => {
              const address = await store.save(this._ipfs);
              const a = address.toString();
              const alreadyHaveStore = this.stores[replicationTopic]?.[a];
              if (options.rejectIfAlreadyOpen) {
                new Error(`Store at ${replicationTopic}/${a} is already created`)
              }
              return alreadyHaveStore || store;
            }
          },
          /*           resourceOptions: options.resourceOptions || this.heapsizeLimitForForks ? { heapSizeLimit: () => this.heapsizeLimitForForks } : undefined,
           */
          onClose: async (store) => {
            await this._onClose(store, replicationTopic)
            if (options.onClose) {
              return options.onClose(store);
            }
            return;
          },
          onDrop: async (store) => {
            await this._onDrop(store)
            if (options.onDrop) {
              return options.onDrop(store);
            }
            return;
          },
          onLoad: async (store) => {
            await this._onLoad(store)
            if (options.onLoad) {
              return options.onLoad(store);
            }
            return;
          },
          onWrite: async (store, entry) => {
            await this._onWrite(store, entry, replicationTopic)
            if (options.onWrite) {
              return options.onWrite(store, entry);
            }
            return;
          },
          onReplicationComplete: async (store) => {
            if (options.onReplicationComplete) {
              options.onReplicationComplete(store);
            }
          },
          onReplicationProgress: async (store, entry) => {
            if (options.onReplicationProgress) {
              options.onReplicationProgress(store, entry);
            }
          },
          onReplicationQueued: async (store, entry) => {
            if (options.onReplicationQueued) {
              options.onReplicationQueued(store, entry);
            }
          },
          onOpen: async (store) => {

            // ID of the store is the address as a string

            // Subscribe to pubsub to get updates from peers,
            // this is what hooks us into the message propagation layer
            // and the p2p network
            /*   if (this._ipfs.pubsub.ls()) { */
            await this.addStore(store, replicationTopic)
            await this.subscribeToReplicationTopic(replicationTopic);

            /* else {
              const msg = new RequestHeadsMessage({
                address: store.address.toString(),
                replicationTopic: store.replicationTopic
              });
              await this._ipfs.pubsub.publish(store.replicationTopic, serialize(await this.decryptedSignedThing(serialize(msg))));
  
            } */
            /*  } */
            if (options.onOpen) {
              return options.onOpen(store);
            }
            return;
          }
        });
        resolve(store)
      } catch (error) {
        reject(error);
      }
    })
    const openStore = await this._openStorePromise;
    if (openStore instanceof TrustedNetwork && !this._trustedNetwork.has(openStore.address.toString())) {
      this._trustedNetwork.set(openStore.address.toString(), openStore)
    }
    return openStore as S
    /*  } */

  }

  getNetwork(address: string | Address): TrustedNetwork | undefined {
    return this._trustedNetwork.get(typeof address === 'string' ? address : address.toString())
  }

  async openNetwork(addressOrNetwork: string | Address | TrustedNetwork, options?: OpenStoreOptions) {
    let network: TrustedNetwork

    if (addressOrNetwork instanceof TrustedNetwork) {
      network = addressOrNetwork;
    }
    else {
      const loaded = await TrustedNetwork.load(this._ipfs, Address.parse(addressOrNetwork.toString()))
      if (loaded instanceof TrustedNetwork === false) {
        throw new Error("Address does not point to a TrustedNetwork")
      }
      network = loaded;
    }
    let address: Address = network.address;
    if (!address) {
      address = await network.save(this._ipfs);
    }

    const openNetwork = await this.open(network, address.toString(), options)
    return openNetwork;
  }

  async joinNetwork(address: Address | string | TrustedNetwork) {
    let trustedNetwork = this._trustedNetwork.get(address instanceof TrustedNetwork ? address.address.toString() : address.toString());
    if (!trustedNetwork) {
      throw new Error("TrustedNetwork is not open, please call `openNetwork` prior")
    }
    // Will be rejected by peers if my identity is not trusted
    // (this will sign our IPFS ID with our client Ed25519 key identity, if peers do not trust our identity, we will be rejected)
    await trustedNetwork.add(new IPFSAddress({ address: this.id.toString() }))
  }

  // Save the database locally
  async _addManifestToCache(cache: Cache<any>, dbAddress: Address) {
    await cache.set(path.join(dbAddress.toString(), '_manifest'), dbAddress.root)
    logger.debug(`Saved manifest to IPFS as '${dbAddress.root}'`)
  }

  /**
   * Check if we have the database, or part of it, saved locally
   * @param  {[Cache]} cache [The OrbitDBCache instance containing the local data]
   * @param  {[Address]} dbAddress [Address of the database to check]
   * @return {[Boolean]} [Returns true if we have cached the db locally, false if not]
   */
  async _haveLocalData(cache: Cache<any>, dbAddress: Address) {
    if (!cache) {
      return false
    }

    const addr = dbAddress.toString()
    const data = await cache.get(path.join(addr, '_manifest'))
    return data !== undefined && data !== null
  }

}
