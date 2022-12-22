import { BlockStore, PutOptions } from "./store.js";
import { Libp2p } from "libp2p";
import {
    stringifyCid,
    cidifyString,
    codecCodes,
    checkDecodeBlock,
} from "./block.js";
import * as Block from "multiformats/block";
import { variant, field, serialize, deserialize } from "@dao-xyz/borsh";
import LRU from "lru-cache";
import { CID } from "multiformats/cid";
import { v4 as uuid } from "uuid";
import { PubSub } from "@libp2p/interface-pubsub";
import { GossipsubEvents, GossipsubMessage } from "@chainsafe/libp2p-gossipsub";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
const logger = loggerFn({ module: "blocks-libp2p" });

export const DEFAULT_BLOCK_TRANSPORT_TOPIC = "_block";

export class BlockMessage {}

@variant(0)
export class BlockRequest extends BlockMessage {
    @field({ type: "string" })
    id: string;

    @field({ type: "string" })
    cid: string;

    constructor(cid: string) {
        super();
        this.id = uuid();
        this.cid = cid;
    }
}

@variant(1)
export class BlockResponse extends BlockMessage {
    @field({ type: "string" })
    id: string;

    @field({ type: "string" })
    cid: string;

    @field({ type: Uint8Array })
    bytes: Uint8Array;

    constructor(cid: string, bytes: Uint8Array) {
        super();
        this.id = uuid();
        this.cid = cid;
        this.bytes = bytes;
    }
}

export class LibP2PBlockStore implements BlockStore {
    _libp2p: Libp2p & { pubsub: PubSub<GossipsubEvents> };
    _transportTopic: string;
    _localStore?: BlockStore;
    _eventHandler?: (evt: CustomEvent<GossipsubMessage>) => any;
    _gossipCache?: LRU<string, Uint8Array>;
    _gossip = false;
    _open = false;
    constructor(
        libp2p: Libp2p,
        localStore?: BlockStore,
        options?: {
            transportTopic?: string;
            localTimeout?: number;
            gossip?: { cache: { max?: number; ttl?: number } | false };
        }
    ) {
        this._libp2p = libp2p;
        this._transportTopic =
            options?.transportTopic || DEFAULT_BLOCK_TRANSPORT_TOPIC;
        const localTimeout = options?.localTimeout || 1000;

        this._localStore = localStore;

        if (options?.gossip) {
            this._gossip = true;
            const gossipCacheOptions =
                options.gossip?.cache !== false
                    ? {
                          max: options.gossip?.cache.max || 1000,
                          ttl: options.gossip?.cache.ttl || 10000,
                      }
                    : undefined; // TODO choose default variables carefully
            this._gossipCache =
                gossipCacheOptions && new LRU(gossipCacheOptions);
        }

        if (
            this._libp2p.pubsub.getTopics().indexOf(this._transportTopic) === -1
        ) {
            this._libp2p.pubsub.subscribe(this._transportTopic);
        }

        this._eventHandler =
            this._localStore || this._gossipCache
                ? async (evt: CustomEvent<GossipsubMessage>) => {
                      if (!evt) {
                          return;
                      }
                      const message = evt.detail;
                      if (
                          /*    message.type === "signed" && */
                          message.msg.topic === this._transportTopic
                      ) {
                          /*    if (message.from.equals(libp2p.peerId)) {
                               return;
                           } */

                          try {
                              const decoded = deserialize(
                                  message.msg.data,
                                  BlockMessage
                              );
                              if (
                                  decoded instanceof BlockRequest &&
                                  this._localStore
                              ) {
                                  const cid = stringifyCid(decoded.cid);
                                  const block = await this._localStore.get<any>(
                                      cid,
                                      {
                                          timeout: localTimeout,
                                      }
                                  );
                                  if (!block) {
                                      return;
                                  }
                                  const response = serialize(
                                      new BlockResponse(cid, block.bytes)
                                  );
                                  await libp2p.pubsub.publish(
                                      this._transportTopic,
                                      response
                                  );
                              } else if (
                                  decoded instanceof BlockResponse &&
                                  this._gossipCache
                              ) {
                                  // TODO make sure we are not storing too much bytes in ram (like filter large blocks)
                                  this._gossipCache.set(
                                      decoded.cid,
                                      decoded.bytes
                                  );
                              }
                          } catch (error) {
                              console.error(
                                  "Got error for libp2p block transport: ",
                                  error
                              );
                              return; // timeout o r invalid cid
                          }
                      }
                  }
                : undefined;
    }

    async put(
        value: Block.Block<any, any, any, any>,
        options?: PutOptions | undefined
    ): Promise<string> {
        if (!this._localStore) {
            throw new Error("Local store not set");
        }

        // "Gossip" i.e. flood the network with blocks an assume they gonna catch them so they dont have to requrest them later
        try {
            if (this._gossip)
                await this._libp2p.pubsub.publish(
                    this._transportTopic,
                    serialize(
                        new BlockResponse(stringifyCid(value.cid), value.bytes)
                    )
                );
        } catch (error) {
            // ignore
        }
        return this._localStore.put(value, options);
    }

    async get<T>(
        cid: string,
        options?: { links?: string[]; timeout?: number; hasher?: any }
    ): Promise<Block.Block<T, any, any, any> | undefined> {
        const cidObject = cidifyString(cid);

        // locally ?
        // store/disc/mem
        const value =
            (await this._readFromGossip(cid, cidObject, options)) ||
            (this._localStore
                ? await this._localStore.get<T>(cid, options)
                : undefined);
        if (value) {
            return value;
        }

        // try to get it remotelly
        return this._readFromPubSub(cid, cidObject, options);
    }

    async rm(cid: string) {
        this._localStore?.rm(cid);
        this._gossipCache?.delete(cid);
    }

    async open(): Promise<void> {
        const hasTopic = this._libp2p.pubsub
            .getTopics()
            .indexOf(this._transportTopic);
        if (!hasTopic) {
            this._libp2p.pubsub.subscribe(this._transportTopic);
        }
        this._libp2p.pubsub.addEventListener(
            "gossipsub:message",
            this._eventHandler!
        );
        await this._localStore?.open();
        this._open = true;
    }

    async _readFromGossip(
        cidString: string,
        cidObject: CID,
        options: { hasher?: any } = {}
    ): Promise<Block.Block<any, any, any, 1> | undefined> {
        const cached = this._gossipCache?.get(cidString);
        if (cached) {
            try {
                const block = await checkDecodeBlock(cidObject, cached, {
                    hasher: options.hasher,
                });
                return block;
            } catch (error) {
                this._gossipCache?.delete(cidString); // something wrong with that block, TODO make better handling here
                return undefined;
            }
        }
    }
    async _readFromPubSub(
        cidString: string,
        cidObject: CID,
        options: { timeout?: number; hasher?: any } = {}
    ): Promise<Block.Block<any, any, any, 1> | undefined> {
        return new Promise<Block.Block<any, any, any, 1> | undefined>(
            (r, _reject) => {
                const timeout = options.timeout || 5000;

                const codec = codecCodes[cidObject.code];
                let value: Block.Block<any, any, any, 1> | undefined =
                    undefined;

                const eventHandler = async (
                    evt: CustomEvent<GossipsubMessage>
                ) => {
                    if (value) {
                        return;
                    }
                    const message = evt.detail;
                    if (message.msg.topic === this._transportTopic) {
                        const decoded = deserialize(
                            message.msg.data,
                            BlockMessage
                        );
                        if (decoded instanceof BlockResponse) {
                            if (decoded.cid !== cidString) {
                                return;
                            }
                            try {
                                if (
                                    !cidObject.equals(cidifyString(decoded.cid))
                                ) {
                                    return;
                                }

                                value = await checkDecodeBlock(
                                    cidObject,
                                    decoded.bytes,
                                    {
                                        codec,
                                        hasher: options?.hasher,
                                    }
                                );

                                resolve(value);
                            } catch (error: any) {
                                // invalid bytes like "CBOR decode error: not enough data for type"
                                // ignore error
                                // TODO add logging
                                logger.info(error?.message);
                            }
                        }
                    }
                };
                const resolve = async (resolvedValue) => {
                    await this._libp2p.pubsub.removeEventListener(
                        "gossipsub:message",
                        eventHandler
                    );
                    r(resolvedValue);
                };
                this._libp2p.pubsub.addEventListener(
                    "gossipsub:message",
                    eventHandler
                );
                setTimeout(() => {
                    resolve(undefined);
                }, timeout);

                this._libp2p.pubsub
                    .publish(
                        this._transportTopic,
                        serialize(new BlockRequest(cidString))
                    )
                    .catch((error) => {
                        // usually insufficient peers error is the reason why we come here
                        logger.warn(error?.message);
                    });
            }
        );
    }

    async idle(): Promise<void> {
        return;
    }

    async close(): Promise<void> {
        this._libp2p.pubsub.removeEventListener(
            "gossipsub:message",
            this._eventHandler
        );
        await this._localStore?.close();
        this._open = false;

        // we dont cleanup subscription because we dont know if someone else is sbuscribing also
    }

    get status() {
        if (this._open) {
            return (
                this._localStore?.status ||
                (this._libp2p.isStarted() ? "open" : "closed")
            );
        } else {
            return "closed";
        }
    }
}
