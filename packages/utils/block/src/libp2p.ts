import { BlockStore, PutOptions } from "./store.js";
import { Libp2p } from "libp2p";
import {
    stringifyCid,
    cidifyString,
    codecCodes,
    defaultHasher,
    checkDecodeBlock,
} from "./block.js";
import * as Block from "multiformats/block";
import { waitFor } from "@dao-xyz/peerbit-time";
import { variant, field, serialize, deserialize } from "@dao-xyz/borsh";
import type { Message } from "@libp2p/interface-pubsub";
import LRU from "lru-cache";
import { CID } from "multiformats/cid";

export const DEFAULT_BLOCK_TRANSPORT_TOPIC = "_block";

export class BlockMessage {}

@variant(0)
export class BlockRequest extends BlockMessage {
    @field({ type: "string" })
    cid: string;

    constructor(cid: string) {
        super();
        this.cid = cid;
    }
}

@variant(1)
export class BlockResponse extends BlockMessage {
    @field({ type: "string" })
    cid: string;

    @field({ type: Uint8Array })
    bytes: Uint8Array;

    constructor(cid: string, bytes: Uint8Array) {
        super();
        this.cid = cid;
        this.bytes = bytes;
    }
}

export class LibP2PBlockStore implements BlockStore {
    _libp2p: Libp2p;
    _transportTopic: string;
    _localStore?: BlockStore;
    _eventHandler?: (evt: any) => any;
    _gossipCache?: LRU<string, Uint8Array>;

    constructor(
        libp2p: Libp2p,
        localStore?: BlockStore,
        options: {
            transportTopic: string;
            localTimeout: number;
            gossip?: { cache: { max: number; ttl: number } | false };
        } = {
            transportTopic: DEFAULT_BLOCK_TRANSPORT_TOPIC,
            localTimeout: 1000,
        }
    ) {
        this._libp2p = libp2p;
        this._transportTopic = options.transportTopic;
        this._localStore = localStore;
        const gossipCacheOptions = (options.gossip?.cache !== false &&
            options.gossip?.cache) || { max: 1000, ttl: 10000 }; // TODO choose default variables carefully
        this._gossipCache = gossipCacheOptions && new LRU(gossipCacheOptions);

        if (
            this._libp2p.pubsub.getTopics().indexOf(options.transportTopic) ===
            -1
        ) {
            this._libp2p.pubsub.subscribe(options.transportTopic);
        }

        this._eventHandler = this._localStore
            ? async (evt: CustomEvent<Message>) => {
                  if (!evt) {
                      return;
                  }
                  const message = evt.detail;
                  if (
                      message.type === "signed" &&
                      message.topic === this._transportTopic
                  ) {
                      if (message.from.equals(libp2p.peerId)) {
                          return;
                      }

                      try {
                          const decoded = deserialize(
                              message.data,
                              BlockMessage
                          );
                          if (decoded instanceof BlockRequest) {
                              const cid = stringifyCid(decoded.cid);
                              const block = await this._localStore!.get<any>(
                                  cid,
                                  {
                                      timeout: options.localTimeout,
                                  }
                              );
                              if (!block) {
                                  return;
                              }
                              const message = serialize(
                                  new BlockResponse(cid, block.bytes)
                              );
                              await libp2p.pubsub.publish(
                                  options.transportTopic,
                                  message
                              );
                          } else if (decoded instanceof BlockResponse) {
                              // TODO make sure we are not storing too much bytes in ram (like filter large blocks)
                              this._gossipCache?.set(
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
        if (!this._localStore) {
            throw new Error("Local store not set");
        }
        const hasTopic = this._libp2p.pubsub
            .getTopics()
            .indexOf(this._transportTopic);
        if (!hasTopic) {
            this._libp2p.pubsub.subscribe(this._transportTopic);
        }
        this._libp2p.pubsub.addEventListener("message", this._eventHandler!);
        await this._localStore.open();
    }

    async close(): Promise<void> {
        this._libp2p.pubsub.removeEventListener("message", this._eventHandler);
        await this._localStore?.close();

        // we dont cleanup subscription because we dont know if someone else is sbuscribing also
    }

    /*   async _read<T>(
          cid: string,
          options: { timeout?: number, hasher?: any } = {}
      ): Promise<Block.Block<T, any, any, any> | undefined> {
          const promises = [this._readFromPubSub(cid, options)];
          try {
              const result = await Promise.any(promises);
              if (!result) {
                  const results = await Promise.all(promises);
                  for (const result of results) {
                      if (result) {
                          return result as Block.Block<T, any, any, any>;
                      }
                  }
              }
              return result as Block.Block<T, any, any, any>;
          } catch (error) {
              return undefined; // failed to resolve
          }
      } */

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
        const timeout = options.timeout || 5000;
        const codec = codecCodes[cidObject.code];
        let value: Block.Block<any, any, any, 1> | undefined = undefined;
        // await libp2p.pubsub.subscribe(BLOCK_TRANSPORT_TOPIC) TODO
        const eventHandler = async (evt) => {
            if (value) {
                return;
            }
            const message = evt.detail;
            if (
                message.type === "signed" &&
                message.topic === this._transportTopic
            ) {
                if (message.from.equals(this._libp2p.peerId)) {
                    return;
                }

                const decoded = deserialize(message.data, BlockMessage);
                if (decoded instanceof BlockResponse) {
                    if (decoded.cid !== cidString) {
                        return;
                    }
                    try {
                        if (!cidObject.equals(cidifyString(decoded.cid))) {
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
                    } catch (error) {
                        // invalid bytes like "CBOR decode error: not enough data for type"
                        return;
                    }
                }
            }
        };
        this._libp2p.pubsub.addEventListener("message", eventHandler);
        try {
            await this._libp2p.pubsub.publish(
                this._transportTopic,
                serialize(new BlockRequest(cidString))
            );

            try {
                await waitFor(() => value !== undefined, {
                    timeout,
                    delayInterval: 100,
                });
            } catch (error) {
                /// TODO, timeout or?
                const t = 123;
            } finally {
                await this._libp2p.pubsub.removeEventListener(
                    "message",
                    eventHandler
                );
            }
        } catch (error) {
            return;
        }
        return value;
    }
}
