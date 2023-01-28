import { BlockStore, GetOptions, PutOptions } from "./store.js";
import { Libp2p } from "libp2p";
import {
	stringifyCid,
	cidifyString,
	codecCodes,
	checkDecodeBlock,
} from "./block.js";
import { variant, field, serialize, deserialize } from "@dao-xyz/borsh";
import LRU from "lru-cache";
import { CID } from "multiformats/cid";
import { DirectStream, DataMessage } from "@dao-xyz/libp2p-direct-stream";
import * as Block from "multiformats/block";
import { delay } from "@dao-xyz/peerbit-time";

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

export class DirectBlock extends DirectStream implements BlockStore {
	_libp2p: Libp2p;
	_localStore?: BlockStore;
	_responseHandler?: (evt: CustomEvent<DataMessage>) => any;
	_resolvers: Map<string, (data: Uint8Array) => void>;
	_gossipCache?: LRU<string, Uint8Array>;
	_gossip = false;
	_open = false;

	constructor(
		libp2p: Libp2p,
		options?: {
			canRelayBlocks?: boolean;
			localStore?: BlockStore;
			transportTopic?: string;
			localTimeout?: number;
			gossip?: { cache: { max?: number; ttl?: number } | false };
		}
	) {
		super(libp2p, ["direct-block/1.0.0"], {
			emitSelf: false,
			signaturePolicy: "StrictNoSign",
			messageProcessingConcurrency: 10,
			canRelayMessage: options?.canRelayBlocks ?? true,
		});

		this._libp2p = libp2p;
		const localTimeout = options?.localTimeout || 1000;
		this._localStore = options?.localStore;

		if (options?.gossip) {
			this._gossip = true;
			const gossipCacheOptions =
				options.gossip?.cache !== false
					? {
							max: options.gossip?.cache.max || 1000,
							ttl: options.gossip?.cache.ttl || 10000,
					  }
					: undefined; // TODO choose default variables carefully
			this._gossipCache = gossipCacheOptions && new LRU(gossipCacheOptions);
		}

		this._resolvers = new Map();
		this._responseHandler = async (evt: CustomEvent<DataMessage>) => {
			if (!evt) {
				return;
			}
			const message = evt.detail;
			try {
				const decoded = deserialize(message.data, BlockMessage);
				if (decoded instanceof BlockRequest && this._localStore) {
					const cid = stringifyCid(decoded.cid);
					const block = await this._localStore.get<any>(cid, {
						timeout: localTimeout,
					});
					if (!block) {
						return;
					}
					const response = serialize(new BlockResponse(cid, block.bytes));
					await this.publish(response);
				} else if (decoded instanceof BlockResponse) {
					// TODO make sure we are not storing too much bytes in ram (like filter large blocks)
					this._gossipCache &&
						this._gossipCache.set(decoded.cid, decoded.bytes);

					this._resolvers.get(decoded.cid)?.(decoded.bytes);
				}
			} catch (error) {
				console.error("Got error for libp2p block transport: ", error);
				return; // timeout o r invalid cid
			}
		};
	}

	async put<T>(
		value: Block.Block<T, any, any, any>,
		options?: PutOptions | undefined
	): Promise<string> {
		if (!this._localStore) {
			throw new Error("Local store not set");
		}

		// "Gossip" i.e. flood the network with blocks an assume they gonna catch them so they dont have to requrest them later
		try {
			if (this._gossip)
				await this.publish(
					serialize(new BlockResponse(stringifyCid(value.cid), value.bytes))
				);
		} catch (error) {
			// ignore
		}
		return this._localStore!.put(value, options);
	}

	async get<T>(
		cid: string,
		options?: GetOptions | undefined
	): Promise<Block.Block<T, any, any, any> | undefined> {
		const cidObject = cidifyString(cid);
		let value =
			(await this._readFromGossip(cid, cidObject, options)) ||
			(this._localStore
				? await this._localStore.get<T>(cid, options)
				: undefined);

		if (!value) {
			// try to get it remotelly
			value = await this._readFromPeers(cid, cidObject, options);
		}
		return value;
	}

	async rm(cid: string) {
		this._localStore?.rm(cid);
		this._gossipCache?.delete(cid);
	}

	async open(): Promise<void> {
		return this.start();
	}
	async start(): Promise<void> {
		await super.start();
		this.addEventListener("data", this._responseHandler!);
		await this._localStore?.open();
		await delay(3000);
		this._open = true;
	}

	async _readFromGossip(
		cidString: string,
		cidObject: CID,
		options: { hasher?: any } = {}
	): Promise<Block.Block<any, any, any, any> | undefined> {
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
	async _readFromPeers(
		cidString: string,
		cidObject: CID,
		options: { timeout?: number; hasher?: any } = {}
	): Promise<Block.Block<any, any, any, any> | undefined> {
		const codec = codecCodes[cidObject.code];
		const promise = new Promise<Block.Block<any, any, any, 1> | undefined>(
			(resolve, reject) => {
				const timeoutCallback = setTimeout(() => {
					resolve(undefined);
				}, options.timeout || 30 * 1000);

				this._resolvers.set(cidString, async (bytes: Uint8Array) => {
					const value = await checkDecodeBlock(cidObject, bytes, {
						codec,
						hasher: options?.hasher,
					});
					clearTimeout(timeoutCallback);
					this._resolvers.delete(cidString); // TODO concurrency might not work as expected here
					resolve(value);
				});
			}
		);

		this.publish(serialize(new BlockRequest(cidString)));

		return promise;
	}

	async close(): Promise<void> {
		return this.stop();
	}
	async stop(): Promise<void> {
		await super.stop();
		this.removeEventListener("data", this._responseHandler);

		await this._localStore?.close();
		this._resolvers.clear();
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
