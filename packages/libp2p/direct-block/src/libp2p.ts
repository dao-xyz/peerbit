import { BlockStore, GetOptions, PutOptions } from "./store.js";
import { Libp2p } from "libp2p";
import {
	stringifyCid,
	cidifyString,
	codecCodes,
	checkDecodeBlock,
} from "./block.js";
import { variant, field, serialize, deserialize } from "@dao-xyz/borsh";
import { CID } from "multiformats/cid";
import { DirectStream, DataMessage } from "@dao-xyz/libp2p-direct-stream";
import * as Block from "multiformats/block";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { DirectStreamComponents } from "@dao-xyz/libp2p-direct-stream";
import { LevelBlockStore, MemoryLevelBlockStore } from "./level.js";
import { Level } from "level";

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

export type DirectBlockComponents = DirectStreamComponents;
export class DirectBlock extends DirectStream implements BlockStore {
	_localStore: BlockStore;
	_responseHandler?: (evt: CustomEvent<DataMessage>) => any;
	_resolvers: Map<string, (data: Uint8Array) => void>;
	_readFromPeersPromises: Map<
		string,
		Promise<Block.Block<any, any, any, 1> | undefined> | undefined
	>;
	_gossip = false;
	_open = false;

	constructor(
		components: DirectBlockComponents,
		options?: {
			directory?: string;
			canRelayMessage?: boolean;
			localTimeout?: number;
		}
	) {
		super(components, ["direct-block/1.0.0"], {
			emitSelf: false,
			signaturePolicy: "StrictNoSign",
			messageProcessingConcurrency: 10,
			canRelayMessage: options?.canRelayMessage ?? true,
		});

		const localTimeout = options?.localTimeout || 1000;
		this._localStore =
			options?.directory != null
				? new LevelBlockStore(new Level(options.directory))
				: new MemoryLevelBlockStore();
		this._resolvers = new Map();
		this._readFromPeersPromises = new Map();
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

	async has(cid: string) {
		return this._localStore.has(cid);
	}
	async get<T>(
		cid: string,
		options?: GetOptions | undefined
	): Promise<Block.Block<T, any, any, any> | undefined> {
		const cidObject = cidifyString(cid);
		let value = this._localStore
			? await this._localStore.get<T>(cid, options)
			: undefined;

		if (!value) {
			// try to get it remotelly
			value = await this._readFromPeers(cid, cidObject, options);
			if (options?.replicate && value) {
				this._localStore!.put(value, options);
			}
		}
		return value;
	}

	async rm(cid: string) {
		this._localStore?.rm(cid);
	}

	async open(): Promise<this> {
		await this.start();
		return this;
	}
	async start(): Promise<void> {
		await this._localStore?.open();
		await super.start();
		this.addEventListener("data", this._responseHandler!);
		this._open = true;
	}

	async _readFromPeers(
		cidString: string,
		cidObject: CID,
		options: { timeout?: number; hasher?: any } = {}
	): Promise<Block.Block<any, any, any, any> | undefined> {
		const codec = codecCodes[cidObject.code];
		let promise = this._readFromPeersPromises.get(cidString);
		if (!promise) {
			promise = new Promise<Block.Block<any, any, any, 1> | undefined>(
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
			const publish = (to?: PublicSignKey[]) =>
				this.publish(serialize(new BlockRequest(cidString)), { to: to });
			await publish();

			const publishOnNewPeers = (e: CustomEvent<PublicSignKey>) =>
				publish([e.detail]);

			this._readFromPeersPromises.set(cidString, promise);

			// we want to make sure that if some new peers join, we also try to ask them
			this.addEventListener("peer:reachable", publishOnNewPeers);

			const result = await promise;
			this._readFromPeersPromises.delete(cidString);

			// stop asking new peers, because we already got an response
			this.removeEventListener("peer:reachable", publishOnNewPeers);
			return result;
		} else {
			const result = await promise;
			return result;
		}
	}

	async close(): Promise<void> {
		return this.stop();
	}
	async stop(): Promise<void> {
		await super.stop();
		this.removeEventListener("data", this._responseHandler);

		await this._localStore?.close();
		this._readFromPeersPromises.clear();
		this._resolvers.clear();
		this._open = false;

		// we dont cleanup subscription because we dont know if someone else is sbuscribing also
	}

	get status() {
		if (this._open) {
			return this._localStore?.status || this.started;
		} else {
			return "closed";
		}
	}
}
