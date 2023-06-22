import { BlockStore } from "./store.js";
import { Blocks as IBlocks } from "@peerbit/blocks-interface";
import {
	stringifyCid,
	cidifyString,
	codecCodes,
	checkDecodeBlock,
} from "./block.js";
import { variant, field, serialize, deserialize } from "@dao-xyz/borsh";
import { CID } from "multiformats/cid";
import { DataMessage } from "@peerbit/stream-interface";
import { DirectStream } from "@peerbit/stream";

import * as Block from "multiformats/block";
import { PublicSignKey } from "@peerbit/crypto";
import { DirectStreamComponents } from "@peerbit/stream";
import { LevelBlockStore, MemoryLevelBlockStore } from "./level.js";
import { Level } from "level";
import { GetOptions, PutOptions } from "@peerbit/blocks-interface";

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
export class DirectBlock extends DirectStream implements IBlocks {
	private _localStore: BlockStore;
	private _responseHandler?: (evt: CustomEvent<DataMessage>) => any;
	private _resolvers: Map<string, (data: Uint8Array) => void>;
	private _readFromPeersPromises: Map<
		string,
		Promise<Block.Block<any, any, any, 1> | undefined> | undefined
	>;
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
					const bytes = await this._localStore.get(cid, {
						timeout: localTimeout,
					});
					if (!bytes) {
						return;
					}
					const response = serialize(new BlockResponse(cid, bytes));
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

	async put(
		bytes: Uint8Array,
		options?: PutOptions | undefined
	): Promise<string> {
		if (!this._localStore) {
			throw new Error("Local store not set");
		}
		return this._localStore!.put(bytes, options);
	}

	async has(cid: string) {
		return this._localStore.has(cid);
	}
	async get(
		cid: string,
		options?: GetOptions | undefined
	): Promise<Uint8Array | undefined> {
		const cidObject = cidifyString(cid);
		let value = this._localStore
			? await this._localStore.get(cid, options)
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

	async start(): Promise<void> {
		await this._localStore?.start();
		await super.start();
		this.addEventListener("data", this._responseHandler!);
		this._open = true;
	}

	private async _readFromPeers(
		cidString: string,
		cidObject: CID,
		options: { timeout?: number; hasher?: any } = {}
	): Promise<Uint8Array | undefined> {
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

			const publishOnNewPeers = (e: CustomEvent<PublicSignKey>) => {
				return publish([e.detail]);
			};

			this._readFromPeersPromises.set(cidString, promise);

			// we want to make sure that if some new peers join, we also try to ask them
			this.addEventListener("peer:reachable", publishOnNewPeers);

			const result = await promise;
			this._readFromPeersPromises.delete(cidString);

			// stop asking new peers, because we already got an response
			this.removeEventListener("peer:reachable", publishOnNewPeers);
			return result?.bytes;
		} else {
			const result = await promise;
			return result?.bytes;
		}
	}

	async stop(): Promise<void> {
		await super.stop();
		this.removeEventListener("data", this._responseHandler);
		await this._localStore?.stop();
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
