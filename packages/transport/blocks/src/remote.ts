import { field, variant } from "@dao-xyz/borsh";
import type { PeerId } from "@libp2p/interface";
import { TypedEventEmitter } from "@libp2p/interface";
import {
	type GetOptions,
	type Blocks as IBlocks,
	checkDecodeBlock,
	cidifyString,
	codecCodes,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { PublicSignKey } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import { AbortError } from "@peerbit/time";
import { CID } from "multiformats";
import { type Block } from "multiformats/block";
import PQueue from "p-queue";
import { AnyBlockStore } from "./any-blockstore.js";
import type { BlockStore } from "./interface.js";

export const logger = loggerFn({ module: "blocks-remote" });

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

export class RemoteBlocks implements IBlocks {
	localStore: BlockStore;

	private _responseHandler?: (data: BlockMessage) => any;
	private _resolvers: Map<string, (data: Uint8Array) => void>;
	private _loadFetchQueue: PQueue;
	private _readFromPeersPromises: Map<
		string,
		Promise<Block<any, any, any, 1> | undefined> | undefined
	>;
	_open = false;
	private _events: TypedEventEmitter<{
		"peer:reachable": CustomEvent<PublicSignKey>;
	}>;
	private closeController: AbortController;

	constructor(
		readonly options: {
			local: AnyBlockStore;
			localTimeout?: number;
			messageProcessingConcurrency?: number;
			publish: (
				message: BlockRequest | BlockResponse,
				options?: { to?: string[] },
			) => Promise<Uint8Array | void>;
			waitFor(peer: PeerId | PublicSignKey): Promise<void>;
		},
	) {
		const localTimeout = options?.localTimeout || 1000;
		this._loadFetchQueue = new PQueue({
			concurrency: options?.messageProcessingConcurrency || 10,
		});
		this.localStore = options?.local;
		this._resolvers = new Map();
		this._readFromPeersPromises = new Map();

		this._responseHandler = async (message: BlockMessage) => {
			try {
				if (message instanceof BlockRequest && this.localStore) {
					this._loadFetchQueue.add(() =>
						this.handleFetchRequest(message, localTimeout),
					);
				} else if (message instanceof BlockResponse) {
					// TODO make sure we are not storing too much bytes in ram (like filter large blocks)

					this._resolvers.get(message.cid)?.(message.bytes);
				}
			} catch (error) {
				logger.error("Got error for libp2p block transport: ", error);
				// timeout o r invalid cid
			}
		};
	}

	async put(bytes: Uint8Array): Promise<string> {
		if (!this.localStore) {
			throw new Error("Local store not set");
		}
		return this.localStore!.put(bytes);
	}

	async has(cid: string) {
		return this.localStore.has(cid);
	}
	async get(
		cid: string,
		options?: GetOptions | undefined,
	): Promise<Uint8Array | undefined> {
		const cidObject = cidifyString(cid);
		let value = this.localStore
			? await this.localStore.get(cid, options)
			: undefined;

		if (!value) {
			// try to get it remotelly
			let remoteOptions = options?.remote === true ? {} : options?.remote;
			if (remoteOptions) {
				value = await this._readFromPeers(cid, cidObject, remoteOptions);
				if (remoteOptions?.replicate && value) {
					await this.localStore!.put(value);
				}
			}
		}
		return value;
	}

	async rm(cid: string) {
		await this.localStore?.rm(cid);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for await (const [key, value] of this.localStore.iterator()) {
			yield [key, value];
		}
	}

	async start(): Promise<void> {
		this._events = new TypedEventEmitter();
		this.closeController = new AbortController();
		await this.localStore?.start();
		this._open = true;
	}

	onMessage(data: BlockMessage) {
		return this._responseHandler!(data);
	}
	onReachable(publicKey: PublicSignKey) {
		this._events.dispatchEvent(
			new CustomEvent("peer:reachable", { detail: publicKey }),
		);
	}

	private async handleFetchRequest(
		request: BlockRequest,
		localTimeout: number,
	) {
		const cid = stringifyCid(request.cid);
		const bytes = await this.localStore.get(cid, {
			remote: {
				timeout: localTimeout,
			},
		});
		if (!bytes) {
			return;
		}
		await this.options.publish(new BlockResponse(cid, bytes));
	}

	private async _readFromPeers(
		cidString: string,
		cidObject: CID,
		options: {
			signal?: AbortSignal;
			timeout?: number;
			hasher?: any;
			from?: string[];
		} = {},
	): Promise<Uint8Array | undefined> {
		const codec = (codecCodes as any)[cidObject.code];
		let promise = this._readFromPeersPromises.get(cidString);
		if (!promise) {
			promise = new Promise<Block<any, any, any, 1> | undefined>(
				(resolve, reject) => {
					const timeoutCallback = setTimeout(
						() => {
							resolve(undefined);
						},
						options.timeout || 30 * 1000,
					);
					const abortHandler = () => {
						clearTimeout(timeoutCallback);
						this._resolvers.delete(cidString);
						this.closeController.signal.removeEventListener(
							"abort",
							abortHandler,
						);
						options?.signal?.removeEventListener("abort", abortHandler);
						reject(new AbortError());
					};
					this.closeController.signal.addEventListener("abort", abortHandler);
					options?.signal?.addEventListener("abort", abortHandler);

					this._resolvers.set(cidString, async (bytes: Uint8Array) => {
						const value = await checkDecodeBlock(cidObject, bytes, {
							codec,
							hasher: options?.hasher,
						});

						clearTimeout(timeoutCallback);
						this._resolvers.delete(cidString); // TODO concurrency might not work as expected here
						this.closeController.signal.removeEventListener(
							"abort",
							abortHandler,
						);
						resolve(value);
					});
				},
			);

			this._readFromPeersPromises.set(cidString, promise);

			const publish = (to: string) => {
				if (!options?.from || options.from.includes(to)) {
					return this.options.publish(new BlockRequest(cidString), {
						to: [to],
					});
				}
			};

			const publishOnNewPeers = (e: CustomEvent<PublicSignKey>) => {
				return publish(e.detail.hashcode());
			};
			this._events.addEventListener("peer:reachable", publishOnNewPeers);
			this.options.publish(new BlockRequest(cidString), {
				to: options.from,
			});

			// we want to make sure that if some new peers join, we also try to ask them

			const result = await promise;
			this._readFromPeersPromises.delete(cidString);

			// stop asking new peers, because we already got an response
			this._events.removeEventListener("peer:reachable", publishOnNewPeers);
			return result?.bytes;
		} else {
			const result = await promise;
			return result?.bytes;
		}
	}

	async stop(): Promise<void> {
		// Dont listen for more incoming messages

		// Wait for processing request
		this.closeController.abort();
		this._loadFetchQueue.clear();
		await this._loadFetchQueue.onIdle(); // wait for pending
		await this.localStore?.stop();
		this._readFromPeersPromises.clear();
		this._resolvers.clear();
		this._open = false;
		// we dont cleanup subscription because we dont know if someone else is sbuscribing also
	}

	waitFor(peer: PeerId | PublicSignKey): Promise<void> {
		return this.options.waitFor(peer);
	}

	async size() {
		return this.localStore.size();
	}

	get status() {
		if (this._open) {
			return this.localStore?.status();
		} else {
			return "closed";
		}
	}

	persisted(): boolean | Promise<boolean> {
		return this.localStore?.persisted() || false;
	}
}
