import { BlockStore, PutOptions } from "./store.js";
import {
	cidifyString,
	codecCodes,
	defaultHasher,
	stringifyCid,
} from "./block.js";
import * as Block from "multiformats/block";
import { AbstractBatchOperation, AbstractLevel } from "abstract-level";
import { MemoryLevel } from "memory-level";
import { waitFor } from "@dao-xyz/peerbit-time";
import { ByteView } from "@ipld/dag-cbor";

export type LevelBatchOptions = {
	interval?: number;
	onError?: (error: any) => void;
};
export type LevelBlockStoreOptions = { batch: LevelBatchOptions | true };
export class LevelBlockStore implements BlockStore {
	_level: AbstractLevel<any, string, Uint8Array>;
	_opening: Promise<any>;
	_closed = false;
	_onClose: (() => any) | undefined;
	_batchOptions?: { interval: number; onError?: (e: any) => void };
	_interval: any;
	_txQueue: AbstractBatchOperation<
		AbstractLevel<any, string, Uint8Array>,
		string,
		Uint8Array
	>[];
	_tempStore: Map<string, ByteView<any>>;
	_txPromise: Promise<void>;

	constructor(
		level: AbstractLevel<any, string, Uint8Array>,
		options?: LevelBlockStoreOptions
	) {
		this._level = level;
		if (options?.batch) {
			this._batchOptions =
				typeof options.batch === "boolean"
					? {
							interval: 300,
					  }
					: {
							interval: options.batch.interval || 100,
							onError: options.batch.onError,
					  };
			if (this._batchOptions!.interval <= 0) {
				throw new Error(
					"Batch interval needs to be greater than 0 or undefined"
				);
			}
		}
	}

	async get<T>(
		cid: string,
		options?: {
			raw?: boolean;
			links?: string[];
			timeout?: number;
			hasher?: any;
		}
	): Promise<Block.Block<T, any, any, any> | undefined> {
		const cidObject = cidifyString(cid);
		try {
			const bytes =
				(this._tempStore && this._tempStore.get(cid)) ||
				(await this._level.get(cid, { valueEncoding: "view" }));
			if (!bytes) {
				return undefined;
			}
			const codec = codecCodes[cidObject.code];
			const block = await Block.decode({
				bytes,
				codec,
				hasher: options?.hasher || defaultHasher,
			});
			return block as Block.Block<T, any, any, any>;
		} catch (error: any) {
			if (
				typeof error?.code === "string" &&
				error?.code?.indexOf("LEVEL_NOT_FOUND") !== -1
			) {
				return undefined;
			}
			throw error;
		}
	}

	async put<T>(
		block: Block.Block<T, any, any, any>,
		options?: PutOptions
	): Promise<string> {
		const cid = stringifyCid(block.cid);
		const bytes = block.bytes;

		if (this._batchOptions) {
			this._tempStore.set(cid, bytes);
			this._txQueue.push({
				type: "put",
				key: cid,
				value: bytes,
				valueEncoding: "view",
			});
		} else {
			await this._level.put(cid, bytes, {
				valueEncoding: "view",
			});
		}

		return cid;
	}

	async rm(cid: string): Promise<void> {
		if (this._batchOptions) {
			this._txQueue.push({ type: "del", key: cid });
		} else {
			await this._level.del(cid);
		}
	}

	async open(): Promise<void> {
		this._closed = false;
		if (this._level.status !== "opening" && this._level.status !== "open") {
			await this._level.open();
		}

		if (this._batchOptions) {
			this._txQueue = [];
			this._tempStore = new Map();

			this._interval = setInterval(() => {
				if (this._level.status === "open" && this._txQueue.length > 0) {
					try {
						const arr = this._txQueue.splice(0, this._txQueue.length);
						if (arr?.length > 0) {
							this._txPromise = (
								this._txPromise ? this._txPromise : Promise.resolve()
							).finally(() => {
								return this._level.batch(arr).then(() => {
									arr.forEach((v) => {
										if (v.type === "put") {
											this._tempStore.delete(v.key);
										} else if (v.type === "del") {
											this._tempStore.delete(v.key);
										}
									});
								});
							});
						}
					} catch (error) {
						this._batchOptions?.onError && this._batchOptions.onError(error);
					}
				}
			}, this._batchOptions.interval);
		}

		try {
			this._opening = waitFor(() => this._level.status === "open", {
				delayInterval: 100,
				timeout: 10 * 1000,
				stopperCallback: (fn) => {
					this._onClose = fn;
				},
			});
			await this._opening;
		} catch (error) {
			if (this._closed) {
				return;
			}
			throw error;
		} finally {
			this._onClose = undefined;
		}
	}

	async close(): Promise<void> {
		await this.idle();
		if (this._batchOptions) {
			clearInterval(this._interval);
			this._interval = undefined;
			this._tempStore.clear();
		}
		this._closed = true;
		this._onClose && this._onClose();
		return this._level.close();
	}

	async idle(): Promise<void> {
		if (this._txQueue) {
			await waitFor(() => this._txQueue.length === 0);
			await this._txPromise;
		}
	}

	get status() {
		if (this._batchOptions && !this._interval) {
			return "closed";
		}
		return this._level.status;
	}
}

export class MemoryLevelBlockStore extends LevelBlockStore {
	constructor(options?: LevelBlockStoreOptions) {
		super(new MemoryLevel({ valueEncoding: "view" }), options);
	}
}
