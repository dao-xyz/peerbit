import { logger as loggerFn } from "@peerbit/logger";
import { waitFor } from "@peerbit/time";
import { AbstractBatchOperation, AbstractLevel } from "abstract-level";
import PQueue from "p-queue";

export type LevelBatchOptions = {
	interval: number;
	queueMaxBytes: number;
	onError?: (error: any) => void;
};
export type LazyLevelOptions = { batch?: LevelBatchOptions };

const logger = loggerFn({ module: "cache" });

// TODO make SimpleLevel = AbstractLevel
type MaybePromise<T> = Promise<T> | T;
export interface SimpleLevel {
	status(): MaybePromise<"opening" | "open" | "closing" | "closed">;
	close(): Promise<void>;
	open(): Promise<void>;
	get(key: string): Promise<Uint8Array | undefined>;
	put(key: string, value: Uint8Array);
	del(key): Promise<void>;
	sublevel(name: string): MaybePromise<SimpleLevel>;
	iterator: () => {
		[Symbol.asyncIterator]: () => AsyncIterator<
			[string, Uint8Array],
			void,
			void
		>;
	};
	clear(): Promise<void>;
	idle?(): Promise<void>;
}

const DEFAULT_BATCH_INTERVAL = 300;
const DEFAULT_MAX_BATCH_SIZE = 5 * 10 ** 6;
const DEFAULT_BATCH_OPTIONS: LevelBatchOptions = {
	interval: DEFAULT_BATCH_INTERVAL,
	queueMaxBytes: DEFAULT_MAX_BATCH_SIZE
};

const DELETE_TX_SIZE = 50; // experimental memory consumption

class TXQueue {
	queue: AbstractBatchOperation<
		AbstractLevel<any, string, Uint8Array>,
		string,
		Uint8Array
	>[];
	currentSize = 0;

	promiseQueue: PQueue;
	private _interval?: ReturnType<typeof setInterval>;

	tempStore: Map<string, Uint8Array>;
	tempDeleted: Set<string>;

	constructor(
		readonly opts: LevelBatchOptions,
		readonly store: AbstractLevel<any, any, any>
	) {}

	open() {
		this.queue = [];

		// TODO can we prevent re-open?
		this.tempStore = this.tempStore || new Map();
		this.tempDeleted = this.tempDeleted || new Set();
		this.promiseQueue = new PQueue();
		this._interval =
			this._interval ||
			setInterval(() => {
				this.processTxQueue();
			}, this.opts.interval);
	}

	async add(
		tx: AbstractBatchOperation<
			AbstractLevel<any, string, Uint8Array>,
			string,
			Uint8Array
		>
	) {
		let size: number;
		if (tx.type === "put") {
			this.tempDeleted.delete(tx.key);
			this.tempStore.set(tx.key, tx.value);
			size = tx.value.byteLength;
		} else if (tx.type == "del") {
			size = DELETE_TX_SIZE;
			this.tempDeleted.add(tx.key);
		} else {
			throw new Error("Unexpected tx type: " + tx["type"]);
		}

		this.queue.push(tx);
		this.currentSize += size;
		if (this.currentSize >= this.opts.queueMaxBytes) {
			await this.processTxQueue();
		}
	}

	async processTxQueue() {
		if (this.store.status === "open" && this.currentSize > 0) {
			const arr = this.queue.splice(0, this.queue.length);
			if (arr?.length > 0) {
				// We manipulate sizes before finishing the tx so that subsequent calls to process processTxQueue end up here because invalid this.currentSize calculations
				for (const v of arr) {
					if (v.type === "put") {
						this.currentSize -= v.value.byteLength;
					} else if (v.type === "del") {
						this.currentSize -= DELETE_TX_SIZE;
					}
				}

				this.promiseQueue.add(() =>
					this.store
						.batch(arr, { valueEncoding: "view" })
						.then(() => {
							arr.forEach((v) => {
								if (v.type === "put") {
									this.tempDeleted?.delete(v.key);
									this.tempStore!.delete(v.key);
								} else if (v.type === "del") {
									this.tempDeleted?.delete(v.key);
									this.tempStore!.delete(v.key);
								}
							});
						})
						.catch((error) => {
							if (this.opts.onError) {
								this.opts.onError(error);
							} else {
								logger.error(error);
							}
						})
				);
			}
		}
	}

	async idle() {
		if (
			this.store.status !== "open" &&
			this.store.status !== "opening" &&
			this.queue &&
			this.queue.length > 0
		) {
			throw new Error("Store is closed, so cache will never finish idling");
		}
		await this.promiseQueue.onIdle();
		await waitFor(() => !this.queue || this.queue.length === 0, {
			timeout: this.opts.interval * 2 + 1000, // TODO, do this better so tests don't fail in slow envs.
			delayInterval: 100,
			timeoutMessage: `Failed to wait for idling, got txQueue with ${this.queue
				?.length} elements. Store status: ${this.store
				?.status}, interval exist: ${!!this._interval}`
		});
	}

	clear() {
		this.queue = [];
		this.tempStore.clear();
		this.tempDeleted.clear();
	}

	async close() {
		await this.idle();
		clearInterval(this._interval);
		this.clear();
		this._interval = undefined;
	}
}
export default class LazyLevel implements SimpleLevel {
	private _store: AbstractLevel<any, any, any>;

	private _opts?: LazyLevelOptions;
	private _sublevels: LazyLevel[] = [];
	txQueue?: TXQueue;

	constructor(
		store: AbstractLevel<any, any, any>,
		opts: LazyLevelOptions | { batch: boolean } = {
			batch: DEFAULT_BATCH_OPTIONS
		}
	) {
		this._store = store;
		if (typeof opts.batch === "boolean") {
			if (opts.batch === true) this._opts = { batch: DEFAULT_BATCH_OPTIONS };
		} else if (opts) {
			this._opts = {
				batch: { ...DEFAULT_BATCH_OPTIONS, ...opts.batch },
				...opts
			};
		}
	}

	status() {
		return this._store.status;
	}

	async idle() {
		await this.txQueue?.idle();
	}

	async close() {
		if (!this._store) {
			return Promise.reject(new Error("No cache store found to close"));
		}

		if (this.txQueue) {
			await this.txQueue.close();
		}
		await Promise.all(this._sublevels.map((l) => l.close()));

		if (this.status() !== "closed" && this.status() !== "closing") {
			await this._store.close();
			return Promise.resolve();
		}
	}

	async open() {
		if (!this._store)
			return Promise.reject(new Error("No cache store found to open"));

		if (this._opts?.batch) {
			(
				this.txQueue ||
				(this.txQueue = new TXQueue(this._opts.batch, this._store))
			).open();
		}

		if (this.status() !== "open") {
			await this._store.open();
			return Promise.resolve();
		}
	}

	async get(key: string): Promise<Uint8Array | undefined> {
		if (this._store.status !== "open") {
			throw new Error("Cache store not open: " + this._store.status);
		}
		let data: Uint8Array;
		try {
			if (this.txQueue) {
				// batching is activated
				if (this.txQueue.tempDeleted.has(key)) {
					return undefined;
				}
				data =
					(this.txQueue.tempStore && this.txQueue.tempStore.get(key)) ||
					(await this._store.get(key, { valueEncoding: "view" }));
			} else {
				data = await this._store.get(key, { valueEncoding: "view" });
			}
		} catch (err: any) {
			if (err.notFound) {
				return undefined;
			}
			throw err;
		}

		return data;
	}

	async getByPrefix(prefix: string): Promise<Uint8Array[]> {
		if (this._store.status !== "open") {
			throw new Error("Cache store not open: " + this._store.status);
		}

		const iterator = this._store.iterator<any, Uint8Array>({
			gte: prefix,
			lte: prefix + "\xFF",
			valueEncoding: "view"
		});
		const ret: Uint8Array[] = [];
		for await (const [_key, value] of iterator) {
			ret.push(value);
		}

		return ret;
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		const iterator = this._store.iterator<any, Uint8Array>({
			valueEncoding: "view"
		});
		for await (const [key, value] of iterator) {
			yield [key, value];
		}
	}

	async clear(clearStore = true): Promise<void> {
		this.txQueue?.clear();
		await this.idle();
		if (clearStore) {
			await this._store.clear(); // will also clear sublevels
		}
		await Promise.all(this._sublevels.map((x) => x.clear(false))); // so we pass false flag here
	}

	async deleteByPrefix(prefix: string): Promise<void> {
		const iterator = this._store.iterator<any, Uint8Array>({
			gte: prefix,
			lte: prefix + "\xFF",
			valueEncoding: "view"
		});
		const keys: string[] = [];
		for await (const [key, _value] of iterator) {
			keys.push(key);
		}

		if (this.txQueue) {
			for (const [key] of this.txQueue.tempStore) {
				if (key.startsWith(prefix)) {
					keys.push(key);
				}
			}
		}
		return this.delAll(keys);
	}
	async put(key: string, value: Uint8Array) {
		if (this.txQueue) {
			await this.txQueue.add({
				type: "put",
				key: key,
				value: value
			});
		} else {
			return this._store.put(key, value, { valueEncoding: "view" });
		}
	}

	// Remove a value and key from the cache
	async del(key: string) {
		if (this._store.status !== "open") {
			throw new Error("Cache store not open: " + this._store.status);
		}

		if (this.txQueue) {
			this.txQueue.add({ type: "del", key: key });
		} else {
			return new Promise<void>((resolve, reject) => {
				this._store.del(key, (err) => {
					if (err) {
						// Ignore error if key was not found
						if (
							err
								.toString()
								.indexOf("NotFoundError: Key not found in database") === -1 &&
							err.toString().indexOf("NotFound") === -1
						) {
							return reject(err);
						}
					}
					resolve();
				});
			});
		}
	}

	async delAll(keys: string[]) {
		for (const key of keys) {
			await this.del(key);
		}
	}

	async sublevel(name: string) {
		const l = new LazyLevel(this._store.sublevel(name), this._opts);
		if (this.status() === "open" || this.status() === "opening") {
			await l.open();
		}
		this._sublevels.push(l);
		return l;
	}
}
