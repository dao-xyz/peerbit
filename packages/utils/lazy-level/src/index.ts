import { logger as loggerFn } from "@peerbit/logger";
import { waitFor } from "@peerbit/time";
import { AbstractBatchOperation, AbstractLevel } from "abstract-level";

export type LevelBatchOptions = {
	interval: number;
	onError?: (error: any) => void;
};
export type LazyLevelOptions = { batch?: LevelBatchOptions };

const logger = loggerFn({ module: "cache" });

// TODO make SimpleLevel = AbstractLevel
export interface SimpleLevel {
	get status(): "opening" | "open" | "closing" | "closed";
	close(): Promise<void>;
	open(): Promise<void>;
	get(key: string): Promise<Uint8Array | undefined>;
	put(key: string, value: Uint8Array);
	del(key): Promise<void>;
	sublevel(name: string): SimpleLevel;
	clear(): Promise<void>;
	idle?(): Promise<void>;
}

export default class LazyLevel implements SimpleLevel {
	private _store: AbstractLevel<any, any, any>;
	private _interval: any;
	private _txQueue?: AbstractBatchOperation<
		AbstractLevel<any, string, Uint8Array>,
		string,
		Uint8Array
	>[];
	private _tempStore?: Map<string, Uint8Array>;
	private _tempDeleted?: Set<string>;
	private _txPromise?: Promise<void>;
	private _opts?: LazyLevelOptions;
	private _sublevels: LazyLevel[] = [];

	constructor(
		store: AbstractLevel<any, any, any>,
		opts: LazyLevelOptions | { batch: boolean } = { batch: { interval: 300 } }
	) {
		this._store = store;
		if (typeof opts.batch === "boolean") {
			if (opts.batch === true) this._opts = { batch: { interval: 300 } };
		} else if (opts) {
			this._opts = { batch: { interval: 300, ...opts.batch }, ...opts };
		}
	}

	get status() {
		return this._store.status;
	}

	async idle() {
		if (this._opts?.batch && this._txQueue) {
			if (
				this._store.status !== "open" &&
				this._store.status !== "opening" &&
				this._txQueue &&
				this._txQueue.length > 0
			) {
				throw new Error("Store is closed, so cache will never finish idling");
			}
			await this._txPromise;
			await waitFor(() => !this._txQueue || this._txQueue.length === 0, {
				timeout: this._opts.batch.interval * 2 + 1000, // TODO, do this better so tests don't fail in slow envs.
				delayInterval: 100,
				timeoutMessage: `Failed to wait for idling, got txQueue with ${
					this._txQueue?.length
				} elements. Store status: ${
					this._store?.status
				}, interval exist: ${!!this._interval}`,
			});
		}
	}
	async close() {
		if (!this._store)
			return Promise.reject(new Error("No cache store found to close"));

		await this.idle(); // idle after clear interval (because else txQueue might be filled with new things that are never removed)
		if (this._opts?.batch) {
			clearInterval(this._interval);
			this._interval = undefined;
			this._tempStore?.clear();
			this._tempDeleted?.clear();
		}
		await Promise.all(this._sublevels.map((l) => l.close()));

		if (this.status !== "closed" && this.status !== "closing") {
			await this._store.close();
			return Promise.resolve();
		}
	}

	async open() {
		if (!this._store)
			return Promise.reject(new Error("No cache store found to open"));

		if (this._opts?.batch && !this._interval) {
			this._txQueue = [];
			this._tempStore = new Map();
			this._tempDeleted = new Set();
			this._interval = setInterval(() => {
				if (
					this._store.status === "open" &&
					this._txQueue &&
					this._txQueue.length > 0
				) {
					const arr = this._txQueue.splice(0, this._txQueue.length);
					if (arr?.length > 0) {
						const next = () =>
							this._store
								.batch(arr, { valueEncoding: "view" })
								.then(() => {
									arr.forEach((v) => {
										if (v.type === "put") {
											this._tempDeleted?.delete(v.key);
											this._tempStore!.delete(v.key);
										} else if (v.type === "del") {
											this._tempDeleted?.delete(v.key);
											this._tempStore!.delete(v.key);
										}
									});
								})
								.catch((error) => {
									if (this._opts?.batch?.onError) {
										this._opts?.batch.onError(error);
									} else {
										logger.error(error);
									}
								});
						this._txPromise = (
							this._txPromise ? this._txPromise : Promise.resolve()
						)
							.then(next)
							.catch(next);
					}
				}
			}, this._opts.batch.interval);
		}

		if (this.status !== "open") {
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
			if (this._tempDeleted) {
				// batching is activated
				if (this._tempDeleted.has(key)) {
					return undefined;
				}
				data =
					(this._tempStore && this._tempStore.get(key)) ||
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
			valueEncoding: "view",
		});
		const ret: Uint8Array[] = [];
		for await (const [_key, value] of iterator) {
			ret.push(value);
		}

		return ret;
	}

	async clear(): Promise<void> {
		this._tempStore?.clear();
		this._tempDeleted?.clear();
		return this._store.clear();
	}

	/* 	async deleteByPrefix(prefix: string): Promise<void> {
			const iterator = this._store.iterator<any, Uint8Array>({
				gte: prefix,
				lte: prefix + "\xFF",
				valueEncoding: "view",
			});
			const keys: string[] = [];
			for await (const [key, _value] of iterator) {
				keys.push(key);
			}
	
			if (this._tempStore) {
				for (const key of this._tempStore.keys()) {
					if (key.startsWith(prefix)) {
						keys.push(key);
					}
				}
			}
			return this.delAll(keys);
		} */

	put(key: string, value: Uint8Array) {
		if (this._opts?.batch) {
			this._tempDeleted!.delete(key);
			this._tempStore!.set(key, value);
			this._txQueue!.push({
				type: "put",
				key: key,
				value: value,
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

		if (this._opts?.batch) {
			this._tempDeleted!.add(key);
			this._txQueue!.push({ type: "del", key: key });
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

	sublevel(name: string) {
		const l = new LazyLevel(this._store.sublevel(name), this._opts);
		this._sublevels.push(l);
		return l;
	}
}
