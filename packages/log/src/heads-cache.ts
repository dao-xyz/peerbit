import PQueue from "p-queue";
import { v4 as uuid } from "uuid";
import { Entry } from "./entry";
import { SimpleLevel } from "@peerbit/lazy-level";
import { variant, option, field, vec } from "@dao-xyz/borsh";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { logger as loggerFn } from "@peerbit/logger";
import path from "path-browserify";
export const logger = loggerFn({ module: "heads-cache" });
export class CachedValue {}
/* export type AppendOptions<T> = {
	signers?: ((data: Uint8Array) => Promise<SignatureWithKey>)[];
	nexts?: Entry<T>[];
	reciever?: EncryptionTemplateMaybeEncrypted;
	type?: EntryType;
};
 */

@variant(0)
export class CachePath {
	@field({ type: "string" })
	path: string;

	constructor(path: string) {
		this.path = path;
	}
}

@variant(0)
export class UnsfinishedReplication {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(opts?: { hashes: string[] }) {
		if (opts) {
			this.hashes = opts.hashes;
		}
	}
}

@variant(0)
export class HeadsCacheToSerialize {
	@field({ type: vec("string") })
	heads: string[];

	@field({ type: option("string") })
	last?: string;

	@field({ type: "u64" })
	counter: bigint;

	constructor(heads: string[], counter: bigint, last?: string) {
		this.heads = heads;
		this.last = last;
		this.counter = counter;
	}
}

const updateHashes = async (
	headCache: HeadsCache<any>,
	headsPath: string,
	lastCid: string | undefined,
	lastCounter: bigint,
	hashes: string[]
): Promise<{ counter: bigint; newPath: string }> => {
	const newHeadsPath = path.join(
		headsPath,
		String(headCache.headsPathCounter),
		uuid()
	);
	const counter = lastCounter + BigInt(hashes.length);
	await Promise.all([
		headCache.cache?.put(
			headsPath,
			serialize(new CachePath(newHeadsPath.toString()))
		),
		headCache.cache?.put(
			newHeadsPath,
			serialize(new HeadsCacheToSerialize(hashes, counter, lastCid))
		),
	]);
	return { counter, newPath: newHeadsPath };
};

interface HeadsIndex {
	id: Uint8Array;
	size: number;
	has(cid: string): boolean;
}
export class HeadsCache<T> /* implements Initiable<T>  */ {
	// An access controller that is note part of the store manifest, usefull for circular store -> access controller -> store structures
	headsPath: string;
	removedHeadsPath: string;
	initialized: boolean;

	private _headsPathCounter = 0;

	private _lastHeadsPath?: string;
	private _lastHeadsCount = 0n;

	private _lastRemovedHeadsPath?: string;
	private _lastRemovedHeadsCount = 0n;

	private _cache?: SimpleLevel;
	private _cacheWriteQueue?: PQueue<any, any>;

	private _loaded = false;
	private _index: HeadsIndex;

	constructor(index: HeadsIndex) {
		this._index = index;
	}

	get cache(): SimpleLevel | undefined {
		return this._cache;
	}

	get headsPathCounter(): number {
		return this._headsPathCounter;
	}

	async init(cache?: SimpleLevel): Promise<this> {
		if (this.initialized) {
			throw new Error("Already initialized");
		}

		this._cache = cache;

		// Set the options (we will use the topic property after thiis)
		await this._cache?.open();

		this.headsPath = "heads";
		this.removedHeadsPath = "heads_removed";

		await this.loadLastHeadsPath();

		// append and log-joins queue. Adding ops and joins to the queue
		// makes sure they get processed sequentially to avoid race conditions
		this._cacheWriteQueue = new PQueue({ concurrency: 1 });
		/* if (this._options.onOpen) {
			await this._options.onOpen();
		} */
		this.initialized = true;

		return this;
	}

	get loaded(): boolean {
		return this._loaded;
	}

	private async _updateCachedHeads(
		change: {
			added?: (Entry<T> | string)[];
			removed?: (Entry<T> | string)[];
		},
		reset?: boolean
	) {
		if (typeof reset !== "boolean" && change.added) {
			// Only reset all heads if loaded once, since we don't want too loose track of unloaded heads
			if (this._loaded && this._index.size <= change.added.length) {
				let addedIsAllHeads = true;
				for (const entry of change.added) {
					const hash = typeof entry === "string" ? entry : entry.hash;
					if (!this._index.has(hash)) {
						addedIsAllHeads = false;
					}
				}
				reset = addedIsAllHeads;
			} else {
				// added size < head size, meaning we have not rewritten all heads
				reset = false;
			}
		}

		// If 'reset' then dont keep references to old heads caches, assume new cache will fully describe all heads

		// TODO dont delete old before saving new
		if (reset) {
			await this.cache?.clear();
			this._lastHeadsPath = undefined;
			this._lastRemovedHeadsPath = undefined;
			this._lastHeadsCount = 0n;
			this._lastRemovedHeadsCount = 0n;
			this._headsPathCounter += 1;
		}

		if (change.added && change.added.length > 0) {
			const update = await updateHashes(
				this,
				this.headsPath,
				this._lastHeadsPath,
				this._lastHeadsCount,
				change.added.map((x) => (typeof x === "string" ? x : x.hash))
			);
			this._lastHeadsPath = update.newPath;
			this._lastHeadsCount = update.counter;
		}

		if (this._lastHeadsPath) {
			// only add removed heads if we actually have added heads, else these are pointless
			if (change.removed && change.removed.length > 0) {
				const update = await updateHashes(
					this,
					this.removedHeadsPath,
					this._lastRemovedHeadsPath,
					this._lastRemovedHeadsCount,
					change.removed.map((x) => (typeof x === "string" ? x : x.hash))
				);
				this._lastRemovedHeadsPath = update.newPath;
				this._lastRemovedHeadsCount = update.counter;
				if (
					update.counter > 0n &&
					2n * update.counter >= this._lastHeadsCount
				) {
					const resetToHeads = await this.getCachedHeads(
						this._lastHeadsPath,
						this._lastRemovedHeadsPath
					);
					await this._updateCachedHeads(
						{ added: resetToHeads, removed: [] },
						true
					);
				}
			}
		}
	}

	async idle(): Promise<void> {
		// Wait for the operations queue to finish processing
		// to make sure everything that all operations that have
		// been queued will be written to disk
		await this._cacheWriteQueue?.onIdle();
		await this._cache?.idle?.();
	}

	async getCachedHeads(
		lastHeadsPath: string | undefined = this._lastHeadsPath,
		lastRemovedHeadsPath: string | undefined = this._lastRemovedHeadsPath
	): Promise<string[]> {
		if (!this._cache) {
			return [];
		}
		const getHashes = async (
			start: string | undefined,
			filter?: Set<string>
		) => {
			const result: string[] = [];
			let next = start;
			while (next) {
				const cache = await this._cache
					?.get(next)
					.then((bytes) => bytes && deserialize(bytes, HeadsCacheToSerialize));
				next = cache?.last;
				cache?.heads.forEach((head) => {
					if (filter && filter.has(head)) {
						return;
					}

					result.push(head);
				});
			}
			return result;
		};

		const removedHeads = new Set(await getHashes(lastRemovedHeadsPath));
		const heads = await getHashes(lastHeadsPath, removedHeads);
		return heads; // Saved heads - removed heads
	}

	/* get logOptions(): LogOptions<T> {
		return {
			logId: this.id,
			trim: this._options.trim && {
				// I can trim if I am not a replicator of an entry

				...this._options.trim,
				filter: this.options.replicator && {
					canTrim: async (gid) => !(await this.options.replicator!(gid)),
					cacheId: this.options.replicatorsCacheId,
				},
			},
		};
	} */

	get closed() {
		return !this._cache || this._cache.status === "closed";
	}

	async close() {
		await this.idle();
		await this._cache?.close();
		this._loaded = false;
		this._lastHeadsPath = undefined;
		this._lastRemovedHeadsPath = undefined;
		this._lastRemovedHeadsCount = 0n;
		this._lastHeadsCount = 0n;
	}

	/**
	 * Drops a database and removes local data
	 */
	async drop() {
		this.initialized = false;

		if (!this._cache) {
			return; // already dropped
		}
		if (this._cache.status !== "open") {
			await this._cache.open();
		}

		await this._cache.del(this.headsPath);
		await this._cache.del(this.removedHeadsPath);
		await this.close();

		delete this._cache;
	}

	private async _loadHeads(): Promise<string[]> {
		if (!this.initialized) {
			throw new Error("Store needs to be initialized before loaded");
		}

		if (this._cache!.status !== "open") {
			await this._cache!.open();
		}

		await this.loadLastHeadsPath();
		return this.getCachedHeads();
	}

	async load() {
		if (!this.initialized) {
			throw new Error("Needs to be initialized before loaded");
		}

		if (this._cache!.status !== "open") {
			await this._cache!.open();
		}

		const heads = await this._loadHeads();
		this._loaded = true;
		return heads;
	}

	async loadLastHeadsPath() {
		this._lastHeadsPath = await this._cache
			?.get(this.headsPath)
			.then((bytes) => bytes && deserialize(bytes, CachePath).path);
		this._lastRemovedHeadsPath = await this._cache
			?.get(this.removedHeadsPath)
			.then((bytes) => bytes && deserialize(bytes, CachePath).path);
		this._lastHeadsCount = this._lastHeadsPath
			? await this.getCachedHeadsCount(this._lastHeadsPath)
			: 0n;
		this._lastRemovedHeadsCount = this._lastRemovedHeadsPath
			? await this.getCachedHeadsCount(this._lastRemovedHeadsPath)
			: 0n;
	}

	async getCachedHeadsCount(headPath?: string): Promise<bigint> {
		if (!headPath) {
			return 0n;
		}
		return (
			(
				await this._cache
					?.get(headPath)
					.then((bytes) => bytes && deserialize(bytes, HeadsCacheToSerialize))
			)?.counter || 0n
		);
	}

	async waitForHeads() {
		if (this.closed) {
			throw new Error("Store is closed");
		}
		if (!this._loaded) {
			return this._cacheWriteQueue?.add(async () => {
				if (this._loaded) {
					return;
				}
				return this.load();
			});
		}
	}

	public queue(
		changes: {
			added?: (Entry<T> | string)[];
			removed?: (Entry<T> | string)[];
		},
		reset?: boolean
	) {
		return this._cacheWriteQueue?.add(() =>
			this._updateCachedHeads(changes, reset)
		);
	}
}
