import { Entry } from "./entry.js";
import LocalStore from "@dao-xyz/lazy-level";
import { HeadsCache } from "./heads-cache.js";
import { BlockStore } from "@dao-xyz/libp2p-direct-block";

export type CacheUpdateOptions = {
	cache?: { update?: false; reset?: false } | { update: true; reset?: boolean };
};
export class HeadsIndex<T> {
	private _id: Uint8Array;
	private _index: Set<string> = new Set();
	private _gids: Map<string, number>;
	private _headsCache: HeadsCache<T> | undefined;
	private _blockstore: BlockStore;
	constructor(id: Uint8Array) {
		this._gids = new Map();
		this._id = id;
	}

	async init(
		blockstore: BlockStore,
		cache?: (name: string) => Promise<LocalStore> | LocalStore,
		options: { entries?: Entry<T>[] } = {}
	) {
		this._blockstore = blockstore;
		await this.reset(options?.entries || []);
		if (cache) {
			this._headsCache = new HeadsCache(this);
			return this._headsCache.init(cache);
		}
	}

	async load(
		options?: {
			timeout?: number;
			replicate?: boolean;
			reload?: boolean;
		} & CacheUpdateOptions
	) {
		if (!this._headsCache || (this._headsCache.loaded && !options?.reload)) {
			return;
		}

		// TODO make below into a promise that concurrenct caklls can wait on?
		const heads = await this._headsCache?.load();
		if (!heads) {
			return;
		}
		const entries = await Promise.all(
			heads.map((x) => Entry.fromMultihash<T>(this._blockstore, x, options))
		);
		await this.reset(entries);
		return entries;
	}

	get headsCache(): HeadsCache<T> | undefined {
		return this._headsCache;
	}

	close() {
		return this._headsCache?.close();
	}

	drop() {
		return this._headsCache?.drop();
	}

	get id(): Uint8Array {
		return this._id;
	}

	get index() {
		return this._index;
	}

	get gids(): Map<string, number> {
		return this._gids;
	}

	get size() {
		return this._index.size;
	}

	async reset(
		entries: Entry<T>[],
		options: CacheUpdateOptions = { cache: { reset: true, update: true } }
	) {
		this._index.clear();
		this._gids = new Map();
		if (entries) {
			await this.putAll(entries, options); // reset cache = true
		}
	}

	has(cid: string) {
		return this._index.has(cid);
	}

	async put(entry: Entry<T>, options?: CacheUpdateOptions) {
		this._putOne(entry);
		if (options?.cache?.update) {
			await this._headsCache?.queue({ added: [entry] }, options.cache.reset);
		}
	}

	async putAll(entries: Entry<T>[], options?: CacheUpdateOptions) {
		this._putAll(entries);
		if (options?.cache?.update) {
			await this._headsCache?.queue({ added: entries }, options.cache.reset);
		}
	}

	async resetHeadsCache() {
		await this._headsCache?.queue(
			{ added: [...this._index], removed: [] },
			true
		);
	}
	async updateHeadsCache(
		change: {
			added?: (Entry<T> | string)[];
			removed?: (Entry<T> | string)[];
		} = {},
		reset?: boolean
	) {
		await this._headsCache?.queue(change, reset);
	}

	private _putOne(entry: Entry<T>) {
		if (!entry.hash) {
			throw new Error("Missing hash");
		}
		if (this._index.has(entry.hash)) {
			return;
		}

		this._index.add(entry.hash);
		if (!this._gids.has(entry.gid)) {
			this._gids.set(entry.gid, 1);
		} else {
			this._gids.set(entry.gid, this._gids.get(entry.gid)! + 1);
		}
	}

	private _putAll(entries: Entry<T>[]) {
		for (const entry of entries) {
			this._putOne(entry);
		}
	}

	async del(
		entry: { hash: string; gid: string },
		options?: CacheUpdateOptions
	): Promise<{
		removed: boolean;
		lastWithGid: boolean;
	}> {
		const wasHead = this._index.delete(entry.hash);
		if (!wasHead) {
			return {
				lastWithGid: false,
				removed: false,
			};
		}
		const newValue = this._gids.get(entry.gid)! - 1;
		const lastWithGid = newValue <= 0;
		if (newValue <= 0) {
			this._gids.delete(entry.gid);
		} else {
			this._gids.set(entry.gid, newValue);
		}
		if (!entry.hash) {
			throw new Error("Missing hash");
		}

		if (wasHead && options?.cache?.update) {
			await this._headsCache?.queue(
				{ removed: [entry.hash] },
				options.cache.reset
			);
		}

		return {
			removed: wasHead,
			lastWithGid: lastWithGid,
		};
		//     this._headsCache = undefined; // TODO do smarter things here, only remove the element needed (?)
	}
}
