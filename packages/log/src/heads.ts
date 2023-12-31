import { Entry } from "./entry.js";
import { AnyStore } from "@peerbit/any-store";
import { HeadsCache } from "./heads-cache.js";
import { Blocks } from "@peerbit/blocks-interface";
import { Keychain } from "@peerbit/keychain";
import { Encoding } from "./encoding.js";
import { Values } from "./values.js";
import { logger } from "./logger.js";
import { EntryIndex } from "./entry-index.js";

export type CacheUpdateOptions = {
	cache?: { update?: false; reset?: false } | { update: true; reset?: boolean };
};

interface Log<T> {
	blocks: Blocks;
	keychain?: Keychain;
	memory?: AnyStore;
	encoding: Encoding<any>;
	entryIndex: EntryIndex<T>;
	values: Values<T>;
}
export class HeadsIndex<T> {
	private _id: Uint8Array;
	private _index: Set<string> = new Set();
	private _gids: Map<string, Map<string, Entry<T>>>; // gid -> hash -> entry
	private _headsCache: HeadsCache<T> | undefined;
	private _config: Log<T>;
	private _onGidRemoved?: (gid: string[]) => Promise<void> | void;
	constructor(id: Uint8Array) {
		this._gids = new Map();
		this._id = id;
	}

	async init(
		log: Log<T>,
		options: {
			entries?: Entry<T>[];
			onGidRemoved?: (gid: string[]) => Promise<void> | void;
		} = {}
	) {
		this._config = log;
		this._onGidRemoved = options.onGidRemoved;
		await this.reset(options?.entries || []);
		if (log.memory) {
			this._headsCache = new HeadsCache(this);
			return this._headsCache.init(await log.memory.sublevel("heads"));
		}
	}

	async load(
		options?: {
			timeout?: number;
			replicate?: boolean;
			reload?: boolean;
		} & CacheUpdateOptions
	): Promise<Entry<T>[] | undefined> {
		if (!this._headsCache || (this._headsCache.loaded && !options?.reload)) {
			return;
		}

		// TODO make below into a promise that concurrenct caklls can wait on?
		const heads = await this._headsCache?.load();
		if (!heads) {
			return;
		}
		const entries = await Promise.all(
			heads.map(async (x) => {
				const entry = await this._config.entryIndex.get(x, { load: true });
				if (!entry) {
					logger.error("Failed to load entry from head with hash: " + x);
					return;
				}
				await entry.getMeta(); // TODO types,decrypt gid
				return entry;
			})
		);
		await this.reset(entries.filter((x) => !!x) as Entry<any>[]);
		return entries as Entry<any>[];
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

	get gids(): Map<string, Map<string, Entry<T>>> {
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
		const gidKeys = [...this._gids.keys()];

		this._gids = new Map();
		if (entries?.length > 0) {
			await this.putAll(entries, options); // reset cache = true
		}

		if (gidKeys.length > 0) {
			this._onGidRemoved?.(gidKeys);
		}
	}

	has(cid: string) {
		return this._index.has(cid);
	}

	async put(entry: Entry<T>, options?: CacheUpdateOptions) {
		await this._putOne(entry);
		if (options?.cache?.update) {
			await this._headsCache?.queue({ added: [entry] }, options.cache.reset);
		}
	}

	async putAll(entries: Entry<T>[], options?: CacheUpdateOptions) {
		await this._putAll(entries);
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

	private async _putOne(entry: Entry<T>) {
		if (!entry.hash) {
			throw new Error("Missing hash");
		}
		if (this._index.has(entry.hash)) {
			return;
		}

		this._index.add(entry.hash);
		const map = this._gids.get(entry.meta.gid);
		if (!map) {
			const newMap = new Map();
			this._gids.set(entry.meta.gid, newMap);
			newMap.set(entry.hash, entry);
		} else {
			map.set(entry.hash, entry);
		}

		for (const next of entry.next) {
			const indexedEntry = this._config.entryIndex.getShallow(next);
			if (indexedEntry) {
				await this.del(indexedEntry);
			}
		}
	}

	private async _putAll(entries: Entry<T>[]) {
		for (const entry of entries) {
			await this._putOne(entry);
		}
	}

	async del(
		entry: { hash: string; meta: { gid: string } },
		options?: CacheUpdateOptions
	): Promise<boolean> {
		const wasHead = this._index.delete(entry.hash);
		if (!wasHead) {
			return false;
		}
		let removedGids: Set<string> | undefined = undefined;
		const map = this._gids.get(entry.meta.gid)!;
		map.delete(entry.hash);
		if (map.size <= 0) {
			this._gids.delete(entry.meta.gid);
			(removedGids || (removedGids = new Set<string>())).add(entry.meta.gid);
		}

		if (!entry.hash) {
			throw new Error("Missing hash");
		}

		if (removedGids) {
			await this._onGidRemoved?.([...removedGids]);
		}

		if (wasHead && options?.cache?.update) {
			await this._headsCache?.queue(
				{ removed: [entry.hash] },
				options.cache.reset
			);
		}

		return wasHead;
		//     this._headsCache = undefined; // TODO do smarter things here, only remove the element needed (?)
	}
}
