import { Cache } from "@peerbit/cache";
import { Entry, ShallowEntry } from "./entry.js";
import { deserialize } from "@dao-xyz/borsh";
import { logger } from "./logger.js";
import { Blocks } from "@peerbit/blocks-interface";

export class EntryIndex<T> {
	_cache: Cache<Entry<T> | null>;
	_blocks: Blocks;
	_init: (entry: Entry<T>) => void;
	_index: Map<string, ShallowEntry>;

	constructor(properties: {
		store: Blocks;
		init: (entry: Entry<T>) => void;
		cache: Cache<Entry<T>>;
	}) {
		this._cache = properties.cache;
		this._blocks = properties.store;
		this._init = properties.init;
		this._index = new Map();
	}

	async set(v: Entry<T>, toMultihash = true) {
		if (toMultihash) {
			const existingHash = v.hash;
			v.hash = undefined as any;
			try {
				const hash = await Entry.toMultihash(this._blocks, v);
				v.hash = existingHash;
				if (v.hash === undefined) {
					v.hash = hash; // can happen if you sync entries that you load directly from ipfs
				} else if (existingHash !== v.hash) {
					logger.error("Head hash didn't match the contents");
					throw new Error("Head hash didn't match the contents");
				}
			} catch (error) {
				logger.error(error);
				throw error;
			}
		}
		this._cache.add(v.hash, v);
		this._index.set(v.hash, v.toShallow());
	}
	has(k: string) {
		return this._index.has(k);
	}

	async get(
		k: string,
		options?: { load?: boolean; replicate?: boolean; timeout?: number }
	): Promise<Entry<T> | undefined> {
		if (this._index.has(k) || options?.load) {
			let mem = this._cache.get(k);
			if (mem === undefined) {
				mem = await this.getFromStore(k, options);
				if (mem) {
					this._init(mem);
					mem.hash = k;
				}
				this._cache.add(k, mem);
			}
			return mem ? mem : undefined;
		}
		return undefined;
	}

	getShallow(k: string) {
		return this._index.get(k);
	}

	private async getFromStore(
		k: string,
		options?: { replicate?: boolean; timeout?: number }
	): Promise<Entry<T> | null> {
		const value = await this._blocks.get(k, options);
		if (value) {
			const entry = deserialize(value, Entry);
			entry.size = value.length;
			return entry;
		}
		return null;
	}

	async delete(k: string) {
		this._cache.del(k);
		this._index.delete(k);
		return this._blocks.rm(k);
	}
}
