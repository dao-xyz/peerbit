import { Cache } from "@peerbit/cache";
import { Entry, EntryType, type ShallowEntry, type ShallowOrFullEntry } from "./entry.js";
import { type Blocks } from "@peerbit/blocks-interface";
import { toId, SearchRequest, BoolQuery, CloseIteratorRequest, CollectNextRequest, type IndexedResults, Query, StringMatch, StringMatchMethod, Sort, SortDirection, type Index, SumRequest, CountRequest, Or, Not } from "@peerbit/indexer-interface";
import type { PublicSignKey } from "@peerbit/crypto";
import { deserialize, serialize } from "@dao-xyz/borsh";
import type { SortFn } from "./log-sorting.js";
import { logger } from "./logger.js";

export type ResultsIterator<T> = {
	close: () => Promise<void>;
	next: (number: number) => Promise<T[]>;
	done: () => boolean;
	all(): Promise<T[]>;
};

const ENTRY_CACHE_MAX_SIZE = 1000; // TODO as param for log

type ResolveOptions = { replicate?: boolean; signal?: AbortSignal, timeout?: number; ignoreMissing?: boolean }
export type MaybeResolveOptions = boolean | ResolveOptions;
export type ReturnTypeFromResolveOptions<R extends MaybeResolveOptions, T> = R extends false | undefined ? ShallowEntry : Entry<T>;

export class EntryIndex<T> {

	private cache: Cache<Entry<T>>;
	private sortReversed: Sort[];
	private initialied = false;
	private _length: number;
	constructor(readonly properties: {
		store: Blocks;
		publicKey: PublicSignKey;
		init: (entry: Entry<T>) => void;
		cache?: Cache<Entry<T>>;
		index: Index<ShallowEntry>
		sort: SortFn,
		onGidRemoved?: (gid: string[]) => Promise<void> | void;
	}) {
		this.sortReversed = properties.sort.sort.map(x => deserialize(serialize(x), Sort));
		this.sortReversed.map((x) => x.direction = x.direction === SortDirection.DESC ? SortDirection.ASC : SortDirection.DESC);
		this.cache = properties.cache ?? new Cache({ max: ENTRY_CACHE_MAX_SIZE })
		this._length = 0;
	}


	getHeads<R extends MaybeResolveOptions = false>(gid?: string, resolve: R = false as R): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		const query: Query[] = [];
		query.push(new BoolQuery({ key: "head", value: true }));
		if (gid) {
			query.push(new StringMatch({ key: ["meta", "gid"], value: gid, caseInsensitive: false, method: StringMatchMethod.exact }));
		}
		return this.query(query, undefined, resolve);
	}

	getHasNext<R extends MaybeResolveOptions>(next: string, resolve?: R): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		const query: Query[] = [new StringMatch({ key: ["meta", "next"], value: next, caseInsensitive: false, method: StringMatchMethod.exact })];
		return this.query(query, undefined, resolve);
	}

	countHasNext(next: string, excludeHash: string | undefined = undefined) {
		const query: Query[] = [new StringMatch({ key: ["meta", "next"], value: next, caseInsensitive: false, method: StringMatchMethod.exact })];
		if (excludeHash) {
			query.push(new Not(new StringMatch({ key: ["hash"], value: excludeHash, caseInsensitive: false, method: StringMatchMethod.exact })));
		}
		return this.properties.index.count(new CountRequest({ query }));

	}

	query<R extends MaybeResolveOptions>(query: Query[], sort = this.properties.sort.sort, options?: R): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		let rq: SearchRequest | undefined = undefined
		let left = 1;
		const done = () => left == 0
		const next = async (amount: number): Promise<ReturnTypeFromResolveOptions<R, T>[]> => {
			let results: IndexedResults<ShallowEntry>;
			if (!rq) {
				rq = new SearchRequest({ query, sort, fetch: amount });
				results = await this.properties.index.query(rq, this.properties.publicKey)
			}
			else {
				results = await this.properties.index.next(new CollectNextRequest({ id: rq.id, amount }), this.properties.publicKey);

			}
			left = results.kept;

			if (options) {
				const maybeResolved = await Promise.all(results.results.map(x => this.resolve(x.value.hash, typeof options === 'boolean' ? undefined : options)))
				return maybeResolved.filter(x => !!x) as ReturnTypeFromResolveOptions<R, T>[]
			}
			else {
				return results.results.map(x => x.value) as ReturnTypeFromResolveOptions<R, T>[]
			}
		}
		const close = async () => {
			if (rq) {
				return this.properties.index.close(new CloseIteratorRequest({ id: rq.id }), this.properties.publicKey)
			}
		}
		return {
			close,
			next,
			done,
			all: async () => {
				const results: ReturnTypeFromResolveOptions<R, T>[] = [];
				while (!done()) {
					for (const element of await next(100)) {
						results.push(element);
					}
				}
				await close()
				return results
			}
		};
	}

	async getOldest<T extends boolean, R = T extends true ? Entry<any> : ShallowEntry>(resolve?: T): Promise<R | undefined> {
		const iterator = this.query([], this.properties.sort.sort, resolve);
		const results = await iterator.next(1)
		await iterator.close()
		return results[0] as R;

	}

	async getNewest<T extends boolean, R = T extends true ? Entry<any> : ShallowEntry>(resolve?: T): Promise<R | undefined> {

		const iterator = this.query([], this.sortReversed, resolve);
		const results = await iterator.next(1)
		await iterator.close()
		return results[0] as R;

	}

	async getBefore<T extends boolean, R = T extends true ? Entry<any> : ShallowEntry>(before: ShallowOrFullEntry<any>, resolve?: T): Promise<R | undefined> {

		const iterator = this.query(this.properties.sort.before(before), this.sortReversed, resolve);
		const results = await iterator.next(1)
		await iterator.close()
		return results[0] as R;

	}
	async getAfter<T extends boolean, R = T extends true ? Entry<any> : ShallowEntry>(before: ShallowOrFullEntry<any>, resolve?: T): Promise<R | undefined> {
		const iterator = this.query(this.properties.sort.after(before), this.properties.sort.sort, resolve);
		const results = await iterator.next(1)
		await iterator.close()
		return results[0] as R;
	}


	async get(k: string, options?: ResolveOptions) {
		return this.resolve(k, options);
	}

	async getShallow(k: string) {
		return this.properties.index.get(toId(k));
	}

	async has(k: string) {
		const result = await this.properties.index.get(toId(k));
		return result != null;
	}

	async put(entry: Entry<any>, properties: { isHead: boolean, toMultiHash: boolean }) {
		if (properties.toMultiHash) {
			const existingHash = entry.hash;
			entry.hash = undefined as any;
			try {
				const hash = await Entry.toMultihash(this.properties.store, entry);
				entry.hash = existingHash;
				if (entry.hash === undefined) {
					entry.hash = hash; // can happen if you sync entries that you load directly from ipfs
				} else if (existingHash !== entry.hash) {
					logger.error("Head hash didn't match the contents");
					throw new Error("Head hash didn't match the contents");
				}
			} catch (error) {
				logger.error(error);
				throw error;
			}
		}
		else {
			if (!entry.hash) {
				throw new Error("Missing hash");
			}
		}

		this.cache.add(entry.hash, entry);
		if (!await this.has(entry.hash)) {
			this._length++;
		}

		await this.properties.index.put(entry.toShallow(properties.isHead));

		// check if gids has been shadowed, by query all nexts that have a different gid
		let nextMatches: Query[] = [];
		for (const next of entry.meta.next) {
			nextMatches.push(new StringMatch({ key: ["hash"], value: next, caseInsensitive: false, method: StringMatchMethod.exact }));
		}

		const nextsWithOthersGids = await this.query([new Or(nextMatches), new Not(new StringMatch({ key: ["meta", "gid"], value: entry.meta.gid }))], undefined, false).all();

		let shadowedGids = new Set<string>();
		for (const next of nextsWithOthersGids) {

			// check that this entry is not referenced by other
			const nexts = await this.countHasNext(next.hash, entry.hash);
			if (nexts > 0) {
				continue;
			}
			shadowedGids.add(next.meta.gid)
		}
		if (shadowedGids.size > 0) {
			this.properties.onGidRemoved?.([...shadowedGids])
		}

		// mark all next entries as not heads
		return this.privateUpdateNextHeadProperty(entry, false);
	}

	async delete(k: string | ShallowEntry) {
		let cid = typeof k === "string" ? k : k.hash;
		this.cache.del(cid);
		const shallow = typeof k === 'string' ? (await this.getShallow(k))?.value : k;
		if (!shallow) {
			return;
		}

		this._length--;
		await this.properties.store.rm(cid);
		await this.properties.index.del(toId(cid));

		// mark all next entries as new heads
		await this.privateUpdateNextHeadProperty(shallow, true);
		return shallow
	}

	async getMemoryUsage() {

		return this.properties.index.sum(new SumRequest({ key: "payloadSize" }));
	}

	private async privateUpdateNextHeadProperty(from: ShallowEntry | Entry<any>, isHead: boolean) {
		if (from.meta.type === EntryType.CUT) {
			// if the next is a cut, we can't update it, since it's not in the index
			return;
		}

		for (const next of from.meta.next) {
			const indexedEntry = await this.properties.index.get(toId(next));

			if (!indexedEntry) {
				continue; // we could end up here because another entry with same next ref is of CUT and has removed it from the index
			}

			const pointersToNext = await this.countHasNext(next) == 0
			if (isHead) {
				if (pointersToNext) {
					indexedEntry.value.head = true;
					if (indexedEntry) {
						await this.properties.index.put(indexedEntry.value);
					}
				}
			}
			else {
				indexedEntry.value.head = false;
				if (indexedEntry) {
					await this.properties.index.put(indexedEntry.value);
				}
			}


		}
	}

	async clear() {
		const iterator = await this.query([], undefined, false)
		while (!iterator.done()) {
			const results = await iterator.next(100);
			for (const result of results) {
				await this.delete(result.hash);
			}
		}
		await this.properties.index.drop();
		this.cache.clear();
	}

	get length() {
		if (!this.initialied) {
			throw new Error("Not initialized");
		}
		return this._length
	}

	async init() {

		this._length = await this.properties.index.getSize();
		this.initialied = true;
	}

	private async resolve(
		k: string,
		options?: ResolveOptions
	): Promise<Entry<T> | undefined> {
		if (await this.has(k)) {
			let mem = this.cache.get(k);
			if (mem === undefined) {
				mem = await this.resolveFromStore(k, options);
				if (mem) {
					this.properties.init(mem);
					mem.hash = k;
				}
				else if (options?.ignoreMissing !== true) {
					throw new Error("Failed to load entry from head with hash: " + k);
				}
				this.cache.add(k, mem ?? undefined);
			}
			return mem ? mem : undefined;
		}
		return undefined;
	}


	private async resolveFromStore(
		k: string,
		options?: { signal?: AbortSignal, replicate?: boolean; timeout?: number }
	): Promise<Entry<T> | null> {
		const value = await this.properties.store.get(k, options);
		if (value) {
			const entry = deserialize(value, Entry);
			entry.size = value.length;
			return entry;
		}
		return null;
	}




}



/* _cache: Cache<Entry<T> | null>;
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
*/