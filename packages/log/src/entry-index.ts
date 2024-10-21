import { deserialize, serialize } from "@dao-xyz/borsh";
import { type Blocks } from "@peerbit/blocks-interface";
import { Cache } from "@peerbit/cache";
import type { PublicSignKey } from "@peerbit/crypto";
import {
	BoolQuery,
	type Index,
	Not,
	Or,
	type Query,
	type Shape,
	Sort,
	SortDirection,
	StringMatch,
	StringMatchMethod,
	toId,
} from "@peerbit/indexer-interface";
import type { ShallowEntry } from "./entry-shallow.js";
import { EntryType } from "./entry-type.js";
import { Entry, type ShallowOrFullEntry } from "./entry.js";
import type { SortFn } from "./log-sorting.js";
import { logger } from "./logger.js";

export type ResultsIterator<T> = {
	close: () => void | Promise<void>;
	next: (number: number) => T[] | Promise<T[]>;
	done: () => boolean | undefined;
	all(): T[] | Promise<T[]>;
};

const ENTRY_CACHE_MAX_SIZE = 10; // TODO as param for log

type ResolveFullyOptions =
	| true
	| {
			type: "full";
			remote?:
				| {
						replicate?: boolean;
						signal?: AbortSignal;
						timeout?: number;
				  }
				| boolean;
			ignoreMissing?: boolean;
	  };
type ResolveShapeOptions = { type: "shape"; shape: Shape };
export type MaybeResolveOptions =
	| false
	| ResolveFullyOptions
	| ResolveShapeOptions;
export type ReturnTypeFromResolveOptions<
	R extends MaybeResolveOptions,
	T,
> = R extends false | undefined
	? ShallowEntry
	: R extends { type: "shape" }
		? any
		: Entry<T>;

export class EntryIndex<T> {
	private cache: Cache<Entry<T>>;
	private sortReversed: Sort[];
	private initialied = false;
	private _length: number;
	private insertionPromises: Map<string, Promise<void>>;
	constructor(
		readonly properties: {
			store: Blocks;
			publicKey: PublicSignKey;
			init: (entry: Entry<T>) => void;
			cache?: Cache<Entry<T>>;
			index: Index<ShallowEntry>;
			sort: SortFn;
			onGidRemoved?: (gid: string[]) => Promise<void> | void;
		},
	) {
		this.sortReversed = properties.sort.sort.map((x) =>
			deserialize(serialize(x), Sort),
		);
		this.sortReversed.map(
			(x) =>
				(x.direction =
					x.direction === SortDirection.DESC
						? SortDirection.ASC
						: SortDirection.DESC),
		);
		this.cache = properties.cache ?? new Cache({ max: ENTRY_CACHE_MAX_SIZE });
		this._length = 0;
		this.insertionPromises = new Map();
	}

	getHeads<R extends MaybeResolveOptions = false>(
		gid?: string,
		resolve: R = false as R,
	): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		const query: Query[] = [];
		query.push(new BoolQuery({ key: "head", value: true }));
		if (gid) {
			query.push(
				new StringMatch({
					key: ["meta", "gid"],
					value: gid,
					caseInsensitive: false,
					method: StringMatchMethod.exact,
				}),
			);
		}
		return this.iterate(query, undefined, resolve);
	}

	getHasNext<R extends MaybeResolveOptions>(
		next: string,
		resolve?: R,
	): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		const query: Query[] = [
			new StringMatch({
				key: ["meta", "next"],
				value: next,
				caseInsensitive: false,
				method: StringMatchMethod.exact,
			}),
		];
		return this.iterate(query, undefined, resolve);
	}

	countHasNext(next: string, excludeHash: string | undefined = undefined) {
		const query: Query[] = [
			new StringMatch({
				key: ["meta", "next"],
				value: next,
				caseInsensitive: false,
				method: StringMatchMethod.exact,
			}),
		];
		if (excludeHash) {
			query.push(
				new Not(
					new StringMatch({
						key: ["hash"],
						value: excludeHash,
						caseInsensitive: false,
						method: StringMatchMethod.exact,
					}),
				),
			);
		}
		return this.properties.index.count({ query });
	}

	iterate<R extends MaybeResolveOptions>(
		query: Query[],
		sort = this.properties.sort.sort,
		options?: R,
	): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		let resolveInFull = options
			? options === true
				? true
				: options.type === "full"
			: false;
		let resolveInFullOptions: ResolveFullyOptions | undefined = resolveInFull
			? (options as ResolveFullyOptions)
			: undefined;

		let nextShape = resolveInFull
			? ({ hash: true } as const)
			: ((options as { shape: Shape })?.shape as Shape);

		const iterator = this.properties.index.iterate(
			{ query, sort },
			{ shape: nextShape },
		);

		const next = async (
			amount: number,
		): Promise<ReturnTypeFromResolveOptions<R, T>[]> => {
			const results = await iterator.next(amount);
			if (resolveInFull) {
				const maybeResolved = await Promise.all(
					results.map((x) => this.resolve(x.value.hash, resolveInFullOptions)),
				);
				return maybeResolved.filter((x) => !!x) as ReturnTypeFromResolveOptions<
					R,
					T
				>[];
			} else {
				return results.map((x) => x.value) as ReturnTypeFromResolveOptions<
					R,
					T
				>[];
			}
		};

		return {
			close: iterator.close,
			done: iterator.done,
			next,
			all: async () => {
				const results: ReturnTypeFromResolveOptions<R, T>[] = [];
				while (!iterator.done()) {
					for (const element of await next(100)) {
						results.push(element);
					}
				}
				await iterator.close();
				return results;
			},
		};
	}

	async getOldest<
		T extends boolean,
		R = T extends true ? Entry<any> : ShallowEntry,
	>(resolve?: T): Promise<R | undefined> {
		const iterator = this.iterate([], this.properties.sort.sort, resolve);
		const results = await iterator.next(1);
		await iterator.close();
		return results[0] as R;
	}

	async getNewest<
		T extends boolean,
		R = T extends true ? Entry<any> : ShallowEntry,
	>(resolve?: T): Promise<R | undefined> {
		const iterator = this.iterate([], this.sortReversed, resolve);
		const results = await iterator.next(1);
		await iterator.close();
		return results[0] as R;
	}

	async getBefore<
		T extends boolean,
		R = T extends true ? Entry<any> : ShallowEntry,
	>(before: ShallowOrFullEntry<any>, resolve?: T): Promise<R | undefined> {
		const iterator = this.iterate(
			this.properties.sort.before(before),
			this.sortReversed,
			resolve,
		);
		const results = await iterator.next(1);
		await iterator.close();
		return results[0] as R;
	}
	async getAfter<
		T extends boolean,
		R = T extends true ? Entry<any> : ShallowEntry,
	>(before: ShallowOrFullEntry<any>, resolve?: T): Promise<R | undefined> {
		const iterator = this.iterate(
			this.properties.sort.after(before),
			this.properties.sort.sort,
			resolve,
		);
		const results = await iterator.next(1);
		await iterator.close();

		return results[0] as R;
	}

	async get(k: string, options?: ResolveFullyOptions) {
		return this.resolve(k, options);
	}

	async getShallow(k: string) {
		return this.properties.index.get(toId(k));
	}

	async has(k: string) {
		const result = await this.properties.index.get(toId(k), {
			shape: { hash: true },
		});
		return result != null;
	}

	async put(
		entry: Entry<any>,
		properties: { unique: boolean; isHead: boolean; toMultiHash: boolean },
	) {
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
		} else {
			if (!entry.hash) {
				throw new Error("Missing hash");
			}
		}

		const existingPromise = this.insertionPromises.get(entry.hash);
		if (existingPromise) {
			return existingPromise;
		} else {
			const fn = async () => {
				this.cache.add(entry.hash, entry);

				if (properties.unique === true || !(await this.has(entry.hash))) {
					this._length++;
				}

				await this.properties.index.put(entry.toShallow(properties.isHead));

				// check if gids has been shadowed, by query all nexts that have a different gid
				if (this.properties.onGidRemoved && entry.meta.next.length > 0) {
					let nextMatches: Query[] = [];

					for (const next of entry.meta.next) {
						nextMatches.push(
							new StringMatch({
								key: ["hash"],
								value: next,
								caseInsensitive: false,
								method: StringMatchMethod.exact,
							}),
						);
					}

					const nextsWithOthersGids: { hash: string; meta: { gid: string } }[] =
						await this.iterate(
							[
								new Or(nextMatches),
								new Not(
									new StringMatch({
										key: ["meta", "gid"],
										value: entry.meta.gid,
									}),
								),
							],
							undefined,
							{ type: "shape", shape: { hash: true, meta: { gid: true } } },
						).all();

					let shadowedGids = new Set<string>();
					for (const next of nextsWithOthersGids) {
						// check that this entry is not referenced by other
						const nexts = await this.countHasNext(next.hash, entry.hash);
						if (nexts > 0) {
							continue;
						}
						shadowedGids.add(next.meta.gid);
					}

					if (shadowedGids.size > 0) {
						this.properties.onGidRemoved?.([...shadowedGids]);
					}
				}

				// mark all next entries as not heads
				await this.privateUpdateNextHeadProperty(entry, false);

				this.insertionPromises.delete(entry.hash);
			};
			const promise = fn();
			this.insertionPromises.set(entry.hash, promise);
			return promise;
		}
	}

	async delete(k: string, shallow: ShallowEntry | undefined = undefined) {
		this.cache.del(k);

		if (shallow && shallow.hash !== k) {
			throw new Error("Shallow hash doesn't match the key");
		}

		shallow = shallow || (await this.getShallow(k))?.value;
		if (!shallow) {
			return; // already deleted
		}

		let deleted = await this.properties.index.del({ query: { hash: k } });
		await this.properties.store.rm(k);

		if (deleted.length > 0) {
			this._length -= deleted.length;

			// mark all next entries as new heads
			await this.privateUpdateNextHeadProperty(shallow, true);
			return shallow;
		}
	}

	async getMemoryUsage() {
		return this.properties.index.sum({ key: "payloadSize" });
	}

	private async privateUpdateNextHeadProperty(
		from: ShallowEntry | Entry<any>,
		isHead: boolean,
	) {
		if (from.meta.type === EntryType.CUT) {
			// if the next is a cut, we can't update it, since it's not in the index
			return;
		}

		for (const next of from.meta.next) {
			const indexedEntry = await this.properties.index.get(toId(next));

			if (!indexedEntry) {
				continue; // we could end up here because another entry with same next ref is of CUT and has removed it from the index
			}

			if (isHead) {
				const noPointersToNext = (await this.countHasNext(next)) === 0;
				if (noPointersToNext) {
					indexedEntry.value.head = true;
					if (indexedEntry) {
						await this.properties.index.put(indexedEntry.value);
					}
				}
			} else {
				indexedEntry.value.head = false;
				if (indexedEntry) {
					await this.properties.index.put(indexedEntry.value);
				}
			}
		}
	}

	async clear() {
		const iterator = this.iterate([], undefined, false);
		while (!iterator.done()) {
			const results = await iterator.next(100);
			for (const result of results) {
				await this.delete(result.hash);
			}
		}
		await this.properties.index.drop();
		await this.properties.index.start();
		this.cache.clear();
	}

	get length() {
		if (!this.initialied) {
			throw new Error("Not initialized");
		}
		return this._length;
	}

	async init() {
		this._length = await this.properties.index.getSize();
		this.initialied = true;
	}

	private async resolve(
		k: string,
		options?: ResolveFullyOptions,
	): Promise<Entry<T> | undefined> {
		let coercedOptions = typeof options === "object" ? options : undefined;
		if (await this.has(k)) {
			let mem = this.cache.get(k);
			if (mem === undefined) {
				mem = await this.resolveFromStore(k, coercedOptions);
				if (mem) {
					this.properties.init(mem);
					mem.hash = k;
				} else if (coercedOptions?.ignoreMissing !== true) {
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
		options?: {
			remote?:
				| { signal?: AbortSignal; replicate?: boolean; timeout?: number }
				| boolean;
		},
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
