import { deserialize, serialize } from "@dao-xyz/borsh";
import { type Blocks, type GetOptions } from "@peerbit/blocks-interface";
import { Cache } from "@peerbit/cache";
import type { PublicSignKey } from "@peerbit/crypto";
import {
	BoolQuery,
	type Index,
	type IndexedResults,
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
import type { SortFn, SortableEntry } from "./log-sorting.js";
import { logger as baseLogger } from "./logger.js";

const log = baseLogger.newScope("entry-index");
const LOG_ENTRY_REMOTE_READ_PRIORITY = 2;

export type ResultsIterator<T> = {
	close: () => void | Promise<void>;
	next: (number: number) => T[] | Promise<T[]>;
	done: () => boolean | undefined;
	all(): T[] | Promise<T[]>;
};

const ENTRY_CACHE_MAX_SIZE = 10; // TODO as param for log
const DEFERRED_INDEX_FLUSH_IDLE_MS = 250;
const NATIVE_GRAPH_REBUILD_BATCH_SIZE = 512;

type BlocksWithGetMany = Blocks & {
	getMany?: (
		cids: string[],
		options?: GetOptions,
	) => Promise<Array<Uint8Array | undefined>> | Array<Uint8Array | undefined>;
};

type NativeLogEntry = {
	hash: string;
	gid: string;
	next: string[];
	type: number;
	head?: boolean;
	payloadSize?: number;
	clock: {
		timestamp: {
			wallTime: bigint | number | string;
			logical?: number;
		};
	};
};

type NativeJoinCutCheck = {
	gid: string;
	wallTime: bigint | number | string;
	logical?: number;
};

export type NativeLogGraph = {
	has: (hash: string) => boolean;
	hasMany: (hashes: Iterable<string>) => Set<string>;
	put: (entry: NativeLogEntry) => void;
	delete: (hash: string) => boolean;
	clear: () => void;
	heads: (gid?: string) => string[];
	headEntries: (gid?: string) => SortableEntry[];
	joinHeadEntries: (gid?: string) => NativeLogJoinEntry[];
	countHasNext: (next: string, excludeHash?: string) => number;
	shadowedGids: (gid: string, next: string[], excludeHash?: string) => string[];
	planJoin: (
		hash: string,
		next: string[],
		type: number,
		reset?: boolean,
		cutCheck?: NativeJoinCutCheck,
	) => JoinPlan;
};

export type JoinPlan = {
	skip: boolean;
	missingParents: string[];
	cutChecked: boolean;
	coveredByCut: boolean;
};

export type NativeLogJoinEntry = SortableEntry & {
	meta: SortableEntry["meta"] & {
		type: EntryType;
		next: string[];
	};
};

type ResolveFullyOptions =
	| true
	| {
			type: "full";
			remote?:
				| {
						replicate?: boolean;
						signal?: AbortSignal;
						timeout?: number;
						from?: string[];
						priority?: number;
				  }
				| boolean;
			ignoreMissing?: boolean;
	  };
type ResolveShapeOptions = { type: "shape"; shape: Shape };
export type MaybeResolveOptions =
	| false
	| ResolveFullyOptions
	| ResolveShapeOptions;

type RemoteReadOptionsWithPriority = {
	remote?:
		| {
				replicate?: boolean;
				signal?: AbortSignal;
				timeout?: number;
				from?: string[];
				priority?: number;
		  }
		| boolean;
};

const withDefaultRemoteReadPriority = <
	O extends RemoteReadOptionsWithPriority | undefined,
>(
	options: O,
): O => {
	if (!options?.remote) {
		return options;
	}
	if (options.remote === true) {
		return {
			...options,
			remote: { priority: LOG_ENTRY_REMOTE_READ_PRIORITY },
		} as O;
	}
	if (options.remote.priority != null) {
		return options;
	}
	return {
		...options,
		remote: {
			...options.remote,
			priority: LOG_ENTRY_REMOTE_READ_PRIORITY,
		},
	} as O;
};

const canBatchResolveFromStore = (
	store: Blocks,
	options?: ResolveFullyOptions,
): store is BlocksWithGetMany => {
	if (typeof (store as BlocksWithGetMany).getMany !== "function") {
		return false;
	}
	return typeof options !== "object" || !options.remote;
};
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
	private pendingIndexWrites: Map<string, ShallowEntry>;
	private pendingIndexFlushTimer?: ReturnType<typeof setTimeout>;
	constructor(
		readonly properties: {
			store: Blocks;
			publicKey: PublicSignKey;
			init: (entry: Entry<T>) => void;
			cache?: Cache<Entry<T>>;
			index: Index<ShallowEntry>;
			sort: SortFn;
			onGidRemoved?: (gid: string[]) => Promise<void> | void;
			nativeGraph?: {
				graph: NativeLogGraph;
				useHeads: boolean;
			};
			resolveRemotePeers?: (
				hash: string,
				options?: { signal?: AbortSignal },
			) => Promise<string[] | undefined> | string[] | undefined;
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
		this.pendingIndexWrites = new Map();
	}

	private schedulePendingIndexWriteFlush() {
		if (this.pendingIndexFlushTimer) {
			clearTimeout(this.pendingIndexFlushTimer);
		}
		this.pendingIndexFlushTimer = setTimeout(() => {
			void this.flushPendingWrites().catch((error) => {
				log.error("Failed to flush deferred entry-index writes", error);
			});
		}, DEFERRED_INDEX_FLUSH_IDLE_MS);
		this.pendingIndexFlushTimer.unref?.();
	}

	private clearPendingIndexFlushTimer() {
		if (!this.pendingIndexFlushTimer) {
			return;
		}
		clearTimeout(this.pendingIndexFlushTimer);
		this.pendingIndexFlushTimer = undefined;
	}

	async flushPendingWrites(hashes?: Iterable<string>) {
		const keys = hashes
			? [...new Set([...hashes].filter((hash): hash is string => !!hash))]
			: [...this.pendingIndexWrites.keys()];
		if (keys.length === 0) {
			return;
		}
		this.clearPendingIndexFlushTimer();
		for (const hash of keys) {
			const pending = this.pendingIndexWrites.get(hash);
			if (!pending) {
				continue;
			}
			await this.properties.index.put(pending);
			this.pendingIndexWrites.delete(hash);
		}
		if (this.pendingIndexWrites.size > 0) {
			this.schedulePendingIndexWriteFlush();
		}
	}

	getHeads<R extends MaybeResolveOptions = false>(
		gid?: string,
		resolve: R = false as R,
	): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		if (this.properties.nativeGraph?.useHeads) {
			return this.iterateNativeHashes(
				() => this.properties.nativeGraph!.graph.heads(gid),
				resolve,
			);
		}

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

	getHeadsForAppend(gid?: string): SortableEntry[] | undefined {
		if (!this.properties.nativeGraph?.useHeads) {
			return undefined;
		}
		return this.properties.nativeGraph.graph.headEntries(gid);
	}

	getJoinHeads(gid?: string): Promise<NativeLogJoinEntry[]> {
		if (this.properties.nativeGraph?.useHeads) {
			return Promise.resolve(
				this.properties.nativeGraph.graph.joinHeadEntries(gid),
			);
		}
		return this.getHeads(gid, {
			type: "shape",
			shape: {
				hash: true,
				meta: { type: true, next: true, gid: true, clock: true },
			},
		}).all() as Promise<NativeLogJoinEntry[]>;
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
		return this._countHasNext(next, excludeHash);
	}

	private async _countHasNext(
		next: string,
		excludeHash: string | undefined = undefined,
	) {
		await this.flushPendingWrites();
		if (this.properties.nativeGraph) {
			return this.properties.nativeGraph.graph.countHasNext(next, excludeHash);
		}
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

	private iterateNativeHashes<R extends MaybeResolveOptions>(
		hashes: () => string[],
		options?: R,
	): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		const resolveInFull = options
			? options === true
				? true
				: options.type === "full"
			: false;
		const resolveInFullOptions: ResolveFullyOptions | undefined = resolveInFull
			? (options as ResolveFullyOptions)
			: undefined;
		const shape = !resolveInFull
			? ((options as { shape?: Shape } | undefined)?.shape as Shape | undefined)
			: undefined;

		let hashPromise: Promise<string[]> | undefined;
		let offset = 0;
		let complete = false;

		const getHashes = async () => {
			if (!hashPromise) {
				hashPromise = this.flushPendingWrites().then(() => hashes());
			}
			return hashPromise;
		};

		const coerce = async (
			hashes: string[],
		): Promise<ReturnTypeFromResolveOptions<R, T>[]> => {
			if (resolveInFull) {
				const resolved = await this.resolveMany(hashes, resolveInFullOptions);
				return resolved.filter(
					(entry) => !!entry,
				) as ReturnTypeFromResolveOptions<R, T>[];
			}

			const shallow = await Promise.all(
				hashes.map((hash) => this.getShallow(hash)),
			);
			return shallow
				.filter((entry) => !!entry)
				.map((entry) =>
					shape ? projectShape(entry.value, shape) : entry.value,
				) as ReturnTypeFromResolveOptions<R, T>[];
		};

		return {
			close: () => undefined,
			done: () => complete,
			next: async (amount: number) => {
				const all = await getHashes();
				const batch = all.slice(offset, offset + amount);
				offset += batch.length;
				complete = offset >= all.length;
				return coerce(batch);
			},
			all: async () => {
				const all = await getHashes();
				const remaining = all.slice(offset);
				offset = all.length;
				complete = true;
				return coerce(remaining);
			},
		};
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

		let iteratorRef:
			| ReturnType<typeof this.properties.index.iterate>
			| undefined;
		let iteratorPromise:
			| Promise<ReturnType<typeof this.properties.index.iterate>>
			| undefined;

		const getIterator = async () => {
			await this.flushPendingWrites();
			if (!iteratorPromise) {
				iteratorRef = this.properties.index.iterate(
					{ query, sort },
					{ shape: nextShape },
				);
				iteratorPromise = Promise.resolve(iteratorRef);
			}
			return iteratorPromise;
		};

		const next = async (
			amount: number,
		): Promise<ReturnTypeFromResolveOptions<R, T>[]> => {
			const results = await (await getIterator()).next(amount);
			return coerce(results);
		};

		const all = async (): Promise<ReturnTypeFromResolveOptions<R, T>[]> => {
			const results = await (await getIterator()).all();
			return coerce(results);
		};

		const coerce = async (
			results: IndexedResults<{
				[x: string]: any;
			}>,
		): Promise<ReturnTypeFromResolveOptions<R, T>[]> => {
			if (resolveInFull) {
				const maybeResolved = await this.resolveMany(
					results.map((x) => x.value.hash),
					resolveInFullOptions,
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
			close: async () => iteratorRef?.close(),
			done: () => iteratorRef?.done(),
			next,
			all,
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

	async getMany(k: string[], options?: ResolveFullyOptions) {
		return this.resolveMany(k, options);
	}

	async planJoin(
		entry: Pick<Entry<T>, "hash" | "meta">,
		reset?: boolean,
	): Promise<JoinPlan> {
		if (this.properties.nativeGraph) {
			const cutCheck = this.properties.nativeGraph.useHeads
				? {
						gid: entry.meta.gid,
						wallTime: entry.meta.clock.timestamp.wallTime,
						logical: entry.meta.clock.timestamp.logical,
					}
				: undefined;
			return this.properties.nativeGraph.graph.planJoin(
				entry.hash,
				entry.meta.next,
				entry.meta.type,
				reset === true,
				cutCheck,
			);
		}
		if (!reset && (await this.getShallow(entry.hash)) != null) {
			return {
				skip: true,
				missingParents: [],
				cutChecked: false,
				coveredByCut: false,
			};
		}
		if (entry.meta.type === EntryType.CUT) {
			return {
				skip: false,
				missingParents: [],
				cutChecked: false,
				coveredByCut: false,
			};
		}

		const missingParents: string[] = [];
		for (const next of entry.meta.next) {
			if (reset || (await this.getShallow(next)) == null) {
				missingParents.push(next);
			}
		}
		return {
			skip: false,
			missingParents,
			cutChecked: false,
			coveredByCut: false,
		};
	}

	async getShallow(k: string) {
		const pending = this.pendingIndexWrites.get(k);
		if (pending) {
			return { id: toId(k), value: pending };
		}
		return this.properties.index.get(toId(k));
	}

	async has(k: string) {
		if (this.properties.nativeGraph) {
			return this.properties.nativeGraph.graph.has(k);
		}
		if (this.pendingIndexWrites.has(k)) {
			return true;
		}
		const result = await this.properties.index.get(toId(k), {
			shape: { hash: true },
		});
		return result != null;
	}

	async hasMany(hashes: Iterable<string>) {
		if (this.properties.nativeGraph) {
			return this.properties.nativeGraph.graph.hasMany(
				new Set([...hashes].filter(Boolean)),
			);
		}
		const batchSize = 64;
		const existing = new Set<string>();
		const missing: string[] = [];
		for (const hash of new Set([...hashes].filter(Boolean))) {
			if (this.pendingIndexWrites.has(hash)) {
				existing.add(hash);
				continue;
			}
			missing.push(hash);
		}

		if (missing.length === 0) {
			return existing;
		}

		for (let i = 0; i < missing.length; i += batchSize) {
			const batch = missing.slice(i, i + batchSize);
			const iterator = this.properties.index.iterate(
				{
					query:
						batch.length === 1
							? new StringMatch({
									key: "hash",
									value: batch[0]!,
								})
							: new Or(
									batch.map(
										(hash) =>
											new StringMatch({
												key: "hash",
												value: hash,
											}),
									),
								),
				},
				{
					shape: { hash: true },
				},
			);
			try {
				const indexed = await iterator.all();
				for (const entry of indexed) {
					existing.add(entry.value.hash);
				}
			} finally {
				await iterator.close();
			}
		}

		return existing;
	}

	async put(
		entry: Entry<any>,
		properties: {
			unique: boolean;
			isHead: boolean;
			toMultiHash: boolean;
			deferIndexWrite?: boolean;
		},
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
					log.error("Head hash didn't match the contents");
					throw new Error("Head hash didn't match the contents");
				}
			} catch (error) {
				log.error(error);
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
				if (properties.unique === true || !(await this.has(entry.hash))) {
					this._length++;
				}

				// add cache after .has check before .has uses the cache
				this.cache.add(entry.hash, entry);
				const shallowEntry = entry.toShallow(properties.isHead);
				const shouldDeferIndexWrite =
					properties.deferIndexWrite === true &&
					properties.isHead &&
					entry.meta.type !== EntryType.CUT &&
					entry.meta.next.length === 0;

				if (shouldDeferIndexWrite) {
					this.pendingIndexWrites.set(entry.hash, shallowEntry);
					this.schedulePendingIndexWriteFlush();
				} else {
					await this.flushPendingWrites(entry.meta.next);
					await this.properties.index.put(shallowEntry);
				}
				this.properties.nativeGraph?.graph.put(toNativeLogEntry(shallowEntry));

				// check if gids has been shadowed, by query all nexts that have a different gid
				await this.notifyShadowedGids(entry);

				// mark all next entries as not heads
				await this.privateUpdateNextHeadProperty(entry, false);

				this.insertionPromises.delete(entry.hash);
			};
			const promise = fn();
			this.insertionPromises.set(entry.hash, promise);
			return promise;
		}
	}

	async putAppendBatch(
		entries: Entry<any>[],
		properties: {
			unique: boolean;
			deferIndexWrite?: boolean;
		},
	) {
		if (entries.length === 0) {
			return;
		}
		if (entries.length === 1) {
			return this.put(entries[0], {
				unique: properties.unique,
				isHead: true,
				toMultiHash: false,
				deferIndexWrite: properties.deferIndexWrite,
			});
		}

		for (const entry of entries) {
			if (!entry.hash) {
				throw new Error("Missing hash");
			}
			const existingPromise = this.insertionPromises.get(entry.hash);
			if (existingPromise) {
				await existingPromise;
			}
		}

		const promise = (async () => {
			const batchHashes = new Set(entries.map((entry) => entry.hash));
			const externalNexts = new Set<string>();
			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i];
				const isHead = i === entries.length - 1;
				if (properties.unique === true || !(await this.has(entry.hash))) {
					this._length++;
				}

				this.cache.add(entry.hash, entry);
				const shallowEntry = entry.toShallow(isHead);
				await this.properties.index.put(shallowEntry);
				this.properties.nativeGraph?.graph.put(toNativeLogEntry(shallowEntry));
				await this.notifyShadowedGids(entry);

				for (const next of entry.meta.next) {
					if (!batchHashes.has(next)) {
						externalNexts.add(next);
					}
				}
			}

			if (externalNexts.size > 0) {
				await this.privateUpdateNextHeadHashes([...externalNexts], false);
			}
		})().finally(() => {
			for (const entry of entries) {
				this.insertionPromises.delete(entry.hash);
			}
		});

		for (const entry of entries) {
			this.insertionPromises.set(entry.hash, promise);
		}

		return promise;
	}

	async delete(k: string, from?: Entry<any> | ShallowEntry) {
		this.cache.del(k);

		if (from && from.hash !== k) {
			throw new Error("Shallow hash doesn't match the key");
		}

		const pending = this.pendingIndexWrites.get(k);
		from = from || pending || (await this.getShallow(k))?.value;
		if (!from) {
			return; // already deleted
		}
		if (pending) {
			this.pendingIndexWrites.delete(k);
			await this.properties.store.rm(k);
			this._length--;
			this.properties.nativeGraph?.graph.delete(k);
			await this.privateUpdateNextHeadProperty(from, true);
			return from;
		}

		let deleted = await this.properties.index.del({ query: { hash: k } });
		await this.properties.store.rm(k);

		if (deleted.length > 0) {
			this._length -= deleted.length;
			this.properties.nativeGraph?.graph.delete(k);

			// mark all next entries as new heads
			await this.privateUpdateNextHeadProperty(from, true);
			return from;
		}
	}

	async getMemoryUsage() {
		const indexed =
			(await this.properties.index.sum({ key: "payloadSize" })) || 0;
		const pending = [...this.pendingIndexWrites.values()].reduce(
			(sum, entry) => sum + (entry.payloadSize || 0),
			0,
		);
		return typeof indexed === "bigint"
			? indexed + BigInt(pending)
			: indexed + pending;
	}

	private async privateUpdateNextHeadProperty(
		from: ShallowEntry | Entry<any>,
		isHead: boolean,
	) {
		if (from.meta.type === EntryType.CUT) {
			// if the next is a cut, we can't update it, since it's not in the index
			return;
		}

		await this.privateUpdateNextHeadHashes(from.meta.next, isHead);
	}

	private async privateUpdateNextHeadHashes(nexts: string[], isHead: boolean) {
		for (const next of nexts) {
			const pending = this.pendingIndexWrites.get(next);
			const indexedEntry = pending
				? { id: toId(next), value: pending }
				: await this.properties.index.get(toId(next));

			if (!indexedEntry) {
				continue; // we could end up here because another entry with same next ref is of CUT and has removed it from the index
			}

			if (isHead) {
				const noPointersToNext = (await this.countHasNext(next)) === 0;
				if (noPointersToNext) {
					indexedEntry.value.head = true;
					if (pending) {
						this.pendingIndexWrites.set(next, indexedEntry.value);
					} else {
						await this.properties.index.put(indexedEntry.value);
					}
				}
			} else {
				indexedEntry.value.head = false;
				if (pending) {
					this.pendingIndexWrites.set(next, indexedEntry.value);
				} else {
					await this.properties.index.put(indexedEntry.value);
				}
			}
		}
	}

	private async notifyShadowedGids(entry: Entry<any>) {
		if (!this.properties.onGidRemoved || entry.meta.next.length === 0) {
			return;
		}
		const shadowedGids: Set<string> = this.properties.nativeGraph
			? new Set<string>(
					this.properties.nativeGraph.graph.shadowedGids(
						entry.meta.gid,
						entry.meta.next,
						entry.hash,
					),
				)
			: await this.findShadowedGids(entry);

		if (shadowedGids.size > 0) {
			this.properties.onGidRemoved([...shadowedGids]);
		}
	}

	async clear() {
		this.clearPendingIndexFlushTimer();
		const hashes = new Set<string>(this.pendingIndexWrites.keys());
		const iterator = this.iterate([], undefined, false);
		while (!iterator.done()) {
			const results = await iterator.next(100);
			for (const result of results) {
				hashes.add(result.hash);
			}
		}
		this.pendingIndexWrites.clear();
		for (const hash of hashes) {
			await this.properties.store.rm(hash);
		}
		await this.properties.index.drop();
		await this.properties.index.start();
		this.properties.nativeGraph?.graph.clear();
		this.cache.clear();
		this._length = 0;
	}

	get length() {
		if (!this.initialied) {
			throw new Error("Not initialized");
		}
		return this._length;
	}

	async init() {
		this.clearPendingIndexFlushTimer();
		this.pendingIndexWrites.clear();
		this._length = await this.properties.index.getSize();
		await this.rebuildNativeGraph();
		this.initialied = true;
	}

	private async rebuildNativeGraph() {
		if (!this.properties.nativeGraph) {
			return;
		}
		const graph = this.properties.nativeGraph.graph;
		graph.clear();
		const iterator = this.properties.index.iterate(
			{ query: [] },
			{
				shape: {
					hash: true,
					meta: { clock: true, gid: true, next: true, type: true },
					payloadSize: true,
					head: true,
				},
			},
		);
		try {
			while (!iterator.done()) {
				const results = await iterator.next(NATIVE_GRAPH_REBUILD_BATCH_SIZE);
				for (const result of results) {
					graph.put(toNativeLogEntry(result.value));
				}
			}
		} finally {
			await iterator.close();
		}
	}

	private async findShadowedGids(entry: Entry<any>) {
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
		return shadowedGids;
	}

	private async resolve(
		k: string,
		options?: ResolveFullyOptions,
	): Promise<Entry<T> | undefined> {
		let coercedOptions = typeof options === "object" ? options : undefined;
		/* if (await this.has(k)) { */
		let mem = this.cache.get(k);
		if (mem === undefined) {
			mem = await this.resolveFromStore(k, coercedOptions);
			if (mem) {
				this.properties.init(mem);
				mem.hash = k;
			} else if (coercedOptions?.ignoreMissing !== true) {
				throw new Error("Failed to load entry from head with hash: " + k);
			}
			if (mem) {
				this.cache.add(k, mem);
			}
		}
		return mem ? mem : undefined;
		/* }
			return undefined; */
	}

	private async resolveMany(
		hashes: string[],
		options?: ResolveFullyOptions,
	): Promise<Array<Entry<T> | undefined>> {
		if (hashes.length === 0) {
			return [];
		}
		if (!canBatchResolveFromStore(this.properties.store, options)) {
			return Promise.all(hashes.map((hash) => this.resolve(hash, options)));
		}

		const coercedOptions = typeof options === "object" ? options : undefined;
		const resolved: Array<Entry<T> | undefined> = new Array(hashes.length);
		const missingHashes: string[] = [];
		const missingPositions: number[] = [];

		for (let i = 0; i < hashes.length; i++) {
			const hash = hashes[i]!;
			const mem = this.cache.get(hash);
			if (mem !== undefined) {
				resolved[i] = mem ? mem : undefined;
				continue;
			}
			missingHashes.push(hash);
			missingPositions.push(i);
		}

		if (missingHashes.length === 0) {
			return resolved;
		}

		const values = await this.properties.store.getMany!(
			missingHashes,
			withDefaultRemoteReadPriority(coercedOptions),
		);
		for (let i = 0; i < values.length; i++) {
			const hash = missingHashes[i]!;
			const value = values[i];
			if (!value) {
				if (coercedOptions?.ignoreMissing !== true) {
					throw new Error("Failed to load entry from head with hash: " + hash);
				}
				continue;
			}

			const entry = deserialize(value, Entry) as Entry<T>;
			this.properties.init(entry);
			entry.hash = hash;
			entry.size = value.length;
			this.cache.add(hash, entry);
			resolved[missingPositions[i]!] = entry;
		}

		return resolved;
	}

	private async resolveFromStore(
		k: string,
		options?: {
			remote?:
				| {
						signal?: AbortSignal;
						replicate?: boolean;
						timeout?: number;
						from?: string[];
						priority?: number;
				  }
				| boolean;
		},
	): Promise<Entry<T> | null> {
		let coercedOptions = options;
		const remote = coercedOptions?.remote;

		if (this.properties.resolveRemotePeers) {
			if (remote === true) {
				try {
					const from = await this.properties.resolveRemotePeers(k);
					if (from && from.length > 0) {
						coercedOptions = { ...coercedOptions, remote: { from } };
					}
				} catch {
					// Best-effort only; fall back to the store's default remote strategy.
				}
			} else if (remote && typeof remote === "object" && remote.from == null) {
				try {
					const from = await this.properties.resolveRemotePeers(k, {
						signal: remote.signal,
					});
					if (from && from.length > 0) {
						coercedOptions = {
							...coercedOptions,
							remote: { ...remote, from },
						};
					}
				} catch {
					// Best-effort only; fall back to the store's default remote strategy.
				}
			}
		}

		const value = await this.properties.store.get(
			k,
			withDefaultRemoteReadPriority(coercedOptions),
		);
		if (value) {
			const entry = deserialize(value, Entry);
			entry.size = value.length;
			return entry;
		}
		return null;
	}
}

const toNativeLogEntry = (entry: ShallowEntry): NativeLogEntry => ({
	hash: entry.hash,
	gid: entry.meta.gid,
	next: entry.meta.next,
	type: entry.meta.type,
	head: entry.head,
	payloadSize: entry.payloadSize,
	clock: {
		timestamp: {
			wallTime: entry.meta.clock.timestamp.wallTime,
			logical: entry.meta.clock.timestamp.logical,
		},
	},
});

const projectShape = (value: any, shape: Shape): any => {
	const out: any = {};
	for (const [key, selector] of Object.entries(shape)) {
		if (selector === true) {
			out[key] = value?.[key];
		} else if (selector && typeof selector === "object") {
			out[key] = projectShape(value?.[key], selector as Shape);
		}
	}
	return out;
};
