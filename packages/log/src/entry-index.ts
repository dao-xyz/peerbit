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
import { FOREGROUND_READ_MESSAGE_PRIORITY } from "@peerbit/stream-interface";
import { LamportClock as Clock, Timestamp } from "./clock.js";
import { ShallowEntry, ShallowMeta } from "./entry-shallow.js";
import { EntryType } from "./entry-type.js";
import {
	Entry,
	type PreparedEntryBlock,
	type PreparedNativeLogEntry,
	type ShallowOrFullEntry,
} from "./entry.js";
import type { SortFn, SortableEntry } from "./log-sorting.js";
import { logger as baseLogger } from "./logger.js";

const log = baseLogger.newScope("entry-index");
const LOG_ENTRY_REMOTE_READ_PRIORITY = FOREGROUND_READ_MESSAGE_PRIORITY;

export type ResultsIterator<T> = {
	close: () => void | Promise<void>;
	next: (number: number) => T[] | Promise<T[]>;
	done: () => boolean | undefined;
	all(): T[] | Promise<T[]>;
};

const ENTRY_CACHE_MAX_SIZE = 10; // TODO as param for log
const DEFERRED_INDEX_FLUSH_IDLE_MS = 250;
const NATIVE_GRAPH_REBUILD_BATCH_SIZE = 512;
const TIMESTAMP_WALL_TIME_KEY = ["meta", "clock", "timestamp", "wallTime"];
const TIMESTAMP_LOGICAL_KEY = ["meta", "clock", "timestamp", "logical"];

type EntryIndexProfileValue = string | number | boolean | undefined;
type EntryIndexProfileEvent = {
	name: string;
	component?: string;
	durationMs?: number;
	entries?: number;
	messages?: number;
	count?: number;
	details?: Record<string, EntryIndexProfileValue>;
};
type EntryIndexProfileSink = (event: EntryIndexProfileEvent) => void;

const entryIndexProfileNow = () =>
	globalThis.performance?.now?.() ?? Date.now();
const entryIndexProfileStart = (sink: EntryIndexProfileSink | undefined) =>
	sink ? entryIndexProfileNow() : 0;
const emitEntryIndexProfileDuration = (
	sink: EntryIndexProfileSink | undefined,
	startedAt: number,
	event: Omit<EntryIndexProfileEvent, "durationMs">,
) => {
	if (!sink) {
		return;
	}
	sink({
		...event,
		durationMs: entryIndexProfileNow() - startedAt,
	});
};

type BlocksWithGetMany = Blocks & {
	getMany?: (
		cids: string[],
		options?: GetOptions,
	) => Promise<Array<Uint8Array | undefined>> | Array<Uint8Array | undefined>;
};

type BlocksWithRmMany = Blocks & {
	rmMany?: (cids: string[]) => Promise<number | void> | number | void;
};

type BlocksWithPutKnown = Blocks & {
	putKnown?: (cid: string, bytes: Uint8Array) => Promise<string> | string;
};

const hasRmMany = (store: Blocks): store is BlocksWithRmMany =>
	typeof (store as BlocksWithRmMany).rmMany === "function";

type IndexWithExactDelete = Index<any> & {
	delIds: (
		deleteIds: string[],
	) => Promise<ReturnType<typeof toId>[]> | ReturnType<typeof toId>[];
};

const hasExactDelete = (index: Index<any>): index is IndexWithExactDelete =>
	typeof (index as IndexWithExactDelete).delIds === "function";

type IndexWithExactDeleteCount = Index<any> & {
	delIdsCount: (deleteIds: string[]) => Promise<number> | number;
};

const hasExactDeleteCount = (
	index: Index<any>,
): index is IndexWithExactDeleteCount =>
	typeof (index as IndexWithExactDeleteCount).delIdsCount === "function";

const putPreparedEntryBlock = async (
	store: Blocks,
	prepared: PreparedEntryBlock,
) => {
	const storeWithKnown = store as BlocksWithPutKnown;
	return typeof storeWithKnown.putKnown === "function"
		? await storeWithKnown.putKnown(prepared.cid, prepared.block.bytes)
		: await store.put(prepared);
};

type NativeLogEntry = PreparedNativeLogEntry;

export type PreparedAppendIndexFacts = {
	hash: string;
	meta: {
		clock: Clock;
		gid: string;
		next: string[];
		type: EntryType;
		data?: Uint8Array;
	};
	size?: number;
	shallowEntry: ShallowEntry;
	nativeEntry?: NativeLogEntry;
};

type NativeJoinCutCheck = {
	gid: string;
	wallTime: bigint | number | string;
	logical?: number;
};

type NativeJoinPlanInput = {
	hash: string;
	next: string[];
	type: number;
	cutCheck?: NativeJoinCutCheck;
};

type MaybePromise<T> = T | Promise<T>;
type PendingIndexWrite = ShallowEntry | (() => ShallowEntry);

const isPromiseLike = <T>(value: MaybePromise<T>): value is Promise<T> =>
	!!value && typeof (value as { then?: unknown }).then === "function";

const mapMaybePromise = <T, R>(
	value: MaybePromise<T>,
	fn: (value: T) => MaybePromise<R>,
): MaybePromise<R> => (isPromiseLike(value) ? value.then(fn) : fn(value));

export type NativeLogGraph = {
	readonly length: number;
	has: (hash: string) => boolean;
	hasMany: (hashes: Iterable<string>) => Set<string>;
	put: (entry: NativeLogEntry) => void;
	putBatch?: (entries: NativeLogEntry[]) => void;
	putAppendChain?: (entries: NativeLogEntry[]) => void;
	prepareEntryV0PlainChainAndPut?: (input: {
		clockId: Uint8Array;
		privateKey: Uint8Array;
		publicKey: Uint8Array;
		wallTimes: Array<bigint | number | string>;
		logicals?: number[];
		gid: string;
		initialNext?: string[];
		type?: number;
		metaDatas?: Array<Uint8Array | undefined>;
		payloadDatas: Uint8Array[];
	}) => MaybePromise<
		Array<{
			bytes: Uint8Array;
			cid: string;
			byteLength: number;
			signature: Uint8Array;
			next: string[];
			metaBytes: Uint8Array;
			payloadBytes: Uint8Array;
			signatureBytes: Uint8Array;
		}>
	>;
	prepareEntryV0PlainEntryAndPut?: (input: {
		clockId: Uint8Array;
		privateKey: Uint8Array;
		publicKey: Uint8Array;
		wallTime: bigint | number | string;
		logical?: number;
		gid: string;
		next?: string[];
		type?: number;
		metaData?: Uint8Array;
		payloadData: Uint8Array;
		includeMaterializationBytes?: boolean;
		includeAppendFactsBytes?: boolean;
		trimLengthTo?: number;
	}) => MaybePromise<{
		bytes: Uint8Array;
		cid: string;
		byteLength: number;
		signature?: Uint8Array;
		next: string[];
		metaBytes?: Uint8Array;
		payloadBytes?: Uint8Array;
		signatureBytes?: Uint8Array;
		hashDigestBytes?: Uint8Array;
		trimmedEntries?: PreparedNativeLogEntry[];
	}>;
	prepareEntryV0PlainChainCommit?: (
		input: {
			clockId: Uint8Array;
			privateKey: Uint8Array;
			publicKey: Uint8Array;
			wallTimes: Array<bigint | number | string>;
			logicals?: number[];
			gid: string;
			initialNext?: string[];
			type?: number;
			metaDatas?: Array<Uint8Array | undefined>;
			payloadDatas: Uint8Array[];
		},
		blockStore: unknown,
	) => MaybePromise<
		| Array<{
				bytes?: Uint8Array;
				cid: string;
				byteLength: number;
				signature: Uint8Array;
				next: string[];
				metaBytes: Uint8Array;
				payloadBytes: Uint8Array;
				signatureBytes: Uint8Array;
		  }>
		| undefined
	>;
	prepareEntryV0PlainEntryCommit?: (
		input: {
			clockId: Uint8Array;
			privateKey: Uint8Array;
			publicKey: Uint8Array;
			wallTime: bigint | number | string;
			logical?: number;
			gid: string;
			next?: string[];
			type?: number;
			metaData?: Uint8Array;
			payloadData: Uint8Array;
			resolveTrimmedEntries?: boolean;
		},
		blockStore: unknown,
	) => MaybePromise<
		| {
				bytes?: Uint8Array;
				cid: string;
				byteLength: number;
				signature?: Uint8Array;
				next: string[];
				metaBytes?: Uint8Array;
				payloadBytes?: Uint8Array;
				signatureBytes?: Uint8Array;
				hashDigestBytes?: Uint8Array;
				trimmedEntries?: PreparedNativeLogEntry[];
				trimmedEntryHashes?: string[];
		  }
		| undefined
	>;
	prepareEntryV0PlainEntriesCommit?: (
		input: {
			clockId: Uint8Array;
			privateKey: Uint8Array;
			publicKey: Uint8Array;
			wallTimes: Array<bigint | number | string>;
			logicals?: number[];
			gids: string[];
			nexts: string[][];
			type?: number;
			metaDatas?: Array<Uint8Array | undefined>;
			payloadDatas: Uint8Array[];
		},
		blockStore: unknown,
	) => MaybePromise<
		| Array<{
				bytes?: Uint8Array;
				cid: string;
				byteLength: number;
				signature?: Uint8Array;
				next: string[];
				metaBytes?: Uint8Array;
				payloadBytes?: Uint8Array;
				signatureBytes?: Uint8Array;
				hashDigestBytes?: Uint8Array;
		  }>
		| undefined
	>;
	delete: (hash: string) => boolean;
	deleteMany?: (hashes: Iterable<string>) => number;
	oldestEntries?: (limit: number) => NativeLogEntry[];
	clear: () => void;
	heads: (gid?: string) => string[];
	hasHead: (gid?: string) => boolean;
	hasAnyHead: (gids: Iterable<string>) => boolean;
	hasAnyHeadBatch: (gidSets: Iterable<Iterable<string>>) => boolean[];
	headDataEntries: (gid?: string) => NativeLogHeadDataEntry[];
	maxHeadDataU32: (gid?: string) => number | undefined;
	maxHeadDataU32Batch?: (gids: Iterable<string>) => Array<number | undefined>;
	headEntries: (gid?: string) => SortableEntry[];
	joinHeadEntries: (gid?: string) => NativeLogJoinEntry[];
	childJoinEntries: (hash: string) => NativeLogJoinEntry[];
	entryMetadataBatch?: (
		hashes: Iterable<string>,
	) => Array<NativeLogEntryMetadata | undefined>;
	entryMetadataHintsBatch?: (
		hashes: Iterable<string>,
	) => Array<NativeLogEntryMetadata | undefined>;
	uniqueReferenceGids: (hash: string) => string[] | undefined;
	uniqueReferenceGidRowsBatch?: (
		hashes: Iterable<string>,
	) => Array<Array<[string, string]> | undefined>;
	uniqueReferenceGidRowsFlatBatch?: (
		hashes: Iterable<string>,
	) => Array<[number, string, string]> | undefined;
	planDeleteRecursively: (
		hashes: Iterable<string>,
		skipFirst?: boolean,
	) => string[];
	payloadSizeSum: () => number;
	oldestHash?: () => string | undefined;
	newestHash?: () => string | undefined;
	countHasNext: (next: string, excludeHash?: string) => number;
	shadowedGids: (gid: string, next: string[], excludeHash?: string) => string[];
	planJoin: (
		hash: string,
		next: string[],
		type: number,
		reset?: boolean,
		cutCheck?: NativeJoinCutCheck,
	) => JoinPlan;
	planJoinBatch?: (
		entries: NativeJoinPlanInput[],
		reset?: boolean,
	) => JoinPlan[];
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

export type NativeLogHeadDataEntry = {
	hash: string;
	meta: {
		data?: Uint8Array;
	};
};

export type NativeLogEntryMetadata = {
	hash: string;
	gid: string;
	data?: Uint8Array;
	replicas?: number;
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

const sortKeyEquals = (sort: Sort | undefined, key: string[]) =>
	Array.isArray(sort?.key) &&
	sort.key.length === key.length &&
	sort.key.every((part, index) => part === key[index]);

const timestampSortDirection = (sort: Sort[]): SortDirection | undefined => {
	if (sort.length < 2) {
		return;
	}
	const [wallTime, logical] = sort;
	if (
		!sortKeyEquals(wallTime, TIMESTAMP_WALL_TIME_KEY) ||
		!sortKeyEquals(logical, TIMESTAMP_LOGICAL_KEY) ||
		wallTime.direction !== logical.direction
	) {
		return;
	}
	return wallTime.direction;
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
	private pendingIndexWrites: Map<string, PendingIndexWrite>;
	private pendingIndexFlushTimer?: ReturnType<typeof setTimeout>;
	private pendingIndexFlushLastWriteMs = 0;
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

	private materializePendingIndexWrite(
		hash: string,
		pending: PendingIndexWrite,
	): ShallowEntry {
		if (typeof pending !== "function") {
			return pending;
		}
		const shallowEntry = pending();
		this.pendingIndexWrites.set(hash, shallowEntry);
		return shallowEntry;
	}

	private getPendingIndexWrite(hash: string): ShallowEntry | undefined {
		const pending = this.pendingIndexWrites.get(hash);
		return pending
			? this.materializePendingIndexWrite(hash, pending)
			: undefined;
	}

	private schedulePendingIndexWriteFlush() {
		this.pendingIndexFlushLastWriteMs = Date.now();
		if (this.pendingIndexFlushTimer) {
			return;
		}
		const flushAfterIdle = () => {
			const remainingMs =
				DEFERRED_INDEX_FLUSH_IDLE_MS -
				(Date.now() - this.pendingIndexFlushLastWriteMs);
			if (remainingMs > 0) {
				this.pendingIndexFlushTimer = setTimeout(flushAfterIdle, remainingMs);
				this.pendingIndexFlushTimer.unref?.();
				return;
			}
			this.pendingIndexFlushTimer = undefined;
			void this.flushPendingWrites().catch((error) => {
				log.error("Failed to flush deferred entry-index writes", error);
			});
		};
		this.pendingIndexFlushTimer = setTimeout(
			flushAfterIdle,
			DEFERRED_INDEX_FLUSH_IDLE_MS,
		);
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
		const writes: ShallowEntry[] = [];
		for (const hash of keys) {
			const pending = this.pendingIndexWrites.get(hash);
			if (!pending) {
				continue;
			}
			writes.push(this.materializePendingIndexWrite(hash, pending));
		}
		if (writes.length === 0) {
			return;
		}
		if (writes.length > 1 && this.properties.index.putBatch) {
			await this.properties.index.putBatch(writes);
		} else {
			for (const write of writes) {
				await this.properties.index.put(write);
			}
		}
		for (const write of writes) {
			this.pendingIndexWrites.delete(write.hash);
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
			const shape = getResolveShape(resolve);
			if (shape && isHeadHashOnlyShape(shape)) {
				return this.iterateNativeProjected(
					() =>
						this.properties
							.nativeGraph!.graph.heads(gid)
							.map((hash) => ({ hash })),
					shape,
				) as ResultsIterator<ReturnTypeFromResolveOptions<R, T>>;
			}
			if (shape && isHeadDataShape(shape)) {
				return this.iterateNativeProjected(
					() => this.properties.nativeGraph!.graph.headDataEntries(gid),
					shape,
				) as ResultsIterator<ReturnTypeFromResolveOptions<R, T>>;
			}
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

	async hasHead(gid?: string): Promise<boolean | undefined> {
		if (!this.properties.nativeGraph?.useHeads) {
			return undefined;
		}
		return this.properties.nativeGraph.graph.hasHead(gid);
	}

	async hasAnyHead(gids: Iterable<string>): Promise<boolean | undefined> {
		if (!this.properties.nativeGraph?.useHeads) {
			return undefined;
		}
		const uniqueGids = new Set([...gids].filter(Boolean));
		if (uniqueGids.size === 0) {
			return false;
		}
		return this.properties.nativeGraph.graph.hasAnyHead(uniqueGids);
	}

	async hasAnyHeadBatch(
		gidSets: Iterable<Iterable<string>>,
	): Promise<boolean[] | undefined> {
		if (!this.properties.nativeGraph?.useHeads) {
			return undefined;
		}
		const normalized = [...gidSets].map(
			(gids) => new Set([...gids].filter(Boolean)),
		);
		if (normalized.length === 0) {
			return [];
		}
		return this.properties.nativeGraph.graph.hasAnyHeadBatch(normalized);
	}

	async getMaxHeadDataU32(gid?: string): Promise<number | undefined> {
		if (!this.properties.nativeGraph?.useHeads) {
			return undefined;
		}
		return this.properties.nativeGraph.graph.maxHeadDataU32(gid);
	}

	async getMaxHeadDataU32Batch(
		gids: Iterable<string>,
	): Promise<Array<number | undefined> | undefined> {
		if (!this.properties.nativeGraph?.useHeads) {
			return undefined;
		}
		const normalized = [...gids];
		if (normalized.length === 0) {
			return [];
		}
		return this.properties.nativeGraph.graph.maxHeadDataU32Batch?.(normalized);
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

	getJoinChildren(next: string): Promise<NativeLogJoinEntry[]> {
		if (this.properties.nativeGraph) {
			return Promise.resolve(
				this.properties.nativeGraph.graph.childJoinEntries(next),
			);
		}
		return this.getHasNext(next, {
			type: "shape",
			shape: {
				hash: true,
				meta: { type: true, next: true, gid: true, clock: true },
			},
		}).all() as Promise<NativeLogJoinEntry[]>;
	}

	getUniqueReferenceGids(hash: string): string[] | undefined {
		if (!this.properties.nativeGraph) {
			return undefined;
		}
		return this.properties.nativeGraph.graph.uniqueReferenceGids(hash);
	}

	getUniqueReferenceGidRowsBatch(
		hashes: Iterable<string>,
	): Array<Array<[string, string]> | undefined> | undefined {
		if (!this.properties.nativeGraph) {
			return undefined;
		}
		const normalized = [...hashes];
		if (normalized.length === 0) {
			return [];
		}
		return this.properties.nativeGraph.graph.uniqueReferenceGidRowsBatch?.(
			normalized,
		);
	}

	getUniqueReferenceGidRowsFlatBatch(
		hashes: Iterable<string>,
	): Array<[number, string, string]> | undefined {
		if (!this.properties.nativeGraph) {
			return undefined;
		}
		const normalized = [...hashes];
		if (normalized.length === 0) {
			return [];
		}
		return this.properties.nativeGraph.graph.uniqueReferenceGidRowsFlatBatch?.(
			normalized,
		);
	}

	planDeleteRecursively(
		from: Iterable<{ hash: string }>,
		skipFirst = false,
	): string[] | undefined {
		if (!this.properties.nativeGraph) {
			return undefined;
		}
		return this.properties.nativeGraph.graph.planDeleteRecursively(
			[...from].map((entry) => entry.hash),
			skipFirst,
		);
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
		if (this.properties.nativeGraph) {
			return this.properties.nativeGraph.graph.countHasNext(next, excludeHash);
		}
		await this.flushPendingWrites();
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
				hashPromise = Promise.resolve(hashes());
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

	private iterateNativeProjected<TValue>(
		values: () => TValue[],
		shape: Shape,
	): ResultsIterator<unknown> {
		let valuesPromise: Promise<TValue[]> | undefined;
		let offset = 0;
		let complete = false;

		const getValues = async () => {
			if (!valuesPromise) {
				valuesPromise = Promise.resolve(values());
			}
			return valuesPromise;
		};

		const coerce = (values: TValue[]) =>
			values.map((value) => projectShape(value, shape));

		return {
			close: () => undefined,
			done: () => complete,
			next: async (amount: number) => {
				const all = await getValues();
				const batch = all.slice(offset, offset + amount);
				offset += batch.length;
				complete = offset >= all.length;
				return coerce(batch);
			},
			all: async () => {
				const all = await getValues();
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
		const nativeHash = this.getNativeTimestampOrderedHash("oldest");
		if (nativeHash) {
			const nativeResult = resolve
				? await this.resolve(nativeHash)
				: (await this.getShallow(nativeHash))?.value;
			if (nativeResult) {
				return nativeResult as R;
			}
		}
		const iterator = this.iterate([], this.properties.sort.sort, resolve);
		const results = await iterator.next(1);
		await iterator.close();
		return results[0] as R;
	}

	async getOldestMany<
		T extends boolean,
		R = T extends true ? Entry<any> : ShallowEntry,
	>(limit: number, resolve?: T): Promise<R[]> {
		return this.getOldestManyMaybe(limit, resolve);
	}

	getOldestManyMaybe<
		T extends boolean,
		R = T extends true ? Entry<any> : ShallowEntry,
	>(limit: number, resolve?: T): MaybePromise<R[]> {
		if (limit <= 0) {
			return [];
		}
		const nativeEntries = this.getNativeOldestEntries(limit, resolve);
		if (nativeEntries) {
			return nativeEntries as R[];
		}
		return this.getOldestManyFromIterator(limit, resolve);
	}

	private async getOldestManyFromIterator<
		T extends boolean,
		R = T extends true ? Entry<any> : ShallowEntry,
	>(limit: number, resolve?: T): Promise<R[]> {
		const iterator = this.iterate([], this.properties.sort.sort, resolve);
		try {
			return (await iterator.next(limit)) as R[];
		} finally {
			await iterator.close();
		}
	}

	private getNativeOldestEntries<T extends boolean>(
		limit: number,
		resolve?: T,
	): ShallowEntry[] | undefined {
		if (resolve || limit <= 0) {
			return;
		}
		const graph = this.properties.nativeGraph?.graph;
		if (!graph?.oldestEntries) {
			return;
		}
		const direction = timestampSortDirection(this.properties.sort.sort);
		if (direction !== SortDirection.ASC) {
			return;
		}
		return graph
			.oldestEntries(limit)
			.map((entry) => this.nativeLogEntryToShallowEntry(entry));
	}

	async getNewest<
		T extends boolean,
		R = T extends true ? Entry<any> : ShallowEntry,
	>(resolve?: T): Promise<R | undefined> {
		const nativeHash = this.getNativeTimestampOrderedHash("newest");
		if (nativeHash) {
			const nativeResult = resolve
				? await this.resolve(nativeHash)
				: (await this.getShallow(nativeHash))?.value;
			if (nativeResult) {
				return nativeResult as R;
			}
		}
		const iterator = this.iterate([], this.sortReversed, resolve);
		const results = await iterator.next(1);
		await iterator.close();
		return results[0] as R;
	}

	private getNativeTimestampOrderedHash(
		position: "oldest" | "newest",
	): string | undefined {
		const graph = this.properties.nativeGraph?.graph;
		if (!graph?.oldestHash || !graph.newestHash) {
			return;
		}
		const direction = timestampSortDirection(this.properties.sort.sort);
		if (direction == null) {
			return;
		}
		const useNewest =
			direction === SortDirection.ASC
				? position === "newest"
				: position === "oldest";
		return useNewest ? graph.newestHash() : graph.oldestHash();
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

	async planJoinBatch(
		entries: Array<Pick<Entry<T>, "hash" | "meta">>,
		reset?: boolean,
		profile?: EntryIndexProfileSink,
	): Promise<JoinPlan[]> {
		if (entries.length === 0) {
			return [];
		}
		if (this.properties.nativeGraph?.graph.planJoinBatch) {
			const prepareStartedAt = entryIndexProfileStart(profile);
			const nativeInputs = entries.map((entry) => ({
				hash: entry.hash,
				next: entry.meta.next,
				type: entry.meta.type,
				cutCheck: this.properties.nativeGraph!.useHeads
					? {
							gid: entry.meta.gid,
							wallTime: entry.meta.clock.timestamp.wallTime,
							logical: entry.meta.clock.timestamp.logical,
						}
					: undefined,
			}));
			emitEntryIndexProfileDuration(profile, prepareStartedAt, {
				name: "log.entryIndex.planJoinBatch.prepareNative",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: {
					cutChecks: nativeInputs.reduce(
						(sum, input) => sum + (input.cutCheck ? 1 : 0),
						0,
					),
				},
			});
			const nativeStartedAt = entryIndexProfileStart(profile);
			const plans = this.properties.nativeGraph.graph.planJoinBatch(
				nativeInputs,
				reset === true,
			);
			emitEntryIndexProfileDuration(profile, nativeStartedAt, {
				name: "log.entryIndex.planJoinBatch.nativeGraph",
				component: "log",
				entries: entries.length,
				messages: 1,
			});
			return plans;
		}

		const fallbackStartedAt = entryIndexProfileStart(profile);
		const plans: JoinPlan[] = [];
		for (const entry of entries) {
			plans.push(await this.planJoin(entry, reset));
		}
		emitEntryIndexProfileDuration(profile, fallbackStartedAt, {
			name: "log.entryIndex.planJoinBatch.fallback",
			component: "log",
			entries: entries.length,
			messages: 1,
		});
		return plans;
	}

	async getShallow(k: string) {
		const pending = this.getPendingIndexWrite(k);
		if (pending) {
			return { id: toId(k), value: pending };
		}
		return this.properties.index.get(toId(k));
	}

	getNativeEntryMetadataBatch(
		hashes: Iterable<string>,
	): Array<NativeLogEntryMetadata | undefined> | undefined {
		if (!this.properties.nativeGraph) {
			return undefined;
		}
		const normalized = [...hashes];
		if (normalized.length === 0) {
			return [];
		}
		return this.properties.nativeGraph.graph.entryMetadataBatch?.(normalized);
	}

	getNativeEntryMetadataHintsBatch(
		hashes: Iterable<string>,
	): Array<NativeLogEntryMetadata | undefined> | undefined {
		if (!this.properties.nativeGraph) {
			return undefined;
		}
		const normalized = [...hashes];
		if (normalized.length === 0) {
			return [];
		}
		return (
			this.properties.nativeGraph.graph.entryMetadataHintsBatch?.(normalized) ??
			this.properties.nativeGraph.graph.entryMetadataBatch?.(normalized)
		);
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
			const preparedBlock = Entry.takePreparedBlock(entry);
			try {
				let hash: string;
				if (preparedBlock) {
					hash = await putPreparedEntryBlock(
						this.properties.store,
						preparedBlock,
					);
					entry.size = preparedBlock.block.bytes.length;
				} else {
					entry.hash = undefined as any;
					hash = await Entry.toMultihash(this.properties.store, entry);
				}
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
				const shallowEntry =
					Entry.takePreparedShallowEntry(entry, properties.isHead) ??
					entry.toShallow(properties.isHead);
				const nativeEntry =
					Entry.takePreparedNativeLogEntry(entry, properties.isHead) ??
					toNativeLogEntry(shallowEntry);
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
				this.properties.nativeGraph?.graph.put(nativeEntry);

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
			externalNextHashes?: string[];
			prepared?: {
				shallowEntries: ShallowEntry[];
				nativeEntries?: NativeLogEntry[];
				nativeGraphUpdated?: boolean;
				nativeBlocksCommitted?: boolean;
			};
			heads?: boolean[];
			deferIndexWrite?: boolean;
			profile?: EntryIndexProfileSink;
		},
	) {
		if (entries.length === 0) {
			return;
		}
		if (
			entries.length === 1 &&
			properties.prepared?.nativeGraphUpdated !== true
		) {
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
			const profile = properties.profile;
			const prepareStartedAt = entryIndexProfileStart(profile);
			const externalNexts = new Set<string>(
				properties.externalNextHashes ?? [],
			);
			const shouldDiscoverExternalNexts = !properties.externalNextHashes;
			const batchHashes = shouldDiscoverExternalNexts
				? new Set(entries.map((entry) => entry.hash))
				: undefined;
			const putBatch =
				!this.properties.onGidRemoved && this.properties.index.putBatch;
			const shallowEntries: ShallowEntry[] = [];
			const nativeGraphUpdated =
				properties.prepared?.nativeGraphUpdated === true;
			const nativeCommitOwnsHotIndex =
				nativeGraphUpdated &&
				properties.prepared?.nativeBlocksCommitted === true &&
				!this.properties.onGidRemoved;
			const nativeGraphPutAppendChain =
				!nativeGraphUpdated &&
				!this.properties.onGidRemoved &&
				properties.externalNextHashes &&
				this.properties.nativeGraph?.graph.putAppendChain
					? this.properties.nativeGraph.graph.putAppendChain.bind(
							this.properties.nativeGraph.graph,
						)
					: undefined;
			const nativeGraphPutBatch =
				!nativeGraphUpdated &&
				!this.properties.onGidRemoved &&
				this.properties.nativeGraph?.graph.putBatch
					? this.properties.nativeGraph.graph.putBatch.bind(
							this.properties.nativeGraph.graph,
						)
					: undefined;
			const deferBatchIndexWrite =
				properties.deferIndexWrite === true &&
				!!putBatch &&
				!!this.properties.nativeGraph &&
				!this.properties.onGidRemoved &&
				entries.every((entry) => entry.meta.type !== EntryType.CUT);
			const nativeEntries: NativeLogEntry[] = [];
			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i];
				const isHead = properties.heads?.[i] ?? i === entries.length - 1;
				if (properties.unique === true || !(await this.has(entry.hash))) {
					this._length++;
				}

				this.cache.add(entry.hash, entry);
				const preparedShallowEntry = properties.prepared?.shallowEntries[i];
				if (preparedShallowEntry) {
					preparedShallowEntry.head = isHead;
				}
				const shallowEntry =
					preparedShallowEntry ??
					Entry.takePreparedShallowEntry(entry, isHead) ??
					entry.toShallow(isHead);
				if (nativeCommitOwnsHotIndex) {
					this.pendingIndexWrites.set(entry.hash, shallowEntry);
					if (batchHashes) {
						for (const next of entry.meta.next) {
							if (!batchHashes.has(next)) {
								externalNexts.add(next);
							}
						}
					}
					continue;
				}
				const preparedNativeEntry = properties.prepared?.nativeEntries?.[i];
				if (preparedNativeEntry) {
					preparedNativeEntry.head = isHead;
				}
				const nativeEntry =
					!nativeGraphUpdated &&
					this.properties.nativeGraph &&
					(preparedNativeEntry ??
						Entry.takePreparedNativeLogEntry(entry, isHead) ??
						toNativeLogEntry(shallowEntry));
				if (putBatch) {
					shallowEntries.push(shallowEntry);
					nativeEntry && nativeEntries.push(nativeEntry);
				} else {
					await this.properties.index.put(shallowEntry);
					if (nativeGraphPutAppendChain || nativeGraphPutBatch) {
						nativeEntry && nativeEntries.push(nativeEntry);
					} else {
						if (nativeEntry) {
							this.properties.nativeGraph?.graph.put(nativeEntry);
							await this.notifyShadowedGids(entry);
						}
					}
				}

				if (batchHashes) {
					for (const next of entry.meta.next) {
						if (!batchHashes.has(next)) {
							externalNexts.add(next);
						}
					}
				}
			}
			emitEntryIndexProfileDuration(profile, prepareStartedAt, {
				name: "log.entryIndex.putAppendBatch.prepare",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: {
					unique: properties.unique,
					usedPreparedShallowEntries:
						properties.prepared?.shallowEntries.length ?? 0,
					usedPreparedNativeEntries:
						properties.prepared?.nativeEntries?.length ?? 0,
					nativeEntries: nativeEntries.length,
					discoverExternalNexts: shouldDiscoverExternalNexts,
				},
			});

			const putNativeEntries = (allowLoopFallback: boolean) => {
				if (nativeEntries.length === 0) {
					return;
				}
				if (nativeGraphPutAppendChain) {
					const nativePutStartedAt = entryIndexProfileStart(profile);
					nativeGraphPutAppendChain(nativeEntries);
					emitEntryIndexProfileDuration(profile, nativePutStartedAt, {
						name: "log.entryIndex.putAppendBatch.nativeGraphPut",
						component: "log",
						entries: nativeEntries.length,
						messages: 1,
						details: { method: "putAppendChain" },
					});
				} else if (nativeGraphPutBatch) {
					const nativePutStartedAt = entryIndexProfileStart(profile);
					nativeGraphPutBatch(nativeEntries);
					emitEntryIndexProfileDuration(profile, nativePutStartedAt, {
						name: "log.entryIndex.putAppendBatch.nativeGraphPut",
						component: "log",
						entries: nativeEntries.length,
						messages: 1,
						details: { method: "putBatch" },
					});
				} else if (allowLoopFallback) {
					const nativePutStartedAt = entryIndexProfileStart(profile);
					for (const nativeEntry of nativeEntries) {
						this.properties.nativeGraph?.graph.put(nativeEntry);
					}
					emitEntryIndexProfileDuration(profile, nativePutStartedAt, {
						name: "log.entryIndex.putAppendBatch.nativeGraphPut",
						component: "log",
						entries: nativeEntries.length,
						messages: 1,
						details: { method: "putLoop" },
					});
				}
			};

			if (nativeCommitOwnsHotIndex) {
				this.schedulePendingIndexWriteFlush();
			} else if (deferBatchIndexWrite) {
				const indexPutStartedAt = entryIndexProfileStart(profile);
				for (const shallowEntry of shallowEntries) {
					this.pendingIndexWrites.set(shallowEntry.hash, shallowEntry);
				}
				this.schedulePendingIndexWriteFlush();
				emitEntryIndexProfileDuration(profile, indexPutStartedAt, {
					name: "log.entryIndex.putAppendBatch.indexPut",
					component: "log",
					entries: shallowEntries.length,
					messages: 1,
					details: { deferred: true },
				});
				putNativeEntries(true);
			} else if (putBatch) {
				const indexPutStartedAt = entryIndexProfileStart(profile);
				await putBatch.call(this.properties.index, shallowEntries);
				emitEntryIndexProfileDuration(profile, indexPutStartedAt, {
					name: "log.entryIndex.putAppendBatch.indexPut",
					component: "log",
					entries: shallowEntries.length,
					messages: 1,
				});
				putNativeEntries(true);
			} else if (nativeEntries.length > 0) {
				putNativeEntries(false);
			}

			if (externalNexts.size > 0) {
				const externalNextStartedAt = entryIndexProfileStart(profile);
				await this.privateUpdateNextHeadHashes([...externalNexts], false);
				emitEntryIndexProfileDuration(profile, externalNextStartedAt, {
					name: "log.entryIndex.putAppendBatch.externalNexts",
					component: "log",
					entries: externalNexts.size,
					messages: 1,
				});
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

	// Internal trusted receive path for callers that can supply prepared append facts.
	async putAppendFactsBatch(
		entries: PreparedAppendIndexFacts[],
		properties: {
			unique: boolean;
			externalNextHashes?: string[];
			heads?: boolean[];
			deferIndexWrite?: boolean;
			nativeGraphUpdated?: boolean;
			profile?: EntryIndexProfileSink;
		},
	) {
		if (entries.length === 0) {
			return;
		}
		if (this.properties.onGidRemoved) {
			throw new Error(
				"Prepared append facts batch requires no onGidRemoved hook",
			);
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
			const profile = properties.profile;
			const prepareStartedAt = entryIndexProfileStart(profile);
			const externalNexts = new Set<string>(
				properties.externalNextHashes ?? [],
			);
			const shouldDiscoverExternalNexts = !properties.externalNextHashes;
			const batchHashes = shouldDiscoverExternalNexts
				? new Set(entries.map((entry) => entry.hash))
				: undefined;
			const putBatch = this.properties.index.putBatch;
			const shallowEntries: ShallowEntry[] = [];
			const nativeEntries: NativeLogEntry[] = [];
			const nativeGraphUpdated = properties.nativeGraphUpdated === true;
			const nativeGraphPutAppendChain =
				!nativeGraphUpdated &&
				properties.externalNextHashes &&
				this.properties.nativeGraph?.graph.putAppendChain
					? this.properties.nativeGraph.graph.putAppendChain.bind(
							this.properties.nativeGraph.graph,
						)
					: undefined;
			const nativeGraphPutBatch =
				!nativeGraphUpdated && this.properties.nativeGraph?.graph.putBatch
					? this.properties.nativeGraph.graph.putBatch.bind(
							this.properties.nativeGraph.graph,
						)
					: undefined;
			const deferBatchIndexWrite =
				properties.deferIndexWrite === true &&
				!!this.properties.nativeGraph &&
				entries.every((entry) => entry.meta.type !== EntryType.CUT);

			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i]!;
				const isHead = properties.heads?.[i] ?? i === entries.length - 1;
				if (properties.unique === true || !(await this.has(entry.hash))) {
					this._length++;
				}

				const shallowEntry = entry.shallowEntry;
				shallowEntry.head = isHead;
				const nativeEntry =
					!nativeGraphUpdated &&
					this.properties.nativeGraph &&
					(entry.nativeEntry ?? toNativeLogEntry(shallowEntry));
				if (nativeEntry) {
					nativeEntry.head = isHead;
				}
				if (deferBatchIndexWrite || putBatch) {
					shallowEntries.push(shallowEntry);
					nativeEntry && nativeEntries.push(nativeEntry);
				} else {
					await this.properties.index.put(shallowEntry);
					nativeEntry && nativeEntries.push(nativeEntry);
				}

				if (batchHashes) {
					for (const next of entry.meta.next) {
						if (!batchHashes.has(next)) {
							externalNexts.add(next);
						}
					}
				}
			}
			emitEntryIndexProfileDuration(profile, prepareStartedAt, {
				name: "log.entryIndex.putAppendFactsBatch.prepare",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: {
					unique: properties.unique,
					nativeEntries: nativeEntries.length,
					discoverExternalNexts: shouldDiscoverExternalNexts,
					nativeGraphUpdated,
				},
			});

			const putNativeEntries = (allowLoopFallback: boolean) => {
				if (nativeEntries.length === 0) {
					return;
				}
				if (nativeGraphPutAppendChain) {
					const nativePutStartedAt = entryIndexProfileStart(profile);
					nativeGraphPutAppendChain(nativeEntries);
					emitEntryIndexProfileDuration(profile, nativePutStartedAt, {
						name: "log.entryIndex.putAppendFactsBatch.nativeGraphPut",
						component: "log",
						entries: nativeEntries.length,
						messages: 1,
						details: { method: "putAppendChain" },
					});
				} else if (nativeGraphPutBatch) {
					const nativePutStartedAt = entryIndexProfileStart(profile);
					nativeGraphPutBatch(nativeEntries);
					emitEntryIndexProfileDuration(profile, nativePutStartedAt, {
						name: "log.entryIndex.putAppendFactsBatch.nativeGraphPut",
						component: "log",
						entries: nativeEntries.length,
						messages: 1,
						details: { method: "putBatch" },
					});
				} else if (allowLoopFallback) {
					const nativePutStartedAt = entryIndexProfileStart(profile);
					for (const nativeEntry of nativeEntries) {
						this.properties.nativeGraph?.graph.put(nativeEntry);
					}
					emitEntryIndexProfileDuration(profile, nativePutStartedAt, {
						name: "log.entryIndex.putAppendFactsBatch.nativeGraphPut",
						component: "log",
						entries: nativeEntries.length,
						messages: 1,
						details: { method: "putLoop" },
					});
				}
			};

			if (deferBatchIndexWrite) {
				const indexPutStartedAt = entryIndexProfileStart(profile);
				for (const shallowEntry of shallowEntries) {
					this.pendingIndexWrites.set(shallowEntry.hash, shallowEntry);
				}
				this.schedulePendingIndexWriteFlush();
				emitEntryIndexProfileDuration(profile, indexPutStartedAt, {
					name: "log.entryIndex.putAppendFactsBatch.indexPut",
					component: "log",
					entries: shallowEntries.length,
					messages: 1,
					details: { deferred: true },
				});
				putNativeEntries(true);
			} else if (putBatch) {
				const indexPutStartedAt = entryIndexProfileStart(profile);
				await putBatch.call(this.properties.index, shallowEntries);
				emitEntryIndexProfileDuration(profile, indexPutStartedAt, {
					name: "log.entryIndex.putAppendFactsBatch.indexPut",
					component: "log",
					entries: shallowEntries.length,
					messages: 1,
				});
				putNativeEntries(true);
			} else if (nativeEntries.length > 0) {
				putNativeEntries(true);
			}

			if (externalNexts.size > 0) {
				const externalNextStartedAt = entryIndexProfileStart(profile);
				await this.privateUpdateNextHeadHashes([...externalNexts], false);
				emitEntryIndexProfileDuration(profile, externalNextStartedAt, {
					name: "log.entryIndex.putAppendFactsBatch.externalNexts",
					component: "log",
					entries: externalNexts.size,
					messages: 1,
				});
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

	/** @internal */
	async putNativeCommittedAppend(
		entry: Entry<any>,
		properties: {
			unique: boolean;
			externalNextHashes: string[];
			shallowEntry?: ShallowEntry;
			isHead?: boolean;
		},
	) {
		if (!entry.hash) {
			throw new Error("Missing hash");
		}
		const existingPromise = this.insertionPromises.get(entry.hash);
		if (existingPromise) {
			await existingPromise;
		}

		const promise = (async () => {
			const isHead = properties.isHead ?? true;
			if (properties.unique === true || !(await this.has(entry.hash))) {
				this._length++;
			}

			this.cache.add(entry.hash, entry);
			const shallowEntry =
				properties.shallowEntry ??
				Entry.takePreparedShallowEntry(entry, isHead) ??
				entry.toShallow(isHead);
			shallowEntry.head = isHead;
			this.pendingIndexWrites.set(entry.hash, shallowEntry);
			this.schedulePendingIndexWriteFlush();

			if (properties.externalNextHashes.length > 0) {
				await this.privateUpdateNextHeadHashes(
					properties.externalNextHashes,
					false,
				);
			}
		})().finally(() => {
			this.insertionPromises.delete(entry.hash);
		});

		this.insertionPromises.set(entry.hash, promise);
		return promise;
	}

	/** @internal */
	putNativeCommittedAppendFacts(properties: {
		hash: string;
		unique: boolean;
		externalNextHashes: string[];
		shallowEntry?: ShallowEntry;
		getShallowEntry?: () => ShallowEntry;
		isHead?: boolean;
	}): Promise<void> | void {
		if (!properties.hash) {
			throw new Error("Missing hash");
		}
		const existingPromise = this.insertionPromises.get(properties.hash);
		if (
			!existingPromise &&
			properties.unique === true &&
			properties.externalNextHashes.length === 0
		) {
			const isHead = properties.isHead ?? true;
			this._length++;
			if (!properties.shallowEntry && !properties.getShallowEntry) {
				throw new Error("Missing shallow entry");
			}
			const pending =
				properties.shallowEntry ??
				(() => {
					const shallowEntry = properties.getShallowEntry!();
					shallowEntry.head = isHead;
					return shallowEntry;
				});
			if (properties.shallowEntry) {
				properties.shallowEntry.head = isHead;
			}
			this.pendingIndexWrites.set(properties.hash, pending);
			this.schedulePendingIndexWriteFlush();
			return;
		}

		return this.putNativeCommittedAppendFactsAsync(properties, existingPromise);
	}

	private async putNativeCommittedAppendFactsAsync(
		properties: {
			hash: string;
			unique: boolean;
			externalNextHashes: string[];
			shallowEntry?: ShallowEntry;
			getShallowEntry?: () => ShallowEntry;
			isHead?: boolean;
		},
		existingPromise?: Promise<void>,
	) {
		if (existingPromise) {
			await existingPromise;
		}
		const promise = (async () => {
			const isHead = properties.isHead ?? true;
			if (properties.unique === true || !(await this.has(properties.hash))) {
				this._length++;
			}

			if (!properties.shallowEntry && !properties.getShallowEntry) {
				throw new Error("Missing shallow entry");
			}
			const pending =
				properties.shallowEntry ??
				(() => {
					const shallowEntry = properties.getShallowEntry!();
					shallowEntry.head = isHead;
					return shallowEntry;
				});
			if (properties.shallowEntry) {
				properties.shallowEntry.head = isHead;
			}
			this.pendingIndexWrites.set(properties.hash, pending);
			this.schedulePendingIndexWriteFlush();

			if (properties.externalNextHashes.length > 0) {
				await this.privateUpdateNextHeadHashes(
					properties.externalNextHashes,
					false,
				);
			}
		})().finally(() => {
			this.insertionPromises.delete(properties.hash);
		});

		this.insertionPromises.set(properties.hash, promise);
		return promise;
	}

	async delete(k: string, from?: Entry<any> | ShallowEntry) {
		this.cache.del(k);

		if (from && from.hash !== k) {
			throw new Error("Shallow hash doesn't match the key");
		}

		const pending = this.getPendingIndexWrite(k);
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

	canDeleteMany(): boolean {
		return (
			!!this.properties.nativeGraph?.graph.deleteMany ||
			hasRmMany(this.properties.store)
		);
	}

	async deleteMany(
		from: ShallowEntry[],
		options?: { skipNextHeadUpdates?: boolean },
	): Promise<ShallowEntry[]> {
		return this.deleteManyMaybe(from, options);
	}

	deleteManyMaybe(
		from: ShallowEntry[],
		options?: { skipNextHeadUpdates?: boolean },
	): MaybePromise<ShallowEntry[]> {
		if (from.length === 0) {
			return [];
		}
		if (from.length === 1) {
			return this.deleteSingleMaybe(from[0]!, options);
		}
		const nodes: ShallowEntry[] = [];
		const seen = new Set<string>();
		for (const node of from) {
			if (seen.has(node.hash)) {
				continue;
			}
			seen.add(node.hash);
			nodes.push(node);
		}

		const indexedByHash = new Map(nodes.map((node) => [node.hash, node]));
		const deletedByHash = new Map<string, ShallowEntry>();
		const indexedHashes: string[] = [];
		const storeHashes: string[] = [];

		for (const node of nodes) {
			this.cache.del(node.hash);
			const pending = this.getPendingIndexWrite(node.hash);
			if (pending) {
				this.pendingIndexWrites.delete(node.hash);
				deletedByHash.set(node.hash, pending);
				storeHashes.push(node.hash);
			} else {
				indexedHashes.push(node.hash);
			}
		}

		const exactDeleteIndex: IndexWithExactDelete | undefined = hasExactDelete(
			this.properties.index,
		)
			? (this.properties.index as IndexWithExactDelete)
			: undefined;
		if (indexedHashes.length > 0 && exactDeleteIndex) {
			return mapMaybePromise(
				exactDeleteIndex.delIds(indexedHashes),
				(deleted) => {
					for (const id of deleted) {
						const hash = String(id.primitive);
						const node = indexedByHash.get(hash);
						if (!node || deletedByHash.has(hash)) {
							continue;
						}
						deletedByHash.set(hash, node);
						storeHashes.push(hash);
					}
					return this.finishDeleteMany(
						deletedByHash,
						nodes,
						storeHashes,
						options,
					);
				},
			);
		}
		if (indexedHashes.length > 0) {
			return this.deleteManyByQuery(
				indexedHashes,
				indexedByHash,
				deletedByHash,
				nodes,
				storeHashes,
				options,
			);
		}
		return this.finishDeleteMany(deletedByHash, nodes, storeHashes, options);
	}

	consumeNativeTrimmedEntriesMaybe(
		from: ShallowEntry[],
		options?: { skipNextHeadUpdates?: boolean; deleteBlocks?: boolean },
	): MaybePromise<ShallowEntry[]> {
		if (from.length === 0) {
			return [];
		}

		const nodes: ShallowEntry[] = [];
		const seen = new Set<string>();
		for (const node of from) {
			if (seen.has(node.hash)) {
				continue;
			}
			seen.add(node.hash);
			nodes.push(node);
		}

		return this.consumeNativeTrimmedEntryNodesMaybe(nodes, options);
	}

	consumeNativeTrimmedEntryHashesMaybe(
		hashes: string[],
		options?: { skipNextHeadUpdates?: boolean; deleteBlocks?: boolean },
	): MaybePromise<ShallowEntry[]> {
		if (hashes.length === 0) {
			return [];
		}

		const nodes: ShallowEntry[] = [];
		const seen = new Set<string>();
		for (const hash of hashes) {
			if (seen.has(hash)) {
				continue;
			}
			seen.add(hash);
			nodes.push(this.nativeTrimmedHashToShallowEntry(hash));
		}

		return this.consumeNativeTrimmedEntryNodesMaybe(nodes, options);
	}

	consumeNativeTrimmedEntryHashesNoReturnMaybe(
		hashes: string[],
		options?: { skipNextHeadUpdates?: boolean; deleteBlocks?: boolean },
	): MaybePromise<boolean> | undefined {
		if (
			!options?.skipNextHeadUpdates ||
			options.deleteBlocks !== false ||
			hashes.length === 0
		) {
			return undefined;
		}

		const indexedHashes: string[] = [];
		let pendingDeleted = 0;
		const seen = new Set<string>();
		for (const hash of hashes) {
			if (seen.has(hash)) {
				continue;
			}
			seen.add(hash);
			this.cache.del(hash);
			if (this.pendingIndexWrites.delete(hash)) {
				pendingDeleted++;
			} else {
				indexedHashes.push(hash);
			}
		}

		const finish = (indexedDeleted: number) => {
			this._length -= pendingDeleted + indexedDeleted;
			return true;
		};
		if (indexedHashes.length === 0) {
			return finish(0);
		}
		const exactDeleteCountIndex = hasExactDeleteCount(this.properties.index)
			? this.properties.index
			: undefined;
		if (exactDeleteCountIndex) {
			return mapMaybePromise(
				exactDeleteCountIndex.delIdsCount(indexedHashes),
				finish,
			);
		}
		const exactDeleteIndex = hasExactDelete(this.properties.index)
			? this.properties.index
			: undefined;
		if (exactDeleteIndex) {
			return mapMaybePromise(
				exactDeleteIndex.delIds(indexedHashes),
				(deleted) => finish(deleted.length),
			);
		}
		return this.consumeNativeTrimmedEntryHashesNoReturnByQuery(
			indexedHashes,
			finish,
		);
	}

	private async consumeNativeTrimmedEntryHashesNoReturnByQuery(
		indexedHashes: string[],
		finish: (indexedDeleted: number) => boolean,
	): Promise<boolean> {
		const batchSize = 64;
		let deletedCount = 0;
		for (let i = 0; i < indexedHashes.length; i += batchSize) {
			const hashes = indexedHashes.slice(i, i + batchSize);
			const deleted = await this.properties.index.del({
				query: createHashMatchQuery(hashes),
			});
			deletedCount += deleted.length;
		}
		return finish(deletedCount);
	}

	private consumeNativeTrimmedEntryNodesMaybe(
		nodes: ShallowEntry[],
		options?: { skipNextHeadUpdates?: boolean; deleteBlocks?: boolean },
	): MaybePromise<ShallowEntry[]> {
		const indexedByHash = new Map(nodes.map((node) => [node.hash, node]));
		const deletedByHash = new Map<string, ShallowEntry>();
		const indexedHashes: string[] = [];
		for (const node of nodes) {
			this.cache.del(node.hash);
			const pending = this.getPendingIndexWrite(node.hash);
			if (pending) {
				this.pendingIndexWrites.delete(node.hash);
				deletedByHash.set(node.hash, pending);
			} else {
				indexedHashes.push(node.hash);
			}
		}

		const finish = (): MaybePromise<ShallowEntry[]> =>
			this.finishConsumeNativeTrimmedEntries(deletedByHash, nodes, options);

		const exactDeleteIndex: IndexWithExactDelete | undefined = hasExactDelete(
			this.properties.index,
		)
			? (this.properties.index as IndexWithExactDelete)
			: undefined;
		if (indexedHashes.length > 0 && exactDeleteIndex) {
			return mapMaybePromise(
				exactDeleteIndex.delIds(indexedHashes),
				(deleted) => {
					for (const id of deleted) {
						const hash = String(id.primitive);
						const node = indexedByHash.get(hash);
						if (!node || deletedByHash.has(hash)) {
							continue;
						}
						deletedByHash.set(hash, node);
					}
					return finish();
				},
			);
		}
		if (indexedHashes.length > 0) {
			return this.consumeNativeTrimmedEntriesByQuery(
				indexedHashes,
				indexedByHash,
				deletedByHash,
				nodes,
				options,
			);
		}
		return finish();
	}

	private deleteSingleMaybe(
		node: ShallowEntry,
		options?: { skipNextHeadUpdates?: boolean },
	): MaybePromise<ShallowEntry[]> {
		this.cache.del(node.hash);
		const pending = this.getPendingIndexWrite(node.hash);
		if (pending) {
			this.pendingIndexWrites.delete(node.hash);
			return this.finishDeleteSingle(pending, options);
		}

		const exactDeleteIndex: IndexWithExactDelete | undefined = hasExactDelete(
			this.properties.index,
		)
			? (this.properties.index as IndexWithExactDelete)
			: undefined;
		if (exactDeleteIndex) {
			return mapMaybePromise(exactDeleteIndex.delIds([node.hash]), (deleted) =>
				deleted.some((id) => String(id.primitive) === node.hash)
					? this.finishDeleteSingle(node, options)
					: [],
			);
		}
		return mapMaybePromise(
			this.properties.index.del({ query: createHashMatchQuery([node.hash]) }),
			(deleted) =>
				deleted.some((id) => String(id.primitive) === node.hash)
					? this.finishDeleteSingle(node, options)
					: [],
		);
	}

	private finishDeleteSingle(
		node: ShallowEntry,
		options?: { skipNextHeadUpdates?: boolean },
	): MaybePromise<ShallowEntry[]> {
		const afterStoreDelete = (): MaybePromise<ShallowEntry[]> => {
			this._length--;
			this.properties.nativeGraph?.graph.delete(node.hash);
			if (!options?.skipNextHeadUpdates && node.meta.type !== EntryType.CUT) {
				return mapMaybePromise(
					this.privateUpdateNextHeadHashes(node.meta.next, true),
					() => [node],
				);
			}
			return [node];
		};
		return mapMaybePromise(
			this.properties.store.rm(node.hash),
			afterStoreDelete,
		);
	}

	private async deleteManyByQuery(
		indexedHashes: string[],
		indexedByHash: Map<string, ShallowEntry>,
		deletedByHash: Map<string, ShallowEntry>,
		nodes: ShallowEntry[],
		storeHashes: string[],
		options?: { skipNextHeadUpdates?: boolean },
	): Promise<ShallowEntry[]> {
		const batchSize = 64;
		for (let i = 0; i < indexedHashes.length; i += batchSize) {
			const hashes = indexedHashes.slice(i, i + batchSize);
			const deleted = await this.properties.index.del({
				query: createHashMatchQuery(hashes),
			});
			for (const id of deleted) {
				const hash = String(id.primitive);
				const node = indexedByHash.get(hash);
				if (!node || deletedByHash.has(hash)) {
					continue;
				}
				deletedByHash.set(hash, node);
				storeHashes.push(hash);
			}
		}
		return this.finishDeleteMany(deletedByHash, nodes, storeHashes, options);
	}

	private async consumeNativeTrimmedEntriesByQuery(
		indexedHashes: string[],
		indexedByHash: Map<string, ShallowEntry>,
		deletedByHash: Map<string, ShallowEntry>,
		nodes: ShallowEntry[],
		options?: { skipNextHeadUpdates?: boolean; deleteBlocks?: boolean },
	): Promise<ShallowEntry[]> {
		const batchSize = 64;
		for (let i = 0; i < indexedHashes.length; i += batchSize) {
			const hashes = indexedHashes.slice(i, i + batchSize);
			const deleted = await this.properties.index.del({
				query: createHashMatchQuery(hashes),
			});
			for (const id of deleted) {
				const hash = String(id.primitive);
				const node = indexedByHash.get(hash);
				if (!node || deletedByHash.has(hash)) {
					continue;
				}
				deletedByHash.set(hash, node);
			}
		}
		return this.finishConsumeNativeTrimmedEntries(
			deletedByHash,
			nodes,
			options,
		);
	}

	private finishConsumeNativeTrimmedEntries(
		deletedByHash: Map<string, ShallowEntry>,
		nodes: ShallowEntry[],
		options?: { skipNextHeadUpdates?: boolean; deleteBlocks?: boolean },
	): MaybePromise<ShallowEntry[]> {
		const deleted = nodes
			.map((node) => deletedByHash.get(node.hash))
			.filter((node): node is ShallowEntry => !!node);
		if (deleted.length === 0) {
			return [];
		}

		const afterStoreDelete = () => {
			this._length -= deleted.length;
			if (!options?.skipNextHeadUpdates) {
				const deletedNexts: string[] = [];
				for (const node of deleted) {
					if (node.meta.type !== EntryType.CUT) {
						deletedNexts.push(...node.meta.next);
					}
				}
				return mapMaybePromise(
					this.privateUpdateNextHeadHashes(deletedNexts, true),
					() => deleted,
				);
			}
			return deleted;
		};

		if (options?.deleteBlocks) {
			const store = this.properties.store;
			const hashes = deleted.map((node) => node.hash);
			const deleteResult =
				hasRmMany(store) && store.rmMany
					? store.rmMany(hashes)
					: Promise.all(hashes.map((hash) => store.rm(hash))).then(() => {});
			return mapMaybePromise(deleteResult, afterStoreDelete);
		}
		return afterStoreDelete();
	}

	private finishDeleteMany(
		deletedByHash: Map<string, ShallowEntry>,
		nodes: ShallowEntry[],
		storeHashes: string[],
		options?: { skipNextHeadUpdates?: boolean },
	): MaybePromise<ShallowEntry[]> {
		const deleted = nodes
			.map((node) => deletedByHash.get(node.hash))
			.filter((node): node is ShallowEntry => !!node);
		if (deleted.length === 0) {
			return [];
		}

		const store = this.properties.store;
		const afterStoreDelete = () => {
			this._length -= deleted.length;
			const graph = this.properties.nativeGraph?.graph;
			if (graph?.deleteMany) {
				graph.deleteMany(storeHashes);
			} else {
				for (const hash of storeHashes) {
					graph?.delete(hash);
				}
			}
			if (!options?.skipNextHeadUpdates) {
				const deletedNexts: string[] = [];
				for (const node of deleted) {
					if (node.meta.type !== EntryType.CUT) {
						deletedNexts.push(...node.meta.next);
					}
				}
				return mapMaybePromise(
					this.privateUpdateNextHeadHashes(deletedNexts, true),
					() => deleted,
				);
			}
			return deleted;
		};
		if (hasRmMany(store) && store.rmMany) {
			return mapMaybePromise(store.rmMany(storeHashes), afterStoreDelete);
		}
		return Promise.all(storeHashes.map((hash) => store.rm(hash))).then(
			afterStoreDelete,
		);
	}

	private nativeLogEntryToShallowEntry(entry: NativeLogEntry): ShallowEntry {
		return new ShallowEntry({
			hash: entry.hash,
			head: entry.head ?? false,
			payloadSize: entry.payloadSize ?? 0,
			meta: new ShallowMeta({
				gid: entry.gid,
				next: entry.next,
				type: entry.type,
				data: entry.data,
				clock: new Clock({
					id: this.properties.publicKey.bytes,
					timestamp: new Timestamp({
						wallTime: BigInt(entry.clock.timestamp.wallTime),
						logical: entry.clock.timestamp.logical ?? 0,
					}),
				}),
			}),
		});
	}

	private nativeTrimmedHashToShallowEntry(hash: string): ShallowEntry {
		return new ShallowEntry({
			hash,
			head: false,
			payloadSize: 0,
			meta: new ShallowMeta({
				gid: "",
				next: [],
				type: EntryType.APPEND,
				clock: new Clock({
					id: this.properties.publicKey.bytes,
					timestamp: new Timestamp({
						wallTime: 0n,
						logical: 0,
					}),
				}),
			}),
		});
	}

	nativeLogEntriesToShallowEntries(entries: NativeLogEntry[]): ShallowEntry[] {
		return entries.map((entry) => this.nativeLogEntryToShallowEntry(entry));
	}

	async getMemoryUsage() {
		if (this.properties.nativeGraph) {
			return this.properties.nativeGraph.graph.payloadSizeSum();
		}
		const indexed =
			(await this.properties.index.sum({ key: "payloadSize" })) || 0;
		let pending = 0;
		for (const [hash, write] of this.pendingIndexWrites) {
			pending +=
				this.materializePendingIndexWrite(hash, write).payloadSize || 0;
		}
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
		const hashes = [...new Set(nexts.filter(Boolean))];
		if (hashes.length === 0) {
			return;
		}
		const existingNexts =
			isHead && this.properties.nativeGraph
				? this.properties.nativeGraph.graph.hasMany(hashes)
				: undefined;
		for (const next of hashes) {
			const pending = this.getPendingIndexWrite(next);
			if (!pending && existingNexts && !existingNexts.has(next)) {
				continue;
			}
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

const createHashMatchQuery = (hashes: string[]): Query =>
	hashes.length === 1
		? new StringMatch({
				key: "hash",
				value: hashes[0]!,
				caseInsensitive: false,
				method: StringMatchMethod.exact,
			})
		: new Or(
				hashes.map(
					(hash) =>
						new StringMatch({
							key: "hash",
							value: hash,
							caseInsensitive: false,
							method: StringMatchMethod.exact,
						}),
				),
			);

const toNativeLogEntry = (entry: ShallowEntry): NativeLogEntry => ({
	hash: entry.hash,
	gid: entry.meta.gid,
	next: entry.meta.next,
	type: entry.meta.type,
	head: entry.head,
	payloadSize: entry.payloadSize,
	data: entry.meta.data,
	clock: {
		timestamp: {
			wallTime: entry.meta.clock.timestamp.wallTime,
			logical: entry.meta.clock.timestamp.logical,
		},
	},
});

const getResolveShape = (options: MaybeResolveOptions | undefined) =>
	options && options !== true && options.type === "shape"
		? options.shape
		: undefined;

const isHeadHashOnlyShape = (shape: Shape) => {
	const keys = Object.keys(shape);
	return keys.length === 1 && shape.hash === true;
};

const isHeadDataShape = (shape: Shape) => {
	const keys = Object.keys(shape);
	if (keys.some((key) => key !== "hash" && key !== "meta")) {
		return false;
	}
	if (shape.hash !== undefined && shape.hash !== true) {
		return false;
	}
	const meta = shape.meta as Shape | undefined;
	if (!meta || typeof meta !== "object") {
		return false;
	}
	const metaKeys = Object.keys(meta);
	return metaKeys.length === 1 && meta.data === true;
};

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
