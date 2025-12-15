import { type AbstractType, field, serialize, variant } from "@dao-xyz/borsh";
import type { PeerId, TypedEventTarget } from "@libp2p/interface";
import type { Multiaddr } from "@multiformats/multiaddr";
import { Cache } from "@peerbit/cache";
import {
	type MaybePromise,
	PublicSignKey,
	sha256Base64Sync,
} from "@peerbit/crypto";
import * as types from "@peerbit/document-interface";
import { CachedIndex, type QueryCacheOptions } from "@peerbit/indexer-cache";
import * as indexerTypes from "@peerbit/indexer-interface";
import { HashmapIndex } from "@peerbit/indexer-simple";
import { BORSH_ENCODING, type Encoding, Entry } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { ClosedError, Program } from "@peerbit/program";
import {
	MissingResponsesError,
	RPC,
	type RPCRequestAllOptions,
	type RPCResponse,
	queryAll,
} from "@peerbit/rpc";
import {
	type CoverRange,
	type ReplicationDomain,
	SharedLog,
} from "@peerbit/shared-log";
import {
	DataMessage,
	type PeerRefs,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError, waitFor } from "@peerbit/time";
import pDefer, { type DeferredPromise } from "p-defer";
import { concat, fromString } from "uint8arrays";
import { copySerialization } from "./borsh.js";
import { MAX_BATCH_SIZE } from "./constants.js";
import type { DocumentEvents, DocumentsChange } from "./events.js";
import type { QueryPredictor } from "./most-common-query-predictor.js";
import MostCommonQueryPredictor, {
	idAgnosticQueryKey,
} from "./most-common-query-predictor.js";
import { type Operation, isPutOperation } from "./operation.js";
import { Prefetch } from "./prefetch.js";
import type { ExtractArgs } from "./program.js";
import { ResumableIterators } from "./resumable-iterator.js";

const WARNING_WHEN_ITERATING_FOR_MORE_THAN = 1e5;

const logger = loggerFn("peerbit:program:document:search");
const warn = logger.newScope("warn");
const documentIndexLogger = loggerFn("peerbit:document:index");
const indexLifecycleLogger = documentIndexLogger.newScope("lifecycle");
const indexRpcLogger = documentIndexLogger.newScope("rpc");
const indexCacheLogger = documentIndexLogger.newScope("cache");
const indexPrefetchLogger = documentIndexLogger.newScope("prefetch");
const indexIteratorLogger = documentIndexLogger.newScope("iterate");

type BufferedResult<T, I extends Record<string, any>> = {
	value: T;
	indexed: WithContext<I>;
	context: types.Context;
	from: PublicSignKey;
};

export type UpdateMergeStrategy<
	T,
	I,
	Resolve extends boolean | undefined,
	_RT = ValueTypeFromRequest<Resolve, T, I>,
> =
	| boolean
	| {
			filter?: (
				evt: DocumentsChange<T, I>,
			) => MaybePromise<DocumentsChange<T, I> | void>;
	  };
export type UpdateReason = "initial" | "manual" | "join" | "change" | "push";

export type UpdateCallbacks<
	T,
	I,
	Resolve extends boolean | undefined,
	RT = ValueTypeFromRequest<Resolve, T, I>,
> = {
	/**
	 * Fires whenever the iterator detects new work (e.g. push, join, change).
	 * Ideal for reactive consumers that need to call `next()` or trigger UI work.
	 */
	notify?: (reason: UpdateReason) => void | Promise<void>;

	/**
	 * Fires whenever the iterator yields a batch to the consumer.
	 * Good for external sync (e.g. React state).
	 */
	onBatch?: (
		batch: RT[],
		meta: { reason: UpdateReason },
	) => void | Promise<void>;
};

/**
 * Unified update options for iterate()/search()/get() and hooks.
 * If you pass `true`, defaults to `{ merge: "sorted" }`.
 */
export type UpdateModeShortcut = "local" | "remote" | "all";

export type UpdateOptions<T, I, Resolve extends boolean | undefined> =
	| boolean
	| UpdateModeShortcut
	| ({
			/** Live update behavior. Only sorted merging is supported; optional filter can mutate/ignore events. */
			merge?: UpdateMergeStrategy<T, I, Resolve>;
			/** Request push-style notifications backed by the prefetch channel. */
			push?: boolean | types.PushUpdatesMode;
	  } & UpdateCallbacks<T, I, Resolve>);

export type JoiningTargets = {
	/** Specific peers you care about */
	peers?: Array<PublicSignKey | PeerId | string>; // string = hash or peer id

	/** Multiaddrs you care about */
	multiaddrs?: (string | Multiaddr)[];

	/**
	 * From the previous cover set (what you "knew" about earlier).
	 * - "any": wait until at least 1 of the known peers is ready
	 * - "all": wait until all known peers are ready
	 * - number: wait until N known peers are ready
	 */
	known?: "any" | "all" | number;
};

export type JoiningTimeoutPolicy = "proceed" | "error";

export type JoiningOnMissedResults = (evt: {
	/** How many items should have preceded the current frontier. */
	amount: number;

	/** The peer whose arrival triggered the gap calculation. */
	peer: PublicSignKey;
}) => void | Promise<void>;

export type OutOfOrderMode = "drop" | "queue";

export type LateResultsItem = {
	indexed: WithContext<any>;
	context: types.Context;
	from: PublicSignKey;
	value?: any;
};

export type LateResultsEvent<
	M extends OutOfOrderMode = "drop",
	Item = LateResultsItem,
> = {
	/** Count of items that should have appeared earlier than the current frontier */
	amount: number;

	/** If attributable, the peer that produced the late items */
	peer?: PublicSignKey;
} & (M extends "queue" ? { items: Item[] } : { items?: undefined });

export type LateResultsHelpers<
	M extends OutOfOrderMode = "drop",
	Item = LateResultsItem,
> = {
	/** Collect concrete late items if available for the chosen mode */
	collect: () => Promise<M extends "queue" ? Item[] : Item[] | undefined>;
};

export type WaitBehavior =
	| "block" // hold the *first* fetch until readiness condition is met or timeout
	| "keep-open"; // return immediately; iterator stays open listening for late peers

export type WaitPolicy = {
	timeout: number; // max time to wait
	until?: "any"; // readiness condition, TODO more options like "cover" (to wait for this.log.watiForReplicators)
	onTimeout?: "proceed" | "error"; // proceed = continue with whoever's ready
	behavior?: WaitBehavior; // default: "keep-open"
};

export type ReachScope = {
	/** who to consider for readiness */
	eager?: boolean; // not yet matured
	discover?: PublicSignKey[]; // wait for these peers to be ready, assumes they are already in the dialqueue or connected, but not actively subscribing yet
};

export type RemoteQueryOptions<Q, R, D> = RPCRequestAllOptions<Q, R> & {
	replicate?: boolean;
	minAge?: number;
	throwOnMissing?: boolean;
	strategy?: "fallback";
	domain?:
		| {
				args: ExtractArgs<D>;
		  }
		| {
				range: CoverRange<number | bigint>;
		  };
	/** WHO can answer? How do we grow the candidate set? */
	reach?: ReachScope;
	/** WHEN are we allowed to proceed? Quorum semantics over a chosen group. */
	wait?: WaitPolicy;
};

export type QueryOptions<T, I, D, Resolve extends boolean | undefined> = {
	remote?:
		| boolean
		| RemoteQueryOptions<
				types.AbstractSearchRequest,
				types.AbstractSearchResult,
				D
		  >;
	local?: boolean;
	resolve?: Resolve;
	signal?: AbortSignal;
	updates?: UpdateOptions<T, I, Resolve>;
	outOfOrder?:
		| {
				mode?: "drop";
				handle?: (
					evt: LateResultsEvent<"drop">,
					helpers: LateResultsHelpers<"drop">,
				) => void | Promise<void>;
		  }
		| {
				mode: "queue";
				handle?: (
					evt: LateResultsEvent<"queue">,
					helpers: LateResultsHelpers<"queue">,
				) => void | Promise<void>;
		  };
	/**
	 * Controls iterator liveness after batches are consumed.
	 * - 'onEmpty' (default): close when no more results
	 * - 'manual': keep open until iterator.close() or program close; good for live updates
	 */
	closePolicy?: "onEmpty" | "manual";
};

export type GetOptions<_T, _I, D, Resolve extends boolean | undefined> = {
	remote?:
		| boolean
		| RemoteQueryOptions<
				types.AbstractSearchRequest,
				types.AbstractSearchResult,
				D
		  >;
	local?: boolean;
	resolve?: Resolve;
	signal?: AbortSignal;
	waitFor?: number; // how long to wait for a non-empty result set
};

export type SearchOptions<
	T,
	I,
	D,
	Resolve extends boolean | undefined,
> = QueryOptions<T, I, D, Resolve>;

type Transformer<T, I> = (obj: T, context: types.Context) => MaybePromise<I>;

export type ResultsIterator<T> = {
	close: () => Promise<void>;
	next: (number: number) => Promise<T[]>;
	done: () => boolean;
	all: () => Promise<T[]>;
	pending: () => MaybePromise<number | undefined>;
	first: () => Promise<T | undefined>;
	[Symbol.asyncIterator]: () => AsyncIterator<T>;
};

type QueryDetailedOptions<
	T,
	I,
	D,
	Resolve extends boolean | undefined,
> = QueryOptions<T, I, D, Resolve> & {
	onResponse?: (
		response: types.AbstractSearchResult,
		from: PublicSignKey,
	) => void | Promise<void>;
	remote?: {
		from?: string[]; // if specified, only query these peers
	};
	fetchFirstForRemote?: Set<string>;
};

type QueryLike = {
	query?: indexerTypes.Query[] | indexerTypes.QueryLike;
	sort?: indexerTypes.Sort[] | indexerTypes.Sort | indexerTypes.SortLike;
};

type ExtractResolveFromOptions<O> =
	O extends QueryOptions<any, any, any, infer X>
		? X extends boolean // if X is a boolean (true or false)
			? X
			: true // else default to true
		: true; // if R isn't QueryLike at all, default to true

const coerceQuery = <Resolve extends boolean | undefined>(
	query:
		| types.SearchRequest
		| types.SearchRequestIndexed
		| types.IterationRequest
		| QueryLike,
	options?: QueryOptions<any, any, any, Resolve>,
	compatibility?: number,
):
	| types.SearchRequest
	| types.SearchRequestIndexed
	| types.IterationRequest => {
	const replicate =
		typeof options?.remote !== "boolean" ? options?.remote?.replicate : false;
	const shouldResolve = options?.resolve !== false;
	const useLegacyRequests = compatibility != null && compatibility <= 9;

	if (
		query instanceof types.SearchRequestIndexed &&
		query.replicate === false &&
		replicate
	) {
		query.replicate = true;
		return query;
	}

	if (
		query instanceof types.SearchRequest ||
		query instanceof types.SearchRequestIndexed
	) {
		return query;
	}

	if (query instanceof types.IterationRequest) {
		if (useLegacyRequests) {
			if (query.resolve === false) {
				return new types.SearchRequestIndexed({
					query: query.query,
					sort: query.sort,
					fetch: query.fetch,
					replicate: query.replicate ?? replicate,
				});
			}
			return new types.SearchRequest({
				query: query.query,
				sort: query.sort,
				fetch: query.fetch,
			});
		}
		return query;
	}

	const queryObject = query as QueryLike;

	if (useLegacyRequests) {
		if (shouldResolve) {
			return new types.SearchRequest({
				query: indexerTypes.toQuery(queryObject.query),
				sort: indexerTypes.toSort(queryObject.sort),
			});
		}
		return new types.SearchRequestIndexed({
			query: indexerTypes.toQuery(queryObject.query),
			sort: indexerTypes.toSort(queryObject.sort),
			replicate,
		});
	}

	return new types.IterationRequest({
		query: indexerTypes.toQuery(queryObject.query),
		sort: indexerTypes.toSort(queryObject.sort),
		fetch: 10,
		resolve: shouldResolve,
		replicate: shouldResolve ? false : replicate,
	});
};

const introduceEntries = async <
	T,
	I,
	D,
	R extends
		| types.SearchRequest
		| types.SearchRequestIndexed
		| types.IterationRequest,
>(
	queryRequest: R,
	responses: { response: types.AbstractSearchResult; from?: PublicSignKey }[],
	documentType: AbstractType<T>,
	indexedType: AbstractType<I>,
	sync: (request: R, response: types.Results<any>) => Promise<void>,
	options?: QueryDetailedOptions<T, I, D, any>,
): Promise<
	RPCResponse<types.Results<types.ResultTypeFromRequest<R, T, I>>>[]
> => {
	const results: RPCResponse<types.Results<any>>[] = [];
	for (const response of responses) {
		if (!response.from) {
			logger.error("Missing from for response");
		}

		if (response.response instanceof types.Results) {
			response.response.results.forEach((r) =>
				r instanceof types.ResultValue
					? r.init(documentType)
					: r.init(indexedType),
			);
			if (typeof options?.remote !== "boolean" && options?.remote?.replicate) {
				await sync(queryRequest, response.response);
			}
			options?.onResponse &&
				(await options.onResponse(response.response, response.from!)); // TODO fix types
			results.push(response as RPCResponse<types.Results<any>>);
		} else if (response.response instanceof types.NoAccess) {
			logger.error("Search resulted in access error");
		} else {
			throw new Error("Unsupported");
		}
	}
	return results;
};

const dedup = <T>(
	allResult: T[],
	dedupBy: (obj: any) => string | Uint8Array | number | bigint,
) => {
	const unique: Set<indexerTypes.IdPrimitive> = new Set();
	const dedup: T[] = [];
	for (const result of allResult) {
		const key = indexerTypes.toId(dedupBy(result));
		const primitive = key.primitive;
		if (unique.has(primitive)) {
			continue;
		}
		unique.add(primitive);
		dedup.push(result);
	}
	return dedup;
};

type AnyIterationRequest =
	| types.SearchRequest
	| types.SearchRequestIndexed
	| types.IterationRequest;

const resolvesDocuments = (req?: AnyIterationRequest) => {
	if (!req) {
		return true;
	}
	if (req instanceof types.SearchRequestIndexed) {
		return false;
	}
	if (req instanceof types.IterationRequest) {
		return req.resolve !== false;
	}
	return true;
};

const replicatesIndex = (req?: AnyIterationRequest) => {
	if (!req) {
		return false;
	}
	if (req instanceof types.SearchRequestIndexed) {
		return req.replicate === true;
	}
	if (req instanceof types.IterationRequest) {
		return req.replicate === true;
	}
	return false;
};

function isSubclassOf(
	SubClass: AbstractType<any>,
	SuperClass: AbstractType<any>,
) {
	// Start with the immediate parent of SubClass
	let proto = Object.getPrototypeOf(SubClass);

	while (proto) {
		if (proto === SuperClass) {
			return true;
		}
		proto = Object.getPrototypeOf(proto);
	}

	return false;
}

const DEFAULT_TIMEOUT = 1e4;
const DEFAULT_KEEP_REMOTE_ITERATOR_TIMEOUT = 3e5;
const DISCOVER_TIMEOUT_FALLBACK = 500;

const DEFAULT_INDEX_BY = "id";

export type CanSearch = (
	request:
		| types.SearchRequest
		| types.IterationRequest
		| types.CollectNextRequest,
	from: PublicSignKey,
) => Promise<boolean> | boolean;

export type CanRead<T> = (
	result: T,
	from: PublicSignKey,
) => Promise<boolean> | boolean;

export type CanReadIndexed<I> = (
	result: I,
	from: PublicSignKey,
) => Promise<boolean> | boolean;

export type WithContext<I> = {
	__context: types.Context;
} & I;

export type WithIndexed<T, I> = {
	// experimental, used to quickly get the indexed representation
	__indexed: I;
} & T;

export type WithIndexedContext<T, I> = WithContext<WithIndexed<T, I>>;

export type ValueTypeFromRequest<
	Resolve extends boolean | undefined,
	T,
	I,
> = Resolve extends false ? WithContext<I> : WithIndexedContext<T, I>;

export type TransformerAsConstructor<T, I> = {
	type?: new (arg: T, context: types.Context) => I;
};

export type TransformerAsFunction<T, I> = {
	type: AbstractType<I>;
	transform: (arg: T, context: types.Context) => I | Promise<I>;
};
export type TransformOptions<T, I> =
	| TransformerAsConstructor<T, I>
	| TransformerAsFunction<T, I>;

const isTransformerWithFunction = <T, I>(
	options: TransformOptions<T, I>,
): options is TransformerAsFunction<T, I> => {
	return (options as TransformerAsFunction<T, I>).transform != null;
};

export type PrefetchOptions = {
	predictor?: QueryPredictor;
	ttl: number;
	accumulator: Prefetch;

	/* When `true` we assume every peer supports prefetch routing,
	 * so it is safe to drop SearchRequests that the predictor marks
	 * as `ignore === true`.
	 *
	 * Default: `false` – be conservative.
	 */
	strict?: boolean;
};
export type OpenOptions<
	T,
	I,
	D extends ReplicationDomain<any, Operation, any>,
> = {
	documentEvents: TypedEventTarget<DocumentEvents<T, I>>;
	documentType: AbstractType<T>;
	dbType: AbstractType<types.IDocumentStore<T>>;
	log: SharedLog<Operation, D, any>;
	canRead?: CanRead<I>;
	canSearch?: CanSearch;
	replicate: (
		request:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest,
		results: types.Results<
			types.ResultTypeFromRequest<
				| types.SearchRequest
				| types.SearchRequestIndexed
				| types.IterationRequest,
				T,
				I
			>
		>,
	) => Promise<void>;
	indexBy?: string | string[];
	transform?: TransformOptions<T, I>;
	cache?: {
		resolver?: number;
		query?: QueryCacheOptions;
	};
	compatibility: 6 | 7 | 8 | 9 | undefined;
	maybeOpen: (value: T & Program) => Promise<T & Program>;
	prefetch?: boolean | Partial<PrefetchOptions>;
	includeIndexed?: boolean; // if true, indexed representations will always be included in the search results
};

type IndexableClass<I> = new (
	value: I,
	context: types.Context,
) => WithContext<I>;

export const coerceWithContext = <T>(
	value: T | WithContext<T>,
	context: types.Context,
): WithContext<T> => {
	let valueWithContext: WithContext<T> = value as any;
	valueWithContext.__context = context;
	return valueWithContext;
};

export const coerceWithIndexed = <T, I>(
	value: T | WithIndexed<T, I>,
	indexed: I,
): WithIndexed<T, I> => {
	let valueWithContext: WithIndexed<T, I> = value as any;
	valueWithContext.__indexed = indexed;
	return valueWithContext;
};

@variant("documents_index")
export class DocumentIndex<
	T,
	I extends Record<string, any>,
	D extends ReplicationDomain<any, Operation, any>,
> extends Program<OpenOptions<T, I, D>> {
	@field({ type: RPC })
	_query: RPC<types.AbstractSearchRequest, types.AbstractSearchResult>;

	// Original document representation
	documentType: AbstractType<T>;

	// transform options
	transformer: Transformer<T, I>;

	// The indexed document wrapped in a context
	wrappedIndexedType: IndexableClass<I>;
	indexedType: AbstractType<I>;

	// The database type, for recursive indexing
	dbType: AbstractType<types.IDocumentStore<T>>;
	indexedTypeIsDocumentType: boolean;

	// Index key
	private indexBy: string[];
	private indexByResolver: (obj: any) => string | Uint8Array;
	index: indexerTypes.Index<WithContext<I>>;
	private _resumableIterators: ResumableIterators<WithContext<I>>;
	private _prefetch?: PrefetchOptions | undefined;
	private includeIndexed: boolean | undefined = undefined;

	compatibility: 6 | 7 | 8 | 9 | undefined;

	// Transformation, indexer
	/* fields: IndexableFields<T, I>; */

	private _valueEncoding: Encoding<T>;

	private _sync: <V extends types.ResultValue<T> | types.ResultIndexedValue<I>>(
		request:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest,
		results: types.Results<V>,
	) => Promise<void>;

	private _log: SharedLog<Operation, D, any>;

	private _resolverProgramCache?: Map<string | number | bigint, T>;
	private _resolverCache?: Cache<T>;
	private isProgramValued: boolean;
	private _maybeOpen: (value: T & Program) => Promise<T & Program>;
	private canSearch?: CanSearch;
	private canRead?: CanRead<I>;

	private documentEvents: TypedEventTarget<DocumentEvents<T, I>>;

	private _joinListener?: (e: { detail: PublicSignKey }) => Promise<void>;

	private _resultQueue: Map<
		string,
		{
			from: PublicSignKey;
			keptInIndex: number;
			timeout: ReturnType<typeof setTimeout>;
			queue: indexerTypes.IndexedResult<WithContext<I>>[];
			fromQuery:
				| types.SearchRequest
				| types.SearchRequestIndexed
				| types.IterationRequest;
			resolveResults?: boolean;
			pushMode?: types.PushUpdatesMode;
			pushInFlight?: boolean;
		}
	>;
	private iteratorKeepAliveTimers?: Map<string, ReturnType<typeof setTimeout>>;

	constructor(properties?: {
		query?: RPC<types.AbstractSearchRequest, types.AbstractSearchResult>;
	}) {
		super();
		this._query = properties?.query || new RPC();
		this.iteratorKeepAliveTimers = new Map();
	}

	get valueEncoding() {
		return this._valueEncoding;
	}

	private ensurePrefetchAccumulator() {
		if (!this._prefetch) {
			this._prefetch = {
				accumulator: new Prefetch(),
				ttl: 5e3,
			};
			return;
		}
		if (!this._prefetch.accumulator) {
			this._prefetch.accumulator = new Prefetch();
		}
	}

	private async wrapPushResults(
		matches: Array<WithContext<T> | WithContext<I>>,
		resolve: boolean,
	): Promise<types.Result[]> {
		if (!matches.length) return [];
		const results: types.Result[] = [];
		for (const match of matches) {
			if (resolve) {
				const doc = match as WithContext<T>;
				const indexedValue = await this.transformer(doc as T, doc.__context);
				const wrappedIndexed = coerceWithContext(indexedValue, doc.__context);
				results.push(
					new types.ResultValue({
						context: doc.__context,
						value: doc as T,
						source: serialize(doc as T),
						indexed: wrappedIndexed,
					}),
				);
			} else {
				const indexed = match as WithContext<I>;
				const head = await this._log.log.get(indexed.__context.head);
				results.push(
					new types.ResultIndexedValue({
						context: indexed.__context,
						source: serialize(indexed as I),
						indexed: indexed as I,
						entries: head ? [head] : [],
					}),
				);
			}
		}
		return results;
	}

	private async drainQueuedResults(
		queueEntries: indexerTypes.IndexedResult<WithContext<I>>[],
		resolve: boolean,
	): Promise<types.Result[]> {
		if (!queueEntries.length) {
			return [];
		}
		const drained = queueEntries.splice(0);
		const results: types.Result[] = [];
		for (const entry of drained) {
			const indexedUnwrapped = Object.assign(
				Object.create(this.indexedType.prototype),
				entry.value,
			);
			if (resolve) {
				const value = await this.resolveDocument({
					indexed: entry.value,
					head: entry.value.__context.head,
				});
				if (!value) continue;
				results.push(
					new types.ResultValue({
						context: entry.value.__context,
						value: value.value,
						source: serialize(value.value),
						indexed: indexedUnwrapped,
					}),
				);
			} else {
				const head = await this._log.log.get(entry.value.__context.head);
				results.push(
					new types.ResultIndexedValue({
						context: entry.value.__context,
						source: serialize(indexedUnwrapped),
						indexed: indexedUnwrapped,
						entries: head ? [head] : [],
					}),
				);
			}
		}
		return results;
	}

	private handleDocumentChange = async (
		event: CustomEvent<DocumentsChange<T, I>>,
	) => {
		const added = event.detail.added;
		if (!added.length) {
			return;
		}

		for (const [_iteratorId, queue] of this._resultQueue) {
			if (
				!queue.pushMode ||
				queue.pushMode !== types.PushUpdatesMode.STREAM ||
				queue.pushInFlight
			) {
				continue;
			}
			if (!(queue.fromQuery instanceof types.IterationRequest)) {
				continue;
			}
			queue.pushInFlight = true;
			try {
				const resolveFlag =
					queue.resolveResults ??
					resolvesDocuments(queue.fromQuery as AnyIterationRequest);
				const batches: types.Result[] = [];
				const queued = await this.drainQueuedResults(queue.queue, resolveFlag);
				if (queued.length) {
					batches.push(...queued);
				}
				// TODO drain only up to the changed document instead of flushing the entire queue
				const matches = await this.updateResults(
					[],
					{ added },
					{
						query: queue.fromQuery.query,
						sort: queue.fromQuery.sort,
					},
					resolveFlag,
				);
				if (matches.length) {
					const wrapped = await this.wrapPushResults(matches, resolveFlag);
					if (wrapped.length) {
						batches.push(...wrapped);
					}
				}
				if (!batches.length) {
					continue;
				}
				const pushMessage = new types.PredictedSearchRequest({
					id: queue.fromQuery.id,
					request: queue.fromQuery,
					results: new types.Results({
						results: batches,
						kept: 0n,
					}),
				});
				await this._query.send(pushMessage, {
					mode: new SilentDelivery({
						to: [queue.from],
						redundancy: 1,
					}),
				});
			} catch (error) {
				logger.error("Failed to push iterator update", error);
			} finally {
				queue.pushInFlight = false;
			}
		}
	};

	private get nestedProperties() {
		return {
			match: (obj: any): obj is types.IDocumentStore<any> =>
				obj instanceof this.dbType,
			iterate: async (
				obj: types.IDocumentStore<any>,
				query: indexerTypes.IterateOptions,
			) => obj.index.search(query),
		};
	}
	async open(properties: OpenOptions<T, I, D>) {
		this._log = properties.log;
		// Allow reopening with partial options (tests override the index transform)
		const previousEvents = this.documentEvents;
		this.documentEvents =
			properties.documentEvents ?? previousEvents ?? (this.events as any);
		this.compatibility =
			properties.compatibility !== undefined
				? properties.compatibility
				: this.compatibility;

		let prefectOptions =
			typeof properties.prefetch === "object"
				? properties.prefetch
				: properties.prefetch
					? {}
					: undefined;
		this._prefetch = prefectOptions
			? {
					...prefectOptions,
					predictor:
						prefectOptions.predictor || new MostCommonQueryPredictor(3),
					ttl: prefectOptions.ttl ?? 5e3,
					accumulator: prefectOptions.accumulator || new Prefetch(),
				}
			: undefined;

		this.documentType = properties.documentType;
		this.indexedTypeIsDocumentType =
			!properties.transform?.type ||
			properties.transform?.type === properties.documentType;
		this.canRead = properties.canRead;
		this.canSearch = properties.canSearch;
		this.includeIndexed = properties.includeIndexed;

		@variant(0)
		class IndexedClassWithContext {
			@field({ type: types.Context })
			__context: types.Context;

			constructor(value: I, context: types.Context) {
				Object.assign(this, value);
				this.__context = context;
			}
		}

		// copy all prototype values from indexedType to IndexedClassWithContext
		this.indexedType = (properties.transform?.type || properties.documentType)!;
		copySerialization(this.indexedType, IndexedClassWithContext);

		this.wrappedIndexedType = IndexedClassWithContext as new (
			value: I,
			context: types.Context,
		) => WithContext<I>;

		// if this.type is a class that extends Program we want to do special functionality
		this.isProgramValued = isSubclassOf(this.documentType, Program);
		this.dbType = properties.dbType;
		this._resultQueue = new Map();
		const replicateFn =
			properties.replicate ?? this._sync ?? (() => Promise.resolve());
		this._sync = (request, results) => {
			let rq:
				| types.SearchRequest
				| types.SearchRequestIndexed
				| types.IterationRequest;
			let rs: types.Results<
				types.ResultTypeFromRequest<
					| types.SearchRequest
					| types.SearchRequestIndexed
					| types.IterationRequest,
					T,
					I
				>
			>;
			if (request instanceof types.PredictedSearchRequest) {
				// TODO is this codepath even reachable?
				throw new Error("Unexpected PredictedSearchRequest in sync operation");
			} else {
				rq = request;
				rs = results as types.Results<
					types.ResultTypeFromRequest<
						| types.SearchRequest
						| types.SearchRequestIndexed
						| types.IterationRequest,
						T,
						I
					>
				>;
			}
			return replicateFn(rq, rs);
		};

		const transformOptions = properties.transform;
		this.transformer = transformOptions
			? isTransformerWithFunction(transformOptions)
				? (obj, context) => transformOptions.transform(obj, context)
				: transformOptions.type
					? (obj, context) => new transformOptions.type!(obj, context)
					: (obj) => obj as any as I
			: (obj) => obj as any as I; // TODO types

		const maybeArr = properties.indexBy || DEFAULT_INDEX_BY;
		this.indexBy = Array.isArray(maybeArr) ? maybeArr : [maybeArr];
		this.indexByResolver = (obj: any) =>
			indexerTypes.extractFieldValue(obj, this.indexBy);

		this._valueEncoding = BORSH_ENCODING(this.documentType);

		this._resolverCache =
			properties.cache?.resolver === 0
				? undefined
				: new Cache({ max: properties.cache?.resolver ?? 100 }); // TODO choose limit better by default (adaptive)

		this.index =
			(await (
				await this.node.indexer.scope(
					sha256Base64Sync(
						concat([this._log.log.id, fromString("/document-index")]),
					),
				)
			).init({
				indexBy: this.indexBy,
				schema: this.wrappedIndexedType,
				nested: this.nestedProperties,
				/* maxBatchSize: MAX_BATCH_SIZE */
			})) || new HashmapIndex<WithContext<I>>();

		if (properties.cache?.query) {
			this.index = new CachedIndex(this.index, properties.cache.query);
		}

		indexLifecycleLogger("opened document index", {
			peer: this.node.identity.publicKey.hashcode(),
			indexBy: this.indexBy,
			includeIndexed: this.includeIndexed === true,
			cacheResolver: Boolean(this._resolverCache),
			prefetch: Boolean(this.prefetch),
		});

		this._resumableIterators = new ResumableIterators(this.index);
		this._maybeOpen = properties.maybeOpen;
		if (this.isProgramValued) {
			this._resolverProgramCache = new Map();
		}

		if (this.prefetch?.predictor) {
			indexPrefetchLogger("prefetch predictor enabled", {
				peer: this.node.identity.publicKey.hashcode(),
				strict: Boolean(this.prefetch?.strict),
			});
			const predictor = this.prefetch.predictor;
			this._joinListener = async (e: { detail: PublicSignKey }) => {
				// on join we emit predicted search results before peers query us (to save latency but for the price of errornous bandwidth usage)

				if ((await this._log.isReplicating()) === false) {
					return;
				}

				indexPrefetchLogger("peer join triggered predictor", {
					target: e.detail.hashcode(),
				});

				// TODO
				// it only makes sense for use to return predicted results if the peer is to choose us as a replicator
				// so we need to calculate the cover set from the peers perspective

				// create an iterator and send the peer the results
				let request = predictor.predictedQuery(e.detail);

				if (!request) {
					indexPrefetchLogger("predictor had no cached query", {
						target: e.detail.hashcode(),
					});
					return;
				}
				indexPrefetchLogger("sending predicted results", {
					target: e.detail.hashcode(),
					request: request.idString,
				});
				const results = await this.handleSearchRequest(request, {
					from: e.detail,
				});

				if (results instanceof types.AbstractSearchResult) {
					// start a resumable iterator for the peer
					const query = new types.PredictedSearchRequest({
						id: request.id,
						request,
						results,
					});
					await this._query.send(query, {
						mode: new SilentDelivery({ to: [e.detail], redundancy: 1 }),
					});
				}
			};

			// we do this before _query.open so that we can receive the join event, even immediate ones
			if (this._joinListener) {
				this._query.events.addEventListener("join", this._joinListener);
			}
		}

		await this._query.open({
			topic: sha256Base64Sync(
				concat([this._log.log.id, fromString("/document")]),
			),
			responseHandler: this.queryRPCResponseHandler.bind(this),
			responseType: types.AbstractSearchResult,
			queryType: types.AbstractSearchRequest,
		});
		if (this.handleDocumentChange) {
			this.documentEvents.addEventListener("change", this.handleDocumentChange);
		}
	}

	get prefetch() {
		return this._prefetch;
	}

	private async queryRPCResponseHandler(
		query: types.AbstractSearchRequest,
		ctx: { from?: PublicSignKey; message: DataMessage },
	) {
		if (!ctx.from) {
			logger("receieved query without from");
			return;
		}
		indexRpcLogger("received request", {
			type: query.constructor.name,
			from: ctx.from.hashcode(),
			id: (query as { idString?: string }).idString,
		});
		if (query instanceof types.PredictedSearchRequest) {
			// put results in a waiting cache so that we eventually in the future will query a matching thing, we already have results available
			this._prefetch?.accumulator.add(
				{
					message: ctx.message,
					response: query,
					from: ctx.from,
				},
				ctx.from!.hashcode(),
			);
			indexPrefetchLogger("cached predicted results", {
				from: ctx.from.hashcode(),
				request: query.idString,
			});
			return;
		}

		if (
			this.prefetch?.predictor &&
			(query instanceof types.SearchRequest ||
				query instanceof types.SearchRequestIndexed ||
				query instanceof types.IterationRequest)
		) {
			const { ignore } = this.prefetch.predictor.onRequest(query, {
				from: ctx.from!,
			});

			if (ignore) {
				indexPrefetchLogger("predictor ignored request", {
					from: ctx.from!.hashcode(),
					request: (query as { idString?: string }).idString,
					strict: Boolean(this.prefetch?.strict),
				});
				if (this.prefetch.strict) {
					return;
				}
			}
		}

		try {
			const out = await this.handleSearchRequest(
				query as
					| types.SearchRequest
					| types.SearchRequestIndexed
					| types.IterationRequest
					| types.CollectNextRequest,
				{
					from: ctx.from!,
				},
			);
			return out;
		} catch (error) {
			throw error;
		}
	}
	private async handleSearchRequest(
		query:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest
			| types.CollectNextRequest,
		ctx: { from: PublicSignKey },
	) {
		indexRpcLogger("handling query", {
			type: query.constructor.name,
			id: (query as { idString?: string }).idString,
			from: ctx.from.hashcode(),
		});
		if (
			this.canSearch &&
			(query instanceof types.SearchRequest ||
				query instanceof types.IterationRequest ||
				query instanceof types.CollectNextRequest) &&
			!(await this.canSearch(
				query as
					| types.SearchRequest
					| types.IterationRequest
					| types.CollectNextRequest,
				ctx.from,
			))
		) {
			indexRpcLogger("denied query", {
				id: (query as { idString?: string }).idString,
				from: ctx.from.hashcode(),
			});
			return new types.NoAccess();
		}

		if (query instanceof types.CloseIteratorRequest) {
			this.processCloseIteratorRequest(query, ctx.from);
		} else {
			const fromQueued =
				query instanceof types.CollectNextRequest
					? this._resultQueue.get(query.idString)?.fromQuery
					: undefined;
			const queryResolvesDocuments =
				query instanceof types.CollectNextRequest
					? resolvesDocuments(fromQueued)
					: resolvesDocuments(query as AnyIterationRequest);

			const shouldIncludedIndexedResults =
				this.includeIndexed && queryResolvesDocuments;

			const results = await this.processQuery(
				query as
					| types.SearchRequest
					| types.SearchRequestIndexed
					| types.IterationRequest
					| types.CollectNextRequest,
				ctx.from,
				false,
				{
					canRead: this.canRead,
				},
			);
			indexRpcLogger("query results ready", {
				id: (query as { idString?: string }).idString,
				from: ctx.from.hashcode(),
				count: results.results.length,
				kept: results.kept,
				includeIndexed: shouldIncludedIndexedResults,
			});

			if (shouldIncludedIndexedResults) {
				let resultsWithIndexed: (
					| types.ResultValue<T>
					| types.ResultIndexedValue<I>
				)[] = results.results;

				let fromLength = results.results.length;
				for (let i = 0; i < fromLength; i++) {
					let result = results.results[i];
					resultsWithIndexed.push(
						new types.ResultIndexedValue<I>({
							source: serialize(result.indexed),
							indexed: result.indexed as I,
							context: result.context,
							entries: [],
						}),
					);
				}

				return new types.Results({
					// Even if results might have length 0, respond, because then we now at least there are no matching results
					results: resultsWithIndexed,
					kept: results.kept,
				});
			}

			return new types.Results({
				// Even if results might have length 0, respond, because then we now at least there are no matching results
				results: results.results,
				kept: results.kept,
			});
		}
	}

	async afterOpen(): Promise<void> {
		if (this.isProgramValued) {
			// re-open the program cache
			for (const { id, value } of await this.index.iterate().all()) {
				const programValue = await this.resolveDocument({
					indexed: value,
					head: value.__context.head,
				});
				if (!programValue) {
					logger.error(
						"Missing program value after re-opening the document index. Hash: " +
							value.__context.head,
					);
					continue;
				}
				programValue.value = await this._maybeOpen(
					programValue.value as Program & T,
				);
				this._resolverProgramCache!.set(id.primitive, programValue.value as T);
			}
		}

		return super.afterOpen();
	}
	async getPending(cursorId: string): Promise<number | undefined> {
		const queue = this._resultQueue.get(cursorId);
		if (queue) {
			return queue.queue.length + queue.keptInIndex;
		}

		return this._resumableIterators.getPending(cursorId);
	}

	get hasPending() {
		if (this._resultQueue.size > 0) {
			return true;
		}
		return false;
	}

	async close(from?: Program): Promise<boolean> {
		const closed = await super.close(from);
		if (closed) {
			if (this._joinListener) {
				this._query.events.removeEventListener("join", this._joinListener);
			}
			if (this.handleDocumentChange) {
				this.documentEvents.removeEventListener(
					"change",
					this.handleDocumentChange,
				);
			}
			this.clearAllResultQueues();
			await this.index.stop?.();
		}
		return closed;
	}

	async drop(from?: Program): Promise<boolean> {
		const dropped = await super.drop(from);
		if (dropped) {
			this.documentEvents.removeEventListener(
				"change",
				this.handleDocumentChange,
			);
			this.clearAllResultQueues();
			await this.index.drop?.();
			await this.index.stop?.();
		}
		return dropped;
	}

	public async get<Options extends GetOptions<T, I, D, true | undefined>>(
		key: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: Options,
	): Promise<WithIndexedContext<T, I>>;

	public async get<Options extends GetOptions<T, I, D, false>>(
		key: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: Options,
	): Promise<WithContext<I>>;

	public async get<
		Options extends GetOptions<T, I, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<Options>,
	>(key: indexerTypes.Ideable | indexerTypes.IdKey, options?: Options) {
		let deferred:
			| DeferredPromise<WithIndexedContext<T, I> | WithContext<I>>
			| undefined;

		// Normalize the id key early so listeners can use it
		let idKey =
			key instanceof indexerTypes.IdKey ? key : indexerTypes.toId(key);

		if (options?.waitFor) {
			// add change listener before query because we might get a concurrent change that matches the query,
			// that will not be included in the query result
			deferred = pDefer<WithIndexedContext<T, I> | WithContext<I>>();

			const listener = (evt: CustomEvent<DocumentsChange<T, I>>) => {
				for (const added of evt.detail.added) {
					const id = indexerTypes.toId(
						this.indexByResolver(added.__indexed),
					).primitive;
					if (id === idKey.primitive) {
						deferred!.resolve(added);
					}
				}
			};
			let cleanedUp = false;
			let cleanup = () => {
				if (cleanedUp) return;
				cleanedUp = true;
				this.documentEvents.removeEventListener("change", listener);
				clearTimeout(timeout);
				this.events.removeEventListener("close", resolveUndefined);
				joinListener?.();
			};

			let resolveUndefined = () => {
				deferred!.resolve(undefined);
			};

			let timeout = setTimeout(resolveUndefined, options.waitFor);
			this.events.addEventListener("close", resolveUndefined);
			this.documentEvents.addEventListener("change", listener);
			deferred.promise.then(cleanup);

			// Prepare remote options without mutating caller options
			const baseRemote =
				options?.remote === false
					? undefined
					: typeof options?.remote === "object"
						? { ...options.remote }
						: {};
			if (baseRemote) {
				const waitPolicy = baseRemote.wait;
				if (
					!waitPolicy ||
					(typeof waitPolicy === "object" &&
						(waitPolicy.timeout || 0) < options.waitFor)
				) {
					baseRemote.wait = {
						...(typeof waitPolicy === "object" ? waitPolicy : {}),
						timeout: options.waitFor,
					};
				}
			}

			// Re-query on peer joins (like iterate), scoped to the joining peer
			let joinListener: (() => void) | undefined;
			if (baseRemote) {
				joinListener = this.createReplicatorJoinListener({
					eager: baseRemote.reach?.eager,
					onPeer: async (pk) => {
						if (cleanedUp) return;
						const hash = pk.hashcode();
						const requeryOptions: QueryOptions<T, I, D, Resolve> = {
							...(options as any),
							remote: {
								...(baseRemote || {}),
								from: [hash],
							},
						};
						const re = await this.getDetailed(idKey, requeryOptions as any);
						const first = re?.[0]?.results[0];
						if (first) {
							deferred!.resolve(first.value as any);
						}
					},
				});
			}
		}

		const result = (await this.getDetailed(idKey, options))?.[0]?.results[0];

		// if no results, and we have remote joining options, we wait for the timout and if there are joining peers we re-query
		if (!result) {
			return deferred?.promise;
		} else if (deferred) {
			deferred.resolve(undefined);
		}
		return result?.value;
	}

	public async getFromGid(gid: string) {
		const iterator = this.index.iterate({ query: { gid } });
		const one = await iterator.next(1);
		await iterator.close();
		return one[0];
	}

	public async getFromHash(hash: string) {
		const iterator = this.index.iterate({ query: { hash } });
		const one = await iterator.next(1);
		await iterator.close();
		return one[0];
	}
	public async put(
		value: T,
		id: indexerTypes.IdKey,
		entry: Entry<Operation>,
		existing: indexerTypes.IndexedResult<WithContext<I>> | null | undefined,
	): Promise<{ context: types.Context; indexable: I }> {
		const existingDefined =
			existing === undefined ? await this.index.get(id) : existing;
		const context = new types.Context({
			created:
				existingDefined?.value.__context.created ||
				entry.meta.clock.timestamp.wallTime,
			modified: entry.meta.clock.timestamp.wallTime,
			head: entry.hash,
			gid: entry.meta.gid,
			size: entry.payload.byteLength,
		});
		return this.putWithContext(value, id, context);
	}

	public async putWithContext(
		value: T,
		id: indexerTypes.IdKey,
		context: types.Context,
	): Promise<{ context: types.Context; indexable: I }> {
		const idString = id.primitive;
		if (
			this.isProgramValued /*
			TODO should we skip caching program value if they are not openend through this db?
			&&
			(value as Program).closed === false &&
			(value as Program).parents.includes(this._log) */
		) {
			// TODO make last condition more efficient if there are many docs
			this._resolverProgramCache!.set(idString, value);
			indexCacheLogger("cache:set:program", { id: idString });
		} else {
			if (this._resolverCache) {
				this._resolverCache.add(idString, value);
				indexCacheLogger("cache:set:value", { id: idString });
			}
		}
		const valueToIndex = await this.transformer(value, context);
		const wrappedValueToIndex = new this.wrappedIndexedType(
			valueToIndex as I,
			context,
		);

		coerceWithIndexed(value, valueToIndex);

		coerceWithContext(value, context);

		await this.index.put(wrappedValueToIndex);
		return { context, indexable: valueToIndex };
	}

	public del(key: indexerTypes.IdKey) {
		if (this.isProgramValued) {
			this._resolverProgramCache!.delete(key.primitive);
			indexCacheLogger("cache:del:program", { id: key.primitive });
		} else {
			if (this._resolverCache?.del(key.primitive)) {
				indexCacheLogger("cache:del:value", { id: key.primitive });
			}
		}
		return this.index.del({
			query: [indexerTypes.getMatcher(this.indexBy, key.key)],
		});
	}

	public async getDetailed<
		Options extends QueryOptions<T, I, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<Options>,
		RT extends types.Result = Resolve extends true
			? types.ResultValue<WithIndexedContext<T, I>>
			: types.ResultIndexedValue<WithContext<I>>,
	>(
		key: indexerTypes.IdKey | indexerTypes.IdPrimitive,
		options?: QueryOptions<T, I, D, Resolve>,
	): Promise<types.Results<RT>[] | undefined> {
		let coercedOptions = options;
		if (options?.remote && typeof options.remote !== "boolean") {
			coercedOptions = {
				...options,
				remote: {
					...options.remote,
					strategy: options.remote?.strategy ?? "fallback",
				},
			};
		} else if (options?.remote === undefined) {
			coercedOptions = {
				...options,
				remote: {
					strategy: "fallback",
				},
			};
		}

		let results:
			| types.Results<
					| types.ResultValue<WithContext<T>>
					| types.ResultIndexedValue<WithContext<I>>
			  >[]
			| undefined;

		const runAndClose = async (
			req: types.SearchRequest | types.SearchRequestIndexed,
		): Promise<typeof results> => {
			const response = await this.queryCommence(
				req,
				coercedOptions as QueryDetailedOptions<T, I, D, boolean | undefined>,
			);
			this._resumableIterators.close({ idString: req.idString });
			this.cancelIteratorKeepAlive(req.idString);
			return response as typeof results;
		};
		const resolve = coercedOptions?.resolve || coercedOptions?.resolve == null;
		let requestClazz = resolve
			? types.SearchRequest
			: types.SearchRequestIndexed;
		if (key instanceof Uint8Array) {
			const request = new requestClazz({
				query: [
					new indexerTypes.ByteMatchQuery({ key: this.indexBy, value: key }),
				],
			});
			results = await runAndClose(request);
		} else {
			const indexableKey = indexerTypes.toIdeable(key);

			if (
				typeof indexableKey === "number" ||
				typeof indexableKey === "bigint"
			) {
				const request = new requestClazz({
					query: [
						new indexerTypes.IntegerCompare({
							key: this.indexBy,
							compare: indexerTypes.Compare.Equal,
							value: indexableKey,
						}),
					],
				});
				results = await runAndClose(request);
			} else if (typeof indexableKey === "string") {
				const request = new requestClazz({
					query: [
						new indexerTypes.StringMatch({
							key: this.indexBy,
							value: indexableKey,
						}),
					],
				});
				results = await runAndClose(request);
			} else if (indexableKey instanceof Uint8Array) {
				const request = new requestClazz({
					query: [
						new indexerTypes.ByteMatchQuery({
							key: this.indexBy,
							value: indexableKey,
						}),
					],
				});
				results = await runAndClose(request);
			} else if ((indexableKey as any) instanceof ArrayBuffer) {
				const request = new requestClazz({
					query: [
						new indexerTypes.ByteMatchQuery({
							key: this.indexBy,
							value: new Uint8Array(indexableKey),
						}),
					],
				});
				results = await runAndClose(request);
			} else {
				throw new Error("Unsupported key type");
			}
		}

		// if we are to resolve the document we need to go through all results and replace the results with the resolved values
		const shouldResolve =
			resolve &&
			requestClazz === types.SearchRequestIndexed &&
			!this.indexedTypeIsDocumentType &&
			results;

		if (results) {
			for (const set of results) {
				let missingValues = false;
				for (let i = 0; i < set.results.length; i++) {
					let value = set.results[i];
					let resolved: T | undefined;
					if (shouldResolve) {
						resolved =
							value instanceof types.ResultIndexedValue
								? (
										await this.resolveDocument({
											indexed: value.value,
											head: value.context.head,
										})
									)?.value
								: value.value;
					} else {
						resolved = value.value as T;
					}
					if (resolved) {
						let indexed = await this.resolveIndexed<any>(
							set.results[i],
							set.results,
						);
						let valueWithWindexed = coerceWithIndexed(resolved, indexed);
						set.results[i]._value = coerceWithContext(
							valueWithWindexed,
							set.results[i].context,
						);
					} else {
						missingValues = true;
					}
				}

				if (missingValues) {
					set.results = set.results.filter((x) => !!x);
				}
			}
		}

		return results as any as types.Results<RT>[];
	}

	getSize(): Promise<number> | number {
		return this.index.getSize();
	}

	private async resolveDocument(value: {
		id?: indexerTypes.IdPrimitive;
		indexed: I;
		head: string;
	}): Promise<{ value: T } | undefined> {
		const id =
			value.id ??
			indexerTypes.toId(this.indexByResolver(value.indexed)).primitive;

		const cached =
			this._resolverCache?.get(id) || this._resolverProgramCache?.get(id);
		if (cached != null) {
			return { value: cached };
		}

		if (this.indexedTypeIsDocumentType) {
			// cast value to T, i.e. convert the class but keep all properties except the __context
			const obj = Object.assign(
				Object.create(this.documentType.prototype),
				value.indexed,
			);
			delete obj.__context;
			return { value: obj as T };
		}

		const head = await this._log.log.get(value.head);
		if (!head) {
			return undefined; // we could end up here if we recently pruned the document and other peers never persisted the entry
			// TODO update changes in index before removing entries from log entry storage
		}
		const payloadValue = await head.getPayloadValue();
		if (isPutOperation(payloadValue)) {
			return {
				value: this.valueEncoding.decoder(payloadValue.data),
				/* size: payloadValue.data.byteLength */
			};
		}

		throw new Error(
			"Unexpected value type when getting document: " +
				payloadValue?.constructor?.name || typeof payloadValue,
		);
	}

	async processQuery<
		R extends
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest
			| types.CollectNextRequest,
	>(
		query: R,
		from: PublicSignKey,
		isLocal: boolean,
		options?: {
			canRead?: CanRead<I>;
		},
	): Promise<types.Results<types.ResultTypeFromRequest<R, T, I>>> {
		// We do special case for querying the id as we can do it faster than iterating

		let prevQueued = isLocal
			? undefined
			: this._resultQueue.get(query.idString);
		if (prevQueued && !from.equals(prevQueued.from)) {
			throw new Error("Different from in queued results");
		}

		let indexedResult: indexerTypes.IndexedResults<WithContext<I>> | undefined =
			undefined;

		let fromQuery:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest
			| undefined;
		let keepAliveRequest: types.IterationRequest | undefined;
		if (
			query instanceof types.SearchRequest ||
			query instanceof types.SearchRequestIndexed ||
			query instanceof types.IterationRequest
		) {
			fromQuery = query;
			if (
				!isLocal &&
				query instanceof types.IterationRequest &&
				query.keepAliveTtl != null
			) {
				keepAliveRequest = query;
			}
			indexedResult = await this._resumableIterators.iterateAndFetch(query, {
				keepAlive: keepAliveRequest !== undefined,
			});
		} else if (query instanceof types.CollectNextRequest) {
			const cachedRequest =
				prevQueued?.fromQuery ||
				this._resumableIterators.queues.get(query.idString)?.request;
			fromQuery = cachedRequest;
			if (
				!isLocal &&
				cachedRequest instanceof types.IterationRequest &&
				cachedRequest.keepAliveTtl != null
			) {
				keepAliveRequest = cachedRequest;
			}
			const hasResumable = this._resumableIterators.has(query.idString);
			indexedResult = hasResumable
				? await this._resumableIterators.next(query, {
						keepAlive: keepAliveRequest !== undefined,
					})
				: [];
		} else {
			throw new Error("Unsupported");
		}

		if (!isLocal && keepAliveRequest) {
			this.scheduleIteratorKeepAlive(
				query.idString,
				keepAliveRequest.keepAliveTtl,
			);
		}

		let resultSize = 0;

		let toIterate = prevQueued
			? [...prevQueued.queue, ...indexedResult]
			: indexedResult;

		if (prevQueued) {
			this._resultQueue.delete(query.idString);
			clearTimeout(prevQueued.timeout);
			prevQueued = undefined;
		}

		let kept = (await this._resumableIterators.getPending(query.idString)) ?? 0;

		if (!isLocal) {
			const resolveFlag = resolvesDocuments(
				(fromQuery || query) as AnyIterationRequest,
			);
			prevQueued = {
				from,
				queue: [],
				timeout: setTimeout(() => {
					this._resultQueue.delete(query.idString);
				}, 6e4),
				keptInIndex: kept,
				fromQuery: (fromQuery || query) as
					| types.SearchRequest
					| types.SearchRequestIndexed
					| types.IterationRequest,
				resolveResults: resolveFlag,
			};
			if (
				fromQuery instanceof types.IterationRequest &&
				fromQuery.pushUpdates
			) {
				prevQueued.pushMode = fromQuery.pushUpdates;
			}
			this._resultQueue.set(query.idString, prevQueued);
		}

		const filteredResults: types.Result[] = [];
		const resolveDocumentsFlag = resolvesDocuments(fromQuery);
		const replicateIndexFlag = replicatesIndex(fromQuery);
		for (const result of toIterate) {
			if (!isLocal) {
				resultSize += result.value.__context.size;
				if (resultSize > MAX_BATCH_SIZE) {
					prevQueued!.queue.push(result);
					continue;
				}
			}
			const indexedUnwrapped = Object.assign(
				Object.create(this.indexedType.prototype),
				result.value,
			);

			if (
				options?.canRead &&
				!(await options.canRead(indexedUnwrapped, from))
			) {
				continue;
			}
			if (resolveDocumentsFlag) {
				const value = await this.resolveDocument({
					indexed: result.value,
					head: result.value.__context.head,
				});

				if (!value) {
					continue;
				}

				filteredResults.push(
					new types.ResultValue({
						context: result.value.__context,
						value: value.value,
						source: serialize(value.value),
						indexed: indexedUnwrapped,
					}),
				);
			} else {
				const context = result.value.__context;
				const head = await this._log.log.get(context.head);
				// assume remote peer will start to replicate (TODO is this ideal?)
				if (replicateIndexFlag) {
					this._log.addPeersToGidPeerHistory(context.gid, [from.hashcode()]);
				}

				filteredResults.push(
					new types.ResultIndexedValue({
						context,
						source: serialize(indexedUnwrapped),
						indexed: indexedUnwrapped,
						entries: head ? [head] : [],
					}),
				);
			}
		}

		const results: types.Results<any> = new types.Results<any>({
			results: filteredResults,
			kept: BigInt(kept + (prevQueued?.queue.length || 0)),
		});
		const keepAliveActive = keepAliveRequest !== undefined;
		const pushActive =
			fromQuery instanceof types.IterationRequest &&
			Boolean(fromQuery.pushUpdates);

		if (!isLocal && results.kept === 0n && !keepAliveActive && !pushActive) {
			this.clearResultsQueue(query);
		}

		return results;
	}

	private scheduleIteratorKeepAlive(idString: string, ttl?: bigint) {
		if (ttl == null) {
			return;
		}
		const ttlNumber = Number(ttl);
		if (!Number.isFinite(ttlNumber) || ttlNumber <= 0) {
			return;
		}

		// Cap max timeout to 1 day	(TODO make configurable?)
		const delay = Math.max(1, Math.min(ttlNumber, 86400000));
		this.cancelIteratorKeepAlive(idString);
		const timers =
			this.iteratorKeepAliveTimers ??
			(this.iteratorKeepAliveTimers = new Map<
				string,
				ReturnType<typeof setTimeout>
			>());
		const timer = setTimeout(() => {
			timers.delete(idString);
			const queued = this._resultQueue.get(idString);
			if (queued) {
				clearTimeout(queued.timeout);
				this._resultQueue.delete(idString);
			}
			this._resumableIterators.close({ idString });
		}, delay);
		timers.set(idString, timer);
	}

	private cancelIteratorKeepAlive(idString: string) {
		const timers = this.iteratorKeepAliveTimers;
		if (!timers) {
			return;
		}
		const timer = timers.get(idString);
		if (timer) {
			clearTimeout(timer);
			timers.delete(idString);
		}
	}

	private clearResultsQueue(
		query:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest
			| types.CollectNextRequest
			| types.CloseIteratorRequest,
	) {
		const queue = this._resultQueue.get(query.idString);
		if (queue) {
			clearTimeout(queue.timeout);
			this._resultQueue.delete(query.idString);
		}
	}

	get countIteratorsInProgress() {
		return this._resumableIterators.queues.size;
	}

	private clearAllResultQueues() {
		for (const [key, queue] of this._resultQueue) {
			clearTimeout(queue.timeout);
			this._resultQueue.delete(key);
			this.cancelIteratorKeepAlive(key);
			this._resumableIterators.close({ idString: key });
		}
	}

	private async waitForCoverReady(params: {
		domain?: { args?: ExtractArgs<D> } | { range: CoverRange<number | bigint> };
		eager?: boolean;
		settle: "any";
		timeout: number;
		signal?: AbortSignal;
		onTimeout?: "proceed" | "error";
	}) {
		const {
			domain,
			eager,
			settle,
			timeout,
			signal,
			onTimeout = "proceed",
		} = params;

		if (settle !== "any") {
			return;
		}

		const properties =
			domain && "range" in domain
				? { range: domain.range }
				: { args: domain?.args };
		const selfHash = this.node.identity.publicKey.hashcode();

		const ready = async () => {
			const cover = await this._log.getCover(properties, { eager });
			return cover.some((hash) => hash !== selfHash);
		};

		if (await ready()) {
			return;
		}

		const deferred = pDefer<void>();
		let settled = false;
		let cleaned = false;
		let timer: ReturnType<typeof setTimeout> | undefined;
		let checking = false;

		const cleanup = () => {
			if (cleaned) {
				return;
			}
			cleaned = true;
			this._log.events.removeEventListener("replicator:join", onEvent);
			this._log.events.removeEventListener("replication:change", onEvent);
			this._log.events.removeEventListener("replicator:mature", onEvent);
			signal?.removeEventListener("abort", onAbort);
			if (timer != null) {
				clearTimeout(timer);
				timer = undefined;
			}
		};

		const resolve = () => {
			if (settled) {
				return;
			}
			settled = true;
			cleanup();
			deferred.resolve();
		};

		const reject = (error: Error) => {
			if (settled) {
				return;
			}
			settled = true;
			cleanup();
			deferred.reject(error);
		};

		const onAbort = () => reject(new AbortError());

		const onEvent = async () => {
			if (checking) {
				return;
			}
			checking = true;
			try {
				if (await ready()) {
					resolve();
				}
			} catch (error) {
				reject(error instanceof Error ? error : new Error(String(error)));
			} finally {
				checking = false;
			}
		};

		if (signal) {
			signal.addEventListener("abort", onAbort);
		}

		if (timeout > 0) {
			timer = setTimeout(() => {
				if (onTimeout === "error") {
					reject(
						new TimeoutError("Timeout waiting for participating replicator"),
					);
				} else {
					resolve();
				}
			}, timeout);
		}

		this._log.events.addEventListener("replicator:join", onEvent);
		this._log.events.addEventListener("replication:change", onEvent);
		this._log.events.addEventListener("replicator:mature", onEvent);

		try {
			await deferred.promise;
		} finally {
			cleanup();
		}
	}

	// Utility: attach a join listener that waits until a peer is a replicator,
	// then invokes the provided callback. Returns a detach function.
	private createReplicatorJoinListener(params: {
		signal?: AbortSignal;
		eager?: boolean;
		onPeer: (pk: PublicSignKey) => Promise<void> | void;
	}): () => void {
		const active = new Set<string>();
		const listener = async (e: { detail: PublicSignKey }) => {
			const pk = e.detail;
			const hash = pk.hashcode();
			if (hash === this.node.identity.publicKey.hashcode()) return;
			if (params.signal?.aborted) return;
			if (active.has(hash)) return;
			active.add(hash);
			try {
				const isReplicator = await this._log
					.waitForReplicator(pk, {
						signal: params.signal,
						eager: params.eager,
					})
					.then(() => true)
					.catch(() => false);
				if (!isReplicator || params.signal?.aborted) return;
				indexIteratorLogger.trace("peer joined as replicator", { peer: hash });
				await params.onPeer(pk);
			} finally {
				active.delete(hash);
			}
		};

		this._query.events.addEventListener("join", listener);
		return () => this._query.events.removeEventListener("join", listener);
	}

	processCloseIteratorRequest(
		query: types.CloseIteratorRequest,
		publicKey: PublicSignKey,
	): void {
		indexIteratorLogger.trace("close request", {
			id: query.idString,
			from: publicKey.hashcode(),
		});
		const queueData = this._resultQueue.get(query.idString);
		if (queueData && !queueData.from.equals(publicKey)) {
			indexIteratorLogger.trace(
				"Ignoring close iterator request from different peer",
			);
			return;
		}
		this.cancelIteratorKeepAlive(query.idString);
		this.clearResultsQueue(query);
		return this._resumableIterators.close(query);
	}

	/**
	 * Query and retrieve results with most details
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	private async queryCommence<
		R extends
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest,
		RT extends types.Result = types.ResultTypeFromRequest<R, T, I>,
	>(
		queryRequest: R,
		options?: QueryDetailedOptions<T, I, D, boolean | undefined>,
		fetchFirstForRemote?: Set<string>,
	): Promise<types.Results<RT>[]> {
		const local = typeof options?.local === "boolean" ? options?.local : true;
		let remote:
			| RemoteQueryOptions<
					types.AbstractSearchRequest,
					types.AbstractSearchResult,
					D
			  >
			| undefined = undefined;
		if (typeof options?.remote === "boolean") {
			if (options?.remote) {
				remote = {};
			} else {
				remote = undefined;
			}
		} else {
			remote = options?.remote || {};
		}
		if (remote && remote.priority == null) {
			// give queries higher priority than other "normal" data activities
			// without this, we might have a scenario that a peer joina  network with large amount of data to be synced, but can not query anything before that is done
			// this will lead to bad UX as you usually want to list/expore whats going on before doing any replication work
			remote.priority = 1;
		}

		if (!local && !remote) {
			throw new Error(
				"Expecting either 'options.remote' or 'options.local' to be true",
			);
		}
		const allResults: types.Results<types.ResultTypeFromRequest<R, T, I>>[] =
			[];

		if (local) {
			const results = await this.processQuery(
				queryRequest,
				this.node.identity.publicKey,
				true,
			);
			if (results.results.length > 0) {
				options?.onResponse &&
					(await options.onResponse(results, this.node.identity.publicKey));
				allResults.push(results);
			}
		}

		let resolved: types.Results<types.ResultTypeFromRequest<R, T, I>>[] = [];
		if (remote && (remote.strategy !== "fallback" || allResults.length === 0)) {
			if (queryRequest instanceof types.CloseIteratorRequest) {
				// don't wait for responses
				throw new Error("Unexpected");
			}

			const replicatorGroups = options?.remote?.from
				? options?.remote?.from
				: await this._log.getCover(remote.domain ?? { args: undefined }, {
						roleAge: remote.minAge,
						eager: remote.reach?.eager,
						reachableOnly: !!remote.wait, // when we want to merge joining we can ignore pending to be online peers and instead consider them once they become online
					});

			if (replicatorGroups) {
				const responseHandler = async (
					results: {
						response: types.AbstractSearchResult;
						from?: PublicSignKey;
					}[],
				) => {
					const resultInitialized = await introduceEntries(
						queryRequest,
						results,
						this.documentType,
						this.indexedType,
						this._sync,
						options,
					);
					for (const r of resultInitialized) {
						resolved.push(r.response);
					}
				};

				let extraPromises: Promise<void>[] | undefined = undefined;

				const groupHashes: string[][] = replicatorGroups
					.filter((hash) => {
						if (hash === this.node.identity.publicKey.hashcode()) {
							return false;
						}

						if (fetchFirstForRemote?.has(hash)) {
							// we already fetched this one for remote, no need to do it again
							return false;
						}
						fetchFirstForRemote?.add(hash);

						const resultAlready = this._prefetch?.accumulator.consume(
							queryRequest,
							hash,
						);
						if (resultAlready) {
							(extraPromises || (extraPromises = [])).push(
								(async () => {
									let from = await this.node.services.pubsub.getPublicKey(hash);
									if (from) {
										return responseHandler([
											{
												response: resultAlready.response.results,
												from,
											},
										]);
									}
								})(),
							);
							return false;
						}
						return true;
					})
					.map((x) => [x]);

				extraPromises && (await Promise.all(extraPromises));
				let tearDown: (() => void) | undefined = undefined;
				const search = this;

				try {
					groupHashes.length > 0 &&
						(await queryAll(
							this._query,
							groupHashes,
							queryRequest,
							responseHandler,
							search._prefetch?.accumulator
								? {
										...remote,
										responseInterceptor(fn) {
											const listener = (evt: {
												detail: {
													consumable: RPCResponse<
														types.PredictedSearchRequest<any>
													>;
												};
											}) => {
												const consumable =
													search._prefetch?.accumulator.consume(
														queryRequest,
														evt.detail.consumable.from!.hashcode(),
													);

												if (consumable) {
													fn({
														message: consumable.message,
														response: consumable.response.results,
														from: consumable.from,
													});
												}
											};

											for (const groups of groupHashes) {
												for (const hash of groups) {
													const consumable =
														search._prefetch?.accumulator.consume(
															queryRequest,
															hash,
														);
													if (consumable) {
														fn({
															message: consumable.message,
															response: consumable.response.results,
															from: consumable.from,
														});
													}
												}
											}
											search.prefetch?.accumulator.addEventListener(
												"add",
												listener,
											);
											tearDown = () => {
												search.prefetch?.accumulator.removeEventListener(
													"add",
													listener,
												);
											};
										},
									}
								: remote,
						));
				} catch (error) {
					if (error instanceof MissingResponsesError) {
						warn("Did not reciveve responses from all shard");
						if (remote?.throwOnMissing) {
							throw error;
						}
					} else {
						throw error;
					}
				} finally {
					tearDown && (tearDown as any)();
				}
			} else {
				// TODO send without direction out to the world? or just assume we can insert?
				/* 	promises.push(
						this._query
							.request(queryRequest, remote)
							.then((results) => introduceEntries(results, this.type, this._sync, options).then(x => x.map(y => y.response)))
					); */
				/* throw new Error(
					"Missing remote replicator info for performing distributed document query"
				); */
			}
		}
		for (const r of resolved) {
			if (r) {
				if (r instanceof Array) {
					allResults.push(...r);
				} else {
					allResults.push(r);
				}
			}
		}
		return allResults as any; // TODO types
	}

	public search(
		queryRequest: QueryLike,
		options?: SearchOptions<T, I, D, true>,
	): Promise<ValueTypeFromRequest<true, T, I>[]>;
	public search(
		queryRequest: QueryLike,
		options?: SearchOptions<T, I, D, false>,
	): Promise<ValueTypeFromRequest<false, T, I>[]>;

	/**
	 * Query and retrieve results
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	public async search<
		R extends
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.IterationRequest
			| QueryLike,
		O extends SearchOptions<T, I, D, Resolve>,
		Resolve extends boolean = ExtractResolveFromOptions<O>,
	>(
		queryRequest: R,
		options?: O,
	): Promise<ValueTypeFromRequest<Resolve, T, I>[]> {
		// Set fetch to search size, or max value (default to max u32 (4294967295))
		const coercedRequest = coerceQuery(
			queryRequest,
			options,
			this.compatibility,
		);
		coercedRequest.fetch = coercedRequest.fetch ?? 0xffffffff;

		// So that the iterator is pre-fetching the right amount of entries
		const iterator = this.iterate<Resolve>(coercedRequest, options);

		// So that this call will not do any remote requests
		const allResults: ValueTypeFromRequest<Resolve, T, I>[] = [];

		while (
			iterator.done() !== true &&
			coercedRequest.fetch > allResults.length
		) {
			// We might need to pull .next multiple time due to data message size limitations

			for (const result of await iterator.next(
				coercedRequest.fetch - allResults.length,
			)) {
				allResults.push(result as ValueTypeFromRequest<Resolve, T, I>);
			}
		}

		await iterator.close();

		// Deduplicate and return values directly
		return dedup(allResults, this.indexByResolver);
	}

	private resolveIndexed<R>(
		result: types.ResultValue<T> | types.ResultIndexedValue<I>,
		results: types.ResultTypeFromRequest<R, T, I>[],
	) {
		if (result instanceof types.ResultIndexedValue) {
			return coerceWithContext(result.value as I, result.context);
		}

		let resolveIndexedDefault = async (result: types.ResultValue<T>) =>
			coerceWithContext(
				(result.indexed as I) ||
					(await this.transformer(result.value, result.context)),
				result.context,
			);

		let resolveIndexed = this.includeIndexed
			? (
					result: types.ResultValue<T>,
					results: types.ResultTypeFromRequest<R, T, I>[],
				) => {
					// look through the search results and see if we can find the indexed representation
					for (const otherResult of results) {
						if (otherResult instanceof types.ResultIndexedValue) {
							if (otherResult.context.head === result.context.head) {
								otherResult.init(this.indexedType);
								return coerceWithContext(
									otherResult.value,
									otherResult.context,
								);
							}
						}
					}
					return resolveIndexedDefault(result);
				}
			: (result: types.ResultValue<T>) => resolveIndexedDefault(result);

		return resolveIndexed(result, results);
	}

	public iterate(
		query?: QueryLike,
		options?: QueryOptions<T, I, D, undefined>,
	): ResultsIterator<ValueTypeFromRequest<true, T, I>>;
	public iterate<Resolve extends boolean>(
		query?: QueryLike,
		options?: QueryOptions<T, I, D, Resolve>,
	): ResultsIterator<ValueTypeFromRequest<Resolve, T, I>>;

	/**
	 * Query and retrieve documents in a iterator
	 * @param queryRequest
	 * @param optionsArg
	 * @returns
	 */
	public iterate<
		R extends types.SearchRequest | types.SearchRequestIndexed | QueryLike,
		O extends SearchOptions<T, I, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<O>,
	>(
		queryRequest?: R,
		optionsArg?: QueryOptions<T, I, D, Resolve>,
	): ResultsIterator<ValueTypeFromRequest<Resolve, T, I>> {
		let options = optionsArg;
		if (
			queryRequest instanceof types.SearchRequest &&
			options?.resolve === false
		) {
			throw new Error("Cannot use resolve=false with SearchRequest"); // TODO make this work
		}

		let queryRequestCoerced = coerceQuery(
			queryRequest ?? {},
			options,
			this.compatibility,
		);

		const self = this;
		function normalizeUpdatesOption(u?: UpdateOptions<T, I, Resolve>): {
			mergePolicy?: {
				merge?:
					| {
							filter?: (
								evt: DocumentsChange<T, I>,
							) => MaybePromise<DocumentsChange<T, I> | void>;
					  }
					| undefined;
			};
			push?: types.PushUpdatesMode;
			callbacks?: UpdateCallbacks<T, I, Resolve>;
		} {
			const identityFilter = (evt: DocumentsChange<T, I>) => evt;
			const buildMergePolicy = (
				merge: UpdateMergeStrategy<T, I, Resolve> | undefined,
				defaultEnabled: boolean,
			) => {
				const effective =
					merge === undefined ? (defaultEnabled ? true : undefined) : merge;
				if (effective === undefined || effective === false) {
					return undefined;
				}
				if (effective === true) {
					return {
						merge: {
							filter: identityFilter,
						},
					};
				}
				return {
					merge: {
						filter: effective.filter ?? identityFilter,
					},
				};
			};

			if (u == null || u === false) {
				return {};
			}

			if (u === true) {
				return {
					mergePolicy: buildMergePolicy(true, true),
					push: undefined,
				};
			}

			if (typeof u === "string") {
				if (u === "remote") {
					self.ensurePrefetchAccumulator();
					return { push: types.PushUpdatesMode.STREAM };
				}
				if (u === "local") {
					return {
						mergePolicy: buildMergePolicy(true, true),
						push: undefined,
					};
				}
				if (u === "all") {
					self.ensurePrefetchAccumulator();
					return {
						mergePolicy: buildMergePolicy(true, true),
						push: types.PushUpdatesMode.STREAM,
					};
				}
			}

			if (typeof u === "object") {
				const hasMergeProp = Object.prototype.hasOwnProperty.call(u, "merge");
				const mergeValue = hasMergeProp ? u.merge : undefined;
				if (u.push) {
					self.ensurePrefetchAccumulator();
				}
				const callbacks =
					u.notify || u.onBatch
						? {
								notify: u.notify,
								onBatch: u.onBatch,
							}
						: undefined;
				return {
					mergePolicy: buildMergePolicy(
						mergeValue,
						!hasMergeProp || mergeValue === undefined,
					),
					push:
						typeof u.push === "number"
							? u.push
							: u.push
								? types.PushUpdatesMode.STREAM
								: undefined,
					callbacks,
				};
			}

			return {};
		}

		const {
			mergePolicy,
			push: pushUpdates,
			callbacks: updateCallbacksRaw,
		} = normalizeUpdatesOption(options?.updates);
		const hasLiveUpdates = mergePolicy !== undefined;
		const originalRemote = options?.remote;
		let remoteOptions =
			typeof originalRemote === "boolean"
				? originalRemote
				: originalRemote
					? { ...originalRemote }
					: undefined;
		if (pushUpdates && remoteOptions !== false) {
			if (typeof remoteOptions === "object") {
				if (remoteOptions.replicate !== true) {
					remoteOptions.replicate = true;
				}
			} else if (remoteOptions === undefined || remoteOptions === true) {
				remoteOptions = { replicate: true };
			}
		}
		if (remoteOptions !== originalRemote) {
			options = Object.assign({}, options, { remote: remoteOptions });
		}
		const outOfOrderMode: OutOfOrderMode = options?.outOfOrder?.mode ?? "drop";

		let resolve = options?.resolve !== false;
		const wantsReplication =
			options?.remote &&
			typeof options.remote !== "boolean" &&
			options.remote.replicate;

		if (
			!(queryRequestCoerced instanceof types.IterationRequest) &&
			pushUpdates
		) {
			// Push streaming only works on IterationRequest; reject legacy compat and upgrade other callers.
			if (this.compatibility !== undefined) {
				throw new Error(
					"updates.push requires IterationRequest support; not available when compatibility is set",
				);
			}
			queryRequestCoerced = new types.IterationRequest({
				query: queryRequestCoerced.query,
				sort: queryRequestCoerced.sort,
				fetch: queryRequestCoerced.fetch,
				resolve,
				pushUpdates,
				mergeUpdates: mergePolicy?.merge ? true : undefined,
			});
			resolve =
				(queryRequestCoerced as types.IterationRequest).resolve !== false;
		} else if (
			!(queryRequestCoerced instanceof types.IterationRequest) &&
			wantsReplication &&
			options?.resolve !== false
		) {
			// Legacy requests can't carry replicate=true; swap to indexed search so replication intent is preserved.
			if (
				(queryRequest instanceof types.SearchRequestIndexed === false &&
					this.compatibility == null) ||
				(this.compatibility != null && this.compatibility > 8)
			) {
				queryRequestCoerced = new types.SearchRequestIndexed({
					query: queryRequestCoerced.query,
					fetch: queryRequestCoerced.fetch,
					sort: queryRequestCoerced.sort,
				});
				resolve = true;
			}
		}

		let replicate =
			options?.remote &&
			typeof options.remote !== "boolean" &&
			(options.remote.replicate || pushUpdates);
		if (
			replicate &&
			queryRequestCoerced instanceof types.SearchRequestIndexed
		) {
			queryRequestCoerced.replicate = true;
		}

		indexIteratorLogger.trace("Iterate with options", {
			query: queryRequestCoerced,
			options,
		});

		let fetchPromise: Promise<any> | undefined = undefined;
		const peerBufferMap: Map<
			string,
			{
				kept: number;
				buffer: BufferedResult<types.ResultTypeFromRequest<R, T, I> | I, I>[];
			}
		> = new Map();
		const visited = new Set<indexerTypes.IdPrimitive>();
		let indexedPlaceholders:
			| Map<
					indexerTypes.IdPrimitive,
					BufferedResult<types.ResultTypeFromRequest<R, T, I> | I, I>
			  >
			| undefined;
		const ensureIndexedPlaceholders = () => {
			if (!indexedPlaceholders) {
				indexedPlaceholders = new Map<
					string | number | bigint,
					BufferedResult<types.ResultTypeFromRequest<R, T, I> | I, I>
				>();
			}
			return indexedPlaceholders;
		};

		let done = false;
		let drain = false; // if true, close on empty once (overrides manual)
		let first = false;

		// TODO handle join/leave while iterating
		let controller: AbortController | undefined = undefined;
		const ensureController = () => {
			if (!controller) {
				return (controller = new AbortController());
			}
			return controller;
		};
		let totalFetchedCounter = 0;
		let lastValueInOrder:
			| {
					indexed: WithContext<I>;
					value: types.ResultTypeFromRequest<R, T, I> | I;
					from: PublicSignKey;
					context: types.Context;
			  }
			| undefined = undefined;
		let lastDeliveredIndexed: WithContext<I> | undefined;

		const peerBuffers = (): {
			indexed: WithContext<I>;
			value: types.ResultTypeFromRequest<R, T, I> | I;
			from: PublicSignKey;
			context: types.Context;
		}[] => {
			return [...peerBufferMap.values()].map((x) => x.buffer).flat();
		};

		const toIndexedForOrdering = (
			value:
				| ValueTypeFromRequest<Resolve, T, I>
				| WithContext<I>
				| WithIndexedContext<T, I>,
		): WithContext<I> | undefined => {
			const candidate = value as any;
			if (candidate && typeof candidate === "object") {
				if ("__indexed" in candidate && candidate.__indexed) {
					return coerceWithContext(candidate.__indexed, candidate.__context);
				}
				if ("__context" in candidate) {
					return candidate as WithContext<I>;
				}
			}
			return undefined;
		};

		const updateLastDelivered = (
			batch: ValueTypeFromRequest<Resolve, T, I>[],
		) => {
			if (!batch.length) {
				return;
			}
			const indexed = toIndexedForOrdering(batch[batch.length - 1]);
			if (indexed) {
				lastDeliveredIndexed = indexed;
			}
		};

		const compareIndexed = (a: WithContext<I>, b: WithContext<I>): number => {
			return indexerTypes.extractSortCompare(a, b, queryRequestCoerced.sort);
		};

		const isLateResult = (indexed: WithContext<I>) => {
			if (!lastDeliveredIndexed) {
				return false;
			}
			return compareIndexed(indexed, lastDeliveredIndexed) < 0;
		};

		let maybeSetDone = () => {
			cleanup();
			done = true;
		};
		let unsetDone = () => {
			done = false;
		};
		let cleanup = () => {
			this.clearResultsQueue(queryRequestCoerced);
		};

		let warmupPromise: Promise<any> | undefined = undefined;

		let discoveredTargetHashes: string[] | undefined;

		if (typeof options?.remote === "object") {
			let waitForTime: number | undefined = undefined;
			if (options.remote.wait) {
				let t0 = +new Date();

				waitForTime =
					typeof options.remote.wait === "boolean"
						? DEFAULT_TIMEOUT
						: (options.remote.wait.timeout ?? DEFAULT_TIMEOUT);
				let setDoneIfTimeout = false;
				maybeSetDone = () => {
					if (t0 + waitForTime! < +new Date()) {
						cleanup();
						done = true;
					} else {
						setDoneIfTimeout = true;
					}
				};
				unsetDone = () => {
					setDoneIfTimeout = false;
					done = false;
				};
				let timeout = setTimeout(() => {
					if (setDoneIfTimeout) {
						cleanup();
						done = true;
					}
				}, waitForTime);

				cleanup = () => {
					this.clearResultsQueue(queryRequestCoerced);
					clearTimeout(timeout);
				};
			}

			if (options.remote.reach?.discover) {
				const discoverTimeout =
					waitForTime ??
					(options.remote.wait ? DEFAULT_TIMEOUT : DISCOVER_TIMEOUT_FALLBACK);
				const discoverPromise = this.waitFor(options.remote.reach.discover, {
					signal: ensureController().signal,
					seek: "present",
					timeout: discoverTimeout,
				})
					.then((hashes) => {
						discoveredTargetHashes = hashes;
					})
					.catch((error) => {
						if (error instanceof TimeoutError || error instanceof AbortError) {
							discoveredTargetHashes = [];
							return;
						}
						throw error;
					});
				const prior = warmupPromise ?? Promise.resolve();
				warmupPromise = prior.then(() => discoverPromise);
				options.remote.reach.eager = true; // include the results from the discovered peer even if it is not mature
			}

			const waitPolicy =
				typeof options.remote.wait === "object"
					? options.remote.wait
					: undefined;
			if (
				waitPolicy?.behavior === "block" &&
				(waitPolicy.until ?? "any") === "any"
			) {
				const blockPromise = this.waitForCoverReady({
					domain: options.remote.domain,
					eager: options.remote.reach?.eager,
					settle: "any",
					timeout: waitPolicy.timeout ?? DEFAULT_TIMEOUT,
					signal: ensureController().signal,
					onTimeout: waitPolicy.onTimeout,
				});
				warmupPromise = warmupPromise
					? Promise.all([warmupPromise, blockPromise]).then(() => undefined)
					: blockPromise;
			}
		}

		const fetchFirst = async (
			n: number,
			fetchOptions?: { from?: string[]; fetchedFirstForRemote?: Set<string> },
		): Promise<boolean> => {
			await warmupPromise;
			let hasMore = false;
			const discoverTargets =
				typeof options?.remote === "object"
					? options.remote.reach?.discover
					: undefined;
			const initialRemoteTargets =
				discoveredTargetHashes !== undefined
					? discoveredTargetHashes
					: discoverTargets?.map((pk) => pk.hashcode().toString());
			const skipRemoteDueToDiscovery =
				typeof options?.remote === "object" &&
				options.remote.reach?.discover &&
				discoveredTargetHashes?.length === 0;

			queryRequestCoerced.fetch = n;
			await this.queryCommence(
				queryRequestCoerced,
				{
					local: fetchOptions?.from != null ? false : options?.local,
					remote:
						options?.remote !== false && !skipRemoteDueToDiscovery
							? {
									...(typeof options?.remote === "object"
										? options.remote
										: {}),
									from: fetchOptions?.from ?? initialRemoteTargets,
								}
							: false,
					resolve,
					onResponse: async (response, from) => {
						if (!from) {
							logger.error("Missing response from");
							return;
						}
						if (response instanceof types.NoAccess) {
							logger.error("Dont have access");
							return;
						} else if (response instanceof types.Results) {
							const results = response as types.Results<
								types.ResultTypeFromRequest<R, T, I>
							>;

							const existingBuffer = peerBufferMap.get(from.hashcode());
							const buffer: BufferedResult<
								types.ResultTypeFromRequest<R, T, I> | I,
								I
							>[] = existingBuffer?.buffer || [];

							if (results.kept === 0n && results.results.length === 0) {
								if (keepRemoteAlive) {
									peerBufferMap.set(from.hashcode(), {
										buffer,
										kept: 0,
									});
								}
								return;
							}

							const reqFetch = queryRequestCoerced.fetch ?? 0;
							const inferredMore =
								reqFetch > 0 && results.results.length > reqFetch;
							const effectiveKept = Math.max(
								Number(results.kept),
								inferredMore ? 1 : 0,
							);

							if (effectiveKept > 0) {
								hasMore = true;
							}

							for (const result of results.results) {
								const indexKey = indexerTypes.toId(
									this.indexByResolver(result.value),
								).primitive;
								if (result instanceof types.ResultValue) {
									const existingIndexed = indexedPlaceholders?.get(indexKey);
									if (existingIndexed) {
										existingIndexed.value =
											result.value as types.ResultTypeFromRequest<R, T, I>;
										existingIndexed.context = result.context;
										existingIndexed.from = from!;
										existingIndexed.indexed = await this.resolveIndexed<R>(
											result,
											results.results,
										);
										indexedPlaceholders?.delete(indexKey);
										continue;
									}
									if (visited.has(indexKey)) {
										continue;
									}
									visited.add(indexKey);

									buffer.push({
										value: result.value as types.ResultTypeFromRequest<R, T, I>,
										context: result.context,
										from,
										indexed: await this.resolveIndexed<R>(
											result,
											results.results,
										),
									});
								} else {
									if (
										visited.has(indexKey) &&
										!indexedPlaceholders?.has(indexKey)
									) {
										continue;
									}
									visited.add(indexKey);
									const indexed = coerceWithContext(
										result.indexed || result.value,
										result.context,
									);
									const placeholder = {
										value: result.value,
										context: result.context,
										from,
										indexed,
									};
									buffer.push(placeholder);
									ensureIndexedPlaceholders().set(indexKey, placeholder);
								}
							}

							peerBufferMap.set(from.hashcode(), {
								buffer,
								kept: effectiveKept,
							});
						} else {
							throw new Error(
								"Unsupported result type: " + response?.constructor?.name,
							);
						}
					},
				},
				fetchOptions?.fetchedFirstForRemote,
			);

			if (!hasMore) {
				maybeSetDone();
			}

			return !hasMore;
		};

		const fetchAtLeast = async (n: number) => {
			if (done && first) {
				return;
			}

			if (this.closed) {
				throw new ClosedError();
			}

			await fetchPromise;

			totalFetchedCounter += n;

			if (!first) {
				first = true;
				fetchPromise = fetchFirst(n);
				return fetchPromise;
			}

			const promises: Promise<any>[] = [];
			let resultsLeft = 0;

			for (const [peer, buffer] of peerBufferMap) {
				if (buffer.buffer.length < n) {
					const hasExistingRemoteResults = buffer.kept > 0;
					if (!hasExistingRemoteResults && !keepRemoteAlive) {
						if (peerBufferMap.get(peer)?.buffer.length === 0) {
							peerBufferMap.delete(peer); // No more results
						}
						continue;
					}

					// TODO buffer more than deleted?
					// TODO batch to multiple 'to's

					const lacking = n - buffer.buffer.length;
					const amount = lacking > 0 ? lacking : keepRemoteAlive ? 1 : 0;

					if (amount <= 0) {
						continue;
					}

					const collectRequest = new types.CollectNextRequest({
						id: queryRequestCoerced.id,
						amount,
					});
					// Fetch locally?
					if (peer === this.node.identity.publicKey.hashcode()) {
						if (!this._resumableIterators.has(queryRequestCoerced.idString)) {
							continue; // no more results
						}
						promises.push(
							this.processQuery(
								collectRequest,
								this.node.identity.publicKey,
								true,
							)
								.then(async (results) => {
									resultsLeft += Number(results.kept);

									if (results.results.length === 0) {
										if (
											!keepRemoteAlive &&
											peerBufferMap.get(peer)?.buffer.length === 0
										) {
											peerBufferMap.delete(peer); // No more results
										}
									} else {
										const peerBuffer = peerBufferMap.get(peer);
										if (!peerBuffer) {
											return;
										}
										peerBuffer.kept = Number(results.kept);

										for (const result of results.results) {
											const keyPrimitive = indexerTypes.toId(
												this.indexByResolver(result.value),
											).primitive;
											if (result instanceof types.ResultValue) {
												const existingIndexed =
													indexedPlaceholders?.get(keyPrimitive);
												if (existingIndexed) {
													existingIndexed.value =
														result.value as types.ResultTypeFromRequest<
															R,
															T,
															I
														>;
													existingIndexed.context = result.context;
													existingIndexed.from = this.node.identity.publicKey;
													existingIndexed.indexed =
														await this.resolveIndexed<R>(
															result,
															results.results as types.ResultTypeFromRequest<
																R,
																T,
																I
															>[],
														);
													indexedPlaceholders?.delete(keyPrimitive);
													continue;
												}
												if (visited.has(keyPrimitive)) {
													continue;
												}
												visited.add(keyPrimitive);
												const indexed = await this.resolveIndexed<R>(
													result,
													results.results as types.ResultTypeFromRequest<
														R,
														T,
														I
													>[],
												);
												peerBuffer.buffer.push({
													value: result.value as types.ResultTypeFromRequest<
														R,
														T,
														I
													>,
													context: result.context,
													from: this.node.identity.publicKey,
													indexed,
												});
											} else {
												if (
													visited.has(keyPrimitive) &&
													!indexedPlaceholders?.has(keyPrimitive)
												) {
													continue;
												}
												visited.add(keyPrimitive);
												const indexed = coerceWithContext(
													result.indexed || result.value,
													result.context,
												);
												const placeholder = {
													value: result.value,
													context: result.context,
													from: this.node.identity.publicKey,
													indexed,
												};
												peerBuffer.buffer.push(placeholder);
												ensureIndexedPlaceholders().set(
													keyPrimitive,
													placeholder,
												);
											}
										}
									}
								})
								.catch((e) => {
									logger.error(
										"Failed to collect sorted results from self. " + e?.message,
									);
									peerBufferMap.delete(peer);
								}),
						);
					} else {
						// Fetch remotely
						const idTranslation =
							this._prefetch?.accumulator.getTranslationMap(
								queryRequestCoerced,
							);
						let remoteCollectRequest: types.CollectNextRequest = collectRequest;
						if (idTranslation) {
							remoteCollectRequest = new types.CollectNextRequest({
								id: idTranslation.get(peer) || collectRequest.id,
								amount: collectRequest.amount,
							});
						}

						promises.push(
							this._query
								.request(remoteCollectRequest, {
									...options,
									signal: options?.signal
										? AbortSignal.any([
												options.signal,
												ensureController().signal,
											])
										: ensureController().signal,
									priority: 1,
									mode: new SilentDelivery({ to: [peer], redundancy: 1 }),
								})
								.then((response) =>
									introduceEntries(
										queryRequestCoerced,
										response,
										this.documentType,
										this.indexedType,
										this._sync,
										options as QueryDetailedOptions<T, I, D, any>,
									)
										.then(async (responses) => {
											return Promise.all(
												responses.map(async (response, i) => {
													resultsLeft += Number(response.response.kept);
													const from = responses[i].from;
													if (!from) {
														logger.error("Missing from for sorted query");
														return;
													}

													if (response.response.results.length === 0) {
														if (
															!keepRemoteAlive &&
															peerBufferMap.get(peer)?.buffer.length === 0
														) {
															peerBufferMap.delete(peer); // No more results
														}
													} else {
														const peerBuffer = peerBufferMap.get(peer);
														if (!peerBuffer) {
															return;
														}
														peerBuffer.kept = Number(response.response.kept);
														for (const result of (
															response as RPCResponse<
																types.Results<
																	types.ResultTypeFromRequest<R, T, I>
																>
															>
														).response.results) {
															const indexKey = indexerTypes.toId(
																this.indexByResolver(result.value),
															).primitive;
															if (result instanceof types.ResultValue) {
																const existingIndexed =
																	indexedPlaceholders?.get(indexKey);
																if (existingIndexed) {
																	existingIndexed.value =
																		result.value as types.ResultTypeFromRequest<
																			R,
																			T,
																			I
																		>;
																	existingIndexed.context = result.context;
																	existingIndexed.from = from!;
																	existingIndexed.indexed =
																		await this.resolveIndexed(
																			result,
																			response.response
																				.results as types.ResultTypeFromRequest<
																				R,
																				T,
																				I
																			>[],
																		);
																	indexedPlaceholders?.delete(indexKey);
																	continue;
																}
																if (visited.has(indexKey)) {
																	continue;
																}
																visited.add(indexKey);

																const indexed = await this.resolveIndexed(
																	result,
																	response.response
																		.results as types.ResultTypeFromRequest<
																		R,
																		T,
																		I
																	>[],
																);
																peerBuffer.buffer.push({
																	value:
																		result.value as types.ResultTypeFromRequest<
																			R,
																			T,
																			I
																		>,
																	context: result.context,
																	from: from!,
																	indexed,
																});
															} else {
																if (
																	visited.has(indexKey) &&
																	!indexedPlaceholders?.has(indexKey)
																) {
																	continue;
																}
																visited.add(indexKey);
																const indexed = coerceWithContext(
																	result.value,
																	result.context,
																);
																const placeholder = {
																	value: result.value,
																	context: result.context,
																	from: from!,
																	indexed,
																};
																peerBuffer.buffer.push(placeholder);
																ensureIndexedPlaceholders().set(
																	indexKey,
																	placeholder,
																);
															}
														}
													}
												}),
											);
										})
										.catch((e) => {
											logger.error(
												"Failed to collect sorted results from: " +
													peer +
													". " +
													e?.message,
											);
											peerBufferMap.delete(peer);
										}),
								),
						);
					}
				} else {
					resultsLeft += peerBufferMap.get(peer)?.kept || 0;
				}
			}
			return (fetchPromise = Promise.all(promises).then(() => {
				return resultsLeft === 0; // 0 results left to fetch and 0 pending results
			}));
		};

		const next = async (n: number) => {
			if (n < 0) {
				throw new Error("Expecting to fetch a positive amount of element");
			}

			if (n === 0) {
				return [];
			}

			// TODO everything below is not very optimized
			const fetchedAll = await fetchAtLeast(n);

			// get n next top entries, shift and pull more results
			const peerBuffersArr = peerBuffers();
			const results = peerBuffersArr.sort((a, b) =>
				indexerTypes.extractSortCompare(
					a.indexed,
					b.indexed,
					queryRequestCoerced.sort,
				),
			);

			lastValueInOrder = results[0] || lastValueInOrder;
			const pendingMoreResults = n < results.length; // check if there are more results to fetch, before splicing
			const batch = results.splice(0, n);
			const hasMore = !fetchedAll || pendingMoreResults;

			for (const result of batch) {
				const arr = peerBufferMap.get(result.from.hashcode());
				if (!arr) {
					logger.error("Unexpected empty result buffer");
					continue;
				}
				const idx = arr.buffer.findIndex((x) => x.value === result.value);
				if (idx >= 0) {
					arr.buffer.splice(idx, 1);
					const consumedId = indexerTypes.toId(
						this.indexByResolver(result.indexed),
					).primitive;
					indexedPlaceholders?.delete(consumedId);
				}
			}

			if (hasMore) {
				unsetDone();
			} else {
				maybeSetDone();
			}

			let coercedBatch: ValueTypeFromRequest<Resolve, T, I>[];
			if (resolve) {
				coercedBatch = (
					await Promise.all(
						batch.map(async (x) => {
							const withContext = coerceWithContext(
								x.value instanceof this.documentType
									? x.value
									: (
											await this.resolveDocument({
												head: x.context.head,
												indexed: x.indexed,
											})
										)?.value,
								x.context,
							);
							const withIndexed = coerceWithIndexed(withContext, x.indexed);
							return withIndexed;
						}),
					)
				).filter((x) => !!x) as ValueTypeFromRequest<Resolve, T, I>[];
			} else {
				coercedBatch = batch.map((x) =>
					coerceWithContext(coerceWithIndexed(x.value, x.indexed), x.context),
				) as ValueTypeFromRequest<Resolve, T, I>[];
			}

			// no extra queued-first/last in simplified API

			const deduped = dedup(coercedBatch, this.indexByResolver);
			const fallbackReason = hasDeliveredResults ? "manual" : "initial";
			updateLastDelivered(deduped);
			await emitOnBatch(deduped, fallbackReason);
			return deduped;
		};

		let cleanupAndDone = () => {
			cleanup();
			(controller as AbortController | undefined)?.abort(
				new AbortError("Iterator closed"),
			);
			controller = undefined;
			this.prefetch?.accumulator.clear(queryRequestCoerced);
			this.processCloseIteratorRequest(
				queryRequestCoerced,
				this.node.identity.publicKey,
			);
			done = true;
		};

		let close = async () => {
			cleanupAndDone();

			// send close to remote
			const closeRequest = new types.CloseIteratorRequest({
				id: queryRequestCoerced.id,
			});
			const promises: Promise<any>[] = [];

			for (const [peer, buffer] of peerBufferMap) {
				if (buffer.kept === 0) {
					peerBufferMap.delete(peer);
					continue;
				}
				if (peer !== this.node.identity.publicKey.hashcode()) {
					// Close remote
					promises.push(
						this._query.send(closeRequest, {
							...options,
							mode: new SilentDelivery({ to: [peer], redundancy: 1 }),
						}),
					);
				}
			}
			await Promise.all(promises);
		};
		options?.signal && options.signal.addEventListener("abort", close);

		let doneFn = () => {
			return done;
		};

		let joinListener: (() => void) | undefined;

		let fetchedFirstForRemote: Set<string> | undefined = undefined;

		let updateDeferred: ReturnType<typeof pDefer> | undefined;
		const onLateResultsQueue =
			options?.outOfOrder?.mode === "queue" &&
			typeof options?.outOfOrder?.handle === "function"
				? (options.outOfOrder.handle as (
						evt: LateResultsEvent<"queue">,
						helpers: LateResultsHelpers<"queue">,
					) => void | Promise<void>)
				: undefined;
		const onLateResultsDrop =
			options?.outOfOrder?.mode === "queue"
				? undefined
				: typeof options?.outOfOrder?.handle === "function"
					? (options.outOfOrder.handle as (
							evt: LateResultsEvent<"drop">,
							helpers: LateResultsHelpers<"drop">,
						) => void | Promise<void>)
					: undefined;
		const normalizeLateItems = (
			items?:
				| {
						indexed: WithContext<I>;
						context: types.Context;
						from: PublicSignKey;
						value?: types.ResultTypeFromRequest<R, T, I> | I;
				  }[]
				| undefined,
		): LateResultsItem[] | undefined => {
			if (!items) return undefined;
			return items.map((item) => {
				const ctx = item.context || item.indexed.__context;
				let value = (item.value ?? item.indexed) as any;
				if (value && ctx && value.__context == null) {
					value.__context = ctx;
				}
				if (value && item.indexed && value.__indexed == null) {
					value.__indexed = item.indexed;
				}
				return {
					indexed: item.indexed,
					context: ctx,
					from: item.from,
					value,
				};
			});
		};

		const notifyLateResults =
			onLateResultsQueue || onLateResultsDrop
				? (
						amount: number,
						peer?: PublicSignKey,
						items?: {
							indexed: WithContext<I>;
							context: types.Context;
							from: PublicSignKey;
							value?: types.ResultTypeFromRequest<R, T, I> | I;
						}[],
					) => {
						if (amount <= 0) {
							return;
						}
						unsetDone();
						const payload = items ? normalizeLateItems(items) : undefined;
						if (outOfOrderMode === "queue" && onLateResultsQueue) {
							const normalized =
								payload ??
								([] as LateResultsEvent<"queue", LateResultsItem>["items"]);
							const collector = () =>
								Promise.resolve(
									normalized as LateResultsEvent<
										"queue",
										LateResultsItem
									>["items"],
								);
							onLateResultsQueue(
								{
									amount,
									peer,
									items: normalized as LateResultsEvent<
										"queue",
										LateResultsItem
									>["items"],
								},
								{ collect: collector },
							);
							return;
						}
						if (onLateResultsDrop) {
							const collector = () =>
								Promise.resolve(
									payload as LateResultsEvent<"drop", LateResultsItem>["items"],
								);
							onLateResultsDrop(
								{
									amount,
									peer,
								},
								{ collect: collector },
							);
						}
					}
				: undefined;
		const runNotify = (reason: UpdateReason) => {
			if (!updateCallbacks?.notify) {
				return;
			}
			Promise.resolve(updateCallbacks.notify(reason)).catch((error) => {
				warn("Update notify callback failed", error);
			});
		};
		const signalUpdate = (reason?: UpdateReason) => {
			if (reason) {
				runNotify(reason);
			}
			updateDeferred?.resolve();
		};
		const _waitForUpdate = () =>
			updateDeferred ? updateDeferred.promise : Promise.resolve();

		// ---------------- Live updates wiring (sorted-only with optional filter) ----------------
		const updateCallbacks = updateCallbacksRaw;
		let pendingBatchReason:
			| Extract<UpdateReason, "join" | "change" | "push">
			| undefined;
		let hasDeliveredResults = false;

		const emitOnBatch = async (
			batch: ValueTypeFromRequest<Resolve, T, I>[],
			defaultReason: Extract<UpdateReason, "initial" | "manual">,
		) => {
			if (!updateCallbacks?.onBatch || batch.length === 0) {
				return;
			}
			let reason: UpdateReason;
			if (pendingBatchReason) {
				reason = pendingBatchReason;
			} else if (!hasDeliveredResults) {
				reason = "initial";
			} else {
				reason = defaultReason;
			}
			pendingBatchReason = undefined;
			hasDeliveredResults = true;
			await updateCallbacks.onBatch(batch, { reason });
		};

		// sorted-only mode: no per-queue handling

		// If live updates enabled, ensure deferred exists so awaiting paths can block until changes
		if (hasLiveUpdates && !updateDeferred) {
			updateDeferred = pDefer<void>();
		}

		const keepOpen =
			options?.closePolicy === "manual" || hasLiveUpdates || pushUpdates;
		const keepRemoteAlive = keepOpen && remoteOptions !== false;

		if (queryRequestCoerced instanceof types.IterationRequest) {
			queryRequestCoerced.resolve = resolve;
			queryRequestCoerced.fetch = queryRequestCoerced.fetch ?? 10;
			const replicateFlag = !resolve && replicate ? true : false;
			queryRequestCoerced.replicate = replicateFlag;
			const ttlSource =
				typeof remoteOptions === "object" &&
				typeof remoteOptions?.wait === "object" &&
				remoteOptions.wait.behavior === "block"
					? (remoteOptions.wait.timeout ?? DEFAULT_KEEP_REMOTE_ITERATOR_TIMEOUT)
					: DEFAULT_KEEP_REMOTE_ITERATOR_TIMEOUT;
			queryRequestCoerced.keepAliveTtl = keepRemoteAlive
				? BigInt(ttlSource)
				: undefined;
			queryRequestCoerced.pushUpdates = pushUpdates;
			queryRequestCoerced.mergeUpdates = mergePolicy?.merge ? true : undefined;
		}

		if (pushUpdates && this.prefetch?.accumulator) {
			const currentPrefetchKey = () => idAgnosticQueryKey(queryRequestCoerced);
			const mergePrefetchedResults = async (
				from: PublicSignKey,
				results: types.Results<types.ResultTypeFromRequest<R, T, I>>,
			) => {
				const peerHash = from.hashcode();
				const existingBuffer = peerBufferMap.get(peerHash);
				const buffer: BufferedResult<
					types.ResultTypeFromRequest<R, T, I> | I,
					I
				>[] = existingBuffer?.buffer || [];

				if (results.kept === 0n && results.results.length === 0) {
					peerBufferMap.set(peerHash, {
						buffer,
						kept: Number(results.kept),
					});
					return;
				}

				const collectLateItems =
					outOfOrderMode !== "drop" && !!notifyLateResults;
				const lateResults = collectLateItems
					? ([] as {
							indexed: WithContext<I>;
							context: types.Context;
							from: PublicSignKey;
							value?: types.ResultTypeFromRequest<R, T, I> | I;
						}[])
					: undefined;
				let lateCount = 0;

				for (const result of results.results) {
					const indexKey = indexerTypes.toId(
						this.indexByResolver(result.value),
					).primitive;
					if (result instanceof types.ResultValue) {
						const existingIndexed = indexedPlaceholders?.get(indexKey);
						if (existingIndexed) {
							existingIndexed.value =
								result.value as types.ResultTypeFromRequest<R, T, I>;
							existingIndexed.context = result.context;
							existingIndexed.from = from;
							existingIndexed.indexed = await this.resolveIndexed<R>(
								result,
								results.results as types.ResultTypeFromRequest<R, T, I>[],
							);
							indexedPlaceholders?.delete(indexKey);
							continue;
						}
						const indexed = await this.resolveIndexed<R>(
							result,
							results.results as types.ResultTypeFromRequest<R, T, I>[],
						);
						const late = isLateResult(indexed);
						if (late) {
							lateCount++;
							lateResults?.push({
								indexed,
								context: result.context,
								from: from!,
								value: result.value as types.ResultTypeFromRequest<R, T, I>,
							});
							if (outOfOrderMode === "drop") {
								visited.add(indexKey);
								continue; // don't buffer late push results
							}
						}
						if (visited.has(indexKey)) {
							continue;
						}
						visited.add(indexKey);
						buffer.push({
							value: result.value as types.ResultTypeFromRequest<R, T, I>,
							context: result.context,
							from,
							indexed,
						});
					} else {
						const indexed = coerceWithContext(
							result.indexed || result.value,
							result.context,
						);
						const late = isLateResult(indexed);
						if (late) {
							lateCount++;
							lateResults?.push({
								indexed,
								context: result.context,
								from: from!,
								value: result.value,
							});
							if (outOfOrderMode === "drop") {
								visited.add(indexKey);
								continue; // don't buffer late push results
							}
						}
						if (visited.has(indexKey) && !indexedPlaceholders?.has(indexKey)) {
							continue;
						}
						visited.add(indexKey);
						const placeholder = {
							value: result.value,
							context: result.context,
							from,
							indexed,
						};
						buffer.push(placeholder);
						ensureIndexedPlaceholders().set(indexKey, placeholder);
					}
				}

				if (lateCount > 0) {
					notifyLateResults?.(
						lateCount,
						from,
						collectLateItems ? lateResults : undefined,
					);
				}

				peerBufferMap.set(peerHash, {
					buffer,
					// Prefetched batches should not claim remote pending counts;
					// we'll collect more explicitly if needed.
					kept: 0,
				});
			};

			const consumePrefetch = async (
				consumable: RPCResponse<types.PredictedSearchRequest<any>>,
			) => {
				const request = consumable.response?.request;
				if (!request) {
					return;
				}
				if (idAgnosticQueryKey(request) !== currentPrefetchKey()) {
					return;
				}
				try {
					const prepared = await introduceEntries(
						queryRequestCoerced,
						[
							{
								response: consumable.response.results,
								from: consumable.from,
							},
						],
						this.documentType,
						this.indexedType,
						this._sync,
						options as QueryDetailedOptions<T, I, D, any>,
					);

					for (const response of prepared) {
						if (!response.from) {
							continue;
						}
						const payload = response.response;
						if (!(payload instanceof types.Results)) {
							continue;
						}
						await mergePrefetchedResults(
							response.from,
							payload as types.Results<types.ResultTypeFromRequest<R, T, I>>,
						);
					}

					if (!pendingBatchReason) {
						pendingBatchReason = "push";
					}
					signalUpdate("push");
				} catch (error) {
					warn("Failed to merge prefetched results", error);
				}
			};

			const onPrefetchAdd = (
				evt: CustomEvent<{
					consumable: RPCResponse<types.PredictedSearchRequest<any>>;
				}>,
			) => {
				void consumePrefetch(evt.detail.consumable);
			};
			this.prefetch.accumulator.addEventListener(
				"add",
				onPrefetchAdd as EventListener,
			);
			const cleanupDefault = cleanup;
			cleanup = () => {
				this.prefetch?.accumulator.removeEventListener(
					"add",
					onPrefetchAdd as EventListener,
				);
				return cleanupDefault();
			};
		}

		let updatesCleanup: (() => void) | undefined;
		if (hasLiveUpdates) {
			const localHash = this.node.identity.publicKey.hashcode();
			if (mergePolicy?.merge) {
				// Ensure local buffer exists for sorted merging
				if (!peerBufferMap.has(localHash)) {
					peerBufferMap.set(localHash, { kept: 0, buffer: [] });
				}
			}

			const queryFiltersForUpdates = indexerTypes.toQuery(
				queryRequestCoerced.query,
			);
			const hasQueryFiltersForUpdates = queryFiltersForUpdates.length > 0;

			const createUpdateFilterIndex = async () => {
				const index = new HashmapIndex<WithContext<I>>();
				await index.init({
					schema: this.wrappedIndexedType,
					indexBy: this.indexBy,
					nested: this.nestedProperties,
				});
				return index;
			};

			const toIndexedWithContext = async (
				value: WithContext<T> | WithContext<I>,
			): Promise<WithContext<I>> => {
				const candidate = value as WithContext<I> & Partial<WithIndexed<T, I>>;
				if ("__indexed" in candidate && candidate.__indexed) {
					return coerceWithContext(candidate.__indexed, candidate.__context);
				}

				if (value instanceof this.documentType) {
					const transformed = await this.transformer(
						value as T,
						value.__context,
					);
					return coerceWithContext(transformed, value.__context);
				}

				return value as WithContext<I>;
			};

			const onChange = async (evt: CustomEvent<DocumentsChange<T, I>>) => {
				// Optional filter to mutate/suppress change events
				indexIteratorLogger.trace(
					"processing live update change event",
					evt.detail,
				);
				let filtered: DocumentsChange<T, I> | void = evt.detail;
				if (mergePolicy?.merge?.filter) {
					filtered = await mergePolicy.merge?.filter(evt.detail);
				}
				if (filtered) {
					let hasRelevantChange = false;

					// Remove entries that were deleted from all pending structures
					if (filtered.removed?.length) {
						const removedIds = new Set<string | number | bigint>();
						for (const removed of filtered.removed) {
							const id = indexerTypes.toId(
								this.indexByResolver(removed.__indexed),
							).primitive;
							removedIds.add(id);
						}
						const matchedRemovedIds = new Set<string | number | bigint>();
						for (const [_peer, entry] of peerBufferMap) {
							if (entry.buffer.length === 0) {
								continue;
							}
							entry.buffer = entry.buffer.filter((x) => {
								const id = indexerTypes.toId(
									this.indexByResolver(x.indexed),
								).primitive;
								if (removedIds.has(id)) {
									matchedRemovedIds.add(id);
									indexedPlaceholders?.delete(id);
									return false;
								}
								return true;
							});
						}
						if (matchedRemovedIds.size > 0) {
							hasRelevantChange = true;
						}
					}

					// Add new entries per strategy (sorted-only)
					if (filtered.added?.length) {
						let buf = peerBufferMap.get(localHash);
						if (!buf) {
							const created: {
								kept: number;
								buffer: BufferedResult<
									types.ResultTypeFromRequest<R, T, I> | I,
									I
								>[];
							} = { kept: 0, buffer: [] };
							peerBufferMap.set(localHash, created);
							buf = created;
						}
						const filterIndex = hasQueryFiltersForUpdates
							? await createUpdateFilterIndex()
							: undefined;
						for (const added of filtered.added) {
							const addedValue = added as WithContext<T> &
								Partial<WithIndexed<T, I>>;
							const indexedCandidate = await toIndexedWithContext(addedValue);
							if (filterIndex) {
								filterIndex.drop();
								filterIndex.put(indexedCandidate);
								const matches =
									(
										await filterIndex
											.iterate(
												{
													query: queryFiltersForUpdates,
													sort: queryRequestCoerced.sort,
												},
												{ reference: true, shape: undefined },
											)
											.next(1)
									).length > 0;
								if (!matches) {
									continue;
								}
							}
							const id = indexerTypes.toId(
								this.indexByResolver(indexedCandidate),
							).primitive;
							const existingIndexed = indexedPlaceholders?.get(id);
							if (existingIndexed) {
								if (resolve) {
									existingIndexed.value = added as any;
									existingIndexed.context = added.__context;
									existingIndexed.from = this.node.identity.publicKey;
									existingIndexed.indexed = indexedCandidate;
									indexedPlaceholders?.delete(id);
								}
								continue;
							}
							if (visited.has(id)) continue; // already presented
							const wasLate = isLateResult(indexedCandidate);
							if (wasLate) {
								notifyLateResults?.(1, this.node.identity.publicKey, [
									{
										indexed: indexedCandidate,
										context: added.__context,
										from: this.node.identity.publicKey,
										value: resolve ? (added as any) : indexedCandidate,
									},
								]);
								if (outOfOrderMode === "drop") {
									continue;
								}
							}
							visited.add(id);
							const valueForBuffer = resolve
								? (added as any)
								: indexedCandidate;
							const placeholder = {
								value: valueForBuffer,
								context: added.__context,
								from: this.node.identity.publicKey,
								indexed: indexedCandidate,
							};
							buf.buffer.push(placeholder);
							if (!resolve) {
								ensureIndexedPlaceholders().set(id, placeholder);
							}
							hasRelevantChange = true;
						}
					}

					if (hasRelevantChange) {
						runNotify("change");
						if (!pendingBatchReason) {
							pendingBatchReason = "change";
						}
						signalUpdate();
					}
				}
				signalUpdate();
			};

			this.documentEvents.addEventListener("change", onChange);
			updatesCleanup = () => {
				this.documentEvents.removeEventListener("change", onChange);
			};
			const cleanupDefaultUpdates = cleanup;
			cleanup = () => {
				updatesCleanup?.();
				return cleanupDefaultUpdates();
			};
		}

		if (typeof options?.remote === "object" && options?.remote.wait) {
			// was used to account for missed results when a peer joins; omitted in this minimal handler

			updateDeferred = pDefer<void>();

			const waitForTime =
				typeof options.remote.wait === "object" && options.remote.wait.timeout;

			const prevMaybeSetDone = maybeSetDone;
			maybeSetDone = () => {
				prevMaybeSetDone();
				if (done) signalUpdate(); // break deferred waits
			};

			let joinTimeoutId =
				waitForTime &&
				setTimeout(() => {
					signalUpdate();
				}, waitForTime);
			ensureController().signal.addEventListener("abort", () => signalUpdate());
			fetchedFirstForRemote = new Set<string>();
			joinListener = this.createReplicatorJoinListener({
				signal: ensureController().signal,
				eager: options.remote.reach?.eager,
				onPeer: async (pk) => {
					if (done) return;
					const hash = pk.hashcode();
					await fetchPromise; // ensure fetches in flight are done
					if (peerBufferMap.has(hash)) return;
					if (fetchedFirstForRemote!.has(hash)) return;
					if (totalFetchedCounter > 0) {
						fetchPromise = fetchFirst(totalFetchedCounter, {
							from: [hash],
							fetchedFirstForRemote,
						});
						await fetchPromise;
						if (onLateResultsQueue || onLateResultsDrop) {
							const pending = peerBufferMap.get(hash)?.buffer;
							if (pending && pending.length > 0) {
								if (lastValueInOrder) {
									const pendingWithLast = [...pending.flat(), lastValueInOrder];
									const results = pendingWithLast.sort((a, b) =>
										indexerTypes.extractSortCompare(
											a.indexed,
											b.indexed,
											queryRequestCoerced.sort,
										),
									);

									const lateResults = results.findIndex(
										(x) => x === lastValueInOrder,
									);
									if (lateResults > 0) {
										const lateItems =
											outOfOrderMode === "queue"
												? results.slice(
														0,
														Math.min(lateResults, results.length),
													)
												: undefined;
										notifyLateResults?.(lateResults, pk, lateItems);
									}
								} else {
									notifyLateResults?.(
										pending.length,
										pk,
										outOfOrderMode === "queue" ? pending : undefined,
									);
								}
							}
						}
					}
					if (!pendingBatchReason) {
						pendingBatchReason = "join";
					}
					signalUpdate("join");
				},
			});
			const cleanupDefault = cleanup;
			cleanup = () => {
				joinListener && joinListener();
				joinTimeoutId && clearTimeout(joinTimeoutId);
				updateDeferred?.resolve();
				updateDeferred = undefined;
				return cleanupDefault();
			};
		}

		if (keepOpen) {
			const prevMaybeSetDone = maybeSetDone;
			maybeSetDone = () => {
				if (drain) {
					prevMaybeSetDone();
				}
			};
		}
		const remoteWaitActive =
			typeof options?.remote === "object" && !!options.remote.wait;

		const waitForUpdateAndResetDeferred = async () => {
			if (remoteWaitActive) {
				// wait until: join fetch adds results, cleanup runs, or the join-wait times out
				await _waitForUpdate();

				// re-arm the deferred for the next cycle (only if joining is enabled and we're not done)
				if (updateDeferred && !doneFn()) {
					updateDeferred = pDefer<void>();
				}
			}
		};

		return {
			close,
			next,
			done: doneFn,
			pending: async () => {
				try {
					await fetchPromise;
					if (!done && keepRemoteAlive) {
						await fetchAtLeast(1);
					}
				} catch (error) {
					warn("Failed to refresh iterator pending state", error);
				}

				let total = 0;
				for (const buffer of peerBufferMap.values()) {
					total += buffer.kept + buffer.buffer.length;
				}
				return total;
			},
			all: async () => {
				drain = true;
				let result: ValueTypeFromRequest<Resolve, T, I>[] = [];
				let c = 0;
				while (doneFn() !== true) {
					let batch = await next(100);
					c += batch.length;
					if (c > WARNING_WHEN_ITERATING_FOR_MORE_THAN) {
						warn(
							"Iterating for more than " +
								WARNING_WHEN_ITERATING_FOR_MORE_THAN +
								" results",
						);
					}
					if (batch.length > 0) {
						result.push(...batch);
						continue;
					}
					await waitForUpdateAndResetDeferred();
				}
				cleanupAndDone();
				return result;
			},
			first: async () => {
				if (doneFn()) {
					return undefined;
				}
				let batch = await next(1);
				cleanupAndDone();
				return batch[0];
			},
			[Symbol.asyncIterator]: async function* () {
				drain = true;
				let c = 0;
				while (doneFn() !== true) {
					const batch = await next(100);
					c += batch.length;
					if (c > WARNING_WHEN_ITERATING_FOR_MORE_THAN) {
						warn(
							"Iterating for more than " +
								WARNING_WHEN_ITERATING_FOR_MORE_THAN +
								" results",
						);
					}
					for (const entry of batch) {
						yield entry;
					}
					await waitForUpdateAndResetDeferred();
				}
				cleanupAndDone();
			},
		};
	}

	public async updateResults<R extends boolean, RT = R extends true ? T : I>(
		into: WithContext<RT>[],
		change: {
			added?: WithContext<T>[] | WithContext<I>[];
			removed?: WithContext<T>[] | WithContext<I>[];
		},
		query: QueryLike,
		resolve: R,
	): Promise<WithContext<RT>[]> {
		let intoIndexable: WithContext<I>[];
		if (into.length > 0) {
			if (resolve && into[0] instanceof this.documentType === false) {
				throw new Error(
					"Expecting 'into' to be of type " + this.documentType.name,
				);
			} else if (!resolve && into[0] instanceof this.indexedType === false) {
				throw new Error(
					"Expecting 'into' to be of type " + this.indexedType.name,
				);
			}

			if (resolve) {
				intoIndexable = await Promise.all(
					into.map(async (x) => {
						const transformed = await this.transformer(x as T, x.__context);
						return coerceWithContext(transformed, x.__context);
					}),
				);
			} else {
				intoIndexable = into as any as WithContext<I>[];
			}
		} else {
			intoIndexable = [];
		}

		const temporaryIndex = new HashmapIndex<WithContext<I>>();
		await temporaryIndex.init({
			schema: this.wrappedIndexedType,
			indexBy: this.indexBy,
			nested: this.nestedProperties,
		});
		for (const value of intoIndexable) {
			temporaryIndex.put(value);
		}

		let anyChange = false;
		if (change.added && change.added.length > 0) {
			for (const added of change.added) {
				const indexed =
					added instanceof this.documentType
						? coerceWithContext(
								await this.transformer(added as T, added.__context),
								added.__context,
							)
						: (added as WithContext<I>);
				temporaryIndex.put(indexed);
				anyChange = true;
			}
		}
		if (change.removed && change.removed.length > 0) {
			for (const removed of change.removed) {
				const indexed =
					removed instanceof this.documentType
						? await this.transformer(removed as T, removed.__context)
						: (removed as WithContext<I>);
				const id = indexerTypes.toId(this.indexByResolver(indexed)).primitive;
				const deleted = await temporaryIndex.del({
					query: [indexerTypes.getMatcher(this.indexBy, id)],
				});

				if (deleted.length > 0) {
					anyChange = true;
				}
			}
		}

		if (!anyChange) {
			return into;
		}

		let all = await temporaryIndex
			.iterate(
				{
					query: indexerTypes.toQuery(query.query),
					sort: indexerTypes.toSort(query.sort),
				},
				{ reference: true, shape: undefined },
			)
			.all();

		if (resolve) {
			return (
				await Promise.all(
					all.map(async ({ id, value }) => {
						return this.resolveDocument({
							indexed: value,
							head: value.__context.head,
							id: id.primitive,
						}).then((resolved) => {
							if (resolved) {
								return coerceWithContext(resolved.value, value.__context) as RT;
							}
							return undefined;
						});
					}),
				)
			).filter((x) => !!x) as WithContext<RT>[];
		}
		return all.map((x) => x.value) as any as WithContext<RT>[];
	}

	/**
	 * Resolve the primary key for a document or indexed representation using the configured indexBy fields.
	 * Useful when consumers need a stable id without assuming a specific property name exists.
	 */
	public resolveId(
		value:
			| ValueTypeFromRequest<any, T, I>
			| WithContext<I>
			| WithIndexedContext<T, I>
			| WithIndexed<T, I>
			| I,
	): indexerTypes.IdKey {
		let candidate: any = value;
		if (candidate && typeof candidate === "object") {
			if ("__indexed" in candidate && candidate.__indexed) {
				candidate = candidate.__indexed;
			}
		}

		const resolved = this.indexByResolver(candidate);
		return indexerTypes.toId(resolved);
	}

	public async waitFor(
		other: PeerRefs,
		options?: {
			seek?: "any" | "present";
			signal?: AbortSignal;
			timeout?: number;
		},
	): Promise<string[]> {
		const hashes = await super.waitFor(other, options);
		for (const key of hashes) {
			await waitFor(
				async () =>
					(await this._log.replicationIndex.count({ query: { hash: key } })) >
					0,
				options,
			);
		}
		return hashes;
	}
}
