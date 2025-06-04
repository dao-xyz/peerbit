import { type AbstractType, field, serialize, variant } from "@dao-xyz/borsh";
import type { PeerId } from "@libp2p/interface";
import { Cache } from "@peerbit/cache";
import {
	type MaybePromise,
	PublicSignKey,
	getPublicKeyFromPeerId,
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
import { DataMessage, SilentDelivery } from "@peerbit/stream-interface";
import { AbortError, waitFor } from "@peerbit/time";
import { concat, fromString } from "uint8arrays";
import { copySerialization } from "./borsh.js";
import { MAX_BATCH_SIZE } from "./constants.js";
import type { QueryPredictor } from "./most-common-query-predictor.js";
import MostCommonQueryPredictor from "./most-common-query-predictor.js";
import { type Operation, isPutOperation } from "./operation.js";
import { Prefetch } from "./prefetch.js";
import type { ExtractArgs } from "./program.js";
import { ResumableIterators } from "./resumable-iterator.js";

const logger = loggerFn({ module: "document-index" });

type BufferedResult<T, I extends Record<string, any>> = {
	value: T;
	indexed: WithContext<I>;
	context: types.Context;
	from: PublicSignKey;
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
	eager?: boolean; // whether to query newly joined peers before they have matured
	joining?:
		| boolean
		| { waitFor?: number; onMissedResults?: (evt: { amount: number }) => void }; // whether to query peers that are joining the network
};
export type QueryOptions<R, D, Resolve extends boolean | undefined> = {
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
};
export type SearchOptions<
	R,
	D,
	Resolve extends boolean | undefined,
> = QueryOptions<R, D, Resolve>;

type Transformer<T, I> = (obj: T, context: types.Context) => MaybePromise<I>;

export type ResultsIterator<T> = {
	close: () => Promise<void>;
	next: (number: number) => Promise<T[]>;
	done: () => boolean;
	all: () => Promise<T[]>;
	pending: () => number | undefined;
};

type QueryDetailedOptions<
	T,
	D,
	Resolve extends boolean | undefined,
> = QueryOptions<T, D, Resolve> & {
	onResponse?: (
		response: types.AbstractSearchResult,
		from: PublicSignKey,
	) => void | Promise<void>;
	remote?: {
		from?: string[]; // if specified, only query these peers
	};
};

type QueryLike = {
	query?: indexerTypes.Query[] | indexerTypes.QueryLike;
	sort?: indexerTypes.Sort[] | indexerTypes.Sort | indexerTypes.SortLike;
};

type ExtractResolveFromOptions<O> =
	O extends QueryOptions<any, any, infer X>
		? X extends boolean // if X is a boolean (true or false)
			? X
			: true // else default to true
		: true; // if R isn't QueryLike at all, default to true

const coerceQuery = <Resolve extends boolean | undefined>(
	query: types.SearchRequest | types.SearchRequestIndexed | QueryLike,
	options?: QueryOptions<any, any, Resolve>,
) => {
	let replicate =
		typeof options?.remote !== "boolean" ? options?.remote?.replicate : false;

	if (
		query instanceof types.SearchRequestIndexed &&
		query.replicate === false &&
		replicate
	) {
		query.replicate = true;
		return query;
	}
	if (query instanceof types.SearchRequest) {
		return query;
	}

	const queryObject = query as QueryLike;

	return options?.resolve || options?.resolve == null
		? new types.SearchRequest({
				query: indexerTypes.toQuery(queryObject.query),
				sort: indexerTypes.toSort(query.sort),
			})
		: new types.SearchRequestIndexed({
				query: indexerTypes.toQuery(queryObject.query),
				sort: indexerTypes.toSort(query.sort),
				replicate,
			});
};

const introduceEntries = async <
	T,
	I,
	D,
	R extends types.SearchRequest | types.SearchRequestIndexed,
>(
	queryRequest: R,
	responses: { response: types.AbstractSearchResult; from?: PublicSignKey }[],
	documentType: AbstractType<T>,
	indexedType: AbstractType<I>,
	sync: (request: R, response: types.Results<any>) => Promise<void>,
	options?: QueryDetailedOptions<T, D, any>,
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

const DEFAULT_INDEX_BY = "id";

export type CanSearch = (
	request: types.SearchRequest | types.CollectNextRequest,
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
	documentType: AbstractType<T>;
	dbType: AbstractType<types.IDocumentStore<T>>;
	log: SharedLog<Operation, D, any>;
	canRead?: CanRead<I>;
	canSearch?: CanSearch;
	replicate: (
		request: types.SearchRequest | types.SearchRequestIndexed,
		results: types.Results<
			types.ResultTypeFromRequest<
				types.SearchRequest | types.SearchRequestIndexed,
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
	compatibility: 6 | 7 | 8 | undefined;
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
	if ((value as WithContext<T>).__context) {
		return value as WithContext<T>;
	}

	let valueWithContext: WithContext<T> = value as any;
	valueWithContext.__context = context;
	return valueWithContext;
};

export const coerceWithIndexed = <T, I>(
	value: T | WithIndexed<T, I>,
	indexed: I,
): WithIndexed<T, I> => {
	if ((value as WithIndexed<T, I>).__indexed) {
		return value as WithIndexed<T, I>;
	}

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

	compatibility: 6 | 7 | 8 | undefined;

	// Transformation, indexer
	/* fields: IndexableFields<T, I>; */

	private _valueEncoding: Encoding<T>;

	private _sync: <V extends types.ResultValue<T> | types.ResultIndexedValue<I>>(
		request: types.SearchRequest | types.SearchRequestIndexed,
		results: types.Results<V>,
	) => Promise<void>;

	private _log: SharedLog<Operation, D, any>;

	private _resolverProgramCache?: Map<string | number | bigint, T>;
	private _resolverCache?: Cache<T>;
	private isProgramValued: boolean;
	private _maybeOpen: (value: T & Program) => Promise<T & Program>;
	private canSearch?: CanSearch;
	private canRead?: CanRead<I>;
	private _joinListener?: (e: { detail: PublicSignKey }) => Promise<void>;

	private _resultQueue: Map<
		string,
		{
			from: PublicSignKey;
			keptInIndex: number;
			timeout: ReturnType<typeof setTimeout>;
			queue: indexerTypes.IndexedResult<WithContext<I>>[];
			fromQuery: types.SearchRequest | types.SearchRequestIndexed;
		}
	>;

	constructor(properties?: {
		query?: RPC<types.AbstractSearchRequest, types.AbstractSearchResult>;
	}) {
		super();
		this._query = properties?.query || new RPC();
	}

	get valueEncoding() {
		return this._valueEncoding;
	}

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

		this.compatibility = properties.compatibility;
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
		this._sync = (request, results) => {
			let rq: types.SearchRequest | types.SearchRequestIndexed;
			let rs: types.Results<
				types.ResultTypeFromRequest<
					types.SearchRequest | types.SearchRequestIndexed,
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
						types.SearchRequest | types.SearchRequestIndexed,
						T,
						I
					>
				>;
			}
			return properties.replicate(rq, rs);
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

		this._resumableIterators = new ResumableIterators(this.index);
		this._maybeOpen = properties.maybeOpen;
		if (this.isProgramValued) {
			this._resolverProgramCache = new Map();
		}

		if (this.prefetch?.predictor) {
			const predictor = this.prefetch.predictor;
			this._joinListener = async (e: { detail: PublicSignKey }) => {
				// on join we emit predicted search results before peers query us (to save latency but for the price of errornous bandwidth usage)

				if ((await this._log.isReplicating()) === false) {
					return;
				}

				// TODO
				// it only makes sense for use to return predicted results if the peer is to choose us as a replicator
				// so we need to calculate the cover set from the peers perspective

				// create an iterator and send the peer the results
				let request = predictor.predictedQuery(e.detail);

				if (!request) {
					return;
				}
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
			this._query.events.addEventListener("join", this._joinListener);
		}

		await this._query.open({
			topic: sha256Base64Sync(
				concat([this._log.log.id, fromString("/document")]),
			),
			responseHandler: this.queryRPCResponseHandler.bind(this),
			responseType: types.AbstractSearchResult,
			queryType: types.AbstractSearchRequest,
		});
	}

	get prefetch() {
		return this._prefetch;
	}

	private async queryRPCResponseHandler(
		query: types.AbstractSearchRequest,
		ctx: { from?: PublicSignKey; message: DataMessage },
	) {
		if (!ctx.from) {
			logger.info("Receieved query without from");
			return;
		}
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
			return;
		}

		if (
			this.prefetch?.predictor &&
			(query instanceof types.SearchRequest ||
				query instanceof types.SearchRequestIndexed)
		) {
			const { ignore } = this.prefetch.predictor.onRequest(query, {
				from: ctx.from!,
			});

			if (ignore) {
				if (this.prefetch.strict) {
					return;
				}
			}
		}

		return this.handleSearchRequest(
			query as
				| types.SearchRequest
				| types.SearchRequestIndexed
				| types.CollectNextRequest,
			{
				from: ctx.from!,
			},
		);
	}
	private async handleSearchRequest(
		query:
			| types.SearchRequest
			| types.SearchRequestIndexed
			| types.CollectNextRequest,
		ctx: { from: PublicSignKey },
	) {
		if (
			this.canSearch &&
			(query instanceof types.SearchRequest ||
				query instanceof types.CollectNextRequest) &&
			!(await this.canSearch(
				query as types.SearchRequest | types.CollectNextRequest,
				ctx.from,
			))
		) {
			return new types.NoAccess();
		}

		if (query instanceof types.CloseIteratorRequest) {
			this.processCloseIteratorRequest(query, ctx.from);
		} else {
			const shouldIncludedIndexedResults =
				this.includeIndexed &&
				(query instanceof types.SearchRequest ||
					(query instanceof types.CollectNextRequest &&
						this._resultQueue.get(query.idString)?.fromQuery instanceof
							types.SearchRequest)); // we do this check here because this._resultQueue might be emptied when this.processQuery is called

			const results = await this.processQuery(
				query as
					| types.SearchRequest
					| types.SearchRequestIndexed
					| types.CollectNextRequest,
				ctx.from,
				false,
				{
					canRead: this.canRead,
				},
			);

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
			this._query.events.removeEventListener("join", this._joinListener);
			this.clearAllResultQueues();
			await this.index.stop?.();
		}
		return closed;
	}

	async drop(from?: Program): Promise<boolean> {
		const dropped = await super.drop(from);
		if (dropped) {
			this.clearAllResultQueues();
			await this.index.drop?.();
			await this.index.stop?.();
		}
		return dropped;
	}

	public async get<Options extends QueryOptions<T, D, true | undefined>>(
		key: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: Options,
	): Promise<WithIndexedContext<T, I>>;

	public async get<Options extends QueryOptions<T, D, false>>(
		key: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: Options,
	): Promise<WithContext<I>>;

	public async get<
		Options extends QueryOptions<T, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<Options>,
	>(key: indexerTypes.Ideable | indexerTypes.IdKey, options?: Options) {
		const result = (
			await this.getDetailed(
				key instanceof indexerTypes.IdKey ? key : indexerTypes.toId(key),
				options,
			)
		)?.[0]?.results[0];
		return result?.value;
	}

	public async getFromGid(gid: string) {
		const iterator = this.index.iterate({ query: { gid } });
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
		} else {
			this._resolverCache?.add(idString, value);
		}
		const valueToIndex = await this.transformer(value, context);
		const wrappedValueToIndex = new this.wrappedIndexedType(
			valueToIndex as I,
			context,
		);
		await this.index.put(wrappedValueToIndex);
		return { context, indexable: valueToIndex };
	}

	public del(key: indexerTypes.IdKey) {
		if (this.isProgramValued) {
			this._resolverProgramCache!.delete(key.primitive);
		} else {
			this._resolverCache?.del(key.primitive);
		}
		return this.index.del({
			query: [indexerTypes.getMatcher(this.indexBy, key.key)],
		});
	}

	public async getDetailed<
		Options extends QueryOptions<T, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<Options>,
		RT extends types.Result = Resolve extends true
			? types.ResultValue<WithIndexedContext<T, I>>
			: types.ResultIndexedValue<WithContext<I>>,
	>(
		key: indexerTypes.IdKey | indexerTypes.IdPrimitive,
		options?: QueryOptions<T, D, Resolve>,
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
		const resolve = coercedOptions?.resolve || coercedOptions?.resolve == null;
		let requestClazz = resolve
			? types.SearchRequest
			: types.SearchRequestIndexed;
		if (key instanceof Uint8Array) {
			results = await this.queryCommence(
				new requestClazz({
					query: [
						new indexerTypes.ByteMatchQuery({ key: this.indexBy, value: key }),
					],
				}),
				coercedOptions,
			);
		} else {
			const indexableKey = indexerTypes.toIdeable(key);

			if (
				typeof indexableKey === "number" ||
				typeof indexableKey === "bigint"
			) {
				results = await this.queryCommence(
					new requestClazz({
						query: [
							new indexerTypes.IntegerCompare({
								key: this.indexBy,
								compare: indexerTypes.Compare.Equal,
								value: indexableKey,
							}),
						],
					}),
					coercedOptions,
				);
			} else if (typeof indexableKey === "string") {
				results = await this.queryCommence(
					new requestClazz({
						query: [
							new indexerTypes.StringMatch({
								key: this.indexBy,
								value: indexableKey,
							}),
						],
					}),
					coercedOptions,
				);
			} else if (indexableKey instanceof Uint8Array) {
				results = await this.queryCommence(
					new requestClazz({
						query: [
							new indexerTypes.ByteMatchQuery({
								key: this.indexBy,
								value: indexableKey,
							}),
						],
					}),
					coercedOptions,
				);
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

		let fromQuery: types.SearchRequest | types.SearchRequestIndexed | undefined;
		if (
			query instanceof types.SearchRequest ||
			query instanceof types.SearchRequestIndexed
		) {
			fromQuery = query;
			indexedResult = await this._resumableIterators.iterateAndFetch(query);
		} else if (query instanceof types.CollectNextRequest) {
			fromQuery =
				prevQueued?.fromQuery ||
				this._resumableIterators.queues.get(query.idString)?.request;
			indexedResult =
				prevQueued?.keptInIndex === 0
					? []
					: await this._resumableIterators.next(query);
		} else {
			throw new Error("Unsupported");
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
			prevQueued = {
				from,
				queue: [],
				timeout: setTimeout(() => {
					this._resultQueue.delete(query.idString);
				}, 6e4),
				keptInIndex: kept,
				fromQuery: (fromQuery || query) as
					| types.SearchRequest
					| types.SearchRequestIndexed,
			};
			this._resultQueue.set(query.idString, prevQueued);
		}

		const filteredResults: types.Result[] = [];

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
			if (fromQuery instanceof types.SearchRequest) {
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
			} else if (fromQuery instanceof types.SearchRequestIndexed) {
				const context = result.value.__context;
				const head = await this._log.log.get(context.head);
				// assume remote peer will start to replicate (TODO is this ideal?)
				if (fromQuery.replicate) {
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

		if (!isLocal && results.kept === 0n) {
			this.clearResultsQueue(query);
		}

		return results;
	}

	private clearResultsQueue(
		query:
			| types.SearchRequest
			| types.SearchRequestIndexed
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
			this._resumableIterators.close({ idString: key });
		}
	}

	async processCloseIteratorRequest(
		query: types.CloseIteratorRequest,
		publicKey: PublicSignKey,
	): Promise<void> {
		const queueData = this._resultQueue.get(query.idString);
		if (queueData && !queueData.from.equals(publicKey)) {
			logger.info("Ignoring close iterator request from different peer");
			return;
		}
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
		R extends types.SearchRequest | types.SearchRequestIndexed,
		RT extends types.Result = types.ResultTypeFromRequest<R, T, I>,
	>(
		queryRequest: R,
		options?: QueryDetailedOptions<T, D, boolean | undefined>,
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
						eager: remote.eager,
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
						logger.warn("Did not reciveve responses from all shard");
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
		options?: SearchOptions<T, D, true>,
	): Promise<ValueTypeFromRequest<true, T, I>[]>;
	public search(
		queryRequest: QueryLike,
		options?: SearchOptions<T, D, false>,
	): Promise<ValueTypeFromRequest<false, T, I>[]>;

	/**
	 * Query and retrieve results
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	public async search<
		R extends types.SearchRequest | types.SearchRequestIndexed | QueryLike,
		O extends SearchOptions<T, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<O>,
	>(
		queryRequest: R,
		options?: SearchOptions<T, D, Resolve>,
	): Promise<ValueTypeFromRequest<Resolve, T, I>[]> {
		// Set fetch to search size, or max value (default to max u32 (4294967295))
		const coercedRequest: types.SearchRequest | types.SearchRequestIndexed =
			coerceQuery(queryRequest, options);
		coercedRequest.fetch = coercedRequest.fetch ?? 0xffffffff;

		// So that the iterator is pre-fetching the right amount of entries
		const iterator = this.iterate(coercedRequest, options);

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
		options?: QueryOptions<T, D, undefined>,
	): ResultsIterator<ValueTypeFromRequest<true, T, I>>;
	public iterate<Resolve extends boolean>(
		query?: QueryLike,
		options?: QueryOptions<T, D, Resolve>,
	): ResultsIterator<ValueTypeFromRequest<Resolve, T, I>>;

	/**
	 * Query and retrieve documents in a iterator
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	public iterate<
		R extends types.SearchRequest | types.SearchRequestIndexed | QueryLike,
		O extends SearchOptions<T, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<O>,
	>(
		queryRequest?: R,
		options?: QueryOptions<T, D, Resolve>,
	): ResultsIterator<ValueTypeFromRequest<Resolve, T, I>> {
		if (
			queryRequest instanceof types.SearchRequest &&
			options?.resolve === false
		) {
			throw new Error("Cannot use resolve=false with SearchRequest"); // TODO make this work
		}

		let queryRequestCoerced: types.SearchRequest | types.SearchRequestIndexed =
			coerceQuery(queryRequest ?? {}, options);

		let resolve = false;
		if (
			options?.remote &&
			typeof options.remote !== "boolean" &&
			options.remote.replicate &&
			options?.resolve !== false
		) {
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
			options.remote.replicate;
		if (
			replicate &&
			queryRequestCoerced instanceof types.SearchRequestIndexed
		) {
			queryRequestCoerced.replicate = true;
		}

		let fetchPromise: Promise<any> | undefined = undefined;
		const peerBufferMap: Map<
			string,
			{
				kept: number;
				buffer: BufferedResult<types.ResultTypeFromRequest<R, T, I> | I, I>[];
			}
		> = new Map();
		const visited = new Set<string | number | bigint>();

		let done = false;
		let first = false;

		// TODO handle join/leave while iterating
		const controller = new AbortController();

		let totalFetchedCounter = 0;
		let lastValueInOrder:
			| {
					indexed: WithContext<I>;
					value: types.ResultTypeFromRequest<R, T, I> | I;
					from: PublicSignKey;
					context: types.Context;
			  }
			| undefined = undefined;

		const peerBuffers = (): {
			indexed: WithContext<I>;
			value: types.ResultTypeFromRequest<R, T, I> | I;
			from: PublicSignKey;
			context: types.Context;
		}[] => {
			return [...peerBufferMap.values()].map((x) => x.buffer).flat();
		};

		let maybeSetDone: () => void;
		let unsetDone: () => void;
		let cleanup = () => {};

		if (typeof options?.remote === "object" && options.remote.joining) {
			let t0 = +new Date();
			let waitForTime =
				typeof options.remote.joining === "boolean"
					? 1e4
					: (options.remote.joining.waitFor ?? 1e4);
			let setDoneIfTimeout = false;
			maybeSetDone = () => {
				if (t0 + waitForTime < +new Date()) {
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
		} else {
			maybeSetDone = () => {
				cleanup();
				done = true;
			};
			unsetDone = () => {
				cleanup();
				done = false;
			};
			cleanup = () => {
				this.clearResultsQueue(queryRequestCoerced);
			};
		}

		const fetchFirst = async (
			n: number,
			fetchOptions?: { from?: string[] },
		): Promise<boolean> => {
			let hasMore = false;

			queryRequestCoerced.fetch = n;
			await this.queryCommence(queryRequestCoerced, {
				local: fetchOptions?.from != null ? false : options?.local,
				remote: {
					...(typeof options?.remote === "object" ? options.remote : {}),
					from: fetchOptions?.from,
				},
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

						if (results.kept === 0n && results.results.length === 0) {
							return;
						}

						if (results.kept > 0n) {
							hasMore = true;
						}
						const buffer: BufferedResult<
							types.ResultTypeFromRequest<R, T, I> | I,
							I
						>[] = [];

						for (const result of results.results) {
							if (result instanceof types.ResultValue) {
								const indexKey = indexerTypes.toId(
									this.indexByResolver(result.value),
								).primitive;
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
								const indexKey = indexerTypes.toId(
									this.indexByResolver(result.value),
								).primitive;

								if (visited.has(indexKey)) {
									continue;
								}
								visited.add(indexKey);
								buffer.push({
									value: result.value,
									context: result.context,
									from,
									indexed: coerceWithContext(
										result.indexed || result.value,
										result.context,
									),
								});
							}
						}

						peerBufferMap.set(from.hashcode(), {
							buffer,
							kept: Number(response.kept),
						});
					} else {
						throw new Error(
							"Unsupported result type: " + response?.constructor?.name,
						);
					}
				},
			});

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
					if (buffer.kept === 0) {
						if (peerBufferMap.get(peer)?.buffer.length === 0) {
							peerBufferMap.delete(peer); // No more results
							console.log("Removed peer from buffer: " + peer);
						}
						continue;
					}

					// TODO buffer more than deleted?
					// TODO batch to multiple 'to's

					const collectRequest = new types.CollectNextRequest({
						id: queryRequestCoerced.id,
						amount: n - buffer.buffer.length,
					});
					// Fetch locally?
					if (peer === this.node.identity.publicKey.hashcode()) {
						promises.push(
							this.processQuery(
								collectRequest,
								this.node.identity.publicKey,
								true,
							)
								.then(async (results) => {
									resultsLeft += Number(results.kept);

									if (results.results.length === 0) {
										if (peerBufferMap.get(peer)?.buffer.length === 0) {
											peerBufferMap.delete(peer); // No more results
										}
									} else {
										const peerBuffer = peerBufferMap.get(peer);
										if (!peerBuffer) {
											return;
										}
										peerBuffer.kept = Number(results.kept);

										for (const result of results.results) {
											if (
												visited.has(
													indexerTypes.toId(this.indexByResolver(result.value))
														.primitive,
												)
											) {
												continue;
											}
											visited.add(
												indexerTypes.toId(this.indexByResolver(result.value))
													.primitive,
											);
											let indexed: WithContext<I>;
											if (result instanceof types.ResultValue) {
												indexed = await this.resolveIndexed<R>(
													result,
													results.results as types.ResultTypeFromRequest<
														R,
														T,
														I
													>[],
												);
											} else {
												indexed = coerceWithContext(
													result.indexed || result.value,
													result.context,
												);
											}
											peerBuffer.buffer.push({
												value: result.value as types.ResultTypeFromRequest<
													R,
													T,
													I
												>,
												context: result.context,
												from: this.node.identity.publicKey,
												indexed: indexed,
											});
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
										? AbortSignal.any([options.signal, controller.signal])
										: controller.signal,
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
										options,
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
														if (peerBufferMap.get(peer)?.buffer.length === 0) {
															peerBufferMap.delete(peer); // No more results
															console.log("Removed peer from buffer: " + peer);
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
															const idPrimitive = indexerTypes.toId(
																this.indexByResolver(result.value),
															).primitive;
															if (visited.has(idPrimitive)) {
																continue;
															}
															visited.add(idPrimitive);

															let indexed: WithContext<I>;
															if (result instanceof types.ResultValue) {
																indexed = await this.resolveIndexed(
																	result,
																	response.response
																		.results as types.ResultTypeFromRequest<
																		R,
																		T,
																		I
																	>[],
																);
															} else {
																indexed = coerceWithContext(
																	result.value,
																	result.context,
																);
															}
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

			const pendingMoreResults = n < results.length;

			lastValueInOrder = results[0] || lastValueInOrder;

			const batch = results.splice(0, n);

			for (const result of batch) {
				const arr = peerBufferMap.get(result.from.hashcode());
				if (!arr) {
					logger.error("Unexpected empty result buffer");
					continue;
				}
				const idx = arr.buffer.findIndex((x) => x.value === result.value);
				if (idx >= 0) {
					arr.buffer.splice(idx, 1);
				}
			}

			const hasMore = !fetchedAll || pendingMoreResults;
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

			return dedup(coercedBatch, this.indexByResolver);
		};

		let close = async () => {
			cleanup();
			done = true;
			controller.abort(new AbortError("Iterator closed"));

			const closeRequest = new types.CloseIteratorRequest({
				id: queryRequestCoerced.id,
			});
			this.prefetch?.accumulator.clear(queryRequestCoerced);
			const promises: Promise<any>[] = [];

			for (const [peer, buffer] of peerBufferMap) {
				if (buffer.kept === 0) {
					peerBufferMap.delete(peer);
					continue;
				}
				// Fetch locally?
				if (peer === this.node.identity.publicKey.hashcode()) {
					promises.push(
						this.processCloseIteratorRequest(
							closeRequest,
							this.node.identity.publicKey,
						),
					);
				} else {
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

		let joinListener: ((e: { detail: PublicSignKey }) => void) | undefined;

		if (typeof options?.remote === "object" && options?.remote.joining) {
			let onMissedResults =
				typeof options?.remote?.joining === "object" &&
				typeof options?.remote.joining.onMissedResults === "function"
					? options.remote.joining.onMissedResults
					: undefined;

			joinListener = async (e: { detail: PublicSignKey }) => {
				if (totalFetchedCounter > 0) {
					// wait for the node to become a replicator, then so query
					await this._log
						.waitForReplicator(e.detail, {
							signal: options?.signal
								? AbortSignal.any([options.signal, controller.signal])
								: controller.signal,
						})
						.then(async () => {
							if (
								e.detail.equals(this.node.identity.publicKey) ||
								peerBufferMap.has(e.detail.hashcode())
							) {
								return;
							}

							if (done) {
								return;
							}
							await fetchFirst(totalFetchedCounter, {
								from: [e.detail.hashcode()],
							});

							if (onMissedResults) {
								const pending = peerBufferMap.get(e.detail.hashcode())?.buffer;

								if (pending && pending.length > 0) {
									if (lastValueInOrder) {
										const pendingWithLast = [
											...pending.flat(),
											lastValueInOrder,
										];
										const results = pendingWithLast.sort((a, b) =>
											indexerTypes.extractSortCompare(
												a.indexed,
												b.indexed,
												queryRequestCoerced.sort,
											),
										);

										let lateResults = results.findIndex(
											(x) => x === lastValueInOrder,
										);

										// consume pending
										if (lateResults > 0) {
											onMissedResults({ amount: lateResults });
										}
									} else {
										onMissedResults({ amount: pending.length });
									}
								}
							}
						})
						.catch(() => {
							/* TODO error handling */
						});
				}
			};
			this._query.events.addEventListener("join", joinListener);
			const cleanupDefault = cleanup;
			cleanup = () => {
				this._query.events.removeEventListener("join", joinListener!);
				return cleanupDefault();
			};
		}

		return {
			close,
			next,
			done: doneFn,
			pending: () => {
				let kept = 0;
				for (const [_, buffer] of peerBufferMap) {
					kept += buffer.kept;
				}
				return kept; // TODO this should be more accurate
			},
			all: async () => {
				let result: ValueTypeFromRequest<Resolve, T, I>[] = [];
				while (doneFn() !== true) {
					let batch = await next(100);
					result.push(...batch);
				}
				return result;
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

	public async waitFor(
		other:
			| PublicSignKey
			| PeerId
			| string
			| (PublicSignKey | string | PeerId)[],
		options?: { signal?: AbortSignal; timeout?: number },
	): Promise<void> {
		await super.waitFor(other, options);
		const ids = Array.isArray(other) ? other : [other];
		const expectedHashes = new Set(
			ids.map((x) =>
				typeof x === "string"
					? x
					: x instanceof PublicSignKey
						? x.hashcode()
						: getPublicKeyFromPeerId(x).hashcode(),
			),
		);

		for (const key of expectedHashes) {
			await waitFor(
				async () =>
					(await this._log.replicationIndex.count({ query: { hash: key } })) >
					0,
				options,
			);
		}
	}
}
