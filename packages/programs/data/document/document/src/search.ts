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
import { type ReplicationDomain, SharedLog } from "@peerbit/shared-log";
import { SilentDelivery } from "@peerbit/stream-interface";
import { AbortError, waitFor } from "@peerbit/time";
import { concat, fromString } from "uint8arrays";
import { copySerialization } from "./borsh.js";
import { MAX_BATCH_SIZE } from "./constants.js";
import { type Operation, isPutOperation } from "./operation.js";
import type { ExtractArgs } from "./program.js";
import { ResumableIterators } from "./resumable-iterator.js";

const logger = loggerFn({ module: "document-index" });

type BufferedResult<T, I extends Record<string, any>> = {
	value: T;
	indexed: I;
	context: types.Context;
	from: PublicSignKey;
};

export type RemoteQueryOptions<R, D> = RPCRequestAllOptions<R> & {
	replicate?: boolean;
	minAge?: number;
	throwOnMissing?: boolean;
	domain?: ExtractArgs<D>;
	eager?: boolean; // whether to query newly joined peers before they have matured
};
export type QueryOptions<R, D, Resolve extends boolean | undefined> = {
	remote?: boolean | RemoteQueryOptions<types.AbstractSearchResult, D>;
	local?: boolean;
	resolve?: Resolve;
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
};

type QueryLike /* <Resolve extends boolean | undefined>  */ = {
	query?: indexerTypes.Query[] | indexerTypes.QueryLike;
	sort?: indexerTypes.Sort[] | indexerTypes.Sort | indexerTypes.SortLike;
	/* 	resolve?: Resolve; */
};

/* type ExtractResolve<R> =
	R extends QueryLike<infer X>
	? X extends boolean // if X is a boolean (true or false)
	? X
	: true // else default to true
	: true; // if R isn't QueryLike at all, default to true */

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
	responses: RPCResponse<types.AbstractSearchResult>[],
	documentType: AbstractType<T>,
	indexedType: AbstractType<I>,
	sync: (
		request: types.SearchRequest | types.SearchRequestIndexed,
		response: types.Results<any>,
	) => Promise<void>,
	options?: QueryDetailedOptions<T, D, any>,
): Promise<RPCResponse<types.Results<types.ResultTypeFromRequest<R>>>[]> => {
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

const DEFAULT_INDEX_BY = "id";

/* 
if (!(await this.canRead(message.sender))) {
	throw new AccessError();
} */

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

type ValueTypeFromRequest<
	Resolve extends boolean | undefined,
	T,
	I,
> = Resolve extends false ? I : T;

@variant(0)
export class IndexableContext implements types.Context {
	@field({ type: "u64" })
	created: bigint;

	@field({ type: "u64" })
	modified: bigint;

	@field({ type: "string" })
	head: string;

	@field({ type: "string" })
	gid: string;

	@field({ type: "u32" })
	size: number; // bytes, we index this so we can query documents and understand their representation sizes

	constructor(properties: {
		created: bigint;
		modified: bigint;
		head: string;
		gid: string;
		size: number;
	}) {
		this.created = properties.created;
		this.modified = properties.modified;
		this.head = properties.head;
		this.gid = properties.gid;
		this.size = properties.size;
	}

	toContext(): types.Context {
		return new types.Context({
			created: this.created,
			modified: this.modified,
			head: this.head,
			gid: this.gid,
		});
	}
}

export type IDocumentWithContext<I> = {
	__context: IndexableContext;
} & I;

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
				types.SearchRequest | types.SearchRequestIndexed
			>
		>,
	) => Promise<void>;
	indexBy?: string | string[];
	transform?: TransformOptions<T, I>;
	compatibility: 6 | 7 | 8 | undefined;
};

type IndexableClass<I> = new (
	value: I,
	context: IndexableContext,
) => IDocumentWithContext<I>;

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
	index: indexerTypes.Index<IDocumentWithContext<I>>;
	private _resumableIterators: ResumableIterators<IDocumentWithContext<I>>;

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
	private _resolverCache: Cache<T>;
	private _isProgramValues: boolean;

	private _resultQueue: Map<
		string,
		{
			from: PublicSignKey;
			keptInIndex: number;
			timeout: ReturnType<typeof setTimeout>;
			queue: indexerTypes.IndexedResult<IDocumentWithContext<I>>[];
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

	async open(properties: OpenOptions<T, I, D>) {
		this._log = properties.log;

		this.documentType = properties.documentType;
		this.indexedTypeIsDocumentType =
			!properties.transform?.type ||
			properties.transform?.type === properties.documentType;

		this.compatibility = properties.compatibility;

		@variant(0)
		class IndexedClassWithContext {
			@field({ type: IndexableContext })
			__context: IndexableContext;

			constructor(value: I, context: IndexableContext) {
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
		) => IDocumentWithContext<I>;

		// if this.type is a class that extends Program we want to do special functionality
		this._isProgramValues = this.documentType instanceof Program;
		this.dbType = properties.dbType;
		this._resultQueue = new Map();
		this._sync = async (request, results) => {
			/*  			
			let allPromises: Promise<void> | undefined = undefined
				if (waitForValue) {
					let promises: Map<string, DeferredPromise<T>> = new Map();
	
				for (const result of results) {
					for (let i = 0; i < result.results.length; i++) {
						let promise = defer<T>(); 
						let r = result.results[i];
							promises.set(r.context.head, promise); 
						const head = result.results[0].context.head;
						let listeners = this.hashToValueListener.get(head);
						if (!listeners) {
							listeners = [];
							this.hashToValueListener.set(head, listeners);
						}
						listeners.push(async (value) => {
								promise.resolve(value); 
							result.results[i] = new types.ResultValue<T>({
								context: r.context,
								value,
								source: serialize(value),
								indexed: r.indexed,
							}) as any;
						});
						promise.promise.finally(() => {
							this.hashToValueListener.delete(head);
						});
					}
				}

				let timeout = setTimeout(() => {
					for (const promise of promises!) {
						promise[1].reject("Timed out resolving search result from value");
					}
				}, 1e4);

				allPromises = Promise.all([...promises.values()].map((x) => x.promise)).then(
					() => {
						clearTimeout(timeout);
					},
				);
			} */

			await properties.replicate(request, results);
			/* if (allPromises) {
				await allPromises;
			} */
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

		if (this._isProgramValues) {
			this._resolverProgramCache = new Map();
		}
		this._resolverCache = new Cache({ max: 10 }); // TODO choose limit better (adaptive)

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
				nested: {
					match: (obj: any): obj is types.IDocumentStore<any> =>
						obj instanceof this.dbType,
					iterate: async (obj: types.IDocumentStore<any>, query) =>
						obj.index.search(query),
				},
				/* maxBatchSize: MAX_BATCH_SIZE */
			})) || new HashmapIndex<IDocumentWithContext<I>>();

		this._resumableIterators = new ResumableIterators(this.index);

		await this._query.open({
			topic: sha256Base64Sync(
				concat([this._log.log.id, fromString("/document")]),
			),
			responseHandler: async (query, ctx) => {
				if (!ctx.from) {
					logger.info("Receieved query without from");
					return;
				}

				if (
					properties.canSearch &&
					(query instanceof types.SearchRequest ||
						query instanceof types.CollectNextRequest) &&
					!(await properties.canSearch(
						query as types.SearchRequest | types.CollectNextRequest,
						ctx.from,
					))
				) {
					return new types.NoAccess();
				}

				if (query instanceof types.CloseIteratorRequest) {
					this.processCloseIteratorRequest(query, ctx.from);
				} else {
					const results = await this.processQuery(
						query as
							| types.SearchRequest
							| types.SearchRequestIndexed
							| types.CollectNextRequest,
						ctx.from,
						false,
						{
							canRead: properties.canRead,
						},
					);

					return new types.Results({
						// Even if results might have length 0, respond, because then we now at least there are no matching results
						results: results.results,
						kept: results.kept,
					});
				}
			},
			responseType: types.AbstractSearchResult,
			queryType: types.AbstractSearchRequest,
		});
	}

	async getPending(cursorId: string): Promise<number | undefined> {
		const queue = this._resultQueue.get(cursorId);
		if (queue) {
			return queue.queue.length + queue.keptInIndex;
		}

		return this._resumableIterators.getPending(cursorId);
	}

	async close(from?: Program): Promise<boolean> {
		const closed = await super.close(from);
		if (closed) {
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
	): Promise<T>;

	public async get<Options extends QueryOptions<T, D, false>>(
		key: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: Options,
	): Promise<I>;

	public async get<
		Options extends QueryOptions<T, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<Options>,
	>(key: indexerTypes.Ideable | indexerTypes.IdKey, options?: Options) {
		return (
			await this.getDetailed(
				key instanceof indexerTypes.IdKey ? key : indexerTypes.toId(key),
				options,
			)
		)?.[0]?.results[0]?.value;
	}

	public async getFromGid(gid: string) {
		const iterator = this.index.iterate({ query: { gid } });
		const one = await iterator.next(1);
		await iterator.close();
		return one[0];
	}

	public async put(value: T, entry: Entry<Operation>, id: indexerTypes.IdKey) {
		const idString = id.primitive;
		if (this._isProgramValues) {
			this._resolverProgramCache!.set(idString, value);
		} else {
			this._resolverCache.add(idString, value);
		}

		const existing = await this.index.get(id);
		const context = new IndexableContext({
			created:
				existing?.value.__context.created ||
				entry.meta.clock.timestamp.wallTime,
			modified: entry.meta.clock.timestamp.wallTime,
			head: entry.hash,
			gid: entry.meta.gid,
			size: entry.payload.byteLength,
		});

		const valueToIndex = await this.transformer(value, context);
		const wrappedValueToIndex = new this.wrappedIndexedType(
			valueToIndex as I,
			context,
		);
		await this.index.put(wrappedValueToIndex);
	}

	public del(key: indexerTypes.IdKey) {
		if (this._isProgramValues) {
			this._resolverProgramCache!.delete(key.primitive);
		} else {
			this._resolverCache.del(key.primitive);
		}
		return this.index.del({
			query: [indexerTypes.getMatcher(this.indexBy, key.key)],
		});
	}

	public async getDetailed<
		Options extends QueryOptions<T, D, Resolve>,
		Resolve extends boolean | undefined = ExtractResolveFromOptions<Options>,
		RT extends types.Result = Resolve extends true
			? types.ResultValue<T>
			: types.ResultIndexedValue<I>,
	>(
		key: indexerTypes.IdKey | indexerTypes.IdPrimitive,
		options?: QueryOptions<T, D, Resolve>,
	): Promise<types.Results<RT>[] | undefined> {
		let results:
			| types.Results<types.ResultValue<T> | types.ResultIndexedValue<I>>[]
			| undefined;
		const resolve = options?.resolve || options?.resolve == null;
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
				options,
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
					options,
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
					options,
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
					options,
				);
			}
		}

		if (
			resolve &&
			requestClazz === types.SearchRequestIndexed &&
			!this.indexedTypeIsDocumentType &&
			results
		) {
			for (const set of results) {
				let coercedResult: types.ResultValue<T>[] = [];

				for (const value of set.results) {
					const resolved =
						value instanceof types.ResultIndexedValue
							? (
									await this.resolveDocument({
										indexed: value.value,
										head: value.context.head,
									})
								)?.value
							: value.value;
					if (resolved) {
						coercedResult.push(
							new types.ResultValue({
								context: value.context,
								value: resolved,
							}),
						);
					}
				}
				set.results = coercedResult;
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
			this._resolverCache.get(id) || this._resolverProgramCache?.get(id);
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
	): Promise<types.Results<types.ResultTypeFromRequest<R>>> {
		// We do special case for querying the id as we can do it faster than iterating

		let prevQueued = isLocal
			? undefined
			: this._resultQueue.get(query.idString);
		if (prevQueued && !from.equals(prevQueued.from)) {
			throw new Error("Different from in queued results");
		}

		let indexedResult:
			| indexerTypes.IndexedResults<IDocumentWithContext<I>>
			| undefined = undefined;

		let fromQuery: types.SearchRequest | types.SearchRequestIndexed | undefined;
		if (
			query instanceof types.SearchRequest ||
			query instanceof types.SearchRequestIndexed
		) {
			fromQuery = query;
			indexedResult = await this._resumableIterators.iterateAndFetch(query);
		} else if (query instanceof types.CollectNextRequest) {
			fromQuery = this._resumableIterators.queues.get(query.idString)?.request;
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
						context: result.value.__context.toContext(),
						value: value.value,
						source: serialize(value.value),
						indexed: indexedUnwrapped,
					}),
				);
			} else if (fromQuery instanceof types.SearchRequestIndexed) {
				const context = result.value.__context.toContext();
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
		RT extends types.Result = R extends types.SearchRequest
			? types.ResultValue<T>
			: types.ResultIndexedValue<I>,
	>(
		queryRequest: R,
		options?: QueryDetailedOptions<T, D, boolean | undefined>,
	): Promise<types.Results<RT>[]> {
		const local = typeof options?.local === "boolean" ? options?.local : true;
		let remote: RemoteQueryOptions<types.AbstractSearchResult, D> | undefined =
			undefined;
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
		const allResults: types.Results<types.ResultTypeFromRequest<R>>[] = [];

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

		let resolved: types.Results<types.ResultTypeFromRequest<R>>[] = [];
		if (remote) {
			const replicatorGroups = await this._log.getCover(
				remote.domain ?? (undefined as any),
				{
					roleAge: remote.minAge,
					eager: remote.eager,
				},
			);

			if (replicatorGroups) {
				const groupHashes: string[][] = replicatorGroups.map((x) => [x]);
				const responseHandler = async (
					results: RPCResponse<types.AbstractSearchResult>[],
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
				try {
					if (queryRequest instanceof types.CloseIteratorRequest) {
						// don't wait for responses
						await this._query.request(queryRequest, { mode: remote!.mode });
					} else {
						await queryAll(
							this._query,
							groupHashes,
							queryRequest,
							responseHandler,
							remote,
						);
					}
				} catch (error) {
					if (error instanceof MissingResponsesError) {
						logger.warn("Did not reciveve responses from all shard");
						if (remote?.throwOnMissing) {
							throw error;
						}
					} else {
						throw error;
					}
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

	public iterate(
		query: QueryLike,
		options?: QueryOptions<T, D, undefined>,
	): ResultsIterator<ValueTypeFromRequest<true, T, I>>;
	public iterate<Resolve extends boolean>(
		query: QueryLike,
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
		queryRequest: R,
		options?: QueryOptions<T, D, Resolve>,
	): ResultsIterator<ValueTypeFromRequest<Resolve, T, I>> {
		let queryRequestCoerced: types.SearchRequest | types.SearchRequestIndexed =
			coerceQuery(queryRequest, options);

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
				buffer: BufferedResult<
					types.ResultValue<T> | types.ResultIndexedValue<I>,
					I
				>[];
			}
		> = new Map();
		const visited = new Set<string | number | bigint>();

		let done = false;
		let first = false;

		// TODO handle join/leave while iterating
		const controller = new AbortController();

		const peerBuffers = (): {
			indexed: I;
			value: types.ResultValue<T> | types.ResultIndexedValue<I>;
			from: PublicSignKey;
			context: types.Context;
		}[] => {
			return [...peerBufferMap.values()].map((x) => x.buffer).flat();
		};

		const fetchFirst = async (n: number): Promise<boolean> => {
			done = true; // Assume we are donne
			queryRequestCoerced.fetch = n;
			await this.queryCommence(queryRequestCoerced, {
				...options,
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
							types.ResultTypeFromRequest<R>
						>;
						if (results.kept === 0n && results.results.length === 0) {
							return;
						}

						if (results.kept > 0n) {
							done = false; // we have more to do later!
						}
						const buffer: BufferedResult<types.ResultTypeFromRequest<R>, I>[] =
							[];

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
									value: result.value,
									context: result.context,
									from,
									indexed:
										(result.indexed as I) ||
										(await this.transformer(result.value, result.context)),
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
									indexed: result.indexed || result.value,
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

			if (done) {
				this.clearResultsQueue(queryRequestCoerced);
			}

			return done;
		};

		const fetchAtLeast = async (n: number) => {
			if (done && first) {
				return;
			}

			if (this.closed) {
				throw new ClosedError();
			}

			await fetchPromise;

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
											peerBuffer.buffer.push({
												value: result.value,
												context: result.context,
												from: this.node.identity.publicKey,
												indexed:
													result.indexed ||
													(await this.transformer(
														result.value,
														result.context,
													)),
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
						promises.push(
							this._query
								.request(collectRequest, {
									...options,
									signal: controller.signal,
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
														}
													} else {
														const peerBuffer = peerBufferMap.get(peer);
														if (!peerBuffer) {
															return;
														}
														peerBuffer.kept = Number(response.response.kept);
														for (const result of response.response.results) {
															const idPrimitive = indexerTypes.toId(
																this.indexByResolver(result.value),
															).primitive;
															if (visited.has(idPrimitive)) {
																continue;
															}
															visited.add(idPrimitive);
															peerBuffer.buffer.push({
																value: result.value,
																context: result.context,
																from: from!,
																indexed:
																	result instanceof types.ResultIndexedValue
																		? result.value
																		: await this.transformer(
																				result.value,
																				result.context,
																			),
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

			done = fetchedAll && !pendingMoreResults;
			let coercedBatch: ValueTypeFromRequest<Resolve, T, I>[];
			if (resolve) {
				coercedBatch = (
					await Promise.all(
						batch.map(async (x) =>
							x.value instanceof this.documentType
								? x.value
								: (
										await this.resolveDocument({
											head: x.context.head,
											indexed: x.indexed,
										})
									)?.value,
						),
					)
				).filter((x) => !!x) as ValueTypeFromRequest<Resolve, T, I>[];
			} else {
				coercedBatch = batch.map((x) => x.value) as ValueTypeFromRequest<
					Resolve,
					T,
					I
				>[];
			}

			return dedup(coercedBatch, this.indexByResolver);
		};

		const close = async () => {
			controller.abort(new AbortError("Iterator closed"));

			const closeRequest = new types.CloseIteratorRequest({
				id: queryRequestCoerced.id,
			});
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
		let doneFn = () => done;
		return {
			close,
			next,
			done: doneFn,
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
