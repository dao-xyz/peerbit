import { type AbstractType, field, serialize, variant } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import {
	type MaybePromise,
	PublicSignKey,
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
import { AbortError } from "@peerbit/time";
import { concat, fromString } from "uint8arrays";
import { copySerialization } from "./borsh.js";
import { MAX_BATCH_SIZE } from "./constants.js";
import { type Operation, isPutOperation } from "./operation.js";
import type { ExtractArgs } from "./program.js";
import { ResumableIterators } from "./resumable-iterator.js";

const logger = loggerFn({ module: "document-index" });

type BufferedResult<T> = {
	value: T;
	indexed: Record<string, any>;
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
export type QueryOptions<R, D> = {
	remote?: boolean | RemoteQueryOptions<types.AbstractSearchResult<R>, D>;
	local?: boolean;
};
export type SearchOptions<R, D> = QueryOptions<R, D>;

type Transformer<T, I> = (obj: T, context: types.Context) => MaybePromise<I>;

export type ResultsIterator<T> = {
	close: () => Promise<void>;
	next: (number: number) => Promise<T[]>;
	done: () => boolean;
};

type QueryDetailedOptions<T, D> = QueryOptions<T, D> & {
	onResponse?: (
		response: types.AbstractSearchResult<T>,
		from: PublicSignKey,
	) => void | Promise<void>;
};

const introduceEntries = async <T, D>(
	responses: RPCResponse<types.AbstractSearchResult<T>>[],
	type: AbstractType<T>,
	sync: (result: types.Results<T>) => Promise<void>,
	options?: QueryDetailedOptions<T, D>,
): Promise<RPCResponse<types.Results<T>>[]> => {
	const results: RPCResponse<types.Results<T>>[] = [];
	for (const response of responses) {
		if (!response.from) {
			logger.error("Missing from for response");
		}

		if (response.response instanceof types.Results) {
			response.response.results.forEach((r) => r.init(type));
			if (typeof options?.remote !== "boolean" && options?.remote?.replicate) {
				await sync(response.response);
			}
			options?.onResponse &&
				(await options.onResponse(response.response, response.from!)); // TODO fix types
			results.push(response as RPCResponse<types.Results<T>>);
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

export type OpenOptions<T, I, D extends ReplicationDomain<any, Operation>> = {
	documentType: AbstractType<T>;
	dbType: AbstractType<types.IDocumentStore<T>>;
	log: SharedLog<Operation, D>;
	canRead?: CanRead<T>;
	canSearch?: CanSearch;
	sync: (result: types.Results<T>) => Promise<void>;
	indexBy?: string | string[];
	transform?: TransformOptions<T, I>;
};

type IndexableClass<I> = new (
	value: I,
	context: IndexableContext,
) => IDocumentWithContext<I>;

@variant("documents_index")
export class DocumentIndex<
	T,
	I extends Record<string, any>,
	D extends ReplicationDomain<any, Operation>,
> extends Program<OpenOptions<T, I, D>> {
	@field({ type: RPC })
	_query: RPC<types.AbstractSearchRequest, types.AbstractSearchResult<T>>;

	// Original document representation
	documentType: AbstractType<T>;

	// transform options
	transformer: Transformer<T, I>;

	// The indexed document wrapped in a context
	wrappedIndexedType: IndexableClass<I>;

	// The database type, for recursive indexing
	dbType: AbstractType<types.IDocumentStore<T>>;
	indexedTypeIsDocumentType: boolean;

	// Index key
	private indexBy: string[];
	private indexByResolver: (obj: any) => string | Uint8Array;
	index: indexerTypes.Index<IDocumentWithContext<I>>;
	private _resumableIterators: ResumableIterators<IDocumentWithContext<I>>;

	// Transformation, indexer
	/* fields: IndexableFields<T, I>; */

	private _valueEncoding: Encoding<T>;

	private _sync: (result: types.Results<T>) => Promise<void>;

	private _log: SharedLog<Operation, D>;

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
		query?: RPC<types.AbstractSearchRequest, types.AbstractSearchResult<T>>;
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
		copySerialization(
			(properties.transform?.type || properties.documentType)!,
			IndexedClassWithContext,
		);

		this.wrappedIndexedType = IndexedClassWithContext as new (
			value: I,
			context: types.Context,
		) => IDocumentWithContext<I>;

		// if this.type is a class that extends Program we want to do special functionality
		this._isProgramValues = this.documentType instanceof Program;
		this.dbType = properties.dbType;
		this._resultQueue = new Map();
		this._sync = properties.sync;

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
							| types.SearchRequest
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
			await this.index.stop?.();
		}
		return closed;
	}

	async drop(from?: Program): Promise<boolean> {
		const dropped = await super.drop(from);
		if (dropped) {
			await this.index.drop?.();
			await this.index.stop?.();
		}
		return dropped;
	}

	public async get(
		key: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: QueryOptions<T, D>,
	): Promise<T | undefined> {
		return (
			await this.getDetailed(
				key instanceof indexerTypes.IdKey ? key : indexerTypes.toId(key),
				options,
			)
		)?.[0]?.results[0]?.value;
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

	public async getDetailed(
		key: indexerTypes.IdKey | indexerTypes.IdPrimitive,
		options?: QueryOptions<T, D>,
	): Promise<types.Results<T>[] | undefined> {
		let results: types.Results<T>[] | undefined;
		if (key instanceof Uint8Array) {
			results = await this.queryDetailed(
				new types.SearchRequest({
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
				results = await this.queryDetailed(
					new types.SearchRequest({
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
				results = await this.queryDetailed(
					new types.SearchRequest({
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
				results = await this.queryDetailed(
					new types.SearchRequest({
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

		return results;
	}

	getSize(): Promise<number> | number {
		return this.index.getSize();
	}

	private async resolveDocument(
		value: indexerTypes.IndexedResult<IDocumentWithContext<I>>,
	): Promise<{ value: T } | undefined> {
		const cached =
			this._resolverCache.get(value.id.primitive) ||
			this._resolverProgramCache?.get(value.id.primitive);
		if (cached != null) {
			return { value: cached };
		}

		if (this.indexedTypeIsDocumentType) {
			// cast value to T, i.e. convert the class but keep all properties except the __context
			const obj = Object.assign(
				Object.create(this.documentType.prototype),
				value.value,
			);
			delete obj.__context;
			return { value: obj as T };
		}

		const head = await this._log.log.get(value.value.__context.head);
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

	async processQuery(
		query: types.SearchRequest | types.CollectNextRequest,
		from: PublicSignKey,
		isLocal: boolean,
		options?: {
			canRead?: CanRead<T>;
		},
	): Promise<types.Results<T>> {
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
		if (query instanceof types.SearchRequest) {
			indexedResult = await this._resumableIterators.iterateAndFetch(query);
		} else if (query instanceof types.CollectNextRequest) {
			indexedResult =
				prevQueued?.keptInIndex === 0
					? []
					: await this._resumableIterators.next(query);
		} else {
			throw new Error("Unsupported");
		}
		const filteredResults: types.ResultWithSource<T>[] = [];
		let resultSize = 0;

		let toIterate = prevQueued
			? [...prevQueued.queue, ...indexedResult]
			: indexedResult;

		if (prevQueued) {
			this._resultQueue.delete(query.idString);
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

		for (const result of toIterate) {
			if (!isLocal) {
				resultSize += result.value.__context.size;
				if (resultSize > MAX_BATCH_SIZE) {
					prevQueued!.queue.push(result);
					continue;
				}
			}

			const value = await this.resolveDocument(result);
			if (
				!value ||
				(options?.canRead && !(await options.canRead(value.value, from)))
			) {
				continue;
			}

			filteredResults.push(
				new types.ResultWithSource({
					context: result.value.__context.toContext(),
					value: value.value,
					source: serialize(value.value),
					indexed: result.value,
				}),
			);
		}
		const results: types.Results<T> = new types.Results({
			results: filteredResults,
			kept: BigInt(kept + (prevQueued?.queue.length || 0)),
		});

		if (!isLocal && results.kept === 0n) {
			this.clearResultsQueue(query);
		}

		return results;
	}

	clearResultsQueue(
		query:
			| types.SearchRequest
			| types.CollectNextRequest
			| types.CloseIteratorRequest,
	) {
		const queue = this._resultQueue.get(query.idString);
		if (queue) {
			clearTimeout(queue.timeout);
			this._resultQueue.delete(query.idString);
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
	public async queryDetailed(
		queryRequest: types.SearchRequest,
		options?: QueryDetailedOptions<T, D>,
	): Promise<types.Results<T>[]> {
		const local = typeof options?.local === "boolean" ? options?.local : true;
		let remote:
			| RemoteQueryOptions<types.AbstractSearchResult<T>, D>
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
		const allResults: types.Results<T>[] = [];

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

		let resolved: types.Results<T>[] = [];
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
					results: RPCResponse<types.AbstractSearchResult<T>>[],
				) => {
					for (const r of await introduceEntries(
						results,
						this.documentType,
						this._sync,
						options,
					)) {
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
		return allResults;
	}

	/**
	 * Query and retrieve results
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	public async search(
		queryRequest: types.SearchRequest,
		options?: SearchOptions<T, D>,
	): Promise<T[]> {
		// Set fetch to search size, or max value (default to max u32 (4294967295))
		queryRequest.fetch = queryRequest.fetch ?? 0xffffffff;

		// So that the iterator is pre-fetching the right amount of entries
		const iterator = this.iterate(queryRequest, options);

		// So that this call will not do any remote requests
		const allResults: T[] = [];
		while (iterator.done() !== true && queryRequest.fetch > allResults.length) {
			// We might need to pull .next multiple time due to data message size limitations
			for (const result of await iterator.next(
				queryRequest.fetch - allResults.length,
			)) {
				allResults.push(result);
			}
		}

		await iterator.close();

		//s Deduplicate and return values directly
		return dedup(allResults, this.indexByResolver);
	}

	/**
	 * Query and retrieve documents in a iterator
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	public iterate(
		queryRequest: types.SearchRequest,
		options?: QueryOptions<T, D>,
	): ResultsIterator<T> {
		let fetchPromise: Promise<any> | undefined = undefined;
		const peerBufferMap: Map<
			string,
			{
				kept: number;
				buffer: BufferedResult<T>[];
			}
		> = new Map();
		const visited = new Set<string | number | bigint>();

		let done = false;
		let first = false;

		// TODO handle join/leave while iterating
		const controller = new AbortController();

		const peerBuffers = (): {
			indexed: Record<string, any>;
			value: T;
			from: PublicSignKey;
			context: types.Context;
		}[] => {
			return [...peerBufferMap.values()].map((x) => x.buffer).flat();
		};

		const fetchFirst = async (n: number): Promise<boolean> => {
			done = true; // Assume we are donne
			queryRequest.fetch = n;
			await this.queryDetailed(queryRequest, {
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
						const results = response as types.Results<T>;
						if (results.kept === 0n && results.results.length === 0) {
							return;
						}

						if (results.kept > 0n) {
							done = false; // we have more to do later!
						}
						const buffer: BufferedResult<T>[] = [];

						for (const result of results.results) {
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
									result.indexed ||
									(await this.transformer(result.value, result.context)),
							});
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
				this.clearResultsQueue(queryRequest);
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
						id: queryRequest.id,
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
										response,
										this.documentType,
										this._sync,
										options,
									)
										.then((responses) => {
											responses.map((response) => {
												resultsLeft += Number(response.response.kept);
												if (!response.from) {
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
														if (
															visited.has(
																indexerTypes.toId(
																	this.indexByResolver(result.value),
																).primitive,
															)
														) {
															continue;
														}
														visited.add(
															indexerTypes.toId(
																this.indexByResolver(result.value),
															).primitive,
														);
														peerBuffer.buffer.push({
															value: result.value,
															context: result.context,
															from: response.from!,
															indexed: this.transformer(
																result.value,
																result.context,
															),
														});
													}
												}
											});
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
					queryRequest.sort,
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

			return dedup(
				batch.map((x) => x.value),
				this.indexByResolver,
			);
		};

		const close = async () => {
			controller.abort(new AbortError("Iterator closed"));

			const closeRequest = new types.CloseIteratorRequest({
				id: queryRequest.id,
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

		return {
			close,
			next,
			done: () => done,
		};
	}
}
