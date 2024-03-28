import { AbstractType, field, serialize, variant } from "@dao-xyz/borsh";
import { BORSH_ENCODING, Encoding, Entry } from "@peerbit/log";
import { Program } from "@peerbit/program";
import * as types from "@peerbit/document-interface";
import {
	RPC,
	RPCResponse,
	queryAll,
	MissingResponsesError,
	RPCRequestAllOptions
} from "@peerbit/rpc";
import { logger as loggerFn } from "@peerbit/logger";
import { PublicSignKey, sha256Base64Sync } from "@peerbit/crypto";
import { SharedLog } from "@peerbit/shared-log";
import { concat, fromString } from "uint8arrays";
import { SilentDelivery } from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";
import { Cache } from "@peerbit/cache";
import { HashmapIndexEngine } from "@peerbit/document-index-simple";
import { MAX_BATCH_SIZE } from "./constants.js";

const logger = loggerFn({ module: "document-index" });

type BufferedResult<T> = {
	value: T;
	indexed: Record<string, any>;
	context: types.Context;
	from: PublicSignKey;
};

@variant(0)
export class Operation /* <T> */ {}

export const BORSH_ENCODING_OPERATION = BORSH_ENCODING(Operation);

/**
 * Put a complete document at a key
 */

@variant(0)
export class PutOperation extends Operation /* <T> */ {
	@field({ type: Uint8Array })
	data: Uint8Array;

	/* _value?: T; */

	constructor(props?: { data: Uint8Array /* value?: T */ }) {
		super();
		if (props) {
			this.data = props.data;
			/* this._value = props.value; */
		}
	}
}

/* @variant(1)
export class PutAllOperation<T> extends Operation<T> {
	@field({ type: vec(PutOperation) })
	docs: PutOperation<T>[];

	constructor(props?: { docs: PutOperation<T>[] }) {
		super();
		if (props) {
			this.docs = props.docs;
		}
	}
}
 */

/**
 * Delete a document at a key
 */
@variant(2)
export class DeleteOperation extends Operation {
	@field({ type: types.IdKey })
	key: types.IdKey;

	constructor(props: { key: types.IdKey }) {
		super();
		this.key = props.key;
	}
}

export type RemoteQueryOptions<R> = RPCRequestAllOptions<R> & {
	sync?: boolean;
	minAge?: number;
	throwOnMissing?: boolean;
};
export type QueryOptions<R> = {
	remote?: boolean | RemoteQueryOptions<types.AbstractSearchResult<R>>;
	local?: boolean;
};
export type SearchOptions<R> = { size?: number } & QueryOptions<R>;
export type IndexableFields<T> = (
	obj: T,
	context: types.Context
) => Record<string, any> | Promise<Record<string, any>>;

export type ResultsIterator<T> = {
	close: () => Promise<void>;
	next: (number: number) => Promise<T[]>;
	done: () => boolean;
};

type QueryDetailedOptions<T> = QueryOptions<T> & {
	onResponse?: (
		response: types.AbstractSearchResult<T>,
		from: PublicSignKey
	) => void | Promise<void>;
};

const introduceEntries = async <T>(
	responses: RPCResponse<types.AbstractSearchResult<T>>[],
	type: AbstractType<T>,
	sync: (result: types.Results<T>) => Promise<void>,
	options?: QueryDetailedOptions<T>
): Promise<RPCResponse<types.Results<T>>[]> => {
	const results: RPCResponse<types.Results<T>>[] = [];
	for (const response of responses) {
		if (!response.from) {
			logger.error("Missing from for response");
		}

		if (response.response instanceof types.Results) {
			response.response.results.forEach((r) => r.init(type));
			if (typeof options?.remote !== "boolean" && options?.remote?.sync) {
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
	dedupBy: (obj: any) => string | Uint8Array | number | bigint
) => {
	const unique: Set<types.IdPrimitive> = new Set();
	const dedup: T[] = [];
	for (const result of allResult) {
		const key = types.toIdeable(dedupBy(result));
		if (unique.has(key)) {
			continue;
		}
		unique.add(key);
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
	from: PublicSignKey
) => Promise<boolean> | boolean;

export type CanRead<T> = (
	result: T,
	from: PublicSignKey
) => Promise<boolean> | boolean;

export type OpenOptions<T> = {
	type: AbstractType<T>;
	dbType: AbstractType<types.IDocumentStore<T>>;
	log: SharedLog<Operation>;
	canRead?: CanRead<T>;
	canSearch?: CanSearch;
	engine?: types.IndexEngine;
	sync: (result: types.Results<T>) => Promise<void>;
	indexBy?: string | string[];
	fields: IndexableFields<T>;
};

@variant("documents_index")
export class DocumentIndex<T> extends Program<OpenOptions<T>> {
	@field({ type: RPC })
	_query: RPC<types.AbstractSearchRequest, types.AbstractSearchResult<T>>;

	engine: types.IndexEngine;

	type: AbstractType<T>;
	dbType: AbstractType<types.IDocumentStore<T>>;

	// Index key
	private indexBy: string | string[];
	private indexByArr: string[];
	private indexByResolver: (obj: any) => string | Uint8Array;

	// Transformation, indexer
	fields: IndexableFields<T>;

	private _valueEncoding: Encoding<T>;

	private _sync: (result: types.Results<T>) => Promise<void>;

	private _log: SharedLog<Operation>;

	private _resolverProgramCache?: Map<string | number | bigint, T>;
	private _resolverCache: Cache<T>;
	private _isProgramValues: boolean;
	constructor(properties?: {
		query?: RPC<types.AbstractSearchRequest, types.AbstractSearchResult<T>>;
	}) {
		super();
		this._query = properties?.query || new RPC();
	}

	get valueEncoding() {
		return this._valueEncoding;
	}

	async open(properties: OpenOptions<T>) {
		this._log = properties.log;
		this.type = properties.type;
		// if this.type is a class that extends Program we want to do special functionality
		this._isProgramValues = this.type instanceof Program;
		this.dbType = properties.dbType;
		this._sync = properties.sync;
		this.fields = properties.fields;
		this.indexBy = properties.indexBy || DEFAULT_INDEX_BY;
		this.indexByArr = Array.isArray(this.indexBy)
			? this.indexBy
			: [this.indexBy];
		this.indexByResolver =
			typeof this.indexBy === "string"
				? (obj) => obj[this.indexBy as string]
				: (obj: any) => types.extractFieldValue(obj, this.indexBy as string[]);

		this._valueEncoding = BORSH_ENCODING(this.type);

		if (this._isProgramValues) {
			this._resolverProgramCache = new Map();
		}
		this._resolverCache = new Cache({ max: 1000 }); // TODO choose limit better (adaptive)

		this.engine = properties.engine || new HashmapIndexEngine();

		await this.engine.init({
			indexBy: this.indexBy,
			nested: {
				match: (obj: any): obj is types.IDocumentStore<any> =>
					obj instanceof this.dbType,
				query: async (obj: types.IDocumentStore<any>, query) =>
					obj.index.search(query)
			},
			maxBatchSize: MAX_BATCH_SIZE
		});

		await this.engine.start?.();
		await this._query.open({
			topic: sha256Base64Sync(
				concat([this._log.log.id, fromString("/document")])
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
						ctx.from
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
						{
							canRead: properties.canRead
						}
					);

					return new types.Results({
						// Even if results might have length 0, respond, because then we now at least there are no matching results
						results: results.results,
						kept: results.kept
					});
				}
			},
			responseType: types.AbstractSearchResult,
			queryType: types.AbstractSearchRequest
		});
	}

	async close(from?: Program): Promise<boolean> {
		const closed = await super.close(from);
		if (closed) {
			await this.engine.stop?.();
		}
		return closed;
	}

	async drop(from?: Program): Promise<boolean> {
		const closed = await super.drop(from);
		if (closed) {
			await this.engine.stop?.();
		}
		return closed;
	}

	public async get(
		key: types.Ideable | types.IdKey,
		options?: QueryOptions<T>
	): Promise<T | undefined> {
		return (
			await this.getDetailed(
				key instanceof types.IdKey ? key : types.toId(key),
				options
			)
		)?.[0]?.results[0]?.value;
	}

	public async put(value: T, entry: Entry<Operation>, id: types.IdKey) {
		const idString = id.primitive;
		if (this._isProgramValues) {
			this._resolverProgramCache!.set(idString, value);
		} else {
			this._resolverCache.add(idString, value);
		}

		const context = new types.Context({
			created:
				(await this.engine.get(id))?.context.created ||
				entry.meta.clock.timestamp.wallTime,
			modified: entry.meta.clock.timestamp.wallTime,
			head: entry.hash,
			gid: entry.gid
		});

		const valueToIndex = await this.fields(value, context);
		this.engine.put({
			id,
			indexed: valueToIndex,
			context,
			size: entry.payload.data.byteLength
			/* reference:
				valueToIndex === value || value instanceof Program
					? { value }
					: undefined */
		});
	}

	public del(key: types.IdPrimitive) {
		const keyObject = types.toId(key);
		if (this._isProgramValues) {
			this._resolverProgramCache!.delete(key);
		} else {
			this._resolverCache.del(key);
		}
		return this.engine.del(keyObject);
	}

	public async getDetailed(
		key: types.IdKey | types.IdPrimitive,
		options?: QueryOptions<T>
	): Promise<types.Results<T>[] | undefined> {
		let results: types.Results<T>[] | undefined;
		if (key instanceof Uint8Array) {
			results = await this.queryDetailed(
				new types.SearchRequest({
					query: [
						new types.ByteMatchQuery({ key: this.indexByArr, value: key })
					]
				}),
				options
			);
		} else {
			const indexableKey = types.toIdeable(key);

			if (
				typeof indexableKey === "number" ||
				typeof indexableKey === "bigint"
			) {
				results = await this.queryDetailed(
					new types.SearchRequest({
						query: [
							new types.IntegerCompare({
								key: this.indexByArr,
								compare: types.Compare.Equal,
								value: indexableKey
							})
						]
					}),
					options
				);
			} else {
				results = await this.queryDetailed(
					new types.SearchRequest({
						query: [
							new types.StringMatch({
								key: this.indexByArr,
								value: indexableKey
							})
						]
					}),
					options
				);
			}
		}

		return results;
	}

	getSize(): Promise<number> | number {
		return this.engine.getSize();
	}

	private async resolveDocument(
		value: types.IndexedResult
	): Promise<{ value: T } | undefined> {
		const cached =
			this._resolverCache.get(value.id.primitive) ||
			this._resolverProgramCache?.get(value.id.primitive);
		if (cached != null) {
			return { value: cached };
		}

		if (value.indexed instanceof this.type) {
			return { value: value.indexed as T };
		}
		const head = await await this._log.log.get(value.context.head);
		if (!head) {
			return undefined; // we could end up here if we recently pruned the document and other peers never persisted the entry
			// TODO update changes in index before removing entries from log entry storage
		}
		const payloadValue = await head.getPayloadValue();
		if (payloadValue instanceof PutOperation) {
			return {
				value: this.valueEncoding.decoder(payloadValue.data)
				/* size: payloadValue.data.byteLength */
			};
		}

		throw new Error(
			"Unexpected value type when getting document: " +
				payloadValue?.constructor?.name || typeof payloadValue
		);
	}

	async processQuery(
		query: types.SearchRequest | types.CollectNextRequest,
		from: PublicSignKey,
		options?: {
			canRead?: CanRead<T>;
		}
	): Promise<types.Results<T>> {
		// We do special case for querying the id as we can do it faster than iterating

		let indexedResult: types.IndexedResults | undefined = undefined;
		if (query instanceof types.SearchRequest) {
			indexedResult = await this.engine.query(query, from);
		} else if (query instanceof types.CollectNextRequest) {
			indexedResult = await this.engine.next(query, from);
		} else {
			throw new Error("Unsupported");
		}
		const filteredResults: types.ResultWithSource<T>[] = [];
		for (const result of indexedResult.results) {
			const value = await this.resolveDocument(result);
			if (
				!value ||
				(options?.canRead && !(await options.canRead(value.value, from)))
			) {
				continue;
			}
			filteredResults.push(
				new types.ResultWithSource({
					context: result.context,
					value: value.value,
					source: serialize(value.value),
					indexed: result.indexed
				})
			);
		}
		const results: types.Results<T> = new types.Results({
			results: filteredResults,
			kept: BigInt(indexedResult.kept)
		});
		return results;
	}

	async processCloseIteratorRequest(
		query: types.CloseIteratorRequest,
		publicKey: PublicSignKey
	): Promise<void> {
		return this.engine.close(query, publicKey);
	}

	/**
	 * Query and retrieve results with most details
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	public async queryDetailed(
		queryRequest: types.SearchRequest,
		options?: QueryDetailedOptions<T>
	): Promise<types.Results<T>[]> {
		const local = typeof options?.local == "boolean" ? options?.local : true;
		let remote: RemoteQueryOptions<types.AbstractSearchResult<T>> | undefined =
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

		const promises: Promise<types.Results<T>[] | undefined>[] = [];
		if (!local && !remote) {
			throw new Error(
				"Expecting either 'options.remote' or 'options.local' to be true"
			);
		}
		const allResults: types.Results<T>[] = [];

		if (local) {
			const results = await this.processQuery(
				queryRequest,
				this.node.identity.publicKey
			);
			if (results.results.length > 0) {
				options?.onResponse &&
					(await options.onResponse(results, this.node.identity.publicKey));
				allResults.push(results);
			}
		}

		if (remote) {
			const replicatorGroups = await this._log.getReplicatorUnion(
				remote.minAge
			);
			if (replicatorGroups) {
				const groupHashes: string[][] = replicatorGroups.map((x) => [x]);
				const fn = async () => {
					const rs: types.Results<T>[] = [];
					const responseHandler = async (
						results: RPCResponse<types.AbstractSearchResult<T>>[]
					) => {
						for (const r of await introduceEntries(
							results,
							this.type,
							this._sync,
							options
						)) {
							rs.push(r.response);
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
								remote
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
					return rs;
				};
				promises.push(fn());
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
		const resolved = await Promise.all(promises);
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
		options?: SearchOptions<T>
	): Promise<T[]> {
		// Set fetch to search size, or max value (default to max u32 (4294967295))
		queryRequest.fetch = options?.size ?? 0xffffffff;

		// So that the iterator is pre-fetching the right amount of entries
		const iterator = this.iterate(queryRequest, options);

		// So that this call will not do any remote requests
		const allResults: T[] = [];
		while (
			iterator.done() === false &&
			queryRequest.fetch > allResults.length
		) {
			// We might need to pull .next multiple time due to data message size limitations
			for (const result of await iterator.next(
				queryRequest.fetch - allResults.length
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
		options?: QueryOptions<T>
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
							const indexKey = types.toIdeable(
								this.indexByResolver(result.value)
							);
							if (visited.has(indexKey)) {
								continue;
							}
							visited.add(indexKey);
							buffer.push({
								value: result.value,
								context: result.context,
								from: from,
								indexed:
									result.indexed ||
									(await this.fields(result.value, result.context))
							});
						}

						peerBufferMap.set(from.hashcode(), {
							buffer,
							kept: Number(response.kept)
						});
					} else {
						throw new Error(
							"Unsupported result type: " + response?.constructor?.name
						);
					}
				}
			});

			return done;
		};

		const fetchAtLeast = async (n: number) => {
			if (done && first) {
				return;
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
						amount: n - buffer.buffer.length
					});
					// Fetch locally?
					if (peer === this.node.identity.publicKey.hashcode()) {
						promises.push(
							this.processQuery(collectRequest, this.node.identity.publicKey)
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
													types.toIdeable(this.indexByResolver(result.value))
												)
											) {
												continue;
											}
											visited.add(
												types.toIdeable(this.indexByResolver(result.value))
											);
											peerBuffer.buffer.push({
												value: result.value,
												context: result.context,
												from: this.node.identity.publicKey,
												indexed:
													result.indexed ||
													(await this.fields(result.value, result.context))
											});
										}
									}
								})
								.catch((e) => {
									logger.error(
										"Failed to collect sorted results from self. " + e?.message
									);
									peerBufferMap.delete(peer);
								})
						);
					} else {
						// Fetch remotely
						promises.push(
							this._query
								.request(collectRequest, {
									...options,
									signal: controller.signal,
									priority: 1,
									mode: new SilentDelivery({ to: [peer], redundancy: 1 })
								})
								.then((response) =>
									introduceEntries(response, this.type, this._sync, options)
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
																types.toIdeable(
																	this.indexByResolver(result.value)
																)
															)
														) {
															continue;
														}
														visited.add(
															types.toIdeable(
																this.indexByResolver(result.value)
															)
														);
														peerBuffer.buffer.push({
															value: result.value,
															context: result.context,
															from: response.from!,
															indexed: this.fields(result.value, result.context)
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
													e?.message
											);
											peerBufferMap.delete(peer);
										})
								)
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
			const results = await types.resolvedSort(
				peerBuffers(),
				queryRequest.sort
			);

			const pendingMoreResults = n < results.length;

			const batch = results.splice(0, n);

			for (const result of batch) {
				const arr = peerBufferMap.get(result.from.hashcode());
				if (!arr) {
					logger.error("Unexpected empty result buffer");
					continue;
				}
				const idx = arr.buffer.findIndex((x) => x.value == result.value);
				if (idx >= 0) {
					arr.buffer.splice(idx, 1);
				}
			}

			done = fetchedAll && !pendingMoreResults;
			return dedup(
				batch.map((x) => x.value),
				this.indexByResolver
			);
		};

		const close = async () => {
			controller.abort(new AbortError("Iterator closed"));

			const closeRequest = new types.CloseIteratorRequest({
				id: queryRequest.id
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
							this.node.identity.publicKey
						)
					);
				} else {
					// Close remote
					promises.push(
						this._query.send(closeRequest, {
							...options,
							mode: new SilentDelivery({ to: [peer], redundancy: 1 })
						})
					);
				}
			}
			await Promise.all(promises);
		};

		return {
			close,
			next,
			done: () => done
		};
	}
}
