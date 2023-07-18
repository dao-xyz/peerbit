import { AbstractType, field, serialize, variant } from "@dao-xyz/borsh";
import { asString, Keyable } from "./utils.js";
import { BORSH_ENCODING, Encoding } from "@peerbit/log";
import { equals } from "@peerbit/uint8arrays";
import { Program } from "@peerbit/program";
import {
	IntegerCompare,
	ByteMatchQuery,
	StringMatch,
	Query,
	ResultWithSource,
	StateFieldQuery,
	compare,
	Context,
	MissingField,
	StringMatchMethod,
	LogicalQuery,
	And,
	Or,
	BoolQuery,
	Sort,
	CollectNextRequest,
	AbstractSearchRequest,
	SearchRequest,
	SortDirection,
	CloseIteratorRequest,
} from "./query.js";
import {
	CanRead,
	RPC,
	RPCOptions,
	RPCResponse,
	queryAll,
	MissingResponsesError,
} from "@peerbit/rpc";
import { Results } from "./query.js";
import { logger as loggerFn } from "@peerbit/logger";
import { Cache } from "@peerbit/cache";
import { PublicSignKey } from "@peerbit/crypto";
import { SharedLog } from "@peerbit/shared-log";

const logger = loggerFn({ module: "document-index" });

const stringArraysEquals = (a: string[] | string, b: string[] | string) => {
	if (a.length !== b.length) {
		return false;
	}
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) {
			return false;
		}
	}
	return true;
};

@variant(0)
export class Operation<T> {}

export const BORSH_ENCODING_OPERATION = BORSH_ENCODING(Operation);

@variant(0)
export class PutOperation<T> extends Operation<T> {
	@field({ type: "string" })
	key: string;

	@field({ type: Uint8Array })
	data: Uint8Array;

	_value?: T;

	constructor(props?: { key: string; data: Uint8Array; value?: T }) {
		super();
		if (props) {
			this.key = props.key;
			this.data = props.data;
			this._value = props.value;
		}
	}

	get value(): T | undefined {
		if (!this._value) {
			throw new Error("Value not decoded, invoke getValue(...) once");
		}
		return this._value;
	}

	getValue(encoding: Encoding<T>): T {
		if (this._value) {
			return this._value;
		}
		this._value = encoding.decoder(this.data);
		return this._value;
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
@variant(2)
export class DeleteOperation extends Operation<any> {
	@field({ type: "string" })
	key: string;

	constructor(props?: { key: string }) {
		super();
		if (props) {
			this.key = props.key;
		}
	}
}

export interface IndexedValue<T> {
	key: string;
	value: Record<string, any> | T; // decrypted, decoded
	context: Context;
	reference?: T;
}

export type RemoteQueryOptions<R> = RPCOptions<R> & {
	sync?: boolean;
	minAge?: number;
};
export type QueryOptions<R> = {
	remote?: boolean | RemoteQueryOptions<Results<R>>;
	local?: boolean;
};
export type SearchOptions<R> = { size?: number } & QueryOptions<R>;
export type Indexable<T> = (
	obj: T,
	context: Context
) => Record<string, any> | Promise<Record<string, any>>;

const extractFieldValue = <T>(doc: any, path: string[]): T => {
	for (let i = 0; i < path.length; i++) {
		doc = doc[path[i]];
	}
	return doc;
};

export type ResultsIterator<T> = {
	close: () => Promise<void>;
	next: (number: number) => Promise<T[]>;
	done: () => boolean;
};

const sortCompare = (av: any, bv: any) => {
	if (typeof av === "string" && typeof bv === "string") {
		return av.localeCompare(bv);
	}
	if (av < bv) {
		return -1;
	} else if (av > bv) {
		return 1;
	}
	return 0;
};
const extractSortCompare = (
	a: Record<string, any>,
	b: Record<string, any>,
	sorts: Sort[]
) => {
	for (const sort of sorts) {
		const av = extractFieldValue(a, sort.key);
		const bv = extractFieldValue(b, sort.key);
		const cmp = sortCompare(av, bv);
		if (cmp != 0) {
			if (sort.direction === SortDirection.ASC) {
				return cmp;
			} else {
				return -cmp;
			}
		}
	}
	return 0;
};

const resolvedSort = async <T, Q extends { value: T; context: Context }>(
	arr: Q[],
	index: Indexable<T>,
	sorts: Sort[]
) => {
	await Promise.all(
		arr.map(
			async (result) =>
				(result[SORT_TMP_KEY] = await index(result.value, result.context))
		)
	);
	arr.sort((a, b) =>
		extractSortCompare(a[SORT_TMP_KEY], b[SORT_TMP_KEY], sorts)
	);
	return arr;
};
/* 
const sortValueWithContext = async<T>(arr: {
	value: T;
	context: Context;
}[], index: Indexable<T>) => {

	
}
 */

const SORT_TMP_KEY = "__sort_ref";

type QueryDetailedOptions<T> = QueryOptions<T> & {
	onResponse?: (response: Results<T>, from?: PublicSignKey) => void;
	excludePeerAgeThreshold?: number;
};
const introduceEntries = async <T>(
	responses: RPCResponse<Results<T>>[],
	type: AbstractType<T>,
	sync: (result: Results<T>) => Promise<void>,
	options?: QueryDetailedOptions<T>
): Promise<RPCResponse<Results<T>>[]> => {
	return Promise.all(
		responses.map(async (x) => {
			x.response.results.forEach((r) => r.init(type));
			if (typeof options?.remote !== "boolean" && options?.remote?.sync) {
				await sync(x.response);
			}
			if (!x.from) {
				logger.error("Missing from for response");
			}
			options?.onResponse && options.onResponse(x.response, x.from!);
			return x;
		})
	);
};

const dedup = <T>(
	allResult: T[],
	dedupBy: (obj: any) => string | Uint8Array
) => {
	const unique: Set<Keyable> = new Set();
	const dedup: T[] = [];
	for (const result of allResult) {
		const key = asString(dedupBy(result));
		if (unique.has(key)) {
			continue;
		}
		unique.add(key);
		dedup.push(result);
	}
	return dedup;
};

const DEFAULT_INDEX_BY = "id";

const DEFAULT_REPLICATOR_MIN_AGE = 10 * 1000; // how long, until we consider a peer unreliable

export type OpenOptions<T> = {
	type: AbstractType<T>;
	log: SharedLog<Operation<T>>;
	canRead: CanRead;
	fields: Indexable<T>;
	sync: (result: Results<T>) => Promise<void>;
	indexBy?: string | string[];
};

@variant("documents_index")
export class DocumentIndex<T> extends Program<OpenOptions<T>> {
	@field({ type: RPC })
	_query: RPC<AbstractSearchRequest, Results<T>>;

	type: AbstractType<T>;

	// Index key
	private _indexBy: string | string[];
	private _indexByArr: string[];

	// Resolve doc value by index key
	indexByResolver: (obj: any) => string | Uint8Array;

	// Indexed (transforms an docuemnt into an obj with fields that ought to be indexed)
	private _toIndex: Indexable<T>;

	private _valueEncoding: Encoding<T>;

	private _sync: (result: Results<T>) => Promise<void>;
	private _index: Map<string, IndexedValue<T>>;

	private _resultsCollectQueue: Cache<{ value: T; context: Context }[]>;

	private _log: SharedLog<Operation<T>>;

	constructor(properties?: { query?: RPC<SearchRequest, Results<T>> }) {
		super();
		this._query = properties?.query || new RPC();
	}

	get index(): Map<string, IndexedValue<T>> {
		return this._index;
	}

	get valueEncoding() {
		return this._valueEncoding;
	}

	get toIndex(): Indexable<T> {
		return this._toIndex;
	}

	async open(properties: OpenOptions<T>) {
		this._index = new Map();
		this._log = properties.log;
		this.type = properties.type;
		this._sync = properties.sync;
		this._toIndex = properties.fields;
		this._indexBy = properties.indexBy || DEFAULT_INDEX_BY;
		this._indexByArr = Array.isArray(this._indexBy)
			? this._indexBy
			: [this._indexBy];

		this.indexByResolver =
			typeof this._indexBy === "string"
				? (obj) => obj[this._indexBy as string]
				: (obj: any) => extractFieldValue(obj, this._indexBy as string[]);
		this._valueEncoding = BORSH_ENCODING(this.type);
		this._resultsCollectQueue = new Cache({ max: 10000 }); // TODO choose limit better

		await this._query.open({
			topic: this._log.log.idString + "/document",
			canRead: properties.canRead,
			responseHandler: async (query) => {
				if (query instanceof CloseIteratorRequest) {
					this.processCloseIteratorRequest(query);
				} else {
					const results = await this.processFetchRequest(
						query as SearchRequest | SearchRequest | CollectNextRequest
					);
					return new Results({
						// Even if results might have length 0, respond, because then we now at least there are no matching results
						results: results.results.map(
							(r) =>
								new ResultWithSource({
									source: serialize(r.value),
									context: r.context,
								})
						),
						kept: BigInt(results.kept),
					});
				}
			},
			responseType: Results,
			queryType: AbstractSearchRequest,
		});
	}

	public async get(
		key: Keyable,
		options?: QueryOptions<T>
	): Promise<T | undefined> {
		return (await this.getDetailed(key, options))?.[0]?.results[0]?.value;
	}

	public async getDetailed(
		key: Keyable,
		options?: QueryOptions<T>
	): Promise<Results<T>[] | undefined> {
		let results: Results<T>[] | undefined;
		if (key instanceof Uint8Array) {
			results = await this.queryDetailed(
				new SearchRequest({
					query: [new ByteMatchQuery({ key: this._indexByArr, value: key })],
				}),
				options
			);
		} else {
			const stringValue = asString(key);
			results = await this.queryDetailed(
				new SearchRequest({
					query: [
						new StringMatch({
							key: this._indexByArr,
							value: stringValue,
						}),
					],
				}),
				options
			);
		}

		return results;
	}

	get size(): number {
		return this._index.size;
	}

	async getDocument(value: {
		reference?: T;
		context: { head: string };
	}): Promise<T> {
		if (value.reference) {
			return value.reference;
		}

		const payloadValue = await (await this._log.log.get(
			value.context.head
		))!.getPayloadValue();
		if (payloadValue instanceof PutOperation) {
			return payloadValue.getValue(this.valueEncoding);
		}
		throw new Error("Unexpected");
	}

	async _queryDocuments(
		filter: (doc: IndexedValue<T>) => boolean
	): Promise<{ context: Context; value: T }[]> {
		// Whether we return the full operation data or just the db value
		const results: { context: Context; value: T }[] = [];
		for (const value of this._index.values()) {
			if (filter(value)) {
				results.push({
					context: value.context,
					value: await this.getDocument(value),
				});
			}
		}
		return results;
	}

	async processFetchRequest(
		query: SearchRequest | CollectNextRequest
	): Promise<{ results: { context: Context; value: T }[]; kept: number }> {
		// We do special case for querying the id as we can do it faster than iterating
		if (query instanceof SearchRequest) {
			if (
				query.query.length === 1 &&
				(query.query[0] instanceof ByteMatchQuery ||
					query.query[0] instanceof StringMatch) &&
				stringArraysEquals(query.query[0].key, this._indexByArr)
			) {
				const firstQuery = query.query[0];
				if (firstQuery instanceof ByteMatchQuery) {
					const doc = this._index.get(asString(firstQuery.value)); // TODO could there be a issue with types here?
					return doc
						? {
								results: [
									{
										value: await this.getDocument(doc),
										context: doc.context,
									},
								],
								kept: 0,
						  }
						: { results: [], kept: 0 };
				} else if (
					firstQuery instanceof StringMatch &&
					firstQuery.method === StringMatchMethod.exact &&
					firstQuery.caseInsensitive === false
				) {
					const doc = this._index.get(firstQuery.value); // TODO could there be a issue with types here?
					return doc
						? {
								results: [
									{
										value: await this.getDocument(doc),
										context: doc.context,
									},
								],
								kept: 0,
						  }
						: { results: [], kept: 0 };
				}
			}

			const results = await this._queryDocuments((doc) => {
				for (const f of query.query) {
					if (!this.handleQueryObject(f, doc)) {
						return false;
					}
				}
				return true;
			});

			// Sort
			await resolvedSort(results, this._toIndex, query.sort);

			const batch = results.splice(0, query.fetch);
			if (results.length > 0) {
				this._resultsCollectQueue.add(query.idString, results); // cache resulst not returned
			}

			return { results: batch, kept: results.length }; // Only return 1 result since we are doing distributed sort, TODO buffer more initially
		} else if (query instanceof CollectNextRequest) {
			const results = this._resultsCollectQueue.get(query.idString);
			if (!results) {
				return {
					results: [],
					kept: 0,
				};
			}

			const batch = results.splice(0, query.amount);

			if (results.length === 0) {
				this._resultsCollectQueue.del(query.idString); // TODO add tests for proper cleanup/timeouts
			}

			return { results: batch, kept: results.length };
		}
		throw new Error("Unsupported");
	}

	async processCloseIteratorRequest(
		query: CloseIteratorRequest
	): Promise<void> {
		this._resultsCollectQueue.del(query.idString);
	}

	private handleQueryObject(f: Query, doc: IndexedValue<T>) {
		if (f instanceof StateFieldQuery) {
			const fv: any = extractFieldValue(doc.value, f.key);

			if (f instanceof StringMatch) {
				let compare = f.value;
				if (f.caseInsensitive) {
					compare = compare.toLowerCase();
				}

				if (Array.isArray(fv)) {
					for (const string of fv) {
						if (this.handleStringMatch(f, compare, string)) {
							return true;
						}
					}
					return false;
				} else {
					if (this.handleStringMatch(f, compare, fv)) {
						return true;
					}
					return false;
				}
			} else if (f instanceof ByteMatchQuery) {
				if (fv instanceof Uint8Array === false) {
					if (stringArraysEquals(f.key, this._indexByArr)) {
						return f.valueString === fv;
					}
					return false;
				}
				return equals(fv, f.value);
			} else if (f instanceof IntegerCompare) {
				const value: bigint | number = fv;

				if (typeof value !== "bigint" && typeof value !== "number") {
					return false;
				}
				return compare(value, f.compare, f.value.value);
			} else if (f instanceof MissingField) {
				return fv == null; // null or undefined
			} else if (f instanceof BoolQuery) {
				return fv === f.value; // true/false
			}
		} else if (f instanceof LogicalQuery) {
			if (f instanceof And) {
				for (const and of f.and) {
					if (!this.handleQueryObject(and, doc)) {
						return false;
					}
				}
				return true;
			}

			if (f instanceof Or) {
				for (const or of f.or) {
					if (this.handleQueryObject(or, doc)) {
						return true;
					}
				}
				return false;
			}
			return false;
		}

		logger.info("Unsupported query type: " + f.constructor.name);
		return false;
	}

	private handleStringMatch(f: StringMatch, compare: string, fv: string) {
		if (typeof fv !== "string") {
			return false;
		}
		if (f.caseInsensitive) {
			fv = fv.toLowerCase();
		}
		if (f.method === StringMatchMethod.exact) {
			return fv === compare;
		}
		if (f.method === StringMatchMethod.prefix) {
			return fv.startsWith(compare);
		}
		if (f.method === StringMatchMethod.contains) {
			return fv.includes(compare);
		}
		throw new Error("Unsupported");
	}

	/**
	 * Query and retrieve results with most details
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	public async queryDetailed(
		queryRequest: SearchRequest,
		options?: QueryDetailedOptions<T>
	): Promise<Results<T>[]> {
		const local = typeof options?.local == "boolean" ? options?.local : true;
		let remote: RemoteQueryOptions<Results<T>> | undefined = undefined;
		if (typeof options?.remote === "boolean") {
			if (options?.remote) {
				remote = {};
			} else {
				remote = undefined;
			}
		} else {
			remote = options?.remote || {};
		}

		const promises: Promise<Results<T> | Results<T>[] | undefined>[] = [];
		if (!local && !remote) {
			throw new Error(
				"Expecting either 'options.remote' or 'options.local' to be true"
			);
		}
		const allResults: Results<T>[] = [];

		if (local) {
			const results = await this.processFetchRequest(queryRequest);
			if (results.results.length > 0) {
				const resultsObject = new Results<T>({
					results: await Promise.all(
						results.results.map(async (r) => {
							const payloadValue = await (
								await this._log.log.get(r.context.head)
							)?.getPayloadValue();
							if (payloadValue instanceof PutOperation) {
								return new ResultWithSource({
									context: r.context,
									value: r.value,
									source: payloadValue.data,
								});
							}
							throw new Error("Unexpected");
						})
					),
					kept: BigInt(results.kept),
				});
				options?.onResponse &&
					options.onResponse(resultsObject, this.node.identity.publicKey);
				allResults.push(resultsObject);
			}
		}

		if (remote) {
			const peerMinAge = remote.minAge ?? DEFAULT_REPLICATOR_MIN_AGE;
			const replicatorGroups = await this._log.replicators?.();
			if (replicatorGroups) {
				// Make sure we don't query peers that are "too" new
				// TODO make this better
				const date = +new Date();
				const groupHashes: string[][] = [];
				for (let i = 0; i < replicatorGroups.length; i++) {
					const filteredGroup = replicatorGroups[i].filter(
						(x) => date - x.timestamp > peerMinAge
					);
					if (filteredGroup.length > 0) {
						groupHashes.push(filteredGroup.map((x) => x.hash));
					} else {
						groupHashes.push(replicatorGroups[i].map((x) => x.hash));
					}
				}
				const fn = async () => {
					const rs: Results<T>[] = [];
					const responseHandler = async (
						results: RPCResponse<Results<T>>[]
					) => {
						await introduceEntries(
							results,
							this.type,
							this._sync,
							options
						).then((x) => x.forEach((y) => rs.push(y.response)));
					};
					try {
						if (queryRequest instanceof CloseIteratorRequest) {
							// don't wait for responses
							await this._query.request(queryRequest, { to: remote!.to });
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
							logger.error("Did not reciveve responses from all shard");
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
		queryRequest: SearchRequest,
		options?: SearchOptions<T>
	): Promise<T[]> {
		// Set fetch to search size, or max value (default to max u32 (4294967295))
		queryRequest.fetch = options?.size ?? 0xffffffff;

		// So that the iterator is pre-fetching the right amount of entries
		const iterator = this.iterate(queryRequest, options);

		// So that this call will not do any remote requests
		const allResult = await iterator.next(queryRequest.fetch);

		await iterator.close();

		//s Deduplicate and return values directly
		return dedup(allResult, this.indexByResolver);
	}

	/**
	 * Query and retrieve documents in a iterator
	 * @param queryRequest
	 * @param options
	 * @returns
	 */
	public iterate(
		queryRequest: SearchRequest,
		options?: QueryOptions<T>
	): ResultsIterator<T> {
		let fetchPromise: Promise<any> | undefined = undefined;
		const peerBufferMap: Map<
			string,
			{
				kept: number;
				buffer: { value: T; context: Context; from: PublicSignKey }[];
			}
		> = new Map();
		const visited = new Set<string>();

		let done = false;
		let first = false;

		// TODO handle join/leave while iterating
		let stopperFns: (() => void)[] = [];

		const peerBuffers = (): {
			value: T;
			from: PublicSignKey;
			context: Context;
		}[] => {
			return [...peerBufferMap.values()].map((x) => x.buffer).flat();
		};

		const fetchFirst = async (n: number): Promise<boolean> => {
			done = true; // Assume we are donne
			queryRequest.fetch = n;
			await this.queryDetailed(queryRequest, {
				...options,
				onResponse: (response, from) => {
					if (!from) {
						logger.error("Missing response from");
						return;
					}

					if (response.kept === 0n && response.results.length === 0) {
						return;
					}

					if (response.kept > 0n) {
						done = false; // we have more to do later!
					}

					peerBufferMap.set(from.hashcode(), {
						buffer: response.results
							.filter(
								(x) => !visited.has(asString(this.indexByResolver(x.value)))
							)
							.map((x) => {
								visited.add(asString(this.indexByResolver(x.value)));
								return { from, value: x.value, context: x.context };
							}),
						kept: Number(response.kept),
					});
				},
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
			stopperFns = [];
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
					const collectRequest = new CollectNextRequest({
						id: queryRequest.id,
						amount: 10, //n - buffer.buffer.length,
					});
					// Fetch locally?
					if (peer === this.node.identity.publicKey.hashcode()) {
						promises.push(
							this.processFetchRequest(collectRequest)
								.then((results) => {
									resultsLeft += results.kept;

									if (results.results.length === 0) {
										if (peerBufferMap.get(peer)?.buffer.length === 0) {
											peerBufferMap.delete(peer); // No more results
										}
									} else {
										const peerBuffer = peerBufferMap.get(peer);
										if (!peerBuffer) {
											return;
										}
										peerBuffer.kept = results.kept;
										peerBuffer.buffer.push(
											...results.results
												.filter(
													(x) =>
														!visited.has(
															asString(this.indexByResolver(x.value))
														)
												)
												.map((x) => {
													visited.add(asString(this.indexByResolver(x.value)));
													return {
														value: x.value,
														context: x.context,
														from: this.node.identity.publicKey,
													};
												})
										);
									}
								})
								.catch((e) => {
									logger.error(
										"Failed to collect sorted results self. " + e?.message
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
									stopper: (fn) => stopperFns.push(fn),
									to: [peer],
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
													peerBuffer.buffer.push(
														...response.response.results
															.filter(
																(x) =>
																	!visited.has(
																		asString(this.indexByResolver(x.value))
																	)
															)
															.map((x) => {
																visited.add(
																	asString(this.indexByResolver(x.value))
																);
																return {
																	value: x.value,
																	context: x.context,
																	from: response.from!,
																};
															})
													);
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
			const results = await resolvedSort(
				peerBuffers(),
				this._toIndex,
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
				const idx = arr.buffer.findIndex((x) => x == result);
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
			for (const fn of stopperFns) {
				fn();
			}

			const closeRequest = new CloseIteratorRequest({ id: queryRequest.id });
			const promises: Promise<any>[] = [];
			for (const [peer, buffer] of peerBufferMap) {
				if (buffer.kept === 0) {
					peerBufferMap.delete(peer);
					continue;
				}
				// Fetch locally?
				if (peer === this.node.identity.publicKey.hashcode()) {
					promises.push(this.processCloseIteratorRequest(closeRequest));
				} else {
					// Fetch remotely
					promises.push(
						this._query.send(closeRequest, {
							...options,
							to: [peer],
						})
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
