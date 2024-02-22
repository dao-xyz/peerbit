import { AbstractType, field, variant } from "@dao-xyz/borsh";
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
	NoAccess,
	AbstractSearchResult
} from "./query.js";
import {
	RPC,
	RPCResponse,
	queryAll,
	MissingResponsesError,
	RPCRequestAllOptions
} from "@peerbit/rpc";
import { Results } from "./query.js";
import { logger as loggerFn } from "@peerbit/logger";
import { Cache } from "@peerbit/cache";
import { PublicSignKey, sha256Base64Sync } from "@peerbit/crypto";
import { SharedLog } from "@peerbit/shared-log";
import { concat, fromString } from "uint8arrays";
import { SilentDelivery } from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";
import { IndexKey, Keyable, keyAsString } from "./types.js";

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

/**
 * Put a complete document at a key
 */
@variant(0)
export class PutOperation<T> extends Operation<T> {
	@field({ type: Uint8Array })
	data: Uint8Array;

	_value?: T;

	constructor(props?: { data: Uint8Array; value?: T }) {
		super();
		if (props) {
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

/**
 * Delete a document at a key
 */
@variant(2)
export class DeleteOperation extends Operation<any> {
	@field({ type: IndexKey })
	key: IndexKey;

	constructor(props: { key: IndexKey }) {
		super();
		this.key = props.key;
	}
}

export interface IndexedValue<T> {
	key: string;
	value: Record<string, any> | T; // decrypted, decoded
	context: Context;
	reference?: ValueWithLastOperation<T>;
}

export type RemoteQueryOptions<R> = RPCRequestAllOptions<R> & {
	sync?: boolean;
	minAge?: number;
	throwOnMissing?: boolean;
};
export type QueryOptions<R> = {
	remote?: boolean | RemoteQueryOptions<AbstractSearchResult<R>>;
	local?: boolean;
};
export type SearchOptions<R> = { size?: number } & QueryOptions<R>;
export type IndexableFields<T> = (
	obj: T,
	context: Context
) => Record<string, any> | Promise<Record<string, any>>;

export type ResultsIterator<T> = {
	close: () => Promise<void>;
	next: (number: number) => Promise<T[]>;
	done: () => boolean;
};
type ValueWithLastOperation<T> = {
	value: T;
	last: PutOperation<Operation<T>>;
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

const extractFieldValue = <T>(doc: any, path: string[]): T => {
	for (let i = 0; i < path.length; i++) {
		doc = doc[path[i]];
	}
	return doc;
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

const resolvedSort = async <
	T,
	Q extends { value: { value: T }; context: Context }
>(
	arr: Q[],
	index: IndexableFields<T>,
	sorts: Sort[]
) => {
	await Promise.all(
		arr.map(
			async (result) =>
				(result[SORT_TMP_KEY] = await index(result.value.value, result.context))
		)
	);
	arr.sort((a, b) =>
		extractSortCompare(a[SORT_TMP_KEY], b[SORT_TMP_KEY], sorts)
	);
	return arr;
};

const SORT_TMP_KEY = "__sort_ref";

type QueryDetailedOptions<T> = QueryOptions<T> & {
	onResponse?: (response: AbstractSearchResult<T>, from: PublicSignKey) => void;
};

const introduceEntries = async <T>(
	responses: RPCResponse<AbstractSearchResult<T>>[],
	type: AbstractType<T>,
	sync: (result: Results<T>) => Promise<void>,
	options?: QueryDetailedOptions<T>
): Promise<RPCResponse<Results<T>>[]> => {
	const results: RPCResponse<Results<T>>[] = [];
	for (const response of responses) {
		if (!response.from) {
			logger.error("Missing from for response");
		}

		if (response.response instanceof Results) {
			response.response.results.forEach((r) => r.init(type));
			if (typeof options?.remote !== "boolean" && options?.remote?.sync) {
				await sync(response.response);
			}
			options?.onResponse &&
				options.onResponse(response.response, response.from!); // TODO fix types
			results.push(response as RPCResponse<Results<T>>);
		} else if (response.response instanceof NoAccess) {
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
	const unique: Set<Keyable> = new Set();
	const dedup: T[] = [];
	for (const result of allResult) {
		const key = keyAsString(dedupBy(result));
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
export const MAX_DOCUMENT_SIZE = 5e6;

const getBatchFromResults = <T>(
	results: { value: ValueWithLastOperation<T>; context: Context }[],
	wantedSize: number,
	maxSize: number = MAX_DOCUMENT_SIZE
) => {
	const batch: { value: ValueWithLastOperation<T>; context: Context }[] = [];
	let size = 0;
	for (const result of results) {
		batch.push(result);
		size += result.value.last.data.length;
		if (size > maxSize) {
			break;
		}
		if (wantedSize <= batch.length) {
			break;
		}
	}
	results.splice(0, batch.length);
	return batch;
};

export type CanSearch = (
	request: SearchRequest | CollectNextRequest,
	from: PublicSignKey
) => Promise<boolean> | boolean;

export type CanRead<T> = (
	result: T,
	from: PublicSignKey
) => Promise<boolean> | boolean;

export type InMemoryIndex<T> = { index: DocumentIndex<T> };

export type OpenOptions<T> = {
	type: AbstractType<T>;
	dbType: AbstractType<InMemoryIndex<T>>;
	log: SharedLog<Operation<T>>;
	canRead?: CanRead<T>;
	canSearch?: CanSearch;
	fields: IndexableFields<T>;
	sync: (result: Results<T>) => Promise<void>;
	indexBy?: string | string[];
};

@variant("documents_index")
export class DocumentIndex<T> extends Program<OpenOptions<T>> {
	@field({ type: RPC })
	_query: RPC<AbstractSearchRequest, AbstractSearchResult<T>>;

	type: AbstractType<T>;
	dbType: AbstractType<InMemoryIndex<T>>;

	// Index key
	private _indexBy: string | string[];
	private _indexByArr: string[];

	// Resolve doc value by index key
	indexByResolver: (obj: any) => string | Uint8Array | number | bigint;

	// Indexed (transforms an docuemnt into an obj with fields that ought to be indexed)
	private _toIndex: IndexableFields<T>;

	private _valueEncoding: Encoding<T>;

	private _sync: (result: Results<T>) => Promise<void>;
	private _index: Map<string, IndexedValue<T>>;
	private _resultsCollectQueue: Cache<{
		from: PublicSignKey;
		arr: { value: ValueWithLastOperation<T>; context: Context }[];
	}>;

	private _log: SharedLog<Operation<T>>;

	constructor(properties?: {
		query?: RPC<AbstractSearchRequest, AbstractSearchResult<T>>;
	}) {
		super();
		this._query = properties?.query || new RPC();
	}

	get index(): Map<string, IndexedValue<T>> {
		return this._index;
	}

	get valueEncoding() {
		return this._valueEncoding;
	}

	get toIndex(): IndexableFields<T> {
		return this._toIndex;
	}

	async open(properties: OpenOptions<T>) {
		this._index = new Map();
		this._log = properties.log;
		this.type = properties.type;
		this.dbType = properties.dbType;
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
					(query instanceof SearchRequest ||
						query instanceof CollectNextRequest) &&
					!(await properties.canSearch(
						query as SearchRequest | CollectNextRequest,
						ctx.from
					))
				) {
					return new NoAccess();
				}

				if (query instanceof CloseIteratorRequest) {
					this.processCloseIteratorRequest(query, ctx.from);
				} else {
					const results = await this.processFetchRequest(
						query as SearchRequest | SearchRequest | CollectNextRequest,
						ctx.from,
						{
							canRead: properties.canRead
						}
					);

					return new Results({
						// Even if results might have length 0, respond, because then we now at least there are no matching results
						results: results.results.map((r) => {
							if (r.value.last instanceof PutOperation === false) {
								throw new Error(
									"Unexpected value type on local results: " +
										(r.value.last as any)?.constructor.name ||
										typeof r.value.last
								);
							}
							return new ResultWithSource({
								source: r.value.last.data,
								context: r.context
							});
						}),
						kept: BigInt(results.kept)
					});
				}
			},
			responseType: AbstractSearchResult,
			queryType: AbstractSearchRequest
		});
	}

	public async get(
		key: Keyable | IndexKey,
		options?: QueryOptions<T>
	): Promise<T | undefined> {
		return (await this.getDetailed(key, options))?.[0]?.results[0]?.value;
	}

	public async getDetailed(
		key: Keyable | IndexKey,
		options?: QueryOptions<T>
	): Promise<Results<T>[] | undefined> {
		let results: Results<T>[] | undefined;
		if (key instanceof Uint8Array) {
			results = await this.queryDetailed(
				new SearchRequest({
					query: [new ByteMatchQuery({ key: this._indexByArr, value: key })]
				}),
				options
			);
		} else {
			const stringValue = keyAsString(key);
			results = await this.queryDetailed(
				new SearchRequest({
					query: [
						new StringMatch({
							key: this._indexByArr,
							value: stringValue
						})
					]
				}),
				options
			);
		}

		return results;
	}

	get size(): number {
		return this._index.size;
	}

	private async getDocumentWithLastOperation(value: {
		reference?: ValueWithLastOperation<T>;
		context: { head: string };
	}): Promise<ValueWithLastOperation<T> | undefined> {
		if (value.reference) {
			return value.reference;
		}

		const head = await await this._log.log.get(value.context.head);
		if (!head) {
			return undefined; // we could end up here if we recently pruned the document and other peers never persisted the entry
			// TODO update changes in index before removing entries from log entry storage
		}
		const payloadValue = await head.getPayloadValue();
		if (payloadValue instanceof PutOperation) {
			return {
				value: payloadValue.getValue(this.valueEncoding),
				last: payloadValue
			};
		}

		throw new Error(
			"Unexpected value type when getting document: " +
				payloadValue?.constructor?.name || typeof payloadValue
		);
	}

	getDocument(value: {
		reference?: ValueWithLastOperation<T>;
		context: { head: string };
	}) {
		return this.getDocumentWithLastOperation(value).then((r) => r?.value);
	}

	async _queryDocuments(
		filter: (doc: IndexedValue<T>) => Promise<boolean>
	): Promise<{ context: Context; value: ValueWithLastOperation<T> }[]> {
		// Whether we return the full operation data or just the db value
		const results: { context: Context; value: ValueWithLastOperation<T> }[] =
			[];
		for (const value of this._index.values()) {
			if (await filter(value)) {
				const topDoc = await this.getDocumentWithLastOperation(value);
				topDoc &&
					results.push({
						context: value.context,
						value: topDoc
					});
			}
		}
		return results;
	}

	async processFetchRequest(
		query: SearchRequest | CollectNextRequest,
		from: PublicSignKey,
		options?: {
			canRead?: CanRead<T>;
		}
	): Promise<{
		results: { context: Context; value: ValueWithLastOperation<T> }[];
		kept: number;
	}> {
		// We do special case for querying the id as we can do it faster than iterating

		if (query instanceof SearchRequest) {
			// Special case querying ids
			if (
				query.query.length === 1 &&
				(query.query[0] instanceof ByteMatchQuery ||
					query.query[0] instanceof StringMatch) &&
				stringArraysEquals(query.query[0].key, this._indexByArr)
			) {
				const firstQuery = query.query[0];
				if (firstQuery instanceof ByteMatchQuery) {
					const doc = this._index.get(keyAsString(firstQuery.value));
					const topDoc = doc && (await this.getDocumentWithLastOperation(doc));
					return topDoc
						? {
								results: [
									{
										value: topDoc,
										context: doc.context
									}
								],
								kept: 0
							}
						: { results: [], kept: 0 };
				} else if (
					firstQuery instanceof StringMatch &&
					firstQuery.method === StringMatchMethod.exact &&
					firstQuery.caseInsensitive === false
				) {
					const doc = this._index.get(firstQuery.value);
					const topDoc = doc && (await this.getDocumentWithLastOperation(doc));
					return topDoc
						? {
								results: [
									{
										value: topDoc,
										context: doc.context
									}
								],
								kept: 0
							}
						: { results: [], kept: 0 };
				}
			}

			// Handle query normally
			let results = await this._queryDocuments(async (doc) => {
				for (const f of query.query) {
					if (!(await this.handleQueryObject(f, doc))) {
						return false;
					}
				}
				return true;
			});

			if (options?.canRead) {
				const keepFilter = await Promise.all(
					results.map((x) => options?.canRead!(x.value.value, from))
				);
				results = results.filter((x, i) => keepFilter[i]);
			}

			// Sort
			await resolvedSort(results, this._toIndex, query.sort);

			const batch = getBatchFromResults(results, query.fetch);

			if (results.length > 0) {
				this._resultsCollectQueue.add(query.idString, {
					arr: results,
					from
				}); // cache resulst not returned
			}

			return { results: batch, kept: results.length }; // Only return 1 result since we are doing distributed sort, TODO buffer more initially
		} else if (query instanceof CollectNextRequest) {
			const results = this._resultsCollectQueue.get(query.idString);
			if (!results) {
				return {
					results: [],
					kept: 0
				};
			}

			const batch = getBatchFromResults(results.arr, query.amount);

			if (results.arr.length === 0) {
				this._resultsCollectQueue.del(query.idString); // TODO add tests for proper cleanup/timeouts
			}

			return { results: batch, kept: results.arr.length };
		}
		throw new Error("Unsupported");
	}

	async processCloseIteratorRequest(
		query: CloseIteratorRequest,
		publicKey: PublicSignKey
	): Promise<void> {
		const entry = this._resultsCollectQueue.get(query.idString);
		if (entry?.from.equals(publicKey)) {
			this._resultsCollectQueue.del(query.idString);
		} else if (entry) {
			logger.warn(
				"Received a close iterator request for a iterator that does not belong to the requesting peer"
			);
		}
	}

	private async handleFieldQuery(
		f: StateFieldQuery,
		obj: T,
		startIndex: number
	) {
		// this clause is needed if we have a field that is of type [][] (we will recursively go through each subarray)
		if (Array.isArray(obj)) {
			for (const element of obj) {
				if (await this.handleFieldQuery(f, element, startIndex)) {
					return true;
				}
			}
			return false;
		}

		// Resolve the field from the key path. If we reach an array or nested Document store,
		// then do a recursive call or a search to look into them
		for (let i = startIndex; i < f.key.length; i++) {
			obj = obj[f.key[i]];
			if (Array.isArray(obj)) {
				for (const element of obj) {
					if (await this.handleFieldQuery(f, element, i + 1)) {
						return true;
					}
				}
				return false;
			}
			if (obj instanceof this.dbType) {
				const queryCloned = f.clone();
				queryCloned.key.splice(0, i + 1); // remove key path until the document store
				const results = await (obj as any as InMemoryIndex<any>).index.search(
					new SearchRequest({ query: [queryCloned] })
				);
				return results.length > 0 ? true : false; // TODO return INNER HITS?
			}
		}

		//  When we reach here, the field value (obj) is comparable
		if (f instanceof StringMatch) {
			let compare = f.value;
			if (f.caseInsensitive) {
				compare = compare.toLowerCase();
			}

			if (this.handleStringMatch(f, compare, obj as string)) {
				return true;
			}
			return false;
		} else if (f instanceof ByteMatchQuery) {
			if (obj instanceof Uint8Array === false) {
				if (stringArraysEquals(f.key, this._indexByArr)) {
					return f.valueString === obj;
				}
				return false;
			}
			return equals(obj as Uint8Array, f.value);
		} else if (f instanceof IntegerCompare) {
			const value: bigint | number = obj as bigint | number;

			if (typeof value !== "bigint" && typeof value !== "number") {
				return false;
			}
			return compare(value, f.compare, f.value.value);
		} else if (f instanceof MissingField) {
			return obj == null; // null or undefined
		} else if (f instanceof BoolQuery) {
			return obj === f.value; // true/false
		}
		logger.warn("Unsupported query type: " + f.constructor.name);
		return false;
	}

	private async handleQueryObject(f: Query, doc: IndexedValue<T>) {
		if (f instanceof StateFieldQuery) {
			return this.handleFieldQuery(f, doc.value as T, 0);
		} else if (f instanceof LogicalQuery) {
			if (f instanceof And) {
				for (const and of f.and) {
					if (!(await this.handleQueryObject(and, doc))) {
						return false;
					}
				}
				return true;
			}

			if (f instanceof Or) {
				for (const or of f.or) {
					if (await this.handleQueryObject(or, doc)) {
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
		let remote: RemoteQueryOptions<AbstractSearchResult<T>> | undefined =
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

		const promises: Promise<Results<T>[] | undefined>[] = [];
		if (!local && !remote) {
			throw new Error(
				"Expecting either 'options.remote' or 'options.local' to be true"
			);
		}
		const allResults: Results<T>[] = [];

		if (local) {
			const results = await this.processFetchRequest(
				queryRequest,
				this.node.identity.publicKey
			);
			if (results.results.length > 0) {
				const resultsObject = new Results<T>({
					results: results.results.map((r) => {
						if (r.value.last instanceof PutOperation === false) {
							throw new Error(
								"Unexpected value type on local results: " +
									(r.value.last as any)?.constructor.name || typeof r.value.last
							);
						}
						return new ResultWithSource({
							context: r.context,
							value: r.value.value,
							source: r.value.last.data
						});
					}),
					kept: BigInt(results.kept)
				});
				options?.onResponse &&
					options.onResponse(resultsObject, this.node.identity.publicKey);
				allResults.push(resultsObject);
			}
		}

		if (remote) {
			const replicatorGroups = await this._log.getReplicatorUnion(
				remote.minAge
			);
			if (replicatorGroups) {
				const groupHashes: string[][] = replicatorGroups.map((x) => [x]);
				const fn = async () => {
					const rs: Results<T>[] = [];
					const responseHandler = async (
						results: RPCResponse<AbstractSearchResult<T>>[]
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
						if (queryRequest instanceof CloseIteratorRequest) {
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
		queryRequest: SearchRequest,
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
		queryRequest: SearchRequest,
		options?: QueryOptions<T>
	): ResultsIterator<T> {
		let fetchPromise: Promise<any> | undefined = undefined;
		const peerBufferMap: Map<
			string,
			{
				kept: number;
				buffer: {
					value: { value: T };
					context: Context;
					from: PublicSignKey;
				}[];
			}
		> = new Map();
		const visited = new Set<string>();

		let done = false;
		let first = false;

		// TODO handle join/leave while iterating
		const controller = new AbortController();

		const peerBuffers = (): {
			value: { value: T };
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
					if (response instanceof NoAccess) {
						logger.error("Dont have access");
						return;
					} else if (response instanceof Results) {
						const results = response as Results<T>;
						if (results.kept === 0n && results.results.length === 0) {
							return;
						}

						if (results.kept > 0n) {
							done = false; // we have more to do later!
						}

						peerBufferMap.set(from.hashcode(), {
							buffer: results.results
								.filter(
									(x) =>
										!visited.has(keyAsString(this.indexByResolver(x.value)))
								)
								.map((x) => {
									visited.add(keyAsString(this.indexByResolver(x.value)));
									return {
										from,
										value: { value: x.value },
										context: x.context
									};
								}),
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
					const collectRequest = new CollectNextRequest({
						id: queryRequest.id,
						amount: n - buffer.buffer.length
					});
					// Fetch locally?
					if (peer === this.node.identity.publicKey.hashcode()) {
						promises.push(
							this.processFetchRequest(
								collectRequest,
								this.node.identity.publicKey
							)
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
															keyAsString(this.indexByResolver(x.value.value))
														)
												)
												.map((x) => {
													visited.add(
														keyAsString(this.indexByResolver(x.value.value))
													);
													return {
														value: x.value,
														context: x.context,
														from: this.node.identity.publicKey
													};
												})
										);
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
													peerBuffer.buffer.push(
														...response.response.results
															.filter(
																(x) =>
																	!visited.has(
																		keyAsString(this.indexByResolver(x.value))
																	)
															)
															.map((x) => {
																visited.add(
																	keyAsString(this.indexByResolver(x.value))
																);
																return {
																	value: { value: x.value },
																	context: x.context,
																	from: response.from!
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
				batch.map((x) => x.value.value),
				this.indexByResolver
			);
		};

		const close = async () => {
			controller.abort(new AbortError("Iterator closed"));

			const closeRequest = new CloseIteratorRequest({ id: queryRequest.id });
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
