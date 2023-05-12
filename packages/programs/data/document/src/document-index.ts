import { AbstractType, field, serialize, variant } from "@dao-xyz/borsh";
import { asString, Keyable } from "./utils.js";
import { BORSH_ENCODING, Encoding, Entry } from "@dao-xyz/peerbit-log";
import { equals } from "@dao-xyz/uint8arrays";
import { ComposableProgram } from "@dao-xyz/peerbit-program";
import {
	IntegerCompare,
	ByteMatchQuery,
	StringMatch,
	DocumentQuery,
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
} from "./query.js";
import {
	CanRead,
	RPC,
	QueryContext,
	RPCOptions,
	RPCResponse,
	queryAll,
	MissingResponsesError,
} from "@dao-xyz/peerbit-rpc";
import { Results } from "./query.js";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import { Log } from "@dao-xyz/peerbit-log";

const logger = loggerFn({ module: "document-index" });

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
}

export type RemoteQueryOptions<R> = RPCOptions<R> & { sync?: boolean };
export type QueryOptions<R> = {
	onResponse?: (response: Results<R>) => void;
	remote?: boolean | RemoteQueryOptions<Results<R>>;
	local?: boolean;
};

export type Indexable<T> = (
	obj: T,
	entry: Entry<Operation<T>>
) => Record<string, any>;

@variant("documents_index")
export class DocumentIndex<T> extends ComposableProgram {
	@field({ type: RPC })
	_query: RPC<DocumentQuery, Results<T>>;

	@field({ type: "string" })
	indexBy: string;

	type: AbstractType<T>;
	private _valueEncoding: Encoding<T>;

	private _sync: (result: Results<T>) => Promise<void>;
	private _index: Map<string, IndexedValue<T>>;
	private _log: Log<Operation<T>>;
	private _replicators: () => string[][] | undefined;
	private _toIndex: Indexable<T>;

	constructor(properties: {
		query?: RPC<DocumentQuery, Results<T>>;
		indexBy: string;
	}) {
		super();
		this._query = properties.query || new RPC();
		this.indexBy = properties.indexBy;
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
	set replicators(replicators: () => string[][] | undefined) {
		this._replicators = replicators;
	}

	async setup(properties: {
		type: AbstractType<T>;
		log: Log<Operation<T>>;
		canRead: CanRead;
		fields: Indexable<T>;
		sync: (result: Results<T>) => Promise<void>;
	}) {
		this._index = new Map();
		this._log = properties.log;
		this.type = properties.type;
		this._sync = properties.sync;
		this._toIndex = properties.fields;
		this._valueEncoding = BORSH_ENCODING(this.type);

		await this._query.setup({
			context: this,
			canRead: properties.canRead,
			responseHandler: async (query, context) => {
				const results = await this.queryHandler(query, context);
				return new Results({
					// Even if results might have length 0, respond, because then we now at least there are no matching results
					results: results.map(
						(r) =>
							new ResultWithSource({
								source: serialize(r.value),
								context: r.context,
							})
					),
				});
			},
			responseType: Results,
			queryType: DocumentQuery,
		});
	}

	public async get(
		key: Keyable,
		options?: QueryOptions<T>
	): Promise<Results<T> | undefined> {
		let results: Results<T>[] | undefined;
		if (key instanceof Uint8Array) {
			results = await this.query(
				new DocumentQuery({
					queries: [new ByteMatchQuery({ key: [this.indexBy], value: key })],
				})
			);
		} else {
			const stringValue = asString(key);
			results = await this.query(
				new DocumentQuery({
					queries: [
						new StringMatch({
							key: [this.indexBy],
							value: stringValue,
						}),
					],
				}),
				options
			);
		}

		return results?.[0];
	}

	get size(): number {
		return this._index.size;
	}

	async getDocument(value: { context: { head: string } }): Promise<T> {
		const payloadValue = await (await this._log.get(
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

	async queryHandler(
		query: DocumentQuery,
		context?: QueryContext // TODO needed?
	): Promise<{ context: Context; value: T }[]> {
		const queries: Query[] = query.queries;
		if (
			query.queries.length === 1 &&
			(query.queries[0] instanceof ByteMatchQuery ||
				query.queries[0] instanceof StringMatch) &&
			query.queries[0].key.length === 1 &&
			query.queries[0].key[0] === this.indexBy
		) {
			if (
				query.queries[0] instanceof StringMatch ||
				query.queries[0] instanceof ByteMatchQuery
			) {
				const doc = this._index.get(asString(query.queries[0].value)); // TODO could there be a issue with types here?
				return doc
					? [
							{
								value: await this.getDocument(doc),
								context: doc.context,
							},
					  ]
					: [];
			}
		}

		const results = await this._queryDocuments((doc) =>
			queries?.length > 0
				? queries
						.map((f) => {
							const nested = this.handleQueryObject(f, doc);
							return nested;
						})
						.reduce((prev, current) => prev && current)
				: true
		);

		return results;
	}

	private handleQueryObject(f: Query, doc: IndexedValue<T>) {
		if (f instanceof StateFieldQuery) {
			let fv: any = doc.value;
			for (let i = 0; i < f.key.length; i++) {
				fv = fv[f.key[i]];
			}

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

	public async query(
		queryRequest: DocumentQuery,
		options?: QueryOptions<T>
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
			const results = await this.queryHandler(queryRequest, {
				address: this.address.toString(),
				from: this.identity.publicKey,
			});
			if (results.length > 0) {
				const resultsObject = new Results<T>({
					results: await Promise.all(
						results.map(async (r) => {
							const payloadValue = await this._log
								.get(r.context.head)
								.then((x) => x?.getPayloadValue());
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
				});
				options?.onResponse && options.onResponse(resultsObject);
				allResults.push(resultsObject);
			}
		}

		if (remote) {
			const initFn = async (responses: RPCResponse<Results<T>>[]) => {
				return Promise.all(
					responses.map(async (x) => {
						x.response.results.forEach((r) => r.init(this.type));
						if (typeof options?.remote !== "boolean" && options?.remote?.sync) {
							await this._sync(x.response);
						}
						options?.onResponse && options.onResponse(x.response);
						return x.response;
					})
				);
			};

			const replicatorGroups = await this._replicators();
			if (replicatorGroups) {
				const fn = async () => {
					const rs: Results<T>[] = [];
					const responseHandler = async (
						results: RPCResponse<Results<T>>[]
					) => {
						const resultsInitialized = await initFn(results);
						rs.push(...resultsInitialized);
					};
					try {
						await queryAll(
							this._query,
							replicatorGroups,
							queryRequest,
							responseHandler,
							remote
						);
					} catch (error) {
						if (error instanceof MissingResponsesError) {
							logger.error("Did not reciveve responses from all shard");
						}
					}
					return rs;
				};
				promises.push(fn());
			} else {
				promises.push(
					this._query
						.send(queryRequest, remote)
						.then((response) => initFn(response))
				);
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
}
