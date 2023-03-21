import { AbstractType, field, serialize, variant } from "@dao-xyz/borsh";
import { asString, Keyable } from "./utils.js";
import { BORSH_ENCODING, Encoding, Entry } from "@dao-xyz/peerbit-log";
import { equals } from "@dao-xyz/uint8arrays";
import { ComposableProgram } from "@dao-xyz/peerbit-program";
import {
	IntegerCompareQuery,
	ByteMatchQuery,
	StringMatchQuery,
	DocumentQueryRequest,
	Query,
	ResultWithSource,
	StateFieldQuery,
	compare,
	Context,
	MissingQuery,
	StringMatchMethod,
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
import { Store } from "@dao-xyz/peerbit-store";
import { EncryptedThing, X25519PublicKey } from "@dao-xyz/peerbit-crypto";
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
	_query: RPC<DocumentQueryRequest, Results<T>>;

	@field({ type: "string" })
	indexBy: string;

	type: AbstractType<T>;
	private _valueEncoding: Encoding<T>;

	private _sync: (result: Results<T>) => Promise<void>;
	private _index: Map<string, IndexedValue<T>>;
	private _store: Store<Operation<T>>;
	private _replicators: () => string[][] | undefined;
	private _toIndex: Indexable<T>;

	constructor(properties: {
		query?: RPC<DocumentQueryRequest, Results<T>>;
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
		store: Store<Operation<T>>;
		canRead: CanRead;
		fields: Indexable<T>;
		sync: (result: Results<T>) => Promise<void>;
	}) {
		this._index = new Map();
		this._store = properties.store;
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
			queryType: DocumentQueryRequest,
		});
	}

	public async get(
		key: Keyable,
		options?: QueryOptions<T>
	): Promise<Results<T> | undefined> {
		let results: Results<T>[] | undefined;
		if (key instanceof Uint8Array) {
			results = await this.query(
				new DocumentQueryRequest({
					queries: [new ByteMatchQuery({ key: [this.indexBy], value: key })],
				})
			);
		} else {
			const stringValue = asString(key);
			results = await this.query(
				new DocumentQueryRequest({
					queries: [
						new StringMatchQuery({
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

	async getDocument(indexedValue: { context: { head: string } }): Promise<T> {
		const payloadValue = await this._store.oplog
			.get(indexedValue.context.head)
			.then((x) => x?.getPayloadValue());
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
		query: DocumentQueryRequest,
		context?: QueryContext // TODO needed?
	): Promise<{ context: Context; value: T }[]> {
		const queries: Query[] = query.queries;
		if (
			query.queries.length === 1 &&
			(query.queries[0] instanceof ByteMatchQuery ||
				query.queries[0] instanceof StringMatchQuery) &&
			query.queries[0].key.length === 1 &&
			query.queries[0].key[0] === this.indexBy
		) {
			if (
				query.queries[0] instanceof StringMatchQuery ||
				query.queries[0] instanceof ByteMatchQuery
			) {
				const doc = this._index.get(asString(query.queries[0].value)); // TODO could there be a issue with types here?
				return doc
					? [{ value: await this.getDocument(doc), context: doc.context }]
					: [];
			}
		}

		const results = await this._queryDocuments((doc) =>
			queries?.length > 0
				? queries
						.map((f) => {
							if (f instanceof StateFieldQuery) {
								let fv: any = doc.value;
								for (let i = 0; i < f.key.length; i++) {
									fv = fv[f.key[i]];
								}

								if (f instanceof StringMatchQuery) {
									if (typeof fv !== "string") {
										return false;
									}
									let compare = f.value;
									if (f.caseInsensitive) {
										fv = fv.toLowerCase();
										compare = compare.toLowerCase();
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
								} else if (f instanceof ByteMatchQuery) {
									if (fv instanceof Uint8Array === false) {
										return false;
									}
									return equals(fv, f.value);
								} else if (f instanceof IntegerCompareQuery) {
									const value: bigint | number = fv;

									if (typeof value !== "bigint" && typeof value !== "number") {
										return false;
									}

									return compare(value, f.compare, f.value.value);
								} else if (f instanceof MissingQuery) {
									return fv == null; // null or undefined
								}
							} /* else if (f instanceof MemoryCompareQuery) {
							const operation = doc.entry.payload.getValue(
								BORSH_ENCODING_OPERATION
							);
							if (!operation) {
								throw new Error(
									"Unexpected, missing cached value for payload"
								);
							}
							if (operation instanceof PutOperation) {
								const bytes = operation.data;
								for (const compare of f.compares) {
									const offsetn = Number(compare.offset); // TODO type check

									for (let b = 0; b < compare.bytes.length; b++) {
										if (bytes[offsetn + b] !== compare.bytes[b]) {
											return false;
										}
									}
								}
							} else {
								// TODO add implementations for PutAll
								return false;
							}
							return true;
						}  else if (f instanceof CreatedAtQuery) {
							for (const created of f.created) {
								if (
									!compare(
										doc.context.created,
										created.compare,
										created.value
									)
								) {
									return false;
								}
							}
							return true;
						} else if (f instanceof ModifiedAtQuery) {
							for (const modified of f.modified) {
								if (
									!compare(
										doc.context.modified,
										modified.compare,
										modified.value
									)
								) {
									return false;
								}
							}
							return true;
						}  else if (f instanceof EntryEncryptedByQuery) {
							if (doc.entry._payload instanceof EncryptedThing) {
								const check = (
									encryptedThing: EncryptedThing<any>,
									keysToFind: X25519PublicKey[]
								) => {
									for (const k of encryptedThing._envelope._ks) {
										for (const s of keysToFind) {
											if (k._recieverPublicKey.equals(s)) {
												return true;
											}
										}
									}
									return false;
								};

								if (f.metadata.length > 0) {
									if (
										!check(
											doc.entry._payload as EncryptedThing<any>,
											f.metadata
										)
									) {
										return false;
									}
								}

								if (f.next.length > 0) {
									if (
										!check(doc.entry._payload as EncryptedThing<any>, f.next)
									) {
										return false;
									}
								}

								if (f.payload.length > 0) {
									if (
										!check(
											doc.entry._payload as EncryptedThing<any>,
											f.payload
										)
									) {
										return false;
									}
								}

								if (f.signatures.length > 0) {
									if (
										!check(
											doc.entry._payload as EncryptedThing<any>,
											f.signatures
										)
									) {
										return false;
									}
								}
							} else {
								return (
									f.signatures.length == 0 &&
									f.payload.length == 0 &&
									f.metadata.length == 0 &&
									f.next.length == 0
								);
							}

							return true;
						} else if (f instanceof SignedByQuery) {
							for (const key of f.publicKeys) {
								let exist = false;
								for (const signature of doc.entry.signatures) {
									if (key.equals(signature.publicKey)) {
										exist = true;
									}
								}

								if (!exist) {
									return false;
								}
							}
							return true;
						} */

							logger.info("Unsupported query type: " + f.constructor.name);
							return false;
						})
						.reduce((prev, current) => prev && current)
				: true
		);

		return results;
	}
	public async query(
		queryRequest: DocumentQueryRequest,
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
							const payloadValue = await this._store.oplog
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
