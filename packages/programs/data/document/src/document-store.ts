import {
	DeleteOperation,
	DocumentIndex,
	Operation,
	PutOperation,
} from "./document-index.js";
import {
	Constructor,
	deserialize,
	field,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { asString } from "./utils.js";
import { AddOperationOptions, Store } from "@dao-xyz/peerbit-store";
import {
	BORSH_ENCODING,
	CanAppend,
	Change,
	Encoding,
	Entry,
	Log,
} from "@dao-xyz/peerbit-log";
import {
	CanOpenSubPrograms,
	ComposableProgram,
	Program,
} from "@dao-xyz/peerbit-program";
import { CanRead } from "@dao-xyz/peerbit-rpc";
import { LogIndex } from "@dao-xyz/peerbit-logindex";
import { AccessError } from "@dao-xyz/peerbit-crypto";
import { Results } from "./query.js";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import { getBlockValue } from "@dao-xyz/libp2p-direct-block"
const logger = loggerFn({ module: "document" });

export class OperationError extends Error {
	constructor(message?: string) {
		super(message);
	}
}
@variant("documents")
export class Documents<T> extends ComposableProgram {
	@field({ type: Store })
	store: Store<Operation<T>>;

	@field({ type: "bool" })
	canEdit: boolean; // "Can I overwrite a document?"

	@field({ type: DocumentIndex })
	_index: DocumentIndex<T>;

	@field({ type: LogIndex })
	_logIndex: LogIndex;

	_clazz?: Constructor<T>;

	_valueEncoding: Encoding<T>;

	_optionCanAppend?: CanAppend<Operation<T>>;

	constructor(properties: {
		canEdit?: boolean;
		index: DocumentIndex<T>;
		logIndex?: LogIndex;
	}) {
		super();
		if (properties) {
			this.store = new Store();
			this.canEdit = properties.canEdit || false;
			this._index = properties.index;
			this._logIndex = properties.logIndex || new LogIndex();
		}
	}

	get logIndex(): LogIndex {
		return this._logIndex;
	}
	get index(): DocumentIndex<T> {
		return this._index;
	}
	async setup(options: {
		type: Constructor<T>;
		canRead?: CanRead;
		canAppend?: CanAppend<Operation<T>>;
	}) {
		this._clazz = options.type;
		this._valueEncoding = BORSH_ENCODING(this._clazz);
		if (options.canAppend) {
			this._optionCanAppend = options.canAppend;
		}
		await this.store.setup({
			encoding: BORSH_ENCODING(Operation),
			canAppend: this.canAppend.bind(this),
			onUpdate: async (change: Change<Operation<T>>) => {
				await this.handleDeletions(change);
				await this._index.updateIndex(change);
			},
		});
		await this._logIndex.setup({
			store: this.store,
			canRead: options.canRead || (() => Promise.resolve(true)),
			context: this,
		});
		await this._index.setup({
			type: this._clazz,
			store: this.store,
			canRead: options.canRead || (() => Promise.resolve(true)),
			sync: async (result: Results<T>) => {
				const entries = (
					await Promise.all(
						result.results.map((result) => {
							return this.store._store
								.get<Uint8Array>(result.context.head, {
									timeout: 10 * 10000,
								})
								.then(async (bytes) => {
									if (!bytes) {
										logger.error(
											"Faield to resolve block: ",
											result.context.head
										);
										return;
									}

									const entry = deserialize(
										await getBlockValue(bytes),
										Entry
									);
									if (!this._optionCanAppend) {
										return entry;
									}
									return Promise.resolve(
										this._optionCanAppend(entry)
									) // we do optionalCanAppend on query because we might not be able to actually check with history whether we can append, TODO make more resilient/robust!
										.then((r) => {
											if (r) {
												return entry;
											}
											return undefined;
										})
										.catch((e: any) => {
											logger.info(
												"canAppend resulted in error: " +
												e.message
											);
											return undefined;
										});
								});
						})
					)
				).filter((x) => !!x) as Entry<any>[];
				await this.store.sync(entries, {
					save: !!this.replicate,
					canAppend:
						this._optionCanAppend?.bind(this) || (() => true),
				});
				const y = 123;
			},
		});
	}
	async canAppend(entry: Entry<Operation<T>>): Promise<boolean> {
		const l0 = await this._canAppend(entry);
		if (!l0) {
			return false;
		}

		if (this._optionCanAppend && !(await this._optionCanAppend(entry))) {
			return false;
		}
		return true;
	}

	async _canAppend(entry: Entry<Operation<T>>): Promise<boolean> {
		const pointsToHistory = (history: Entry<Operation<T>>) => {
			// make sure nexts only points to this document at some point in history
			let current = history;
			const next = entry.next[0];
			while (
				current?.hash &&
				next !== current?.hash &&
				current.next.length > 0
			) {
				current = this.store.oplog.get(current.next[0])!;
			}
			if (current?.hash === next) {
				return true; // Ok, we are pointing this new edit to some exising point in time of the old document
			}
			return false;
		};

		try {
			entry.init({
				encoding: this.store._oplog._encoding,
				encryption: this.store._oplog._encryption,
			});
			const operation = await entry.getPayloadValue();
			if (operation instanceof PutOperation) {
				// check nexts
				const putOperation = operation as PutOperation<T>;

				const key = putOperation.getValue(this._valueEncoding)[
					this._index.indexBy
				];
				if (!key) {
					throw new Error(
						"Expecting document to contained index field"
					);
				}
				const existingDocument = this._index.get(key);
				if (existingDocument) {
					if (!this.canEdit) {
						//Key already exist and this instance Documents can note overrite/edit'
						return false;
					}

					if (entry.next.length !== 1) {
						return false;
					}

					return pointsToHistory(existingDocument.entry);
				} else {
					if (entry.next.length !== 0) {
						return false;
					}
				}
			} else if (operation instanceof DeleteOperation) {
				if (entry.next.length !== 1) {
					return false;
				}
				2;
				const existingDocument = this._index.get(operation.key);
				if (!existingDocument) {
					// already deleted
					return false;
				}
				return pointsToHistory(existingDocument.entry); // references the existing document
			}
		} catch (error) {
			if (error instanceof AccessError) {
				return false; // we cant index because we can not decrypt
			}
			throw error;
		}
		return true;
	}

	public put(doc: T, options?: AddOperationOptions<Operation<T>>) {
		if (doc instanceof Program) {
			if (!(this.parentProgram as any as CanOpenSubPrograms).canOpen) {
				throw new Error(
					"Class " +
					this.parentProgram.constructor.name +
					" needs to implement CanOpenSubPrograms for this Documents store to progams"
				);
			}
			doc.owner = this.parentProgram.address.toString();
			doc.setupIndices();
		}

		const key = (doc as any)[this._index.indexBy];
		if (!key) {
			throw new Error(
				`The provided document doesn't contain field '${this._index.indexBy}'`
			);
		}
		const ser = serialize(doc);
		const existingDocument = this._index.get(key);

		return this.store.addOperation(
			new PutOperation({
				key: asString((doc as any)[this._index.indexBy]),
				data: ser,
				value: doc,
			}),
			{
				nexts: existingDocument ? [existingDocument.entry] : [],
				...options,
			}
		);
	}

	del(
		key: string,
		options?: AddOperationOptions<Operation<T>> & { permanent?: boolean }
	) {
		const existing = this._index.get(key);
		if (!existing) {
			throw new Error(`No entry with key '${key}' in the database`);
		}

		return this.store.addOperation(
			new DeleteOperation({
				key: asString(key),
				permanently: options?.permanent,
			}),
			{ nexts: [existing.entry], ...options }
		);
	}

	async handleDeletions(change: Change<Operation<T>>): Promise<void> {
		const entries = change.added.sort(this.store.oplog._sortFn).reverse();
		for (const entry of entries) {
			try {
				const payload = await entry.getPayloadValue();
				if (payload instanceof DeleteOperation) {
					if (payload.permanently) {
						// delete all nexts recursively (but dont delete the DELETE record (because we might want to share this with others))
						const nexts = entry.next
							.map((n) => this.store.oplog.get(n))
							.filter((x) => !!x) as Entry<any>[];

						await this.store.removeOperation(nexts, {
							recursively: true,
						});
					}
				} else {
					// Unknown operation
				}
			} catch (error) {
				if (error instanceof AccessError) {
					continue;
				}
				throw error;
			}
		}
	}
}
