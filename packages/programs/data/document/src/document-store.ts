import {
	Indexable,
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	DocumentIndex,
	Operation,
	PutOperation,
} from "./document-index.js";
import {
	AbstractType,
	deserialize,
	field,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { asString, Keyable } from "./utils.js";
import { AddOperationOptions, Store } from "@dao-xyz/peerbit-store";
import { CanAppend, Change, Entry, EntryType } from "@dao-xyz/peerbit-log";
import {
	ComposableProgram,
	Program,
	ProgramInitializationOptions,
} from "@dao-xyz/peerbit-program";
import { CanRead } from "@dao-xyz/peerbit-rpc";
import { AccessError, DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { Context, Results } from "./query.js";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import { ReplicatorType } from "@dao-xyz/peerbit-program";
import { EventEmitter, CustomEvent } from "@libp2p/interfaces/events";

const logger = loggerFn({ module: "document" });

export class OperationError extends Error {
	constructor(message?: string) {
		super(message);
	}
}
export interface DocumentsChange<T> {
	added: T[];
	removed: T[];
}
export interface DocumentEvents<T> {
	change: CustomEvent<DocumentsChange<T>>;
}

@variant("documents")
export class Documents<
	T extends Record<string, any>
> extends ComposableProgram {
	@field({ type: Store })
	store: Store<Operation<T>>;

	@field({ type: "bool" })
	immutable: boolean; // "Can I overwrite a document?"

	@field({ type: DocumentIndex })
	private _index: DocumentIndex<T>;

	private _clazz?: AbstractType<T>;

	private _optionCanAppend?: CanAppend<Operation<T>>;
	canOpen?: (program: Program, entry: Entry<Operation<T>>) => Promise<boolean>;
	private _events: EventEmitter<DocumentEvents<T>>;

	constructor(properties: { immutable?: boolean; index: DocumentIndex<T> }) {
		super();
		if (properties) {
			this.store = new Store();
			this.immutable = properties.immutable ?? false;
			this._index = properties.index;
		}
	}

	get index(): DocumentIndex<T> {
		return this._index;
	}

	get events(): EventEmitter<DocumentEvents<T>> {
		if (!this._events) {
			throw new Error("Program not open");
		}
		return this._events;
	}

	async init(_, __, options: ProgramInitializationOptions) {
		this._index.replicators = options.replicators;
		return super.init(_, __, options);
	}
	async setup(options: {
		type: AbstractType<T>;
		canRead?: CanRead;
		canAppend?: CanAppend<Operation<T>>;
		canOpen?: (program: Program) => Promise<boolean>;
		index?: {
			fields: Indexable<T>;
		};
	}) {
		this._clazz = options.type;
		this.canOpen = options.canOpen;
		this._events = new EventEmitter();

		/* eslint-disable */
		if (Program.isPrototypeOf(this._clazz)) {
			if (!this.canOpen) {
				throw new Error(
					"setup needs to be called with the canOpen option when the document type is a Program"
				);
			}
		}
		if (options.canAppend) {
			this._optionCanAppend = options.canAppend;
		}
		await this.store.setup({
			encoding: BORSH_ENCODING_OPERATION,
			canAppend: this.canAppend.bind(this),
			onUpdate: this.handleChanges.bind(this),
		});

		await this._index.setup({
			type: this._clazz,
			store: this.store,
			canRead: options.canRead || (() => Promise.resolve(true)),
			fields: options.index?.fields || ((obj) => obj),
			sync: async (result: Results<T>) =>
				this.store.sync(result.results.map((x) => x.context.head)),
		});
	}

	private async _resolveEntry(history: Entry<Operation<T>> | string) {
		return typeof history === "string"
			? (await this.store.oplog.get(history)) ||
					(await Entry.fromMultihash<Operation<T>>(
						this.store.oplog.storage,
						history
					))
			: history;
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
		const resolve = async (history: Entry<Operation<T>> | string) => {
			return typeof history === "string"
				? this.store.oplog.get(history) ||
						(await Entry.fromMultihash(this.store.oplog.storage, history))
				: history;
		};
		const pointsToHistory = async (history: Entry<Operation<T>> | string) => {
			// make sure nexts only points to this document at some point in history
			let current = await resolve(history);

			const next = entry.next[0];
			while (
				current?.hash &&
				next !== current?.hash &&
				current.next.length > 0
			) {
				current = await this.store.oplog.get(current.next[0])!;
			}
			if (current?.hash === next) {
				return true; // Ok, we are pointing this new edit to some exising point in time of the old document
			}
			return false;
		};

		try {
			entry.init({
				encoding: this.store.oplog.encoding,
				encryption: this.store.oplog.encryption,
			});
			const operation =
				entry._payload instanceof DecryptedThing
					? entry.payload.getValue(entry.encoding)
					: await entry.getPayloadValue();
			if (operation instanceof PutOperation) {
				// check nexts
				const putOperation = operation as PutOperation<T>;

				const key = putOperation.getValue(this.index.valueEncoding)[
					this._index.indexBy
				];
				if (!key) {
					throw new Error("Expecting document to contained index field");
				}
				const existingDocument = this._index.index.get(key);
				if (existingDocument) {
					if (this.immutable) {
						//Key already exist and this instance Documents can note overrite/edit'
						return false;
					}

					if (entry.next.length !== 1) {
						return false;
					}
					let doc = await this.store.oplog.get(existingDocument.context.head);
					if (!doc) {
						logger.error("Failed to find Document from head");
						return false;
					}
					return pointsToHistory(doc);
				} else {
					if (entry.next.length !== 0) {
						return false;
					}
				}
			} else if (operation instanceof DeleteOperation) {
				if (entry.next.length !== 1) {
					return false;
				}
				const existingDocument = this._index.index.get(operation.key); //  (await this._index.get(operation.key))?.results[0];
				if (!existingDocument) {
					// already deleted
					return false;
				}
				let doc = await this.store.oplog.get(existingDocument.context.head);
				if (!doc) {
					logger.error("Failed to find Document from head");
					return false;
				}
				return pointsToHistory(doc); // references the existing document
			}
		} catch (error) {
			if (error instanceof AccessError) {
				return false; // we cant index because we can not decrypt
			}
			throw error;
		}
		return true;
	}

	public async put(
		doc: T,
		options?: AddOperationOptions<Operation<T>> & { unique?: boolean }
	) {
		if (doc instanceof Program) {
			if (this.parentProgram == null) {
				throw new Error(
					`Program ${this.constructor.name} have not been opened, as 'parentProgram' property is missing`
				);
			}
			doc.setupIndices();
		}

		const key = (doc as any)[this._index.indexBy];
		if (!key) {
			throw new Error(
				`The provided document doesn't contain field '${this._index.indexBy}'`
			);
		}

		const ser = serialize(doc);
		const existingDocument = options?.unique
			? undefined
			: (await this._index.get(key, { local: true, remote: { sync: true } }))
					?.results[0];
		return this.store.append(
			new PutOperation({
				key: asString((doc as any)[this._index.indexBy]),
				data: ser,
				value: doc,
			}),
			{
				nexts: existingDocument
					? [await this._resolveEntry(existingDocument.context.head)]
					: [], //
				...options,
			}
		);
	}

	async del(key: Keyable, options?: AddOperationOptions<Operation<T>>) {
		const existing = (
			await this._index.get(key, { local: true, remote: { sync: true } })
		)?.results[0];
		if (!existing) {
			throw new Error(`No entry with key '${key}' in the database`);
		}

		return this.store.append(
			new DeleteOperation({
				key: asString(key),
			}),
			{
				nexts: [await this._resolveEntry(existing.context.head)],
				type: EntryType.CUT,
				...options,
			} //
		);
	}

	async handleChanges(change: Change<Operation<T>>): Promise<void> {
		const removed = [...(change.removed || [])];
		const removedSet = new Map<string, Entry<Operation<T>>>();
		for (const r of removed) {
			removedSet.set(r.hash, r);
		}
		const entries = [...change.added, ...(removed || [])]
			.sort(this.store.oplog.sortFn)
			.reverse(); // sort so we get newest to oldest

		// There might be a case where change.added and change.removed contains the same document id. Usaully because you use the "trim" option
		// in combination with inserting the same document. To mitigate this, we loop through the changes and modify the behaviour for this

		let visited = new Map<string, Entry<Operation<T>>[]>();
		for (const item of entries) {
			const payload =
				item._payload instanceof DecryptedThing
					? item.payload.getValue(item.encoding)
					: await item.getPayloadValue();
			let itemKey: string;
			if (
				payload instanceof PutOperation ||
				payload instanceof DeleteOperation
			) {
				itemKey = payload.key;
			} else {
				throw new Error("Unsupported operation type");
			}

			let arr = visited.get(itemKey);
			if (!arr) {
				arr = [];
				visited.set(itemKey, arr);
			}
			arr.push(item);
		}

		let documentsChanged: DocumentsChange<T> = {
			added: [],
			removed: [],
		};

		for (const [itemKey, entries] of visited) {
			try {
				const item = entries[0];
				const payload =
					item._payload instanceof DecryptedThing
						? item.payload.getValue(item.encoding)
						: await item.getPayloadValue();
				if (payload instanceof PutOperation && !removedSet.has(item.hash)) {
					const key = payload.key;
					const value = this.deserializeOrPass(payload);

					documentsChanged.added.push(value);

					this._index.index.set(key, {
						key: payload.key,
						value: this._index.toIndex(value, item),
						context: new Context({
							created:
								this._index.index.get(key)?.context.created ||
								item.metadata.clock.timestamp.wallTime,
							modified: item.metadata.clock.timestamp.wallTime,
							head: item.hash,
						}),
					});

					// Program specific
					if (value instanceof Program) {
						if (!this.open) {
							throw new Error(
								"Documents have not been initialized with the open function, which is required for types that extends Program"
							);
						}

						// if replicator, then open
						if (
							(await this.canOpen!(value, item)) &&
							this.role instanceof ReplicatorType &&
							(await this.store.options.replicator!(item.gid))
						) {
							await this.open!(value);
						}
					}
				} else if (
					(payload instanceof DeleteOperation && !removedSet.has(item.hash)) ||
					payload instanceof PutOperation ||
					removedSet.has(item.hash)
				) {
					const key = (payload as DeleteOperation | PutOperation<T>).key;
					if (!this.index.index.has(key)) {
						continue;
					}

					let value: T;
					if (payload instanceof PutOperation) {
						value = this.deserializeOrPass(payload);
					} else if (payload instanceof DeleteOperation) {
						value = await this.getDocumentFromEntry(entries[1]!);
					} else {
						throw new Error("Unexpected");
					}

					documentsChanged.removed.push(value);

					if (value instanceof Program) {
						// TODO is this tested?
						await value.close(this);
					}

					// update index
					this._index.index.delete(key);
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

		this.events.dispatchEvent(
			new CustomEvent("change", { detail: documentsChanged })
		);
	}

	private async getDocumentFromEntry(entry: Entry<Operation<T>>) {
		const payloadValue = await entry.getPayloadValue();
		if (payloadValue instanceof PutOperation) {
			return payloadValue.getValue(this.index.valueEncoding);
		}
		throw new Error("Unexpected");
	}
	deserializeOrPass(value: PutOperation<T>): T {
		if (value._value) {
			return value._value;
		} else {
			value._value = deserialize(value.data, this.index.type);
			return value._value!;
		}
	}
}
