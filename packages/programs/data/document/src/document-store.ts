import {
	AbstractType,
	BorshError,
	deserialize,
	field,
	serialize,
	variant
} from "@dao-xyz/borsh";
import { Change, Entry, EntryType, TrimOptions } from "@peerbit/log";
import { Program, ProgramEvents } from "@peerbit/program";
import { AccessError, DecryptedThing } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import { AppendOptions } from "@peerbit/log";
import { CustomEvent } from "@libp2p/interface/events";
import {
	Role,
	Observer,
	Replicator,
	SharedLog,
	SharedLogOptions,
	SharedAppendOptions
} from "@peerbit/shared-log";

export { Role, Observer, Replicator }; // For convenience (so that consumers does not have to do the import above from shared-log packages)

import {
	IndexableFields,
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	DocumentIndex,
	Operation,
	PutOperation,
	CanSearch,
	CanRead,
	IndexedDB
} from "./document-index.js";
import { asString, checkKeyable, Keyable } from "./utils.js";
import { Context, Results } from "./query.js";

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

export type TransactionContext<T> = {
	entry: Entry<Operation<T>>;
};

type MaybePromise = Promise<boolean> | boolean;

export type CanPerform<T> = (
	operation: PutOperation<T> | DeleteOperation,
	context: TransactionContext<T>
) => MaybePromise;

export type SetupOptions<T> = {
	type: AbstractType<T>;
	canOpen?: (program: T) => MaybePromise;
	canPerform?: CanPerform<T>;
	index?: {
		key?: string | string[];
		fields?: IndexableFields<T>;
		canSearch?: CanSearch;
		canRead?: CanRead<T>;
	};
	log?: {
		trim?: TrimOptions;
	};
} & SharedLogOptions;

@variant("documents")
export class Documents<T extends Record<string, any>>
	extends Program<SetupOptions<T>, DocumentEvents<T> & ProgramEvents>
	implements IndexedDB<T>
{
	@field({ type: SharedLog })
	log: SharedLog<Operation<T>>;

	@field({ type: "bool" })
	immutable: boolean; // "Can I overwrite a document?"

	@field({ type: DocumentIndex })
	private _index: DocumentIndex<T>;

	private _clazz: AbstractType<T>;

	private _optionCanPerform?: CanPerform<T>;

	canOpen?: (
		program: T,
		entry: Entry<Operation<T>>
	) => Promise<boolean> | boolean;

	constructor(properties?: {
		id?: Uint8Array;
		immutable?: boolean;
		index?: DocumentIndex<T>;
	}) {
		super();

		this.log = new SharedLog(properties);
		this.immutable = properties?.immutable ?? false;
		this._index = properties?.index || new DocumentIndex();
	}

	get index(): DocumentIndex<T> {
		return this._index;
	}

	async open(options: SetupOptions<T>) {
		this._clazz = options.type;
		this.canOpen = options.canOpen;

		/* eslint-disable */
		if (Program.isPrototypeOf(this._clazz)) {
			if (!this.canOpen) {
				throw new Error(
					"Document store needs to be opened with canOpen option when the document type is a Program"
				);
			}
		}

		this._optionCanPerform = options.canPerform;

		await this._index.open({
			type: this._clazz,
			log: this.log,
			canRead: options?.index?.canRead,
			canSearch: options.index?.canSearch,
			fields: options.index?.fields || ((obj) => obj),
			indexBy: options.index?.key,
			sync: async (result: Results<T>) =>
				this.log.log.join(result.results.map((x) => x.context.head)),
			dbType: this.constructor
		});

		await this.log.open({
			encoding: BORSH_ENCODING_OPERATION,
			canReplicate: options?.canReplicate,
			canAppend: this.canAppend.bind(this),
			onChange: this.handleChanges.bind(this),
			trim: options?.log?.trim,
			role: options?.role,
			replicas: options?.replicas
		});
	}

	async recover() {
		return this.log.recover();
	}

	private async _resolveEntry(history: Entry<Operation<T>> | string) {
		return typeof history === "string"
			? (await this.log.log.get(history)) ||
					(await Entry.fromMultihash<Operation<T>>(
						this.log.log.storage,
						history
					))
			: history;
	}

	async updateRole(role: Replicator | Observer) {
		await this.log.updateRole(role);
	}
	get role(): Replicator | Observer {
		return this.log.role;
	}

	async canAppend(entry: Entry<Operation<T>>): Promise<boolean> {
		const l0 = await this._canAppend(entry);
		if (!l0) {
			return false;
		}

		try {
			const payload = await entry.getPayloadValue();
			if (payload instanceof PutOperation) {
				(payload as PutOperation<T>).getValue(this.index.valueEncoding); // Decode they value so callbacks can jsut do .value
			}

			if (this._optionCanPerform) {
				if (
					!(await this._optionCanPerform(
						payload as PutOperation<T> | DeleteOperation,
						{
							entry
						}
					))
				) {
					return false;
				}
			}
		} catch (error) {
			if (error instanceof BorshError) {
				logger.warn("Received payload that could not be decoded, skipping");
				return false;
			}
			throw error;
		}

		return true;
	}

	async _canAppend(entry: Entry<Operation<T>>): Promise<boolean> {
		const resolve = async (history: Entry<Operation<T>> | string) => {
			return typeof history === "string"
				? this.log.log.get(history) ||
						(await Entry.fromMultihash(this.log.log.storage, history))
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
				current = await this.log.log.get(current.next[0])!;
			}
			if (current?.hash === next) {
				return true; // Ok, we are pointing this new edit to some exising point in time of the old document
			}
			return false;
		};

		try {
			entry.init({
				encoding: this.log.log.encoding,
				keychain: this.node.keychain
			});
			const operation =
				entry._payload instanceof DecryptedThing
					? entry.payload.getValue(entry.encoding)
					: await entry.getPayloadValue();
			if (operation instanceof PutOperation) {
				// check nexts
				const putOperation = operation as PutOperation<T>;

				const key = this._index.indexByResolver(
					putOperation.getValue(this.index.valueEncoding)
				) as Keyable;

				checkKeyable(key);

				const existingDocument = this.index.index.get(asString(key));
				if (existingDocument) {
					if (this.immutable) {
						//Key already exist and this instance Documents can note overrite/edit'
						return false;
					}

					if (entry.next.length !== 1) {
						return false;
					}
					let doc = await this.log.log.get(existingDocument.context.head);
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
				const existingDocument = this._index.index.get(operation.key);
				if (!existingDocument) {
					// already deleted
					return false;
				}
				let doc = await this.log.log.get(existingDocument.context.head);
				if (!doc) {
					logger.error("Failed to find Document from head");
					return false;
				}
				return pointsToHistory(doc); // references the existing document
			}
		} catch (error) {
			if (error instanceof AccessError) {
				return false; // we cant index because we can not decrypt
			} else if (error instanceof BorshError) {
				logger.warn("Received payload that could not be decoded, skipping");
				return false;
			}
			throw error;
		}
		return true;
	}

	public async put(
		doc: T,
		options?: SharedAppendOptions<Operation<T>> & { unique?: boolean }
	) {
		const key = this._index.indexByResolver(doc as any as Keyable);
		checkKeyable(key);
		const ser = serialize(doc);
		const existingDocument = options?.unique
			? undefined
			: (
					await this._index.getDetailed(key, {
						local: true,
						remote: { sync: true } // only query remote if we know they exist
					})
			  )?.[0]?.results[0];

		return this.log.append(
			new PutOperation({
				key: asString(key),
				data: ser,
				value: doc
			}),
			{
				...options,
				meta: {
					next: existingDocument
						? [await this._resolveEntry(existingDocument.context.head)]
						: [],
					...options?.meta
				} //
			}
		);
	}

	async del(key: Keyable, options?: AppendOptions<Operation<T>>) {
		const existing = (
			await this._index.getDetailed(key, {
				local: true,
				remote: { sync: true }
			})
		)?.[0]?.results[0];
		if (!existing) {
			throw new Error(`No entry with key '${key}' in the database`);
		}

		return this.log.append(
			new DeleteOperation({
				key: asString(key)
			}),
			{
				...options,
				meta: {
					next: [await this._resolveEntry(existing.context.head)],
					type: EntryType.CUT,
					...options?.meta
				}
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
			.sort(this.log.log.sortFn)
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
			removed: []
		};

		for (const [_key, entries] of visited) {
			try {
				const item = entries[0];
				const payload =
					item._payload instanceof DecryptedThing
						? item.payload.getValue(item.encoding)
						: await item.getPayloadValue();
				if (payload instanceof PutOperation && !removedSet.has(item.hash)) {
					const key = payload.key;

					let value = this.deserializeOrPass(payload);

					// Program specific
					if (value instanceof Program) {
						// if replicator, then open
						if (
							(await this.canOpen!(value, item)) &&
							this.log.role instanceof Replicator &&
							(await this.log.replicator(item)) // TODO types, throw runtime error if replicator is not provided
						) {
							value = (await this.node.open(value, {
								parent: this as Program<any, any>,
								existing: "reuse"
							})) as any as T; // TODO types
						}
					}
					documentsChanged.added.push(value);

					const context = new Context({
						created:
							this._index.index.get(key)?.context.created ||
							item.meta.clock.timestamp.wallTime,
						modified: item.meta.clock.timestamp.wallTime,
						head: item.hash
					});

					const valueToIndex = this._index.toIndex(value, context);
					this._index.index.set(key, {
						key: payload.key,
						value: isPromise(valueToIndex) ? await valueToIndex : valueToIndex,
						context,
						reference:
							valueToIndex === value || value instanceof Program
								? value
								: undefined
					});
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
						await value.drop(this);
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

function isPromise(value) {
	return Boolean(value && typeof value.then === "function");
}
