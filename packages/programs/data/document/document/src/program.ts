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
import { CustomEvent } from "@libp2p/interface";
import {
	RoleOptions,
	Observer,
	Replicator,
	SharedLog,
	SharedLogOptions,
	SharedAppendOptions
} from "@peerbit/shared-log";
import * as types from "@peerbit/document-interface";

export type { RoleOptions }; // For convenience (so that consumers does not have to do the import above from shared-log packages)

import {
	IndexableFields,
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	DocumentIndex,
	Operation,
	PutOperation,
	CanSearch,
	CanRead
} from "./search.js";
import { MAX_BATCH_SIZE } from "./constants.js";

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

type MaybePromise<T> = Promise<T> | T;

type CanPerformPut<T> = {
	type: "put";
	value: T;
	operation: PutOperation;
	entry: Entry<PutOperation>;
};

type CanPerformDelete<T> = {
	type: "delete";
	operation: DeleteOperation;
	entry: Entry<DeleteOperation>;
};

export type CanPerformOperations<T> = CanPerformPut<T> | CanPerformDelete<T>;
export type CanPerform<T> = (
	properties: CanPerformOperations<T>
) => MaybePromise<boolean>;

export type SetupOptions<T> = {
	type: AbstractType<T>;
	canOpen?: (program: T) => MaybePromise<boolean>;
	canPerform?: CanPerform<T>;
	id?: (obj: any) => types.IdPrimitive;
	index?: {
		idProperty?: string | string[];
		canSearch?: CanSearch;
		canRead?: CanRead<T>;
		fields?: IndexableFields<T>;
	};
	log?: {
		trim?: TrimOptions;
	};
} & SharedLogOptions<Operation>;

@variant("documents")
export class Documents<T> extends Program<
	SetupOptions<T>,
	DocumentEvents<T> & ProgramEvents
> {
	@field({ type: SharedLog })
	log: SharedLog<Operation>;

	@field({ type: "bool" })
	immutable: boolean; // "Can I overwrite a document?"

	@field({ type: DocumentIndex })
	private _index: DocumentIndex<T>;

	private _clazz: AbstractType<T>;

	private _optionCanPerform?: CanPerform<T>;
	private _manuallySynced: Set<string>;
	private idResolver: (any: any) => types.IdPrimitive;

	canOpen?: (program: T, entry: Entry<Operation>) => Promise<boolean> | boolean;

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
		this._manuallySynced = new Set();
		const idProperty = options.index?.idProperty || "id";
		const idResolver =
			options.id ||
			(typeof idProperty === "string"
				? (obj) => obj[idProperty as string]
				: (obj: any) => types.extractFieldValue(obj, idProperty as string[]));

		this.idResolver = idResolver;

		let transform: IndexableFields<T>;
		if (options.index?.fields) {
			if (typeof options.index.fields === "function") {
				transform = options.index.fields;
			} else {
				transform = options.index.fields;
			}
		} else {
			transform = (obj) => obj as Record<string, any>; // TODO check types
		}
		await this._index.open({
			type: this._clazz,
			log: this.log,
			canRead: options?.index?.canRead,
			canSearch: options.index?.canSearch,
			fields: transform,
			indexBy: idProperty,
			sync: async (result: types.Results<T>) => {
				// here we arrive for all the results we want to persist.
				// we we need to do here is
				// 1. add the entry to a list of entries that we should persist through prunes
				let heads: string[] = [];
				for (const entry of result.results) {
					this._manuallySynced.add(entry.context.gid);
					heads.push(entry.context.head);
				}
				return this.log.log.join(heads);
			},
			dbType: this.constructor
		});

		await this.log.open({
			encoding: BORSH_ENCODING_OPERATION,
			canReplicate: options?.canReplicate,
			canAppend: this.canAppend.bind(this),
			onChange: this.handleChanges.bind(this),
			trim: options?.log?.trim,
			role: options?.role,
			replicas: options?.replicas,
			sync: (entry) => {
				// here we arrive when ever a insertion/pruning behaviour processes an entry
				// returning true means that it should persist
				return this._manuallySynced.has(entry.gid);
			}
		});
	}

	async recover() {
		return this.log.recover();
	}

	private async _resolveEntry(history: Entry<Operation> | string) {
		return typeof history === "string"
			? (await this.log.log.get(history)) ||
					(await Entry.fromMultihash<Operation>(this.log.log.blocks, history))
			: history;
	}

	async updateRole(role: RoleOptions) {
		await this.log.updateRole(role);
	}
	get role(): Replicator | Observer {
		return this.log.role;
	}

	async canAppend(
		entry: Entry<Operation>,
		reference?: { document: T; operation: PutOperation }
	): Promise<boolean> {
		const l0 = await this._canAppend(entry as Entry<Operation>, reference);
		if (!l0) {
			return false;
		}

		try {
			let operation: PutOperation | DeleteOperation = l0;
			let document: T | undefined = reference?.document;
			if (!document) {
				if (l0 instanceof PutOperation) {
					document = this._index.valueEncoding.decoder(l0.data);
					if (!document) {
						return false;
					}
				} else if (l0 instanceof DeleteOperation) {
					// Nothing to do here by default
					// checking if the document exists is not necessary
					// since it might already be deleted
				} else {
					throw new Error("Unsupported operation");
				}
			}

			if (this._optionCanPerform) {
				if (
					!(await this._optionCanPerform(
						operation instanceof PutOperation
							? {
									type: "put",
									value: document!,
									operation,
									entry: entry as any as Entry<PutOperation>
								}
							: {
									type: "delete",
									operation,
									entry: entry as any as Entry<DeleteOperation>
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

	async _canAppend(
		entry: Entry<Operation>,
		reference?: { document: T; operation: PutOperation }
	): Promise<PutOperation | DeleteOperation | false> {
		const resolve = async (history: Entry<Operation> | string) => {
			return typeof history === "string"
				? this.log.log.get(history) ||
						(await Entry.fromMultihash(this.log.log.blocks, history))
				: history;
		};
		const pointsToHistory = async (history: Entry<Operation> | string) => {
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
				keychain: this.node.services.keychain
			});
			const operation =
				reference?.operation || entry._payload instanceof DecryptedThing
					? entry.payload.getValue(entry.encoding)
					: await entry.getPayloadValue();
			if (operation instanceof PutOperation) {
				// check nexts
				const putOperation = operation as PutOperation;
				let value =
					reference?.document ??
					this.index.valueEncoding.decoder(putOperation.data);
				const keyValue = this.idResolver(value);

				const key = types.toId(keyValue);

				const existingDocument = await this.index.engine.get(key);
				if (existingDocument && existingDocument.context.head !== entry.hash) {
					//  econd condition can false if we reset the operation log, while not  resetting the index. For example when doing .recover
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
					const referenceHistoryCorrectly = await pointsToHistory(doc);
					return referenceHistoryCorrectly ? putOperation : false;
				} else {
					if (entry.next.length !== 0) {
						return false;
					}
				}
			} else if (operation instanceof DeleteOperation) {
				if (entry.next.length !== 1) {
					return false;
				}
				const existingDocument = await this._index.engine.get(operation.key);
				if (!existingDocument) {
					// already deleted
					return operation; // assume ok
				}
				let doc = await this.log.log.get(existingDocument.context.head);
				if (!doc) {
					logger.error("Failed to find Document from head");
					return false;
				}
				if (await pointsToHistory(doc)) {
					// references the existing document
					return operation;
				}
				return false;
			} else {
				throw new Error("Unsupported operation");
			}

			return operation;
		} catch (error) {
			if (error instanceof AccessError) {
				return false; // we cant index because we can not decrypt
			} else if (error instanceof BorshError) {
				logger.warn("Received payload that could not be decoded, skipping");
				return false;
			}
			throw error;
		}
	}

	public async put(
		doc: T,
		options?: SharedAppendOptions<Operation> & { unique?: boolean }
	) {
		const keyValue = this.idResolver(doc);

		// type check the key
		types.checkId(keyValue);

		const ser = serialize(doc);
		if (ser.length > MAX_BATCH_SIZE) {
			throw new Error(
				`Document is too large (${
					ser.length * 1e-6
				}) mb). Needs to be less than ${MAX_BATCH_SIZE * 1e-6} mb`
			);
		}

		const existingDocument = options?.unique
			? undefined
			: (
					await this._index.getDetailed(keyValue, {
						local: true,
						remote: { sync: true } // only query remote if we know they exist
					})
				)?.[0]?.results[0];

		const operation = new PutOperation({
			data: ser
		});
		const appended = await this.log.append(operation, {
			...options,
			meta: {
				next: existingDocument
					? [await this._resolveEntry(existingDocument.context.head)]
					: [],
				...options?.meta
			},
			canAppend: (entry) => {
				return this.canAppend(entry, { document: doc, operation });
			},
			onChange: (change) => {
				return this.handleChanges(change, { document: doc, operation });
			}
		});

		return appended;
	}

	async del(id: types.Ideable, options?: SharedAppendOptions<Operation>) {
		const key = types.toId(id);
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
				key
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

	async handleChanges(
		change: Change<Operation>,
		reference?: { document: T; operation: PutOperation }
	): Promise<void> {
		const isAppendOperation =
			change?.added.length === 1 ? !!change.added[0] : false;

		const removed = [...(change.removed || [])];
		const removedSet = new Map<string, Entry<Operation>>();
		for (const r of removed) {
			removedSet.set(r.hash, r);
		}
		const sortedEntries = [...change.added, ...(removed || [])]
			.sort(this.log.log.sortFn)
			.reverse(); // sort so we get newest to oldest

		// There might be a case where change.added and change.removed contains the same document id. Usaully because you use the "trim" option
		// in combination with inserting the same document. To mitigate this, we loop through the changes and modify the behaviour for this

		let documentsChanged: DocumentsChange<T> = {
			added: [],
			removed: []
		};

		let modified: Set<string | number | bigint> = new Set();
		for (const item of sortedEntries) {
			try {
				const payload =
					item._payload instanceof DecryptedThing
						? item.payload.getValue(item.encoding)
						: await item.getPayloadValue();

				if (payload instanceof PutOperation && !removedSet.has(item.hash)) {
					let value =
						(isAppendOperation &&
							reference?.operation === payload &&
							reference?.document) ||
						this.index.valueEncoding.decoder(payload.data);

					// get index key from value
					const keyObject = this.idResolver(value);

					const key = types.toId(keyObject);

					// document is already updated with more recent entry
					if (modified.has(key.primitive)) {
						continue;
					}

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
					this._index.put(value, item, key);
					modified.add(key.primitive);
				} else if (
					(payload instanceof DeleteOperation && !removedSet.has(item.hash)) ||
					payload instanceof PutOperation ||
					removedSet.has(item.hash)
				) {
					this._manuallySynced.delete(item.gid);

					let value: T;
					let key: string | number | bigint;

					if (payload instanceof PutOperation) {
						value = this.index.valueEncoding.decoder(payload.data);
						key = types.toIdeable(this.idResolver(value));
						// document is already updated with more recent entry
						if (modified.has(key)) {
							continue;
						}
					} else if (payload instanceof DeleteOperation) {
						key = payload.key.primitive;
						// document is already updated with more recent entry
						if (modified.has(key)) {
							continue;
						}
						const document = await this._index.get(key, {
							local: true,
							remote: false
						});
						if (!document) {
							continue;
						}
						value = document;
					} else {
						throw new Error("Unexpected");
					}

					documentsChanged.removed.push(value);

					if (value instanceof Program) {
						await value.drop(this);
					}

					// update index
					this._index.del(key);

					modified.add(key);
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
}
