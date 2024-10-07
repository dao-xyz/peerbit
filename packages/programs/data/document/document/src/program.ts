import {
	type AbstractType,
	BorshError,
	field,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { AccessError } from "@peerbit/crypto";
import * as documentsTypes from "@peerbit/document-interface";
import * as indexerTypes from "@peerbit/indexer-interface";
import {
	type Change,
	Entry,
	EntryType,
	type ShallowOrFullEntry,
	type TrimOptions,
} from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { Program, type ProgramEvents } from "@peerbit/program";
import {
	type ReplicationDomain,
	type SharedAppendOptions,
	SharedLog,
	type SharedLogOptions,
} from "@peerbit/shared-log";
import { MAX_BATCH_SIZE } from "./constants.js";
import {
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	type Operation,
	PutOperation,
	PutWithKeyOperation,
	coerceDeleteOperation,
	isDeleteOperation,
	isPutOperation,
} from "./operation.js";
import {
	type CanRead,
	type CanSearch,
	DocumentIndex,
	type TransformOptions,
} from "./search.js";

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
	properties: CanPerformOperations<T>,
) => MaybePromise<boolean>;

export type SetupOptions<
	T,
	I = T,
	D extends ReplicationDomain<any, Operation> = any,
> = {
	type: AbstractType<T>;
	canOpen?: (program: T) => MaybePromise<boolean>;
	canPerform?: CanPerform<T>;
	id?: (obj: any) => indexerTypes.IdPrimitive;
	index?: {
		canSearch?: CanSearch;
		canRead?: CanRead<T>;
		idProperty?: string | string[];
	} & TransformOptions<T, I>;
	log?: {
		trim?: TrimOptions;
	};
	compatibility?: 6;
} & Exclude<SharedLogOptions<Operation, D>, "compatibility">;

export type ExtractArgs<T> =
	T extends ReplicationDomain<infer Args, any> ? Args : never;

@variant("documents")
export class Documents<
	T,
	I extends Record<string, any> = T extends Record<string, any> ? T : any,
	D extends ReplicationDomain<any, Operation> = any,
> extends Program<SetupOptions<T, I, D>, DocumentEvents<T> & ProgramEvents> {
	@field({ type: SharedLog })
	log: SharedLog<Operation, D>;

	@field({ type: "bool" })
	immutable: boolean; // "Can I overwrite a document?"

	@field({ type: DocumentIndex })
	private _index: DocumentIndex<T, I, D>;

	private _clazz!: AbstractType<T>;

	private _optionCanPerform?: CanPerform<T>;
	private idResolver!: (any: any) => indexerTypes.IdPrimitive;

	canOpen?: (program: T, entry: Entry<Operation>) => Promise<boolean> | boolean;

	compatibility: 6 | undefined;

	constructor(properties?: {
		id?: Uint8Array;
		immutable?: boolean;
		index?: DocumentIndex<T, I, ExtractArgs<D>>;
	}) {
		super();

		this.log = new SharedLog(properties);
		this.immutable = properties?.immutable ?? false;
		this._index = properties?.index || new DocumentIndex();
	}

	get index(): DocumentIndex<T, I, D> {
		return this._index;
	}

	async open(options: SetupOptions<T, I, D>) {
		this._clazz = options.type;
		this.canOpen = options.canOpen;

		/* eslint-disable */
		if (Program.isPrototypeOf(this._clazz)) {
			if (!this.canOpen) {
				throw new Error(
					"Document store needs to be opened with canOpen option when the document type is a Program",
				);
			}
		}

		this._optionCanPerform = options.canPerform;
		const idProperty = options.index?.idProperty || "id";
		const idResolver =
			options.id ||
			(typeof idProperty === "string"
				? (obj: any) => obj[idProperty as string]
				: (obj: any) =>
						indexerTypes.extractFieldValue(obj, idProperty as string[]));

		this.idResolver = idResolver;
		this.compatibility = options.compatibility;

		await this._index.open({
			log: this.log,
			canRead: options?.index?.canRead,
			canSearch: options.index?.canSearch,
			documentType: this._clazz,
			transform: options.index,
			indexBy: idProperty,
			sync: async (result: documentsTypes.Results<T>) => {
				// here we arrive for all the results we want to persist.
				// we we need to do here is
				// 1. add the entry to a list of entries that we should persist through prunes
				await this.log.join(
					result.results.map((x) => x.context.head),
					{ replicate: true },
				);
			},
			dbType: this.constructor,
		});

		await this.log.open({
			encoding: BORSH_ENCODING_OPERATION,
			canReplicate: options?.canReplicate,
			canAppend: this.canAppend.bind(this),
			onChange: this.handleChanges.bind(this),
			trim: options?.log?.trim,
			replicate: options?.replicate,
			replicas: options?.replicas,
			domain: options?.domain,

			// document v6 and below need log compatibility of v8 or below
			compatibility:
				(options?.compatibility ?? Number.MAX_SAFE_INTEGER < 7) ? 8 : undefined,
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

	async canAppend(
		entry: Entry<Operation>,
		reference?: { document: T; operation: PutOperation },
	): Promise<boolean> {
		const l0 = await this._canAppend(entry as Entry<Operation>, reference);
		if (!l0) {
			return false;
		}

		try {
			let operation: PutOperation | DeleteOperation = l0;
			let document: T | undefined = reference?.document;
			if (!document) {
				if (isPutOperation(l0)) {
					document = this._index.valueEncoding.decoder(l0.data);
					if (!document) {
						return false;
					}
				} else if (isDeleteOperation(l0)) {
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
						isPutOperation(operation)
							? {
									type: "put",
									value: document!,
									operation,
									entry: entry as any as Entry<PutOperation>,
								}
							: {
									type: "delete",
									operation,
									entry: entry as any as Entry<DeleteOperation>,
								},
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
		reference?: { document: T; operation: PutOperation },
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

			const next = entry.meta.next[0];
			while (
				current?.hash &&
				next !== current?.hash &&
				current.meta.next.length > 0
			) {
				current = await this.log.log.get(current.meta.next[0])!;
			}
			if (current?.hash === next) {
				return true; // Ok, we are pointing this new edit to some exising point in time of the old document
			}
			return false;
		};

		try {
			entry.init({
				encoding: this.log.log.encoding,
				keychain: this.node.services.keychain,
			});
			const operation =
				reference?.operation ||
				/* entry._payload instanceof DecryptedThing
					? entry.payload.getValue(entry.encoding)
					:  */ (await entry.getPayloadValue()); // TODO implement sync api for resolving entries that does not deep decryption
			if (isPutOperation(operation)) {
				// check nexts
				const putOperation = operation as PutOperation;
				let value =
					reference?.document ??
					this.index.valueEncoding.decoder(putOperation.data);
				const keyValue = this.idResolver(value);

				const key = indexerTypes.toId(keyValue);

				const existingDocument = (
					await this.index.getDetailed(key, {
						local: true,
						remote: this.immutable,
					})
				)?.[0]?.results[0];
				if (existingDocument && existingDocument.context.head !== entry.hash) {
					//  econd condition can false if we reset the operation log, while not  resetting the index. For example when doing .recover
					if (this.immutable) {
						// key already exist but pick the oldest entry
						// this is because we can not overwrite same id if immutable
						if (
							existingDocument.context.created <
							entry.meta.clock.timestamp.wallTime
						) {
							return false;
						}

						if (entry.meta.next.length > 0) {
							return false; // can not append to immutable document
						}

						return putOperation;
					} else {
						if (entry.meta.next.length !== 1) {
							return false;
						}

						const prevEntry = await this.log.log.entryIndex.get(
							existingDocument.context.head,
						);
						if (!prevEntry) {
							logger.error(
								"Failed to find previous entry for document edit: " +
									entry.hash,
							);
							return false;
						}
						const referenceHistoryCorrectly = await pointsToHistory(prevEntry);
						return referenceHistoryCorrectly ? putOperation : false;
					}
				} else {
					if (entry.meta.next.length !== 0) {
						return false;
					}
				}
			} else if (isDeleteOperation(operation)) {
				if (entry.meta.next.length !== 1) {
					return false;
				}
				const existingDocument = (
					await this.index.getDetailed(operation.key, {
						local: true,
						remote: this.immutable,
					})
				)?.[0]?.results[0];

				if (!existingDocument) {
					// already deleted
					return coerceDeleteOperation(operation); // assume ok
				}
				let doc = await this.log.log.get(existingDocument.context.head);
				if (!doc) {
					logger.error("Failed to find Document from head");
					return false;
				}
				if (await pointsToHistory(doc)) {
					// references the existing document
					return coerceDeleteOperation(operation);
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
		options?: SharedAppendOptions<Operation> & {
			unique?: boolean;
			replicate?: boolean;
		},
	) {
		const keyValue = this.idResolver(doc);

		// type check the key
		indexerTypes.checkId(keyValue);

		const ser = serialize(doc);
		if (ser.length > MAX_BATCH_SIZE) {
			throw new Error(
				`Document is too large (${
					ser.length * 1e-6
				}) mb). Needs to be less than ${MAX_BATCH_SIZE * 1e-6} mb`,
			);
		}

		const existingDocument = options?.unique
			? undefined
			: (
					await this._index.getDetailed(keyValue, {
						local: true,
						remote: { replicate: options?.replicate }, // only query remote if we know they exist
					})
				)?.[0]?.results[0];

		let operation: PutOperation | PutWithKeyOperation;
		if (this.compatibility === 6) {
			if (typeof keyValue === "string") {
				operation = new PutWithKeyOperation({
					key: keyValue,
					data: ser,
				});
			} else {
				throw new Error("Key must be a string in compatibility mode v6");
			}
		} else {
			operation = new PutOperation({
				data: ser,
			});
		}

		const appended = await this.log.append(operation, {
			...options,
			meta: {
				next: existingDocument
					? [await this._resolveEntry(existingDocument.context.head)]
					: [],
				...options?.meta,
			},
			canAppend: (entry) => {
				return this.canAppend(entry, { document: doc, operation });
			},
			onChange: (change) => {
				return this.handleChanges(change, { document: doc, operation });
			},
			replicate: options?.replicate,
		});

		return appended;
	}

	async del(
		id: indexerTypes.Ideable,
		options?: SharedAppendOptions<Operation>,
	) {
		const key = indexerTypes.toId(id);
		const existing = (
			await this._index.getDetailed(key, {
				local: true,
				remote: { replicate: options?.replicate },
			})
		)?.[0]?.results[0];

		if (!existing) {
			throw new Error(`No entry with key '${key.primitive}' in the database`);
		}

		const entry = await this._resolveEntry(existing.context.head);
		return this.log.append(
			new DeleteOperation({
				key,
			}),
			{
				...options,
				meta: {
					next: [entry],
					type: EntryType.CUT,
					...options?.meta,
				},
			}, //
		);
	}

	async handleChanges(
		change: Change<Operation>,
		reference?: { document: T; operation: PutOperation },
	): Promise<void> {
		const isAppendOperation =
			change?.added.length === 1 ? !!change.added[0] : false;
		const removedSet = new Map<string, ShallowOrFullEntry<Operation>>();

		for (const r of change.removed) {
			removedSet.set(r.hash, r);
		}

		const sortedEntries = [
			...change.added.map((x) => x.entry),
			...((await Promise.all(
				change.removed.map((x) =>
					x instanceof Entry ? x : this.log.log.entryIndex.get(x.hash),
				),
			)) || []),
		]; // TODO assert sorting
		/*  const sortedEntries = [...change.added, ...(removed || [])]
					.sort(this.log.log.sortFn)
					.reverse(); // sort so we get newest to oldest */

		// There might be a case where change.added and change.removed contains the same document id. Usaully because you use the "trim" option
		// in combination with inserting the same document. To mitigate this, we loop through the changes and modify the behaviour for this

		let documentsChanged: DocumentsChange<T> = {
			added: [],
			removed: [],
		};

		let modified: Set<string | number | bigint> = new Set();
		for (const item of sortedEntries) {
			if (!item) continue;

			try {
				const payload =
					/* item._payload instanceof DecryptedThing
						? item.payload.getValue(item.encoding)
						:  */ await item.getPayloadValue(); // TODO implement sync api for resolving entries that does not deep decryption

				if (isPutOperation(payload) && !removedSet.has(item.hash)) {
					let value =
						(isAppendOperation &&
							reference?.operation === payload &&
							reference?.document) ||
						this.index.valueEncoding.decoder(payload.data);

					// get index key from value
					const keyObject = this.idResolver(value);
					const key = indexerTypes.toId(keyObject);

					// document is already updated with more recent entry
					if (modified.has(key.primitive)) {
						continue;
					}

					// Program specific
					if (value instanceof Program) {
						// if replicator, then open
						if (
							(await this.canOpen!(value, item)) &&
							(await this.log.isReplicator(item)) // TODO types, throw runtime error if replicator is not provided
						) {
							value = (await this.node.open(value, {
								parent: this as Program<any, any>,
								existing: "reuse",
							})) as any as T; // TODO types
						}
					}
					documentsChanged.added.push(value);
					await this._index.put(value, item, key);
					modified.add(key.primitive);
				} else if (
					(isDeleteOperation(payload) && !removedSet.has(item.hash)) ||
					isPutOperation(payload) ||
					removedSet.has(item.hash)
				) {
					let value: T;
					let key: indexerTypes.IdKey;

					if (isPutOperation(payload)) {
						value = this.index.valueEncoding.decoder(payload.data);
						key = indexerTypes.toId(this.idResolver(value));
						// document is already updated with more recent entry
						if (modified.has(key.primitive)) {
							continue;
						}
					} else if (isDeleteOperation(payload)) {
						key = coerceDeleteOperation(payload).key;
						// document is already updated with more recent entry
						if (modified.has(key.primitive)) {
							continue;
						}
						const document = await this._index.get(key, {
							local: true,
							remote: false,
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
					await this._index.del(key);
					modified.add(key.primitive);
				} else {
					// Unknown operation
					throw new OperationError("Unknown operation");
				}
			} catch (error) {
				if (error instanceof AccessError) {
					continue;
				}
				throw error;
			}
		}

		this.events.dispatchEvent(
			new CustomEvent("change", { detail: documentsChanged }),
		);
	}
}
