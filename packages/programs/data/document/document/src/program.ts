import {
	type AbstractType,
	BorshError,
	field,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { AccessError, SignatureWithKey } from "@peerbit/crypto";
import {
	Context,
	NotFoundError,
	type ResultIndexedValue,
} from "@peerbit/document-interface";
import {
	encodeContextSuffix as encodeNativeContextSuffix,
	encodeContextSuffixBatch as encodeNativeContextSuffixBatch,
} from "@peerbit/document-rust";
import type { QueryCacheOptions } from "@peerbit/indexer-cache";
import * as indexerTypes from "@peerbit/indexer-interface";
import {
	type Change,
	Entry,
	EntryType,
	LamportClock,
	ShallowEntry,
	ShallowMeta,
	type ShallowOrFullEntry,
	Timestamp,
	type TrimOptions,
} from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { Program, type ProgramEvents } from "@peerbit/program";
import {
	type EntryReplicated,
	type ReplicationDomain,
	type SharedAppendOptions,
	SharedLog,
	type SharedLogOptions,
} from "@peerbit/shared-log";
import { MAX_BATCH_SIZE } from "./constants.js";
import type { CustomDocumentDomain } from "./domain.js";
import type { DocumentEvents, DocumentsChange } from "./events.js";
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
import { isResultIndexedValue } from "./result-shape.js";
import {
	type CanRead,
	type CanSearch,
	DocumentIndex,
	type GetOptions,
	INDEX_CONTEXT_SHAPE,
	type IndexedContextOnly,
	type PrefetchOptions,
	type ReachScope,
	type TransformOptions,
	type WithContext,
	type WithIndexedContext,
	coerceWithContext,
	coerceWithIndexed,
} from "./search.js";

const logger = loggerFn("peerbit:program:document");
const warn = logger.newScope("warn");

export class OperationError extends Error {
	constructor(message?: string) {
		super(message);
	}
}

type MaybePromise<T> = Promise<T> | T;

export type CountEstimate = {
	estimate: number;
	/**
	 * Relative error margin (0..1), where e.g. `0.1` means ~±10%.
	 *
	 * Only non-`undefined` when the replication domain is expected to be uniformly
	 * distributed (currently: `domain.type === "hash"`), and when there is
	 * sufficient local sample information to compute it.
	 *
	 * When `undefined`, the caller should treat `estimate` as unreliable.
	 */
	errorMargin: number | undefined;
};

type CanPerformPut<T> = {
	type: "put";
	value: T;
	operation: PutOperation;
	entry: Entry<PutOperation>;
};

type CanPerformDelete = {
	type: "delete";
	operation: DeleteOperation;
	entry: Entry<DeleteOperation>;
};

export type CanPerformOperations<T> = CanPerformPut<T> | CanPerformDelete;
export type CanPerform<T> = (
	properties: CanPerformOperations<T>,
) => MaybePromise<boolean>;

const PUT_OPERATION_PREFIX_LENGTH = 6;
const encodePutOperationPayload = (data: Uint8Array): Uint8Array => {
	const encoded = new Uint8Array(PUT_OPERATION_PREFIX_LENGTH + data.byteLength);
	encoded[0] = 0;
	encoded[1] = 3;
	const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
	view.setUint32(2, data.byteLength, true);
	encoded.set(data, PUT_OPERATION_PREFIX_LENGTH);
	return encoded;
};

type PutChangeReference<T, I extends Record<string, any>> = {
	document: T;
	operation: PutOperation | PutWithKeyOperation;
	key: indexerTypes.IdKey;
	unique?: boolean;
	existing?:
		| indexerTypes.IndexedResult<IndexedContextOnly<I>>
		| null
		| undefined;
};

type DocumentPutOptions = SharedAppendOptions<Operation> & {
	unique?: boolean;
	replicate?: boolean;
	checkRemote?: boolean;
};

type PreparedPut<T> = {
	document: T;
	encodedDocument: Uint8Array;
	encodedOperation?: Uint8Array;
	keyValue: indexerTypes.IdPrimitive;
	key: indexerTypes.IdKey;
	operation: PutOperation | PutWithKeyOperation;
};

type PreparedPlainPut<T> = {
	document: T;
	encodedDocument: Uint8Array;
	operationPayloadBytes: Uint8Array;
	keyValue: indexerTypes.IdPrimitive;
	key: indexerTypes.IdKey;
};

type PlainPutCommitPlan<T, I extends Record<string, any>> = {
	document: T;
	encodedDocument: Uint8Array;
	payloadData: Uint8Array;
	key: indexerTypes.IdKey;
	operation?: PutOperation;
	next: Entry<Operation>[] | ShallowEntry[];
	skipMissingNextJoin: boolean;
	resolveTrimmedEntries: boolean;
	useGenericChangeHandler: boolean;
	unique?: boolean;
	existing?:
		| indexerTypes.IndexedResult<IndexedContextOnly<I>>
		| null
		| undefined;
};

type LocalAppendCommitFacts = {
	hash: string;
	gid: string;
	wallTime: bigint;
	payloadSize: number;
};

type ContextualEncodedValueParts = {
	prefix: Uint8Array;
	suffix: Uint8Array;
};

type DocumentAppendCommitFacts<T, I extends Record<string, any>> = {
	document: T;
	key: indexerTypes.IdKey;
	operation?: PutOperation;
	encodedDocument: Uint8Array;
	operationPayloadBytes: Uint8Array;
	entry: Entry<Operation>;
	removed: ShallowOrFullEntry<Operation>[];
	append: LocalAppendCommitFacts;
	context: Context;
	contextBytes: Uint8Array;
	contextualEncodedValueParts: ContextualEncodedValueParts;
	unique?: boolean;
	existing?:
		| indexerTypes.IndexedResult<IndexedContextOnly<I>>
		| null
		| undefined;
};

type NativeDocumentAppendCommitFactsInput<
	T,
	I extends Record<string, any>,
> = {
	document: T;
	key: indexerTypes.IdKey;
	documentBytes: Uint8Array;
	operationPayloadBytes: Uint8Array;
	operation?: PutOperation;
	unique?: boolean;
	existing?:
		| indexerTypes.IndexedResult<IndexedContextOnly<I>>
		| null
		| undefined;
};

type NativeDocumentAppendCommitInput<
	T,
	I extends Record<string, any>,
> = NativeDocumentAppendCommitFactsInput<T, I> & {
	next: Entry<Operation>[] | ShallowEntry[];
	skipMissingNextJoin: boolean;
	resolveTrimmedEntries: boolean;
	options?: DocumentPutOptions;
};

type NativeDocumentAppendManyCommitInput<
	T,
	I extends Record<string, any>,
> = {
	puts: NativeDocumentAppendCommitFactsInput<T, I>[];
	resolveTrimmedEntries: boolean;
	options?: DocumentPutOptions;
};

type DocumentAppendManyCommitFacts<T, I extends Record<string, any>> = {
	entries: Entry<Operation>[];
	removed: ShallowOrFullEntry<Operation>[];
	commits: DocumentAppendCommitFacts<T, I>[];
};

type InferR<D> = D extends ReplicationDomain<any, any, infer I> ? I : "u32";

export type SetupOptions<
	T,
	I extends Record<string, any> = T extends Record<string, any> ? T : any,
	D extends ReplicationDomain<any, Operation, any> = any,
> = {
	type: AbstractType<T>;
	canOpen?: (program: T) => MaybePromise<boolean>;
	canPerform?: CanPerform<T>;
	strictHistory?: boolean;
	id?: (obj: any) => indexerTypes.IdPrimitive;
	index?: {
		canSearch?: CanSearch;
		canRead?: CanRead<I>;
		idProperty?: string | string[];
		cache?: {
			resolver?: number;
			query?: QueryCacheOptions;
		};
		prefetch?: boolean | Partial<PrefetchOptions>;
		includeIndexed?: boolean;
	} & TransformOptions<T, I>;
	log?: {
		trim?: TrimOptions;
	};
	compatibility?: 6 | 7;
	domain?: (db: Documents<T, I, D>) => CustomDocumentDomain<InferR<D>>;
	keep?:
		| ((
				entry: ShallowOrFullEntry<Operation> | EntryReplicated<InferR<D>>,
		  ) => Promise<boolean> | boolean)
		| "self";
} & Omit<
	SharedLogOptions<Operation, D, InferR<D>>,
	"compatibility" | "domain" | "keep"
>;

export type ExtractArgs<T> =
	T extends ReplicationDomain<infer Args, any, any> ? Args : never;

@variant("documents")
export class Documents<
	T,
	I extends Record<string, any> = T extends Record<string, any> ? T : any,
	D extends ReplicationDomain<any, Operation, any> = any,
> extends Program<SetupOptions<T, I, D>, DocumentEvents<T, I> & ProgramEvents> {
	@field({ type: SharedLog })
	log: SharedLog<Operation, D, InferR<D>>;

	@field({ type: "bool" })
	immutable: boolean; // "Can I overwrite a document?"

	@field({ type: DocumentIndex })
	private _index: DocumentIndex<T, I, D>;

	private _clazz!: AbstractType<T>;

	private _optionCanPerform?: CanPerform<T>;
	private idResolver!: (any: any) => indexerTypes.IdPrimitive;
	private domain?: CustomDocumentDomain<InferR<D>>;
	private strictHistory: boolean;
	canOpen?: (program: T) => Promise<boolean> | boolean;

	compatibility: 6 | 7 | undefined;

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

	private getLocalIndexedContext(
		key: indexerTypes.IdKey,
	): Promise<indexerTypes.IndexedResult<IndexedContextOnly<I>> | undefined> {
		return this._index.index.get(key, {
			shape: INDEX_CONTEXT_SHAPE,
		}) as Promise<
			indexerTypes.IndexedResult<IndexedContextOnly<I>> | undefined
		>;
	}

	private getExistingContext(
		existing:
			| ResultIndexedValue<WithContext<I>>
			| indexerTypes.IndexedResult<WithContext<I>>
			| indexerTypes.IndexedResult<IndexedContextOnly<I>>
			| null
			| undefined,
	):
		| indexerTypes.ReturnTypeFromShape<
				WithContext<I>,
				typeof INDEX_CONTEXT_SHAPE
		  >["__context"]
		| undefined {
		return isResultIndexedValue(existing)
			? existing.context
			: existing?.value.__context;
	}

	get changes() {
		return this.events;
	}

	private async maybeSubprogramOpen(value: T & Program): Promise<T & Program> {
		if (await this.canOpen!(value)) {
			return (await this.node.open(value, {
				parent: this as Program<any, any>,
				existing: "reuse",
			})) as any as T & Program; // TODO types
		}

		return value;
	}
	private keepCache: Set<string> | undefined = undefined;
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
		const idProperty =
			options.index?.idProperty ||
			indexerTypes.getIdProperty(this._clazz) ||
			"id";
		const idResolver =
			options.id ||
			(typeof idProperty === "string"
				? (obj: any) => obj[idProperty as string]
				: (obj: any) =>
						indexerTypes.extractFieldValue(obj, idProperty as string[]));

		this.idResolver = idResolver;
		this.compatibility = options.compatibility;
		this.strictHistory = options.strictHistory ?? false;

		await this._index.open({
			documentEvents: this.events,
			log: this.log,
			canRead: options?.index?.canRead,
			canSearch: options.index?.canSearch,
			documentType: this._clazz,
			transform: options.index,
			indexBy: idProperty,
			compatibility: options.compatibility,
			cache: options?.index?.cache,
			replicate: async (query, results) => {
				// here we arrive for all the results we want to persist.

				let mergeSegments = this.domain?.canProjectToOneSegment(query);
				await this.log.join(
					results.results
						.flat()
						.map((x) =>
							isResultIndexedValue(x) && x.entries.length > 0
								? x.entries[0]
								: x.context.head,
						),
					{ replicate: { assumeSynced: true, mergeSegments } },
				);
			},
			dbType: this.constructor,
			maybeOpen: this.maybeSubprogramOpen.bind(this),
			prefetch: options.index?.prefetch,
			includeIndexed: options.index?.includeIndexed,
		});

		// document v6 and below need log compatibility of v8 or below
		// document v7 needs log compatibility of v9
		let logCompatiblity: number | undefined = undefined;
		if (options.compatibility === 6) {
			logCompatiblity = 8;
		} else if (options.compatibility === 7) {
			logCompatiblity = 9;
		}

		this.domain = options.domain?.(this);

		let keepFunction:
			| ((
					entry: ShallowOrFullEntry<Operation> | EntryReplicated<InferR<D>>,
			  ) => Promise<boolean> | boolean)
			| undefined;
		if (options?.keep === "self") {
			this.keepCache = new Set();
			keepFunction = async (e) => {
				if (this.keepCache?.has(e.hash)) {
					return true;
				}
				let signatures: SignatureWithKey[] | undefined = undefined;
				if (e instanceof Entry) {
					signatures = e.signatures;
				} else {
					const entry = await this.log.log.get(e.hash);
					signatures = entry?.signatures;
				}

				if (!signatures) {
					return false;
				}

				for (const signature of signatures) {
					if (signature.publicKey.equals(this.node.identity.publicKey)) {
						this.keepCache?.add(e.hash);
						return true;
					}
				}
				return false; // TODO also cache this?
			};
		} else {
			keepFunction = options?.keep;
		}

		await this.log.open({
			encoding: BORSH_ENCODING_OPERATION,
			canReplicate: options?.canReplicate,
			canAppend: this.canAppend.bind(this),
			onChange: this.handleChanges.bind(this),
			trim: options?.log?.trim,
			replicate: options?.replicate,
			replicas: options?.replicas,
			respondToIHaveTimeout: options?.respondToIHaveTimeout,
			sync: options?.sync,
			syncronizer: options?.syncronizer,
			timeUntilRoleMaturity: options?.timeUntilRoleMaturity,
			waitForReplicatorTimeout: options?.waitForReplicatorTimeout,
			waitForPruneDelay: options?.waitForPruneDelay,
			distributionDebounceTime: options?.distributionDebounceTime,
			strictFullReplicaFallback: false,
			domain: (options?.domain
				? (log: any) => options.domain!(this)
				: undefined) as any, /// TODO types,
			compatibility: logCompatiblity,
			eagerBlocks: options?.eagerBlocks,
			fanout: options?.fanout,
			keep: keepFunction,
		});
	}

	async recover() {
		return this.log.recover();
	}

	private async _resolveEntry(
		history: Entry<Operation> | string,
		options?: {
			remote?:
				| {
						timeout?: number;
						replicate?: boolean;
				  }
				| boolean;
		},
	) {
		return typeof history === "string"
			? (await this.log.log.get(history, options)) ||
					(await Entry.fromMultihash<Operation>(
						this.log.log.blocks,
						history,
						options,
					))
			: history;
	}

	protected async canAppend(
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
				warn("Received payload that could not be decoded, skipping");
				return false;
			}
			throw error;
		}

		return true;
	}

	protected async _canAppend(
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

				const existingDocument = this.immutable
					? (
							await this.index.getDetailed(key, {
								resolve: false,
								local: true,
								remote: { strategy: "fallback" },
							})
						)?.[0]?.results[0]
					: await this.getLocalIndexedContext(key);
				const existingContext = this.getExistingContext(existingDocument);
				if (existingContext && existingContext.head !== entry.hash) {
					//  econd condition can false if we reset the operation log, while not  resetting the index. For example when doing .recover
					if (this.immutable) {
						// key already exist but pick the oldest entry
						// this is because we can not overwrite same id if immutable
						if (existingContext.created < entry.meta.clock.timestamp.wallTime) {
							return false;
						}

						if (entry.meta.next.length > 0) {
							return false; // can not append to immutable document
						}

						return putOperation;
					} else {
						if (this.strictHistory) {
							// make sure that the next pointer exist and points to the existing documents
							if (entry.meta.next.length !== 1) {
								return false;
							}

							const prevEntry = await this.log.log.entryIndex.get(
								existingContext.head,
							);
							if (!prevEntry) {
								logger.error(
									"Failed to find previous entry for document edit: " +
										entry.hash,
								);
								return false;
							}
							const referenceHistoryCorrectly =
								await pointsToHistory(prevEntry);
							return referenceHistoryCorrectly ? putOperation : false;
						} else {
							return putOperation;
						}
					}
				} else {
					// TODO should re reject next pointers to other documents?
					// like if (entry.meta.next.length > 0) { return false; }
					// for now the default behaviour will allow us to build document dependencies
				}
			} else if (isDeleteOperation(operation)) {
				if (entry.meta.next.length !== 1) {
					return false;
				}
				const existingDocument = this.immutable
					? (
							await this.index.getDetailed(operation.key, {
								resolve: false,
								local: true,
								remote: true,
							})
						)?.[0]?.results[0]
					: await this.getLocalIndexedContext(
							operation.key instanceof indexerTypes.IdKey
								? operation.key
								: indexerTypes.toId(operation.key),
						);
				const existingHead = this.getExistingContext(existingDocument)?.head;

				if (!existingHead) {
					// already deleted
					return coerceDeleteOperation(operation); // assume ok
				}
				let doc = await this.log.log.get(existingHead);
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
				warn("Received payload that could not be decoded, skipping");
				return false;
			}
			throw error;
		}
	}

	private preparePut(doc: T): PreparedPut<T> {
		const keyValue = this.idResolver(doc);
		indexerTypes.checkId(keyValue);
		let encodedDocument = serialize(doc);
		if (encodedDocument.length > MAX_BATCH_SIZE) {
			throw new Error(
				`Document is too large (${
					encodedDocument.length * 1e-6
				}) mb). Needs to be less than ${MAX_BATCH_SIZE * 1e-6} mb`,
			);
		}

		const key = indexerTypes.toId(keyValue);
		let operation: PutOperation | PutWithKeyOperation;
		let encodedOperation: Uint8Array | undefined;
		if (this.compatibility === 6) {
			if (typeof keyValue === "string") {
				operation = new PutWithKeyOperation({
					key: keyValue,
					data: encodedDocument,
				});
			} else {
				throw new Error("Key must be a string in compatibility mode v6");
			}
		} else {
			encodedOperation = encodePutOperationPayload(encodedDocument);
			encodedDocument = encodedOperation.subarray(PUT_OPERATION_PREFIX_LENGTH);
			operation = new PutOperation({
				data: encodedDocument,
			});
		}
		return {
			document: doc,
			encodedDocument,
			encodedOperation,
			keyValue,
			key,
			operation,
		};
	}

	private preparePlainPut(doc: T): PreparedPlainPut<T> {
		if (this.compatibility === 6) {
			throw new Error("Plain put preparation is not supported in v6 mode");
		}
		const keyValue = this.idResolver(doc);
		indexerTypes.checkId(keyValue);
		const documentBytes = serialize(doc);
		if (documentBytes.length > MAX_BATCH_SIZE) {
			throw new Error(
				`Document is too large (${
					documentBytes.length * 1e-6
				}) mb). Needs to be less than ${MAX_BATCH_SIZE * 1e-6} mb`,
			);
		}
		const operationPayloadBytes = encodePutOperationPayload(documentBytes);
		return {
			document: doc,
			encodedDocument: operationPayloadBytes.subarray(
				PUT_OPERATION_PREFIX_LENGTH,
			),
			operationPayloadBytes,
			keyValue,
			key: indexerTypes.toId(keyValue),
		};
	}

	public async put(doc: T, options?: DocumentPutOptions) {
		const prepared = this.canUsePlainPutFastPath(options)
			? this.preparePlainPut(doc)
			: this.preparePut(doc);
		let existingLocalContext:
			| indexerTypes.IndexedResult<IndexedContextOnly<I>>
			| null
			| undefined;
		let existingHead: string | undefined;
		if (!options?.unique) {
			if (options?.checkRemote) {
				existingHead = (
					await this._index.getDetailed(prepared.keyValue, {
						resolve: false,
						local: true,
						remote: { replicate: options?.replicate },
					})
				)?.[0]?.results[0]?.context.head;
			} else {
				existingLocalContext =
					(await this.getLocalIndexedContext(prepared.key)) || null;
				existingHead = existingLocalContext?.value.__context.head;
			}
		}

		const plainPutPlan = await this.createPlainPutCommitPlan(
			prepared,
			existingHead,
			existingLocalContext,
			options,
		);
		if (plainPutPlan) {
			return this.commitPlainPutPlan(plainPutPlan, options);
		}

		const operation =
			"operation" in prepared
				? prepared.operation
				: new PutOperation({ data: prepared.encodedDocument });
		const appended = await this.log.append(operation, {
			...options,
			meta: {
				next: existingHead ? [await this._resolveEntry(existingHead)] : [],
				...options?.meta,
			},
			canAppend: (entry) => {
				return this.canAppend(entry, {
					document: prepared.document,
					operation,
				});
			},
			onChange: (change) => {
				return this.handleChanges(change, {
					document: prepared.document,
					operation,
					key: prepared.key,
					unique: options?.unique,
					existing: existingLocalContext,
				});
			},
			replicate: options?.replicate,
		});
		this.keepCache?.add(appended.entry.hash);
		return appended;
	}

	public async putMany(
		docs: T[],
		options?: DocumentPutOptions,
	): Promise<{
		entries: Entry<Operation>[];
		removed: ShallowOrFullEntry<Operation>[];
	}> {
		if (docs.length === 0) {
			return { entries: [], removed: [] };
		}
		if (!this.canUsePlainPutManyFastPath(options)) {
			return this.putManySequential(docs, options);
		}

		const prepared = docs.map((doc) => this.preparePlainPut(doc));
		if (this.hasDuplicatePreparedPutKeys(prepared)) {
			return this.putManySequential(docs, options);
		}

		const documentAppendCommit = await this.commitNativeDocumentAppendMany({
			puts: prepared.map((item) => ({
				document: item.document,
				key: item.key,
				documentBytes: item.encodedDocument,
				operationPayloadBytes: item.operationPayloadBytes,
				unique: options?.unique,
				existing: null,
			})),
			resolveTrimmedEntries: !this._index.canGetIdentityIndexedByHead(),
			options,
		});
		if (!documentAppendCommit) {
			return this.putManySequential(docs, options);
		}

		await this.handlePreparedPlainPutManyCommit(documentAppendCommit);
		for (const entry of documentAppendCommit.entries) {
			this.keepCache?.add(entry.hash);
		}
		return {
			entries: documentAppendCommit.entries,
			removed: documentAppendCommit.removed,
		};
	}

	private async putManySequential(
		docs: T[],
		options?: DocumentPutOptions,
	): Promise<{
		entries: Entry<Operation>[];
		removed: ShallowOrFullEntry<Operation>[];
	}> {
		const entries: Entry<Operation>[] = [];
		const removed: ShallowOrFullEntry<Operation>[] = [];
		for (const doc of docs) {
			const appended = await this.put(doc, options);
			entries.push(appended.entry);
			removed.push(...appended.removed);
		}
		return { entries, removed };
	}

	private hasDuplicatePreparedPutKeys(
		prepared: Array<{ key: indexerTypes.IdKey }>,
	): boolean {
		const keys = new Set<string | number | bigint>();
		for (const item of prepared) {
			if (keys.has(item.key.primitive)) {
				return true;
			}
			keys.add(item.key.primitive);
		}
		return false;
	}

	private canUsePlainPutFastPath(
		options?: DocumentPutOptions,
	): boolean {
		return (
			!this._optionCanPerform &&
			!this.immutable &&
			!this.strictHistory &&
			this.compatibility !== 6 &&
			!Program.isPrototypeOf(this._clazz) &&
			!options?.canAppend &&
			!options?.onChange &&
			!options?.signers &&
			!options?.identity &&
			!options?.encryption &&
			!options?.trim &&
			!options?.meta?.type &&
			!options?.meta?.next &&
			!options?.meta?.timestamp
		);
	}

	private canUsePlainPutManyFastPath(options?: DocumentPutOptions): boolean {
		return (
			options?.unique === true &&
			options?.replicate !== true &&
			options?.target === "none" &&
			(options?.delivery === undefined || options.delivery === false) &&
			this.canUsePlainPutFastPath(options)
		);
	}

	private async createPlainPutCommitPlan(
		prepared: PreparedPut<T> | PreparedPlainPut<T>,
		existingHead: string | undefined,
		existingLocalContext:
			| indexerTypes.IndexedResult<IndexedContextOnly<I>>
			| null
			| undefined,
		options:
			| DocumentPutOptions
			| undefined,
	): Promise<PlainPutCommitPlan<T, I> | undefined> {
		if (
			("operation" in prepared &&
				!(prepared.operation instanceof PutOperation)) ||
			!this.canUsePlainPutFastPath(options)
		) {
			return;
		}
		const indexedContextNext = existingHead
			? this.nextFromIndexedContext(existingHead, existingLocalContext)
			: undefined;
		const next = existingHead
			? indexedContextNext
				? [indexedContextNext]
				: [await this._resolveEntry(existingHead)]
			: [];
		return {
			document: prepared.document,
			encodedDocument: prepared.encodedDocument,
			payloadData:
				"operationPayloadBytes" in prepared
					? prepared.operationPayloadBytes
					: (prepared.encodedOperation ??
						encodePutOperationPayload(prepared.operation.data)),
			key: prepared.key,
			operation: "operation" in prepared ? prepared.operation : undefined,
			next,
			skipMissingNextJoin: !options?.checkRemote,
			resolveTrimmedEntries: !this._index.canGetIdentityIndexedByHead(),
			useGenericChangeHandler:
				!options?.unique && existingLocalContext === undefined,
			unique: options?.unique,
			existing: existingLocalContext,
		};
	}

	private async commitPlainPutPlan(
		plan: PlainPutCommitPlan<T, I>,
		options:
			| DocumentPutOptions
			| undefined,
	) {
		const documentAppendCommit = await this.commitNativeDocumentAppend({
			document: plan.document,
			key: plan.key,
			operation: plan.operation,
			documentBytes: plan.encodedDocument,
			operationPayloadBytes: plan.payloadData,
			next: plan.next,
			skipMissingNextJoin: plan.skipMissingNextJoin,
			resolveTrimmedEntries: plan.resolveTrimmedEntries,
			options,
			unique: plan.unique,
			existing: plan.existing,
		});
		if (plan.useGenericChangeHandler) {
			const operation =
				documentAppendCommit.operation ??
				plan.operation ??
				new PutOperation({ data: plan.encodedDocument });
			await this.handleChanges(
				{
					added: [{ head: true, entry: documentAppendCommit.entry }],
					removed: documentAppendCommit.removed,
				},
				{
					document: plan.document,
					operation,
					key: plan.key,
					unique: plan.unique,
					existing: plan.existing,
				},
			);
		} else {
			await this.handlePreparedPlainPutCommit(documentAppendCommit);
		}
		this.keepCache?.add(documentAppendCommit.entry.hash);
		return {
			entry: documentAppendCommit.entry,
			removed: documentAppendCommit.removed,
		};
	}

	private async commitNativeDocumentAppend(
		input: NativeDocumentAppendCommitInput<T, I>,
	): Promise<DocumentAppendCommitFacts<T, I>> {
		const appendOptions = {
			...input.options,
			meta: {
				next: input.next,
				...input.options?.meta,
			},
			replicate: input.options?.replicate,
		};
		const appendProperties = {
			skipMissingNextJoin: input.skipMissingNextJoin,
			resolveTrimmedEntries: input.resolveTrimmedEntries,
			payloadData: input.operationPayloadBytes,
		};
		const appended = input.operation
			? await this.log.appendLocallyPrepared(
					input.operation,
					appendOptions,
					appendProperties,
				)
			: await this.log.appendLocallyPreparedPayload(
					input.operationPayloadBytes,
					appendOptions,
					appendProperties,
				);
		return this.createDocumentAppendCommitFacts(input, appended);
	}

	private async commitNativeDocumentAppendMany(
		input: NativeDocumentAppendManyCommitInput<T, I>,
	): Promise<DocumentAppendManyCommitFacts<T, I> | undefined> {
		const appended = await this.log.appendLocallyPreparedPayloadsManyIndependent(
			input.puts.map((put) => put.operationPayloadBytes),
			{
				...input.options,
				replicate: input.options?.replicate,
			},
			{
				resolveTrimmedEntries: input.resolveTrimmedEntries,
			},
		);
		if (!appended) {
			return undefined;
		}
		const appendInputs = input.puts.map((put, index) => ({
			input: put,
			appended: {
				entry: appended.entries[index]!,
				removed: [],
				appendCommit: appended.appendCommits[index]!,
			},
		}));
		return {
			entries: appended.entries,
			removed: appended.removed,
			commits: await this.createDocumentAppendCommitFactsBatch(appendInputs),
		};
	}

	private async createDocumentAppendCommitFacts(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		appended: {
			entry: Entry<Operation>;
			removed: ShallowOrFullEntry<Operation>[];
			appendCommit: LocalAppendCommitFacts;
		},
	): Promise<DocumentAppendCommitFacts<T, I>> {
		const append = appended.appendCommit;
		const existing =
			input.unique || input.existing === null ? null : input.existing;
		const context = new Context({
			created: existing?.value.__context.created || append.wallTime,
			modified: append.wallTime,
			head: append.hash,
			gid: append.gid,
			size: append.payloadSize,
		});
		const contextBytes = await encodeNativeContextSuffix(context);
		return this.createDocumentAppendCommitFactsWithContext(
			input,
			appended,
			context,
			contextBytes,
		);
	}

	private async createDocumentAppendCommitFactsBatch(
		rows: Array<{
			input: NativeDocumentAppendCommitFactsInput<T, I>;
			appended: {
				entry: Entry<Operation>;
				removed: ShallowOrFullEntry<Operation>[];
				appendCommit: LocalAppendCommitFacts;
			};
		}>,
	): Promise<DocumentAppendCommitFacts<T, I>[]> {
		const contexts = rows.map(({ input, appended }) => {
			const append = appended.appendCommit;
			const existing =
				input.unique || input.existing === null ? null : input.existing;
			return new Context({
				created: existing?.value.__context.created || append.wallTime,
				modified: append.wallTime,
				head: append.hash,
				gid: append.gid,
				size: append.payloadSize,
			});
		});
		const contextBytes = await encodeNativeContextSuffixBatch(contexts);
		return rows.map((row, index) =>
			this.createDocumentAppendCommitFactsWithContext(
				row.input,
				row.appended,
				contexts[index]!,
				contextBytes[index]!,
			),
		);
	}

	private createDocumentAppendCommitFactsWithContext(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		appended: {
			entry: Entry<Operation>;
			removed: ShallowOrFullEntry<Operation>[];
			appendCommit: LocalAppendCommitFacts;
		},
		context: Context,
		contextBytes: Uint8Array,
	): DocumentAppendCommitFacts<T, I> {
		const append = appended.appendCommit;
		return {
			document: input.document,
			key: input.key,
			operation: input.operation,
			encodedDocument: input.documentBytes,
			operationPayloadBytes: input.operationPayloadBytes,
			entry: appended.entry,
			removed: appended.removed,
			append,
			context,
			contextBytes,
			contextualEncodedValueParts: {
				prefix: input.documentBytes,
				suffix: contextBytes,
			},
			unique: input.unique,
			existing: input.existing,
		};
	}

	private nextFromIndexedContext(
		existingHead: string,
		existing:
			| indexerTypes.IndexedResult<IndexedContextOnly<I>>
			| null
			| undefined,
	): ShallowEntry | undefined {
		const context = existing?.value.__context;
		if (!context || context.head !== existingHead) {
			return;
		}
		return new ShallowEntry({
			hash: context.head,
			head: false,
			payloadSize: context.size,
			meta: new ShallowMeta({
				gid: context.gid,
				clock: new LamportClock({
					id: this.log.log.identity.publicKey.bytes,
					timestamp: new Timestamp({
						wallTime: context.modified,
						logical: 0,
					}),
				}),
				next: [],
				type: EntryType.APPEND,
			}),
		});
	}

	private async handlePreparedPlainPutCommit(
		commit: DocumentAppendCommitFacts<T, I>,
	): Promise<void> {
		const documentsChanged: DocumentsChange<T, I> = {
			added: [],
			removed: [],
		};
		const modified: Set<string | number | bigint> = new Set();
		const existing =
			commit.unique || commit.existing === null ? null : commit.existing;

		if (!this.strictHistory && existing) {
			const shouldIgnoreChange = this.immutable
				? existing.value.__context.modified <
					commit.append.wallTime
				: existing.value.__context.modified >
					commit.append.wallTime;
			if (shouldIgnoreChange) {
				modified.add(commit.key.primitive);
			}
		}

		if (!modified.has(commit.key.primitive)) {
			const { indexable } = await this._index.putWithContext(
				commit.document,
				commit.key,
				commit.context,
				{
					replace: existing != null,
					encodedValueParts: commit.contextualEncodedValueParts,
				},
			);
			documentsChanged.added.push(
				coerceWithIndexed(
					coerceWithContext(commit.document, commit.context),
					indexable,
				),
			);
			modified.add(commit.key.primitive);
		}

		for (const removed of commit.removed) {
			if (
				!(removed instanceof Entry) &&
				(await this.collectRemovedDocumentChangeFromIndexedHead(
					removed.hash,
					modified,
					documentsChanged,
				))
			) {
				continue;
			}
			const entry =
				removed instanceof Entry
					? removed
					: await this.log.log.entryIndex.get(removed.hash);
			if (!entry) {
				continue;
			}
			try {
				await this.collectRemovedDocumentChange(
					await entry.getPayloadValue(),
					modified,
					documentsChanged,
				);
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

	private async handlePreparedPlainPutManyCommit(
		commit: DocumentAppendManyCommitFacts<T, I>,
	): Promise<void> {
		const documentsChanged: DocumentsChange<T, I> = {
			added: [],
			removed: [],
		};
		const modified: Set<string | number | bigint> = new Set();

		const putsToIndex: Array<{
			document: T;
			encodedDocument?: Uint8Array;
			key: indexerTypes.IdKey;
			context: Context;
			contextualEncodedValueParts?: ContextualEncodedValueParts;
		}> = [];
		for (const put of commit.commits) {
			if (modified.has(put.key.primitive)) {
				continue;
			}
			putsToIndex.push({
				document: put.document,
				encodedDocument: put.encodedDocument,
				key: put.key,
				context: put.context,
				contextualEncodedValueParts: put.contextualEncodedValueParts,
			});
			modified.add(put.key.primitive);
		}
		const indexed = await this._index.putManyWithContext(
			putsToIndex.map((put) => ({
				value: put.document,
				id: put.key,
				context: put.context,
				options: {
					replace: false,
					encodedValueParts: put.contextualEncodedValueParts,
				},
			})),
		);
		for (let i = 0; i < putsToIndex.length; i++) {
			const put = putsToIndex[i]!;
			const { indexable } = indexed[i]!;
			documentsChanged.added.push(
				coerceWithIndexed(coerceWithContext(put.document, put.context), indexable),
			);
		}

		const handledRemovedHeads =
			await this.collectRemovedDocumentChangesFromIndexedHeads(
				commit.removed,
				modified,
				documentsChanged,
			);
		for (const removed of commit.removed) {
			if (handledRemovedHeads.has(removed.hash)) {
				continue;
			}
			if (
				!(removed instanceof Entry) &&
				(await this.collectRemovedDocumentChangeFromIndexedHead(
					removed.hash,
					modified,
					documentsChanged,
				))
			) {
				continue;
			}
			const entry =
				removed instanceof Entry
					? removed
					: await this.log.log.entryIndex.get(removed.hash);
			if (!entry) {
				continue;
			}
			try {
				await this.collectRemovedDocumentChange(
					await entry.getPayloadValue(),
					modified,
					documentsChanged,
				);
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

	private async collectRemovedDocumentChangeFromIndexedHead(
		head: string,
		modified: Set<string | number | bigint>,
		documentsChanged: DocumentsChange<T, I>,
	): Promise<boolean> {
		const indexed = await this._index.getIdentityIndexedByHead(head);
		if (!indexed) {
			return false;
		}

		const key = indexed.id;
		if (modified.has(key.primitive)) {
			return true;
		}

		const value = coerceWithIndexed(
			indexed.value as unknown as WithIndexedContext<T, I>,
			indexed.value as unknown as I,
		);
		documentsChanged.removed.push(value);

		await this._index.del(key);
		modified.add(key.primitive);
		return true;
	}

	private async collectRemovedDocumentChangesFromIndexedHeads(
		removed: ShallowOrFullEntry<Operation>[],
		modified: Set<string | number | bigint>,
		documentsChanged: DocumentsChange<T, I>,
	): Promise<Set<string>> {
		const shallowRemoved = removed.filter(
			(entry): entry is ShallowEntry => !(entry instanceof Entry),
		);
		if (shallowRemoved.length === 0) {
			return new Set();
		}
		const indexedByHead = await this._index.getIdentityIndexedByHeads(
			shallowRemoved.map((entry) => entry.hash),
		);
		if (!indexedByHead) {
			return new Set();
		}

		const handled = new Set<string>();
		const deleteKeys: indexerTypes.IdKey[] = [];
		for (let i = 0; i < shallowRemoved.length; i++) {
			const indexed = indexedByHead[i];
			if (!indexed) {
				continue;
			}
			const key = indexed.id;
			handled.add(shallowRemoved[i]!.hash);
			if (modified.has(key.primitive)) {
				continue;
			}
			const value = coerceWithIndexed(
				indexed.value as unknown as WithIndexedContext<T, I>,
				indexed.value as unknown as I,
			);
			documentsChanged.removed.push(value);
			deleteKeys.push(key);
			modified.add(key.primitive);
		}
		await this._index.delMany(deleteKeys);
		return handled;
	}

	private async collectRemovedDocumentChange(
		payload: Operation,
		modified: Set<string | number | bigint>,
		documentsChanged: DocumentsChange<T, I>,
	) {
		let value: WithIndexedContext<T, I>;
		let key: indexerTypes.IdKey;

		if (isPutOperation(payload)) {
			const valueWithoutContext = this.index.valueEncoding.decoder(payload.data);
			key = indexerTypes.toId(this.idResolver(valueWithoutContext));
			if (modified.has(key.primitive)) {
				return;
			}

			const document = await this._index.get(key, {
				local: true,
				remote: false,
			});
			if (!document) {
				return;
			}
			value = document;
		} else if (isDeleteOperation(payload)) {
			key = coerceDeleteOperation(payload).key;
			if (modified.has(key.primitive)) {
				return;
			}
			const document = await this._index.get(key, {
				local: true,
				remote: false,
			});
			if (!document) {
				return;
			}
			value = document;
		} else {
			throw new Error("Unexpected");
		}

		documentsChanged.removed.push(value);

		if (
			value instanceof Program &&
			value.closed !== true &&
			value.parents.includes(this)
		) {
			await value.drop(this);
		}

		await this._index.del(key);
		modified.add(key.primitive);
	}

	public async get(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: Omit<GetOptions<T, I, D, true | undefined>, "resolve">,
	): Promise<T | undefined> {
		const resolved = await this.index.get(id, {
			...(options ?? {}),
			resolve: true,
		} as GetOptions<T, I, D, true>);
		return resolved ? (resolved as T) : undefined;
	}

	async del(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: SharedAppendOptions<Operation>,
	) {
		const key = id instanceof indexerTypes.IdKey ? id : indexerTypes.toId(id);
		const existing = (
			await this._index.getDetailed(key, {
				resolve: false,
				local: true,
				remote: { replicate: options?.replicate },
			})
		)?.[0]?.results[0];

		if (!existing) {
			throw new NotFoundError(
				`No entry with key '${key.primitive}' in the database`,
			);
		}

		this.keepCache?.delete(existing.value.__context.head);
		const entry = await this._resolveEntry(existing.context.head, {
			remote: true,
		});

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
		reference?: PutChangeReference<T, I>,
	): Promise<void> {
		logger.trace("handleChanges called", change);
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
		// in combinatpion with inserting the same document. To mitigate this, we loop through the changes and modify the behaviour for this

		let documentsChanged: DocumentsChange<T, I> = {
			added: [],
			removed: [],
		};

		let modified: Set<string | number | bigint> = new Set();
		for (const item of sortedEntries) {
			if (!item) {
				continue;
			}

			try {
				const isReferencedAppendEntry =
					isAppendOperation &&
					reference?.operation &&
					change.added[0]?.entry.hash === item.hash;
				const payload = isReferencedAppendEntry
					? reference.operation
					: /* item._payload instanceof DecryptedThing
							? item.payload.getValue(item.encoding)
							:  */ await item.getPayloadValue(); // TODO implement sync api for resolving entries that does not deep decryption

				if (isPutOperation(payload) && !removedSet.has(item.hash)) {
					let value =
						(isReferencedAppendEntry && reference?.document) ||
						this.index.valueEncoding.decoder(payload.data);

					// get index key from value
					const key =
						isReferencedAppendEntry && reference?.key
							? reference.key
							: indexerTypes.toId(this.idResolver(value));

					// document is already updated with more recent entry
					if (modified.has(key.primitive)) {
						continue;
					}

					// if no casual ordering is used, use timestamps to order docs
					let existing =
						reference?.unique || reference?.existing === null
							? null
							: isReferencedAppendEntry && reference?.existing !== undefined
								? reference.existing
								: (await this.getLocalIndexedContext(key)) || null;
					if (!this.strictHistory && existing) {
						// if immutable use oldest, else use newest
						let shouldIgnoreChange = this.immutable
							? existing.value.__context.modified <
								item.meta.clock.timestamp.wallTime
							: existing.value.__context.modified >
								item.meta.clock.timestamp.wallTime;
						if (shouldIgnoreChange) {
							continue;
						}
					}

					// Program specific
					if (value instanceof Program) {
						// if replicator, then open
						value = await this.maybeSubprogramOpen(value);
					}
					const { context, indexable } = await this._index.put(
						value,
						key,
						item,
						existing,
					);
					documentsChanged.added.push(
						coerceWithIndexed(coerceWithContext(value, context), indexable),
					);

					modified.add(key.primitive);
				} else if (
					(isDeleteOperation(payload) && !removedSet.has(item.hash)) ||
					isPutOperation(payload) ||
					removedSet.has(item.hash)
				) {
					await this.collectRemovedDocumentChange(
						payload,
						modified,
						documentsChanged,
					);
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

	/**
	 * Count documents locally (default), or estimate the global count.
	 *
	 * - `count()` / `count({ query })`: exact local count from the index.
	 * - `count({ approximate: true })`: estimated global count from replication metadata (no remote queries) + error margin when available.
	 */
	async count(options?: {
		query?: indexerTypes.Query | indexerTypes.QueryLike;
		approximate?: false | undefined;
	}): Promise<number>;
	async count(options: {
		query?: indexerTypes.Query | indexerTypes.QueryLike;
		approximate: true | { scope?: ReachScope };
	}): Promise<CountEstimate>;
	async count(options?: {
		query?: indexerTypes.Query | indexerTypes.QueryLike;
		approximate?: false | true | { scope?: ReachScope };
	}): Promise<number | CountEstimate> {
		// Local/exact count
		if (!options?.approximate) {
			return this.index.index.count({ query: options?.query });
		}

		const indexedDocumentsCount = await this.index.index.count({
			query: options?.query,
		});

		const fallbackToLocal = (): CountEstimate => ({
			estimate: indexedDocumentsCount,
			errorMargin: undefined,
		});

		const isReplicating = await this.log.isReplicating();
		if (!isReplicating) {
			return fallbackToLocal();
		}

		const myTotalParticipation = await this.log.calculateMyTotalParticipation();
		const minReplicasValue = this.log.replicas.min.getValue(this.log);
		const pRaw = minReplicasValue * myTotalParticipation;
		const inclusionProbability = Math.min(1, pRaw);

		if (
			!Number.isFinite(inclusionProbability) ||
			inclusionProbability <= 0 ||
			inclusionProbability > 1
		) {
			return fallbackToLocal();
		}

		const scaleFactor =
			inclusionProbability >= 1 ? 1 : 1 / inclusionProbability; // same saturation as SharedLog.countHeads
		if (
			!Number.isFinite(scaleFactor) ||
			scaleFactor > Number.MAX_SAFE_INTEGER
		) {
			return fallbackToLocal();
		}

		// heads strictly assigned to us (sample size for the head-count estimator)
		const ownedHeadCount = await this.log.countAssignedHeads({ strict: true });

		// heads we have in our index (includes boundary assignments)
		const totalAssignedHeads = await this.log.countAssignedHeads({
			strict: false,
		});

		if (totalAssignedHeads === 0) {
			return fallbackToLocal();
		}

		const totalHeadCount = Math.round(ownedHeadCount * scaleFactor);
		const nonDeletedDocumentsRatio = indexedDocumentsCount / totalAssignedHeads; // [0, 1]
		const expectedAmountOfDocuments = totalHeadCount * nonDeletedDocumentsRatio;

		if (
			!Number.isFinite(expectedAmountOfDocuments) ||
			expectedAmountOfDocuments > Number.MAX_SAFE_INTEGER
		) {
			return fallbackToLocal();
		}

		const estimate = Math.round(expectedAmountOfDocuments);
		if (!Number.isFinite(estimate) || estimate > Number.MAX_SAFE_INTEGER) {
			return fallbackToLocal();
		}

		const domainType = this.log.domain?.type;
		const canProvideErrorMargin =
			domainType === "hash" && ownedHeadCount > 0 && indexedDocumentsCount > 0;

		let errorMargin: number | undefined = undefined;
		if (canProvideErrorMargin) {
			// 95% relative margin for the scaled head-count estimator
			const headMargin =
				1.96 * Math.sqrt((1 - inclusionProbability) / ownedHeadCount);

			// 95% relative margin for the ratio estimator (docs among assigned heads)
			const rHat = nonDeletedDocumentsRatio;
			const ratioMargin =
				rHat > 0 && rHat < 1
					? 1.96 * Math.sqrt((1 - rHat) / (rHat * totalAssignedHeads))
					: 0;

			const combined = Math.sqrt(
				headMargin * headMargin + ratioMargin * ratioMargin,
			);
			if (Number.isFinite(combined)) {
				errorMargin = Math.min(1, Math.max(0, combined));
			}
		}

		return { estimate, errorMargin };
	}
}
