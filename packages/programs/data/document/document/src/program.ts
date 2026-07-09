import {
	type AbstractType,
	BorshError,
	field,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { AccessError, type PublicSignKey } from "@peerbit/crypto";
import {
	Context,
	NotFoundError,
	type ResultIndexedValue,
} from "@peerbit/document-interface";
import {
	type SimpleDocumentFieldExtractionPlan,
	type SimpleDocumentProjectionPlan,
	extractDocumentFieldSimple,
	initializeDocumentRust,
	planDocumentContext,
	planDocumentContextBatch,
	tryPlanDocumentContext,
	tryPlanDocumentContextBatch,
} from "./native-rust.js";
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
	entryV0PlainPayloadDataFromStorage,
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
import {
	type CanPerformPolicyDescriptor,
	type CanPerformPolicyEvaluator,
	createCanPerformPolicyEvaluator,
	createCanPerformDeletePolicyEvaluator,
	getCanPerformPolicyDescriptor,
	canPerformPolicyDeleteFieldPaths,
	canPerformPolicyNeedsDeleteValue,
	canPerformPolicyNeedsPreviousEntries,
	canPerformPolicyPutNeedsEntryPublicKeys,
	canPerformPolicySignedByFieldPaths,
} from "./policy.js";
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
	coerceWithLazyIndexed,
	encodeContextSuffix as encodeDocumentContextSuffix,
} from "./search.js";
import {
	type DocumentTransformer,
	getDocumentTransformDescriptor,
} from "./transform.js";

const logger = loggerFn("peerbit:program:document");
const warn = logger.newScope("warn");

export class OperationError extends Error {
	constructor(message?: string) {
		super(message);
	}
}

export type DocumentMode = "auto" | "compat" | "native";

export class NativeDocumentModeError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "NativeDocumentModeError";
	}
}

type MaybePromise<T> = Promise<T> | T;

/**
 * True when an error signals that an entry's payload bytes were never
 * materialized on the JS side (a hollow entry backed by a native block store).
 * `DecryptedThing.getValue` throws `Error("Missing data")` in that case. Used to
 * decide whether the auto-mode append/delete read-back should recover the
 * operation from the storage-bytes / block-store path instead.
 */
const isMissingPayloadDataError = (error: unknown): boolean =>
	error instanceof Error && error.message === "Missing data";

const isPromiseLike = <T>(value: MaybePromise<T>): value is Promise<T> =>
	!!value && typeof (value as Promise<T>).then === "function";

const mapMaybePromise = <T, R>(
	value: MaybePromise<T>,
	fn: (value: T) => MaybePromise<R>,
): MaybePromise<R> => (isPromiseLike(value) ? value.then(fn) : fn(value));

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
	if (left.byteLength !== right.byteLength) {
		return false;
	}
	for (let i = 0; i < left.byteLength; i++) {
		if (left[i] !== right[i]) {
			return false;
		}
	}
	return true;
};

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
	previousEntries?: Entry<Operation>[];
};

type CanPerformDelete<T> = {
	type: "delete";
	value?: T;
	operation: DeleteOperation;
	entry: Entry<DeleteOperation>;
};

export type CanPerformOperations<T> = CanPerformPut<T> | CanPerformDelete<T>;
export type CanPerform<T> = (
	properties: CanPerformOperations<T>,
) => MaybePromise<boolean>;

const PUT_OPERATION_PREFIX_LENGTH = 6;
const encodePutOperationPayload = (data: Uint8Array): Uint8Array => {
	const encoded = new Uint8Array(PUT_OPERATION_PREFIX_LENGTH + data.byteLength);
	encoded[0] = 0;
	encoded[1] = 3;
	const view = new DataView(
		encoded.buffer,
		encoded.byteOffset,
		encoded.byteLength,
	);
	view.setUint32(2, data.byteLength, true);
	encoded.set(data, PUT_OPERATION_PREFIX_LENGTH);
	return encoded;
};

const toContextBigInt = (value: bigint | number | string): bigint =>
	typeof value === "bigint" ? value : BigInt(value);

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

const NATIVE_LOCAL_PUT_OPTIONS = Object.freeze({
	replicate: false,
	target: "none" as const,
}) as DocumentPutOptions;
const NATIVE_LOCAL_UNIQUE_PUT_OPTIONS = Object.freeze({
	unique: true,
	replicate: false,
	target: "none" as const,
}) as DocumentPutOptions;

const cachedNativeLocalPutOptions = (
	options: DocumentPutOptions | undefined,
): DocumentPutOptions | undefined => {
	if (!options) {
		return NATIVE_LOCAL_PUT_OPTIONS;
	}
	let empty = true;
	for (const key in options) {
		empty = false;
		if (key !== "unique") {
			return;
		}
	}
	if (empty) {
		return NATIVE_LOCAL_PUT_OPTIONS;
	}
	return options.unique === true ? NATIVE_LOCAL_UNIQUE_PUT_OPTIONS : undefined;
};

type DocumentPutResult = {
	readonly entry: Entry<Operation>;
	removed: ShallowOrFullEntry<Operation>[];
};

type DocumentPutManyResult = {
	entries: Entry<Operation>[];
	removed: ShallowOrFullEntry<Operation>[];
};
type DocumentDeleteResult = DocumentPutResult;

interface DocumentBackend<T> {
	put(doc: T, options?: DocumentPutOptions): MaybePromise<DocumentPutResult>;
	putMany(
		docs: T[],
		options?: DocumentPutOptions,
	): MaybePromise<DocumentPutManyResult>;
	del(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: DocumentPutOptions,
	): MaybePromise<DocumentDeleteResult>;
}

type NativeDocumentBackendContext<T, I extends Record<string, any>> = {
	assertPlainPutSupported(doc: T, options?: DocumentPutOptions): void;
	assertPlainPutPolicySupported(
		doc: T,
		existing?: indexerTypes.IndexedResult<IndexedContextOnly<I>> | null,
		previousSignerPublicKey?: Uint8Array,
	): MaybePromise<void>;
	assertPlainPutManySupported(docs: T[], options?: DocumentPutOptions): void;
	normalizePutOptions(
		options: DocumentPutOptions | undefined,
	): DocumentPutOptions | undefined;
	preparePlainPut(doc: T): PreparedPlainPut<T>;
	hasDuplicatePreparedPutKeys(
		prepared: Array<{ key: indexerTypes.IdKey }>,
	): boolean;
	getIndexedContextHead(
		existing?: indexerTypes.IndexedResult<IndexedContextOnly<I>> | null,
	): string | undefined;
	getNextFromIndexedContext(
		existing?: indexerTypes.IndexedResult<IndexedContextOnly<I>> | null,
	): ShallowEntry | undefined;
	getNativeEntrySignerPublicKeys(
		hashes: string[],
	): Array<Uint8Array | undefined> | undefined;
	getNativePreviousEntrySignerPublicKey(
		key: indexerTypes.IdKey,
	): { exists: boolean; publicKey?: Uint8Array } | undefined;
	getNativeIndexedContextsAndPreviousSignerPublicKeys(
		keys: indexerTypes.IdKey[],
	):
		| {
				contexts: Array<
					indexerTypes.IndexedResult<IndexedContextOnly<I>> | undefined
				>;
				publicKeys: Array<Uint8Array | undefined>;
		  }
		| undefined;
	getNativeAppendRequiredPreviousSignerPublicKey(): Uint8Array | undefined;
	plainPutPolicyNeedsExistingContext(): boolean;
	shouldResolveTrimmedEntries(): boolean;
	commitNativeDocumentAppend(
		input: NativeDocumentAppendCommitInput<T, I>,
	): MaybePromise<NativeDocumentAppendTransaction<T, I>>;
	commitNativeDocumentAppendMany(
		input: NativeDocumentAppendManyCommitInput<T, I>,
	): MaybePromise<DocumentAppendManyCommitFacts<T, I> | undefined>;
	handlePreparedPlainPutCommit(
		commit: NativeDocumentAppendTransaction<T, I>,
	): MaybePromise<void>;
	handlePreparedPlainPutManyCommit(
		commit: DocumentAppendManyCommitFacts<T, I>,
	): MaybePromise<void>;
	deleteDocument(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: DocumentPutOptions,
	): MaybePromise<DocumentDeleteResult>;
	keepEntry(hash: string): void;
	nativeModeError(message: string): NativeDocumentModeError;
};

type DocumentBackendPut<T> = (
	doc: T,
	options?: DocumentPutOptions,
) => MaybePromise<DocumentPutResult>;

type DocumentBackendPutMany<T> = (
	docs: T[],
	options?: DocumentPutOptions,
) => MaybePromise<DocumentPutManyResult>;
type DocumentBackendDelete = (
	id: indexerTypes.Ideable | indexerTypes.IdKey,
	options?: DocumentPutOptions,
) => MaybePromise<DocumentDeleteResult>;

class CompatDocumentBackend<T> implements DocumentBackend<T> {
	constructor(
		private readonly putImpl: DocumentBackendPut<T>,
		private readonly putManyImpl: DocumentBackendPutMany<T>,
		private readonly deleteImpl: DocumentBackendDelete,
	) {}

	put(doc: T, options?: DocumentPutOptions): MaybePromise<DocumentPutResult> {
		return this.putImpl(doc, options);
	}

	putMany(
		docs: T[],
		options?: DocumentPutOptions,
	): MaybePromise<DocumentPutManyResult> {
		return this.putManyImpl(docs, options);
	}

	del(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: DocumentPutOptions,
	): MaybePromise<DocumentDeleteResult> {
		return this.deleteImpl(id, options);
	}
}

class NativeDocumentBackend<T, I extends Record<string, any>>
	implements DocumentBackend<T>
{
	constructor(private readonly context: NativeDocumentBackendContext<T, I>) {}

	put(doc: T, options?: DocumentPutOptions): MaybePromise<DocumentPutResult> {
		this.context.assertPlainPutSupported(doc, options);
		const putOptions = this.context.normalizePutOptions(options);
		const prepared = this.context.preparePlainPut(doc);
		const commit = (
			existing?:
				| indexerTypes.IndexedResult<IndexedContextOnly<I>>
				| null
				| undefined,
			useNativeExistingDocumentContext = !putOptions?.unique &&
				existing === undefined,
			requiredPreviousSignerPublicKey?: Uint8Array,
		) => {
			const nextEntry = existing
				? this.context.getNextFromIndexedContext(existing)
				: undefined;
			if (existing && !nextEntry) {
				throw this.context.nativeModeError(
					"requires indexed document context for native put",
				);
			}
			const next = nextEntry ? [nextEntry] : [];
			return mapMaybePromise(
				this.context.commitNativeDocumentAppend({
					document: prepared.document,
					key: prepared.key,
					documentBytes: prepared.encodedDocument,
					operationPayloadBytes: prepared.operationPayloadBytes,
					next: next as ShallowEntry[],
					skipMissingNextJoin: true,
					resolveTrimmedEntries: this.context.shouldResolveTrimmedEntries(),
					options: putOptions,
					unique: putOptions?.unique,
					useNativeExistingDocumentContext,
					requiredPreviousSignerPublicKey,
					existing,
				}),
				(documentAppendCommit) =>
					mapMaybePromise(
						this.context.handlePreparedPlainPutCommit(documentAppendCommit),
						() => {
							this.context.keepEntry(documentAppendCommit.append.hash);
							return {
								get entry() {
									return documentAppendCommit.entry;
								},
								removed: documentAppendCommit.removed,
							};
						},
					),
			);
		};
		const assertPolicyAndCommit = (
			existingContext?: indexerTypes.IndexedResult<
				IndexedContextOnly<I>
			> | null,
			previousSignerPublicKey?: Uint8Array,
		) => {
			const existingHead = this.context.getIndexedContextHead(existingContext);
			const nativePreviousSignerPublicKey =
				previousSignerPublicKey ??
				(existingHead
					? this.context.getNativeEntrySignerPublicKeys([existingHead])?.[0]
					: undefined);
			return mapMaybePromise(
				this.context.assertPlainPutPolicySupported(
					prepared.document,
					existingContext,
					nativePreviousSignerPublicKey,
				),
				() => commit(existingContext),
			);
		};

		if (
			!putOptions?.unique &&
			this.context.plainPutPolicyNeedsExistingContext()
		) {
			const requiredPreviousSignerPublicKey =
				this.context.getNativeAppendRequiredPreviousSignerPublicKey();
			if (requiredPreviousSignerPublicKey) {
				return commit(undefined, true, requiredPreviousSignerPublicKey);
			}
			const nativeContexts =
				this.context.getNativeIndexedContextsAndPreviousSignerPublicKeys([
					prepared.key,
				]);
			if (nativeContexts) {
				return assertPolicyAndCommit(
					nativeContexts.contexts[0] ?? null,
					nativeContexts.publicKeys[0],
				);
			}
			const nativePreviousSigner =
				this.context.getNativePreviousEntrySignerPublicKey(prepared.key);
			if (nativePreviousSigner) {
				if (!nativePreviousSigner.exists) {
					return assertPolicyAndCommit(null);
				}
				if (nativePreviousSigner.publicKey) {
					return mapMaybePromise(
						this.context.assertPlainPutPolicySupported(
							prepared.document,
							undefined,
							nativePreviousSigner.publicKey,
						),
						() => commit(),
					);
				}
			}
			throw this.context.nativeModeError(
				"requires native document context/signature facts",
			);
		}

		return assertPolicyAndCommit();
	}

	async putMany(
		docs: T[],
		options?: DocumentPutOptions,
	): Promise<DocumentPutManyResult> {
		if (docs.length === 0) {
			return { entries: [], removed: [] };
		}
		const putOptions = this.context.normalizePutOptions(options);
		for (const doc of docs) {
			this.context.assertPlainPutSupported(doc, putOptions);
		}
		const prepared = docs.map((doc) => this.context.preparePlainPut(doc));
		if (this.context.hasDuplicatePreparedPutKeys(prepared)) {
			const results: DocumentPutResult[] = [];
			for (const doc of docs) {
				results.push(await this.put(doc, putOptions));
			}
			let entries: Entry<Operation>[] | undefined;
			return {
				get entries() {
					return (entries ??= results.map((result) => result.entry));
				},
				removed: results.flatMap((result) => result.removed),
			};
		}
		this.context.assertPlainPutManySupported(docs, putOptions);
		let existingContexts:
			| Array<indexerTypes.IndexedResult<IndexedContextOnly<I>> | undefined>
			| undefined;
		let previousSignerPublicKeys: Array<Uint8Array | undefined> | undefined;
		const policyNeedsExistingContext =
			this.context.plainPutPolicyNeedsExistingContext();
		const requiredPreviousSignerPublicKey =
			this.context.getNativeAppendRequiredPreviousSignerPublicKey();
		const useNativeExistingDocumentContext =
			putOptions?.unique !== true &&
			(!policyNeedsExistingContext || !!requiredPreviousSignerPublicKey);
		if (putOptions?.unique !== true && !useNativeExistingDocumentContext) {
			const keys = prepared.map((item) => item.key);
			const nativeContexts =
				this.context.getNativeIndexedContextsAndPreviousSignerPublicKeys(keys);
			if (nativeContexts) {
				existingContexts = nativeContexts.contexts;
				if (policyNeedsExistingContext) {
					previousSignerPublicKeys = nativeContexts.publicKeys;
				}
			} else {
				throw this.context.nativeModeError(
					"requires native document context/signature batch facts",
				);
			}
		}
		if (
			existingContexts &&
			!previousSignerPublicKeys &&
			policyNeedsExistingContext
		) {
			const previousHeads = existingContexts.map((existing) =>
				this.context.getIndexedContextHead(existing ?? null),
			);
			if (previousHeads.some((head) => head != null)) {
				previousSignerPublicKeys = new Array(prepared.length);
				const lookupIndexes: number[] = [];
				const lookupHashes: string[] = [];
				for (let i = 0; i < previousHeads.length; i++) {
					const head = previousHeads[i];
					if (head) {
						lookupIndexes.push(i);
						lookupHashes.push(head);
					}
				}
				const lookup =
					this.context.getNativeEntrySignerPublicKeys(lookupHashes);
				if (lookup) {
					for (let i = 0; i < lookup.length; i++) {
						previousSignerPublicKeys[lookupIndexes[i]!] = lookup[i];
					}
				}
			}
		}
		if (!requiredPreviousSignerPublicKey) {
			await Promise.all(
				prepared.map((item, index) =>
					this.context.assertPlainPutPolicySupported(
						item.document,
						existingContexts ? (existingContexts[index] ?? null) : undefined,
						previousSignerPublicKeys?.[index],
					),
				),
			);
			}
			return mapMaybePromise(
				this.context.commitNativeDocumentAppendMany({
					puts: prepared.map((item, index) => ({
						document: item.document,
						key: item.key,
						documentBytes: item.encodedDocument,
						operationPayloadBytes: item.operationPayloadBytes,
						unique: putOptions?.unique,
						requiredPreviousSignerPublicKey,
						existing: existingContexts
							? (existingContexts[index] ?? null)
							: useNativeExistingDocumentContext
								? undefined
								: null,
					})),
					resolveTrimmedEntries: this.context.shouldResolveTrimmedEntries(),
					options: putOptions,
					useNativeExistingDocumentContext,
				}),
				(documentAppendCommit) => {
				if (!documentAppendCommit) {
					throw this.context.nativeModeError(
						"requires native batched payload append support",
					);
				}
				return mapMaybePromise(
					this.context.handlePreparedPlainPutManyCommit(documentAppendCommit),
					() => {
						for (const commit of documentAppendCommit.commits) {
							this.context.keepEntry(commit.append.hash);
						}
						return {
							get entries() {
								return documentAppendCommit.entries;
							},
							removed: documentAppendCommit.removed,
						};
					},
				);
			},
		);
	}

	del(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: DocumentPutOptions,
	): MaybePromise<DocumentDeleteResult> {
		const deleteOptions = this.context.normalizePutOptions(options);
		return this.context.deleteDocument(id, deleteOptions);
	}
}

type PreparedPut<T> = {
	document: T;
	encodedDocument: Uint8Array;
	encodedOperation?: Uint8Array;
	keyValue: indexerTypes.Ideable;
	key: indexerTypes.IdKey;
	operation: PutOperation | PutWithKeyOperation;
};

type PreparedPlainPut<T> = {
	document: T;
	encodedDocument: Uint8Array;
	operationPayloadBytes: Uint8Array;
	keyValue: indexerTypes.Ideable;
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

type NativeDocumentCoordinateFacts = {
	hash: string;
	hashNumber: number | bigint;
	hashNumberString?: string;
	gid: string;
	coordinates: Array<number | bigint>;
	coordinateStrings?: string[];
	wallTime: bigint;
	wallTimeString?: string;
	assignedToRangeBoundary: boolean;
	metaBytes: Uint8Array;
};

type NativeDocumentContextFacts = {
	created: bigint;
	modified: bigint;
	head: string;
	gid: string;
	size: number;
};

const nativeDocumentContextFactsAsContext = (
	facts: NativeDocumentContextFacts,
): Context => facts as unknown as Context;

type LocalAppendCommitFacts = {
	hash: string;
	gid: string;
	wallTime: bigint;
	payloadSize: number;
	hashNumber?: number | bigint;
	coordinateFields?: NativeDocumentCoordinateFacts;
	nativeBackboneDocumentIndexCommitted?: boolean;
	nativeBackboneDocumentIndexTrimmedHeadsProcessed?: boolean;
	nativeBackboneDocumentDeleteCommitted?: boolean;
	documentPreviousContext?: NativeDocumentContextFacts;
};

type NativeDocumentAppendResult = {
	entry: Entry<Operation>;
	removed: ShallowOrFullEntry<Operation>[];
	removedHashes?: string[];
	appendCommit: LocalAppendCommitFacts;
};

type TrustedDocumentSharedLogAppendProperties = {
	skipMissingNextJoin?: boolean;
	resolveTrimmedEntries?: boolean;
	payloadData?: Uint8Array;
	nativeBackboneDocumentIndex?: NativeBackboneDocumentIndexCommitInput;
	prepareNativeBackboneDocumentIndex?: (
		facts: NativeBackboneDocumentIndexAppendFactsInput,
	) => NativeBackboneDocumentIndexCommitInput | undefined;
	useNativeExistingDocumentContext?: boolean;
	nativeBackboneDocumentDeleteKey?: string;
};

type TrustedDocumentSharedLogAppendManyProperties = {
	resolveTrimmedEntries?: boolean;
	nexts?: ShallowOrFullEntry<Operation>[][];
	nativeBackboneDocumentIndexes?: NativeBackboneDocumentIndexCommitInput[];
	retainMaterializationBytes?: boolean;
};

type TrustedDocumentSharedLogAppendManyResult = {
	entries: Entry<Operation>[];
	materializeEntries?: Array<() => Entry<Operation>>;
	removed: ShallowOrFullEntry<Operation>[];
	appendCommits: LocalAppendCommitFacts[];
};

type TrustedDocumentSharedLog = {
	appendLocallyPrepared(
		data: Operation,
		options?: SharedAppendOptions<Operation>,
		properties?: TrustedDocumentSharedLogAppendProperties,
	): Promise<NativeDocumentAppendResult>;
	appendLocallyPreparedPayload(
		payloadData: Uint8Array,
		options?: SharedAppendOptions<Operation>,
		properties?: TrustedDocumentSharedLogAppendProperties,
	): Promise<NativeDocumentAppendResult>;
	appendLocallyPreparedPayloadCommitOnly(
		payloadData: Uint8Array,
		options?: SharedAppendOptions<Operation>,
		properties?: TrustedDocumentSharedLogAppendProperties,
	): MaybePromise<NativeDocumentAppendResult | undefined>;
	appendStrictNativeDocumentPayloadCommitOnly(
		payloadData: Uint8Array,
		options?: SharedAppendOptions<Operation>,
		properties?: TrustedDocumentSharedLogAppendProperties,
	): MaybePromise<NativeDocumentAppendResult | undefined>;
	appendLocallyPreparedPayloadsManyIndependent(
		payloadDatas: Uint8Array[],
		options?: SharedAppendOptions<Operation>,
		properties?: TrustedDocumentSharedLogAppendManyProperties,
	): Promise<TrustedDocumentSharedLogAppendManyResult | undefined>;
};

const asTrustedDocumentSharedLog = (
	log: SharedLog<Operation, any, any>,
): TrustedDocumentSharedLog => log as unknown as TrustedDocumentSharedLog;

type ContextualEncodedValueParts = {
	prefix: Uint8Array;
	suffix: Uint8Array;
};

type NativeBackboneDocumentIndexAppendFacts = {
	wallTime: bigint;
	gid: string;
	payloadSize: number;
};

type NativeBackboneDocumentIndexAppendFactsInput = {
	wallTime: bigint | number | string;
	gid: string;
	payloadSize: number;
};

const documentIndexStoreKey = (id: indexerTypes.IdKey): string => {
	const key = indexerTypes.toIdeable(id);
	if (key instanceof Uint8Array || ArrayBuffer.isView(key)) {
		return `bytes:${id.primitive.toString()}`;
	}
	return `${typeof key}:${key.toString()}`;
};

type NativeDocumentAppendTransactionContext = {
	getContext: () => Context;
	getContextBytes: () => Uint8Array;
};

// Internal native document write boundary. Keep Entry/context access lazy so the
// native commit facts can flow through the hot path without eager JS objects.
type NativeDocumentAppendTransaction<T, I extends Record<string, any>> = {
	document: T;
	key: indexerTypes.IdKey;
	operation?: PutOperation;
	encodedDocument: Uint8Array;
	operationPayloadBytes: Uint8Array;
	entry: Entry<Operation>;
	removed: ShallowOrFullEntry<Operation>[];
	removedHashes?: string[];
	append: LocalAppendCommitFacts;
	coordinateFields?: NativeDocumentCoordinateFacts;
	context: Context;
	contextBytes: Uint8Array;
	contextualEncodedValueParts: ContextualEncodedValueParts;
	nativeBackboneDocumentIndexCommitted?: boolean;
	nativeBackboneDocumentIndexTrimmedHeadsProcessed?: boolean;
	nativeBackboneDocumentIndex?: {
		valuePrefixBytes?: Uint8Array;
		projection?: {
			encodedDocument: Uint8Array;
			plan: SimpleDocumentProjectionPlan;
			signer?: Uint8Array;
		};
		indexable?: I;
		getIndexable?: () => I;
		setContext?: (context: Context) => void;
	};
	unique?: boolean;
	existing?:
		| indexerTypes.IndexedResult<IndexedContextOnly<I>>
		| null
		| undefined;
};

type DocumentAppendCommitFacts<
	T,
	I extends Record<string, any>,
> = NativeDocumentAppendTransaction<T, I>;

type NativeDocumentAppendCommitFactsInput<T, I extends Record<string, any>> = {
	document: T;
	key: indexerTypes.IdKey;
	documentBytes: Uint8Array;
	operationPayloadBytes: Uint8Array;
	operation?: PutOperation;
	unique?: boolean;
	requiredPreviousSignerPublicKey?: Uint8Array;
	existing?:
		| indexerTypes.IndexedResult<IndexedContextOnly<I>>
		| null
		| undefined;
	nativeBackboneDocumentIndex?: {
		valuePrefixBytes?: Uint8Array;
		projection?: {
			encodedDocument: Uint8Array;
			plan: SimpleDocumentProjectionPlan;
			signer?: Uint8Array;
		};
		indexable?: I;
		getIndexable?: () => I;
		setContext?: (context: Context) => void;
	};
};

type NativeBackboneDocumentIndexCommitInput = {
	key: string;
	valuePrefixBytes?: Uint8Array;
	usePlainPutPayload?: boolean;
	projection?: {
		encodedDocument: Uint8Array;
		plan: SimpleDocumentProjectionPlan;
		signer?: Uint8Array;
	};
	existingCreated?: bigint;
	deleteTrimmedHeads?: boolean;
	useLatestContext?: boolean;
	requiredPreviousSignerPublicKey?: Uint8Array;
};

type PreparedNativeBackboneDocumentIndexCommit<I> = {
	valuePrefixBytes?: Uint8Array;
	usePlainPutPayload?: boolean;
	projection?: {
		encodedDocument: Uint8Array;
		plan: SimpleDocumentProjectionPlan;
		signer?: Uint8Array;
	};
	indexable?: I;
	getIndexable?: () => I;
	setContext?: (context: Context) => void;
};

type TrustedDocumentIndexTransformFacts = {
	entryPublicKeys?: PublicSignKey[];
};

type TrustedDocumentIndex<T, I extends Record<string, any>> = {
	attachNativeBackboneDocumentIndex(
		backbone: unknown,
		options?: { preserveExisting?: boolean },
	): boolean;
	getNativeDocumentFieldExtractionPlan(
		path: string | readonly string[],
	): SimpleDocumentFieldExtractionPlan | undefined;
	canPrepareNativeBackboneDocumentIndexCommitWithAppendFacts(): boolean;
	canUseNativeBackboneContextualBatch(): boolean;
	prepareNativeBackboneDocumentIndexCommit(
		value: T,
		encodedDocument: Uint8Array,
		transformFacts?: TrustedDocumentIndexTransformFacts,
	): MaybePromise<PreparedNativeBackboneDocumentIndexCommit<I> | undefined>;
	prepareNativeBackboneDocumentIndexCommitWithAppendFacts(
		value: T,
		encodedDocument: Uint8Array,
		context: Context,
		transformFacts?: TrustedDocumentIndexTransformFacts,
	): PreparedNativeBackboneDocumentIndexCommit<I> | undefined;
	prepareNativeBackboneDocumentIndexStoredCommitWithAppendFacts(
		encodedDocument: Uint8Array,
		context: Context,
		transformFacts?: TrustedDocumentIndexTransformFacts,
	): PreparedNativeBackboneDocumentIndexCommit<I> | undefined;
	_putPreparedNativeBackboneDocumentIndexWithContext(
		value: T,
		id: indexerTypes.IdKey,
		context: Context,
		nativeDocumentIndex: PreparedNativeBackboneDocumentIndexCommit<I>,
		options?: { replace?: boolean },
	): MaybePromise<WithIndexedContext<T, I> | undefined>;
	_putPreparedNativeBackboneDocumentIndexStoredWithContext(
		id: indexerTypes.IdKey,
		context: Context,
		nativeDocumentIndex: PreparedNativeBackboneDocumentIndexCommit<I>,
		options?: { replace?: boolean },
	): MaybePromise<boolean | undefined>;
	_persistPreparedNativeBackboneDocumentIndexStoredWithContext(
		id: indexerTypes.IdKey,
		context: Context,
		nativeDocumentIndex?: PreparedNativeBackboneDocumentIndexCommit<I>,
		encodedValueParts?: ContextualEncodedValueParts,
		options?: { replace?: boolean },
	): MaybePromise<boolean | undefined>;
	_putManyPreparedNativeBackboneDocumentIndexWithContext(
		values: Array<{
			value: T;
			id: indexerTypes.IdKey;
			context: Context;
			nativeDocumentIndex?: PreparedNativeBackboneDocumentIndexCommit<I>;
			options?: { replace?: boolean };
		}>,
	): Promise<WithIndexedContext<T, I>[] | undefined>;
	_putManyPreparedNativeBackboneDocumentIndexStored(
		values: Array<{
			value: T;
			id: indexerTypes.IdKey;
			context: Context;
			encodedValueParts?: ContextualEncodedValueParts;
			nativeDocumentIndex?: PreparedNativeBackboneDocumentIndexCommit<I>;
			options?: { replace?: boolean };
		}>,
	): Promise<boolean | undefined>;
};

const asTrustedDocumentIndex = <
	T,
	I extends Record<string, any>,
	D extends ReplicationDomain<any, Operation, any>,
>(
	index: DocumentIndex<T, I, D>,
): TrustedDocumentIndex<T, I> =>
	index as unknown as TrustedDocumentIndex<T, I>;

type NativeDocumentAppendCommitInput<
	T,
	I extends Record<string, any>,
> = NativeDocumentAppendCommitFactsInput<T, I> & {
	next: Entry<Operation>[] | ShallowEntry[];
	skipMissingNextJoin: boolean;
	resolveTrimmedEntries: boolean;
	options?: DocumentPutOptions;
	useNativeExistingDocumentContext?: boolean;
};

type NativeDocumentAppendManyCommitInput<T, I extends Record<string, any>> = {
	puts: NativeDocumentAppendCommitFactsInput<T, I>[];
	resolveTrimmedEntries: boolean;
	options?: DocumentPutOptions;
	useNativeExistingDocumentContext?: boolean;
};

type DocumentAppendManyCommitFacts<T, I extends Record<string, any>> = {
	entries: Entry<Operation>[];
	removed: ShallowOrFullEntry<Operation>[];
	commits: NativeDocumentAppendTransaction<T, I>[];
};

type InferR<D> = D extends ReplicationDomain<any, any, infer I> ? I : "u32";

export type SetupOptions<
	T,
	I extends Record<string, any> = T extends Record<string, any> ? T : any,
	D extends ReplicationDomain<any, Operation, any> = any,
> = {
	type: AbstractType<T>;
	mode?: DocumentMode;
	canOpen?: (program: T) => MaybePromise<boolean>;
	canPerform?: CanPerform<T>;
	strictHistory?: boolean;
	id?: (obj: any) => indexerTypes.Ideable;
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
	private _optionCanPerformNativePolicy?: CanPerformPolicyDescriptor;
	private _optionCanPerformNativeFastPath?: CanPerformPolicyEvaluator;
	private _nativeBackboneDocumentIndexEnabled = false;
	private _mode: DocumentMode = "auto";
	private _nativeModeReplicatedOpen = false;
	private _valueClassIsProgram = false;
	private _documentChangeListeners: Array<{
		listener: unknown;
		capture: boolean;
	}> = [];
	private _documentChangeListenerCount = 0;
	private _documentInternalChangeListenerCount = 0;
	private _documentChangeListenerTrackingInitialized = false;
	private _documentBackend!: DocumentBackend<T>;
	private _canAppendDecodedDocuments = new WeakMap<PutOperation, T>();
	private _nativeDocumentIdExtractionPlan?: SimpleDocumentFieldExtractionPlan;
	private _nativeDocumentFieldExtractionPlans?: Map<
		string,
		SimpleDocumentFieldExtractionPlan | undefined
	>;
	private _hasLogTrim = false;
	private idResolver!: (any: any) => indexerTypes.Ideable;
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
		this.trackDocumentChangeListeners();
		this._documentBackend = this.createDocumentBackend();
	}

	get index(): DocumentIndex<T, I, D> {
		return this._index;
	}

	private isNativeMode(): boolean {
		return this._mode === "native";
	}

	private createDocumentBackend(): DocumentBackend<T> {
		return this.isNativeMode()
			? new NativeDocumentBackend(this.createNativeDocumentBackendContext())
			: new CompatDocumentBackend(
					this.putCompatDocumentBackend.bind(this),
					this.putManyCompatDocumentBackend.bind(this),
					this.delCompatDocumentBackend.bind(this),
				);
	}

	private createNativeDocumentBackendContext(): NativeDocumentBackendContext<
		T,
		I
	> {
		return {
			assertPlainPutSupported: (doc, options) => {
				this.assertNativeModePlainPutSupported(doc, options);
			},
			assertPlainPutPolicySupported: (doc, existing, previousSignerPublicKey) =>
				this.assertNativeModePlainPutPolicySupported(
					doc,
					existing,
					previousSignerPublicKey,
				),
			assertPlainPutManySupported: (docs, options) => {
				this.assertNativeModePlainPutManySupported(docs, options);
			},
			normalizePutOptions: (options) =>
				this.normalizeNativeModePutOptions(options),
			preparePlainPut: (doc) => this.preparePlainPut(doc),
			hasDuplicatePreparedPutKeys: (prepared) =>
				this.hasDuplicatePreparedPutKeys(prepared),
			getIndexedContextHead: (existing) =>
				this.getExistingContext(existing)?.head,
			getNextFromIndexedContext: (existing) => {
				const existingHead = this.getExistingContext(existing)?.head;
				return existingHead
					? this.nextFromIndexedContext(existingHead, existing)
					: undefined;
			},
			getNativeEntrySignerPublicKeys: (hashes) =>
				this.getNativeEntrySignerPublicKeys(hashes),
			getNativePreviousEntrySignerPublicKey: (key) =>
				this.getNativePreviousEntrySignerPublicKey(key),
			getNativeIndexedContextsAndPreviousSignerPublicKeys: (keys) =>
				this.getNativeIndexedContextsAndPreviousSignerPublicKeys(keys),
			getNativeAppendRequiredPreviousSignerPublicKey: () =>
				this.nativePlainPutPolicyRequiredPreviousSignerPublicKey(),
			plainPutPolicyNeedsExistingContext: () =>
				this.nativePlainPutPolicyNeedsPreviousEntries(),
			shouldResolveTrimmedEntries: () => {
				return !this._index.canGetIndexedKeyByHead();
			},
			commitNativeDocumentAppend: (input) =>
				this.commitNativeDocumentAppend(input),
			commitNativeDocumentAppendMany: (input) =>
				this.commitNativeDocumentAppendMany(input),
			handlePreparedPlainPutCommit: (commit) =>
				this.handlePreparedPlainPutCommit(commit),
			handlePreparedPlainPutManyCommit: (commit) =>
				this.handlePreparedPlainPutManyCommit(commit),
			deleteDocument: (id, options) =>
				this.delNativeDocumentBackend(id, options),
			keepEntry: (hash) => {
				this.keepCache?.add(hash);
			},
			nativeModeError: (message) => this.nativeModeError(message),
		};
	}

	private nativeModeError(message: string): NativeDocumentModeError {
		return new NativeDocumentModeError(`Documents native mode ${message}`);
	}

	private assertNativeModeOpenOptions(options: SetupOptions<T, I, D>): void {
		if (!this.isNativeMode()) {
			return;
		}
		const unsupported: string[] = [];
		const nativeBackbone = options.nativeBackbone as
			| {
					documentIndex?: boolean;
					optional?: boolean;
					coordinatePersistence?: unknown;
					heads?: boolean;
			  }
			| boolean
			| undefined;
		const indexTransform = options.index as
			| {
					type?: unknown;
					transform?: unknown;
			  }
			| undefined;
		const nativeIndexTransformDescriptor =
			typeof indexTransform?.transform === "function"
				? getDocumentTransformDescriptor(
						indexTransform.transform as DocumentTransformer<
							unknown,
							unknown
						>,
					)
				: undefined;

		if (
			!nativeBackbone ||
			typeof nativeBackbone !== "object" ||
			nativeBackbone.documentIndex !== true
		) {
			unsupported.push("missing nativeBackbone.documentIndex");
		} else if (nativeBackbone.optional !== false) {
			unsupported.push("optional nativeBackbone");
		} else if (!nativeBackbone.coordinatePersistence) {
			unsupported.push("missing nativeBackbone.coordinatePersistence");
		} else if (nativeBackbone.heads === false) {
			unsupported.push("disabled native heads");
		}
		if (options.domain) {
			unsupported.push("custom domain");
		}
		if (options.compatibility != null) {
			unsupported.push("legacy compatibility");
		}
		if (options.strictHistory) {
			unsupported.push("strict history");
		}
		if (this.immutable) {
			unsupported.push("immutable documents");
		}
		if (options.appendDurability) {
			unsupported.push("custom append durability");
		}
		if (Program.isPrototypeOf(options.type)) {
			unsupported.push("program-valued document type");
		}
		if (
			options.canPerform &&
			!getCanPerformPolicyDescriptor(options.canPerform)
		) {
			unsupported.push("arbitrary canPerform");
		}
		if (options.canOpen) {
			unsupported.push("custom canOpen");
		}
		if (options.id) {
			unsupported.push("custom id");
		}
		if (options.index?.canRead) {
			unsupported.push("custom canRead");
		}
		if (options.index?.canSearch) {
			unsupported.push("custom canSearch");
		}
		if (options.index?.prefetch) {
			unsupported.push("index prefetch");
		}
		if (options.index?.cache?.query) {
			unsupported.push("index query cache");
		}
		if (options.canReplicate) {
			unsupported.push("custom canReplicate");
		}
		if (options.keep) {
			unsupported.push("custom keep");
		}
		if (options.fanout) {
			unsupported.push("fanout");
		}
		if (options.syncronizer) {
			unsupported.push("custom syncronizer");
		}
		if (options.sync?.priority) {
			unsupported.push("custom sync priority");
		}
		if (options.sync?.profile) {
			unsupported.push("custom sync profile");
		}
		if (
			options.log?.trim &&
			(options.log.trim.type !== "length" || options.log.trim.filter?.canTrim)
		) {
			unsupported.push("unsupported log trim");
		}
		if (indexTransform?.transform && !nativeIndexTransformDescriptor) {
			unsupported.push("arbitrary index transform");
		}
		if (
			indexTransform?.type &&
			!indexTransform.transform &&
			indexTransform.type !== options.type
		) {
			unsupported.push("constructor index transform");
		}

		if (unsupported.length > 0) {
			throw this.nativeModeError(`does not support ${unsupported.join(", ")}`);
		}
	}

	private assertNativeModeReady(): void {
		if (!this.isNativeMode()) {
			return;
		}
		if (!this._nativeBackboneDocumentIndexEnabled) {
			throw this.nativeModeError(
				"requires an attached native backbone document index",
			);
		}
		if (
			!asTrustedDocumentIndex(this._index).canPrepareNativeBackboneDocumentIndexCommitWithAppendFacts()
		) {
			throw this.nativeModeError(
				"requires a native-compatible document index transform",
			);
		}
		if (!this._nativeDocumentIdExtractionPlan) {
			throw this.nativeModeError(
				"requires a native-compatible document id field",
			);
		}
		if (this._optionCanPerformNativePolicy) {
			const signedByFieldPaths = canPerformPolicySignedByFieldPaths(
				this._optionCanPerformNativePolicy,
			);
			for (const path of signedByFieldPaths) {
				if (!this.getNativeDocumentFieldExtractionPlan(path)) {
					const label = Array.isArray(path) ? path.join(".") : path;
					throw this.nativeModeError(
						`requires native-compatible signedByField policy path: ${label}`,
					);
				}
			}
			const deleteFieldPaths = canPerformPolicyDeleteFieldPaths(
				this._optionCanPerformNativePolicy,
			);
			if (
				deleteFieldPaths.length > 0 &&
				!this._index.canReadNativeIndexedFieldValues(deleteFieldPaths)
			) {
				const labels = deleteFieldPaths.map((path) =>
					Array.isArray(path) ? path.join(".") : path,
				);
				throw this.nativeModeError(
					`requires native index to read delete policy field${labels.length > 1 ? "s" : ""}: ${labels.join(", ")}`,
				);
			}
		}
	}

	private canPerformAllowsPlainPutFastPath(doc: T): boolean {
		return (
			!this._optionCanPerform || !!this._optionCanPerformNativeFastPath?.(doc)
		);
	}

	private nativePlainPutPolicyNeedsPreviousEntries(): boolean {
		return (
			!!this._optionCanPerformNativePolicy &&
			canPerformPolicyNeedsPreviousEntries(
				this._optionCanPerformNativePolicy,
			)
		);
	}

	private nativePlainPutPolicyRequiredPreviousSignerPublicKey():
		| Uint8Array
		| undefined {
		const descriptor = this._optionCanPerformNativePolicy;
		const policyDescriptor =
			descriptor?.kind === "put" ? descriptor.policy : descriptor;
		if (policyDescriptor?.kind !== "sameSignersAsPrevious") {
			return;
		}
		return (this.log.log.identity.publicKey as { publicKey?: Uint8Array })
			.publicKey;
	}

	private unsupportedNativePutOptions(
		options: DocumentPutOptions | undefined,
	): string[] {
		const unsupported: string[] = [];
		if (options?.canAppend) {
			unsupported.push("per-call canAppend");
		}
		if (options?.onChange) {
			unsupported.push("per-call onChange");
		}
		if (options?.signers) {
			unsupported.push("custom signers");
		}
		if (options?.identity) {
			unsupported.push("custom identity");
		}
		if (options?.encryption) {
			unsupported.push("encryption");
		}
		if (options?.trim) {
			unsupported.push("per-call trim");
		}
		if (options?.durability) {
			unsupported.push("per-call durability");
		}
		if (options?.deferIndexWrite !== undefined) {
			unsupported.push("per-call index write deferral");
		}
		if (options?.meta?.type) {
			unsupported.push("custom entry type");
		}
		if (options?.meta && "data" in options.meta) {
			unsupported.push("custom metadata");
		}
		if (options?.meta?.next) {
			unsupported.push("custom next");
		}
		if (options?.meta?.timestamp) {
			unsupported.push("custom timestamp");
		}
		if (options?.meta?.gidSeed) {
			unsupported.push("custom gid seed");
		}
		if (options?.replicate === true) {
			unsupported.push("replicated put");
		}
		if (options?.target && options.target !== "none") {
			unsupported.push("non-local target");
		}
		if (options?.delivery !== undefined && options.delivery !== false) {
			unsupported.push("delivery");
		}
		if (options?.checkRemote) {
			unsupported.push("remote existing-head check");
		}
		if (options?.replicas !== undefined) {
			unsupported.push("per-call replicas");
		}
		return unsupported;
	}

	private assertNativeModePlainPutSupported(
		doc: T,
		options?: DocumentPutOptions,
	): boolean {
		if (!this.isNativeMode()) {
			return false;
		}
		const unsupported = this.unsupportedNativePutOptions(options);
		if (this.immutable) {
			unsupported.push("immutable documents");
		}
		if (this.strictHistory) {
			unsupported.push("strict history");
		}
		if (this.compatibility === 6) {
			unsupported.push("legacy compatibility");
		}
		if (Program.isPrototypeOf(this._clazz)) {
			unsupported.push("program-valued document type");
		}
		if (unsupported.length > 0) {
			throw this.nativeModeError(`does not support ${unsupported.join(", ")}`);
		}
		return true;
	}

	private assertNativeModePlainPutPolicySupported(
		doc: T,
		existing?: indexerTypes.IndexedResult<IndexedContextOnly<I>> | null,
		previousSignerPublicKey?: Uint8Array,
	): MaybePromise<void> {
		return mapMaybePromise(
			this.canPerformAllowsNativePlainPut(
				doc,
				existing,
				previousSignerPublicKey,
			),
			(allowed) => {
				if (!allowed) {
					throw this.nativeModeError(
						"canPerform policy rejected this document",
					);
				}
			},
		);
	}

	private canPerformAllowsNativePlainPut(
		doc: T,
		existing?: indexerTypes.IndexedResult<IndexedContextOnly<I>> | null,
		previousSignerPublicKey?: Uint8Array,
	): MaybePromise<boolean> {
		if (!this._optionCanPerform) {
			return true;
		}
		if (!this._optionCanPerformNativePolicy) {
			return false;
		}
		if (!this.nativePlainPutPolicyNeedsPreviousEntries()) {
			return this.canPerformAllowsPlainPutFastPath(doc);
		}
		const previousEntries: Entry<Operation>[] = [];
		const existingHead = this.getExistingContext(existing)?.head;
		if (existingHead) {
			if (previousSignerPublicKey) {
				return this.nativePutPolicyAllows(
					this._optionCanPerformNativePolicy!,
					doc,
					previousEntries,
					[previousSignerPublicKey],
				);
			}
			if (this.isNativeMode()) {
				throw this.nativeModeError("requires native previous signer facts");
			}
			return mapMaybePromise(
				this._resolveEntry(existingHead, {
					remote: true,
				}),
				(previousEntry) => {
					if (previousEntry) {
						previousEntries.push(previousEntry);
					}
					return this.nativePutPolicyAllows(
						this._optionCanPerformNativePolicy!,
						doc,
						previousEntries,
					);
				},
			);
		}
		return this.nativePutPolicyAllows(
			this._optionCanPerformNativePolicy,
			doc,
			previousEntries,
		);
	}

	private async nativePutPolicyAllows(
		descriptor: CanPerformPolicyDescriptor,
		doc: T,
		previousEntries: Entry<Operation>[],
		previousSignerPublicKeys: Uint8Array[] = [],
	): Promise<boolean> {
		return this.nativePutOperationPolicyAllows(
			descriptor,
			undefined,
			doc,
			previousEntries,
			previousSignerPublicKeys,
		);
	}

	private nativeFieldValueMatchesLocalPublicKey(value: unknown): boolean {
		const localPublicKey = this.log.log.identity.publicKey;
		return this.nativeFieldValueMatchesPublicKey(value, localPublicKey);
	}

	private nativeFieldValueMatchesPublicKey(
		value: unknown,
		publicKey: PublicSignKey,
	): boolean {
		const localRawPublicKey = (
			publicKey as { publicKey?: Uint8Array }
		).publicKey;
		return (
			value instanceof Uint8Array &&
			(bytesEqual(value, publicKey.bytes) ||
				(localRawPublicKey ? bytesEqual(value, localRawPublicKey) : false))
		);
	}

	private nativeFieldValueMatchesPublicKeys(
		value: unknown,
		publicKeys: readonly PublicSignKey[],
	): boolean {
		for (const publicKey of publicKeys) {
			if (this.nativeFieldValueMatchesPublicKey(value, publicKey)) {
				return true;
			}
		}
		return false;
	}

	private nativeDeletePolicyNeedsEntryPublicKeys(
		descriptor: CanPerformPolicyDescriptor,
	): boolean {
		switch (descriptor.kind) {
			case "signedByPublicKey":
			case "deleteSignedByExistingField":
				return true;
			case "delete":
				return this.nativeDeletePolicyNeedsEntryPublicKeys(descriptor.policy);
			case "and":
			case "or":
				return descriptor.policies.some((policy) =>
					this.nativeDeletePolicyNeedsEntryPublicKeys(policy),
				);
			default:
				return false;
		}
	}

	private async nativePutOperationPolicyAllows(
		descriptor: CanPerformPolicyDescriptor,
		operation: PutOperation | undefined,
		doc: T | undefined,
		previousEntries: Entry<Operation>[],
		previousSignerPublicKeys: Uint8Array[] = [],
		entryPublicKeys: readonly PublicSignKey[] = [],
	): Promise<boolean> {
		switch (descriptor.kind) {
			case "allowAll":
				return createCanPerformPolicyEvaluator(
					descriptor,
					this.log.log.identity.publicKey,
				)(doc as unknown);
			case "signedByPublicKey":
				return entryPublicKeys.length > 0
					? this.nativeFieldValueMatchesPublicKeys(
							descriptor.publicKey,
							entryPublicKeys,
						)
					: createCanPerformPolicyEvaluator(
							descriptor,
							this.log.log.identity.publicKey,
						)(doc as unknown);
			case "signedByField": {
				if (doc) {
					return createCanPerformPolicyEvaluator(
						descriptor,
						this.log.log.identity.publicKey,
					)(doc);
				}
				if (!operation) {
					return false;
				}
				const value = await this.getNativeDocumentFieldFromPutOperation(
					operation,
					descriptor.path,
				);
				return this.nativeFieldValueMatchesPublicKeys(value, entryPublicKeys);
			}
			case "put":
				return this.nativePutOperationPolicyAllows(
					descriptor.policy,
					operation,
					doc,
					previousEntries,
					previousSignerPublicKeys,
					entryPublicKeys,
				);
			case "delete":
			case "deleteSignedByExistingField":
				return false;
			case "sameSignersAsPrevious": {
				if (previousSignerPublicKeys.length > 0) {
					const currentPublicKeys =
						entryPublicKeys.length > 0
							? entryPublicKeys
							: [this.log.log.identity.publicKey];
					if (currentPublicKeys.length !== previousSignerPublicKeys.length) {
						return false;
					}
					for (const previousSignerPublicKey of previousSignerPublicKeys) {
						if (
							!this.nativeFieldValueMatchesPublicKeys(
								previousSignerPublicKey,
								currentPublicKeys,
							)
						) {
							return false;
						}
					}
					return true;
				}
				if (previousEntries.length === 0) {
					return true;
				}
				const localPublicKey = this.log.log.identity.publicKey;
				for (const previousEntry of previousEntries) {
					const publicKeys = await previousEntry.getPublicKeys();
					if (
						publicKeys.length !== 1 ||
						!publicKeys[0]!.equals(localPublicKey)
					) {
						return false;
					}
				}
				return true;
			}
			case "and":
				for (const policy of descriptor.policies) {
					if (
						!(await this.nativePutOperationPolicyAllows(
							policy,
							operation,
							doc,
							previousEntries,
							previousSignerPublicKeys,
							entryPublicKeys,
						))
					) {
						return false;
					}
				}
				return true;
			case "or":
				for (const policy of descriptor.policies) {
					if (
						await this.nativePutOperationPolicyAllows(
							policy,
							operation,
							doc,
							previousEntries,
							previousSignerPublicKeys,
							entryPublicKeys,
						)
					) {
						return true;
					}
				}
				return false;
		}
	}

	private async nativeDeleteOperationPolicyAllows(
		descriptor: CanPerformPolicyDescriptor,
		operation: DeleteOperation,
		entryPublicKeys?: readonly PublicSignKey[],
	): Promise<boolean> {
		switch (descriptor.kind) {
			case "allowAll":
				return true;
			case "signedByPublicKey":
				return entryPublicKeys
					? this.nativeFieldValueMatchesPublicKeys(
							descriptor.publicKey,
							entryPublicKeys,
						)
					: this.nativeFieldValueMatchesLocalPublicKey(descriptor.publicKey);
			case "delete":
				return this.nativeDeleteOperationPolicyAllows(
					descriptor.policy,
					operation,
					entryPublicKeys,
				);
			case "deleteSignedByExistingField": {
				const value = this.getNativeDeletePolicyFieldValue(
					operation,
					descriptor.path,
				);
				return entryPublicKeys
					? this.nativeFieldValueMatchesPublicKeys(value, entryPublicKeys)
					: this.nativeFieldValueMatchesLocalPublicKey(value);
			}
			case "and":
				for (const policy of descriptor.policies) {
					if (
						!(await this.nativeDeleteOperationPolicyAllows(
							policy,
							operation,
							entryPublicKeys,
						))
					) {
						return false;
					}
				}
				return true;
			case "or":
				for (const policy of descriptor.policies) {
					if (
						await this.nativeDeleteOperationPolicyAllows(
							policy,
							operation,
							entryPublicKeys,
						)
					) {
						return true;
					}
				}
				return false;
			case "put":
			case "signedByField":
			case "sameSignersAsPrevious":
				return false;
		}
	}

	private assertNativeModePlainPutManySupported(
		docs: T[],
		options?: DocumentPutOptions,
	): void {
		if (!this.isNativeMode()) {
			return;
		}
		const unsupported = this.unsupportedNativePutOptions(options);
		if (this.immutable) {
			unsupported.push("immutable documents");
		}
		if (this.strictHistory) {
			unsupported.push("strict history");
		}
		if (this.compatibility === 6) {
			unsupported.push("legacy compatibility");
		}
		if (Program.isPrototypeOf(this._clazz)) {
			unsupported.push("program-valued document type");
		}
		if (!asTrustedDocumentIndex(this._index).canUseNativeBackboneContextualBatch()) {
			unsupported.push("native batch document index");
		}
		if (unsupported.length > 0) {
			throw this.nativeModeError(`does not support ${unsupported.join(", ")}`);
		}
	}

	private assertNativeModeDeleteSupported(options?: DocumentPutOptions): void {
		if (!this.isNativeMode()) {
			return;
		}
		const unsupported = this.unsupportedNativePutOptions(options);
		if (options?.unique !== undefined) {
			unsupported.push("unique delete");
		}
		if (this.immutable) {
			unsupported.push("immutable documents");
		}
		if (this.strictHistory) {
			unsupported.push("strict history");
		}
		if (this.compatibility === 6) {
			unsupported.push("legacy compatibility");
		}
		if (Program.isPrototypeOf(this._clazz)) {
			unsupported.push("program-valued document type");
		}
		if (unsupported.length > 0) {
			throw this.nativeModeError(`does not support ${unsupported.join(", ")}`);
		}
	}

	private async canPerformAllowsNativeDelete(properties: {
		operation: DeleteOperation;
		getExistingEntry: () => Promise<Entry<Operation>>;
		getExistingDocument?: () => MaybePromise<T | undefined>;
	}): Promise<boolean> {
		if (!this._optionCanPerform) {
			return true;
		}
		if (!this._optionCanPerformNativePolicy) {
			return false;
		}
		if (this.isNativeMode()) {
			return this.nativeDeleteOperationPolicyAllows(
				this._optionCanPerformNativePolicy,
				properties.operation,
			);
		}
		let deleteValue: T | undefined;
		if (
			canPerformPolicyNeedsDeleteValue(
				this._optionCanPerformNativePolicy,
			)
		) {
			deleteValue = await properties.getExistingDocument?.();
			if (deleteValue === undefined) {
				const existingEntry = await properties.getExistingEntry();
				const existingOperation = await existingEntry.getPayloadValue();
				if (isPutOperation(existingOperation)) {
					deleteValue = this._index.valueEncoding.decoder(existingOperation.data);
				}
			}
		}
		return createCanPerformDeletePolicyEvaluator(
			this._optionCanPerformNativePolicy,
			this.log.log.identity.publicKey,
		)(deleteValue);
	}

	private normalizeNativeModePutOptions(
		options: DocumentPutOptions | undefined,
	): DocumentPutOptions | undefined {
		if (!this.isNativeMode()) {
			return options;
		}
		if (options?.replicate === false && options.target === "none") {
			return options;
		}
		if (this._nativeModeReplicatedOpen) {
			return options?.target === "none"
				? options
				: {
						...options,
						target: "none",
					};
		}
		const cached = cachedNativeLocalPutOptions(options);
		if (cached) {
			return cached;
		}
		return {
			...options,
			replicate: false,
			target: "none",
		};
	}

	private trackDocumentChangeListeners(): void {
		if (this._documentChangeListenerTrackingInitialized) {
			return;
		}
		this._documentChangeListenerTrackingInitialized = true;
		this._documentChangeListeners ??= [];
		this._documentChangeListenerCount ??= 0;
		this._documentInternalChangeListenerCount ??= 0;
		const events = this.events;
		const addEventListener = events.addEventListener.bind(events);
		const removeEventListener = events.removeEventListener.bind(events);
		const captureFromOptions = (options: unknown): boolean =>
			typeof options === "boolean"
				? options
				: !!(options as { capture?: boolean } | undefined)?.capture;
		events.addEventListener = ((type: string, ...args: unknown[]) => {
			if (type === "change") {
				const listener = args[0];
				const capture = captureFromOptions(args[1]);
				if (
					listener &&
					!this._documentChangeListeners.some(
						(entry) => entry.listener === listener && entry.capture === capture,
					)
				) {
					this._documentChangeListeners.push({ listener, capture });
					this._documentChangeListenerCount =
						this._documentChangeListeners.length;
				}
			}
			return (addEventListener as (...innerArgs: unknown[]) => unknown)(
				type,
				...args,
			);
		}) as typeof events.addEventListener;
		events.removeEventListener = ((type: string, ...args: unknown[]) => {
			if (type === "change") {
				const listener = args[0];
				const capture = captureFromOptions(args[1]);
				const index = this._documentChangeListeners.findIndex(
					(entry) => entry.listener === listener && entry.capture === capture,
				);
				if (index >= 0) {
					this._documentChangeListeners.splice(index, 1);
					this._documentChangeListenerCount =
						this._documentChangeListeners.length;
				}
			}
			return (removeEventListener as (...innerArgs: unknown[]) => unknown)(
				type,
				...args,
			);
		}) as typeof events.removeEventListener;
	}

	private getLocalIndexedContext(
		key: indexerTypes.IdKey,
	): Promise<indexerTypes.IndexedResult<IndexedContextOnly<I>> | undefined> {
		const getContextById = (
			this._index.index as {
				getContextById?: (key: indexerTypes.IdKey) => Context | undefined;
			}
		).getContextById;
		const context = getContextById?.call(this._index.index, key);
		if (context) {
			return Promise.resolve({
				id: key,
				value: { __context: context } as IndexedContextOnly<I>,
			});
		}
		return this._index.index.get(key, {
			shape: INDEX_CONTEXT_SHAPE,
		}) as Promise<
			indexerTypes.IndexedResult<IndexedContextOnly<I>> | undefined
		>;
	}

	private getNativeEntrySignerPublicKeys(
		hashes: string[],
	): Array<Uint8Array | undefined> | undefined {
		if (hashes.length === 0) {
			return [];
		}
		const nativeGraph = this.log.log.entryIndex.properties.nativeGraph
			?.graph as
			| {
					entrySignaturePublicKeysBatch?: (
						hashes: Iterable<string>,
					) => Array<Uint8Array | undefined>;
			  }
			| undefined;
		return nativeGraph?.entrySignaturePublicKeysBatch?.(hashes);
	}

	private getSharedLogNativeBackbone<T>(): T | undefined {
		return (this.log as unknown as { _nativeBackbone?: T })._nativeBackbone;
	}

	private getNativePreviousEntrySignerPublicKey(
		key: indexerTypes.IdKey,
	): { exists: boolean; publicKey?: Uint8Array } | undefined {
		const nativeBackbone = this.getSharedLogNativeBackbone<
			| {
					documentPreviousSignaturePublicKey?: (
						key: string,
					) => { exists: boolean; publicKey?: Uint8Array } | undefined;
			  }
			| undefined
		>();
		return nativeBackbone?.documentPreviousSignaturePublicKey?.(
			documentIndexStoreKey(key),
		);
	}

	private async getNativePreviousEntrySignerPublicKeyForPutOperation(
		operation: PutOperation,
	): Promise<{ exists: boolean; publicKey?: Uint8Array } | undefined> {
		const keyValue = await this.getNativeDocumentIdFromPutOperation(operation);
		return keyValue == null
			? undefined
			: this.getNativePreviousEntrySignerPublicKey(indexerTypes.toId(keyValue));
	}

	private getNativeIndexedContext(
		key: indexerTypes.IdKey,
	):
		| indexerTypes.IndexedResult<IndexedContextOnly<I>>
		| undefined {
		const nativeBackbone = this.getSharedLogNativeBackbone<
			| {
					documentContext?: (
						key: string,
					) => [string, string, string, string, number] | undefined;
			  }
			| undefined
		>();
		const row = nativeBackbone?.documentContext?.(documentIndexStoreKey(key));
		const context = row
			? {
					created: BigInt(row[0]),
					modified: BigInt(row[1]),
					head: row[2],
					gid: row[3],
					size: row[4],
				}
			: undefined;
		return context
			? {
					id: key,
					value: {
						__context: nativeDocumentContextFactsAsContext(context),
					} as IndexedContextOnly<I>,
				}
			: undefined;
	}

	private getNativeModeIndexedContext(
		key: indexerTypes.IdKey,
	): indexerTypes.IndexedResult<IndexedContextOnly<I>> | undefined {
		if (!this.hasNativeDocumentContextLookup()) {
			throw this.nativeModeError("requires native document context lookup");
		}
		return this.getNativeIndexedContext(key);
	}

	private hasNativeDocumentContextLookup(): boolean {
		const nativeBackbone = this.getSharedLogNativeBackbone<
			| {
					documentContext?: (key: string) => unknown;
			  }
			| undefined
		>();
		return typeof nativeBackbone?.documentContext === "function";
	}

	private getNativeIndexedContextsAndPreviousSignerPublicKeys(
		keys: indexerTypes.IdKey[],
	):
		| {
				contexts: Array<
					indexerTypes.IndexedResult<IndexedContextOnly<I>> | undefined
				>;
				publicKeys: Array<Uint8Array | undefined>;
		  }
		| undefined {
		if (keys.length === 0) {
			return { contexts: [], publicKeys: [] };
		}
		const nativeBackbone = this.getSharedLogNativeBackbone<
			| {
					documentContextsAndPreviousSignaturePublicKeys?: (
						keys: string[],
					) =>
						| Array<{
								context?: {
									created: bigint;
									modified: bigint;
									head: string;
									gid: string;
									size: number;
								};
								publicKey?: Uint8Array;
						  }>
						| undefined;
			  }
			| undefined
		>();
		const rows =
			nativeBackbone?.documentContextsAndPreviousSignaturePublicKeys?.(
				keys.map(documentIndexStoreKey),
			);
		if (!rows) {
			return;
		}
		return {
			contexts: rows.map((row, index) =>
				row.context
					? {
							id: keys[index]!,
							value: {
								__context: nativeDocumentContextFactsAsContext(
									row.context,
								),
							} as IndexedContextOnly<I>,
						}
					: undefined,
			),
			publicKeys: rows.map((row) => row.publicKey),
		};
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

	private documentFromIdentityIndexedValue(
		indexed: indexerTypes.IndexedResult<WithContext<I>> | undefined,
	): T | undefined {
		const value = indexed?.value as unknown as WithContext<T> | undefined;
		if (!value) {
			return;
		}
		if (typeof value !== "object") {
			return value as unknown as T;
		}
		const document = Object.assign(
			Object.create(this._clazz.prototype),
			value,
		) as WithContext<T>;
		delete (document as Partial<WithContext<T>>).__context;
		return document as unknown as T;
	}

	private async getLocalIdentityDocumentByHead(
		head: string,
	): Promise<T | undefined> {
		return this.documentFromIdentityIndexedValue(
			await this._index.getIdentityIndexedByHead(head),
		);
	}

	private async getLocalIndexedDocumentForNativeDeletePolicy(
		key: indexerTypes.IdKey,
	): Promise<T | undefined> {
		if (!this._optionCanPerformNativePolicy) {
			return;
		}
		const fieldPaths = canPerformPolicyDeleteFieldPaths(
			this._optionCanPerformNativePolicy,
		);
		if (
			fieldPaths.length === 0 ||
			!this._index.canReadOriginalFieldPathsFromIndexedValue(fieldPaths)
		) {
			return;
		}
		return (await this._index.get(key, {
			local: true,
			remote: false,
			resolve: false,
		} as GetOptions<T, I, D, false>)) as unknown as T | undefined;
	}

	private getNativeDeletePolicyFieldValue(
		operation: DeleteOperation,
		path: string | readonly string[],
	): unknown {
		const key =
			operation.key instanceof indexerTypes.IdKey
				? operation.key
				: indexerTypes.toId(operation.key);
		return this._index.getNativeIndexedFieldValue(key, path);
	}

	get changes() {
		return this.events;
	}

	private async maybeSubprogramOpen(value: T & Program): Promise<T & Program> {
		if (await this.canOpen!(value)) {
			return (await this.node.open(value, {
				parent: this as Program<any, any>,
				existing: "reuse",
			})) as unknown as T & Program;
		}

		return value;
	}
	private keepCache: Set<string> | undefined = undefined;
	async open(options: SetupOptions<T, I, D>) {
		this.trackDocumentChangeListeners();
		// Deserialized instances skip constructor/field initializers (borsh creates
		// objects via Object.create), so re-establish constructor-only state here.
		this._canAppendDecodedDocuments ??= new WeakMap<PutOperation, T>();
		this._clazz = options.type;
		this._valueClassIsProgram = Program.isPrototypeOf(this._clazz);
		this.canOpen = options.canOpen;
		this._mode = options.mode ?? "auto";
		this._nativeModeReplicatedOpen =
			this.isNativeMode() &&
			options.replicate !== undefined &&
			options.replicate !== false;
		this.assertNativeModeOpenOptions(options);

		if (Program.isPrototypeOf(this._clazz)) {
			if (!this.canOpen) {
				throw new Error(
					"Document store needs to be opened with canOpen option when the document type is a Program",
				);
			}
		}

		this._optionCanPerform = options.canPerform;
		this._optionCanPerformNativePolicy = getCanPerformPolicyDescriptor(
			options.canPerform,
		);
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
		this._hasLogTrim = options.log?.trim != null;

		const changeListenersBeforeIndexOpen = this._documentChangeListenerCount;
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
			immutable: this.immutable,
		});
		this._documentInternalChangeListenerCount = Math.max(
			0,
			this._documentChangeListenerCount - changeListenersBeforeIndexOpen,
		);
		this._nativeDocumentFieldExtractionPlans ??= new Map();
		this._nativeDocumentFieldExtractionPlans.clear();
		this._nativeDocumentIdExtractionPlan =
			asTrustedDocumentIndex(this._index).getNativeDocumentFieldExtractionPlan(idProperty);

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
				// Only the signer identity matters here. Use getPublicKeys so
				// prepared native entries (hollow in JS, signature bytes in the
				// native store) can still answer; if the signer is genuinely
				// unknowable we cannot prove the entry is ours, so do not keep.
				const resolveKeys = async (entry: Entry<Operation>) => {
					try {
						return await entry.getPublicKeys();
					} catch {
						return undefined;
					}
				};
				let publicKeys: PublicSignKey[] | undefined = undefined;
				if (e instanceof Entry) {
					publicKeys = await resolveKeys(e);
				} else {
					const entry = await this.log.log.get(e.hash);
					publicKeys = entry ? await resolveKeys(entry) : undefined;
				}

				if (!publicKeys) {
					return false;
				}

				for (const publicKey of publicKeys) {
					if (publicKey.equals(this.node.identity.publicKey)) {
						this.keepCache?.add(e.hash);
						return true;
					}
				}
				return false;
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
			appendDurability: options?.appendDurability,
			nativeBackbone: options?.nativeBackbone,
			nativeGraph: options?.nativeGraph,
			nativeRangePlanner: options?.nativeRangePlanner,
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
			domain: options?.domain
				? () => options.domain!(this) as unknown as D
				: undefined,
			compatibility: logCompatiblity,
			eagerBlocks: options?.eagerBlocks,
			fanout: options?.fanout,
			keep: keepFunction,
		});
		this._nativeBackboneDocumentIndexEnabled = false;
		if (
			this._mode !== "compat" &&
			options?.nativeBackbone &&
			typeof options.nativeBackbone !== "boolean" &&
			options.nativeBackbone.documentIndex === true
		) {
			this._nativeBackboneDocumentIndexEnabled =
				asTrustedDocumentIndex(this._index).attachNativeBackboneDocumentIndex(
					this.getSharedLogNativeBackbone(),
					{ preserveExisting: this._mode === "native" },
				) === true;
			if (this._nativeBackboneDocumentIndexEnabled) {
				await initializeDocumentRust();
			}
		}

		this._optionCanPerformNativeFastPath = this._optionCanPerformNativePolicy
			? createCanPerformPolicyEvaluator(
					this._optionCanPerformNativePolicy,
					this.log.log.identity.publicKey,
				)
			: undefined;
		this.assertNativeModeReady();
		this._documentBackend = this.createDocumentBackend();
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
			if (this._optionCanPerform) {
				if (this._optionCanPerformNativePolicy && this.isNativeMode()) {
					return this.nativeCanPerformAllowsAppend(
						this._optionCanPerformNativePolicy,
						operation,
						entry,
						reference?.document,
					);
				}
				let document: T | undefined = reference?.document;
				if (!document) {
					if (isPutOperation(l0)) {
						document =
							this._canAppendDecodedDocuments.get(l0) ??
							this._index.valueEncoding.decoder(l0.data);
						if (!document) {
							return false;
						}
					} else if (isDeleteOperation(l0)) {
						// Nothing to do here by default.
						// Checking if the document exists is not necessary since it
						// might already be deleted.
					} else {
						throw new Error("Unsupported operation");
					}
				}
				const previousEntries =
					this._optionCanPerformNativePolicy &&
					isPutOperation(operation) &&
					canPerformPolicyNeedsPreviousEntries(
						this._optionCanPerformNativePolicy,
					)
						? await this.resolveCanPerformPreviousEntries(entry)
						: undefined;
				const deleteValue =
					this._optionCanPerformNativePolicy &&
					isDeleteOperation(operation) &&
					canPerformPolicyNeedsDeleteValue(
						this._optionCanPerformNativePolicy,
					)
						? await this.resolveCanPerformDeleteValue(operation)
						: undefined;
				if (
					!(await this._optionCanPerform(
						isPutOperation(operation)
							? {
									type: "put",
									value: document!,
									operation,
									entry: entry as unknown as Entry<PutOperation>,
									previousEntries,
								}
							: {
									type: "delete",
									value: deleteValue,
									operation,
									entry: entry as unknown as Entry<DeleteOperation>,
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

	private async nativeCanPerformAllowsAppend(
		descriptor: CanPerformPolicyDescriptor,
		operation: PutOperation | DeleteOperation,
		entry: Entry<Operation>,
		document: T | undefined,
	): Promise<boolean> {
		if (isPutOperation(operation)) {
			let previousSignerPublicKeys: Uint8Array[] = [];
			let previousEntries: Entry<Operation>[] = [];
			if (canPerformPolicyNeedsPreviousEntries(descriptor)) {
				const lookup = this.getNativeEntrySignerPublicKeys(entry.meta.next);
				if (lookup && lookup.every((key) => key != null)) {
					previousSignerPublicKeys = lookup as Uint8Array[];
				} else if (this.isNativeMode()) {
					if (entry.meta.next.length > 1) {
						return false;
					}
					const previousSigner =
						await this.getNativePreviousEntrySignerPublicKeyForPutOperation(
							operation,
						);
					if (previousSigner?.publicKey) {
						previousSignerPublicKeys = [previousSigner.publicKey];
					} else if (previousSigner?.exists || entry.meta.next.length > 0) {
						return false;
					}
				} else {
					previousEntries = await this.resolveCanPerformPreviousEntries(entry);
				}
			}
			const entryPublicKeys =
				!document && canPerformPolicyPutNeedsEntryPublicKeys(descriptor)
					? entry.publicKeys.length > 0
						? entry.publicKeys
						: await entry.getPublicKeys()
					: [];
			return this.nativePutOperationPolicyAllows(
				descriptor,
				operation,
				document,
				previousEntries,
				previousSignerPublicKeys,
				entryPublicKeys,
			);
		}
		const entryPublicKeys = this.nativeDeletePolicyNeedsEntryPublicKeys(
			descriptor,
		)
			? entry.publicKeys.length > 0
				? entry.publicKeys
				: await entry.getPublicKeys()
			: undefined;
		return this.nativeDeleteOperationPolicyAllows(
			descriptor,
			operation,
			entryPublicKeys,
		);
	}

	private async resolveCanPerformPreviousEntries(
		entry: Entry<Operation>,
	): Promise<Entry<Operation>[]> {
		const entries: Entry<Operation>[] = [];
		for (const hash of entry.meta.next) {
			const previous = await this._resolveEntry(hash);
			if (previous) {
				entries.push(previous);
			}
		}
		return entries;
	}

	private async resolveCanPerformDeleteValue(
		operation: DeleteOperation,
		options?: { allowEntryFallback?: boolean },
	): Promise<T | undefined> {
		const key =
			operation.key instanceof indexerTypes.IdKey
				? operation.key
				: indexerTypes.toId(operation.key);
		const existing = await this.getLocalIndexedContext(key);
		const existingHead = this.getExistingContext(existing)?.head;
		if (!existingHead) {
			return;
		}
		const indexedDocument =
			await this.getLocalIdentityDocumentByHead(existingHead);
		if (indexedDocument) {
			return indexedDocument;
		}
		const indexedPolicyDocument =
			await this.getLocalIndexedDocumentForNativeDeletePolicy(key);
		if (indexedPolicyDocument) {
			return indexedPolicyDocument;
		}
		if (options?.allowEntryFallback === false) {
			return;
		}
		const existingEntry = await this._resolveEntry(existingHead, {
			remote: true,
		});
		const existingOperation = await existingEntry.getPayloadValue();
		if (!isPutOperation(existingOperation)) {
			return;
		}
		return this._index.valueEncoding.decoder(existingOperation.data);
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

		let initialized = false;
		const ensureInitialized = () => {
			if (!initialized) {
				entry.init({
					encoding: this.log.log.encoding,
					keychain: this.node.services.keychain,
				});
				initialized = true;
			}
		};
		try {
			const operation =
				reference?.operation ||
				(await this.getAppendOperation(entry, ensureInitialized));
			if (!operation) {
				return false;
			}
			if (isPutOperation(operation)) {
				// check nexts
				const putOperation = operation as PutOperation;
				let keyValue: indexerTypes.Ideable | undefined;
				if (reference?.document) {
					keyValue = this.idResolver(reference.document);
				} else {
					keyValue = await this.getNativeDocumentIdFromPutOperation(putOperation);
					if (keyValue == null) {
						if (this.isNativeMode()) {
							return false;
						}
						const value = this.index.valueEncoding.decoder(putOperation.data);
						this._canAppendDecodedDocuments.set(putOperation, value);
						keyValue = this.idResolver(value);
					}
				}

				const key = indexerTypes.toId(keyValue);

				const existingDocument = this.isNativeMode()
					? this.hasNativeDocumentContextLookup()
						? this.getNativeIndexedContext(key)
						: undefined
					: this.immutable
						? (
								await this.index.getDetailed(key, {
									resolve: false,
									local: true,
									remote: { strategy: "fallback" },
								})
							)?.[0]?.results[0]
						: await this.getLocalIndexedContext(key);
				if (this.isNativeMode() && !this.hasNativeDocumentContextLookup()) {
					return false;
				}
				const existingContext = this.getExistingContext(existingDocument);
				if (existingContext && existingContext.head !== entry.hash) {
					// This can happen if we reset the operation log without resetting the index, for example during recover.
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
							if (entry.meta.next[0] === existingContext.head) {
								return putOperation;
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
					// Keep existing behavior: next pointers may express document dependencies.
				}
			} else if (isDeleteOperation(operation)) {
				if (entry.meta.next.length !== 1) {
					return false;
				}
				const deleteKey =
					operation.key instanceof indexerTypes.IdKey
						? operation.key
						: indexerTypes.toId(operation.key);
				const existingDocument = this.isNativeMode()
					? this.hasNativeDocumentContextLookup()
						? this.getNativeIndexedContext(deleteKey)
						: undefined
					: this.immutable
						? (
								await this.index.getDetailed(operation.key, {
									resolve: false,
									local: true,
									remote: true,
								})
							)?.[0]?.results[0]
						: await this.getLocalIndexedContext(deleteKey);
				if (this.isNativeMode() && !this.hasNativeDocumentContextLookup()) {
					return false;
				}
				const existingHead = this.getExistingContext(existingDocument)?.head;

				if (!existingHead) {
					// already deleted
					return coerceDeleteOperation(operation); // assume ok
				}
				if (entry.meta.next[0] === existingHead) {
					return coerceDeleteOperation(operation);
				}
				if (this.isNativeMode()) {
					return false;
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

	private async getAppendOperation(
		entry: Entry<Operation>,
		ensureInitialized?: () => void,
	): Promise<Operation | undefined> {
		if (this.isNativeMode()) {
			const operation = await this.getPlainEntryOperationFromStorage(entry);
			if (operation) {
				return operation;
			}
			return;
		}
		ensureInitialized?.();
		try {
			return await entry.getPayloadValue();
		} catch (error) {
			// Auto (non-native document) mode with a native block store: the entry
			// materialized in the entry index can be a hollow shell whose in-memory
			// payload bytes were never loaded (the native store keeps the block
			// bytes at the storage layer, not on the JS entry). `getPayloadValue`
			// then throws "Missing data". The block itself is present, so recover
			// the operation via the storage-bytes / block-store read path. This is
			// a no-op for the pure-JS backend, where `getPayloadValue` succeeds.
			if (!isMissingPayloadDataError(error)) {
				throw error;
			}
			const operation = await this.getPlainEntryOperationFromStorage(entry);
			if (operation) {
				return operation;
			}
			throw error;
		}
	}

	private async getPlainEntryOperationFromStorage(
		entry: Entry<Operation>,
	): Promise<Operation | undefined> {
		let storageBytes: Uint8Array | undefined;
		try {
			storageBytes =
				Entry.getPreparedStorageBytes(entry) ?? entry.getStorageBytes();
		} catch {
			// The entry object is hollow (its payload bytes never materialized on
			// the JS side), so it cannot re-serialize itself. Fall through to the
			// block-store fallback below.
			storageBytes = undefined;
		}
		// Fall back to the raw block held by the block store, keyed by the entry
		// hash, whenever the entry could not (or did not) yield its own bytes.
		storageBytes ??= await this.getEntryStorageBytesFromBlocks(entry);
		if (!storageBytes) {
			return;
		}
		try {
			const payloadData =
				await entryV0PlainPayloadDataFromStorage(storageBytes);
			return payloadData
				? BORSH_ENCODING_OPERATION.decoder(payloadData)
				: undefined;
		} catch {
			try {
				const payloadData = (entry as { payload?: { data?: Uint8Array } })
					.payload?.data;
				return payloadData
					? BORSH_ENCODING_OPERATION.decoder(payloadData)
					: undefined;
			} catch {
				return;
			}
		}
	}

	private async getEntryStorageBytesFromBlocks(
		entry: Entry<Operation>,
	): Promise<Uint8Array | undefined> {
		const hash = entry.hash;
		if (!hash) {
			return;
		}
		try {
			const bytes = await this.log.log.blocks.get(hash);
			return bytes ?? undefined;
		} catch {
			return;
		}
	}

	private getNativeDocumentFieldExtractionPlan(
		path: string | readonly string[],
	): SimpleDocumentFieldExtractionPlan | undefined {
		const key = JSON.stringify(typeof path === "string" ? [path] : path);
		const plans = (this._nativeDocumentFieldExtractionPlans ??= new Map());
		if (plans.has(key)) {
			return plans.get(key);
		}
		const plan = asTrustedDocumentIndex(this._index).getNativeDocumentFieldExtractionPlan(path);
		plans.set(key, plan);
		return plan;
	}

	private async getNativeDocumentFieldFromPutOperation(
		operation: PutOperation,
		path: string | readonly string[],
	): Promise<string | number | bigint | Uint8Array | undefined> {
		if (!this.isNativeMode()) {
			return;
		}
		const plan = this.getNativeDocumentFieldExtractionPlan(path);
		if (!plan) {
			return;
		}
		try {
			return await extractDocumentFieldSimple(operation.data, plan);
		} catch {
			return;
		}
	}

	private async getNativeDocumentIdFromPutOperation(
		operation: PutOperation,
	): Promise<indexerTypes.Ideable | undefined> {
		if (!this.isNativeMode() || !this._nativeDocumentIdExtractionPlan) {
			return;
		}
		try {
			const id = await extractDocumentFieldSimple(
				operation.data,
				this._nativeDocumentIdExtractionPlan,
			);
			return id;
		} catch {
			return;
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

	public async put(
		doc: T,
		options?: DocumentPutOptions,
	): Promise<DocumentPutResult> {
		return this._documentBackend.put(doc, options);
	}

	private async putCompatDocumentBackend(
		doc: T,
		options?: DocumentPutOptions,
	): Promise<DocumentPutResult> {
		const putOptions = this.normalizeNativeModePutOptions(options);
		const prepared = this.canUsePlainPutFastPath(doc, putOptions)
			? this.preparePlainPut(doc)
			: this.preparePut(doc);
		let existingLocalContext:
			| indexerTypes.IndexedResult<IndexedContextOnly<I>>
			| null
			| undefined;
		let existingHead: string | undefined;
		if (!putOptions?.unique) {
			if (putOptions?.checkRemote) {
				existingHead = (
					await this._index.getDetailed(prepared.key, {
						resolve: false,
						local: true,
						remote: { replicate: putOptions?.replicate },
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
			putOptions,
		);
		if (plainPutPlan) {
			return this.commitPlainPutPlan(plainPutPlan, putOptions);
		}

		const operation =
			"operation" in prepared
				? prepared.operation
				: new PutOperation({ data: prepared.encodedDocument });
		const appended = await this.log.append(operation, {
			...putOptions,
			meta: {
				next: existingHead ? [await this._resolveEntry(existingHead)] : [],
				...putOptions?.meta,
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
					unique: putOptions?.unique,
					existing: existingLocalContext,
				});
			},
			replicate: putOptions?.replicate,
		});
		this.keepCache?.add(appended.entry.hash);
		return appended;
	}

	public async putMany(
		docs: T[],
		options?: DocumentPutOptions,
	): Promise<DocumentPutManyResult> {
		return this._documentBackend.putMany(docs, options);
	}

	private async putManyCompatDocumentBackend(
		docs: T[],
		options?: DocumentPutOptions,
	): Promise<DocumentPutManyResult> {
		if (docs.length === 0) {
			return { entries: [], removed: [] };
		}
		if (!this.canUsePlainPutManyFastPath(docs, options)) {
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
		for (const commit of documentAppendCommit.commits) {
			this.keepCache?.add(commit.append.hash);
		}
		return {
			get entries() {
				return documentAppendCommit.entries;
			},
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
		doc: T,
		options?: DocumentPutOptions,
	): boolean {
		return (
			this._mode !== "compat" &&
			this.canPerformAllowsPlainPutFastPath(doc) &&
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
			!options?.durability &&
			options?.deferIndexWrite === undefined &&
			!options?.meta?.type &&
			!(options?.meta && "data" in options.meta) &&
			!options?.meta?.next &&
			!options?.meta?.timestamp &&
			!options?.meta?.gidSeed &&
			options?.replicate !== true &&
			(!options?.target || options.target === "none") &&
			(options?.delivery === undefined || options.delivery === false) &&
			!options?.checkRemote &&
			options?.replicas === undefined
		);
	}

	private canUsePlainPutManyFastPath(
		docs: T[],
		options?: DocumentPutOptions,
	): boolean {
		return (
			options?.unique === true &&
			options?.replicate !== true &&
			options?.target === "none" &&
			(options?.delivery === undefined || options.delivery === false) &&
			docs.every((doc) => this.canUsePlainPutFastPath(doc, options))
		);
	}

	private async createPlainPutCommitPlan(
		prepared: PreparedPut<T> | PreparedPlainPut<T>,
		existingHead: string | undefined,
		existingLocalContext:
			| indexerTypes.IndexedResult<IndexedContextOnly<I>>
			| null
			| undefined,
		options: DocumentPutOptions | undefined,
		assumePlainPutFastPath = false,
	): Promise<PlainPutCommitPlan<T, I> | undefined> {
		if (
			("operation" in prepared &&
				!(prepared.operation instanceof PutOperation)) ||
			(!assumePlainPutFastPath &&
				!this.canUsePlainPutFastPath(prepared.document, options))
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
		const canCleanupTrimmedHeads = this._index.canGetIndexedKeyByHead();
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
			resolveTrimmedEntries: !canCleanupTrimmedHeads,
			useGenericChangeHandler:
				!options?.unique && existingLocalContext === undefined,
			unique: options?.unique,
			existing: existingLocalContext,
		};
	}

	private commitPlainPutPlan(
		plan: PlainPutCommitPlan<T, I>,
		options: DocumentPutOptions | undefined,
	): MaybePromise<{
		readonly entry: Entry<Operation>;
		removed: ShallowOrFullEntry<Operation>[];
	}> {
		return mapMaybePromise(
			this.commitNativeDocumentAppend({
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
			}),
			(documentAppendCommit) => {
				const handled = plan.useGenericChangeHandler
					? this.handleChanges(
							{
								added: [{ head: true, entry: documentAppendCommit.entry }],
								removed: documentAppendCommit.removed,
							},
							{
								document: plan.document,
								operation:
									documentAppendCommit.operation ??
									plan.operation ??
									new PutOperation({ data: plan.encodedDocument }),
								key: plan.key,
								unique: plan.unique,
								existing: plan.existing,
							},
						)
					: this.handlePreparedPlainPutCommit(documentAppendCommit);
				return mapMaybePromise(handled, () => {
					this.keepCache?.add(documentAppendCommit.append.hash);
					return {
						get entry() {
							return documentAppendCommit.entry;
						},
						removed: documentAppendCommit.removed,
					};
				});
			},
		);
	}

	private commitNativeDocumentAppend(
		input: NativeDocumentAppendCommitInput<T, I>,
	): MaybePromise<NativeDocumentAppendTransaction<T, I>> {
		const trustedLog = asTrustedDocumentSharedLog(this.log);
		const appendOptions = {
			...input.options,
			meta: {
				next: input.next,
				...input.options?.meta,
			},
			replicate: input.options?.replicate,
		};
		const prepareNativeDocumentIndexWithAppendFacts =
			this.createNativeBackboneDocumentIndexAppendFactsPreparer(input);
		const preferAppendFactsDocumentIndex =
			this.isNativeMode() && !!prepareNativeDocumentIndexWithAppendFacts;
		return mapMaybePromise(
			preferAppendFactsDocumentIndex
				? undefined
				: this.prepareNativeBackboneDocumentIndexCommit(input),
			(nativeDocumentIndexCommit) => {
				let committedNativeDocumentIndex = nativeDocumentIndexCommit;
				const prepareNativeDocumentIndexWithAppendFactsForCommit =
					nativeDocumentIndexCommit
						? undefined
						: prepareNativeDocumentIndexWithAppendFacts;
				if (
					this.isNativeMode() &&
					!nativeDocumentIndexCommit &&
					!prepareNativeDocumentIndexWithAppendFactsForCommit
				) {
					throw this.nativeModeError("requires native document-index commit");
				}
				const appendProperties = {
					skipMissingNextJoin: input.skipMissingNextJoin,
					resolveTrimmedEntries: input.resolveTrimmedEntries,
					payloadData: input.operationPayloadBytes,
					useNativeExistingDocumentContext:
						input.useNativeExistingDocumentContext,
					...(nativeDocumentIndexCommit
						? {
								nativeBackboneDocumentIndex:
									this.toNativeBackboneDocumentIndexCommitInput(
										input,
										nativeDocumentIndexCommit,
									),
							}
						: {}),
					...(prepareNativeDocumentIndexWithAppendFactsForCommit
						? {
								prepareNativeBackboneDocumentIndex: (
									facts: NativeBackboneDocumentIndexAppendFactsInput,
								) => {
									committedNativeDocumentIndex =
										prepareNativeDocumentIndexWithAppendFactsForCommit(facts);
									return committedNativeDocumentIndex
										? this.toNativeBackboneDocumentIndexCommitInput(
												input,
												committedNativeDocumentIndex,
											)
										: undefined;
								},
							}
						: {}),
				};
				if (input.operation) {
					if (this.isNativeMode()) {
						throw this.nativeModeError(
							"requires payload-backed put operations",
						);
					}
					return mapMaybePromise(
						trustedLog.appendLocallyPrepared(
							input.operation,
							appendOptions,
							appendProperties,
						),
						(appended) =>
							this.createNativeCheckedDocumentAppendCommitFacts(
								input,
								appended,
								committedNativeDocumentIndex,
							),
					);
				}
				const commitOnlyAppend = this.isNativeMode()
					? trustedLog.appendStrictNativeDocumentPayloadCommitOnly(
							input.operationPayloadBytes,
							appendOptions,
							appendProperties,
						)
					: trustedLog.appendLocallyPreparedPayloadCommitOnly(
							input.operationPayloadBytes,
							appendOptions,
							appendProperties,
						);
				return mapMaybePromise(
					commitOnlyAppend,
					(commitOnly) => {
						if (commitOnly) {
							return this.createNativeCheckedDocumentAppendCommitFacts(
								input,
								commitOnly,
								committedNativeDocumentIndex,
							);
						}
						if (this.isNativeMode()) {
							throw this.nativeModeError(
								"requires native payload commit-only append",
							);
						}
						return this.commitNativeDocumentAppendPayloadFallback(
							input,
							appendOptions,
							appendProperties,
							committedNativeDocumentIndex,
						);
					},
				);
			},
		);
	}

	private createNativeCheckedDocumentAppendCommitFacts(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		appended: NativeDocumentAppendResult,
		nativeBackboneDocumentIndex?: PreparedNativeBackboneDocumentIndexCommit<I>,
	): MaybePromise<NativeDocumentAppendTransaction<T, I>> {
		return mapMaybePromise(
			this.createDocumentAppendCommitFacts(
				input,
				appended,
				nativeBackboneDocumentIndex,
			),
			(commit) => {
				this.assertNativeModeDocumentAppendCommit(commit);
				return commit;
			},
		);
	}

	private assertNativeModeDocumentAppendCommit(
		commit: DocumentAppendCommitFacts<T, I>,
	): void {
		if (!this.isNativeMode()) {
			return;
		}
		if (!commit.nativeBackboneDocumentIndexCommitted) {
			throw this.nativeModeError("requires native document-index commit");
		}
	}

	private toNativeBackboneDocumentIndexCommitInput(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		commit: PreparedNativeBackboneDocumentIndexCommit<I>,
		useLatestContext = false,
		): NativeBackboneDocumentIndexCommitInput {
		const canUsePlainPutPayload =
			commit.usePlainPutPayload === true ||
			(!!input.operationPayloadBytes && !!commit.projection);
		return {
			key: documentIndexStoreKey(input.key),
			valuePrefixBytes: commit.valuePrefixBytes,
			usePlainPutPayload: canUsePlainPutPayload,
			projection: commit.projection,
			existingCreated:
				input.unique || input.existing === null
					? undefined
					: input.existing?.value.__context.created,
			deleteTrimmedHeads:
				!this.hasDocumentChangeConsumers() &&
				this._index.canGetIndexedKeyByHead(),
			useLatestContext,
			requiredPreviousSignerPublicKey:
				input.requiredPreviousSignerPublicKey,
		};
	}

	private prepareNativeBackboneDocumentIndexCommit(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
	): MaybePromise<PreparedNativeBackboneDocumentIndexCommit<I> | undefined> {
		if (!this._nativeBackboneDocumentIndexEnabled) {
			return;
		}
		return asTrustedDocumentIndex(this._index).prepareNativeBackboneDocumentIndexCommit(
			input.document,
			input.documentBytes,
			{ entryPublicKeys: [this.log.log.identity.publicKey] },
		);
	}

	private createNativeBackboneDocumentIndexAppendFactsPreparer(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
	):
		| ((
				facts: NativeBackboneDocumentIndexAppendFactsInput,
		  ) => PreparedNativeBackboneDocumentIndexCommit<I> | undefined)
		| undefined {
		if (
			!this._nativeBackboneDocumentIndexEnabled ||
			!asTrustedDocumentIndex(this._index).canPrepareNativeBackboneDocumentIndexCommitWithAppendFacts()
		) {
			return;
		}
		const existing =
			input.unique || input.existing === null ? null : input.existing;
		return (facts) => {
			const appendFacts: NativeBackboneDocumentIndexAppendFacts = {
				wallTime: BigInt(facts.wallTime),
				gid: facts.gid,
				payloadSize: facts.payloadSize,
			};
			const context = nativeDocumentContextFactsAsContext({
				created: existing?.value.__context.created || appendFacts.wallTime,
				modified: appendFacts.wallTime,
				head: "",
				gid: appendFacts.gid,
				size: appendFacts.payloadSize,
			});
			return asTrustedDocumentIndex(this._index).prepareNativeBackboneDocumentIndexCommitWithAppendFacts(
				input.document,
				input.documentBytes,
				context,
				{ entryPublicKeys: [this.log.log.identity.publicKey] },
			);
		};
	}

	private async commitNativeDocumentAppendPayloadFallback(
		input: NativeDocumentAppendCommitInput<T, I>,
		appendOptions: SharedAppendOptions<Operation>,
		appendProperties: {
			skipMissingNextJoin: boolean;
			resolveTrimmedEntries: boolean;
			payloadData: Uint8Array;
			prepareNativeBackboneDocumentIndex?: (
				facts: NativeBackboneDocumentIndexAppendFactsInput,
			) => NativeBackboneDocumentIndexCommitInput | undefined;
		},
		nativeBackboneDocumentIndex?: PreparedNativeBackboneDocumentIndexCommit<I>,
	): Promise<NativeDocumentAppendTransaction<T, I>> {
		if (this.isNativeMode()) {
			throw this.nativeModeError("requires native payload append support");
		}
		const trustedLog = asTrustedDocumentSharedLog(this.log);
		let appended: NativeDocumentAppendResult;
		try {
			appended = await trustedLog.appendLocallyPreparedPayload(
				input.operationPayloadBytes,
				appendOptions,
				appendProperties,
			);
		} catch (error) {
			if (
				!(error instanceof Error) ||
				error.message !==
					"appendLocallyPrepared payload-only path requires native append support"
			) {
				throw error;
			}
			appended = await trustedLog.appendLocallyPrepared(
				new PutOperation({ data: input.documentBytes }),
				appendOptions,
				appendProperties,
			);
		}
		return this.createDocumentAppendCommitFacts(
			input,
			appended,
			nativeBackboneDocumentIndex,
		);
	}

	private async commitNativeDocumentAppendMany(
		input: NativeDocumentAppendManyCommitInput<T, I>,
	): Promise<DocumentAppendManyCommitFacts<T, I> | undefined> {
		const trustedLog = asTrustedDocumentSharedLog(this.log);
		const nativeBackboneDocumentIndexes =
			await this.prepareNativeBackboneDocumentIndexCommitBatch(input.puts);
		const nativeBackboneDocumentIndexInputs =
			nativeBackboneDocumentIndexes?.map((commit, index) =>
				this.toNativeBackboneDocumentIndexCommitInput(
					input.puts[index]!,
					commit,
					input.useNativeExistingDocumentContext === true,
				),
			);
		const nexts = input.puts.map((put) => {
			if (input.useNativeExistingDocumentContext === true) {
				return [];
			}
			const existing =
				put.unique || put.existing === null ? null : put.existing;
			if (!existing) {
				return [];
			}
			const context = existing.value.__context;
			const next = this.nextFromIndexedContext(context.head, existing);
			if (!next) {
				throw this.nativeModeError(
					"requires indexed document context for non-unique putMany",
				);
			}
			return [next];
		});
		const appended = await trustedLog.appendLocallyPreparedPayloadsManyIndependent(
			input.puts.map((put) => put.operationPayloadBytes),
			{
				...input.options,
				replicate: input.options?.replicate,
			},
			{
				resolveTrimmedEntries: input.resolveTrimmedEntries,
				nexts,
				nativeBackboneDocumentIndexes:
					nativeBackboneDocumentIndexInputs,
				retainMaterializationBytes: this._hasLogTrim,
			},
		);
		if (!appended) {
			if (this.isNativeMode()) {
				throw this.nativeModeError(
					"requires native batched payload append support",
				);
			}
			return undefined;
		}
		const appendInputs = input.puts.map((put, index) => ({
			input: nativeBackboneDocumentIndexes?.[index]
				? {
						...put,
						nativeBackboneDocumentIndex:
							nativeBackboneDocumentIndexes[index],
					}
				: put,
			appended: (() => {
				const materializeEntry = appended.materializeEntries?.[index];
				let entry: Entry<Operation> | undefined;
				return {
					get entry() {
						return (entry ??= materializeEntry
							? materializeEntry()
							: appended.entries[index]!);
					},
					removed: [],
					appendCommit: appended.appendCommits[index]!,
				};
			})(),
		}));
		const commits =
			await this.createDocumentAppendCommitFactsBatch(appendInputs);
		let entries: Entry<Operation>[] | undefined;
		return {
			get entries() {
				return (entries ??= commits.map((commit) => commit.entry));
			},
			removed: appended.removed,
			commits,
		};
	}

	private prepareNativeBackboneDocumentIndexCommitBatch(
		inputs: NativeDocumentAppendCommitFactsInput<T, I>[],
	):
		| PreparedNativeBackboneDocumentIndexCommit<I>[]
		| Promise<PreparedNativeBackboneDocumentIndexCommit<I>[] | undefined>
		| undefined {
		if (!this._nativeBackboneDocumentIndexEnabled || inputs.length === 0) {
			return;
		}
		const commits: PreparedNativeBackboneDocumentIndexCommit<I>[] = [];
		const finishAsync = (
			firstAsyncIndex: number,
			firstAsyncCommit: Promise<
				PreparedNativeBackboneDocumentIndexCommit<I> | undefined
			>,
		) =>
			Promise.all([
				firstAsyncCommit,
				...inputs
					.slice(firstAsyncIndex + 1)
					.map((input) =>
						this.prepareNativeBackboneDocumentIndexCommit(input),
					),
			]).then((resolvedCommits) => {
				for (const commit of resolvedCommits) {
					if (!commit) {
						return;
					}
					commits.push(commit);
				}
				return commits;
			});
		for (let i = 0; i < inputs.length; i++) {
			const commit = this.prepareNativeBackboneDocumentIndexCommit(
				inputs[i]!,
			);
			if (isPromiseLike(commit)) {
				return finishAsync(i, commit);
			}
			if (!commit) {
				return;
			}
			commits.push(commit);
		}
		return commits;
	}

	private createDocumentAppendCommitFacts(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		appended: NativeDocumentAppendResult,
		nativeBackboneDocumentIndex?: PreparedNativeBackboneDocumentIndexCommit<I>,
	): MaybePromise<NativeDocumentAppendTransaction<T, I>> {
		const append = appended.appendCommit;
		const nativePreviousContext =
			append.documentPreviousContext == null
				? undefined
				: nativeDocumentContextFactsAsContext(append.documentPreviousContext);
		const nativePreviousIndexedContext = nativePreviousContext
			? ({
					id: input.key,
					value: {
						__context: nativePreviousContext,
					} as IndexedContextOnly<I>,
				} satisfies indexerTypes.IndexedResult<IndexedContextOnly<I>>)
			: undefined;
		const inputWithExisting =
			input.existing === undefined && nativePreviousIndexedContext
				? {
						...input,
						existing: nativePreviousIndexedContext,
					}
				: input;
		const existing =
			inputWithExisting.unique || inputWithExisting.existing === null
				? null
				: inputWithExisting.existing;
		const contextInput = {
			existingCreated: existing?.value.__context.created,
			modified: append.wallTime,
			head: append.hash,
			gid: append.gid,
			size: append.payloadSize,
		};
		if (append.nativeBackboneDocumentIndexCommitted) {
			return this.createDocumentAppendCommitFactsWithLazyContext(
				inputWithExisting,
				appended,
				contextInput,
				nativeBackboneDocumentIndex,
			);
		}
		const contextPlan = tryPlanDocumentContext(contextInput);
		if (contextPlan) {
			return this.createDocumentAppendCommitFactsWithContext(
				inputWithExisting,
				appended,
				contextPlan,
				nativeBackboneDocumentIndex,
			);
		}
		return planDocumentContext(contextInput).then((plannedContext) =>
			plannedContext
				? this.createDocumentAppendCommitFactsWithContext(
						inputWithExisting,
						appended,
						plannedContext,
						nativeBackboneDocumentIndex,
					)
				: this.createDocumentAppendCommitFactsWithLazyContext(
						inputWithExisting,
						appended,
						contextInput,
						nativeBackboneDocumentIndex,
					),
		);
	}

	private createDocumentAppendCommitFactsWithLazyContext(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		appended: NativeDocumentAppendResult,
		contextInput: {
			existingCreated?: bigint | number | string | null;
			modified: bigint | number | string;
			head: string;
			gid: string;
			size: number;
		},
		nativeBackboneDocumentIndex = input.nativeBackboneDocumentIndex,
	): NativeDocumentAppendTransaction<T, I> {
		let contextValues:
			| {
					created: bigint;
					modified: bigint;
					head: string;
					gid: string;
					size: number;
			  }
			| undefined;
		let context: Context | undefined;
		let contextBytes: Uint8Array | undefined;
		const getContextValues = () => {
			if (contextValues) {
				return contextValues;
			}
			const modified = toContextBigInt(contextInput.modified);
			const existingCreated =
				contextInput.existingCreated == null
					? undefined
					: toContextBigInt(contextInput.existingCreated);
			return (contextValues = {
				created:
					existingCreated == null || existingCreated === 0n
						? modified
						: existingCreated,
				modified,
				head: contextInput.head,
				gid: contextInput.gid,
				size: contextInput.size,
			});
		};
		const getContext = () => (context ??= new Context(getContextValues()));
		const getContextBytes = () =>
			(contextBytes ??= encodeDocumentContextSuffix(getContext()));
		return this.createNativeDocumentAppendTransaction(
			input,
			appended,
			{
				getContext,
				getContextBytes,
			},
			nativeBackboneDocumentIndex,
		);
	}

	private createNativeDocumentAppendTransaction(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		appended: NativeDocumentAppendResult,
		contextAccessors: NativeDocumentAppendTransactionContext,
		nativeBackboneDocumentIndex = input.nativeBackboneDocumentIndex,
	): NativeDocumentAppendTransaction<T, I> {
		const append = appended.appendCommit;
		let contextualEncodedValueParts: ContextualEncodedValueParts | undefined;
		let exposedNativeBackboneDocumentIndex:
			| NativeDocumentAppendTransaction<T, I>["nativeBackboneDocumentIndex"]
			| undefined;
		let nativeBackboneDocumentIndexContextSet = false;
		const ensureNativeBackboneDocumentIndexContext = () => {
			if (
				nativeBackboneDocumentIndexContextSet ||
				!nativeBackboneDocumentIndex?.setContext
			) {
				return;
			}
			nativeBackboneDocumentIndex.setContext(contextAccessors.getContext());
			nativeBackboneDocumentIndexContextSet = true;
		};
		const getNativeBackboneDocumentIndex = () => {
			if (!nativeBackboneDocumentIndex) {
				return;
			}
			return (exposedNativeBackboneDocumentIndex ??= {
				valuePrefixBytes: nativeBackboneDocumentIndex.valuePrefixBytes,
				projection: nativeBackboneDocumentIndex.projection,
				indexable: nativeBackboneDocumentIndex.indexable,
				getIndexable: nativeBackboneDocumentIndex.getIndexable
					? () => {
							ensureNativeBackboneDocumentIndexContext();
							return nativeBackboneDocumentIndex.getIndexable!();
						}
					: undefined,
				setContext: nativeBackboneDocumentIndex.setContext
					? (context) => {
							nativeBackboneDocumentIndex.setContext!(context);
							nativeBackboneDocumentIndexContextSet = true;
						}
					: undefined,
			});
		};
		return {
			document: input.document,
			key: input.key,
			operation: input.operation,
			encodedDocument: input.documentBytes,
			operationPayloadBytes: input.operationPayloadBytes,
			get entry() {
				return appended.entry;
			},
			removed: appended.removed,
			removedHashes: appended.removedHashes,
			append,
			coordinateFields: append.coordinateFields,
			get context() {
				return contextAccessors.getContext();
			},
			get contextBytes() {
				return contextAccessors.getContextBytes();
			},
			get contextualEncodedValueParts() {
				return (contextualEncodedValueParts ??= {
					prefix: input.documentBytes,
					suffix: contextAccessors.getContextBytes(),
				});
			},
				nativeBackboneDocumentIndexCommitted:
					appended.appendCommit.nativeBackboneDocumentIndexCommitted,
				nativeBackboneDocumentIndexTrimmedHeadsProcessed:
					appended.appendCommit.nativeBackboneDocumentIndexTrimmedHeadsProcessed,
				get nativeBackboneDocumentIndex() {
					return getNativeBackboneDocumentIndex();
				},
				unique: input.unique,
				existing: input.existing,
			};
	}

	private async createDocumentAppendCommitFactsBatch(
		rows: Array<{
			input: NativeDocumentAppendCommitFactsInput<T, I>;
			appended: NativeDocumentAppendResult;
		}>,
	): Promise<NativeDocumentAppendTransaction<T, I>[]> {
		const contextInputs = rows.map(({ input, appended }) => {
			const append = appended.appendCommit;
			const nativePreviousContext =
				append.documentPreviousContext == null
					? undefined
					: nativeDocumentContextFactsAsContext(
							append.documentPreviousContext,
						);
			const nativePreviousIndexedContext = nativePreviousContext
				? ({
						id: input.key,
						value: {
							__context: nativePreviousContext,
						} as IndexedContextOnly<I>,
					} satisfies indexerTypes.IndexedResult<IndexedContextOnly<I>>)
				: undefined;
			const inputWithExisting =
				input.existing === undefined && nativePreviousIndexedContext
					? {
							...input,
							existing: nativePreviousIndexedContext,
						}
					: input;
			const existing =
				inputWithExisting.unique || inputWithExisting.existing === null
					? null
					: inputWithExisting.existing;
			return {
				existingCreated: existing?.value.__context.created,
				modified: append.wallTime,
				head: append.hash,
				gid: append.gid,
				size: append.payloadSize,
			};
		});
		const contextPlans =
			tryPlanDocumentContextBatch(contextInputs) ??
			(await planDocumentContextBatch(contextInputs));
		return rows.map((row, index) => {
			const append = row.appended.appendCommit;
			const nativePreviousContext =
				append.documentPreviousContext == null
					? undefined
					: nativeDocumentContextFactsAsContext(
							append.documentPreviousContext,
						);
			const nativePreviousIndexedContext = nativePreviousContext
				? ({
						id: row.input.key,
						value: {
							__context: nativePreviousContext,
						} as IndexedContextOnly<I>,
					} satisfies indexerTypes.IndexedResult<IndexedContextOnly<I>>)
				: undefined;
			const input =
				row.input.existing === undefined && nativePreviousIndexedContext
					? {
							...row.input,
							existing: nativePreviousIndexedContext,
					}
					: row.input;
			const contextPlan = contextPlans?.[index];
			if (!contextPlan) {
				return this.createDocumentAppendCommitFactsWithLazyContext(
					input,
					row.appended,
					contextInputs[index]!,
					input.nativeBackboneDocumentIndex,
				);
			}
			if (input.nativeBackboneDocumentIndex) {
				let context: Context | undefined;
				return this.createNativeDocumentAppendTransaction(
					input,
					row.appended,
					{
						getContext: () => (context ??= new Context(contextPlan)),
						getContextBytes: () => contextPlan.contextBytes,
					},
					input.nativeBackboneDocumentIndex,
				);
			}
			return this.createDocumentAppendCommitFactsWithContext(
				input,
				row.appended,
				contextPlan,
			);
		});
	}

	private createDocumentAppendCommitFactsWithContext(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		appended: NativeDocumentAppendResult,
		contextPlan: {
			created: bigint;
			modified: bigint;
			head: string;
			gid: string;
			size: number;
			contextBytes: Uint8Array;
		},
		preparedNativeBackboneDocumentIndex = input.nativeBackboneDocumentIndex,
	): NativeDocumentAppendTransaction<T, I> {
		const append = appended.appendCommit;
		const context = new Context(contextPlan);
		const nativeBackboneDocumentIndex =
			preparedNativeBackboneDocumentIndex ??
			(append.nativeBackboneDocumentIndexCommitted
				? undefined
				: this._nativeBackboneDocumentIndexEnabled
					? asTrustedDocumentIndex(this._index).prepareNativeBackboneDocumentIndexCommitWithAppendFacts(
							input.document,
							input.documentBytes,
							context,
							{ entryPublicKeys: [this.log.log.identity.publicKey] },
						)
					: undefined);
		return this.createNativeDocumentAppendTransaction(
			input,
			appended,
			{
				getContext: () => context,
				getContextBytes: () => contextPlan.contextBytes,
			},
			nativeBackboneDocumentIndex,
		);
	}

	private hasDocumentChangeConsumers(): boolean {
		if (this._valueClassIsProgram === true) {
			// Program-valued documents must always materialize removed values so
			// that open subprograms are dropped on delete, even without listeners.
			return true;
		}
		const changeListenerCount = this._documentChangeListenerCount ?? 0;
		const internalChangeListenerCount =
			this._documentInternalChangeListenerCount ?? 0;
		return (
			changeListenerCount > internalChangeListenerCount ||
			this._index.hasPending === true
		);
	}

	private dispatchDocumentChangeIfObserved(
		documentsChanged: DocumentsChange<T, I>,
	): void {
		if (!this.hasDocumentChangeConsumers()) {
			return;
		}
		this.events.dispatchEvent(
			new CustomEvent("change", { detail: documentsChanged }),
		);
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

	private handlePreparedPlainPutCommit(
		commit: DocumentAppendCommitFacts<T, I>,
	): MaybePromise<void> {
		const shouldPrepareChange = this.hasDocumentChangeConsumers();
		const removedAlreadyHandled =
			commit.nativeBackboneDocumentIndexTrimmedHeadsProcessed === true;
		const removedHashes = commit.removedHashes ?? [];
		const hasRemovedFacts =
			commit.removed.length > 0 || removedHashes.length > 0;
		const existing =
			commit.unique || commit.existing === null ? null : commit.existing;
		const persistNativeBackboneDocumentIndexCommit = (): MaybePromise<
			boolean | undefined
		> => {
			if (!commit.nativeBackboneDocumentIndexCommitted) {
				return;
			}
			if (this._mode === "native") {
				return true;
			}
			return asTrustedDocumentIndex(this._index)._persistPreparedNativeBackboneDocumentIndexStoredWithContext(
				commit.key,
				commit.context,
				commit.nativeBackboneDocumentIndex,
				commit.contextualEncodedValueParts,
				{
					replace: existing != null,
				},
			);
		};
		if (
			!shouldPrepareChange &&
			(!hasRemovedFacts || removedAlreadyHandled) &&
			commit.nativeBackboneDocumentIndexCommitted
		) {
			if (!this.strictHistory && existing) {
				const shouldIgnoreChange = this.immutable
					? existing.value.__context.modified < commit.append.wallTime
					: existing.value.__context.modified > commit.append.wallTime;
				if (shouldIgnoreChange) {
					return;
				}
			}
			const finishCommitted = () => {
				this._index._cacheResolvedIdentityValue(
					commit.key.primitive,
					commit.document,
				);
			};
			const persisted = persistNativeBackboneDocumentIndexCommit();
			return persisted === undefined || persisted === false
				? finishCommitted()
				: mapMaybePromise(persisted, finishCommitted);
		}

		const documentsChanged: DocumentsChange<T, I> | undefined =
			shouldPrepareChange
				? {
						added: [],
						removed: [],
					}
				: undefined;
		const modified: Set<string | number | bigint> = new Set();

		if (!this.strictHistory && existing) {
			const shouldIgnoreChange = this.immutable
				? existing.value.__context.modified < commit.append.wallTime
				: existing.value.__context.modified > commit.append.wallTime;
			if (shouldIgnoreChange) {
				modified.add(commit.key.primitive);
			}
		}

		const finishRemoved = (): MaybePromise<void> => {
			if (!shouldPrepareChange) {
				if (!hasRemovedFacts || removedAlreadyHandled) {
					return undefined;
				}
				if (commit.removed.length === 0 && removedHashes.length > 0) {
					const handled =
						this.tryHandlePreparedPlainPutCommitRemovedHashesFromHeads(
							removedHashes,
							modified,
						);
					if (handled !== undefined) {
						return mapMaybePromise(handled, (handledHeads) => {
							if (handledHeads.size === removedHashes.length) {
								return undefined;
							}
							const remaining = removedHashes.filter(
								(hash) => !handledHeads.has(hash),
							);
							return remaining.length === 0
								? undefined
								: this.handlePreparedPlainPutCommitRemovedHashes(
										remaining,
										modified,
									);
						});
					}
					return this.handlePreparedPlainPutCommitRemovedHashes(
						removedHashes,
						modified,
					);
				}
				const handled = this.tryHandlePreparedPlainPutCommitRemovedFromHeads(
					commit.removed,
					modified,
				);
				if (handled !== undefined) {
					return mapMaybePromise(handled, (handledHeads) => {
						if (handledHeads.size === commit.removed.length) {
							return undefined;
						}
						const remaining = commit.removed.filter(
							(entry) => !handledHeads.has(entry.hash),
						);
						return remaining.length === 0
							? undefined
							: this.handlePreparedPlainPutCommitRemoved(remaining, modified);
					});
				}
				return this.handlePreparedPlainPutCommitRemoved(
					commit.removed,
					modified,
				);
			}
			if (commit.removed.length === 0) {
				if (removedHashes.length > 0) {
					return this.handlePreparedPlainPutCommitRemovedHashes(
						removedHashes,
						modified,
						documentsChanged!,
					);
				}
				this.dispatchDocumentChangeIfObserved(documentsChanged!);
				return;
			}
			return this.handlePreparedPlainPutCommitRemoved(
				commit.removed,
				modified,
				documentsChanged!,
			);
		};

		const finishIndexed = (
			indexedDocument: WithIndexedContext<T, I> | undefined,
		): MaybePromise<void> => {
			if (indexedDocument) {
				if (documentsChanged) {
					documentsChanged.added.push(indexedDocument);
				}
				modified.add(commit.key.primitive);
				return finishRemoved();
			}
			return mapMaybePromise(
				this._index.putWithContext(
					commit.document,
					commit.key,
					commit.context,
					{
						replace: existing != null,
						encodedValueParts: commit.contextualEncodedValueParts,
						transformFacts: { entryPublicKeys: commit.entry.publicKeys },
					},
				),
				({ indexable }) => {
					if (documentsChanged) {
						documentsChanged.added.push(
							coerceWithIndexed(
								coerceWithContext(commit.document, commit.context),
								indexable,
							),
						);
					}
					modified.add(commit.key.primitive);
					return finishRemoved();
				},
			);
		};

		if (!modified.has(commit.key.primitive)) {
			if (commit.nativeBackboneDocumentIndexCommitted) {
				const finishCommitted = (): MaybePromise<void> => {
					this._index._cacheResolvedIdentityValue(
						commit.key.primitive,
						commit.document,
					);
					if (!shouldPrepareChange) {
						modified.add(commit.key.primitive);
						return finishRemoved();
					}
					const withContext = coerceWithContext(commit.document, commit.context);
					if (commit.nativeBackboneDocumentIndex?.indexable) {
						return finishIndexed(
							coerceWithIndexed(
								withContext,
								commit.nativeBackboneDocumentIndex.indexable,
							),
						);
					}
					if (commit.nativeBackboneDocumentIndex?.getIndexable) {
						return finishIndexed(
							coerceWithLazyIndexed(
								withContext,
								commit.nativeBackboneDocumentIndex.getIndexable,
							),
						);
					}
					return finishIndexed(
						coerceWithIndexed(withContext, commit.document as unknown as I),
					);
				};
				const persisted = persistNativeBackboneDocumentIndexCommit();
				return persisted === undefined || persisted === false
					? finishCommitted()
					: mapMaybePromise(persisted, finishCommitted);
			}
			if (commit.nativeBackboneDocumentIndex) {
				const nativePreparedIndexPut =
					asTrustedDocumentIndex(this._index)._putPreparedNativeBackboneDocumentIndexWithContext(
						commit.document,
						commit.key,
						commit.context,
						commit.nativeBackboneDocumentIndex,
						{
							replace: existing != null,
						},
					);
				if (nativePreparedIndexPut !== undefined) {
					return mapMaybePromise(nativePreparedIndexPut, finishIndexed);
				}
			}
			const storedIdentityPut = this._index._putStoredIdentityWithContext(
				commit.document,
				commit.key,
				commit.context,
				commit.contextualEncodedValueParts,
				{
					replace: existing != null,
				},
			);
			if (storedIdentityPut !== undefined) {
				return mapMaybePromise(storedIdentityPut, finishIndexed);
			}
			return mapMaybePromise(
				this._index._putIdentityWithContext(
					commit.document,
					commit.key,
					commit.context,
					{
						replace: existing != null,
						encodedValueParts: commit.contextualEncodedValueParts,
						transformFacts: { entryPublicKeys: commit.entry.publicKeys },
					},
				),
				finishIndexed,
			);
		}

		return finishRemoved();
	}

	private tryHandlePreparedPlainPutCommitRemovedFromHeads(
		removedEntries: ShallowOrFullEntry<Operation>[],
		modified: Set<string | number | bigint>,
	): MaybePromise<Set<string>> | undefined {
		const handled = new Set<string>();
		const deleteKeys: indexerTypes.IdKey[] = [];
		for (const removed of removedEntries) {
			if (removed instanceof Entry) {
				continue;
			}
			const resolved = this._index.tryGetIdentityIndexedKeyByHead(removed.hash);
			if (!resolved.supported) {
				return;
			}
			if (!resolved.key) {
				continue;
			}
			handled.add(removed.hash);
			if (modified.has(resolved.key.primitive)) {
				continue;
			}
			deleteKeys.push(resolved.key);
			modified.add(resolved.key.primitive);
		}
		if (deleteKeys.length === 0) {
			return handled;
		}
		return mapMaybePromise(this._index.delManyMaybe(deleteKeys), () => handled);
	}

	private tryHandlePreparedPlainPutCommitRemovedHashesFromHeads(
		removedHashes: string[],
		modified: Set<string | number | bigint>,
	): MaybePromise<Set<string>> | undefined {
		const handled = new Set<string>();
		const deleteKeys: indexerTypes.IdKey[] = [];
		for (const hash of removedHashes) {
			const resolved = this._index.tryGetIdentityIndexedKeyByHead(hash);
			if (!resolved.supported) {
				return;
			}
			if (!resolved.key) {
				continue;
			}
			handled.add(hash);
			if (modified.has(resolved.key.primitive)) {
				continue;
			}
			deleteKeys.push(resolved.key);
			modified.add(resolved.key.primitive);
		}
		if (deleteKeys.length === 0) {
			return handled;
		}
		return mapMaybePromise(this._index.delManyMaybe(deleteKeys), () => handled);
	}

	private async handlePreparedPlainPutCommitRemoved(
		removedEntries: ShallowOrFullEntry<Operation>[],
		modified: Set<string | number | bigint>,
		documentsChanged?: DocumentsChange<T, I>,
	): Promise<void> {
		const handledRemovedHeads =
			await this.collectRemovedDocumentChangesFromIndexedHeads(
				removedEntries,
				modified,
				documentsChanged,
			);
		for (const removed of removedEntries) {
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
					: await this.log.log.entryIndex.get(removed.hash, {
							type: "full",
							ignoreMissing: true,
						});
			if (!entry) {
				continue;
			}
			try {
				const payload = await this.getAppendOperation(entry);
				if (!payload) {
					continue;
				}
				await this.collectRemovedDocumentChange(
					payload,
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

		if (documentsChanged) {
			this.dispatchDocumentChangeIfObserved(documentsChanged);
		}
	}

	private async handlePreparedPlainPutCommitRemovedHashes(
		removedHashes: string[],
		modified: Set<string | number | bigint>,
		documentsChanged?: DocumentsChange<T, I>,
	): Promise<void> {
		const handledRemovedHeads =
			await this.collectRemovedDocumentChangesFromIndexedHeadHashes(
				removedHashes,
				modified,
				documentsChanged,
			);
		for (const hash of removedHashes) {
			if (handledRemovedHeads.has(hash)) {
				continue;
			}
			const entry = await this.log.log.entryIndex.get(hash, {
				type: "full",
				ignoreMissing: true,
			});
			if (!entry) {
				continue;
			}
			try {
				const payload = await this.getAppendOperation(entry);
				if (!payload) {
					continue;
				}
				await this.collectRemovedDocumentChange(
					payload,
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

		if (documentsChanged) {
			this.dispatchDocumentChangeIfObserved(documentsChanged);
		}
	}

	private async handlePreparedPlainPutManyCommit(
		commit: DocumentAppendManyCommitFacts<T, I>,
	): Promise<void> {
		if (
			!this.hasDocumentChangeConsumers() &&
			commit.removed.length === 0
		) {
			const stored =
				await asTrustedDocumentIndex(this._index)._putManyPreparedNativeBackboneDocumentIndexStored(
					commit.commits.map((put) => {
						const existing =
							put.unique || put.existing === null ? null : put.existing;
						return {
							value: put.document,
							id: put.key,
							context: put.context,
							encodedValueParts: put.contextualEncodedValueParts,
							nativeDocumentIndex: put.nativeBackboneDocumentIndex,
							options: {
								replace: existing != null,
							},
						};
					}),
				);
			if (stored === true) {
				return;
			}
		}

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
			nativeBackboneDocumentIndex?: NativeDocumentAppendTransaction<
				T,
				I
			>["nativeBackboneDocumentIndex"];
			replace: boolean;
		}> = [];
		for (const put of commit.commits) {
			if (modified.has(put.key.primitive)) {
				continue;
			}
			const existing =
				put.unique || put.existing === null ? null : put.existing;
			putsToIndex.push({
				document: put.document,
				encodedDocument: put.encodedDocument,
				key: put.key,
				context: put.context,
				contextualEncodedValueParts: put.contextualEncodedValueParts,
				nativeBackboneDocumentIndex: put.nativeBackboneDocumentIndex,
				replace: existing != null,
			});
			modified.add(put.key.primitive);
		}
		let indexedDocuments = await this._index._putManyIdentityWithContext(
			putsToIndex.map((put) => ({
				value: put.document,
				id: put.key,
				context: put.context,
				options: {
					replace: put.replace,
					encodedValueParts: put.contextualEncodedValueParts,
				},
			})),
		);
		indexedDocuments ??=
			await asTrustedDocumentIndex(this._index)._putManyPreparedNativeBackboneDocumentIndexWithContext(
				putsToIndex.map((put) => ({
					value: put.document,
					id: put.key,
					context: put.context,
					nativeDocumentIndex: put.nativeBackboneDocumentIndex,
					options: {
						replace: put.replace,
					},
				})),
			);
		if (indexedDocuments) {
			documentsChanged.added.push(...indexedDocuments);
		} else {
			if (this.isNativeMode()) {
				throw this.nativeModeError(
					"requires native batch document-index commit",
				);
			}
			const indexed = await this._index.putManyWithContext(
				putsToIndex.map((put) => ({
					value: put.document,
					id: put.key,
					context: put.context,
					options: {
						replace: put.replace,
						encodedValueParts: put.contextualEncodedValueParts,
					},
				})),
			);
			for (let i = 0; i < putsToIndex.length; i++) {
				const put = putsToIndex[i]!;
				const { indexable } = indexed[i]!;
				documentsChanged.added.push(
					coerceWithIndexed(
						coerceWithContext(put.document, put.context),
						indexable,
					),
				);
			}
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
					: await this.log.log.entryIndex.get(removed.hash, {
							type: "full",
							ignoreMissing: true,
						});
			if (!entry) {
				continue;
			}
			try {
				const payload = await this.getAppendOperation(entry);
				if (!payload) {
					continue;
				}
				await this.collectRemovedDocumentChange(
					payload,
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
		documentsChanged?: DocumentsChange<T, I>,
	): Promise<boolean> {
		const key = await this._index.getIdentityIndexedKeyByHead(head);
		if (key) {
			if (await this.collectRemovedDocumentChangeFromIndexedKey(
				key,
				modified,
				documentsChanged,
			)) {
				return true;
			}
		}
		if (!documentsChanged) {
			return false;
		}
		const indexed = await this._index.getIdentityIndexedByHead(head);
		if (!indexed) {
			return false;
		}

		return this.collectRemovedDocumentChangeFromIndexedKey(
			indexed.id,
			modified,
			documentsChanged,
			() =>
				coerceWithIndexed(
					indexed.value as unknown as WithIndexedContext<T, I>,
					indexed.value as unknown as I,
				),
		);
	}

	private async collectRemovedDocumentChangeFromIndexedKey(
		key: indexerTypes.IdKey,
		modified: Set<string | number | bigint>,
		documentsChanged?: DocumentsChange<T, I>,
		valueProvider?: () => WithIndexedContext<T, I>,
	): Promise<boolean> {
		if (modified.has(key.primitive)) {
			return true;
		}

		let value: WithIndexedContext<T, I> | undefined;

		if (documentsChanged) {
			value =
				valueProvider?.() ??
				(await this._index.get(key, {
					local: true,
					remote: false,
				}));
			if (!value) {
				return false;
			}
			documentsChanged.removed.push(value);
		}

		if (
			value instanceof Program &&
			value.closed !== true &&
			value.parents.includes(this)
		) {
			await value.drop(this);
		}

		await this._index.delMany([key]);
		modified.add(key.primitive);
		return true;
	}

	private async collectRemovedDocumentChangesFromIndexedHeads(
		removed: ShallowOrFullEntry<Operation>[],
		modified: Set<string | number | bigint>,
		documentsChanged?: DocumentsChange<T, I>,
	): Promise<Set<string>> {
		return this.collectRemovedDocumentChangesFromIndexedHeadHashes(
			removed.map((entry) => entry.hash),
			modified,
			documentsChanged,
		);
	}

	private async collectRemovedDocumentChangesFromIndexedHeadHashes(
		removedHashes: string[],
		modified: Set<string | number | bigint>,
		documentsChanged?: DocumentsChange<T, I>,
	): Promise<Set<string>> {
		if (removedHashes.length === 0) {
			return new Set();
		}
		if (!documentsChanged) {
			const handled = new Set<string>();
			const deleteKeys: indexerTypes.IdKey[] = [];
			for (const hash of removedHashes) {
				const key = await this._index.getIdentityIndexedKeyByHead(hash);
				if (!key) {
					continue;
				}
				handled.add(hash);
				if (modified.has(key.primitive)) {
					continue;
				}
				deleteKeys.push(key);
				modified.add(key.primitive);
			}
			await this._index.delMany(deleteKeys);
			return handled;
		}
		const keyByHead = this._index.getIndexedKeysByHeads(removedHashes);
		if (keyByHead) {
			const handled = new Set<string>();
			const deleteKeys: indexerTypes.IdKey[] = [];
			for (let i = 0; i < removedHashes.length; i++) {
				const key = keyByHead[i];
				if (!key) {
					continue;
				}
				handled.add(removedHashes[i]!);
				if (modified.has(key.primitive)) {
					continue;
				}
				const value = await this._index.get(key, {
					local: true,
					remote: false,
				});
				if (!value) {
					handled.delete(removedHashes[i]!);
					continue;
				}
				documentsChanged.removed.push(value);
				deleteKeys.push(key);
				modified.add(key.primitive);
			}
			await this._index.delMany(deleteKeys);
			return handled;
		}
		const indexedByHead =
			await this._index.getIdentityIndexedByHeads(removedHashes);
		if (!indexedByHead) {
			return new Set();
		}

		const handled = new Set<string>();
		const deleteKeys: indexerTypes.IdKey[] = [];
		for (let i = 0; i < removedHashes.length; i++) {
			const indexed = indexedByHead[i];
			if (!indexed) {
				continue;
			}
			const key = indexed.id;
			handled.add(removedHashes[i]!);
			if (modified.has(key.primitive)) {
				continue;
			}
			if (documentsChanged) {
				const value = coerceWithIndexed(
					indexed.value as unknown as WithIndexedContext<T, I>,
					indexed.value as unknown as I,
				);
				documentsChanged.removed.push(value);
			}
			deleteKeys.push(key);
			modified.add(key.primitive);
		}
		await this._index.delMany(deleteKeys);
		return handled;
	}

	private async collectRemovedDocumentChange(
		payload: Operation,
		modified: Set<string | number | bigint>,
		documentsChanged?: DocumentsChange<T, I>,
	) {
		let value: WithIndexedContext<T, I> | undefined;
		let key: indexerTypes.IdKey;

		if (isPutOperation(payload)) {
			const keyValue = this.isNativeMode()
				? await this.getNativeDocumentIdFromPutOperation(payload)
				: undefined;
			if (this.isNativeMode() && keyValue == null) {
				throw this.nativeModeError(
					"requires native document id extraction for removed put",
				);
			}
			key = indexerTypes.toId(
				keyValue ??
					this.idResolver(this.index.valueEncoding.decoder(payload.data)),
			);
			if (modified.has(key.primitive)) {
				return;
			}

			if (documentsChanged) {
				const document = await this._index.get(key, {
					local: true,
					remote: false,
				});
				if (!document) {
					return;
				}
				value = document;
			}
		} else if (isDeleteOperation(payload)) {
			key = coerceDeleteOperation(payload).key;
			if (modified.has(key.primitive)) {
				return;
			}
			if (documentsChanged) {
				const document = await this._index.get(key, {
					local: true,
					remote: false,
				});
				if (!document) {
					return;
				}
				value = document;
			}
		} else {
			throw new Error("Unexpected");
		}

		if (documentsChanged && value) {
			documentsChanged.removed.push(value);
		}

		if (
			value instanceof Program &&
			value.closed !== true &&
			value.parents.includes(this)
		) {
			await value.drop(this);
		}

		await this._index.delMany([key]);
		modified.add(key.primitive);
	}

	private async collectRemovedPutChangeFromNativeId(
		payload: Operation,
		modified: Set<string | number | bigint>,
	): Promise<boolean> {
		if (!this.isNativeMode() || !isPutOperation(payload)) {
			return false;
		}
		const keyValue = await this.getNativeDocumentIdFromPutOperation(payload);
		if (keyValue == null) {
			throw this.nativeModeError(
				"requires native document id extraction for removed put",
			);
		}
		const key = indexerTypes.toId(keyValue);
		if (modified.has(key.primitive)) {
			return true;
		}
		await this._index.delMany([key]);
		modified.add(key.primitive);
		return true;
	}

	private putStrictNativeReceivedDocumentIndexWithContext(
		value: T,
		key: indexerTypes.IdKey,
		entry: Entry<Operation>,
		payload: PutOperation,
		existing: indexerTypes.IndexedResult<IndexedContextOnly<I>> | null,
	): MaybePromise<WithIndexedContext<T, I> | undefined> {
		if (!this.isNativeMode() || !this._nativeBackboneDocumentIndexEnabled) {
			return;
		}
		if (value instanceof Program) {
			return;
		}
		const existingContext = this.getExistingContext(existing);
		const modified = entry.meta.clock.timestamp.wallTime;
		const context = new Context({
			created: existingContext?.created || modified,
			modified,
			head: entry.hash,
			gid: entry.meta.gid,
			size: encodePutOperationPayload(payload.data).byteLength,
		});
		const nativeDocumentIndex =
			asTrustedDocumentIndex(this._index).prepareNativeBackboneDocumentIndexCommitWithAppendFacts(
				value,
				payload.data,
				context,
				{ entryPublicKeys: entry.publicKeys },
			);
		if (!nativeDocumentIndex) {
			return;
		}
		return asTrustedDocumentIndex(this._index)._putPreparedNativeBackboneDocumentIndexWithContext(
			value,
			key,
			context,
			nativeDocumentIndex,
			{
				replace: existing != null,
			},
		);
	}

	private putStrictNativeReceivedDocumentIndexStoredWithContext(
		key: indexerTypes.IdKey,
		entry: Entry<Operation>,
		payload: PutOperation,
		existing: indexerTypes.IndexedResult<IndexedContextOnly<I>> | null,
	): MaybePromise<boolean | undefined> {
		if (!this.isNativeMode() || !this._nativeBackboneDocumentIndexEnabled) {
			return;
		}
		const existingContext = this.getExistingContext(existing);
		const modified = entry.meta.clock.timestamp.wallTime;
		const context = new Context({
			created: existingContext?.created || modified,
			modified,
			head: entry.hash,
			gid: entry.meta.gid,
			size: encodePutOperationPayload(payload.data).byteLength,
		});
		const nativeDocumentIndex =
			asTrustedDocumentIndex(this._index).prepareNativeBackboneDocumentIndexStoredCommitWithAppendFacts(
				payload.data,
				context,
				{ entryPublicKeys: entry.publicKeys },
			);
		if (!nativeDocumentIndex) {
			return;
		}
		return asTrustedDocumentIndex(this._index)._putPreparedNativeBackboneDocumentIndexStoredWithContext(
			key,
			context,
			nativeDocumentIndex,
			{
				replace: existing != null,
			},
		);
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
		return this._documentBackend.del(id, options);
	}

	private async delCompatDocumentBackend(
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

	private async delNativeDocumentBackend(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: SharedAppendOptions<Operation>,
	): Promise<DocumentDeleteResult> {
		const deleteOptions = this.normalizeNativeModePutOptions(options);
		this.assertNativeModeDeleteSupported(deleteOptions);
		const key = id instanceof indexerTypes.IdKey ? id : indexerTypes.toId(id);
		if (!this.hasNativeDocumentContextLookup()) {
			throw this.nativeModeError("requires native document context lookup");
		}
		const existing = this.getNativeIndexedContext(key);
		const existingContext = this.getExistingContext(existing);
		if (!existingContext?.head) {
			throw new NotFoundError(
				`No entry with key '${key.primitive}' in the database`,
			);
		}
		let previousEntry: Entry<Operation> | undefined;
		let existingDocumentChecked = false;
		let existingDocument: T | undefined;
		const getPreviousEntry = async () => {
			if (previousEntry) {
				return previousEntry;
			}
			previousEntry = await this._resolveEntry(existingContext.head, {
				remote: true,
			});
			if (!previousEntry) {
				throw new NotFoundError(
					`No entry with key '${key.primitive}' in the database`,
				);
			}
			return previousEntry;
		};
		const getExistingDocument = async () => {
			if (!existingDocumentChecked) {
				existingDocumentChecked = true;
				existingDocument = await this.getLocalIdentityDocumentByHead(
					existingContext.head,
				);
				existingDocument ??=
					await this.getLocalIndexedDocumentForNativeDeletePolicy(key);
			}
			return existingDocument;
		};
		const operation = new DeleteOperation({ key });
		if (
			!(await this.canPerformAllowsNativeDelete({
				operation,
				getExistingEntry: getPreviousEntry,
				getExistingDocument,
			}))
		) {
			throw this.nativeModeError("canPerform policy rejected this delete");
		}

		const operationPayloadBytes = BORSH_ENCODING_OPERATION.encoder(operation);
		const documentsChanged: DocumentsChange<T, I> | undefined =
			this.hasDocumentChangeConsumers()
				? {
						added: [],
						removed: [],
					}
				: undefined;
		const removedDocument = documentsChanged
			? await this._index.get(key, {
					local: true,
					remote: false,
				})
			: undefined;
		this.keepCache?.delete(existingContext.head);
		const previousForAppend =
			this.nextFromIndexedContext(existingContext.head, existing) ??
			(await getPreviousEntry());
		const trustedLog = asTrustedDocumentSharedLog(this.log);
		const appended =
			await trustedLog.appendStrictNativeDocumentPayloadCommitOnly(
				operationPayloadBytes,
				{
					...deleteOptions,
					meta: {
						next: [previousForAppend as Entry<Operation>],
						type: EntryType.CUT,
						...deleteOptions?.meta,
					},
				},
				{
					skipMissingNextJoin: true,
					resolveTrimmedEntries: false,
					nativeBackboneDocumentDeleteKey: documentIndexStoreKey(key),
				},
			);
		if (!appended) {
			throw this.nativeModeError("requires native delete append support");
		}
		const result: DocumentDeleteResult = {
			get entry() {
				return appended.entry;
			},
			removed: appended.removed,
		};
			if (appended.appendCommit.nativeBackboneDocumentDeleteCommitted) {
				this._index.clearResolvedCacheForKeys([key]);
			} else {
				await this._index.delManyMaybe([key]);
			}
		if (documentsChanged && removedDocument) {
			documentsChanged.removed.push(removedDocument);
			this.dispatchDocumentChangeIfObserved(documentsChanged);
		}
		return result;
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

		const shouldPrepareDocumentChanges = this.hasDocumentChangeConsumers();
		const canRemoveByIndexedHead =
			!shouldPrepareDocumentChanges && this._index.canGetIndexedKeyByHead();
		const removedEntries = canRemoveByIndexedHead
			? []
			: ((await Promise.all(
					change.removed.map((x) =>
						x instanceof Entry ? x : this.log.log.entryIndex.get(x.hash),
					),
				)) ?? []);
		const sortedEntries = [
			...change.added.map((x) => x.entry),
			...removedEntries,
		]; // TODO assert sorting
		/*  const sortedEntries = [...change.added, ...(removed || [])]
					.sort(this.log.log.sortFn)
					.reverse(); // sort so we get newest to oldest */

		// There might be a case where change.added and change.removed contains the same document id. Usaully because you use the "trim" option
		// in combinatpion with inserting the same document. To mitigate this, we loop through the changes and modify the behaviour for this

		let documentsChanged: DocumentsChange<T, I> | undefined =
			shouldPrepareDocumentChanges
				? {
						added: [],
						removed: [],
					}
				: undefined;

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
					: await this.getAppendOperation(item);
				if (!payload) {
					continue;
				}

				if (isPutOperation(payload) && !removedSet.has(item.hash)) {
					if (!documentsChanged && this.isNativeMode()) {
						const keyValue =
							await this.getNativeDocumentIdFromPutOperation(payload);
						if (keyValue != null) {
							const key = indexerTypes.toId(keyValue);
							if (modified.has(key.primitive)) {
								continue;
							}
							const existing =
								reference?.unique || reference?.existing === null
									? null
									: isReferencedAppendEntry &&
										  reference?.existing !== undefined
										? reference.existing
										: this.getNativeModeIndexedContext(key) || null;
							if (!this.strictHistory && existing) {
								const shouldIgnoreChange = this.immutable
									? existing.value.__context.modified <
										item.meta.clock.timestamp.wallTime
									: existing.value.__context.modified >
										item.meta.clock.timestamp.wallTime;
								if (shouldIgnoreChange) {
									continue;
								}
							}
							const stored =
								await this.putStrictNativeReceivedDocumentIndexStoredWithContext(
									key,
									item,
									payload,
									existing,
								);
							if (stored) {
								modified.add(key.primitive);
								continue;
							}
						}
					}
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
								: this.isNativeMode()
									? this.getNativeModeIndexedContext(key) || null
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
					const nativeStoredIndexed =
						payload instanceof PutOperation
							? await this.putStrictNativeReceivedDocumentIndexWithContext(
									value,
									key,
									item,
									payload,
									existing,
								)
							: undefined;
					if (nativeStoredIndexed) {
						documentsChanged?.added.push(nativeStoredIndexed);
						modified.add(key.primitive);
						continue;
					}
					const { context, indexable } = await this._index.put(
						value,
						key,
						item,
						existing,
					);
					documentsChanged?.added.push(
						coerceWithIndexed(coerceWithContext(value, context), indexable),
					);

					modified.add(key.primitive);
				} else if (
					(isDeleteOperation(payload) && !removedSet.has(item.hash)) ||
					isPutOperation(payload) ||
					removedSet.has(item.hash)
				) {
					if (
						!documentsChanged &&
						(await this.collectRemovedPutChangeFromNativeId(payload, modified))
					) {
						continue;
					}
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

		if (canRemoveByIndexedHead && change.removed.length > 0) {
			const handled = await this.collectRemovedDocumentChangesFromIndexedHeads(
				change.removed,
				modified,
			);
			const remainingRemoved = change.removed.filter(
				(entry) => !handled.has(entry.hash),
			);
			for (const removed of remainingRemoved) {
				const entry =
					removed instanceof Entry
						? removed
						: await this.log.log.entryIndex.get(removed.hash, {
								type: "full",
								ignoreMissing: true,
							});
				if (!entry) {
					continue;
				}
				try {
					const payload = await this.getAppendOperation(entry);
					if (!payload) {
						continue;
					}
					if (
						!(await this.collectRemovedPutChangeFromNativeId(payload, modified))
					) {
						await this.collectRemovedDocumentChange(payload, modified);
					}
				} catch (error) {
					if (error instanceof AccessError) {
						continue;
					}
					throw error;
				}
			}
		}

		if (documentsChanged) {
			this.dispatchDocumentChangeIfObserved(documentsChanged);
		}
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
