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
	type SimpleDocumentProjectionPlan,
	initializeDocumentRust,
	planDocumentContext,
	planDocumentContextBatch,
	tryPlanDocumentContext,
	tryPlanDocumentContextBatch,
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
import {
	type NativeCanPerformPolicyDescriptor,
	type NativeFastPathCanPerformPolicyEvaluator,
	createNativeFastPathCanPerformPolicyEvaluator,
	getNativeCanPerformPolicyDescriptor,
	nativeCanPerformPolicyNeedsDeleteValue,
	nativeCanPerformPolicyNeedsPreviousEntries,
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
import { getNativeDocumentTransformDescriptor } from "./transform.js";

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

const isPromiseLike = <T>(value: MaybePromise<T>): value is Promise<T> =>
	!!value && typeof (value as Promise<T>).then === "function";

const mapMaybePromise = <T, R>(
	value: MaybePromise<T>,
	fn: (value: T) => MaybePromise<R>,
): MaybePromise<R> => (isPromiseLike(value) ? value.then(fn) : fn(value));

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
	nativeBackboneDocumentIndexCommitted?: boolean;
	nativeBackboneDocumentIndexTrimmedHeadsProcessed?: boolean;
};

type NativeDocumentAppendResult = {
	entry: Entry<Operation>;
	removed: ShallowOrFullEntry<Operation>[];
	removedHashes?: string[];
	appendCommit: LocalAppendCommitFacts;
};

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
	};
};

type NativeBackboneDocumentIndexCommitInput = {
	key: string;
	valuePrefixBytes?: Uint8Array;
	projection?: {
		encodedDocument: Uint8Array;
		plan: SimpleDocumentProjectionPlan;
		signer?: Uint8Array;
	};
	existingCreated?: bigint;
	deleteTrimmedHeads?: boolean;
};

type PreparedNativeBackboneDocumentIndexCommit<I> = {
	valuePrefixBytes?: Uint8Array;
	projection?: {
		encodedDocument: Uint8Array;
		plan: SimpleDocumentProjectionPlan;
		signer?: Uint8Array;
	};
	indexable?: I;
	getIndexable?: () => I;
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

type NativeDocumentAppendManyCommitInput<T, I extends Record<string, any>> = {
	puts: NativeDocumentAppendCommitFactsInput<T, I>[];
	resolveTrimmedEntries: boolean;
	options?: DocumentPutOptions;
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
	private _optionCanPerformNativePolicy?: NativeCanPerformPolicyDescriptor;
	private _optionCanPerformNativeFastPath?: NativeFastPathCanPerformPolicyEvaluator;
	private _nativeBackboneDocumentIndexEnabled = false;
	private _mode: DocumentMode = "auto";
	private _documentChangeListeners: Array<{
		listener: unknown;
		capture: boolean;
	}> = [];
	private _documentChangeListenerCount = 0;
	private _documentInternalChangeListenerCount = 0;
	private _documentChangeListenerTrackingInitialized = false;
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
		this.trackDocumentChangeListeners();
	}

	get index(): DocumentIndex<T, I, D> {
		return this._index;
	}

	private isNativeMode(): boolean {
		return this._mode === "native";
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
			| { documentIndex?: boolean }
			| boolean
			| undefined;
		const indexTransform = options.index as
			| {
					type?: unknown;
					transform?: unknown;
			  }
			| undefined;

		if (
			!nativeBackbone ||
			typeof nativeBackbone !== "object" ||
			nativeBackbone.documentIndex !== true
		) {
			unsupported.push("missing nativeBackbone.documentIndex");
		}
		if (options.domain) {
			unsupported.push("custom domain");
		}
		if (options.compatibility != null) {
			unsupported.push("legacy compatibility");
		}
		if (options.replicate !== undefined && options.replicate !== false) {
			unsupported.push("replication");
		}
		if (Program.isPrototypeOf(options.type)) {
			unsupported.push("program-valued document type");
		}
		if (
			options.canPerform &&
			!getNativeCanPerformPolicyDescriptor(options.canPerform)
		) {
			unsupported.push("arbitrary canPerform");
		}
		if (options.index?.canRead) {
			unsupported.push("custom canRead");
		}
		if (options.index?.canSearch) {
			unsupported.push("custom canSearch");
		}
		if (
			indexTransform?.transform &&
			!getNativeDocumentTransformDescriptor(indexTransform.transform as any)
		) {
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
			throw this.nativeModeError(
				`does not support ${unsupported.join(", ")}`,
			);
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
		if (!this._index.canPrepareNativeBackboneDocumentIndexCommitWithAppendFacts()) {
			throw this.nativeModeError(
				"requires a native-compatible document index transform",
			);
		}
	}

	private canPerformAllowsPlainPutFastPath(doc: T): boolean {
		return (
			!this._optionCanPerform || !!this._optionCanPerformNativeFastPath?.(doc)
		);
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
		if (options?.meta?.type) {
			unsupported.push("custom entry type");
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
		if (options?.replicas) {
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
			throw this.nativeModeError(
				`does not support ${unsupported.join(", ")}`,
			);
		}
		if (!this.canPerformAllowsPlainPutFastPath(doc)) {
			throw this.nativeModeError("canPerform policy rejected this document");
		}
		return true;
	}

	private assertNativeModePutManySupported(): void {
		if (!this.isNativeMode()) {
			return;
		}
		throw this.nativeModeError(
			"does not support putMany until native batch document-index commit is available",
		);
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
		this.trackDocumentChangeListeners();
		this._clazz = options.type;
		this.canOpen = options.canOpen;
		this._mode = options.mode ?? "auto";
		this.assertNativeModeOpenOptions(options);

		/* eslint-disable */
		if (Program.isPrototypeOf(this._clazz)) {
			if (!this.canOpen) {
				throw new Error(
					"Document store needs to be opened with canOpen option when the document type is a Program",
				);
			}
		}

		this._optionCanPerform = options.canPerform;
		this._optionCanPerformNativePolicy = getNativeCanPerformPolicyDescriptor(
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
		});
		this._documentInternalChangeListenerCount = Math.max(
			0,
			this._documentChangeListenerCount - changeListenersBeforeIndexOpen,
		);

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
			domain: (options?.domain
				? (log: any) => options.domain!(this)
				: undefined) as any, /// TODO types,
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
				this._index.attachNativeBackboneDocumentIndex(
					(this.log as { nativeBackbone?: unknown }).nativeBackbone,
				) === true;
			if (this._nativeBackboneDocumentIndexEnabled) {
				await initializeDocumentRust();
			}
		}

		this._optionCanPerformNativeFastPath = this._optionCanPerformNativePolicy
			? createNativeFastPathCanPerformPolicyEvaluator(
					this._optionCanPerformNativePolicy,
					this.log.log.identity.publicKey,
				)
			: undefined;
		this.assertNativeModeReady();
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
				const previousEntries =
					this._optionCanPerformNativePolicy &&
					isPutOperation(operation) &&
					nativeCanPerformPolicyNeedsPreviousEntries(
						this._optionCanPerformNativePolicy,
					)
						? await this.resolveCanPerformPreviousEntries(entry)
						: undefined;
				const deleteValue =
					this._optionCanPerformNativePolicy &&
					isDeleteOperation(operation) &&
					nativeCanPerformPolicyNeedsDeleteValue(
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
									entry: entry as any as Entry<PutOperation>,
									previousEntries,
								}
							: {
									type: "delete",
									value: deleteValue,
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
		const nativePlainPut = this.assertNativeModePlainPutSupported(doc, options);
		const putOptions = this.normalizeNativeModePutOptions(options);
		const prepared = (
			nativePlainPut || this.canUsePlainPutFastPath(doc, putOptions)
		)
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
					await this._index.getDetailed(prepared.keyValue, {
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
			nativePlainPut,
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
	): Promise<{
		entries: Entry<Operation>[];
		removed: ShallowOrFullEntry<Operation>[];
	}> {
		if (docs.length === 0) {
			return { entries: [], removed: [] };
		}
		this.assertNativeModePutManySupported();
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
			!options?.meta?.type &&
			!options?.meta?.next &&
			!options?.meta?.timestamp
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
		const canCleanupTrimmedHeads = this.hasDocumentChangeConsumers()
			? this._index.canGetIdentityIndexedByHead()
			: this._index.canGetIndexedKeyByHead();
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
		const appendOptions = {
			...input.options,
			meta: {
				next: input.next,
				...input.options?.meta,
			},
			replicate: input.options?.replicate,
		};
		return mapMaybePromise(
			this.prepareNativeBackboneDocumentIndexCommit(input),
			(nativeDocumentIndexCommit) => {
				let committedNativeDocumentIndex = nativeDocumentIndexCommit;
				const prepareNativeDocumentIndexWithAppendFacts =
					nativeDocumentIndexCommit
						? undefined
						: this.createNativeBackboneDocumentIndexAppendFactsPreparer(input);
				if (
					this.isNativeMode() &&
					!nativeDocumentIndexCommit &&
					!prepareNativeDocumentIndexWithAppendFacts
				) {
					throw this.nativeModeError("requires native document-index commit");
				}
				const appendProperties = {
					skipMissingNextJoin: input.skipMissingNextJoin,
					resolveTrimmedEntries: input.resolveTrimmedEntries,
					payloadData: input.operationPayloadBytes,
					...(nativeDocumentIndexCommit
						? {
								nativeBackboneDocumentIndex:
									this.toNativeBackboneDocumentIndexCommitInput(
										input,
										nativeDocumentIndexCommit,
									),
							}
						: {}),
					...(prepareNativeDocumentIndexWithAppendFacts
						? {
								prepareNativeBackboneDocumentIndex: (
									facts: NativeBackboneDocumentIndexAppendFactsInput,
								) => {
									committedNativeDocumentIndex =
										prepareNativeDocumentIndexWithAppendFacts(facts);
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
						throw this.nativeModeError("requires payload-backed put operations");
					}
					return mapMaybePromise(
						this.log.appendLocallyPrepared(
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
				return mapMaybePromise(
					this.log.appendLocallyPreparedPayloadCommitOnly(
						input.operationPayloadBytes,
						appendOptions,
						appendProperties,
					),
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
	): NativeBackboneDocumentIndexCommitInput {
		return {
			key: documentIndexStoreKey(input.key),
			valuePrefixBytes: commit.valuePrefixBytes,
			projection: commit.projection,
			existingCreated:
				input.unique || input.existing === null
					? undefined
					: input.existing?.value.__context.created,
			deleteTrimmedHeads:
				!this.hasDocumentChangeConsumers() &&
				this._index.canGetIndexedKeyByHead(),
		};
	}

	private prepareNativeBackboneDocumentIndexCommit(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
	): MaybePromise<PreparedNativeBackboneDocumentIndexCommit<I> | undefined> {
		if (!this._nativeBackboneDocumentIndexEnabled) {
			return;
		}
		return this._index.prepareNativeBackboneDocumentIndexCommit(
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
			!this._index.canPrepareNativeBackboneDocumentIndexCommitWithAppendFacts()
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
			const context = new Context({
				created: existing?.value.__context.created || appendFacts.wallTime,
				modified: appendFacts.wallTime,
				head: "",
				gid: appendFacts.gid,
				size: appendFacts.payloadSize,
			});
			return this._index.prepareNativeBackboneDocumentIndexCommitWithAppendFacts(
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
		let appended: Awaited<ReturnType<typeof this.log.appendLocallyPrepared>>;
		try {
			appended = await this.log.appendLocallyPreparedPayload(
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
			appended = await this.log.appendLocallyPrepared(
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
		const appended =
			await this.log.appendLocallyPreparedPayloadsManyIndependent(
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
			if (this.isNativeMode()) {
				throw this.nativeModeError(
					"requires native batched payload append support",
				);
			}
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
		const commits = await this.createDocumentAppendCommitFactsBatch(appendInputs);
		if (this.isNativeMode()) {
			for (const commit of commits) {
				this.assertNativeModeDocumentAppendCommit(commit);
			}
		}
		return {
			entries: appended.entries,
			removed: appended.removed,
			commits,
		};
	}

	private createDocumentAppendCommitFacts(
		input: NativeDocumentAppendCommitFactsInput<T, I>,
		appended: NativeDocumentAppendResult,
		nativeBackboneDocumentIndex?: PreparedNativeBackboneDocumentIndexCommit<I>,
	): MaybePromise<NativeDocumentAppendTransaction<T, I>> {
		const append = appended.appendCommit;
		const existing =
			input.unique || input.existing === null ? null : input.existing;
		const contextInput = {
			existingCreated: existing?.value.__context.created,
			modified: append.wallTime,
			head: append.hash,
			gid: append.gid,
			size: append.payloadSize,
		};
		if (append.nativeBackboneDocumentIndexCommitted) {
			return this.createDocumentAppendCommitFactsWithLazyContext(
				input,
				appended,
				contextInput,
				nativeBackboneDocumentIndex,
			);
		}
		const contextPlan = tryPlanDocumentContext(contextInput);
		if (contextPlan) {
			return this.createDocumentAppendCommitFactsWithContext(
				input,
				appended,
				contextPlan,
				nativeBackboneDocumentIndex,
			);
		}
		return planDocumentContext(contextInput).then((plannedContext) =>
			this.createDocumentAppendCommitFactsWithContext(
				input,
				appended,
				plannedContext,
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
			nativeBackboneDocumentIndex: nativeBackboneDocumentIndex
				? {
						valuePrefixBytes:
							nativeBackboneDocumentIndex.valuePrefixBytes,
						projection: nativeBackboneDocumentIndex.projection,
						indexable: nativeBackboneDocumentIndex.indexable,
						getIndexable: nativeBackboneDocumentIndex.getIndexable,
					}
				: undefined,
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
			const existing =
				input.unique || input.existing === null ? null : input.existing;
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
		return rows.map((row, index) =>
			this.createDocumentAppendCommitFactsWithContext(
				row.input,
				row.appended,
				contextPlans[index]!,
			),
		);
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
					? this._index.prepareNativeBackboneDocumentIndexCommitWithAppendFacts(
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
			this._index._cacheResolvedIdentityValue(
				commit.key.primitive,
				commit.document,
			);
			return;
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
					coerceWithIndexed(withContext, commit.document as any as I),
				);
			}
			if (commit.nativeBackboneDocumentIndex) {
				const nativePreparedIndexPut =
					this._index._putPreparedNativeBackboneDocumentIndexWithContext(
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

		if (documentsChanged) {
			this.dispatchDocumentChangeIfObserved(documentsChanged);
		}
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
		const indexedDocuments = await this._index._putManyIdentityWithContext(
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
		if (indexedDocuments) {
			documentsChanged.added.push(...indexedDocuments);
		} else {
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
		documentsChanged?: DocumentsChange<T, I>,
	): Promise<boolean> {
		if (!documentsChanged) {
			const key = await this._index.getIdentityIndexedKeyByHead(head);
			if (!key) {
				return false;
			}
			if (modified.has(key.primitive)) {
				return true;
			}
			await this._index.delMany([key]);
			modified.add(key.primitive);
			return true;
		}
		const indexed = await this._index.getIdentityIndexedByHead(head);
		if (!indexed) {
			return false;
		}

		const key = indexed.id;
		if (modified.has(key.primitive)) {
			return true;
		}

		if (documentsChanged) {
			const value = coerceWithIndexed(
				indexed.value as unknown as WithIndexedContext<T, I>,
				indexed.value as unknown as I,
			);
			documentsChanged.removed.push(value);
		}

		await this._index.del(key);
		modified.add(key.primitive);
		return true;
	}

	private async collectRemovedDocumentChangesFromIndexedHeads(
		removed: ShallowOrFullEntry<Operation>[],
		modified: Set<string | number | bigint>,
		documentsChanged?: DocumentsChange<T, I>,
	): Promise<Set<string>> {
		const shallowRemovedHashes = removed
			.filter((entry): entry is ShallowEntry => !(entry instanceof Entry))
			.map((entry) => entry.hash);
		return this.collectRemovedDocumentChangesFromIndexedHeadHashes(
			shallowRemovedHashes,
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
		const indexedByHead = await this._index.getIdentityIndexedByHeads(
			removedHashes,
		);
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
			const valueWithoutContext = this.index.valueEncoding.decoder(
				payload.data,
			);
			key = indexerTypes.toId(this.idResolver(valueWithoutContext));
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
		if (this.isNativeMode()) {
			throw this.nativeModeError("does not support deletes");
		}
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
