import { deserialize, field, fixedArray, variant } from "@dao-xyz/borsh";
import { type AnyStore } from "@peerbit/any-store";
import {
	type Blocks,
	type GetOptions,
	cidifyString,
} from "@peerbit/blocks-interface";
import {
	Ed25519Keypair,
	type Identity,
	SignatureWithKey,
	X25519Keypair,
	randomBytes,
	sha256Base64Sync,
} from "@peerbit/crypto";
import { type Indices } from "@peerbit/indexer-interface";
import { type CryptoKeychain } from "@peerbit/keychain";
import { type Change } from "./change.js";
import {
	LamportClock as Clock,
	HLC,
	LamportClock,
	Timestamp,
} from "./clock.js";
import { type Encoding, NO_ENCODING } from "./encoding.js";
import {
	EntryIndex,
	type EntryIndexHashMutationLockOwner,
	type MaybeResolveOptions,
	type NativeCommittedAppendFactsTransaction,
	type NativeLogGraph,
	type PreparedAppendIndexFacts,
	type ResultsIterator,
	type ReturnTypeFromResolveOptions,
} from "./entry-index.js";
import { ShallowEntry, ShallowMeta } from "./entry-shallow.js";
import { EntryType } from "./entry-type.js";
import { type EncryptionTemplateMaybeEncrypted, EntryV0 } from "./entry-v0.js";
import { type EntryWithRefs } from "./entry-with-refs.js";
import {
	type CanAppend,
	Entry,
	type PreparedAppendChain,
	type PreparedAppendCommitOnlyChain,
	type PreparedAppendFacts,
	type PreparedEntryBlock,
	type PreparedNativeLogEntry,
	type ShallowOrFullEntry,
} from "./entry.js";
import { findUniques } from "./find-uniques.js";
import * as LogError from "./log-errors.js";
import * as Sorting from "./log-sorting.js";
import { logger as baseLogger } from "./logger.js";
import type { Payload } from "./payload.js";
import { canUseOptionalNativeModuleImports } from "./runtime.js";
import { Trim, type TrimOptions } from "./trim.js";

const { LastWriteWins } = Sorting;
const warn = baseLogger.newScope("warn");

type BlocksWithPutMany = Blocks & {
	putMany?: (blocks: PreparedEntryBlock[]) => Promise<string[]> | string[];
	rmMany?: (cids: string[]) => Promise<number | void> | number | void;
};

type BlocksWithPutKnownMany = Blocks & {
	putKnownMany: (
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	) => Promise<string[]> | string[];
};

type BlocksWithPutKnownManyColumns = Blocks & {
	putKnownManyColumns: (
		cids: string[],
		bytes: Uint8Array[],
	) => Promise<string[]> | string[];
};

type BlocksWithPutKnown = Blocks & {
	putKnown: (cid: string, bytes: Uint8Array) => Promise<string> | string;
};

type BlocksWithDurableWriteBarrier = Blocks & {
	waitForDurableWrites: () => Promise<void> | void;
};

type BlocksWithDurableFailureGuard = Blocks & {
	throwIfDurableWritesFailed: () => void;
};

type BlocksWithFailedNativeRollback = Blocks & {
	rollbackFailedNativeCommits: (
		cids: string[],
		restoreNativeCids?: string[],
		ownershipToken?: unknown,
	) => Promise<void>;
};

type BlocksWithNativeCommitOwnershipAck = Blocks & {
	acknowledgeNativeCommitOwnership: (ownershipToken: unknown) => void;
};

const hasPutMany = (storage: Blocks): storage is BlocksWithPutMany =>
	typeof (storage as BlocksWithPutMany).putMany === "function";

const hasPutKnown = (storage: Blocks): storage is BlocksWithPutKnown =>
	typeof (storage as BlocksWithPutKnown).putKnown === "function";

const hasPutKnownMany = (storage: Blocks): storage is BlocksWithPutKnownMany =>
	typeof (storage as BlocksWithPutKnownMany).putKnownMany === "function";

const hasPutKnownManyColumns = (
	storage: Blocks,
): storage is BlocksWithPutKnownManyColumns =>
	typeof (storage as BlocksWithPutKnownManyColumns).putKnownManyColumns ===
	"function";

const hasDurableWriteBarrier = (
	storage: Blocks,
): storage is BlocksWithDurableWriteBarrier =>
	typeof (storage as BlocksWithDurableWriteBarrier).waitForDurableWrites ===
	"function";

const hasDurableFailureGuard = (
	storage: Blocks,
): storage is BlocksWithDurableFailureGuard =>
	typeof (storage as BlocksWithDurableFailureGuard)
		.throwIfDurableWritesFailed === "function";

const hasFailedNativeRollback = (
	storage: Blocks,
): storage is BlocksWithFailedNativeRollback =>
	typeof (storage as BlocksWithFailedNativeRollback)
		.rollbackFailedNativeCommits === "function";

const hasNativeCommitOwnershipAck = (
	storage: Blocks,
): storage is BlocksWithNativeCommitOwnershipAck =>
	typeof (storage as BlocksWithNativeCommitOwnershipAck)
		.acknowledgeNativeCommitOwnership === "function";

type MaybePromise<T> = T | Promise<T>;

type InternalProfileValue = string | number | boolean | undefined;
type InternalProfileEvent = {
	name: string;
	component?: string;
	durationMs?: number;
	entries?: number;
	bytes?: number;
	messages?: number;
	count?: number;
	details?: Record<string, InternalProfileValue>;
};
type InternalProfileSink = (event: InternalProfileEvent) => void;
type InternalAppendHashesSink = (hashes: string[]) => void | Promise<void>;

type NativeCommittedAppendFinalizer = {
	acknowledge(onLowerMarkerDurable?: () => Promise<void>): Promise<void>;
	retainForRecovery(): void;
	rollback(): Promise<void>;
	settleForTerminal(): Promise<void>;
};

type NativeCommittedAppendAdmission = {
	settled: Promise<void>;
	release(): void;
};

type LogLifecycleState =
	| "closed"
	| "opening"
	| "active"
	| "closing"
	| "close-failed"
	| "dropping"
	| "drop-failed"
	| "dropped";

type LogCloseProgress = {
	admissionsSettled: boolean;
	finalizersSettled: boolean;
	rollbacksRetried: boolean;
	pendingWritesFlushed: boolean;
	blockHashesRetained: boolean;
	indexerStopped: boolean;
};

type LogDropProgress = {
	admissionsSettled: boolean;
	finalizersSettled: boolean;
	entryIndexCleared: boolean;
	indexerDropped: boolean;
	indexerStopped: boolean;
};

const createLogCloseProgress = (): LogCloseProgress => ({
	admissionsSettled: false,
	finalizersSettled: false,
	rollbacksRetried: false,
	pendingWritesFlushed: false,
	blockHashesRetained: false,
	indexerStopped: false,
});

const createLogDropProgress = (): LogDropProgress => ({
	admissionsSettled: false,
	finalizersSettled: false,
	entryIndexCleared: false,
	indexerDropped: false,
	indexerStopped: false,
});

const internalProfileNow = () => globalThis.performance?.now?.() ?? Date.now();
const internalProfileStart = (sink: InternalProfileSink | undefined) =>
	sink ? internalProfileNow() : 0;
const emitInternalProfileDuration = (
	sink: InternalProfileSink | undefined,
	startedAt: number,
	event: Omit<InternalProfileEvent, "durationMs">,
) => {
	if (!sink) {
		return;
	}
	sink({
		...event,
		durationMs: internalProfileNow() - startedAt,
	});
};
const EMPTY_NEXT_HASHES: string[] = [];
const EMPTY_NEXT_ENTRIES: Sorting.SortableEntry[] = [];
const normalizedUniqueStrings = (values: string[]): string[] =>
	values.length <= 1 ? values : [...new Set(values)];

type PreparedCommitOnlyAppendResult<T> = {
	entry: Entry<T>;
	materializeEntry: () => Entry<T>;
	removed: ShallowOrFullEntry<T>[];
	removedHashes?: string[];
	appendFacts: PreparedAppendFacts;
	shallowEntry: ShallowEntry;
	documentTrimmedHeadsProcessed?: boolean;
	documentPreviousContext?: {
		created: bigint;
		modified: bigint;
		head: string;
		gid: string;
		size: number;
	};
	nativeCommittedAppendFinalizer?: NativeCommittedAppendFinalizer;
};

type PreparedCommitOnlyAppendBatchResult<T> = {
	entries: Entry<T>[];
	materializeEntries: Array<() => Entry<T>>;
	removed: ShallowOrFullEntry<T>[];
	removedHashes?: string[];
	appendFacts: PreparedAppendFacts[];
	documentTrimmedHeadsProcessed?: boolean[];
	nativeCommittedAppendFinalizer?: NativeCommittedAppendFinalizer;
};

type PreparedIndependentAppendBatch = {
	blocks: PreparedEntryBlock[];
	prepared?: {
		shallowEntries: ShallowEntry[];
		nativeEntries?: PreparedNativeLogEntry[];
	};
};

type PreparedJoinNativeCommitInput = {
	entries: PreparedAppendJoinFacts[];
	hashes: string[];
	headFlags: boolean[];
	headFlagsBytes: Uint8Array;
	trustedMissing: boolean;
	validatePlan?: boolean;
};

type PreparedJoinCommittedInput = {
	entries: PreparedAppendJoinFacts[];
	hashes: string[];
	headFlags: boolean[];
	nativePreparedCommitted: boolean;
};

export type PreparedAppendJoinFacts = PreparedAppendIndexFacts & {
	bytes: Uint8Array;
	byteLength: number;
	materializeEntry?: () => Entry<any>;
};

type NativePreparedNoNextCommit = {
	bytes?: Uint8Array;
	getBytes?: (hash: string) => Uint8Array | undefined;
	cid?: string;
	hash?: string;
	gid?: string;
	next?: string[];
	byteLength: number;
	metaBytes?: Uint8Array;
	hashDigestBytes?: Uint8Array;
	trimmedEntries?: PreparedNativeLogEntry[];
	trimmedEntryHashes?: string[];
	nativeBlocksDeleted?: boolean;
	nativeDeleteCleanupToken?: unknown;
	nativeCommitOwnershipToken?: unknown;
	nativeIndexMutationLockOwner?: EntryIndexHashMutationLockOwner;
	documentTrimmedHeadsProcessed?: boolean;
	documentPreviousContext?: PreparedCommitOnlyAppendResult<unknown>["documentPreviousContext"];
};

type NativeNoNextCommitInput = {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	wallTime: bigint;
	logical: number;
	gid: string;
	type: EntryType;
	metaData?: Uint8Array;
	payloadData: Uint8Array;
	resolveTrimmedEntries?: boolean;
	trimLengthTo?: number;
};

type NativeCommitInput = NativeNoNextCommitInput & {
	next: string[];
};

const isPromiseLike = <T>(value: Promise<T> | T): value is Promise<T> =>
	typeof (value as { then?: unknown })?.then === "function";

const mapMaybePromise = <T, R>(
	value: MaybePromise<T>,
	fn: (value: T) => MaybePromise<R>,
): MaybePromise<R> => (isPromiseLike(value) ? value.then(fn) : fn(value));

const getErrorName = (error: unknown) =>
	typeof (error as { name?: unknown })?.name === "string"
		? (error as { name: string }).name
		: undefined;

const getErrorMessage = (error: unknown) =>
	error instanceof Error
		? error.message
		: typeof error === "string"
			? error
			: typeof (error as { message?: unknown })?.message === "string"
				? (error as { message: string }).message
				: String(error);

const isRecoverableJoinResolveError = (error: unknown): boolean => {
	const name = getErrorName(error);
	const message = getErrorMessage(error);
	return (
		message.includes("Failed to resolve block") ||
		message.includes("Failed to load entry from head") ||
		message.includes("Message did not have any valid receivers") ||
		name === "AbortError" ||
		name === "DeliveryError" ||
		name === "StreamStateError"
	);
};

type CreateSqliteIndexer = typeof import("@peerbit/indexer-sqlite3").create;
let sqliteCreate: CreateSqliteIndexer | undefined;
const createDefaultIndexer = async (): Promise<Indices> => {
	if (!sqliteCreate) {
		const mod = await import("@peerbit/indexer-sqlite3");
		sqliteCreate = mod.create;
	}
	return sqliteCreate();
};

export type LogEvents<T> = {
	onChange?: (change: Change<T> /* , reference?: R */) => void;
	onGidRemoved?: (gids: string[]) => Promise<void> | void;
};

export type MemoryProperties = {
	storage?: AnyStore;
	indexer?: Indices;
};

export type NativeGraphOptions = {
	heads?: boolean;
	optional?: boolean;
	graph?: NativeLogGraph;
};

export type LogProperties<T> = {
	keychain?: CryptoKeychain;
	encoding?: Encoding<T>;
	clock?: LamportClock;
	appendDurability?: AppendDurability;
	nativeGraph?: boolean | NativeGraphOptions;
	sortFn?: Sorting.SortFn;
	trim?: TrimOptions;
	canAppend?: CanAppend<T>;
	resolveRemotePeers?: (
		hash: string,
		options?: { signal?: AbortSignal },
	) => Promise<string[] | undefined> | string[] | undefined;
};

export type LogOptions<T> = LogProperties<T> & LogEvents<T> & MemoryProperties;

export type AppendDurability = "strict" | "buffered";

export type AppendOptions<T> = {
	durability?: AppendDurability;
	deferIndexWrite?: boolean;
	meta?: {
		type?: EntryType;
		gidSeed?: Uint8Array;
		data?: Uint8Array;
		timestamp?: Timestamp;
		next?: Entry<any>[] | ShallowEntry[];
	};

	identity?: Identity;
	signers?: ((
		data: Uint8Array,
	) => Promise<SignatureWithKey> | SignatureWithKey)[];

	trim?: TrimOptions;
	encryption?: {
		keypair: X25519Keypair;
		receiver: EncryptionTemplateMaybeEncrypted;
	};
	onChange?: OnChange<T>;
	canAppend?: CanAppend<T>;
};

type TrustedAppendOptions<T> = AppendOptions<T> & {
	__peerbitCanAppendAlreadyValidated?: boolean;
};

const canAppendAlreadyValidated = (options?: unknown): boolean =>
	(options as { __peerbitCanAppendAlreadyValidated?: unknown } | undefined)
		?.__peerbitCanAppendAlreadyValidated === true;

const withCanAppendAlreadyValidated = <T>(
	options: AppendOptions<T> = {},
): TrustedAppendOptions<T> =>
	canAppendAlreadyValidated(options)
		? (options as TrustedAppendOptions<T>)
		: {
				...options,
				__peerbitCanAppendAlreadyValidated: true,
			};

export type { PreparedAppendFacts } from "./entry.js";

type OnChange<T> = (
	change: Change<T>,
	reference?: undefined,
) => void | Promise<void>;

export type JoinOptions<T> = {
	verifySignatures?: boolean;
	trim?: TrimOptions;
	timeout?: number;
	onChange?: OnChange<T>;
	reset?: boolean;
};

type TrustedJoinOptions<T> = JoinOptions<T> & {
	__peerbitBatchIndependent?: boolean;
	__peerbitEntriesAlreadyMissing?: boolean;
	__peerbitCanAppendAlreadyValidated?: boolean;
	__peerbitOnAppendHashes?: InternalAppendHashesSink;
	__peerbitDeferIndexWrite?: boolean;
	__peerbitProfile?: InternalProfileSink;
};

type TrustedPreparedAppendFactsBatchJoinOptions = {
	__peerbitEntriesAlreadyMissing?: boolean;
	__peerbitCanAppendAlreadyValidated?: boolean;
	__peerbitOnAppendHashes?: InternalAppendHashesSink;
	__peerbitDeferIndexWrite?: boolean;
	__peerbitProfile?: InternalProfileSink;
	__peerbitNativePreparedJoinCommit?: (
		input: PreparedJoinNativeCommitInput,
	) => MaybePromise<boolean>;
	__peerbitNativePreparedJoinCommitValidatesPlan?: boolean;
	__peerbitOnPreparedJoinCommitted?: (
		input: PreparedJoinCommittedInput,
	) => MaybePromise<void>;
};

export type JoinableEntry = {
	meta: {
		clock: {
			timestamp: Timestamp;
		};
		next: string[];
		gid: string;
		type: EntryType;
	};
	hash: string;
};

export const ENTRY_JOIN_SHAPE = {
	hash: true,
	meta: { type: true, next: true, gid: true, clock: true },
} as const;

type PendingDelete<T> = {
	entry: ShallowOrFullEntry<T>;
	fn: () => Promise<ShallowEntry | undefined>;
};

type EntryWithMetaBytes = {
	getMetaBytes?: () => Uint8Array | undefined;
	getHashDigestBytes?: () => Uint8Array | undefined;
};

type MutationCallback = (...args: any[]) => any;

@variant(0)
export class Log<T> {
	@field({ type: fixedArray("u8", 32) })
	private _id: Uint8Array;

	/* private _sortFn!: Sorting.ISortFunction; */
	private _storage!: Blocks;
	private _hlc!: HLC;

	// Identity
	private _identity!: Identity;

	// Keeping track of entries
	private _entryIndex!: EntryIndex<T>;
	/* 	private _headsIndex!: HeadsIndex<T>;
		private _values!: Values<T>;
	 */
	// Index of all next pointers in this log
	/* private _nextsIndex!: Map<string, Set<string>>; */
	private _keychain?: CryptoKeychain;
	private _encoding!: Encoding<T>;
	private _trim!: Trim<T>;
	private _canAppend?: CanAppend<T>;
	private _onChange?: OnChange<T>;
	private _closed = true;
	private _dropCompleted = false;
	private _closeCompleted = false;
	private _terminalAdmissionClosed = true;
	private _lifecycleState: LogLifecycleState = "closed";
	private _lifecycleEpoch = 0;
	private _openPromise?: Promise<void>;
	private _closePromise?: Promise<void>;
	private _dropPromise?: Promise<void>;
	private _terminalLifecycleQueue: Promise<void> = Promise.resolve();
	private _closeProgress = createLogCloseProgress();
	private _dropProgress = createLogDropProgress();
	private _closeController!: AbortController;
	private _loadedOnce = false;
	private _indexer!: Indices;
	private _openingIndexer?: Indices;
	private _appendDurability!: AppendDurability;
	private _joining!: Map<string, Promise<any>>; // entry hashes that are currently joining into this log
	private _sortFn!: Sorting.SortFn;
	private _hasCustomCanAppend = false;
	private _nativeCommittedAppendFinalizers?: Set<NativeCommittedAppendFinalizer>;
	private _nativeCommittedAppendAdmissions?: Set<Promise<void>>;
	private _mutationCallbacksInFlight = 0;
	private _mutationCallbackWrappers = new WeakMap<
		MutationCallback,
		MutationCallback
	>();

	constructor(properties?: { id?: Uint8Array }) {
		this._id = properties?.id || randomBytes(32);
		this.ensureRuntimeState();
	}

	private ensureRuntimeState(): void {
		// Borsh creates instances with Object.create by default, so undecorated
		// runtime fields and their class initializers are absent after a round trip.
		// Initialize each field independently to preserve failed close/drop progress
		// and in-flight lifecycle ownership on ordinary, already-live instances.
		this._closed ??= true;
		this._dropCompleted ??= false;
		this._closeCompleted ??= false;
		this._terminalAdmissionClosed ??= true;
		this._lifecycleState ??= "closed";
		this._lifecycleEpoch ??= 0;
		this._terminalLifecycleQueue ??= Promise.resolve();
		this._closeProgress ??= createLogCloseProgress();
		this._dropProgress ??= createLogDropProgress();
		this._closeController ??= new AbortController();
		this._loadedOnce ??= false;
		this._hasCustomCanAppend ??= false;
		this._mutationCallbacksInFlight ??= 0;
		this._mutationCallbackWrappers ??= new WeakMap();
	}

	private throwIfDurableWritesFailed(): void {
		this.ensureRuntimeState();
		if (this._terminalAdmissionClosed) {
			throw new Error("Log is closed");
		}
		if (hasDurableFailureGuard(this._storage)) {
			this._storage.throwIfDurableWritesFailed();
		}
		this._entryIndex?.throwIfNativeDurableTransactionMutationsFailed();
	}

	private waitForDurableWrites(): MaybePromise<void> {
		return hasDurableWriteBarrier(this._storage)
			? this._storage.waitForDurableWrites()
			: undefined;
	}

	async open(store: Blocks, identity: Identity, options: LogOptions<T> = {}) {
		this.ensureRuntimeState();
		// Reject before touching lifecycle ownership. A rejected reopen must never
		// change the state of the live generation, and one in-flight initializer
		// must remain the sole owner of fields/resources.
		if (
			this._openPromise ||
			!this._closed ||
			this._lifecycleState === "active" ||
			this._lifecycleState === "opening"
		) {
			throw new Error("Already open");
		}
		if (this._dropPromise || this._lifecycleState === "dropping") {
			throw new Error("Log drop must complete before reopening");
		}
		if (this._closePromise || this._lifecycleState === "closing") {
			throw new Error("Log close must complete before reopening");
		}
		if (this._lifecycleState === "drop-failed") {
			throw new Error("Failed log drop must be retried before reopening");
		}
		if (this._lifecycleState === "close-failed") {
			throw new Error("Failed log close must be retried before reopening");
		}
		// Admission of a new generation resets terminal progress synchronously.
		// A close admitted in the next microtask must not inherit completed stages
		// from the previous generation and skip teardown of this initializer.
		this._dropCompleted = false;
		this._closeCompleted = false;
		this._terminalAdmissionClosed = true;
		this._closeProgress = createLogCloseProgress();
		this._dropProgress = createLogDropProgress();
		const epoch = ++this._lifecycleEpoch;
		const terminalTail = this._terminalLifecycleQueue;
		const operation = (async () => {
			await terminalTail;
			if (epoch === this._lifecycleEpoch) {
				this._lifecycleState = "opening";
			}
			try {
				await this.openInternal(store, identity, options);
			} catch (error) {
				const openingIndexer = this._openingIndexer;
				this._openingIndexer = undefined;
				let cleanupError: unknown;
				if (openingIndexer) {
					try {
						await openingIndexer.stop?.();
					} catch (stopError) {
						cleanupError = stopError;
					}
				}
				this._closed = true;
				if (cleanupError !== undefined) {
					this._closeProgress = {
						admissionsSettled: true,
						finalizersSettled: true,
						rollbacksRetried: true,
						pendingWritesFlushed: true,
						blockHashesRetained: true,
						indexerStopped: false,
					};
					if (epoch === this._lifecycleEpoch) {
						this._lifecycleState = "close-failed";
					}
					throw new AggregateError(
						[error, cleanupError],
						"Log open and indexer cleanup both failed",
					);
				}
				// The failed initializer successfully stopped every resource it opened.
				// Treat close as complete so a later/concurrent close never scans the
				// stopped partial EntryIndex. Keep the stopped indexer reference only so
				// an explicitly queued drop can restart it and erase existing rows.
				this._closeProgress = {
					admissionsSettled: true,
					finalizersSettled: true,
					rollbacksRetried: true,
					pendingWritesFlushed: true,
					blockHashesRetained: false,
					indexerStopped: true,
				};
				this._closeCompleted = true;
				this._loadedOnce = false;
				if (epoch === this._lifecycleEpoch) {
					this._lifecycleState = "closed";
				}
				throw error;
			}
			this._openingIndexer = undefined;
			if (epoch !== this._lifecycleEpoch) {
				// A close/drop admitted while startup was in flight owns teardown.
				this._closed = true;
				return;
			}
			this._closed = false;
			this._terminalAdmissionClosed = false;
			this._lifecycleState = "active";
			this._closeController = new AbortController();
		})();
		const wrapped: Promise<void> = operation.finally(() => {
			if (this._openPromise === wrapped) {
				this._openPromise = undefined;
			}
		});
		this._openPromise = wrapped;
		return wrapped;
	}

	private async openInternal(
		store: Blocks,
		identity: Identity,
		options: LogOptions<T> = {},
	) {
		if (store == null) {
			throw LogError.BlockStoreNotDefinedError();
		}

		if (identity == null) {
			throw new Error("Identity is required");
		}

		if (this.closed === false) {
			throw new Error("Already open");
		}

		this._closeController = new AbortController();

		const {
			encoding,
			trim,
			keychain,
			indexer,
			onGidRemoved,
			sortFn,
			resolveRemotePeers,
		} = options;

		// TODO do correctly with tie breaks
		this._sortFn = sortFn || LastWriteWins;

		this._storage = store;
		this._indexer = indexer || (await createDefaultIndexer());
		this._openingIndexer = this._indexer;
		await this._indexer.start?.();

		this._encoding = encoding || NO_ENCODING;
		this._joining = new Map();

		// Identity
		this._identity = identity;

		// encoder/decoder
		this._keychain = keychain;

		// Clock
		this._hlc = new HLC();

		const id = this.id;
		if (!id) {
			throw new Error("Id not set");
		}

		const nativeGraphOption = options.nativeGraph;
		const nativeGraph =
			nativeGraphOption &&
			(await (async () => {
				const nativeGraphOptions =
					typeof nativeGraphOption === "object" ? nativeGraphOption : undefined;
				if (nativeGraphOptions?.graph) {
					const headsRequested = nativeGraphOptions.heads !== false;
					return {
						graph: nativeGraphOptions.graph,
						useHeads: headsRequested && this._sortFn === LastWriteWins,
					};
				}
				if (!canUseOptionalNativeModuleImports()) {
					if (nativeGraphOptions?.optional === true) {
						return undefined;
					}
					throw new Error(
						"Log nativeGraph is unavailable in service worker contexts",
					);
				}
				let createLogGraphIndex: () => Promise<NativeLogGraph>;
				try {
					({ createLogGraphIndex } = (await import(
						/* @vite-ignore */ ["@peerbit", "log-rust"].join("/")
					)) as {
						createLogGraphIndex: () => Promise<NativeLogGraph>;
					});
					const headsRequested = nativeGraphOptions?.heads !== false;
					return {
						graph: await createLogGraphIndex(),
						useHeads: headsRequested && this._sortFn === LastWriteWins,
					};
				} catch {
					if (nativeGraphOptions?.optional === true) {
						return undefined;
					}
					throw new Error(
						"Log nativeGraph requires @peerbit/log-rust to be installed and built",
					);
				}
			})());

		this._entryIndex = new EntryIndex({
			store: this._storage,
			init: (e) => e.init(this),
			onGidRemoved,
			nativeGraph: nativeGraph || undefined,
			index: await (
				await this._indexer.scope("heads")
			).init({ schema: ShallowEntry }),
			publicKey: this._identity.publicKey,
			sort: this._sortFn,
			resolveRemotePeers,
		});
		await this._entryIndex.init();
		this._appendDurability =
			options.appendDurability ??
			((await this._entryIndex.properties.index.persisted())
				? "strict"
				: "buffered");
		/* 	this._values = new Values(this._entryIndex, this._sortFn); */

		this._trim = new Trim(
			{
				index: this._entryIndex,
				deleteNode: async (
					node: ShallowEntry,
					options?: { resolveDeletedEntry?: boolean },
				) => {
					const shouldResolve = options?.resolveDeletedEntry !== false;
					const resolved = shouldResolve
						? await this.get(node.hash)
						: undefined;
					const deleted = await this._entryIndex.delete(node.hash, node);
					await this._storage.rm(node.hash);
					if (!deleted) {
						return resolved;
					}
					return shouldResolve ? resolved : deleted;
				},
				deleteNodes: this._entryIndex.canDeleteMany()
					? (
							nodes: ShallowEntry[],
							options?: {
								resolveDeletedEntry?: boolean;
								skipNextHeadUpdates?: boolean;
							},
						): MaybePromise<(Entry<T> | ShallowEntry)[]> => {
							if (nodes.length === 0) {
								return [];
							}
							const shouldResolve = options?.resolveDeletedEntry !== false;
							if (!shouldResolve) {
								return this._entryIndex.deleteManyMaybe(nodes, {
									skipNextHeadUpdates: options?.skipNextHeadUpdates,
								});
							}
							return (async () => {
								const resolvedByHash = new Map<string, Entry<T>>();
								const resolved = await this._entryIndex.getMany(
									nodes.map((node) => node.hash),
									{ type: "full", ignoreMissing: true },
								);
								for (const entry of resolved) {
									if (entry) {
										resolvedByHash.set(entry.hash, entry);
									}
								}
								const deleted = await this._entryIndex.deleteMany(nodes, {
									skipNextHeadUpdates: options?.skipNextHeadUpdates,
								});
								return deleted
									.map((node) => resolvedByHash.get(node.hash))
									.filter((entry): entry is Entry<T> => !!entry);
							})();
						}
					: undefined,
				sortFn: this._sortFn,
				getLength: () => this.length,
			},
			this.wrapTrimCallbacks(trim),
		);

		this._canAppend = async (entry) => {
			if (options?.canAppend) {
				if (
					!(await this.runWithMutationCallback(() => options.canAppend!(entry)))
				) {
					return false;
				}
			}
			return true;
		};
		this._hasCustomCanAppend = !!options?.canAppend;

		this._onChange = options?.onChange
			? this.wrapMutationCallback(options.onChange)
			: undefined;
	}

	private _idString: string | undefined;

	get idString() {
		if (!this.id) {
			throw new Error("Id not set");
		}
		return this._idString || (this._idString = Log.createIdString(this.id));
	}

	public static createIdString(id: Uint8Array) {
		return sha256Base64Sync(id);
	}

	get id() {
		return this._id;
	}
	set id(id: Uint8Array) {
		if (this.closed === false) {
			throw new Error("Can not change id after open");
		}
		this._idString = undefined;
		this._id = id;
	}

	/**
	 * Returns the length of the log.
	 */
	get length() {
		if (this.closed) {
			throw new Error("Closed");
		}
		return this._entryIndex.length;
	}

	get canAppend() {
		return this._canAppend;
	}

	/**
	 * Checks if a entry is part of the log
	 * @param {string} hash The hash of the entry
	 * @returns {boolean}
	 */

	has(cid: string) {
		return this._entryIndex.has(cid);
	}

	hasMany(cids: Iterable<string>) {
		return this._entryIndex.hasMany(cids);
	}

	/**
	 * Get all entries sorted. Don't use this method anywhere where performance matters
	 */
	async toArray(): Promise<Entry<T>[]> {
		// we call init, because the values might be unitialized
		return this.entryIndex.iterate([], this.sortFn.sort, true).all();
	}

	/**
	 * Returns the head index
	 */

	getHeads<R extends MaybeResolveOptions = false>(
		resolve: R = false as R,
	): ResultsIterator<ReturnTypeFromResolveOptions<R, T>> {
		return this.entryIndex.getHeads(undefined, resolve);
	}

	/**
	 * Returns an array of Entry objects that reference entries which
	 * are not in the log currently.
	 * @returns {Array<Entry<T>>}
	 */
	async getTails(): Promise<Entry<T>[]> {
		return Log.findTails(await this.toArray());
	}

	/**
	 * Returns an array of hashes that are referenced by entries which
	 * are not in the log currently.
	 * @returns {Array<string>} Array of hashes
	 */
	async getTailHashes(): Promise<string[]> {
		return Log.findTailHashes(await this.toArray());
	}

	/**
	 * Get local HLC
	 */
	get hlc(): HLC {
		return this._hlc;
	}

	get identity(): Identity {
		return this._identity;
	}

	get blocks(): Blocks {
		return this._storage;
	}

	get entryIndex(): EntryIndex<T> {
		return this._entryIndex;
	}

	get keychain() {
		return this._keychain;
	}

	get encoding() {
		return this._encoding;
	}

	get sortFn() {
		return this._sortFn;
	}

	get closed() {
		return this._closed !== false;
	}

	/**
	 * Set the identity for the log
	 * @param {Identity} [identity] The identity to be set
	 */
	setIdentity(identity: Identity) {
		this._identity = identity;
	}

	/**
	 * Get an entry.
	 * @param {string} [hash] The hashes of the entry
	 */
	get(hash: string, options?: GetOptions): Promise<Entry<T> | undefined> {
		return this._entryIndex.get(
			hash,
			options
				? {
						type: "full",
						remote: options.remote,
						ignoreMissing: true, // always return undefined instead of throwing errors on missing entries
					}
				: { type: "full", ignoreMissing: true },
		);
	}

	/**
	 * Get entries while preserving the order and duplicates in `hashes`.
	 * Missing entries are returned as `undefined`.
	 */
	getMany(
		hashes: string[],
		options?: GetOptions,
	): Promise<Array<Entry<T> | undefined>> {
		return this._entryIndex.getMany(
			hashes,
			options
				? {
						type: "full",
						remote: options.remote,
						ignoreMissing: true,
					}
				: { type: "full", ignoreMissing: true },
		);
	}

	/**
	 * Get a entry with shallow representation
	 * @param {string} [hash] The hashes of the entry
	 */
	async getShallow(hash: string): Promise<ShallowEntry | undefined> {
		return (await this._entryIndex.getShallow(hash))?.value;
	}

	async getReferenceSamples(
		from: Entry<T>,
		options?: { pointerCount?: number; memoryLimit?: number },
	): Promise<Entry<T>[]> {
		const hashes = new Set<string>();
		const pointerCount = options?.pointerCount || 0;
		const memoryLimit = options?.memoryLimit;
		const maxDistance = Math.min(pointerCount, this.entryIndex.length);
		if (maxDistance === 0) {
			return [];
		}
		hashes.add(from.hash);
		let memoryCounter = from.payload.byteLength;
		if (from.meta.next?.length > 0 && pointerCount >= 2) {
			let next = new Set(from.meta.next);
			let prev = 2;
			outer: for (let i = 2; i <= maxDistance - 1; i *= 2) {
				for (let j = prev; j < i; j++) {
					if (next.size === 0) {
						break outer;
					}
					const nextNext = new Set<string>();
					for (const n of next) {
						const nentry = await this.get(n);
						if (nentry) {
							for (const n2 of nentry.meta.next) {
								nextNext.add(n2);
							}
						}
					}
					next = nextNext;
				}
				prev = i;
				if (next) {
					for (const n of next) {
						if (!memoryLimit) {
							hashes.add(n);
						} else {
							const entry = await this.get(n);
							if (!entry) {
								break outer;
							}
							memoryCounter += entry.payload.byteLength;
							if (memoryCounter > memoryLimit) {
								break outer;
							}
							hashes.add(n);
						}
						if (hashes.size === pointerCount) {
							break outer;
						}
					}
				}
			}
		}

		const ret: Entry<any>[] = [];
		for (const hash of hashes) {
			const entry = await this.get(hash);
			if (entry) {
				ret.push(entry);
			}
		}
		return ret;
	}

	/**
	 * Append an entry to the log.
	 * @param {T} data The data to be appended
	 * @param {AppendOptions} [options] The options for the append
	 * @returns {{ entry: Entry<T>; removed: ShallowEntry[] }} The appended entry and an array of removed entries
	 */
	async append(
		data: T,
		options: AppendOptions<T> = {},
	): Promise<{ entry: Entry<T>; removed: ShallowOrFullEntry<T>[] }> {
		this.throwIfDurableWritesFailed();
		return this.withNativeCommittedAppendAdmission(() =>
			this.appendAdmitted(data, options),
		);
	}

	private async appendAdmitted(
		data: T,
		options: AppendOptions<T>,
	): Promise<{ entry: Entry<T>; removed: ShallowOrFullEntry<T>[] }> {
		const nexts = await this.getNextsForAppend(options);
		const deferBlockStore = hasPutMany(this._storage);
		type MutationResult = {
			entry: Entry<T>;
			pendingDeletes: (
				| PendingDelete<T>
				| { entry: ShallowOrFullEntry<T>; fn: undefined }
			)[];
			removed: ShallowOrFullEntry<T>[];
			changes: Change<T>;
		};
		const finishMutation = async (entry: Entry<T>): Promise<MutationResult> => {
			const pendingDeletes: MutationResult["pendingDeletes"] =
				await this.processEntry(entry);
			entry.init({ encoding: this._encoding, keychain: this._keychain });
			const trimmed = await this.trimIfConfigured(options.trim);
			if (trimmed) {
				for (const trimmedEntry of trimmed) {
					pendingDeletes.push({ entry: trimmedEntry, fn: undefined });
				}
			}
			const removed = pendingDeletes.map((pending) => pending.entry);
			return {
				entry,
				pendingDeletes,
				removed,
				changes: { added: [{ head: true, entry }], removed },
			};
		};

		let mutation: MutationResult | undefined;
		if (this.entryIndex.properties.nativeGraph) {
			const nativeAppendChain = await this.createNativePlainAppendChain(
				[data],
				options,
				nexts,
				deferBlockStore,
			);
			if (nativeAppendChain) {
				const entry = nativeAppendChain.entries[0]!;
				try {
					await this.joinMissingNexts(entry, nexts);
					if (deferBlockStore && !nativeAppendChain.nativeBlocksCommitted) {
						await this.putAppendEntryBlocks([entry], nativeAppendChain.blocks);
					}
					await this.putAppendEntries(
						[entry],
						options,
						nexts.map((next) => next.hash),
						nativeAppendChain,
					);
				} catch (error) {
					if (nativeAppendChain.nativeGraphUpdated) {
						this.rollbackNativeAppendGraph([entry]);
					}
					if (nativeAppendChain.nativeBlocksCommitted) {
						await this.rollbackNativeAppendBlocks([entry]);
					}
					throw error;
				}
				mutation = await finishMutation(entry);
			}
		}

		if (!mutation) {
			const entry = await this.createAppendEntry(data, options, nexts);
			await this.joinMissingNexts(entry, nexts);
			await this.putAppendEntry(entry, options);
			mutation = await finishMutation(entry);
		}

		if (options.onChange) {
			await this.runWithMutationCallback(() =>
				options.onChange!(mutation.changes),
			);
		} else {
			await this._onChange?.(mutation.changes);
		}
		await Promise.all(mutation.pendingDeletes.map((pending) => pending.fn?.()));
		return { entry: mutation.entry, removed: mutation.removed };
	}

	// Internal trusted local append path for callers that already handled validation
	// and want to apply change observers themselves.
	private async appendLocallyPrepared(
		data: T,
		options: AppendOptions<T> = {},
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
			payloadData?: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
		},
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
		change: Change<T>;
		appendFacts: PreparedAppendFacts;
	}> {
		this.throwIfDurableWritesFailed();
		return this.withNativeCommittedAppendAdmission(() =>
			this.appendLocallyPreparedAdmitted(data, options, properties),
		);
	}

	private async appendLocallyPreparedAdmitted(
		data: T,
		options: AppendOptions<T>,
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
			payloadData?: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
		},
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
		change: Change<T>;
		appendFacts: PreparedAppendFacts;
	}> {
		if (
			options.canAppend ||
			options.onChange ||
			options.meta?.type === EntryType.CUT
		) {
			throw new Error(
				"appendLocallyPrepared only supports trusted plain local appends",
			);
		}

		const appendOptions = withCanAppendAlreadyValidated(options);
		const nextsResult = this.getNextsForAppend(appendOptions);
		const nexts = isPromiseLike(nextsResult) ? await nextsResult : nextsResult;
		const deferBlockStore = hasPutMany(this._storage);
		const nativeAppendChain = this.entryIndex.properties.nativeGraph
			? await this.createNativePlainAppendChain(
					[data],
					appendOptions,
					nexts,
					deferBlockStore,
					properties?.payloadData ? [properties.payloadData] : undefined,
				)
			: undefined;
		let entry: Entry<T>;
		if (nativeAppendChain) {
			entry = nativeAppendChain.entries[0]!;
			try {
				if (!properties?.skipMissingNextJoin) {
					await this.joinMissingNexts(entry, nexts);
				}
				if (deferBlockStore && !nativeAppendChain.nativeBlocksCommitted) {
					await this.putAppendEntryBlocks([entry], nativeAppendChain.blocks);
				}
				await this.putAppendEntries(
					[entry],
					appendOptions,
					nexts.map((next) => next.hash),
					nativeAppendChain,
				);
			} catch (error) {
				if (nativeAppendChain.nativeGraphUpdated) {
					this.rollbackNativeAppendGraph([entry]);
				}
				if (nativeAppendChain.nativeBlocksCommitted) {
					await this.rollbackNativeAppendBlocks([entry]);
				}
				throw error;
			}
		} else {
			if (data == null && properties?.payloadData) {
				throw new Error(
					"appendLocallyPrepared payload-only path requires native append support",
				);
			}
			entry = await this.createAppendEntry(data, appendOptions, nexts);
			if (!properties?.skipMissingNextJoin) {
				await this.joinMissingNexts(entry, nexts);
			}
			await this.putAppendEntry(entry, appendOptions);
		}

		entry.init({ encoding: this._encoding, keychain: this._keychain });

		const trimmed = await this.trimIfConfigured(appendOptions.trim, {
			resolveDeletedEntries: properties?.resolveTrimmedEntries,
		});
		const removed = trimmed ?? [];
		const change: Change<T> = {
			added: [{ head: true, entry }],
			removed,
		};
		const appendFacts = this.createPreparedAppendFacts(
			[entry],
			nativeAppendChain,
		)[0]!;

		return { entry, removed, change, appendFacts };
	}

	// Internal trusted local append path for callers that can consume compact
	// append facts before a public Entry object is needed.
	private appendLocallyPreparedCommitOnly(
		data: T,
		options: AppendOptions<T> = {},
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
			payloadData?: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
		},
	): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> {
		this.throwIfDurableWritesFailed();
		if (
			options.canAppend ||
			options.onChange ||
			options.meta?.type === EntryType.CUT
		) {
			throw new Error(
				"appendLocallyPreparedCommitOnly only supports trusted plain local appends",
			);
		}

		const appendOptions = withCanAppendAlreadyValidated(options);
		const nextsResult = this.getNextsForAppend(appendOptions);
		return mapMaybePromise(nextsResult, (nexts) =>
			this.appendLocallyPreparedCommitOnlyWithNexts(
				data,
				appendOptions,
				nexts,
				properties,
				this.getNativeCommitOnlyTrimLengthTo(
					appendOptions.trim,
					properties?.resolveTrimmedEntries,
				),
			),
		);
	}

	private appendLocallyPreparedNativeNoNextCommitOnly(
		data: T,
		options: AppendOptions<T> = {},
		properties: {
			payloadData?: Uint8Array;
			resolveTrimmedEntries?: boolean;
			skipMissingNextJoin?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			input: NativeNoNextCommitInput,
		) => MaybePromise<NativePreparedNoNextCommit | undefined>,
	): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> {
		this.throwIfDurableWritesFailed();
		const directResult = this.appendLocallyPreparedNativeKnownNoNextCommitOnly(
			data,
			options,
			properties,
			prepare,
		);
		if (directResult !== undefined) {
			return directResult;
		}
		return this.appendLocallyPreparedNativeCommitOnly(
			data,
			options,
			properties,
			prepare,
		);
	}

	private appendLocallyPreparedNativeKnownNoNextCommitOnly(
		data: T,
		options: AppendOptions<T> = {},
		properties: {
			payloadData?: Uint8Array;
			resolveTrimmedEntries?: boolean;
			skipMissingNextJoin?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			input: NativeNoNextCommitInput,
		) => MaybePromise<NativePreparedNoNextCommit | undefined>,
	): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> {
		this.throwIfDurableWritesFailed();
		if (options.meta?.next == null || options.meta.next.length !== 0) {
			return undefined;
		}
		const resolvedTrim = options.trim ?? this._trim.options;
		const supportsNativeTrim =
			!resolvedTrim ||
			(resolvedTrim.type === "length" &&
				!resolvedTrim.filter?.canTrim &&
				properties.resolveTrimmedEntries === false);
		if (!supportsNativeTrim) {
			return undefined;
		}
		const nativeTrimLengthTo = this.getNativeCommitOnlyTrimLengthTo(
			options.trim,
			properties.resolveTrimmedEntries,
		);
		if (!options.meta?.gidSeed) {
			const directResult =
				this.appendLocallyPreparedNativeKnownNoNextDirectCommitOnly(
					data,
					options,
					properties,
					prepare,
					nativeTrimLengthTo,
				);
			if (directResult !== undefined) {
				return directResult;
			}
		}
		return this.appendLocallyPreparedNativeCommitOnly(
			data,
			options,
			properties,
			prepare,
			true,
		);
	}

	private appendLocallyPreparedNativeKnownNoNextDirectCommitOnly(
		data: T,
		options: AppendOptions<T> = {},
		properties: {
			payloadData?: Uint8Array;
			resolveTrimmedEntries?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			input: NativeNoNextCommitInput,
		) => MaybePromise<NativePreparedNoNextCommit | undefined>,
		nativeTrimLengthTo?: number,
	): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> {
		if (
			options.canAppend ||
			options.onChange ||
			options.encryption ||
			options.signers ||
			options.identity ||
			options.meta?.timestamp ||
			options.meta?.type === EntryType.CUT ||
			options.meta?.next == null ||
			options.meta.next.length !== 0 ||
			options.meta?.gidSeed ||
			(this._hasCustomCanAppend && !canAppendAlreadyValidated(options))
		) {
			return undefined;
		}
		const identity = this._identity;
		if (!(identity instanceof Ed25519Keypair)) {
			return undefined;
		}
		const payloadData =
			properties.payloadData ??
			(data == null ? undefined : this._encoding.encoder(data));
		if (!payloadData || !hasPutMany(this._storage)) {
			return undefined;
		}

		const resolvedGid = EntryV0.createGid() as string;
		const timestamp = this._hlc.now();
		const entryType = options.meta?.type ?? EntryType.APPEND;
		const nativePreparation = this.prepareNativeCommittedAppend(() =>
			prepare({
				clockId: identity.publicKey.bytes,
				privateKey: identity.privateKey.privateKey,
				publicKey: identity.publicKey.publicKey,
				wallTime: timestamp.wallTime,
				logical: timestamp.logical,
				gid: resolvedGid,
				type: entryType,
				metaData: options.meta?.data,
				payloadData,
				resolveTrimmedEntries: properties.resolveTrimmedEntries,
				trimLengthTo: nativeTrimLengthTo,
			}),
		);
		const consumePrepared = (
			prepared: NativePreparedNoNextCommit | undefined,
		): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> => {
			if (!prepared) {
				return undefined;
			}
			const hash = prepared.cid ?? prepared.hash;
			if (!hash) {
				return undefined;
			}
			const shouldRetainMaterializationBytes =
				properties.retainMaterializationBytes === true ||
				!!(options.trim ?? this._trim.options);
			let retainedMaterializationBytes: Uint8Array | undefined;
			const retainMaterializationBytes = () => {
				if (
					retainedMaterializationBytes ||
					!shouldRetainMaterializationBytes ||
					prepared.bytes
				) {
					return;
				}
				const bytes =
					prepared.getBytes?.(hash) ??
					(this._storage.get(hash) as Uint8Array | undefined);
				if (bytes && typeof (bytes as { then?: unknown }).then !== "function") {
					retainedMaterializationBytes = bytes;
				}
			};
			const effectiveNextHashes = prepared.next ?? EMPTY_NEXT_HASHES;
			const effectiveGid = prepared.gid ?? resolvedGid;
			let clock: Clock | undefined;
			const getClock = () =>
				(clock ??= new Clock({
					id: identity.publicKey.bytes,
					timestamp,
				}));
			let shallowEntry: ShallowEntry | undefined;
			const getShallowEntry = () =>
				(shallowEntry ??= new ShallowEntry({
					hash,
					payloadSize: payloadData.byteLength,
					head: true,
					meta: new ShallowMeta({
						gid: effectiveGid,
						data: options.meta?.data,
						clock: getClock(),
						next: effectiveNextHashes,
						type: entryType,
					}),
				}));
			const appendFacts: PreparedAppendFacts = {
				hash,
				gid: effectiveGid,
				next: effectiveNextHashes,
				wallTime: timestamp.wallTime,
				logical: timestamp.logical,
				clockId: identity.publicKey.bytes,
				type: entryType,
				metaData: options.meta?.data,
				payloadSize: payloadData.byteLength,
				metaBytes: prepared.metaBytes,
				hashDigestBytes: prepared.hashDigestBytes,
			};
			let materializedEntry: Entry<T> | undefined;
			const materializeEntry = () => {
				if (materializedEntry) {
					return materializedEntry;
				}
				const bytes =
					prepared.bytes ??
					retainedMaterializationBytes ??
					prepared.getBytes?.(hash) ??
					(this._storage.get(hash) as Uint8Array | undefined);
				if (
					!bytes ||
					typeof (bytes as { then?: unknown }).then === "function"
				) {
					throw new Error("Missing synchronous native append block bytes");
				}
				const entry = deserialize(bytes, Entry) as Entry<T>;
				entry.hash = hash;
				entry.size = prepared.byteLength;
				entry.createdLocally = true;
				Entry.prepareShallowEntry(entry, getShallowEntry());
				entry.init({ encoding: this._encoding, keychain: this._keychain });
				materializedEntry = entry;
				return entry;
			};
			let indexTransaction: NativeCommittedAppendFactsTransaction | undefined;
			const finish = (): PreparedCommitOnlyAppendResult<T> => {
				retainMaterializationBytes();
				return {
					get entry() {
						return materializeEntry();
					},
					materializeEntry,
					removed: [],
					appendFacts,
					get shallowEntry() {
						return getShallowEntry();
					},
					documentTrimmedHeadsProcessed: prepared.documentTrimmedHeadsProcessed,
					documentPreviousContext: prepared.documentPreviousContext,
				};
			};
			const finishTrim = ():
				| PreparedCommitOnlyAppendResult<T>
				| Promise<PreparedCommitOnlyAppendResult<T>> => {
				retainMaterializationBytes();
				if (prepared.trimmedEntryHashes) {
					if (prepared.trimmedEntryHashes.length === 0) {
						return finish();
					}
					if (
						properties.resolveTrimmedEntries === false ||
						prepared.documentTrimmedHeadsProcessed === true
					) {
						const trimmedEntryHashes =
							prepared.trimmedEntryHashes.length === 1
								? prepared.trimmedEntryHashes
								: [...new Set(prepared.trimmedEntryHashes)];
						const consumedNoReturn =
							this.entryIndex.consumeNativeTrimmedEntryHashesNoReturnMaybe(
								trimmedEntryHashes,
								{
									skipNextHeadUpdates: true,
									deleteBlocks: false,
									nativeBlocksDeleted: prepared.nativeBlocksDeleted === true,
									nativeDeleteCleanupToken: prepared.nativeDeleteCleanupToken,
									nativeCommittedAppendFactsTransaction: indexTransaction,
								},
							);
						if (consumedNoReturn !== undefined) {
							return mapMaybePromise(consumedNoReturn, () => ({
								get entry() {
									return materializeEntry();
								},
								materializeEntry,
								removed: [],
								removedHashes: trimmedEntryHashes,
								appendFacts,
								get shallowEntry() {
									return getShallowEntry();
								},
								documentTrimmedHeadsProcessed:
									prepared.documentTrimmedHeadsProcessed,
								documentPreviousContext: prepared.documentPreviousContext,
							}));
						}
					}
					const consumedResult =
						this.entryIndex.consumeNativeTrimmedEntryHashesMaybe(
							prepared.trimmedEntryHashes,
							{
								skipNextHeadUpdates: true,
								deleteBlocks: false,
								nativeBlocksDeleted: prepared.nativeBlocksDeleted === true,
								nativeDeleteCleanupToken: prepared.nativeDeleteCleanupToken,
								nativeCommittedAppendFactsTransaction: indexTransaction,
							},
						);
					return mapMaybePromise(consumedResult, (removed) => ({
						get entry() {
							return materializeEntry();
						},
						materializeEntry,
						removed,
						removedHashes: prepared.trimmedEntryHashes,
						appendFacts,
						get shallowEntry() {
							return getShallowEntry();
						},
						documentTrimmedHeadsProcessed:
							prepared.documentTrimmedHeadsProcessed,
						documentPreviousContext: prepared.documentPreviousContext,
					}));
				}
				if (!prepared.trimmedEntries) {
					return finish();
				}
				const trimmedEntries = this.entryIndex.nativeLogEntriesToShallowEntries(
					prepared.trimmedEntries,
				);
				const consumedResult = this.entryIndex.consumeNativeTrimmedEntriesMaybe(
					trimmedEntries,
					{
						skipNextHeadUpdates: true,
						deleteBlocks: false,
						nativeBlocksDeleted: prepared.nativeBlocksDeleted === true,
						nativeDeleteCleanupToken: prepared.nativeDeleteCleanupToken,
						nativeCommittedAppendFactsTransaction: indexTransaction,
					},
				);
				return mapMaybePromise(consumedResult, (removed) => ({
					get entry() {
						return materializeEntry();
					},
					materializeEntry,
					removed,
					appendFacts,
					get shallowEntry() {
						return getShallowEntry();
					},
					documentTrimmedHeadsProcessed: prepared.documentTrimmedHeadsProcessed,
					documentPreviousContext: prepared.documentPreviousContext,
				}));
			};
			const finishBlocks = ():
				| PreparedCommitOnlyAppendResult<T>
				| Promise<PreparedCommitOnlyAppendResult<T>> => {
				if (!prepared.bytes) {
					return finishTrim();
				}
				return mapMaybePromise(
					this.putPreparedAppendBlocks([
						Entry.preparedBlockFromBytes(prepared.bytes, hash),
					]),
					finishTrim,
				);
			};
			const trimmedHashes =
				prepared.trimmedEntryHashes ??
				prepared.trimmedEntries?.map((entry) => entry.hash) ??
				[];
			let finalizer: NativeCommittedAppendFinalizer | undefined;
			const rollback = async (error: unknown): Promise<never> => {
				if (finalizer) {
					try {
						await finalizer.rollback();
					} catch (rollbackError) {
						throw new AggregateError(
							[error, rollbackError],
							"Native append and its compensation both failed",
						);
					}
					throw error;
				}
				this.rollbackNativeAppendGraphHashes([hash]);
				return this.rollbackNativeAppendFactsAndBlocksHashesPreservingError(
					indexTransaction,
					[hash],
					error,
					trimmedHashes,
					prepared.nativeCommitOwnershipToken,
				);
			};
			try {
				indexTransaction =
					prepared.nativeCommitOwnershipToken !== undefined ||
					properties.deferNativeTransactionAcknowledgement === true
						? this.entryIndex.beginNativeCommittedAppendFactsTransaction(
								[hash, ...effectiveNextHashes, ...trimmedHashes],
								prepared.nativeIndexMutationLockOwner,
							)
						: undefined;
				finalizer = indexTransaction
					? this.createNativeCommittedAppendFinalizer({
							transaction: indexTransaction,
							hashes: [hash],
							restoreNativeCids: trimmedHashes,
							ownershipToken: prepared.nativeCommitOwnershipToken,
						})
					: undefined;
				const putResult = this.entryIndex.putNativeCommittedAppendFacts(
					{
						hash,
						unique: true,
						externalNextHashes: effectiveNextHashes,
						getShallowEntry,
						isHead: true,
					},
					indexTransaction,
				);
				const result = mapMaybePromise(putResult, finishBlocks);
				const acknowledged = mapMaybePromise(result, async (value) => {
					if (
						finalizer &&
						properties.deferNativeTransactionAcknowledgement === true
					) {
						value.nativeCommittedAppendFinalizer = finalizer;
						return value;
					}
					if (finalizer) {
						await finalizer.acknowledge();
					} else if (hasNativeCommitOwnershipAck(this._storage)) {
						this._storage.acknowledgeNativeCommitOwnership(
							prepared.nativeCommitOwnershipToken,
						);
					}
					return value;
				});
				return isPromiseLike(acknowledged)
					? acknowledged.catch(rollback)
					: acknowledged;
			} catch (error) {
				return rollback(error);
			}
		};
		return this.finishNativeCommittedAppend(nativePreparation, consumePrepared);
	}

	private appendLocallyPreparedNativeCommitOnly(
		data: T,
		options: AppendOptions<T> = {},
		properties: {
			payloadData?: Uint8Array;
			resolveTrimmedEntries?: boolean;
			skipMissingNextJoin?: boolean;
			knownNoNext?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			input: NativeCommitInput,
		) => MaybePromise<NativePreparedNoNextCommit | undefined>,
		knownNoNext = false,
	): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> {
		this.throwIfDurableWritesFailed();
		const resolvedTrim = options.trim ?? this._trim.options;
		const supportsNativeTrim =
			!resolvedTrim ||
			(resolvedTrim.type === "length" &&
				!resolvedTrim.filter?.canTrim &&
				properties.resolveTrimmedEntries === false);
		const nativeTrimLengthTo = this.getNativeCommitOnlyTrimLengthTo(
			options.trim,
			properties.resolveTrimmedEntries,
		);
		if (
			options.canAppend ||
			options.onChange ||
			options.encryption ||
			options.signers ||
			options.identity ||
			options.meta?.timestamp ||
			!supportsNativeTrim ||
			(this._hasCustomCanAppend && !canAppendAlreadyValidated(options))
		) {
			return undefined;
		}
		const identity = this._identity;
		if (!(identity instanceof Ed25519Keypair)) {
			return undefined;
		}
		const payloadData =
			properties.payloadData ??
			(data == null ? undefined : this._encoding.encoder(data));
		if (!payloadData || !hasPutMany(this._storage)) {
			return undefined;
		}

		const appendOptions = withCanAppendAlreadyValidated(options);
		const knownNoNextAppend = knownNoNext || properties.knownNoNext === true;
		const nextsResult = knownNoNextAppend
			? EMPTY_NEXT_ENTRIES
			: this.getNextsForAppend(appendOptions);
		return mapMaybePromise(nextsResult, (nexts) => {
			if (nexts.length > 0 && properties.skipMissingNextJoin !== true) {
				return undefined;
			}

			const nextHashes: string[] = knownNoNextAppend ? EMPTY_NEXT_HASHES : [];
			let nextGid: string | undefined;
			if (nexts.length > 0) {
				if ((appendOptions.meta as { gid?: string } | undefined)?.gid) {
					return undefined;
				}
				for (const next of nexts) {
					if (!next.hash) {
						return undefined;
					}
					nextHashes.push(next.hash);
					nextGid =
						nextGid == null || next.meta.gid < nextGid
							? next.meta.gid
							: nextGid;
				}
			}

			const gid = nextGid ?? EntryV0.createGid(appendOptions.meta?.gidSeed);
			return mapMaybePromise(gid, (resolvedGid) => {
				const clock = new Clock({
					id: identity.publicKey.bytes,
					timestamp: this._hlc.now(),
				});
				const entryType = appendOptions.meta?.type ?? EntryType.APPEND;
				const nativePreparation = this.prepareNativeCommittedAppend(() =>
					prepare({
						clockId: identity.publicKey.bytes,
						privateKey: identity.privateKey.privateKey,
						publicKey: identity.publicKey.publicKey,
						wallTime: clock.timestamp.wallTime,
						logical: clock.timestamp.logical,
						gid: resolvedGid,
						next: nextHashes,
						type: entryType,
						metaData: appendOptions.meta?.data,
						payloadData,
						resolveTrimmedEntries: properties.resolveTrimmedEntries,
						trimLengthTo: nativeTrimLengthTo,
					}),
				);
				const consumePrepared = (
					prepared: NativePreparedNoNextCommit | undefined,
				): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> => {
					if (!prepared) {
						return undefined;
					}
					const hash = prepared.cid ?? prepared.hash;
					if (!hash) {
						return undefined;
					}
					const shouldRetainMaterializationBytes =
						properties.retainMaterializationBytes === true || !!resolvedTrim;
					let retainedMaterializationBytes: Uint8Array | undefined;
					const retainMaterializationBytes = () => {
						if (
							retainedMaterializationBytes ||
							!shouldRetainMaterializationBytes ||
							prepared.bytes
						) {
							return;
						}
						const bytes =
							prepared.getBytes?.(hash) ??
							(this._storage.get(hash) as Uint8Array | undefined);
						if (
							bytes &&
							typeof (bytes as { then?: unknown }).then !== "function"
						) {
							retainedMaterializationBytes = bytes;
						}
					};
					const effectiveNextHashes = prepared.next ?? nextHashes;
					const effectiveGid = prepared.gid ?? resolvedGid;
					const shallowEntry = new ShallowEntry({
						hash,
						payloadSize: payloadData.byteLength,
						head: true,
						meta: new ShallowMeta({
							gid: effectiveGid,
							data: appendOptions.meta?.data,
							clock,
							next: effectiveNextHashes,
							type: entryType,
						}),
					});
					const appendFacts: PreparedAppendFacts = {
						hash,
						gid: effectiveGid,
						next: effectiveNextHashes,
						wallTime: clock.timestamp.wallTime,
						logical: clock.timestamp.logical,
						clockId: clock.id,
						type: entryType,
						metaData: appendOptions.meta?.data,
						payloadSize: payloadData.byteLength,
						metaBytes: prepared.metaBytes,
						hashDigestBytes: prepared.hashDigestBytes,
					};
					let materializedEntry: Entry<T> | undefined;
					const materializeEntry = () => {
						if (materializedEntry) {
							return materializedEntry;
						}
						const bytes =
							prepared.bytes ??
							retainedMaterializationBytes ??
							prepared.getBytes?.(hash) ??
							(this._storage.get(hash) as Uint8Array | undefined);
						if (
							!bytes ||
							typeof (bytes as { then?: unknown }).then === "function"
						) {
							throw new Error("Missing synchronous native append block bytes");
						}
						const entry = deserialize(bytes, Entry) as Entry<T>;
						entry.hash = hash;
						entry.size = prepared.byteLength;
						entry.createdLocally = true;
						Entry.prepareShallowEntry(entry, shallowEntry);
						entry.init({
							encoding: this._encoding,
							keychain: this._keychain,
						});
						materializedEntry = entry;
						return entry;
					};
					let indexTransaction:
						| NativeCommittedAppendFactsTransaction
						| undefined;
					const finish = (): PreparedCommitOnlyAppendResult<T> => {
						retainMaterializationBytes();
						return {
							get entry() {
								return materializeEntry();
							},
							materializeEntry,
							removed: [],
							appendFacts,
							shallowEntry,
							documentTrimmedHeadsProcessed:
								prepared.documentTrimmedHeadsProcessed,
							documentPreviousContext: prepared.documentPreviousContext,
						};
					};
					const finishBlocks = ():
						| PreparedCommitOnlyAppendResult<T>
						| Promise<PreparedCommitOnlyAppendResult<T>> => {
						if (!prepared.bytes) {
							return finishTrim();
						}
						return mapMaybePromise(
							this.putPreparedAppendBlocks([
								Entry.preparedBlockFromBytes(prepared.bytes, hash),
							]),
							finishTrim,
						);
					};
					const finishTrim = ():
						| PreparedCommitOnlyAppendResult<T>
						| Promise<PreparedCommitOnlyAppendResult<T>> => {
						retainMaterializationBytes();
						if (prepared.trimmedEntryHashes) {
							if (prepared.trimmedEntryHashes.length === 0) {
								return finish();
							}
							if (
								properties.resolveTrimmedEntries === false ||
								prepared.documentTrimmedHeadsProcessed === true
							) {
								const trimmedEntryHashes =
									prepared.trimmedEntryHashes.length === 1
										? prepared.trimmedEntryHashes
										: [...new Set(prepared.trimmedEntryHashes)];
								const consumedNoReturn =
									this.entryIndex.consumeNativeTrimmedEntryHashesNoReturnMaybe(
										trimmedEntryHashes,
										{
											skipNextHeadUpdates: true,
											deleteBlocks: false,
											nativeBlocksDeleted:
												prepared.nativeBlocksDeleted === true,
											nativeDeleteCleanupToken:
												prepared.nativeDeleteCleanupToken,
											nativeCommittedAppendFactsTransaction: indexTransaction,
										},
									);
								if (consumedNoReturn !== undefined) {
									return mapMaybePromise(consumedNoReturn, () => ({
										get entry() {
											return materializeEntry();
										},
										materializeEntry,
										removed: [],
										removedHashes: trimmedEntryHashes,
										appendFacts,
										shallowEntry,
										documentTrimmedHeadsProcessed:
											prepared.documentTrimmedHeadsProcessed,
										documentPreviousContext: prepared.documentPreviousContext,
									}));
								}
							}
							const consumedResult =
								this.entryIndex.consumeNativeTrimmedEntryHashesMaybe(
									prepared.trimmedEntryHashes,
									{
										skipNextHeadUpdates: true,
										deleteBlocks: false,
										nativeBlocksDeleted: prepared.nativeBlocksDeleted === true,
										nativeDeleteCleanupToken: prepared.nativeDeleteCleanupToken,
										nativeCommittedAppendFactsTransaction: indexTransaction,
									},
								);
							return mapMaybePromise(consumedResult, (removed) => ({
								get entry() {
									return materializeEntry();
								},
								materializeEntry,
								removed,
								appendFacts,
								shallowEntry,
								documentTrimmedHeadsProcessed:
									prepared.documentTrimmedHeadsProcessed,
								documentPreviousContext: prepared.documentPreviousContext,
							}));
						}
						if (!prepared.trimmedEntries) {
							return finish();
						}
						const trimmedEntries =
							this.entryIndex.nativeLogEntriesToShallowEntries(
								prepared.trimmedEntries,
							);
						const consumedResult =
							this.entryIndex.consumeNativeTrimmedEntriesMaybe(trimmedEntries, {
								skipNextHeadUpdates: true,
								deleteBlocks: false,
								nativeBlocksDeleted: prepared.nativeBlocksDeleted === true,
								nativeDeleteCleanupToken: prepared.nativeDeleteCleanupToken,
								nativeCommittedAppendFactsTransaction: indexTransaction,
							});
						return mapMaybePromise(consumedResult, (removed) => ({
							get entry() {
								return materializeEntry();
							},
							materializeEntry,
							removed,
							appendFacts,
							shallowEntry,
							documentTrimmedHeadsProcessed:
								prepared.documentTrimmedHeadsProcessed,
							documentPreviousContext: prepared.documentPreviousContext,
						}));
					};
					const trimmedHashes =
						prepared.trimmedEntryHashes ??
						prepared.trimmedEntries?.map((entry) => entry.hash) ??
						[];
					let finalizer: NativeCommittedAppendFinalizer | undefined;
					const rollback = async (error: unknown): Promise<never> => {
						if (finalizer) {
							try {
								await finalizer.rollback();
							} catch (rollbackError) {
								throw new AggregateError(
									[error, rollbackError],
									"Native append and its compensation both failed",
								);
							}
							throw error;
						}
						this.rollbackNativeAppendGraphHashes([hash]);
						return this.rollbackNativeAppendFactsAndBlocksHashesPreservingError(
							indexTransaction,
							[hash],
							error,
							trimmedHashes,
							prepared.nativeCommitOwnershipToken,
						);
					};
					try {
						indexTransaction =
							prepared.nativeCommitOwnershipToken !== undefined ||
							properties.deferNativeTransactionAcknowledgement === true
								? this.entryIndex.beginNativeCommittedAppendFactsTransaction(
										[hash, ...nextHashes, ...trimmedHashes],
										prepared.nativeIndexMutationLockOwner,
									)
								: undefined;
						finalizer = indexTransaction
							? this.createNativeCommittedAppendFinalizer({
									transaction: indexTransaction,
									hashes: [hash],
									restoreNativeCids: trimmedHashes,
									ownershipToken: prepared.nativeCommitOwnershipToken,
								})
							: undefined;
						const putResult = this.entryIndex.putNativeCommittedAppendFacts(
							{
								hash,
								unique: true,
								externalNextHashes: nextHashes,
								shallowEntry,
								isHead: true,
							},
							indexTransaction,
						);
						const result = mapMaybePromise(putResult, finishBlocks);
						const acknowledged = mapMaybePromise(result, async (value) => {
							if (
								finalizer &&
								properties.deferNativeTransactionAcknowledgement === true
							) {
								value.nativeCommittedAppendFinalizer = finalizer;
								return value;
							}
							if (finalizer) {
								await finalizer.acknowledge();
							} else if (hasNativeCommitOwnershipAck(this._storage)) {
								this._storage.acknowledgeNativeCommitOwnership(
									prepared.nativeCommitOwnershipToken,
								);
							}
							return value;
						});
						return isPromiseLike(acknowledged)
							? acknowledged.catch(rollback)
							: acknowledged;
					} catch (error) {
						return rollback(error);
					}
				};
				return this.finishNativeCommittedAppend(
					nativePreparation,
					consumePrepared,
				);
			});
		});
	}

	private appendLocallyPreparedCommitOnlyWithNexts(
		data: T,
		appendOptions: AppendOptions<T>,
		nexts: Sorting.SortableEntry[],
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
			payloadData?: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
		},
		nativeTrimLengthTo?: number,
	): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> {
		return this.withNativeCommittedAppendAdmission(() =>
			this.appendLocallyPreparedCommitOnlyWithNextsAdmitted(
				data,
				appendOptions,
				nexts,
				properties,
				nativeTrimLengthTo,
			),
		);
	}

	private appendLocallyPreparedCommitOnlyWithNextsAdmitted(
		data: T,
		appendOptions: AppendOptions<T>,
		nexts: Sorting.SortableEntry[],
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
			payloadData?: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
		},
		nativeTrimLengthTo?: number,
	): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> {
		const deferBlockStore = hasPutMany(this._storage);
		const nativeAppendChainResult = this.createNativePlainAppendCommitOnly(
			[data],
			appendOptions,
			nexts,
			deferBlockStore,
			properties?.payloadData ? [properties.payloadData] : undefined,
			properties?.includeMaterializationBytes,
			properties?.includeAppendFactsBytes,
			nativeTrimLengthTo,
		);
		return mapMaybePromise(nativeAppendChainResult, (nativeAppendChain) =>
			this.finishLocallyPreparedCommitOnlyAppend(
				nativeAppendChain,
				appendOptions,
				nexts,
				deferBlockStore,
				properties,
			),
		);
	}

	private finishLocallyPreparedCommitOnlyAppend(
		nativeAppendChain: PreparedAppendCommitOnlyChain<T> | undefined,
		appendOptions: AppendOptions<T>,
		nexts: Sorting.SortableEntry[],
		deferBlockStore: boolean,
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
		},
	): MaybePromise<PreparedCommitOnlyAppendResult<T> | undefined> {
		if (!nativeAppendChain) {
			return undefined;
		}

		const appendFacts = nativeAppendChain.appendFacts[0]!;
		const shallowEntry = nativeAppendChain.shallowEntries[0]!;
		let materializedEntry: Entry<T> | undefined;
		const materializeEntry = () => {
			const entry = materializedEntry ?? nativeAppendChain.materializeEntry(0);
			entry.init({ encoding: this._encoding, keychain: this._keychain });
			materializedEntry = entry;
			return entry;
		};
		const finishTrim = (): MaybePromise<PreparedCommitOnlyAppendResult<T>> => {
			if (nativeAppendChain.trimmedNativeEntryHashes) {
				const trimmedEntryHashes = [
					...new Set(nativeAppendChain.trimmedNativeEntryHashes),
				];
				if (
					properties?.resolveTrimmedEntries === false &&
					nativeAppendChain.trimmedNativeBlocksDeleted === true
				) {
					const consumedNoReturn =
						this.entryIndex.consumeNativeTrimmedEntryHashesNoReturnMaybe(
							trimmedEntryHashes,
							{
								skipNextHeadUpdates: true,
								deleteBlocks: false,
								nativeBlocksDeleted: true,
							},
						);
					if (consumedNoReturn !== undefined) {
						return mapMaybePromise(consumedNoReturn, () => ({
							get entry() {
								return materializeEntry();
							},
							materializeEntry,
							removed: [],
							removedHashes: trimmedEntryHashes,
							appendFacts,
							shallowEntry,
						}));
					}
				}
				const consumedResult =
					this.entryIndex.consumeNativeTrimmedEntryHashesMaybe(
						trimmedEntryHashes,
						{
							skipNextHeadUpdates: true,
							deleteBlocks:
								nativeAppendChain.trimmedNativeBlocksDeleted !== true,
							nativeBlocksDeleted:
								nativeAppendChain.trimmedNativeBlocksDeleted === true,
						},
					);
				return mapMaybePromise(consumedResult, (removed) => ({
					get entry() {
						return materializeEntry();
					},
					materializeEntry,
					removed,
					removedHashes: trimmedEntryHashes,
					appendFacts,
					shallowEntry,
				}));
			}
			if (nativeAppendChain.trimmedNativeEntries) {
				const trimmedEntries = this.entryIndex.nativeLogEntriesToShallowEntries(
					nativeAppendChain.trimmedNativeEntries,
				);
				const consumedResult = this.entryIndex.consumeNativeTrimmedEntriesMaybe(
					trimmedEntries,
					{
						skipNextHeadUpdates: true,
						deleteBlocks: nativeAppendChain.trimmedNativeBlocksDeleted !== true,
						nativeBlocksDeleted:
							nativeAppendChain.trimmedNativeBlocksDeleted === true,
					},
				);
				return mapMaybePromise(consumedResult, (removed) => ({
					get entry() {
						return materializeEntry();
					},
					materializeEntry,
					removed,
					appendFacts,
					shallowEntry,
				}));
			}
			const trimmedResult = this.trimIfConfigured(appendOptions.trim, {
				resolveDeletedEntries: properties?.resolveTrimmedEntries,
			});
			return mapMaybePromise(trimmedResult, (trimmed) => {
				const removed = trimmed ?? [];
				return {
					get entry() {
						return materializeEntry();
					},
					materializeEntry,
					removed,
					appendFacts,
					shallowEntry,
				};
			});
		};
		const finishFacts = (): MaybePromise<PreparedCommitOnlyAppendResult<T>> => {
			const putFactsResult = this.entryIndex.putNativeCommittedAppendFacts({
				hash: appendFacts.hash,
				unique: true,
				externalNextHashes: nexts.map((next) => next.hash),
				shallowEntry,
				isHead: true,
			});
			return mapMaybePromise(putFactsResult, finishTrim);
		};
		const finishBlocks = (): MaybePromise<
			PreparedCommitOnlyAppendResult<T>
		> => {
			if (deferBlockStore && !nativeAppendChain.nativeBlocksCommitted) {
				return mapMaybePromise(
					this.putPreparedAppendBlocks(nativeAppendChain.blocks),
					finishFacts,
				);
			}
			if (
				nativeAppendChain.nativeBlocksCommitted &&
				hasDurableWriteBarrier(this._storage)
			) {
				return mapMaybePromise(
					this._storage.waitForDurableWrites(),
					finishFacts,
				);
			}
			return finishFacts();
		};
		const rollback = (error: unknown): never | Promise<never> => {
			if (nativeAppendChain.nativeGraphUpdated) {
				this.rollbackNativeAppendGraphHashes([appendFacts.hash]);
			}
			if (nativeAppendChain.nativeBlocksCommitted) {
				return this.rollbackNativeAppendBlocksHashesPreservingError(
					[appendFacts.hash],
					error,
					nativeAppendChain.trimmedNativeEntryHashes ??
						nativeAppendChain.trimmedNativeEntries?.map((entry) => entry.hash),
				);
			}
			throw error;
		};
		try {
			let result: MaybePromise<PreparedCommitOnlyAppendResult<T>>;
			if (!properties?.skipMissingNextJoin && nexts.length > 0) {
				result = mapMaybePromise(
					this.joinMissingNexts(materializeEntry(), nexts),
					finishBlocks,
				);
			} else {
				result = finishBlocks();
			}
			return isPromiseLike(result) ? result.catch(rollback) : result;
		} catch (error) {
			return rollback(error);
		}
	}

	private async appendLocallyPreparedManyIndependent(
		data: T[],
		options: AppendOptions<T> = {},
		properties?: {
			resolveTrimmedEntries?: boolean;
			payloadDatas?: Uint8Array[];
			nexts?: Sorting.SortableEntry[][];
		},
	): Promise<
		| {
				entries: Entry<T>[];
				removed: ShallowOrFullEntry<T>[];
				change: Change<T>;
				appendFacts: PreparedAppendFacts[];
		  }
		| undefined
	> {
		this.throwIfDurableWritesFailed();
		return this.withNativeCommittedAppendAdmission(() =>
			this.appendLocallyPreparedManyIndependentAdmitted(
				data,
				options,
				properties,
			),
		);
	}

	private async appendLocallyPreparedManyIndependentAdmitted(
		data: T[],
		options: AppendOptions<T>,
		properties?: {
			resolveTrimmedEntries?: boolean;
			payloadDatas?: Uint8Array[];
			nexts?: Sorting.SortableEntry[][];
		},
	): Promise<
		| {
				entries: Entry<T>[];
				removed: ShallowOrFullEntry<T>[];
				change: Change<T>;
				appendFacts: PreparedAppendFacts[];
		  }
		| undefined
	> {
		if (data.length === 0) {
			return {
				entries: [],
				removed: [],
				change: { added: [], removed: [] },
				appendFacts: [],
			};
		}
		if (
			options.canAppend ||
			options.onChange ||
			options.meta?.type === EntryType.CUT
		) {
			throw new Error(
				"appendLocallyPreparedManyIndependent only supports trusted plain local appends",
			);
		}

		const appendOptions = withCanAppendAlreadyValidated(options);
		const deferBlockStore = hasPutMany(this._storage);
		const nativeAppendBatch = await this.createNativePlainAppendEntriesBatch(
			data,
			appendOptions,
			deferBlockStore,
			properties?.payloadDatas,
			properties?.nexts,
		);
		if (!nativeAppendBatch) {
			return undefined;
		}

		const entries = nativeAppendBatch.entries;
		const externalNextHashes =
			properties?.nexts?.flatMap((nexts) => nexts.map((next) => next.hash)) ??
			[];
		try {
			if (deferBlockStore && !nativeAppendBatch.nativeBlocksCommitted) {
				await this.putAppendEntryBlocks(entries, nativeAppendBatch.blocks);
			}
			await this.putAppendEntries(
				entries,
				appendOptions,
				externalNextHashes,
				nativeAppendBatch,
				entries.map(() => true),
			);
		} catch (error) {
			if (nativeAppendBatch.nativeGraphUpdated) {
				this.rollbackNativeAppendGraph(entries);
			}
			if (nativeAppendBatch.nativeBlocksCommitted) {
				await this.rollbackNativeAppendBlocks(entries);
			}
			throw error;
		}

		for (const entry of entries) {
			entry.init({ encoding: this._encoding, keychain: this._keychain });
		}

		const trimmed = await this.trimIfConfigured(appendOptions.trim, {
			resolveDeletedEntries: properties?.resolveTrimmedEntries,
		});
		const removed = trimmed ?? [];
		const change: Change<T> = {
			added: entries.map((entry) => ({ head: true, entry })),
			removed,
		};
		const appendFacts = this.createPreparedAppendFacts(
			entries,
			nativeAppendBatch,
		);

		return { entries, removed, change, appendFacts };
	}

	private appendLocallyPreparedNativeKnownNoNextCommitOnlyBatch(
		data: T[],
		options: AppendOptions<T> = {},
		properties: {
			payloadDatas: Uint8Array[];
			resolveTrimmedEntries?: boolean;
			allowPreparedNexts?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			inputs: NativeNoNextCommitInput[],
		) => MaybePromise<
			Array<NativePreparedNoNextCommit | undefined> | undefined
		>,
	): MaybePromise<PreparedCommitOnlyAppendBatchResult<T> | undefined> {
		this.throwIfDurableWritesFailed();
		if (data.length === 0) {
			return {
				entries: [],
				materializeEntries: [],
				removed: [],
				appendFacts: [],
			};
		}
		if (data.length !== properties.payloadDatas.length) {
			throw new Error("Mismatched native batch payload count");
		}
		const resolvedTrim = options.trim ?? this._trim.options;
		const supportsNativeTrim =
			!resolvedTrim ||
			(resolvedTrim.type === "length" &&
				!resolvedTrim.filter?.canTrim &&
				properties.resolveTrimmedEntries === false);
		if (
			options.canAppend ||
			options.onChange ||
			options.encryption ||
			options.signers ||
			options.identity ||
			options.meta?.timestamp ||
			options.meta?.type === EntryType.CUT ||
			(options.meta?.next != null && options.meta.next.length !== 0) ||
			options.meta?.gidSeed ||
			!supportsNativeTrim ||
			(this._hasCustomCanAppend && !canAppendAlreadyValidated(options))
		) {
			return undefined;
		}
		const identity = this._identity;
		if (!(identity instanceof Ed25519Keypair) || !hasPutMany(this._storage)) {
			return undefined;
		}
		const nativeTrimLengthTo = this.getNativeCommitOnlyTrimLengthTo(
			options.trim,
			properties.resolveTrimmedEntries,
		);
		const entryType = options.meta?.type ?? EntryType.APPEND;
		const rows = properties.payloadDatas.map((payloadData) => {
			const gid = EntryV0.createGid() as string;
			const timestamp = this._hlc.now();
			return {
				gid,
				timestamp,
				payloadData,
				input: {
					clockId: identity.publicKey.bytes,
					privateKey: identity.privateKey.privateKey,
					publicKey: identity.publicKey.publicKey,
					wallTime: timestamp.wallTime,
					logical: timestamp.logical,
					gid,
					type: entryType,
					metaData: options.meta?.data,
					payloadData,
					resolveTrimmedEntries: properties.resolveTrimmedEntries,
					trimLengthTo: nativeTrimLengthTo,
				} satisfies NativeNoNextCommitInput,
			};
		});
		const nativePreparation = this.prepareNativeCommittedAppend(() =>
			prepare(rows.map((row) => row.input)),
		);
		const consumePrepared = (
			preparedRows: Array<NativePreparedNoNextCommit | undefined> | undefined,
		): MaybePromise<PreparedCommitOnlyAppendBatchResult<T> | undefined> => {
			if (!preparedRows || preparedRows.length !== rows.length) {
				return undefined;
			}
			const appendFacts: PreparedAppendFacts[] = [];
			const materializeEntries: Array<() => Entry<T>> = [];
			const retainMaterializationBytesFns: Array<() => void> = [];
			const indexRows: Parameters<
				EntryIndex<T>["putNativeCommittedAppendFactsBatch"]
			>[0] = [];
			const trimmedEntryHashes: string[] = [];
			let trimmedNativeBlocksDeleted = true;
			let nativeDeleteCleanupToken: unknown;
			let nativeCommitOwnershipToken: unknown;
			let nativeIndexMutationLockOwner:
				| EntryIndexHashMutationLockOwner
				| undefined;
			const documentTrimmedHeadsProcessed: boolean[] = [];
			for (let index = 0; index < rows.length; index++) {
				const row = rows[index]!;
				const prepared = preparedRows[index];
				if (!prepared) {
					return undefined;
				}
				const hash = prepared.cid ?? prepared.hash;
				if (!hash) {
					return undefined;
				}
				const shouldRetainMaterializationBytes =
					properties.retainMaterializationBytes === true || !!resolvedTrim;
				let retainedMaterializationBytes: Uint8Array | undefined;
				const retainMaterializationBytes = () => {
					if (
						retainedMaterializationBytes ||
						!shouldRetainMaterializationBytes ||
						prepared.bytes
					) {
						return;
					}
					const bytes =
						prepared.getBytes?.(hash) ??
						(this._storage.get(hash) as Uint8Array | undefined);
					if (
						bytes &&
						typeof (bytes as { then?: unknown }).then !== "function"
					) {
						retainedMaterializationBytes = bytes;
					}
				};
				const effectiveNextHashes = prepared.next ?? EMPTY_NEXT_HASHES;
				if (
					effectiveNextHashes.length !== 0 &&
					properties.allowPreparedNexts !== true
				) {
					return undefined;
				}
				const effectiveGid = prepared.gid ?? row.gid;
				let clock: Clock | undefined;
				const getClock = () =>
					(clock ??= new Clock({
						id: identity.publicKey.bytes,
						timestamp: row.timestamp,
					}));
				let shallowEntry: ShallowEntry | undefined;
				const getShallowEntry = () =>
					(shallowEntry ??= new ShallowEntry({
						hash,
						payloadSize: row.payloadData.byteLength,
						head: true,
						meta: new ShallowMeta({
							gid: effectiveGid,
							data: options.meta?.data,
							clock: getClock(),
							next: effectiveNextHashes,
							type: entryType,
						}),
					}));
				const facts: PreparedAppendFacts = {
					hash,
					gid: effectiveGid,
					next: effectiveNextHashes,
					wallTime: row.timestamp.wallTime,
					logical: row.timestamp.logical,
					clockId: identity.publicKey.bytes,
					type: entryType,
					metaData: options.meta?.data,
					payloadSize: row.payloadData.byteLength,
					metaBytes: prepared.metaBytes,
					hashDigestBytes: prepared.hashDigestBytes,
				};
				let materializedEntry: Entry<T> | undefined;
				const materializeEntry = () => {
					if (materializedEntry) {
						return materializedEntry;
					}
					const bytes =
						prepared.bytes ??
						retainedMaterializationBytes ??
						prepared.getBytes?.(hash) ??
						(this._storage.get(hash) as Uint8Array | undefined);
					if (
						!bytes ||
						typeof (bytes as { then?: unknown }).then === "function"
					) {
						throw new Error("Missing synchronous native append block bytes");
					}
					const entry = deserialize(bytes, Entry) as Entry<T>;
					entry.hash = hash;
					entry.size = prepared.byteLength;
					entry.createdLocally = true;
					Entry.prepareShallowEntry(entry, getShallowEntry());
					entry.init({ encoding: this._encoding, keychain: this._keychain });
					materializedEntry = entry;
					return entry;
				};
				appendFacts.push(facts);
				materializeEntries.push(materializeEntry);
				retainMaterializationBytesFns.push(retainMaterializationBytes);
				indexRows.push({
					hash,
					unique: true,
					externalNextHashes: effectiveNextHashes,
					getShallowEntry,
					isHead: true,
				});
				nativeCommitOwnershipToken ??= prepared.nativeCommitOwnershipToken;
				nativeIndexMutationLockOwner ??= prepared.nativeIndexMutationLockOwner;
				if (prepared.trimmedEntryHashes?.length) {
					trimmedEntryHashes.push(...prepared.trimmedEntryHashes);
					trimmedNativeBlocksDeleted &&= prepared.nativeBlocksDeleted === true;
					nativeDeleteCleanupToken ??= prepared.nativeDeleteCleanupToken;
				}
				documentTrimmedHeadsProcessed.push(
					prepared.documentTrimmedHeadsProcessed === true,
				);
			}
			const appendHashes = appendFacts.map((facts) => facts.hash);
			let indexTransaction: NativeCommittedAppendFactsTransaction | undefined;
			let finalizer: NativeCommittedAppendFinalizer | undefined;
			const rollback = async (error: unknown): Promise<never> => {
				if (finalizer) {
					try {
						await finalizer.rollback();
					} catch (rollbackError) {
						throw new AggregateError(
							[error, rollbackError],
							"Native append batch and its compensation both failed",
						);
					}
					throw error;
				}
				this.rollbackNativeAppendGraphHashes(appendHashes);
				return this.rollbackNativeAppendFactsAndBlocksHashesPreservingError(
					indexTransaction,
					appendHashes,
					error,
					trimmedEntryHashes,
					nativeCommitOwnershipToken,
				);
			};
			const finish = (): PreparedCommitOnlyAppendBatchResult<T> => {
				for (const retainMaterializationBytes of retainMaterializationBytesFns) {
					retainMaterializationBytes();
				}
				let entries: Entry<T>[] | undefined;
				return {
					get entries() {
						return (entries ??= materializeEntries.map((materializeEntry) =>
							materializeEntry(),
						));
					},
					materializeEntries,
					removed: [],
					removedHashes:
						trimmedEntryHashes.length > 0
							? normalizedUniqueStrings(trimmedEntryHashes)
							: undefined,
					appendFacts,
					documentTrimmedHeadsProcessed,
				};
			};
			const finishTrim = ():
				| PreparedCommitOnlyAppendBatchResult<T>
				| Promise<PreparedCommitOnlyAppendBatchResult<T>>
				| undefined => {
				if (trimmedEntryHashes.length === 0) {
					return finish();
				}
				if (
					properties.resolveTrimmedEntries !== false &&
					!documentTrimmedHeadsProcessed.every(Boolean)
				) {
					return undefined;
				}
				const uniqueTrimmedHashes = normalizedUniqueStrings(trimmedEntryHashes);
				const consumedNoReturn =
					this.entryIndex.consumeNativeTrimmedEntryHashesNoReturnMaybe(
						uniqueTrimmedHashes,
						{
							skipNextHeadUpdates: true,
							deleteBlocks: false,
							nativeBlocksDeleted: trimmedNativeBlocksDeleted,
							nativeDeleteCleanupToken,
							nativeCommittedAppendFactsTransaction: indexTransaction,
						},
					);
				if (consumedNoReturn === undefined) {
					return undefined;
				}
				return mapMaybePromise(consumedNoReturn, finish);
			};
			try {
				indexTransaction =
					nativeCommitOwnershipToken !== undefined ||
					properties.deferNativeTransactionAcknowledgement === true
						? this.entryIndex.beginNativeCommittedAppendFactsTransaction(
								[
									...appendHashes,
									...indexRows.flatMap((row) => row.externalNextHashes),
									...trimmedEntryHashes,
								],
								nativeIndexMutationLockOwner,
							)
						: undefined;
				finalizer = indexTransaction
					? this.createNativeCommittedAppendFinalizer({
							transaction: indexTransaction,
							hashes: appendHashes,
							restoreNativeCids: trimmedEntryHashes,
							ownershipToken: nativeCommitOwnershipToken,
						})
					: undefined;
				const putResult = this.entryIndex.putNativeCommittedAppendFactsBatch(
					indexRows,
					indexTransaction,
				);
				const result = mapMaybePromise(putResult, finishTrim);
				const acknowledged = mapMaybePromise(result, async (value) => {
					if (!value) {
						await finalizer?.rollback();
						return value;
					}
					if (
						finalizer &&
						properties.deferNativeTransactionAcknowledgement === true
					) {
						value.nativeCommittedAppendFinalizer = finalizer;
						return value;
					}
					if (finalizer) {
						await finalizer.acknowledge();
					} else if (hasNativeCommitOwnershipAck(this._storage)) {
						this._storage.acknowledgeNativeCommitOwnership(
							nativeCommitOwnershipToken,
						);
					}
					return value;
				});
				return isPromiseLike(acknowledged)
					? acknowledged.catch(rollback)
					: acknowledged;
			} catch (error) {
				return rollback(error);
			}
		};
		return this.finishNativeCommittedAppend(nativePreparation, consumePrepared);
	}

	private createPreparedAppendFacts(
		entries: Entry<T>[],
		prepared?: PreparedAppendChain<T>,
	): PreparedAppendFacts[] {
		if (prepared?.appendFacts?.length === entries.length) {
			return prepared.appendFacts;
		}
		return entries.map((entry, index) => {
			const shallowEntry = prepared?.shallowEntries[index];
			if (shallowEntry) {
				return {
					hash: shallowEntry.hash,
					gid: shallowEntry.meta.gid,
					next: shallowEntry.meta.next,
					wallTime: shallowEntry.meta.clock.timestamp.wallTime,
					logical: shallowEntry.meta.clock.timestamp.logical,
					clockId: shallowEntry.meta.clock.id,
					type: shallowEntry.meta.type,
					metaData: shallowEntry.meta.data,
					payloadSize: shallowEntry.payloadSize,
					metaBytes: (entry as EntryWithMetaBytes).getMetaBytes?.(),
					hashDigestBytes: (entry as EntryWithMetaBytes).getHashDigestBytes?.(),
				};
			}
			return {
				hash: entry.hash,
				gid: entry.meta.gid,
				next: entry.meta.next,
				wallTime: entry.meta.clock.timestamp.wallTime,
				logical: entry.meta.clock.timestamp.logical,
				clockId: entry.meta.clock.id,
				type: entry.meta.type,
				metaData: entry.meta.data,
				payloadSize: entry.payload.byteLength,
				metaBytes: (entry as EntryWithMetaBytes).getMetaBytes?.(),
				hashDigestBytes: (entry as EntryWithMetaBytes).getHashDigestBytes?.(),
			};
		});
	}

	async appendMany(
		data: T[],
		options: AppendOptions<T> = {},
	): Promise<{ entries: Entry<T>[]; removed: ShallowOrFullEntry<T>[] }> {
		this.throwIfDurableWritesFailed();
		return this.withNativeCommittedAppendAdmission(() =>
			this.appendManyAdmitted(data, options),
		);
	}

	private async appendManyAdmitted(
		data: T[],
		options: AppendOptions<T>,
	): Promise<{ entries: Entry<T>[]; removed: ShallowOrFullEntry<T>[] }> {
		if (data.length === 0) {
			return { entries: [], removed: [] };
		}
		if (options.meta?.type === EntryType.CUT) {
			throw new Error("appendMany does not support CUT entries");
		}

		const initialNexts = await this.getNextsForAppend(options);
		const deferBlockStore = hasPutMany(this._storage);
		type MutationResult = {
			entries: Entry<T>[];
			removed: ShallowOrFullEntry<T>[];
			changes: Change<T>;
		};
		const finishMutation = async (
			entries: Entry<T>[],
		): Promise<MutationResult> => {
			for (const entry of entries) {
				entry.init({ encoding: this._encoding, keychain: this._keychain });
			}
			const removed =
				(await this.trimIfConfigured(options.trim))?.map((entry) => entry) ??
				[];
			return {
				entries,
				removed,
				changes: {
					added: entries.map((entry, index) => ({
						head: index === entries.length - 1,
						entry,
					})),
					removed,
				},
			};
		};

		let mutation: MutationResult | undefined;
		const nativeAppendChain = await this.createNativePlainAppendChain(
			data,
			options,
			initialNexts,
			deferBlockStore,
		);
		if (nativeAppendChain) {
			const entries = nativeAppendChain.entries;
			try {
				await this.joinMissingNexts(entries[0]!, initialNexts);
				if (deferBlockStore && !nativeAppendChain.nativeBlocksCommitted) {
					await this.putAppendEntryBlocks(entries, nativeAppendChain.blocks);
				}
				await this.putAppendEntries(
					entries,
					options,
					initialNexts.map((entry) => entry.hash),
					nativeAppendChain,
				);
			} catch (error) {
				if (nativeAppendChain.nativeGraphUpdated) {
					this.rollbackNativeAppendGraph(entries);
				}
				if (nativeAppendChain.nativeBlocksCommitted) {
					await this.rollbackNativeAppendBlocks(entries);
				}
				throw error;
			}
			mutation = await finishMutation(entries);
		}

		if (!mutation) {
			const entries: Entry<T>[] = [];
			let nexts = initialNexts;
			for (const item of data) {
				const entry = await this.createAppendEntry(item, options, nexts, {
					deferStore: deferBlockStore,
				});
				entries.push(entry);
				nexts = [entry];
			}
			await this.joinMissingNexts(entries[0]!, initialNexts);
			if (deferBlockStore) {
				await this.putAppendEntryBlocks(entries);
			}
			await this.putAppendEntries(
				entries,
				options,
				initialNexts.map((entry) => entry.hash),
			);
			mutation = await finishMutation(entries);
		}

		if (options.onChange) {
			await this.runWithMutationCallback(() =>
				options.onChange!(mutation.changes),
			);
		} else {
			await this._onChange?.(mutation.changes);
		}
		return { entries: mutation.entries, removed: mutation.removed };
	}

	private createNativePlainAppendChain(
		data: T[],
		options: AppendOptions<T>,
		nexts: Sorting.SortableEntry[],
		deferBlockStore: boolean,
		payloadDatas?: Uint8Array[],
	): Promise<PreparedAppendChain<T> | undefined> {
		const canAppendAlreadyValidatedForOptions =
			canAppendAlreadyValidated(options);
		if (
			!deferBlockStore ||
			options.encryption ||
			options.signers ||
			options.canAppend ||
			(this._hasCustomCanAppend && !canAppendAlreadyValidatedForOptions) ||
			options.meta?.timestamp ||
			options.meta?.type === EntryType.CUT
		) {
			return Promise.resolve(undefined);
		}

		const nativeGraph =
			!this.entryIndex.properties.onGidRemoved &&
			(this.entryIndex.properties.nativeGraph?.graph
				.prepareEntryV0PlainChainCommit ||
				this.entryIndex.properties.nativeGraph?.graph
					.prepareEntryV0PlainEntryCommit ||
				this.entryIndex.properties.nativeGraph?.graph
					.prepareEntryV0PlainChainAndPut ||
				this.entryIndex.properties.nativeGraph?.graph
					.prepareEntryV0PlainEntryAndPut)
				? this.entryIndex.properties.nativeGraph.graph
				: undefined;
		return EntryV0.createPlainAppendChainBatch<T>({
			data,
			meta: {
				clocks: () =>
					data.map(
						() =>
							new Clock({
								id: this._identity.publicKey.bytes,
								timestamp: this._hlc.now(),
							}),
					),
				type: options.meta?.type,
				gidSeed: options.meta?.gidSeed,
				data: options.meta?.data,
				next: nexts,
			},
			encoding: this._encoding,
			payloadDatas,
			identity: options.identity || this._identity,
			deferStore: deferBlockStore,
			cachePreparedEntries: false,
			nativeGraph,
			nativeBlockStore: this._storage,
		});
	}

	private createNativePlainAppendCommitOnly(
		data: T[],
		options: AppendOptions<T>,
		nexts: Sorting.SortableEntry[],
		deferBlockStore: boolean,
		payloadDatas?: Uint8Array[],
		includeMaterializationBytes?: boolean,
		includeAppendFactsBytes?: boolean,
		nativeTrimLengthTo?: number,
	): MaybePromise<PreparedAppendCommitOnlyChain<T> | undefined> {
		const canAppendAlreadyValidatedForOptions =
			canAppendAlreadyValidated(options);
		if (
			data.length !== 1 ||
			!deferBlockStore ||
			options.encryption ||
			options.signers ||
			options.canAppend ||
			(this._hasCustomCanAppend && !canAppendAlreadyValidatedForOptions) ||
			options.meta?.timestamp ||
			options.meta?.type === EntryType.CUT
		) {
			return undefined;
		}

		const nativeGraph =
			!this.entryIndex.properties.onGidRemoved &&
			(this.entryIndex.properties.nativeGraph?.graph
				.prepareEntryV0PlainEntryCommit ||
				this.entryIndex.properties.nativeGraph?.graph
					.prepareEntryV0PlainEntryAndPut)
				? this.entryIndex.properties.nativeGraph.graph
				: undefined;
		if (!nativeGraph) {
			return undefined;
		}
		return EntryV0.createPlainAppendChainCommitOnly<T>({
			data,
			meta: {
				clocks: () => [
					new Clock({
						id: this._identity.publicKey.bytes,
						timestamp: this._hlc.now(),
					}),
				],
				type: options.meta?.type,
				gidSeed: options.meta?.gidSeed,
				data: options.meta?.data,
				next: nexts,
			},
			encoding: this._encoding,
			payloadDatas,
			identity: options.identity || this._identity,
			deferStore: deferBlockStore,
			nativeGraph,
			nativeBlockStore: this._storage,
			includeMaterializationBytes,
			includeAppendFactsBytes,
			nativeTrimLengthTo,
		});
	}

	private async createNativePlainAppendEntriesBatch(
		data: T[],
		options: AppendOptions<T>,
		deferBlockStore: boolean,
		payloadDatas?: Uint8Array[],
		nexts?: Sorting.SortableEntry[][],
	): Promise<PreparedAppendChain<T> | undefined> {
		const canAppendAlreadyValidatedForOptions =
			canAppendAlreadyValidated(options);
		if (
			!deferBlockStore ||
			data.length === 0 ||
			options.encryption ||
			options.signers ||
			options.canAppend ||
			(this._hasCustomCanAppend && !canAppendAlreadyValidatedForOptions) ||
			options.meta?.timestamp ||
			options.meta?.type === EntryType.CUT ||
			options.meta?.gidSeed ||
			options.meta?.next ||
			(nexts && nexts.length !== data.length) ||
			(payloadDatas && payloadDatas.length !== data.length)
		) {
			return undefined;
		}

		const nativeGraph =
			!this.entryIndex.properties.onGidRemoved &&
			this.entryIndex.properties.nativeGraph?.graph
				? this.entryIndex.properties.nativeGraph.graph
				: undefined;

		const generatedGids = EntryV0.createGids(data.length);
		const gids = generatedGids.map((generatedGid, index) => {
			const entryNexts = nexts?.[index];
			if (!entryNexts || entryNexts.length === 0) {
				return generatedGid;
			}
			let gid = entryNexts[0]!.meta.gid;
			for (let i = 1; i < entryNexts.length; i++) {
				const nextGid = entryNexts[i]!.meta.gid;
				if (nextGid < gid) {
					gid = nextGid;
				}
			}
			return gid;
		});
		const clockId = this._identity.publicKey.bytes;
		const clocks = this._hlc.nowBatch(data.length).map(
			(timestamp) =>
				new Clock({
					id: clockId,
					timestamp,
				}),
		);
		const metaDatas = data.map(() => options.meta?.data);
		const directBatch = nativeGraph?.prepareEntryV0PlainEntriesCommit
			? await EntryV0.createPlainAppendEntriesBatch<T>({
					data,
					payloadDatas,
					meta: {
						clocks: () => clocks,
						gids,
						nexts,
						type: options.meta?.type,
						datas: metaDatas,
					},
					encoding: this._encoding,
					identity: options.identity || this._identity,
					deferStore: deferBlockStore,
					cachePreparedEntries: false,
					nativeGraph,
					nativeBlockStore: this._storage,
				})
			: undefined;
		if (directBatch) {
			return directBatch;
		}

		const entries: Entry<T>[] = [];
		const blocks: PreparedEntryBlock[] = [];
		const shallowEntries: PreparedAppendChain<T>["shallowEntries"] = [];
		const nativeEntries: NonNullable<PreparedAppendChain<T>["nativeEntries"]> =
			[];
		let nativeGraphUpdated = false;
		let nativeBlocksCommitted = true;
		for (let i = 0; i < data.length; i++) {
			const entryNexts = nexts?.[i] ?? [];
			const prepared = await EntryV0.createPlainAppendChainBatch<T>({
				data: [data[i]!],
				payloadDatas: payloadDatas ? [payloadDatas[i]!] : undefined,
				meta: {
					clocks: () => [clocks[i]!],
					gid: entryNexts.length === 0 ? gids[i]! : undefined,
					type: options.meta?.type,
					data: metaDatas[i],
					next: entryNexts,
				},
				encoding: this._encoding,
				identity: options.identity || this._identity,
				deferStore: deferBlockStore,
				cachePreparedEntries: false,
				nativeGraph,
				nativeBlockStore: this._storage,
			});
			if (!prepared) {
				return undefined;
			}
			entries.push(prepared.entries[0]!);
			if (prepared.blocks) {
				blocks.push(...prepared.blocks);
			}
			shallowEntries.push(...prepared.shallowEntries);
			if (prepared.nativeEntries) {
				nativeEntries.push(...prepared.nativeEntries);
			}
			nativeGraphUpdated ||= prepared.nativeGraphUpdated === true;
			nativeBlocksCommitted &&= prepared.nativeBlocksCommitted === true;
		}

		if (!nativeBlocksCommitted && blocks.length !== entries.length) {
			return undefined;
		}
		return {
			entries,
			blocks: blocks.length > 0 ? blocks : undefined,
			shallowEntries,
			nativeEntries,
			nativeGraphUpdated,
			nativeBlocksCommitted,
		};
	}

	private rollbackNativeAppendGraph(entries: Entry<T>[]) {
		this.rollbackNativeAppendGraphHashes(entries.map((entry) => entry.hash));
	}

	private rollbackNativeAppendGraphHashes(hashes: string[]) {
		const graph = this.entryIndex.properties.nativeGraph?.graph;
		if (!graph) {
			return;
		}
		for (let i = hashes.length - 1; i >= 0; i--) {
			graph.delete(hashes[i]!);
		}
	}

	private async rollbackNativeAppendBlocks(entries: Entry<T>[]) {
		await this.rollbackNativeAppendBlocksHashes(
			entries.map((entry) => entry.hash),
		);
	}

	private async rollbackNativeAppendBlocksHashes(
		hashes: string[],
		restoreNativeCids: string[] = [],
		ownershipToken?: unknown,
	) {
		if (hasFailedNativeRollback(this._storage)) {
			await this._storage.rollbackFailedNativeCommits(
				hashes,
				restoreNativeCids,
				ownershipToken,
			);
			return;
		}
		const storage = this._storage as BlocksWithPutMany;
		if (typeof storage.rmMany === "function") {
			await storage.rmMany(hashes);
			return;
		}
		await Promise.all(hashes.map((hash) => this._storage.rm(hash)));
	}

	/** Reserve an in-flight native mutation before its prepare callback can commit
	 * blocks. Terminal teardown closes admission synchronously and waits for every
	 * earlier reservation to either finish or compensate. */
	private beginNativeCommittedAppendAdmission(): NativeCommittedAppendAdmission {
		if (this._terminalAdmissionClosed || this._lifecycleState !== "active") {
			throw new Error(
				"Native append transaction cannot start while the log is closing or dropped",
			);
		}
		let resolveSettled!: () => void;
		const settled = new Promise<void>((resolve) => {
			resolveSettled = resolve;
		});
		(this._nativeCommittedAppendAdmissions ??= new Set()).add(settled);
		let released = false;
		return {
			settled,
			release: () => {
				if (released) {
					return;
				}
				released = true;
				this._nativeCommittedAppendAdmissions?.delete(settled);
				resolveSettled();
			},
		};
	}

	private prepareNativeCommittedAppend<TValue>(
		prepare: () => MaybePromise<TValue>,
	): {
		admission: NativeCommittedAppendAdmission;
		prepared: MaybePromise<TValue>;
	} {
		const admission = this.beginNativeCommittedAppendAdmission();
		let prepared: MaybePromise<TValue>;
		try {
			prepared = prepare();
		} catch (error) {
			admission.release();
			throw error;
		}
		return { admission, prepared };
	}

	private finishNativeCommittedAppend<TValue, TResult>(
		preparation: {
			admission: NativeCommittedAppendAdmission;
			prepared: MaybePromise<TValue>;
		},
		operation: (value: TValue) => MaybePromise<TResult>,
	): MaybePromise<TResult> {
		let result: MaybePromise<TResult>;
		try {
			result = mapMaybePromise(preparation.prepared, operation);
		} catch (error) {
			preparation.admission.release();
			throw error;
		}
		if (isPromiseLike(result)) {
			return result.finally(preparation.admission.release);
		}
		preparation.admission.release();
		return result;
	}

	private withNativeCommittedAppendAdmission<TResult>(
		operation: () => Promise<TResult>,
	): Promise<TResult>;
	private withNativeCommittedAppendAdmission<TResult>(
		operation: () => MaybePromise<TResult>,
	): MaybePromise<TResult>;
	private withNativeCommittedAppendAdmission<TResult>(
		operation: () => MaybePromise<TResult>,
	): MaybePromise<TResult> {
		const preparation = this.prepareNativeCommittedAppend(operation);
		return this.finishNativeCommittedAppend(preparation, (result) => result);
	}

	private runWithMutationCallback<TResult>(
		callback: () => MaybePromise<TResult>,
	): MaybePromise<TResult> {
		this._mutationCallbacksInFlight++;
		let result: MaybePromise<TResult>;
		try {
			result = callback();
		} catch (error) {
			this._mutationCallbacksInFlight--;
			throw error;
		}
		if (isPromiseLike(result)) {
			return result.finally(() => {
				this._mutationCallbacksInFlight--;
			});
		}
		this._mutationCallbacksInFlight--;
		return result;
	}

	private wrapMutationCallback<TCallback extends (...args: any[]) => any>(
		callback: TCallback,
	): TCallback {
		const existing = this._mutationCallbackWrappers.get(callback);
		if (existing) {
			return existing as TCallback;
		}
		const wrapped = ((...args: Parameters<TCallback>) =>
			this.runWithMutationCallback(() => callback(...args))) as TCallback;
		this._mutationCallbackWrappers.set(callback, wrapped);
		return wrapped;
	}

	private wrapTrimCallbacks(option?: TrimOptions): TrimOptions | undefined {
		if (!option?.filter?.canTrim) {
			return option;
		}
		return {
			...option,
			filter: {
				...option.filter,
				canTrim: this.wrapMutationCallback(option.filter.canTrim),
			},
		};
	}

	private async settleNativeCommittedAppendAdmissions(): Promise<void> {
		while ((this._nativeCommittedAppendAdmissions?.size ?? 0) > 0) {
			await Promise.all([...this._nativeCommittedAppendAdmissions!]);
		}
	}

	private createNativeCommittedAppendFinalizer(properties: {
		transaction: NativeCommittedAppendFactsTransaction;
		hashes: string[];
		restoreNativeCids?: string[];
		ownershipToken?: unknown;
	}): NativeCommittedAppendFinalizer {
		if (this._terminalAdmissionClosed || this._lifecycleState !== "active") {
			throw new Error(
				"Native append transaction cannot start while the log is closing or dropped",
			);
		}
		let state:
			| "open"
			| "acknowledging"
			| "acknowledged"
			| "rolling-back"
			| "rollback-required"
			| "rolled-back" = "open";
		let terminalSettlementRequested = false;
		let acknowledgePromise: Promise<void> | undefined;
		let rollbackPromise: Promise<void> | undefined;
		const finalizer: NativeCommittedAppendFinalizer = {
			acknowledge: async (onLowerMarkerDurable) => {
				if (state === "acknowledged") {
					return;
				}
				if (
					state === "rolled-back" ||
					state === "rolling-back" ||
					state === "rollback-required"
				) {
					throw new Error("Native append transaction was already rolled back");
				}
				if (acknowledgePromise) {
					return acknowledgePromise;
				}
				state = "acknowledging";
				acknowledgePromise = (async () => {
					await this.entryIndex.flushNativeCommittedExternalNextFacts(
						properties.transaction,
					);
					await this.entryIndex.flushNativeCommittedAppendFacts(
						properties.transaction,
					);
					await onLowerMarkerDurable?.();
					await this.entryIndex.flushNativeCommittedTrimFacts(
						properties.transaction,
					);
					if (hasNativeCommitOwnershipAck(this._storage)) {
						this._storage.acknowledgeNativeCommitOwnership(
							properties.ownershipToken,
						);
					}
					this.entryIndex.acknowledgeNativeCommittedAppendFacts(
						properties.transaction,
					);
					state = "acknowledged";
					this._nativeCommittedAppendFinalizers?.delete(finalizer);
				})();
				try {
					await acknowledgePromise;
				} catch (error) {
					state = terminalSettlementRequested ? "rollback-required" : "open";
					acknowledgePromise = undefined;
					throw error;
				}
			},
			retainForRecovery: () => {
				if (state === "acknowledged") {
					return;
				}
				if (state !== "open" || terminalSettlementRequested) {
					throw new Error(
						"Native append transaction cannot be retained for recovery",
					);
				}
				const failures: unknown[] = [];
				try {
					if (hasNativeCommitOwnershipAck(this._storage)) {
						this._storage.acknowledgeNativeCommitOwnership(
							properties.ownershipToken,
						);
					}
				} catch (error) {
					failures.push(error);
				}
				try {
					// The durable strict intent is now the recovery authority. Finalize
					// in-memory ownership and locks so close cannot roll back a lower
					// marker that may still be durably true; reopen finishes any trim debt.
					this.entryIndex.acknowledgeNativeCommittedAppendFacts(
						properties.transaction,
					);
				} catch (error) {
					failures.push(error);
				}
				state = "acknowledged";
				this._nativeCommittedAppendFinalizers?.delete(finalizer);
				if (failures.length > 0) {
					throw new AggregateError(
						failures,
						"Failed to retain a native append transaction for recovery",
					);
				}
			},
			rollback: async () => {
				if (state === "rolled-back") {
					return;
				}
				if (state === "acknowledged" || state === "acknowledging") {
					throw new Error("Native append transaction was already acknowledged");
				}
				if (rollbackPromise) {
					return rollbackPromise;
				}
				// Compensation direction is irrevocable once any rollback begins. A
				// partial failure may retry rollback, but can never switch to publish.
				terminalSettlementRequested = true;
				state = "rolling-back";
				rollbackPromise = (async () => {
					const failures: unknown[] = [];
					try {
						this.rollbackNativeAppendGraphHashes(properties.hashes);
					} catch (error) {
						failures.push(error);
					}
					try {
						await this.entryIndex.rollbackNativeCommittedAppendFacts(
							properties.transaction,
						);
					} catch (error) {
						failures.push(error);
					}
					try {
						await this.rollbackNativeAppendBlocksHashes(
							properties.hashes,
							properties.restoreNativeCids,
							properties.ownershipToken,
						);
					} catch (error) {
						failures.push(error);
					}
					try {
						await this.entryIndex.restoreNativeGraphFromIndex();
					} catch (error) {
						failures.push(error);
					}
					if (failures.length > 0) {
						throw new AggregateError(
							failures,
							"Failed to compensate a native append transaction",
						);
					}
					state = "rolled-back";
					this._nativeCommittedAppendFinalizers?.delete(finalizer);
				})();
				try {
					await rollbackPromise;
				} catch (error) {
					state = "rollback-required";
					rollbackPromise = undefined;
					throw error;
				}
			},
			settleForTerminal: () => {
				terminalSettlementRequested = true;
				if (state === "acknowledged" || state === "rolled-back") {
					return Promise.resolve();
				}
				if (state === "acknowledging" && acknowledgePromise) {
					return acknowledgePromise;
				}
				if (state === "rolling-back" && rollbackPromise) {
					return rollbackPromise;
				}
				return finalizer.rollback();
			},
		};
		(this._nativeCommittedAppendFinalizers ??= new Set()).add(finalizer);
		return finalizer;
	}

	private validateExplicitNexts(options: AppendOptions<T>) {
		if (!options.meta?.next) {
			return;
		}
		for (const n of options.meta.next) {
			if (!n.hash) {
				throw new Error(
					"Expecting nexts to already be saved. missing hash for one or more entries",
				);
			}
		}
	}

	private async rollbackNativeAppendBlocksHashesPreservingError(
		hashes: string[],
		error: unknown,
		restoreNativeCids: string[] = [],
		ownershipToken?: unknown,
	): Promise<never> {
		try {
			await this.rollbackNativeAppendBlocksHashes(
				hashes,
				restoreNativeCids,
				ownershipToken,
			);
		} catch (rollbackError) {
			throw new AggregateError(
				[error, rollbackError],
				"Native append and block compensation both failed",
			);
		}
		throw error;
	}

	private async rollbackNativeAppendFactsAndBlocksHashesPreservingError(
		transaction: NativeCommittedAppendFactsTransaction | undefined,
		hashes: string[],
		error: unknown,
		restoreNativeCids: string[] = [],
		ownershipToken?: unknown,
	): Promise<never> {
		const rollbackFailures: unknown[] = [];
		if (transaction) {
			try {
				await this.entryIndex.rollbackNativeCommittedAppendFacts(transaction);
			} catch (rollbackError) {
				rollbackFailures.push(rollbackError);
			}
		}
		try {
			await this.rollbackNativeAppendBlocksHashes(
				hashes,
				restoreNativeCids,
				ownershipToken,
			);
		} catch (rollbackError) {
			rollbackFailures.push(rollbackError);
		}
		if (rollbackFailures.length > 0) {
			throw new AggregateError(
				[error, ...rollbackFailures],
				"Native append and compensation both failed",
			);
		}
		throw error;
	}

	private getNextsForAppend(
		options: AppendOptions<T>,
	): MaybePromise<Sorting.SortableEntry[]> {
		this.validateExplicitNexts(options);
		return (
			options.meta?.next ||
			this.entryIndex.getHeadsForAppend() ||
			this.entryIndex
				.getHeads(undefined, { type: "shape", shape: Sorting.ENTRY_SORT_SHAPE })
				.all()
		);
	}

	private async createAppendEntry(
		data: T,
		options: AppendOptions<T>,
		nexts: Sorting.SortableEntry[],
		storeOptions?: {
			deferStore?: boolean;
		},
	): Promise<Entry<T>> {
		const clock = new Clock({
			id: this._identity.publicKey.bytes,
			timestamp: options?.meta?.timestamp || this._hlc.now(),
		});

		const entry = await EntryV0.create<T>({
			store: this._storage,
			identity: options.identity || this._identity,
			signers: options.signers?.map((signer) =>
				this.wrapMutationCallback(signer),
			),
			data,
			meta: {
				clock,
				type: options.meta?.type,
				gidSeed: options.meta?.gidSeed,
				data: options.meta?.data,
				next: nexts,
			},
			encoding: this._encoding,
			encryption: options.encryption
				? {
						keypair: options.encryption.keypair,
						receiver: {
							...options.encryption.receiver,
						},
					}
				: undefined,
			canAppend: canAppendAlreadyValidated(options)
				? undefined
				: options.canAppend
					? (entry) =>
							this.runWithMutationCallback(() => options.canAppend!(entry))
					: this._hasCustomCanAppend
						? this._canAppend
						: undefined,
			deferStore: storeOptions?.deferStore,
		});

		if (!entry.hash) {
			throw new Error("Unexpected");
		}
		return entry;
	}

	private async joinMissingNexts(
		entry: Entry<T>,
		nexts: Sorting.SortableEntry[],
	) {
		if (entry.meta.type === EntryType.CUT) {
			return;
		}
		for (const e of nexts) {
			if (await this.has(e.hash)) {
				continue;
			}
			let nextEntry: Entry<any>;
			if (e instanceof Entry) {
				nextEntry = e;
			} else {
				const resolved = await this.entryIndex.get(e.hash);
				if (!resolved) {
					warn("Unexpected missing entry when joining", e.hash);
					continue;
				}
				nextEntry = resolved;
			}
			await this.join([nextEntry]);
		}
	}

	private async putAppendEntry(entry: Entry<T>, options: AppendOptions<T>) {
		await this.entryIndex.put(entry, {
			unique: true,
			isHead: true,
			toMultiHash: false,
			deferIndexWrite:
				options.deferIndexWrite ??
				(options.durability
					? options.durability === "buffered"
					: this._appendDurability === "buffered"),
		});
	}

	private async putAppendEntries(
		entries: Entry<T>[],
		options: AppendOptions<T>,
		externalNextHashes: string[],
		preparedAppendChain?: PreparedAppendChain<T>,
		heads?: boolean[],
	) {
		const prepared =
			preparedAppendChain &&
			entries.length === preparedAppendChain.entries.length
				? {
						shallowEntries: preparedAppendChain.shallowEntries,
						nativeEntries: preparedAppendChain.nativeEntries,
						nativeGraphUpdated: preparedAppendChain.nativeGraphUpdated,
						nativeBlocksCommitted: preparedAppendChain.nativeBlocksCommitted,
					}
				: undefined;
		if (
			prepared?.nativeBlocksCommitted === true &&
			hasDurableWriteBarrier(this._storage)
		) {
			await this._storage.waitForDurableWrites();
		}
		if (
			entries.length === 1 &&
			prepared?.nativeGraphUpdated === true &&
			!this.entryIndex.properties.onGidRemoved
		) {
			await this.entryIndex.putNativeCommittedAppend(entries[0]!, {
				unique: true,
				externalNextHashes,
				shallowEntry: prepared.shallowEntries[0],
				isHead: heads?.[0] ?? true,
			});
			return;
		}

		await this.entryIndex.putAppendBatch(entries, {
			unique: true,
			externalNextHashes,
			heads,
			prepared,
			deferIndexWrite:
				options.deferIndexWrite ??
				(options.durability
					? options.durability === "buffered"
					: this._appendDurability === "buffered"),
		});
	}

	private async putAppendEntryBlocks(
		entries: Entry<T>[],
		preparedBlocks?: PreparedEntryBlock[],
	) {
		const blocks =
			preparedBlocks && preparedBlocks.length === entries.length
				? preparedBlocks
				: entries.map((entry) => {
						const prepared = Entry.takePreparedBlock(entry);
						if (!prepared) {
							throw new Error("Missing prepared entry block");
						}
						return prepared;
					});

		if (blocks.length === 1 && hasPutKnown(this._storage)) {
			const block = blocks[0]!;
			const cidResult = this._storage.putKnown(block.cid, block.block.bytes);
			const cid = isPromiseLike(cidResult) ? await cidResult : cidResult;
			if (cid !== block.cid) {
				throw new Error("Unexpected block cid");
			}
			await this.waitForDurableWrites();
			return;
		}
		if (hasPutKnownManyColumns(this._storage)) {
			const cids = new Array<string>(blocks.length);
			const bytes = new Array<Uint8Array>(blocks.length);
			for (let i = 0; i < blocks.length; i++) {
				const block = blocks[i]!;
				cids[i] = block.cid;
				bytes[i] = block.block.bytes;
			}
			const cidsResult = this._storage.putKnownManyColumns(cids, bytes);
			const result = isPromiseLike(cidsResult) ? await cidsResult : cidsResult;
			if (result.length !== blocks.length) {
				throw new Error("Unexpected block batch result length");
			}
			for (let i = 0; i < result.length; i++) {
				if (result[i] !== cids[i]) {
					throw new Error("Unexpected block batch cid");
				}
			}
			await this.waitForDurableWrites();
			return;
		}
		if (hasPutKnownMany(this._storage)) {
			const cidsResult = this._storage.putKnownMany(
				blocks.map((block) => [block.cid, block.block.bytes] as const),
			);
			const cids = isPromiseLike(cidsResult) ? await cidsResult : cidsResult;
			if (cids.length !== blocks.length) {
				throw new Error("Unexpected block batch result length");
			}
			for (let i = 0; i < cids.length; i++) {
				if (cids[i] !== blocks[i]!.cid) {
					throw new Error("Unexpected block batch cid");
				}
			}
			await this.waitForDurableWrites();
			return;
		}
		const cids = await (this._storage as BlocksWithPutMany).putMany!(blocks);
		if (cids.length !== blocks.length) {
			throw new Error("Unexpected block batch result length");
		}
		for (let i = 0; i < cids.length; i++) {
			if (cids[i] !== blocks[i].cid) {
				throw new Error("Unexpected block batch cid");
			}
		}
		await this.waitForDurableWrites();
	}

	private async putKnownEntryBytesBatch(
		blocks: Array<{ cid: string; bytes: Uint8Array }>,
	) {
		if (blocks.length === 0) {
			return;
		}
		if (blocks.length === 1 && hasPutKnown(this._storage)) {
			const block = blocks[0]!;
			const cidResult = this._storage.putKnown(block.cid, block.bytes);
			const cid = isPromiseLike(cidResult) ? await cidResult : cidResult;
			if (cid !== block.cid) {
				throw new Error("Unexpected block cid");
			}
			await this.waitForDurableWrites();
			return;
		}
		if (hasPutKnownManyColumns(this._storage)) {
			const cids = new Array<string>(blocks.length);
			const bytes = new Array<Uint8Array>(blocks.length);
			for (let i = 0; i < blocks.length; i++) {
				const block = blocks[i]!;
				cids[i] = block.cid;
				bytes[i] = block.bytes;
			}
			const cidsResult = this._storage.putKnownManyColumns(cids, bytes);
			const result = isPromiseLike(cidsResult) ? await cidsResult : cidsResult;
			if (result.length !== blocks.length) {
				throw new Error("Unexpected block batch result length");
			}
			for (let i = 0; i < result.length; i++) {
				if (result[i] !== cids[i]) {
					throw new Error("Unexpected block batch cid");
				}
			}
			await this.waitForDurableWrites();
			return;
		}
		if (hasPutKnownMany(this._storage)) {
			const cidsResult = this._storage.putKnownMany(
				blocks.map((block) => [block.cid, block.bytes] as const),
			);
			const cids = isPromiseLike(cidsResult) ? await cidsResult : cidsResult;
			if (cids.length !== blocks.length) {
				throw new Error("Unexpected block batch result length");
			}
			for (let i = 0; i < cids.length; i++) {
				if (cids[i] !== blocks[i]!.cid) {
					throw new Error("Unexpected block batch cid");
				}
			}
			await this.waitForDurableWrites();
			return;
		}
		const preparedBlocks = blocks.map((block) =>
			Entry.preparedBlockFromBytes(block.bytes, block.cid),
		);
		const cids = await (this._storage as BlocksWithPutMany).putMany!(
			preparedBlocks,
		);
		if (cids.length !== blocks.length) {
			throw new Error("Unexpected block batch result length");
		}
		for (let i = 0; i < cids.length; i++) {
			if (cids[i] !== blocks[i]!.cid) {
				throw new Error("Unexpected block batch cid");
			}
		}
		await this.waitForDurableWrites();
	}

	private putPreparedAppendBlocks(
		preparedBlocks?: PreparedEntryBlock[],
	): MaybePromise<void> {
		if (!preparedBlocks || preparedBlocks.length === 0) {
			throw new Error("Missing prepared entry block");
		}
		if (preparedBlocks.length === 1 && hasPutKnown(this._storage)) {
			const block = preparedBlocks[0]!;
			const cidResult = this._storage.putKnown(block.cid, block.block.bytes);
			const checkCid = (cid: string) => {
				if (cid !== block.cid) {
					throw new Error("Unexpected block cid");
				}
			};
			return mapMaybePromise(cidResult, (cid) => {
				checkCid(cid);
				return this.waitForDurableWrites();
			});
		}
		const checkCids = (cids: string[]) => {
			if (cids.length !== preparedBlocks.length) {
				throw new Error("Unexpected block batch result length");
			}
			for (let i = 0; i < cids.length; i++) {
				if (cids[i] !== preparedBlocks[i]!.cid) {
					throw new Error("Unexpected block batch cid");
				}
			}
		};
		if (hasPutKnownManyColumns(this._storage)) {
			const cids = new Array<string>(preparedBlocks.length);
			const bytes = new Array<Uint8Array>(preparedBlocks.length);
			for (let i = 0; i < preparedBlocks.length; i++) {
				const block = preparedBlocks[i]!;
				cids[i] = block.cid;
				bytes[i] = block.block.bytes;
			}
			const cidsResult = this._storage.putKnownManyColumns(cids, bytes);
			return mapMaybePromise(cidsResult, (result) => {
				checkCids(result);
				return this.waitForDurableWrites();
			});
		}
		if (hasPutKnownMany(this._storage)) {
			const cidsResult = this._storage.putKnownMany(
				preparedBlocks.map((block) => [block.cid, block.block.bytes] as const),
			);
			return mapMaybePromise(cidsResult, (result) => {
				checkCids(result);
				return this.waitForDurableWrites();
			});
		}
		const cidsResult = (this._storage as BlocksWithPutMany).putMany!(
			preparedBlocks,
		);
		return mapMaybePromise(cidsResult, (result) => {
			checkCids(result);
			return this.waitForDurableWrites();
		});
	}

	async remove(
		entry:
			| { hash: string; meta: { next: string[] } }
			| { hash: string; meta: { next: string[] } }[],
		options?: { recursively?: boolean },
	): Promise<Change<T>> {
		this.throwIfDurableWritesFailed();
		/* await this.load({ reload: false }); */
		const entries = Array.isArray(entry) ? entry : [entry];

		if (entries.length === 0) {
			return {
				added: [],
				removed: [],
			};
		}

		let removed: {
			entry: ShallowOrFullEntry<T>;
			fn: () => Promise<ShallowEntry | undefined>;
		}[];

		if (options?.recursively) {
			removed = await this.prepareDeleteRecursively(entry);
		} else {
			removed = [];
			for (const entry of entries) {
				const deleteFn = await this.prepareDelete(entry.hash);
				deleteFn.entry &&
					removed.push({ entry: deleteFn.entry, fn: deleteFn.fn });
			}
		}

		const change: Change<T> = {
			added: [],
			removed: removed.map((x) => x.entry),
		};

		await this._onChange?.(change);

		// invoke deletions
		await Promise.all(removed.map((x) => x.fn()));

		return change;
	}

	async trim(
		option: TrimOptions | undefined = this._trim.options,
		properties?: { resolveDeletedEntries?: boolean },
	) {
		this.throwIfDurableWritesFailed();
		return this._trim.trim(option, properties);
	}

	private trimIfConfigured(
		option?: TrimOptions,
		properties?: { resolveDeletedEntries?: boolean },
	): MaybePromise<ShallowOrFullEntry<T>[] | undefined> {
		const resolved = option
			? this.wrapTrimCallbacks(option)
			: this._trim.options;
		return resolved ? this.trim(resolved, properties) : undefined;
	}

	private getNativeCommitOnlyTrimLengthTo(
		option: TrimOptions | undefined,
		resolveDeletedEntries: boolean | undefined,
	): number | undefined {
		const resolved = option ?? this._trim.options;
		if (
			!resolved ||
			resolved.type !== "length" ||
			resolved.filter?.canTrim ||
			resolveDeletedEntries !== false
		) {
			return;
		}

		const from = resolved.from ?? resolved.to;
		if (this.length + 1 < from) {
			return;
		}
		return resolved.to;
	}

	async join(
		entriesOrLog:
			| (string | Entry<T> | ShallowEntry | EntryWithRefs<T>)[]
			| Log<T>
			| ResultsIterator<Entry<any>>,
		options?: JoinOptions<T>,
	): Promise<void>;
	async join(
		entriesOrLog:
			| (string | Entry<T> | ShallowEntry | EntryWithRefs<T>)[]
			| Log<T>
			| ResultsIterator<Entry<any>>,
		options?: TrustedJoinOptions<T>,
	): Promise<void> {
		this.throwIfDurableWritesFailed();
		let entries: Entry<T>[];
		const references: Map<string, Entry<T>> = new Map();

		const fromCache = new Map<string, string[] | null>();
		const resolveRemoteFrom = async (hash: string, signal?: AbortSignal) => {
			const cached = fromCache.get(hash);
			if (cached !== undefined) return cached === null ? undefined : cached;

			let from: string[] | undefined;
			try {
				from = await this.entryIndex.properties.resolveRemotePeers?.(hash, {
					signal,
				});
			} catch {
				from = undefined;
			}
			const normalized = from && from.length > 0 ? from : undefined;
			fromCache.set(hash, normalized ?? null);
			return normalized;
		};

		const remote: NonNullable<Exclude<GetOptions["remote"], boolean>> = {
			timeout: options?.timeout,
			signal: this._closeController.signal,
		};

		if (entriesOrLog instanceof Log) {
			if (entriesOrLog.entryIndex.length === 0) return;
			entries = await entriesOrLog.toArray();
			for (const element of entries) references.set(element.hash, element);
		} else if (Array.isArray(entriesOrLog)) {
			if (entriesOrLog.length === 0) return;
			const existingHashes =
				options?.reset || options?.__peerbitEntriesAlreadyMissing === true
					? new Set<string>()
					: await this.entryIndex.hasMany(
							entriesOrLog.map((element) =>
								typeof element === "string"
									? element
									: element instanceof Entry
										? element.hash
										: element instanceof ShallowEntry
											? element.hash
											: element.entry.hash,
							),
						);

			entries = [];
			for (const element of entriesOrLog) {
				if (element instanceof Entry) {
					if (existingHashes.has(element.hash)) {
						continue;
					}
					entries.push(element);
					references.set(element.hash, element);
					continue;
				}

				if (typeof element === "string") {
					if (existingHashes.has(element)) {
						continue; // already in log
					}

					const from = await resolveRemoteFrom(
						element,
						this._closeController.signal,
					);
					let entry: Entry<T>;
					try {
						entry = await Entry.fromMultihash<T>(this._storage, element, {
							remote: {
								timeout: remote.timeout,
								signal: remote.signal,
								...(from && from.length > 0 ? { from } : {}),
							},
						});
					} catch (error) {
						if (isRecoverableJoinResolveError(error)) {
							continue;
						}
						throw error;
					}
					entries.push(entry);
					references.set(entry.hash, entry);
					continue;
				}

				if (element instanceof ShallowEntry) {
					if (existingHashes.has(element.hash)) {
						continue; // already in log
					}

					const from = await resolveRemoteFrom(
						element.hash,
						this._closeController.signal,
					);
					let entry: Entry<T>;
					try {
						entry = await Entry.fromMultihash<T>(this._storage, element.hash, {
							remote: {
								timeout: remote.timeout,
								signal: remote.signal,
								...(from && from.length > 0 ? { from } : {}),
							},
						});
					} catch (error) {
						if (isRecoverableJoinResolveError(error)) {
							continue;
						}
						throw error;
					}
					entries.push(entry);
					references.set(entry.hash, entry);
					continue;
				}

				entries.push(element.entry);
				references.set(element.entry.hash, element.entry);
				for (const ref of element.references) {
					references.set(ref.hash, ref);
				}
			}
		} else {
			const all = await entriesOrLog.all(); // TODO dont load all at once
			if (all.length === 0) return;
			entries = all;
		}

		const profile = options?.__peerbitProfile;
		const headsStartedAt = internalProfileStart(profile);
		const heads: Map<string, boolean> = new Map();
		for (const entry of entries) {
			if (heads.has(entry.hash)) continue;
			heads.set(entry.hash, true);
			const nexts =
				options?.__peerbitBatchIndependent === true
					? entry.meta.next
					: await entry.getNext();
			for (const next of nexts) heads.set(next, false);
		}
		emitInternalProfileDuration(profile, headsStartedAt, {
			name: "log.join.prepareHeads",
			component: "log",
			entries: entries.length,
			count: heads.size,
			messages: 1,
			details: {
				batchIndependent: options?.__peerbitBatchIndependent === true,
			},
		});

		if (
			options?.__peerbitBatchIndependent === true &&
			(await this.tryJoinIndependentAppendBatch(entries, heads, options))
		) {
			return;
		}

		for (const entry of entries) {
			const isHead = heads.get(entry.hash)!;
			const prev = this._joining.get(entry.hash);
			if (prev) {
				await prev;
				continue;
			}

			const p = this.joinRecursively(entry, {
				references,
				isHead,
				reset: options?.reset,
				verifySignatures: options?.verifySignatures,
				trim: options?.trim,
				onChange: options?.onChange,
				remote,
				resolveRemoteFrom,
			});
			this._joining.set(entry.hash, p);
			p.finally(() => {
				this._joining.delete(entry.hash);
			});
			await p;
		}
	}

	// Internal trusted receive path for callers that can supply prepared append facts.
	private async joinPreparedAppendFactsBatch(
		entries: PreparedAppendJoinFacts[],
		options?: TrustedPreparedAppendFactsBatchJoinOptions,
	): Promise<boolean> {
		this.throwIfDurableWritesFailed();
		return this.withNativeCommittedAppendAdmission(() =>
			this.joinPreparedAppendFactsBatchAdmitted(entries, options),
		);
	}

	private async joinPreparedAppendFactsBatchAdmitted(
		entries: PreparedAppendJoinFacts[],
		options?: TrustedPreparedAppendFactsBatchJoinOptions,
	): Promise<boolean> {
		if (
			entries.length === 0 ||
			!canAppendAlreadyValidated(options) ||
			entries.some((entry) => this._joining.has(entry.hash))
		) {
			return false;
		}

		const resolvedOptions = options!;
		const profile = resolvedOptions.__peerbitProfile;
		const prepareStartedAt = internalProfileStart(profile);
		const entryHashes = new Array<string>(entries.length);
		let hasAnyNext = false;
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			// byteLength stands in for the bytes presence check: prepared facts
			// carry both, and probing `bytes` would force stash-backed heads to
			// materialize block bytes the native commit never reads.
			if (
				!entry.hash ||
				entry.byteLength == null ||
				entry.meta.type !== EntryType.APPEND
			) {
				return false;
			}
			entryHashes[i] = entry.hash;
			if (entry.meta.next.length > 0) {
				hasAnyNext = true;
			}
		}
		const batchHashes = new Set(entryHashes);
		let heads: Map<string, boolean> | undefined;
		if (hasAnyNext) {
			heads = new Map();
			for (const entry of entries) {
				if (heads.has(entry.hash)) {
					continue;
				}
				heads.set(entry.hash, true);
				for (const next of entry.meta.next) {
					heads.set(next, false);
				}
			}
		}
		emitInternalProfileDuration(profile, prepareStartedAt, {
			name: "log.joinPreparedFacts.prepare",
			component: "log",
			entries: entries.length,
			count: heads?.size ?? entries.length,
			messages: 1,
		});

		const nativeCommitValidatesPlan =
			resolvedOptions.__peerbitNativePreparedJoinCommitValidatesPlan === true &&
			!!resolvedOptions.__peerbitNativePreparedJoinCommit;
		const headFlags: boolean[] = [];
		const headFlagsBytes = new Uint8Array(entries.length);
		const pushHeadFlag = (index: number, isHead: boolean) => {
			headFlags.push(isHead);
			headFlagsBytes[index] = isHead ? 1 : 0;
		};
		if (nativeCommitValidatesPlan) {
			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i]!;
				pushHeadFlag(i, heads?.get(entry.hash) ?? true);
			}
			emitInternalProfileDuration(profile, internalProfileStart(profile), {
				name: "log.joinPreparedFacts.plan",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: { nativeCommitValidatesPlan: true },
			});
			emitInternalProfileDuration(profile, internalProfileStart(profile), {
				name: "log.joinPreparedFacts.validatePlan",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: { nativeCommitValidatesPlan: true },
			});
		} else {
			const planStartedAt = internalProfileStart(profile);
			const joinPlans = await this.entryIndex.planJoinBatch(
				entries,
				false,
				profile,
			);
			emitInternalProfileDuration(profile, planStartedAt, {
				name: "log.joinPreparedFacts.plan",
				component: "log",
				entries: entries.length,
				messages: 1,
			});
			const validatePlanStartedAt = internalProfileStart(profile);
			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i]!;
				const joinPlan = joinPlans[i]!;
				if (
					joinPlan.skip ||
					joinPlan.coveredByCut ||
					!joinPlan.cutChecked ||
					joinPlan.missingParents.some((hash) => !batchHashes.has(hash))
				) {
					return false;
				}
				pushHeadFlag(i, heads?.get(entry.hash) ?? true);
			}
			emitInternalProfileDuration(profile, validatePlanStartedAt, {
				name: "log.joinPreparedFacts.validatePlan",
				component: "log",
				entries: entries.length,
				messages: 1,
			});
		}

		let nativeValidatedCommitRejected = false;
		const batchPromise = (async () => {
			const clockStartedAt = internalProfileStart(profile);
			for (const entry of entries) {
				this._hlc.update(entry.meta.clock.timestamp);
			}
			emitInternalProfileDuration(profile, clockStartedAt, {
				name: "log.joinPreparedFacts.clock",
				component: "log",
				entries: entries.length,
				messages: 1,
			});

			const trustedMissing =
				resolvedOptions.__peerbitEntriesAlreadyMissing === true &&
				batchHashes.size === entries.length;
			let nativePreparedCommitted = false;
			if (resolvedOptions.__peerbitNativePreparedJoinCommit) {
				const nativeCommitStartedAt = internalProfileStart(profile);
				nativePreparedCommitted =
					(await this.runWithMutationCallback(() =>
						resolvedOptions.__peerbitNativePreparedJoinCommit!({
							entries,
							hashes: entryHashes,
							headFlags,
							headFlagsBytes,
							trustedMissing,
							validatePlan: nativeCommitValidatesPlan,
						}),
					)) === true;
				emitInternalProfileDuration(profile, nativeCommitStartedAt, {
					name: "log.joinPreparedFacts.nativePreparedCommit",
					component: "log",
					entries: entries.length,
					messages: 1,
					details: { nativePreparedCommitted },
				});
			}
			if (nativeCommitValidatesPlan && !nativePreparedCommitted) {
				nativeValidatedCommitRejected = true;
				return;
			}
			const blocksStartedAt = internalProfileStart(profile);
			if (!nativePreparedCommitted) {
				await this.putKnownEntryBytesBatch(
					entries.map((entry) => ({
						cid: entry.hash,
						bytes: entry.bytes,
					})),
				);
			} else {
				// A native prepared receive may have used the synchronous columnar block
				// callback. Hold index/head/change publication behind the one batch-level
				// durability barrier just like the generic block-store fallback.
				await this.waitForDurableWrites();
			}
			emitInternalProfileDuration(profile, blocksStartedAt, {
				name: "log.joinPreparedFacts.blocks",
				component: "log",
				entries: entries.length,
				bytes: entries.reduce((sum, entry) => sum + entry.byteLength, 0),
				messages: 1,
				details: { nativePreparedCommitted },
			});

			const indexStartedAt = internalProfileStart(profile);
			let nativeCommittedFactsIndexed = false;
			if (
				nativePreparedCommitted &&
				resolvedOptions.__peerbitDeferIndexWrite === true
			) {
				const indexBatchHashes = entries.length > 1 ? batchHashes : undefined;
				const indexRows = entries.map((entry, index) => {
					const isHead = headFlags[index] ?? true;
					const externalNextHashes = indexBatchHashes
						? entry.meta.next.filter((next) => !indexBatchHashes.has(next))
						: entry.meta.next;
					return {
						hash: entry.hash,
						unique: trustedMissing,
						externalNextHashes,
						getShallowEntry: () => {
							const shallowEntry =
								entry.shallowEntry ?? entry.getShallowEntry?.(isHead);
							if (!shallowEntry) {
								throw new Error("Missing prepared append shallow entry");
							}
							shallowEntry.head = isHead;
							entry.shallowEntry = shallowEntry;
							return shallowEntry;
						},
						isHead,
					};
				});
				await this.entryIndex.putNativeCommittedAppendFactsBatch(indexRows);
				nativeCommittedFactsIndexed = true;
			} else {
				const externalNextHashes =
					entries.length === 1 ? entries[0]!.meta.next : undefined;
				await this.entryIndex.putAppendFactsBatch(entries, {
					unique: trustedMissing,
					externalNextHashes,
					heads: headFlags,
					deferIndexWrite: resolvedOptions.__peerbitDeferIndexWrite,
					nativeGraphUpdated: nativePreparedCommitted,
					profile,
				});
			}
			emitInternalProfileDuration(profile, indexStartedAt, {
				name: "log.joinPreparedFacts.entryIndex",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: { trustedMissing, nativeCommittedFactsIndexed },
			});

			if (resolvedOptions.__peerbitOnPreparedJoinCommitted) {
				const committedStartedAt = internalProfileStart(profile);
				await this.runWithMutationCallback(() =>
					resolvedOptions.__peerbitOnPreparedJoinCommitted!({
						entries,
						hashes: entryHashes,
						headFlags,
						nativePreparedCommitted,
					}),
				);
				emitInternalProfileDuration(profile, committedStartedAt, {
					name: "log.joinPreparedFacts.committed",
					component: "log",
					entries: entries.length,
					messages: 1,
					details: { nativePreparedCommitted },
				});
			}

			const changeStartedAt = internalProfileStart(profile);
			if (resolvedOptions.__peerbitOnAppendHashes) {
				await this.runWithMutationCallback(() =>
					resolvedOptions.__peerbitOnAppendHashes!(
						entries.map((entry) => entry.hash),
					),
				);
			} else {
				const change: Change<T> = {
					added: entries.map((entry, index) => {
						const materializeEntry = entry.materializeEntry;
						if (!materializeEntry) {
							throw new Error("Missing prepared append materializer");
						}
						return {
							head: headFlags[index]!,
							entry: materializeEntry() as Entry<T>,
						};
					}),
					removed: [],
				};
				await this._onChange?.(change);
			}
			emitInternalProfileDuration(profile, changeStartedAt, {
				name: "log.joinPreparedFacts.change",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: { hashOnly: !!resolvedOptions.__peerbitOnAppendHashes },
			});
		})().finally(() => {
			for (const entry of entries) {
				this._joining.delete(entry.hash);
			}
		});

		for (const entry of entries) {
			this._joining.set(entry.hash, batchPromise);
		}
		await batchPromise;
		if (nativeValidatedCommitRejected) {
			return false;
		}
		return true;
	}

	private async tryJoinIndependentAppendBatch(
		entries: Entry<T>[],
		heads: Map<string, boolean>,
		options: TrustedJoinOptions<T>,
	): Promise<boolean> {
		if (
			entries.length < 2 ||
			options.reset ||
			options.trim ||
			options.verifySignatures ||
			entries.some((entry) => this._joining.has(entry.hash))
		) {
			return false;
		}

		const profile = options.__peerbitProfile;
		const prepareStartedAt = internalProfileStart(profile);
		const batchHashes = new Set(entries.map((entry) => entry.hash));
		const headFlags: boolean[] = [];
		for (const entry of entries) {
			if (!entry.hash || !Entry.hasPreparedBlock(entry)) {
				return false;
			}
			entry.init(this);
			if (entry.meta.type !== EntryType.APPEND) {
				return false;
			}
		}
		emitInternalProfileDuration(profile, prepareStartedAt, {
			name: "log.joinIndependent.prepare",
			component: "log",
			entries: entries.length,
			messages: 1,
		});

		const planStartedAt = internalProfileStart(profile);
		const joinPlans = await this.entryIndex.planJoinBatch(
			entries,
			false,
			profile,
		);
		emitInternalProfileDuration(profile, planStartedAt, {
			name: "log.joinIndependent.plan",
			component: "log",
			entries: entries.length,
			messages: 1,
		});
		const validatePlanStartedAt = internalProfileStart(profile);
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			const joinPlan = joinPlans[i]!;
			if (
				joinPlan.skip ||
				joinPlan.coveredByCut ||
				!joinPlan.cutChecked ||
				joinPlan.missingParents.some((hash) => !batchHashes.has(hash))
			) {
				return false;
			}
			headFlags.push(heads.get(entry.hash) ?? true);
		}
		emitInternalProfileDuration(profile, validatePlanStartedAt, {
			name: "log.joinIndependent.validatePlan",
			component: "log",
			entries: entries.length,
			messages: 1,
		});

		if (!canAppendAlreadyValidated(options)) {
			const canAppendStartedAt = internalProfileStart(profile);
			for (const entry of entries) {
				if (this._canAppend && !(await this._canAppend(entry))) {
					return false;
				}
			}
			emitInternalProfileDuration(profile, canAppendStartedAt, {
				name: "log.joinIndependent.canAppend",
				component: "log",
				entries: entries.length,
				messages: 1,
			});
		}

		const preparedBatch = this.takePreparedIndependentAppendBatch(
			entries,
			headFlags,
		);
		if (!preparedBatch) {
			return false;
		}

		const batchPromise = (async () => {
			const clockStartedAt = internalProfileStart(profile);
			for (const entry of entries) {
				this._hlc.update(entry.meta.clock.timestamp);
			}
			emitInternalProfileDuration(profile, clockStartedAt, {
				name: "log.joinIndependent.clock",
				component: "log",
				entries: entries.length,
				messages: 1,
			});

			const blocksStartedAt = internalProfileStart(profile);
			await this.putAppendEntryBlocks(entries, preparedBatch.blocks);
			emitInternalProfileDuration(profile, blocksStartedAt, {
				name: "log.joinIndependent.blocks",
				component: "log",
				entries: entries.length,
				bytes: entries.reduce((sum, entry) => sum + (entry.size ?? 0), 0),
				messages: 1,
			});
			const indexStartedAt = internalProfileStart(profile);
			const trustedMissing =
				options.__peerbitEntriesAlreadyMissing === true &&
				batchHashes.size === entries.length;
			await this.entryIndex.putAppendBatch(entries, {
				unique: trustedMissing,
				heads: headFlags,
				prepared: preparedBatch.prepared,
				deferIndexWrite: options.__peerbitDeferIndexWrite,
				profile,
			});
			emitInternalProfileDuration(profile, indexStartedAt, {
				name: "log.joinIndependent.entryIndex",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: { trustedMissing },
			});

			const changeStartedAt = internalProfileStart(profile);
			if (options.__peerbitOnAppendHashes && !options.onChange) {
				await options.__peerbitOnAppendHashes(
					entries.map((entry) => entry.hash),
				);
			} else {
				const change: Change<T> = {
					added: entries.map((entry, index) => ({
						head: headFlags[index]!,
						entry,
					})),
					removed: [],
				};
				if (options.onChange) {
					await this.runWithMutationCallback(() => options.onChange!(change));
				}
				await this._onChange?.(change);
			}
			emitInternalProfileDuration(profile, changeStartedAt, {
				name: "log.joinIndependent.change",
				component: "log",
				entries: entries.length,
				messages: 1,
				details: {
					hashOnly: !!options.__peerbitOnAppendHashes && !options.onChange,
				},
			});
		})().finally(() => {
			for (const entry of entries) {
				this._joining.delete(entry.hash);
			}
		});

		for (const entry of entries) {
			this._joining.set(entry.hash, batchPromise);
		}
		await batchPromise;
		return true;
	}

	private takePreparedIndependentAppendBatch(
		entries: Entry<T>[],
		headFlags: boolean[],
	): PreparedIndependentAppendBatch | undefined {
		if (!entries.every((entry) => Entry.hasPreparedBlock(entry))) {
			return;
		}
		const hasPreparedShallowEntries = entries.every((entry) =>
			Entry.hasPreparedShallowEntry(entry),
		);
		const hasPreparedNativeEntries =
			hasPreparedShallowEntries &&
			entries.every((entry) => Entry.hasPreparedNativeLogEntry(entry));

		const blocks = entries.map((entry) => {
			const prepared = Entry.takePreparedBlock(entry);
			if (!prepared) {
				throw new Error("Missing prepared entry block");
			}
			return prepared;
		});
		if (!hasPreparedShallowEntries) {
			return { blocks };
		}

		const shallowEntries = entries.map((entry, index) => {
			const shallowEntry = Entry.takePreparedShallowEntry(
				entry,
				headFlags[index] ?? true,
			);
			if (!shallowEntry) {
				throw new Error("Missing prepared shallow entry");
			}
			return shallowEntry;
		});
		const nativeEntries = hasPreparedNativeEntries
			? entries.map((entry, index) => {
					const nativeEntry = Entry.takePreparedNativeLogEntry(
						entry,
						headFlags[index] ?? true,
					);
					if (!nativeEntry) {
						throw new Error("Missing prepared native log entry");
					}
					return nativeEntry;
				})
			: undefined;

		return {
			blocks,
			prepared: {
				shallowEntries,
				nativeEntries,
			},
		};
	}

	/**
	 * Bottom up join of entries into the log
	 * @param entry
	 * @param options
	 * @returns
	 */

	private async joinRecursively(
		entry: Entry<T>,
		options: {
			verifySignatures?: boolean;
			trim?: TrimOptions;
			length?: number;
			references?: Map<string, Entry<T>>;
			isHead: boolean;
			reset?: boolean;
			onChange?: OnChange<T>;
			remote?: GetOptions["remote"];
			resolveRemoteFrom?: (
				hash: string,
				signal?: AbortSignal,
			) => Promise<string[] | undefined>;
		},
	): Promise<boolean> {
		if (this.entryIndex.length > (options.length ?? Number.MAX_SAFE_INTEGER)) {
			return false;
		}

		if (!entry.hash) {
			throw new Error("Unexpected");
		}

		const joinPlan = await this.entryIndex.planJoin(entry, options.reset);
		if (joinPlan.skip) {
			return false;
		}

		entry.init(this);

		if (options.verifySignatures) {
			if (!(await entry.verifySignatures())) {
				throw new Error(`Invalid signature entry with hash "${entry.hash}"`);
			}
		}

		if (joinPlan.coveredByCut) {
			return false;
		}

		if (!joinPlan.cutChecked) {
			const headsWithGid: JoinableEntry[] = await this.entryIndex.getJoinHeads(
				entry.meta.gid,
			);
			for (const v of headsWithGid) {
				// TODO second argument should be a time compare instead? what about next nexts?
				// and check the cut entry is newer than the current 'entry'
				if (
					v.meta.type === EntryType.CUT &&
					v.meta.next.includes(entry.hash) &&
					Sorting.compare(entry, v, this._sortFn) < 0
				) {
					return false; // already deleted
				}
			}
		}

		if (entry.meta.type !== EntryType.CUT) {
			const remote =
				options.remote && typeof options.remote === "object"
					? options.remote
					: undefined;
			const parents: Array<{ hash: string; entry?: Entry<T> }> = [];
			const unresolvedParentHashes: string[] = [];

			for (const a of joinPlan.missingParents) {
				const prev = this._joining.get(a);
				if (prev) {
					await prev;
					continue;
				}

				const referenced = options.references?.get(a);
				parents.push({ hash: a, entry: referenced });
				if (!referenced) {
					unresolvedParentHashes.push(a);
				}
			}

			const localParents =
				unresolvedParentHashes.length > 0
					? await this.entryIndex.getMany(unresolvedParentHashes, {
							type: "full",
							ignoreMissing: true,
						})
					: [];
			const localParentByHash = new Map<string, Entry<T>>();
			for (const parent of localParents) {
				if (parent) {
					localParentByHash.set(parent.hash, parent);
				}
			}

			for (const parent of parents) {
				const a = parent.hash;
				const prev = this._joining.get(a);
				if (prev) {
					await prev;
					continue;
				}

				let nested = parent.entry ?? localParentByHash.get(a);
				try {
					if (!nested) {
						const from = await options.resolveRemoteFrom?.(a, remote?.signal);
						nested = await Entry.fromMultihash<T>(this._storage, a, {
							remote: {
								timeout: remote?.timeout,
								signal: remote?.signal,
								...(from && from.length > 0 ? { from } : {}),
							},
						});
					}
				} catch (error) {
					if (isRecoverableJoinResolveError(error)) {
						return false;
					}
					throw error;
				}

				const p = this.joinRecursively(
					nested,
					options.isHead ? { ...options, isHead: false } : options,
				);
				this._joining.set(nested.hash, p);
				p.finally(() => {
					this._joining.delete(nested.hash);
				});
				await p;
			}
		}

		if (this._canAppend && !(await this._canAppend(entry))) {
			return false;
		}

		const clock = await entry.getClock();
		this._hlc.update(clock.timestamp);

		await this._entryIndex.put(entry, {
			unique: false,
			isHead: options.isHead,
			toMultiHash: true,
		});

		const pendingDeletes: (
			| PendingDelete<T>
			| { entry: ShallowOrFullEntry<T>; fn: undefined }
		)[] = await this.processEntry(entry);
		const trimmed = await this.trimIfConfigured(options.trim);

		if (trimmed) {
			for (const removedEntry of trimmed) {
				pendingDeletes.push({ entry: removedEntry, fn: undefined });
			}
		}

		const removed = pendingDeletes.map((x) => x.entry);

		if (options.onChange) {
			await this.runWithMutationCallback(() =>
				options.onChange!({
					added: [{ head: options.isHead, entry }],
					removed,
				}),
			);
		}
		await this._onChange?.({
			added: [{ head: options.isHead, entry }],
			removed,
		});

		await Promise.all(pendingDeletes.map((x) => x.fn?.()));
		return true;
	}

	private async processEntry(entry: Entry<T>): Promise<
		{
			entry: ShallowOrFullEntry<T>;
			fn: () => Promise<ShallowEntry | undefined>;
		}[]
	> {
		if (entry.meta.type === EntryType.CUT) {
			return this.prepareDeleteRecursively(entry, true);
		}
		return [];
	}

	async deleteRecursively(
		from:
			| { hash: string; meta: { next: string[] } }
			| { hash: string; meta: { next: string[] } }[],
		skipFirst = false,
	) {
		const toDelete = await this.prepareDeleteRecursively(from, skipFirst);
		const removedEntries: ShallowEntry[] = [];
		for (const x of toDelete) {
			const removedEntry = await x.fn();
			if (removedEntry) {
				removedEntries.push(removedEntry);
			}
		}
		return removedEntries;
	}

	/// TODO simplify methods below
	async prepareDeleteRecursively(
		from:
			| { hash: string; meta: { next: string[] } }
			| { hash: string; meta: { next: string[] } }[],
		skipFirst = false,
	) {
		const entries = Array.isArray(from) ? [...from] : [from];
		const nativeDeletePlan = this.entryIndex.planDeleteRecursively(
			entries,
			skipFirst,
		);
		if (nativeDeletePlan) {
			const toDelete: PendingDelete<T>[] = [];
			for (const hash of nativeDeletePlan) {
				const deleteFn = await this.prepareDelete(hash);
				deleteFn.entry &&
					toDelete.push({ entry: deleteFn.entry, fn: deleteFn.fn });
			}
			return toDelete;
		}

		const stack = entries;
		const promises: (Promise<void> | void)[] = [];
		let counter = 0;
		const toDelete: PendingDelete<T>[] = [];

		while (stack.length > 0) {
			const entry = stack.pop()!;
			const skip = counter === 0 && skipFirst;
			if (!skip) {
				const deleteFn = await this.prepareDelete(entry.hash);
				deleteFn.entry &&
					toDelete.push({ entry: deleteFn.entry, fn: deleteFn.fn });
			}

			for (const next of entry.meta.next) {
				const entriesThatHasNext = await this.entryIndex.getJoinChildren(next);

				// if there are no entries which is not of "CUT" type, we can safely delete the next entry
				// figureately speaking, these means where are cutting all branches to a stem, so we can delete the stem as well
				let hasAlternativeNext = !!entriesThatHasNext.find(
					(x) => x.meta.type !== EntryType.CUT && x.hash !== entry.hash, // second arg is to avoid references to the same entry that is to be deleted (i.e we are looking for other entries)
				);
				if (!hasAlternativeNext) {
					const ne = await this.get(next);
					if (ne) {
						stack.push(ne);
					}
				}
			}
			counter++;
		}
		await Promise.all(promises);
		return toDelete;
	}

	async prepareDelete(
		hash: string,
	): Promise<PendingDelete<T> | { entry: undefined }> {
		this.throwIfDurableWritesFailed();
		let entry = await this._entryIndex.getShallow(hash);
		if (!entry) {
			return { entry: undefined };
		}
		return {
			entry: entry.value,
			fn: async () => {
				this.throwIfDurableWritesFailed();
				await this._trim.deleteFromCache(hash);
				const removedEntry = (await this._entryIndex.delete(
					hash,
					entry.value,
				)) as ShallowEntry;
				return removedEntry;
			},
		};
	}

	async delete(hash: string): Promise<ShallowEntry | undefined> {
		this.throwIfDurableWritesFailed();
		const deleteFn = await this.prepareDelete(hash);
		return deleteFn.entry && deleteFn.fn();
	}

	/**
	 * Returns the log entries as a formatted string.
	 * @returns {string}
	 * @example
	 * two
	 * └─one
	 *   └─three
	 */
	async toString(
		payloadMapper: (payload: Payload<T>) => string = (payload) =>
			(payload.getValue(this.encoding) as any).toString(),
	): Promise<string> {
		return (
			await Promise.all(
				(await this.toArray())
					.slice()
					.reverse()
					.map(async (e, idx) => {
						const parents: Entry<any>[] = Entry.findDirectChildren(
							e,
							await this.toArray(),
						);
						const len = parents.length;
						let padding = new Array(Math.max(len - 1, 0));
						padding = len > 1 ? padding.fill("  ") : padding;
						padding = len > 0 ? padding.concat(["└─"]) : padding;
						return (
							padding.join("") +
							(payloadMapper?.(e.payload) || (e.payload as any as string))
						);
					}),
			)
		).join("\n");
	}

	private enqueueTerminalLifecycle<T>(operation: () => Promise<T>): Promise<T> {
		const next = this._terminalLifecycleQueue.then(operation);
		this._terminalLifecycleQueue = next.then(
			() => undefined,
			() => undefined,
		);
		return next;
	}

	private async settleNativeCommittedAppendFinalizers(): Promise<void> {
		while ((this._nativeCommittedAppendFinalizers?.size ?? 0) > 0) {
			const finalizers = [...this._nativeCommittedAppendFinalizers!];
			for (const finalizer of finalizers) {
				await finalizer.settleForTerminal();
			}
		}
	}

	close(): Promise<void> {
		this.ensureRuntimeState();
		// Mutation callbacks run inside terminal admission. Waiting for teardown
		// from one would create a self-deadlock, so reject before changing any
		// lifecycle state and let the caller retry after the mutation settles.
		if (this._mutationCallbacksInFlight > 0) {
			return Promise.reject(
				new Error(
					"Cannot close a log while a mutation callback is running; retry after the callback completes",
				),
			);
		}
		if (this._dropCompleted) {
			return Promise.resolve();
		}
		if (this._dropPromise) {
			return this._dropPromise;
		}
		if (this._lifecycleState === "drop-failed") {
			// Drop is the stronger terminal operation. Never let close downgrade a
			// failed erase into a clean closed state that can be reopened.
			return this.drop();
		}
		if (this._closePromise) {
			return this._closePromise;
		}
		if (this._closeCompleted) {
			return Promise.resolve();
		}

		const opening = this._openPromise;
		this._lifecycleEpoch++;
		this._closed = true;
		this._terminalAdmissionClosed = true;
		this._lifecycleState = "closing";
		this._closeController?.abort();
		const operation = this.enqueueTerminalLifecycle(async () => {
			// If startup was admitted first, it owns initialization and this close
			// owns every teardown stage that follows. Its failure does not skip cleanup.
			await opening?.catch(() => undefined);
			this._closed = true;
			this._lifecycleState = "closing";
			if (!this._closeProgress.admissionsSettled) {
				await this.settleNativeCommittedAppendAdmissions();
				this._closeProgress.admissionsSettled = true;
			}
			if (!this._closeProgress.finalizersSettled) {
				await this.settleNativeCommittedAppendFinalizers();
				this._closeProgress.finalizersSettled = true;
			}
			if (!this._closeProgress.rollbacksRetried) {
				await this._entryIndex?.retryFailedNativeCommittedAppendFactsRollbacks();
				this._closeProgress.rollbacksRetried = true;
			}
			if (!this._closeProgress.pendingWritesFlushed) {
				await this._entryIndex?.flushPendingWrites();
				this._closeProgress.pendingWritesFlushed = true;
			}
			if (!this._closeProgress.blockHashesRetained && this._entryIndex) {
				const explicitlyPreservesData =
					await this._indexer?.preservesDataOnStop?.();
				const preservesDataOnStop =
					explicitlyPreservesData ??
					(await this._indexer?.persisted?.()) ??
					false;
				// A destructive/unknown stop must snapshot before stopping regardless of
				// when drop is requested. Data-preserving backends avoid this O(n) work
				// on ordinary close and can be restarted for a later drop.
				if (this._dropPromise || !preservesDataOnStop) {
					await this._entryIndex.retainBlockHashesForDrop();
					this._closeProgress.blockHashesRetained = true;
				}
			}
			if (!this._closeProgress.indexerStopped) {
				await this._indexer?.stop?.();
				this._closeProgress.indexerStopped = true;
			}
			this._loadedOnce = false;
			this._closeCompleted = true;
			this._lifecycleState = "closed";
		});
		const wrapped: Promise<void> = operation
			.catch((error) => {
				this._lifecycleState = "close-failed";
				throw error;
			})
			.finally(() => {
				if (this._closePromise === wrapped) {
					this._closePromise = undefined;
				}
			});
		this._closePromise = wrapped;
		return wrapped;
	}

	drop(): Promise<void> {
		this.ensureRuntimeState();
		// See close(): terminal reentrancy from a mutation callback must fail before
		// this stronger terminal operation mutates lifecycle ownership.
		if (this._mutationCallbacksInFlight > 0) {
			return Promise.reject(
				new Error(
					"Cannot drop a log while a mutation callback is running; retry after the callback completes",
				),
			);
		}
		if (this._dropCompleted) {
			return Promise.resolve();
		}
		if (this._dropPromise) {
			return this._dropPromise;
		}

		const opening = this._openPromise;
		this._lifecycleEpoch++;
		this._closed = true;
		this._terminalAdmissionClosed = true;
		this._lifecycleState = "dropping";
		this._closeController?.abort();
		const operation = this.enqueueTerminalLifecycle(async () => {
			await opening?.catch(() => undefined);
			this._closed = true;
			this._lifecycleState = "dropping";
			if (!this._dropProgress.admissionsSettled) {
				await this.settleNativeCommittedAppendAdmissions();
				this._dropProgress.admissionsSettled = true;
			}
			if (!this._dropProgress.entryIndexCleared) {
				// start() is required even when a prior stop rejected: backends may
				// reject after applying stop, and start is intentionally idempotent.
				await this._indexer?.start?.();
				if (
					!this._closeProgress.blockHashesRetained &&
					(this._closeCompleted || this._closeProgress.pendingWritesFlushed) &&
					this._entryIndex
				) {
					const reopenedSize =
						await this._entryIndex.properties.index.getSize();
					if (reopenedSize !== this._entryIndex.length) {
						throw new Error(
							"Cannot drop after close discarded an ephemeral entry index; request drop before close completes",
						);
					}
				}
			}
			if (!this._dropProgress.finalizersSettled) {
				await this.settleNativeCommittedAppendFinalizers();
				await this._entryIndex?.retryFailedNativeCommittedAppendFactsRollbacks();
				this._dropProgress.finalizersSettled = true;
			}
			if (!this._dropProgress.entryIndexCleared) {
				await this._entryIndex?.clear();
				this._dropProgress.entryIndexCleared = true;
			}
			if (!this._dropProgress.indexerDropped) {
				await this._indexer?.drop();
				this._dropProgress.indexerDropped = true;
			}
			if (!this._dropProgress.indexerStopped) {
				await this._indexer?.stop?.();
				this._dropProgress.indexerStopped = true;
			}
			this._indexer = undefined as any;
			this._loadedOnce = false;
			this._dropCompleted = true;
			this._closeCompleted = true;
			this._lifecycleState = "dropped";
		});
		const wrapped: Promise<void> = operation
			.catch((error) => {
				this._lifecycleState = "drop-failed";
				throw error;
			})
			.finally(() => {
				if (this._dropPromise === wrapped) {
					this._dropPromise = undefined;
				}
			});
		this._dropPromise = wrapped;
		return wrapped;
	}

	async recover() {
		// merge existing
		const existing = await this.getHeads(true).all();

		const allHeads: Map<string, Entry<any>> = new Map();
		for (const head of existing) {
			allHeads.set(head.hash, head);
		}

		// fetch all possible entries
		for await (const [key, value] of this._storage.iterator()) {
			if (allHeads.has(key)) {
				continue;
			}
			try {
				cidifyString(key);
			} catch (error) {
				continue;
			}

			try {
				const der = deserialize(value, Entry);
				der.hash = key;
				der.init(this);
				allHeads.set(key, der);
			} catch (error) {
				continue; // invalid entries
			}
		}

		// assume they are valid, (let access control reject them if not)
		await this.load({ reset: true, heads: [...allHeads.values()] });
	}

	async load(
		opts: {
			heads?: Entry<T>[];
			fetchEntryTimeout?: number;
			ignoreMissing?: boolean;
			timeout?: number;
			reset?: boolean;
		} = {},
	) {
		if (this.closed) {
			throw new Error("Closed");
		}

		if (this._loadedOnce && !opts.reset) {
			return;
		}

		this._loadedOnce = true;
		const heads =
			opts.heads ??
			(await this.entryIndex
				.getHeads(undefined, {
					type: "full",
					signal: this._closeController.signal,
					ignoreMissing: opts.ignoreMissing,
					timeout: opts.timeout,
				})
				.all());

		if (heads) {
			// Load the log
			await this.join(heads instanceof Entry ? [heads] : heads, {
				timeout: opts?.fetchEntryTimeout,
				reset: opts?.reset,
			});

			if (opts.heads) {
				// remove all heads that are not in the provided heads
				const allHeads = this.getHeads(false);
				const allProvidedHeadsHashes = new Set(opts.heads.map((x) => x.hash));
				while (!allHeads.done()) {
					let next = await allHeads.next(100);
					for (const head of next) {
						if (!allProvidedHeadsHashes.has(head.hash)) {
							await this.remove(head);
						}
					}
				}
			}
		}
	}

	static async fromEntry<T>(
		store: Blocks,
		identity: Identity,
		entryOrHash: string | string[] | Entry<T> | Entry<T>[],
		options: {
			id?: Uint8Array;
			/* length?: number; TODO */
			timeout?: number;
		} & LogOptions<T> = { id: randomBytes(32) },
	): Promise<Log<T>> {
		const log = new Log<T>(options.id && { id: options.id });
		await log.open(store, identity, options);
		await log.join(!Array.isArray(entryOrHash) ? [entryOrHash] : entryOrHash, {
			timeout: options.timeout,
			trim: options.trim,
			verifySignatures: true,
		});
		return log;
	}

	/**
	 * Find heads from a collection of entries.
	 *
	 * Finds entries that are the heads of this collection,
	 * ie. entries that are not referenced by other entries.
	 *
	 * @param {Array<Entry<T>>} entries - Entries to search heads from
	 * @returns {Array<Entry<T>>}
	 */
	static findHeads<T>(entries: Entry<T>[]) {
		const indexReducer = (
			res: { [key: string]: string },
			entry: Entry<any>,
		) => {
			const addToResult = (e: string) => (res[e] = entry.hash);
			entry.meta.next.forEach(addToResult);
			return res;
		};

		const items = entries.reduce(indexReducer, {});
		const exists = (e: Entry<T>) => items[e.hash] === undefined;
		return entries.filter(exists);
	}

	// Find entries that point to another entry that is not in the
	// input array
	static findTails<T>(entries: Entry<T>[]): Entry<T>[] {
		// Reverse index { next -> entry }
		const reverseIndex: { [key: string]: Entry<T>[] } = {};
		// Null index containing entries that have no parents (nexts)
		const nullIndex: Entry<T>[] = [];
		// Hashes for all entries for quick lookups
		const hashes: { [key: string]: boolean } = {};
		// Hashes of all next entries
		let nexts: string[] = [];

		const addToIndex = (e: Entry<T>) => {
			if (e.meta.next.length === 0) {
				nullIndex.push(e);
			}
			const addToReverseIndex = (a: any) => {
				/* istanbul ignore else */
				if (!reverseIndex[a]) reverseIndex[a] = [];
				reverseIndex[a].push(e);
			};

			// Add all entries and their parents to the reverse index
			e.meta.next.forEach(addToReverseIndex);
			// Get all next references
			nexts = nexts.concat(e.meta.next);
			// Get the hashes of input entries
			hashes[e.hash] = true;
		};

		// Create our indices
		entries.forEach(addToIndex);

		const addUniques = (
			res: Entry<T>[],
			entries: Entry<T>[],
			_idx: any,
			_arr: any,
		) => res.concat(findUniques(entries, "hash"));
		const exists = (e: string) => hashes[e] === undefined;
		const findFromReverseIndex = (e: string) => reverseIndex[e];

		// Drop hashes that are not in the input entries
		const tails = nexts // For every hash in nexts:
			.filter(exists) // Remove undefineds and nulls
			.map(findFromReverseIndex) // Get the Entry from the reverse index
			.reduce(addUniques, []) // Flatten the result and take only uniques
			.concat(nullIndex); // Combine with tails the have no next refs (ie. first-in-their-chain)

		return findUniques(tails, "hash").sort(Entry.compare);
	}

	// Find the hashes to entries that are not in a collection
	// but referenced by other entries
	static findTailHashes(entries: Entry<any>[]) {
		const hashes: { [key: string]: boolean } = {};
		const addToIndex = (e: Entry<any>) => (hashes[e.hash] = true);
		const reduceTailHashes = (
			res: string[],
			entry: Entry<any>,
			idx: number,
			arr: Entry<any>[],
		) => {
			const addToResult = (e: string) => {
				/* istanbul ignore else */
				if (hashes[e] === undefined) {
					res.splice(0, 0, e);
				}
			};
			entry.meta.next.reverse().forEach(addToResult);
			return res;
		};

		entries.forEach(addToIndex);
		return entries.reduce(reduceTailHashes, []);
	}
}
