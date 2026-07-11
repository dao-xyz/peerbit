import type { NativeDurabilityCheckpointTransactionState } from "./codec.js";
import {
	NATIVE_DURABILITY_MAX_U64,
	NATIVE_DURABILITY_MAX_WRITER_ID_BYTES,
	type NativeDurabilityLease,
} from "./lease.js";

export const NATIVE_DURABILITY_STORAGE_VERSION = 1 as const;
export const NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES = 1024;

export type NativeDurabilityStorageKind = "memory" | "node-fsync";

export type NativeDurabilityOperationScope = {
	transactionId: string;
	txSequence: bigint;
	recordLsn: bigint;
};

export type NativeDurabilityStagedBlock = {
	ordinal: number;
	cid: string;
	bytes: Uint8Array;
	digest: Uint8Array;
};

export type NativeDurabilityStagedBlockReference = Omit<
	NativeDurabilityStagedBlock,
	"bytes"
> & {
	byteLength: number;
};

export type NativeDurabilityStagingManifest = {
	version: typeof NATIVE_DURABILITY_STORAGE_VERSION;
	scope: NativeDurabilityOperationScope;
	fence: NativeDurabilityLease["fence"];
	blocks: NativeDurabilityStagedBlockReference[];
	manifestDigest: Uint8Array;
};

export type NativeDurabilityStageRequest = {
	scope: NativeDurabilityOperationScope;
	blocks: readonly NativeDurabilityStagedBlock[];
};

export type NativeDurabilityJournalAppendRequest = {
	transactionId: string;
	txSequence: bigint;
	firstRecordLsn: bigint;
	lastRecordLsn: bigint;
	expectedOffset: number;
	frames: Uint8Array;
	framesDigest: Uint8Array;
};

/**
 * The storage adapter asks an injected journal codec to produce this result
 * from the exact bytes it is about to truncate. Complete-frame corruption must
 * throw from the classifier and can never be represented as `incomplete-tail`.
 */
export type NativeDurabilityJournalClassification =
	| {
			kind: "complete";
			validLength: number;
			lastRecordLsn: bigint;
	  }
	| {
			kind: "incomplete-tail";
			validLength: number;
			lastRecordLsn: bigint;
			reason: "short-header" | "short-body" | "short-trailer";
	  };

export interface NativeDurabilityJournalClassifier {
	classify(
		bytes: Uint8Array,
	):
		| NativeDurabilityJournalClassification
		| Promise<NativeDurabilityJournalClassification>;
}

export type NativeDurabilityIncompleteTailReconciliationRequest = {
	transactionId: string;
	txSequence: bigint;
};

export type NativeDurabilityStagingCoverage = {
	transactionId: string;
	txSequence: bigint;
	coveredThroughLsn: bigint;
	stagingManifestDigest: Uint8Array;
};

export type NativeDurabilityCheckpointRequest = {
	scope: NativeDurabilityOperationScope;
	checkpointLsn: bigint;
	txSequenceHighwater: bigint;
	bytes: Uint8Array;
	digest: Uint8Array;
	stagingCoverage: readonly NativeDurabilityStagingCoverage[];
	retainedTransactions: readonly NativeDurabilityCheckpointTransactionState[];
};

export type NativeDurabilityCheckpoint = {
	version: typeof NATIVE_DURABILITY_STORAGE_VERSION;
	generation: bigint;
	checkpointLsn: bigint;
	txSequenceHighwater: bigint;
	bytes: Uint8Array;
	digest: Uint8Array;
	originFence: NativeDurabilityLease["fence"];
	manifestSlot: "a" | "b";
	stagingCoverage: NativeDurabilityStagingCoverage[];
	retainedTransactions: NativeDurabilityCheckpointTransactionState[];
};

export type NativeDurabilityDeleteTarget =
	| { kind: "staging"; transactionId: string }
	| { kind: "checkpoint"; generation: bigint };

export type NativeDurabilityDeleteRequest = {
	scope: NativeDurabilityOperationScope;
	targets: readonly NativeDurabilityDeleteTarget[];
};

type NativeDurabilityReceiptBase<TKind extends string> = {
	version: typeof NATIVE_DURABILITY_STORAGE_VERSION;
	kind: TKind;
	domainId: string;
	fence: NativeDurabilityLease["fence"];
	transactionId: string;
	txSequence: bigint;
	firstRecordLsn: bigint;
	lastRecordLsn: bigint;
	scopeDigest: Uint8Array;
	barrierOrdinal: bigint;
};

export type NativeDurabilityStageReceipt =
	NativeDurabilityReceiptBase<"stage"> & {
		blocks: NativeDurabilityStagedBlockReference[];
		manifestDigest: Uint8Array;
	};

export type NativeDurabilityJournalReceipt =
	NativeDurabilityReceiptBase<"journal-append"> & {
		offset: number;
		endOffset: number;
		framesDigest: Uint8Array;
	};

export type NativeDurabilityJournalReconciliationReceipt =
	NativeDurabilityReceiptBase<"journal-tail-reconciliation"> & {
		previousLength: number;
		validLength: number;
		observedDigest: Uint8Array;
	};

export type NativeDurabilityCheckpointReceipt =
	NativeDurabilityReceiptBase<"checkpoint"> & {
		generation: bigint;
		checkpointLsn: bigint;
		checkpointDigest: Uint8Array;
		manifestSlot: "a" | "b";
		stagingCoverageDigest: Uint8Array;
	};

export type NativeDurabilityDeleteReceipt =
	NativeDurabilityReceiptBase<"delete"> & {
		targets: NativeDurabilityDeleteTarget[];
	};

export type NativeDurabilityStorageStats = {
	kind: NativeDurabilityStorageKind;
	domainId: string;
	strictBarrierCount: bigint;
	strictDeleteCount: bigint;
	journalBytes: number;
	stagingTransactions: number;
	stagedBlocks: number;
	stagedBytes: number;
	checkpointGenerations: number;
	checkpointBytes: number;
};

export interface NativeDurabilityStorage {
	readonly version: typeof NATIVE_DURABILITY_STORAGE_VERSION;
	readonly kind: NativeDurabilityStorageKind;
	readonly domainId: string;
	readonly fence: NativeDurabilityLease["fence"];
	readonly crashSafe: boolean;

	stageAndSync(
		request: NativeDurabilityStageRequest,
	): Promise<NativeDurabilityStageReceipt>;
	readStagingManifest(
		transactionId: string,
	): Promise<NativeDurabilityStagingManifest | undefined>;
	readStagedBlock(
		transactionId: string,
		ordinal: number,
	): Promise<Uint8Array | undefined>;
	listStagingTransactionIds(): Promise<string[]>;

	appendJournalAndSync(
		request: NativeDurabilityJournalAppendRequest,
	): Promise<NativeDurabilityJournalReceipt>;
	readJournal(): Promise<Uint8Array>;
	reconcileIncompleteJournalTailAndSync(
		request: NativeDurabilityIncompleteTailReconciliationRequest,
	): Promise<NativeDurabilityJournalReconciliationReceipt>;

	writeCheckpointAndSync(
		request: NativeDurabilityCheckpointRequest,
	): Promise<NativeDurabilityCheckpointReceipt>;
	readLatestCheckpoint(): Promise<NativeDurabilityCheckpoint | undefined>;

	deleteAndSync(
		request: NativeDurabilityDeleteRequest,
	): Promise<NativeDurabilityDeleteReceipt>;
	stats(): Promise<NativeDurabilityStorageStats>;
	close(): Promise<void>;
}

export class NativeDurabilityStorageUnsupportedError extends Error {
	readonly code = "ERR_NATIVE_DURABILITY_STORAGE_UNSUPPORTED";

	constructor(message: string) {
		super(message);
		this.name = "NativeDurabilityStorageUnsupportedError";
	}
}

export class NativeDurabilityMigrationRequiredError extends Error {
	readonly code = "ERR_NATIVE_DURABILITY_MIGRATION_REQUIRED";

	constructor(readonly directory: string) {
		super(
			`Native crash-safe durability requires an explicit migration for nonempty program directory: ${directory}`,
		);
		this.name = "NativeDurabilityMigrationRequiredError";
	}
}

export class NativeDurabilityStorageClosedError extends Error {
	readonly code = "ERR_NATIVE_DURABILITY_STORAGE_CLOSED";

	constructor() {
		super("Native durability storage is closed");
		this.name = "NativeDurabilityStorageClosedError";
	}
}

export class NativeDurabilityStorageCorruptionError extends Error {
	readonly code = "ERR_NATIVE_DURABILITY_STORAGE_CORRUPTION";

	constructor(
		message: string,
		readonly cause?: unknown,
	) {
		super(message);
		this.name = "NativeDurabilityStorageCorruptionError";
	}
}

export class NativeDurabilityDigestMismatchError extends Error {
	readonly code = "ERR_NATIVE_DURABILITY_DIGEST_MISMATCH";

	constructor(readonly subject: string) {
		super(`Native durability digest mismatch for ${subject}`);
		this.name = "NativeDurabilityDigestMismatchError";
	}
}

export class NativeDurabilityJournalOffsetConflictError extends Error {
	readonly code = "ERR_NATIVE_DURABILITY_JOURNAL_OFFSET_CONFLICT";

	constructor(
		readonly expectedOffset: number,
		readonly actualOffset: number,
	) {
		super(
			`Native durability journal offset conflict: expected ${expectedOffset}, found ${actualOffset}`,
		);
		this.name = "NativeDurabilityJournalOffsetConflictError";
	}
}

export class NativeDurabilityOutcomeUnknownError extends Error {
	readonly code = "ERR_NATIVE_DURABILITY_OUTCOME_UNKNOWN";
	readonly outcome = "unknown" as const;

	constructor(
		readonly operation:
			| "stage"
			| "journal-append"
			| "journal-tail-reconciliation"
			| "checkpoint"
			| "delete",
		readonly transactionId: string,
		readonly txSequence: bigint,
		readonly cause: unknown,
	) {
		super(
			`Native durability ${operation} outcome is unknown for transaction ${transactionId}`,
		);
		this.name = "NativeDurabilityOutcomeUnknownError";
	}
}

export class NativeDurabilityIncompleteTailMismatchError extends Error {
	readonly code = "ERR_NATIVE_DURABILITY_INCOMPLETE_TAIL_MISMATCH";

	constructor(message: string) {
		super(message);
		this.name = "NativeDurabilityIncompleteTailMismatchError";
	}
}

export const copyNativeDurabilityBytes = (bytes: Uint8Array): Uint8Array =>
	new Uint8Array(bytes);

export const nativeDurabilityBytesEqual = (
	left: Uint8Array,
	right: Uint8Array,
): boolean => {
	if (left.byteLength !== right.byteLength) return false;
	for (let i = 0; i < left.byteLength; i++) {
		if (left[i] !== right[i]) return false;
	}
	return true;
};

export const nativeDurabilityDigestHex = (digest: Uint8Array): string =>
	Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");

export const nativeDurabilityDigestFromHex = (value: string): Uint8Array => {
	if (!/^[0-9a-f]{64}$/.test(value)) {
		throw new NativeDurabilityStorageCorruptionError(
			"Native durability digest must be 32 lowercase hexadecimal bytes",
		);
	}
	const bytes = new Uint8Array(32);
	for (let i = 0; i < bytes.byteLength; i++) {
		bytes[i] = Number.parseInt(value.slice(i * 2, i * 2 + 2), 16);
	}
	return bytes;
};

const canonicalValue = (value: unknown): unknown => {
	if (typeof value === "bigint") return { bigint: value.toString() };
	if (value instanceof Uint8Array) {
		return { bytes: nativeDurabilityDigestHex(value) };
	}
	if (Array.isArray(value)) return value.map(canonicalValue);
	if (value && typeof value === "object") {
		return Object.fromEntries(
			Object.entries(value as Record<string, unknown>)
				.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
				.map(([key, entry]) => [key, canonicalValue(entry)]),
		);
	}
	return value;
};

export const encodeNativeDurabilityCanonical = (value: unknown): Uint8Array =>
	new TextEncoder().encode(JSON.stringify(canonicalValue(value)));

export const sha256NativeDurability = async (
	bytes: Uint8Array,
): Promise<Uint8Array> => {
	if (!globalThis.crypto?.subtle) {
		throw new NativeDurabilityStorageUnsupportedError(
			"SHA-256 requires Web Crypto in the memory durability adapter",
		);
	}
	const input = new Uint8Array(bytes.byteLength);
	input.set(bytes);
	return new Uint8Array(
		await globalThis.crypto.subtle.digest("SHA-256", input.buffer),
	);
};

export const assertNativeDurabilityDigest = async (
	subject: string,
	bytes: Uint8Array,
	expected: Uint8Array,
): Promise<void> => {
	if (expected.byteLength !== 32) {
		throw new NativeDurabilityDigestMismatchError(subject);
	}
	const actual = await sha256NativeDurability(bytes);
	if (!nativeDurabilityBytesEqual(actual, expected)) {
		throw new NativeDurabilityDigestMismatchError(subject);
	}
};

export const nativeDurabilityScopeDigest = async (
	value: unknown,
): Promise<Uint8Array> =>
	sha256NativeDurability(encodeNativeDurabilityCanonical(value));

const assertU64 = (name: string, value: bigint): void => {
	if (
		typeof value !== "bigint" ||
		value < 0n ||
		value > NATIVE_DURABILITY_MAX_U64
	) {
		throw new TypeError(`${name} must be an unsigned 64-bit bigint`);
	}
};

export const assertNativeDurabilityOperationScope = (
	scope: NativeDurabilityOperationScope,
): void => {
	if (
		!scope ||
		typeof scope.transactionId !== "string" ||
		!scope.transactionId
	) {
		throw new TypeError("transactionId must be a non-empty string");
	}
	if (
		new TextEncoder().encode(scope.transactionId).byteLength >
		NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES
	) {
		throw new TypeError("transactionId exceeds the journal format limit");
	}
	assertU64("txSequence", scope.txSequence);
	assertU64("recordLsn", scope.recordLsn);
};

export const assertNativeDurabilityFence = (
	fence: NativeDurabilityLease["fence"],
): void => {
	if (
		!fence ||
		typeof fence.ownerId !== "string" ||
		!fence.ownerId ||
		typeof fence.domainId !== "string" ||
		!fence.domainId
	) {
		throw new TypeError("Durability fence ownerId/domainId must be non-empty");
	}
	if (
		new TextEncoder().encode(fence.ownerId).byteLength >
			NATIVE_DURABILITY_MAX_WRITER_ID_BYTES ||
		new TextEncoder().encode(fence.domainId).byteLength >
			NATIVE_DURABILITY_MAX_WRITER_ID_BYTES
	) {
		throw new TypeError(
			"Durability fence ownerId/domainId exceeds format limit",
		);
	}
	assertU64("fence.epoch", fence.epoch);
	if (fence.epoch === 0n) {
		throw new TypeError("Durability fence epoch must be non-zero");
	}
};

export const assertNativeDurabilityStageRequest = (
	request: NativeDurabilityStageRequest,
): void => {
	assertNativeDurabilityOperationScope(request?.scope);
	if (request.scope.txSequence === 0n || request.scope.recordLsn === 0n) {
		throw new RangeError("Staging txSequence and recordLsn must be non-zero");
	}
	if (!Array.isArray(request.blocks)) {
		throw new TypeError("Staging blocks must be an array");
	}
	const sorted = [...request.blocks].sort(
		(left, right) => left.ordinal - right.ordinal,
	);
	for (let index = 0; index < sorted.length; index++) {
		const block = sorted[index];
		if (block.ordinal !== index) {
			throw new RangeError("Staging ordinals must be contiguous from zero");
		}
		if (typeof block.cid !== "string" || !block.cid) {
			throw new TypeError(`Staging block ${index} has an invalid CID`);
		}
		if (!(block.bytes instanceof Uint8Array)) {
			throw new TypeError(`Staging block ${index} bytes must be Uint8Array`);
		}
		if (
			!(block.digest instanceof Uint8Array) ||
			block.digest.byteLength !== 32
		) {
			throw new TypeError(`Staging block ${index} digest must be 32 bytes`);
		}
	}
};

export const assertNativeDurabilityJournalAppendRequest = (
	request: NativeDurabilityJournalAppendRequest,
): void => {
	if (
		!request ||
		typeof request.transactionId !== "string" ||
		!request.transactionId
	) {
		throw new TypeError("transactionId must be a non-empty string");
	}
	if (
		new TextEncoder().encode(request.transactionId).byteLength >
		NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES
	) {
		throw new TypeError("transactionId exceeds the journal format limit");
	}
	assertU64("txSequence", request.txSequence);
	assertU64("firstRecordLsn", request.firstRecordLsn);
	assertU64("lastRecordLsn", request.lastRecordLsn);
	if (
		request.txSequence === 0n ||
		request.firstRecordLsn === 0n ||
		request.lastRecordLsn === 0n
	) {
		throw new RangeError("Journal sequence and LSN values must be non-zero");
	}
	if (request.lastRecordLsn < request.firstRecordLsn) {
		throw new RangeError("lastRecordLsn must not precede firstRecordLsn");
	}
	if (
		!Number.isSafeInteger(request.expectedOffset) ||
		request.expectedOffset < 0
	) {
		throw new RangeError("expectedOffset must be a non-negative safe integer");
	}
	if (
		!(request.frames instanceof Uint8Array) ||
		request.frames.byteLength === 0
	) {
		throw new TypeError("Journal frames must be a non-empty Uint8Array");
	}
	if (
		!(request.framesDigest instanceof Uint8Array) ||
		request.framesDigest.byteLength !== 32
	) {
		throw new TypeError("framesDigest must be 32 bytes");
	}
};

export const assertNativeDurabilityCheckpointRequest = (
	request: NativeDurabilityCheckpointRequest,
): void => {
	assertNativeDurabilityOperationScope(request?.scope);
	assertU64("checkpointLsn", request.checkpointLsn);
	assertU64("txSequenceHighwater", request.txSequenceHighwater);
	if (
		request.scope.txSequence === 0n ||
		request.scope.recordLsn === 0n ||
		request.scope.recordLsn > request.checkpointLsn ||
		request.scope.txSequence > request.txSequenceHighwater
	) {
		throw new RangeError(
			"Checkpoint scope cannot exceed its LSN or transaction highwater",
		);
	}
	if (!(request.bytes instanceof Uint8Array)) {
		throw new TypeError("Checkpoint bytes must be Uint8Array");
	}
	if (
		!(request.digest instanceof Uint8Array) ||
		request.digest.byteLength !== 32
	) {
		throw new TypeError("Checkpoint digest must be 32 bytes");
	}
	if (!Array.isArray(request.stagingCoverage)) {
		throw new TypeError("Checkpoint stagingCoverage must be an array");
	}
	const transactionIds = new Set<string>();
	for (const coverage of request.stagingCoverage) {
		if (
			typeof coverage.transactionId !== "string" ||
			!coverage.transactionId ||
			transactionIds.has(coverage.transactionId)
		) {
			throw new TypeError(
				"Checkpoint staging coverage transaction IDs must be unique",
			);
		}
		transactionIds.add(coverage.transactionId);
		assertU64("coverage.txSequence", coverage.txSequence);
		assertU64("coverage.coveredThroughLsn", coverage.coveredThroughLsn);
		if (coverage.coveredThroughLsn > request.checkpointLsn) {
			throw new RangeError(
				"Staging coverage cannot extend beyond checkpointLsn",
			);
		}
		if (
			new TextEncoder().encode(coverage.transactionId).byteLength >
				NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES ||
			!(coverage.stagingManifestDigest instanceof Uint8Array) ||
			coverage.stagingManifestDigest.byteLength !== 32
		) {
			throw new TypeError(
				"Checkpoint staging coverage fields exceed their format limits",
			);
		}
	}
	if (!Array.isArray(request.retainedTransactions)) {
		throw new TypeError("Checkpoint retainedTransactions must be an array");
	}
	const retainedIds = new Set<string>();
	const retainedSequences = new Set<bigint>();
	for (const retained of request.retainedTransactions) {
		assertU64("retained transaction sequence", retained.txSequence);
		if (
			retained.txSequence === 0n ||
			retained.txSequence > request.txSequenceHighwater ||
			typeof retained.transactionId !== "string" ||
			!retained.transactionId ||
			new TextEncoder().encode(retained.transactionId).byteLength >
				NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES ||
			retainedIds.has(retained.transactionId) ||
			retainedSequences.has(retained.txSequence) ||
			!Number.isInteger(retained.phase) ||
			retained.phase < 1 ||
			retained.phase > 6 ||
			!Number.isInteger(retained.operationKind) ||
			retained.operationKind < 1 ||
			retained.operationKind > 5 ||
			!(retained.planDigest instanceof Uint8Array) ||
			retained.planDigest.byteLength !== 32
		) {
			throw new TypeError("Checkpoint retained transaction is invalid");
		}
		retainedIds.add(retained.transactionId);
		retainedSequences.add(retained.txSequence);
	}
	for (const coverage of request.stagingCoverage) {
		const retained = request.retainedTransactions.find(
			(candidate) => candidate.transactionId === coverage.transactionId,
		);
		if (
			!retained ||
			retained.txSequence !== coverage.txSequence ||
			retained.phase !== 6
		) {
			throw new TypeError(
				"Checkpoint staging coverage requires the exact retained CLEAN transaction",
			);
		}
	}
};

export const assertNativeDurabilityDeleteRequest = (
	request: NativeDurabilityDeleteRequest,
): void => {
	assertNativeDurabilityOperationScope(request?.scope);
	if (!Array.isArray(request.targets) || request.targets.length === 0) {
		throw new TypeError("Delete targets must be a non-empty array");
	}
	if (request.scope.txSequence === 0n || request.scope.recordLsn === 0n) {
		throw new RangeError("Delete scope sequence and LSN must be non-zero");
	}
	const targetKeys = new Set<string>();
	for (const target of request.targets) {
		if (target.kind === "staging") {
			if (
				typeof target.transactionId !== "string" ||
				!target.transactionId ||
				new TextEncoder().encode(target.transactionId).byteLength >
					NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES
			) {
				throw new TypeError("Staging delete transactionId must be non-empty");
			}
		} else if (target.kind === "checkpoint") {
			assertU64("checkpoint generation", target.generation);
		} else {
			throw new TypeError("Unknown native durability delete target");
		}
		const key =
			target.kind === "staging"
				? `staging:${target.transactionId}`
				: `checkpoint:${target.generation}`;
		if (targetKeys.has(key)) {
			throw new TypeError("Delete targets must be unique");
		}
		targetKeys.add(key);
	}
};
