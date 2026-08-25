import { fromBase64, sha256Sync, toBase64, toHexString } from "@peerbit/crypto";
import deterministicStringify from "json-stringify-deterministic";
import { captureBoundedUint8Array } from "./bounded-bytes.js";
import {
	CUSTODY_HANDOFF_PROFILE_ID,
	CUSTODY_HANDOFF_PROFILE_MASK,
	decodeCustodyHandoffManifestV1,
	decodeCustodyHandoffReceiptV1,
} from "./custody-handoff-codec.js";
import { MAX_U64 } from "./integers.js";

export type CustodyStoreDurability = "strict" | "memory";
export type CustodyRecordSlot = "a" | "b";
export type CustodyRecordRole = "source" | "destination";

export type CustodyRecordCatalogCursor = Readonly<{
	mutationSequence: bigint;
	moveKey: string;
}>;

declare const custodyRecordCatalogFenceBrand: unique symbol;

/** Opaque adapter-local upper fence for a passive catalog scan. */
export type CustodyRecordCatalogFence = Readonly<{
	catalogEpoch: string;
	upperMutationSequence: bigint;
	readonly [custodyRecordCatalogFenceBrand]: true;
}>;

export type CustodyRecordCatalogStatus = Readonly<{
	catalogEpoch: string;
	lastMutationSequence: bigint;
	migrationState: "building" | "ready";
	migrationAfter?: string;
}>;

/** Passive discovery hint. It is not custody, pin, receipt, or prune proof. */
export type CustodyRecordCatalogCandidate = Readonly<{
	catalogEpoch: string;
	mutationSequence: bigint;
	moveKey: string;
	recordSequence: bigint;
	slot: CustodyRecordSlot;
	state: CustodyRecordState;
	entryHash?: string;
	handoffId?: string;
	frameChecksum: string;
	domainId: string;
	writerEpoch: bigint;
	writerOwner: string;
}>;

export type CustodyRecordCatalogPage = Readonly<{
	fence: CustodyRecordCatalogFence;
	candidates: readonly CustodyRecordCatalogCandidate[];
	next?: CustodyRecordCatalogCursor;
}>;

export type CustodyRecordCatalogCandidateRead =
	| Readonly<{
			status: "current";
			candidate: CustodyRecordCatalogCandidate;
			frame: Uint8Array;
	  }>
	| Readonly<{ status: "changed" }>;

export type CustodyRecordCatalogMigrationPage = Readonly<{
	migrationState: "building" | "ready";
	migrationAfter?: string;
	processed: number;
}>;

/**
 * Exact semantic namespace binding for a custody record store.
 *
 * The byte strings are the canonical log identifier and serialized Peerbit
 * public key. A production persistence factory must always provide this
 * binding; the optional form on `open()` exists only for the disconnected
 * memory/reducer model.
 */
export type CustodyRecordBinding = Readonly<{
	logId: Uint8Array;
	localPublicKey: Uint8Array;
	role: CustodyRecordRole;
}>;

/**
 * Logical per-record A/B persistence used by the disconnected custody model.
 *
 * One adapter instance, and the complete namespace behind it, must be owned by
 * one store for the store's lifetime. The Node SQLite adapter supplies the
 * cross-process keyed lease/fence; disconnected tests may use bounded memory
 * adapters. The optional Node catalog is strictly paged and passive; the
 * interface deliberately has no unbounded list, delete, drop, or release.
 */
export interface CustodyRecordPersistence {
	/** Reject an oversized value before allocating or returning its contents. */
	read(
		moveKey: string,
		slot: CustodyRecordSlot,
		maxBytes: number,
	): Promise<Uint8Array | undefined>;
	/** Atomically replace one complete logical slot; never append. */
	write(
		moveKey: string,
		slot: CustodyRecordSlot,
		bytes: Uint8Array,
	): Promise<void>;
	/**
	 * Persist the exact replacement and namespace metadata needed to recover it
	 * after a crash. Strict stores require this operation.
	 */
	durableBarrier?(moveKey: string, slot: CustodyRecordSlot): Promise<void>;
	/** Passive bounded catalog APIs; only the strict Node adapter implements them. */
	readCatalogStatus?(): Promise<CustodyRecordCatalogStatus>;
	captureCatalogFence?(): Promise<CustodyRecordCatalogFence>;
	scanRecoveryPage?(input: {
		fence: CustodyRecordCatalogFence;
		state: CustodyRecordState;
		after?: CustodyRecordCatalogCursor;
		maxRows?: number;
		maxBytes?: number;
	}): Promise<CustodyRecordCatalogPage>;
	scanEntryPinsPage?(input: {
		fence: CustodyRecordCatalogFence;
		entryHash: string;
		after?: CustodyRecordCatalogCursor;
		maxRows?: number;
		maxBytes?: number;
	}): Promise<CustodyRecordCatalogPage>;
	readCatalogCandidate?(
		candidate: CustodyRecordCatalogCandidate,
		maxBytes?: number,
	): Promise<CustodyRecordCatalogCandidateRead>;
	migrateCatalogPage?(input?: {
		maxMoveKeys?: number;
		maxBytes?: number;
	}): Promise<CustodyRecordCatalogMigrationPage>;
	/** Internal health signal used to revoke store authority after ambiguity. */
	isPoisoned?(): boolean;
	close?(options?: { flush?: false }): Promise<void>;
}

export type CustodyStoreLimits = Readonly<{
	maxArtifactBytes: number;
	maxFrameBytes: number;
	maxPendingOperations: number;
}>;

/** Hard ceilings may only be lowered by a caller. */
export const DEFAULT_CUSTODY_STORE_LIMITS: CustodyStoreLimits = Object.freeze({
	maxArtifactBytes: 4 * 1024,
	maxFrameBytes: 16 * 1024,
	maxPendingOperations: 64,
});

export const CUSTODY_RECORD_SLOTS = Object.freeze([
	"a",
	"b",
] as const satisfies readonly CustodyRecordSlot[]);

export type CustodyRecordState =
	| "absent"
	| "source-prepared"
	| "source-receipt-durable"
	| "destination-collecting"
	| "destination-pinned"
	| "destination-receipted";

export type CustodyDestinationPinFacts = Readonly<{
	moveKey: string;
	handoffId: string;
	custodyProfileId: string;
	custodyProfileMask: number;
	custodyEpoch: string;
	pinSequence: bigint;
	compositeCommitId: string;
}>;

declare const custodyDestinationPinEvidenceBrand: unique symbol;

/**
 * Opaque evidence issued only after an external composite block+row+pin
 * durability barrier. The model persists copied bound facts, never this object.
 */
export type CustodyDestinationPinEvidence = Readonly<{
	readonly [custodyDestinationPinEvidenceBrand]: true;
}>;

/**
 * Bounded diagnostic snapshot of one logical record. Artifacts are canonical
 * base64 strings so callers cannot mutate bytes retained by the store.
 */
export type CustodyRecordSnapshot = Readonly<{
	moveKey: string;
	revision: bigint;
	/** Memory snapshots are reducer simulations and never durable custody facts. */
	durability: CustodyStoreDurability;
	state: CustodyRecordState;
	manifest?: string;
	receipt?: string;
	pin?: CustodyDestinationPinFacts;
}>;

declare const durableCustodyRecordCommitBrand: unique symbol;

/**
 * Evidence that this exact strict-store record frame passed its physical
 * barrier. It is not placement consensus, custody transfer, or prune authority.
 */
export type DurableCustodyRecordCommit = Readonly<{
	moveKey: string;
	revision: bigint;
	state: CustodyRecordState;
	frameChecksum: string;
	readonly [durableCustodyRecordCommitBrand]: true;
}>;

declare const durableCustodySourceReceiptCommitBrand: unique symbol;
declare const custodySourceReceiptAuthorityBrand: unique symbol;

/**
 * Opaque opened-store pairing generation. Closing revokes new pairings, while
 * an already-paired work store may consume its immutable terminal proof.
 */
export type CustodySourceReceiptAuthority = Readonly<{
	readonly [custodySourceReceiptAuthorityBrand]: true;
}>;

/**
 * Opaque proof issued only for an exact strict source-receipt-durable frame.
 * The work cursor consumes its registered facts; callers cannot manufacture a
 * structurally similar object to advance a pending handoff.
 */
export type DurableCustodySourceReceiptCommit = DurableCustodyRecordCommit &
	Readonly<{
		state: "source-receipt-durable";
		handoffId: string;
		receiptId: string;
		readonly [durableCustodySourceReceiptCommitBrand]: true;
	}>;

export type DurableCustodySourceReceiptFacts = Readonly<{
	moveKey: string;
	handoffId: string;
	receiptId: string;
}>;

type RegisteredCustodySourceReceiptFacts = DurableCustodySourceReceiptFacts &
	Readonly<{ authority: CustodySourceReceiptAuthority }>;

export type CustodyRecordReadResult = Readonly<{
	snapshot: CustodyRecordSnapshot;
	/** Present only for an explicit strict frame confirmed by its barrier. */
	durableCommit?: DurableCustodyRecordCommit;
}>;

export type CustodySourceReceiptReadResult = Readonly<{
	snapshot: CustodyRecordSnapshot;
	/** Present only for the exact strict source-receipt-durable frame. */
	durableCommit?: DurableCustodySourceReceiptCommit;
}>;

type StoredFramePayload = {
	format: typeof CUSTODY_RECORD_FORMAT;
	version: 1;
	durability: CustodyStoreDurability;
	moveKey: string;
	sequence: string;
	state: CustodyRecordState;
	manifest: string | null;
	receipt: string | null;
	pin: StoredPinFacts | null;
};

type StoredPinFacts = {
	moveKey: string;
	handoffId: string;
	custodyProfileId: string;
	custodyProfileMask: number;
	custodyEpoch: string;
	pinSequence: string;
	compositeCommitId: string;
};

type LoadedFrame = {
	moveKey: string;
	sequence: bigint;
	slot: CustodyRecordSlot;
	durability?: CustodyStoreDurability;
	state: CustodyRecordState;
	manifest?: string;
	receipt?: string;
	pin?: StoredPinFacts;
	handoffId?: string;
	entryHash?: string;
	receiptId?: string;
	checksum?: string;
	durabilityConfirmed?: boolean;
	implicit?: boolean;
};

type CapturedTarget = Readonly<{
	moveKey: string;
	state: Exclude<CustodyRecordState, "absent">;
	manifest: string;
	receipt?: string;
	pin?: StoredPinFacts;
	handoffId: string;
	receiptId?: string;
}>;

type CapturedCustodyRecordBinding = Readonly<{
	logId: string;
	localPublicKey: string;
	role: CustodyRecordRole;
}>;

const CUSTODY_RECORD_FORMAT = "peerbit-shared-log-custody-record" as const;
const MOVE_KEY_PATTERN = /^[0-9a-f]{64}$/;
const CHECKSUM_PATTERN = /^[0-9a-f]{64}$/;
const DECIMAL_PATTERN = /^(0|[1-9][0-9]*)$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const openPersistenceAdapters = new WeakSet<CustodyRecordPersistence>();
const destinationPinEvidenceFacts = new WeakMap<
	CustodyDestinationPinEvidence,
	StoredPinFacts
>();
const sourceReceiptCommitFacts = new WeakMap<
	DurableCustodySourceReceiptCommit,
	RegisteredCustodySourceReceiptFacts
>();
const sourceReceiptAuthorities = new WeakMap<
	CustodySourceReceiptAuthority,
	Readonly<{ logId: string; localPublicKey: string }>
>();

/** @internal Reject a structurally forged custody-store authority. */
export const assertCustodySourceReceiptAuthority = (
	authority: CustodySourceReceiptAuthority,
) => {
	if (
		!authority ||
		typeof authority !== "object" ||
		!sourceReceiptAuthorities.has(authority)
	) {
		throw new Error("Invalid custody source receipt authority");
	}
	return authority;
};

/** @internal Fail before opening a work namespace for a different log. */
export const assertCustodySourceReceiptAuthorityLogId = (
	authority: CustodySourceReceiptAuthority,
	logIdValue: Uint8Array,
) => {
	const facts = sourceReceiptAuthorities.get(
		assertCustodySourceReceiptAuthority(authority),
	);
	const logId = captureBoundedUint8Array(
		logIdValue,
		1,
		512,
		"custody authority log id",
	);
	if (!facts || facts.logId !== toHexString(logId)) {
		throw new Error("Custody source receipt authority log mismatch");
	}
	return authority;
};

/** @internal Bind a staged manifest to the authority's exact log and key. */
export const assertCustodySourceReceiptAuthorityManifest = (
	authority: CustodySourceReceiptAuthority,
	manifest: Readonly<{
		logId: string;
		source: Readonly<{ publicKey: string }>;
	}>,
) => {
	const facts = sourceReceiptAuthorities.get(
		assertCustodySourceReceiptAuthority(authority),
	);
	if (
		!facts ||
		manifest?.logId !== facts.logId ||
		manifest?.source?.publicKey !== facts.localPublicKey
	) {
		throw new Error("Custody manifest does not match its source authority");
	}
	return authority;
};

/** @internal Validate and copy facts from a commit issued by this module. */
export const getDurableCustodySourceReceiptFacts = (
	commit: DurableCustodySourceReceiptCommit,
): DurableCustodySourceReceiptFacts => {
	if (!commit || typeof commit !== "object") {
		throw new Error("Invalid durable custody source receipt commit");
	}
	const facts = sourceReceiptCommitFacts.get(commit);
	if (!facts) {
		throw new Error("Invalid durable custody source receipt commit");
	}
	return Object.freeze({
		moveKey: facts.moveKey,
		handoffId: facts.handoffId,
		receiptId: facts.receiptId,
	});
};

/** @internal Verify an issued proof belongs to one exact paired generation. */
export const verifyDurableCustodySourceReceipt = (
	commit: DurableCustodySourceReceiptCommit,
	authority: CustodySourceReceiptAuthority,
): DurableCustodySourceReceiptFacts => {
	if (!commit || typeof commit !== "object") {
		throw new Error("Invalid durable custody source receipt commit");
	}
	const facts = sourceReceiptCommitFacts.get(commit);
	if (!facts || facts.authority !== authority) {
		throw new Error("Invalid durable custody source receipt commit authority");
	}
	return Object.freeze({
		moveKey: facts.moveKey,
		handoffId: facts.handoffId,
		receiptId: facts.receiptId,
	});
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
	value != null && typeof value === "object" && !Array.isArray(value);

const captureRecordBinding = (
	value: CustodyRecordBinding | undefined,
): CapturedCustodyRecordBinding | undefined => {
	if (value === undefined) return undefined;
	const logId = value.logId;
	const localPublicKey = value.localPublicKey;
	const role = value.role;
	if (role !== "source" && role !== "destination") {
		throw new Error("Invalid custody record binding");
	}
	let capturedLogId: Uint8Array;
	let capturedLocalPublicKey: Uint8Array;
	try {
		capturedLogId = captureBoundedUint8Array(
			logId,
			1,
			512,
			"custody record binding log id",
		);
		capturedLocalPublicKey = captureBoundedUint8Array(
			localPublicKey,
			1,
			512,
			"custody record binding public key",
		);
	} catch (error) {
		throw new Error("Invalid custody record binding", { cause: error });
	}
	return Object.freeze({
		logId: toHexString(capturedLogId),
		localPublicKey: toHexString(capturedLocalPublicKey),
		role,
	});
};

const hasExactKeys = (
	value: Record<string, unknown>,
	keys: readonly string[],
) => {
	const actual = Object.keys(value).sort();
	const expected = [...keys].sort();
	return (
		actual.length === expected.length &&
		actual.every((key, index) => key === expected[index])
	);
};

const assertPositiveSafeLimit = (
	value: unknown,
	name: string,
	hardMaximum: number,
) => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value <= 0 ||
		value > hardMaximum
	) {
		throw new Error(`Invalid ${name}`);
	}
	return value;
};

const normalizeLimits = (
	limits?: Partial<CustodyStoreLimits>,
): CustodyStoreLimits =>
	Object.freeze({
		maxArtifactBytes: assertPositiveSafeLimit(
			limits?.maxArtifactBytes ?? DEFAULT_CUSTODY_STORE_LIMITS.maxArtifactBytes,
			"custody artifact byte bound",
			DEFAULT_CUSTODY_STORE_LIMITS.maxArtifactBytes,
		),
		maxFrameBytes: assertPositiveSafeLimit(
			limits?.maxFrameBytes ?? DEFAULT_CUSTODY_STORE_LIMITS.maxFrameBytes,
			"custody frame byte bound",
			DEFAULT_CUSTODY_STORE_LIMITS.maxFrameBytes,
		),
		maxPendingOperations: assertPositiveSafeLimit(
			limits?.maxPendingOperations ??
				DEFAULT_CUSTODY_STORE_LIMITS.maxPendingOperations,
			"custody pending-operation bound",
			DEFAULT_CUSTODY_STORE_LIMITS.maxPendingOperations,
		),
	});

const assertMoveKey = (value: unknown, name = "custody move key") => {
	if (
		typeof value !== "string" ||
		value.length !== 64 ||
		!MOVE_KEY_PATTERN.test(value)
	) {
		throw new Error(`Invalid ${name}`);
	}
	return value;
};

const assertFixedDigest = (value: unknown, name: string) =>
	assertMoveKey(value, name);

const assertNonZeroDigest = (value: unknown, name: string) => {
	const digest = assertFixedDigest(value, name);
	if (/^0{64}$/.test(digest)) throw new Error(`Invalid ${name}`);
	return digest;
};

const assertProfileMask = (value: unknown) => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value < 0 ||
		value > 0xffffffff
	) {
		throw new Error("Invalid custody profile mask");
	}
	return value;
};

const assertPinSequence = (value: unknown) => {
	if (typeof value !== "bigint" || value <= 0n || value > MAX_U64) {
		throw new Error("Invalid custody pin sequence");
	}
	return value;
};

const captureStoredPinFacts = (
	value: CustodyDestinationPinFacts,
): StoredPinFacts =>
	Object.freeze({
		moveKey: assertMoveKey(value.moveKey),
		handoffId: assertFixedDigest(value.handoffId, "custody handoff id"),
		custodyProfileId: assertFixedDigest(
			value.custodyProfileId,
			"custody profile id",
		),
		custodyProfileMask: assertProfileMask(value.custodyProfileMask),
		custodyEpoch: assertNonZeroDigest(value.custodyEpoch, "custody epoch"),
		pinSequence: assertPinSequence(value.pinSequence).toString(),
		compositeCommitId: assertNonZeroDigest(
			value.compositeCommitId,
			"custody composite commit id",
		),
	});

const runtimePinFacts = (value: StoredPinFacts): CustodyDestinationPinFacts =>
	Object.freeze({
		moveKey: value.moveKey,
		handoffId: value.handoffId,
		custodyProfileId: value.custodyProfileId,
		custodyProfileMask: value.custodyProfileMask,
		custodyEpoch: value.custodyEpoch,
		pinSequence: BigInt(value.pinSequence),
		compositeCommitId: value.compositeCommitId,
	});

/**
 * @internal Test seam for the disconnected model. Production code must issue
 * evidence from the component that owns the composite block+row+pin barrier.
 */
export const issueCustodyDestinationPinEvidenceForTest = (
	facts: CustodyDestinationPinFacts,
): CustodyDestinationPinEvidence => {
	const captured = captureStoredPinFacts(facts);
	const evidence = Object.freeze({}) as CustodyDestinationPinEvidence;
	destinationPinEvidenceFacts.set(evidence, captured);
	return evidence;
};

const parseSequence = (value: unknown) => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > 20 ||
		!DECIMAL_PATTERN.test(value)
	) {
		throw new Error("Invalid custody record sequence");
	}
	const parsed = BigInt(value);
	if (parsed <= 0n || parsed > MAX_U64) {
		throw new Error("Invalid custody record sequence");
	}
	return parsed;
};

const assertExpectedRevision = (value: unknown) => {
	if (typeof value !== "bigint" || value < 0n || value > MAX_U64) {
		throw new Error("Invalid expected custody revision");
	}
	return value;
};

const captureArtifact = (
	value: Uint8Array,
	name: string,
	limits: CustodyStoreLimits,
) =>
	captureBoundedUint8Array(
		value,
		1,
		limits.maxArtifactBytes,
		`${name} within configured byte bound`,
	);

const encodeArtifact = (bytes: Uint8Array) => toBase64(bytes);

const decodeArtifact = (
	value: unknown,
	name: string,
	limits: CustodyStoreLimits,
) => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > Math.ceil((limits.maxArtifactBytes * 4) / 3) + 4
	) {
		throw new Error(`Invalid ${name}`);
	}
	let bytes: Uint8Array;
	try {
		bytes = fromBase64(value);
	} catch (error) {
		throw new Error(`Invalid ${name}`, { cause: error });
	}
	if (
		bytes.byteLength === 0 ||
		bytes.byteLength > limits.maxArtifactBytes ||
		toBase64(bytes) !== value
	) {
		throw new Error(`Invalid ${name}`);
	}
	return bytes;
};

const decodedString = (decoded: unknown, field: string, name: string) => {
	if (!isRecord(decoded)) throw new Error(`Invalid decoded ${name}`);
	const direct = decoded[field];
	if (typeof direct === "string") return direct;
	const body = decoded.body;
	if (isRecord(body) && typeof body[field] === "string") return body[field];
	throw new Error(`Decoded ${name} is missing ${field}`);
};

const decodeAndValidateArtifacts = async (
	manifestBytes: Uint8Array,
	receiptBytes: Uint8Array | undefined,
) => {
	const manifest = await decodeCustodyHandoffManifestV1(manifestBytes);
	const moveKey = assertMoveKey(
		decodedString(manifest, "moveKey", "custody manifest"),
		"custody manifest move key",
	);
	const handoffId = assertMoveKey(
		decodedString(manifest, "handoffId", "custody manifest"),
		"custody manifest handoff id",
	);
	let receipt:
		| Awaited<ReturnType<typeof decodeCustodyHandoffReceiptV1>>
		| undefined;
	if (receiptBytes) {
		receipt = await decodeCustodyHandoffReceiptV1(receiptBytes, manifest);
		if (
			assertMoveKey(
				decodedString(receipt, "moveKey", "custody receipt"),
				"custody receipt move key",
			) !== moveKey ||
			assertMoveKey(
				decodedString(receipt, "handoffId", "custody receipt"),
				"custody receipt handoff id",
			) !== handoffId
		) {
			throw new Error("Custody receipt does not match manifest");
		}
	}
	return {
		moveKey,
		handoffId,
		manifest,
		receipt,
		receiptId: receipt?.receiptId,
	};
};

const assertStateMatchesRecordBinding = (
	state: CustodyRecordState,
	binding: CapturedCustodyRecordBinding | undefined,
) => {
	if (binding === undefined || state === "absent") return;
	const sourceState =
		state === "source-prepared" || state === "source-receipt-durable";
	if (
		(binding.role === "source" && !sourceState) ||
		(binding.role === "destination" && sourceState)
	) {
		throw new Error("Custody record state does not match its namespace role");
	}
};

const assertArtifactsMatchRecordBinding = (
	decoded: Awaited<ReturnType<typeof decodeAndValidateArtifacts>>,
	binding: CapturedCustodyRecordBinding | undefined,
) => {
	if (binding === undefined) return;
	const manifest = decoded.manifest as unknown;
	if (
		!isRecord(manifest) ||
		manifest.logId !== binding.logId ||
		manifest.custodyProfileId !== CUSTODY_HANDOFF_PROFILE_ID ||
		manifest.custodyProfileMask !== CUSTODY_HANDOFF_PROFILE_MASK
	) {
		throw new Error("Custody manifest does not match its namespace log");
	}
	const identity = manifest[binding.role];
	if (!isRecord(identity) || identity.publicKey !== binding.localPublicKey) {
		throw new Error("Custody manifest does not match its namespace identity");
	}
};

const stateHasReceipt = (state: CustodyRecordState) =>
	state === "source-receipt-durable" || state === "destination-receipted";

const stateHasPin = (state: CustodyRecordState) =>
	state === "destination-pinned" || state === "destination-receipted";

const revisionForState = (state: CustodyRecordState) => {
	if (state === "absent") return 1n;
	if (state === "source-prepared" || state === "destination-collecting") {
		return 2n;
	}
	if (state === "source-receipt-durable" || state === "destination-pinned") {
		return 3n;
	}
	return 4n;
};

const validateStoredPin = (value: unknown): StoredPinFacts => {
	if (
		!isRecord(value) ||
		!hasExactKeys(value, [
			"moveKey",
			"handoffId",
			"custodyProfileId",
			"custodyProfileMask",
			"custodyEpoch",
			"pinSequence",
			"compositeCommitId",
		])
	) {
		throw new Error("Invalid stored custody pin facts");
	}
	const pinSequence = parseSequence(value.pinSequence);
	return {
		moveKey: assertMoveKey(value.moveKey),
		handoffId: assertFixedDigest(value.handoffId, "custody handoff id"),
		custodyProfileId: assertFixedDigest(
			value.custodyProfileId,
			"custody profile id",
		),
		custodyProfileMask: assertProfileMask(value.custodyProfileMask),
		custodyEpoch: assertNonZeroDigest(value.custodyEpoch, "custody epoch"),
		pinSequence: pinSequence.toString(),
		compositeCommitId: assertNonZeroDigest(
			value.compositeCommitId,
			"custody composite commit id",
		),
	};
};

const assertPinMatchesManifest = (
	pin: StoredPinFacts,
	decoded: Awaited<ReturnType<typeof decodeAndValidateArtifacts>>,
) => {
	const manifest = decoded.manifest as unknown;
	if (
		pin.moveKey !== decoded.moveKey ||
		pin.handoffId !== decoded.handoffId ||
		pin.custodyProfileId !==
			decodedString(manifest, "custodyProfileId", "custody manifest") ||
		!isRecord(manifest) ||
		pin.custodyProfileMask !== manifest.custodyProfileMask
	) {
		throw new Error("Custody pin evidence does not match manifest");
	}
};

const assertReceiptMatchesPin = (
	pin: StoredPinFacts,
	decoded: Awaited<ReturnType<typeof decodeAndValidateArtifacts>>,
) => {
	const receipt = decoded.receipt;
	if (
		!isRecord(receipt) ||
		receipt.custodyEpoch !== pin.custodyEpoch ||
		receipt.pinSequence !== BigInt(pin.pinSequence) ||
		receipt.custodyProfileId !== pin.custodyProfileId ||
		receipt.custodyProfileMask !== pin.custodyProfileMask
	) {
		throw new Error("Custody receipt does not match durable pin facts");
	}
};

const validateStoredArtifacts = async (
	payload: StoredFramePayload,
	limits: CustodyStoreLimits,
	binding: CapturedCustodyRecordBinding | undefined,
) => {
	assertStateMatchesRecordBinding(payload.state, binding);
	if (payload.state === "absent") {
		if (
			payload.manifest !== null ||
			payload.receipt !== null ||
			payload.pin !== null
		) {
			throw new Error("Absent custody record retains artifacts");
		}
		return {};
	}
	const manifest = decodeArtifact(
		payload.manifest,
		"custody manifest artifact",
		limits,
	);
	let receipt: Uint8Array | undefined;
	if (stateHasReceipt(payload.state)) {
		receipt = decodeArtifact(
			payload.receipt,
			"custody receipt artifact",
			limits,
		);
	} else if (payload.receipt !== null) {
		throw new Error("Nonterminal custody record retains a receipt");
	}
	const decoded = await decodeAndValidateArtifacts(manifest, receipt);
	assertArtifactsMatchRecordBinding(decoded, binding);
	if (decoded.moveKey !== payload.moveKey) {
		throw new Error("Custody record key does not match manifest");
	}
	let pin: StoredPinFacts | undefined;
	if (stateHasPin(payload.state)) {
		pin = validateStoredPin(payload.pin);
		assertPinMatchesManifest(pin, decoded);
		if (payload.state === "destination-receipted") {
			assertReceiptMatchesPin(pin, decoded);
		}
	} else if (payload.pin !== null) {
		throw new Error("Unpinned custody record retains pin facts");
	}
	return {
		manifest: payload.manifest!,
		receipt: payload.receipt ?? undefined,
		pin,
		handoffId: decoded.handoffId,
		entryHash: decoded.manifest.entryHash,
		receiptId: decoded.receiptId,
	};
};

const assertReachableFrameState = (
	sequence: bigint,
	state: CustodyRecordState,
) => {
	const reachable =
		(sequence === 1n && state === "absent") ||
		(sequence === 2n &&
			(state === "source-prepared" || state === "destination-collecting")) ||
		(sequence === 3n &&
			(state === "source-receipt-durable" || state === "destination-pinned")) ||
		(sequence === 4n && state === "destination-receipted");
	if (!reachable) {
		throw new Error("Unreachable custody record generation");
	}
};

const encodeFrame = (
	moveKey: string,
	sequence: bigint,
	state: CustodyRecordState,
	manifest: string | undefined,
	receipt: string | undefined,
	pin: StoredPinFacts | undefined,
	durability: CustodyStoreDurability,
	limits: CustodyStoreLimits,
) => {
	const payloadValue: StoredFramePayload = {
		format: CUSTODY_RECORD_FORMAT,
		version: 1,
		durability,
		moveKey,
		sequence: sequence.toString(),
		state,
		manifest: manifest ?? null,
		receipt: receipt ?? null,
		pin: pin ?? null,
	};
	const payload = deterministicStringify(payloadValue);
	const checksum = toHexString(sha256Sync(encoder.encode(payload)));
	const bytes = encoder.encode(JSON.stringify({ payload, checksum }));
	if (bytes.byteLength > limits.maxFrameBytes) {
		throw new Error("Custody record frame exceeds configured byte bound");
	}
	return { bytes, checksum };
};

const decodeFrame = async (
	bytes: Uint8Array,
	expectedMoveKey: string,
	slot: CustodyRecordSlot,
	limits: CustodyStoreLimits,
	binding: CapturedCustodyRecordBinding | undefined,
): Promise<LoadedFrame> => {
	if (bytes.byteLength === 0 || bytes.byteLength > limits.maxFrameBytes) {
		throw new Error("Invalid custody record frame size");
	}
	let outer: unknown;
	try {
		outer = JSON.parse(decoder.decode(bytes));
	} catch (error) {
		throw new Error("Invalid custody record frame JSON", { cause: error });
	}
	if (
		!isRecord(outer) ||
		!hasExactKeys(outer, ["payload", "checksum"]) ||
		typeof outer.payload !== "string" ||
		typeof outer.checksum !== "string" ||
		outer.checksum.length !== 64 ||
		!CHECKSUM_PATTERN.test(outer.checksum)
	) {
		throw new Error("Invalid custody record frame");
	}
	if (
		toHexString(sha256Sync(encoder.encode(outer.payload))) !== outer.checksum
	) {
		throw new Error("Custody record frame checksum mismatch");
	}
	let payload: unknown;
	try {
		payload = JSON.parse(outer.payload);
	} catch (error) {
		throw new Error("Invalid custody record payload JSON", { cause: error });
	}
	if (deterministicStringify(payload) !== outer.payload) {
		throw new Error("Custody record payload is not canonical");
	}
	if (
		!isRecord(payload) ||
		!hasExactKeys(payload, [
			"format",
			"version",
			"durability",
			"moveKey",
			"sequence",
			"state",
			"manifest",
			"receipt",
			"pin",
		]) ||
		payload.format !== CUSTODY_RECORD_FORMAT ||
		payload.version !== 1 ||
		(payload.durability !== "strict" && payload.durability !== "memory") ||
		(payload.state !== "absent" &&
			payload.state !== "source-prepared" &&
			payload.state !== "source-receipt-durable" &&
			payload.state !== "destination-collecting" &&
			payload.state !== "destination-pinned" &&
			payload.state !== "destination-receipted")
	) {
		throw new Error("Invalid custody record payload");
	}
	const moveKey = assertMoveKey(payload.moveKey);
	if (moveKey !== expectedMoveKey) {
		throw new Error("Custody record was stored under the wrong key");
	}
	const sequence = parseSequence(payload.sequence);
	const expectedSlot: CustodyRecordSlot = sequence % 2n === 0n ? "a" : "b";
	if (slot !== expectedSlot) {
		throw new Error("Custody record generation is in the wrong slot");
	}
	assertReachableFrameState(sequence, payload.state);
	const artifacts = await validateStoredArtifacts(
		payload as StoredFramePayload,
		limits,
		binding,
	);
	return {
		moveKey,
		sequence,
		slot,
		durability: payload.durability,
		state: payload.state,
		manifest: artifacts.manifest,
		receipt: artifacts.receipt,
		pin: artifacts.pin,
		handoffId: artifacts.handoffId,
		entryHash: artifacts.entryHash,
		receiptId: artifacts.receiptId,
		checksum: outer.checksum,
	};
};

const oppositeSlot = (slot: CustodyRecordSlot): CustodyRecordSlot =>
	slot === "a" ? "b" : "a";

/**
 * Handoff/receipt IDs hash the complete authenticated canonical bodies. A
 * semantically identical valid re-signature is therefore a retry: retain and
 * return the first stored envelope bytes instead of rewriting the record.
 */
const sameTarget = (frame: LoadedFrame, target: CapturedTarget) =>
	frame.state === target.state &&
	frame.handoffId === target.handoffId &&
	(target.receiptId === undefined
		? frame.receiptId === undefined
		: frame.receiptId === target.receiptId) &&
	deterministicStringify(frame.pin ?? null) ===
		deterministicStringify(target.pin ?? null);

const assertTransition = (current: LoadedFrame, target: CapturedTarget) => {
	const allowed =
		(current.state === "absent" && target.state === "source-prepared") ||
		(current.state === "source-prepared" &&
			target.state === "source-receipt-durable") ||
		(current.state === "absent" && target.state === "destination-collecting") ||
		(current.state === "destination-collecting" &&
			target.state === "destination-pinned") ||
		(current.state === "destination-pinned" &&
			target.state === "destination-receipted");
	if (!allowed) {
		throw new Error(
			`Invalid custody record transition ${current.state} -> ${target.state}`,
		);
	}
	if (
		current.handoffId !== undefined &&
		current.handoffId !== target.handoffId
	) {
		throw new Error("Custody transition changed the signed manifest");
	}
};

const assertCoherentFramePair = (lower: LoadedFrame, higher: LoadedFrame) => {
	if (higher.sequence !== lower.sequence + 1n) {
		throw new Error("Non-adjacent custody record generations");
	}
	const edge = `${lower.state}->${higher.state}`;
	if (
		edge !== "absent->source-prepared" &&
		edge !== "source-prepared->source-receipt-durable" &&
		edge !== "absent->destination-collecting" &&
		edge !== "destination-collecting->destination-pinned" &&
		edge !== "destination-pinned->destination-receipted"
	) {
		throw new Error("Invalid custody record generation history");
	}
	if (lower.state !== "absent") {
		if (
			lower.handoffId !== higher.handoffId ||
			lower.manifest !== higher.manifest
		) {
			throw new Error("Custody record generation changed its manifest");
		}
	}
	if (lower.state === "destination-pinned") {
		if (
			deterministicStringify(lower.pin) !== deterministicStringify(higher.pin)
		) {
			throw new Error("Custody record generation changed its pin facts");
		}
	} else if (
		lower.pin !== undefined &&
		higher.state !== "destination-receipted"
	) {
		throw new Error("Invalid custody record generation pin carry");
	}
};

export type SelectedCustodyRecordCatalogFrame = Readonly<{
	moveKey: string;
	recordSequence: bigint;
	slot: CustodyRecordSlot;
	state: CustodyRecordState;
	entryHash?: string;
	handoffId?: string;
	frameChecksum: string;
}>;

/**
 * @internal Validate at most one complete A/B pair and select the exact logical
 * head. This is shared with the Node catalog so indexing cannot trust a caller
 * supplied head or blindly follow the slot most recently written.
 */
export const selectCustodyRecordCatalogFrame = async (input: {
	moveKey: string;
	frames: readonly Readonly<{
		slot: CustodyRecordSlot;
		bytes: Uint8Array;
	}>[];
	durability: CustodyStoreDurability;
	limits?: Partial<CustodyStoreLimits>;
	binding?: CustodyRecordBinding;
}): Promise<SelectedCustodyRecordCatalogFrame> => {
	if (!input || typeof input !== "object") {
		throw new Error("Invalid custody catalog frame input");
	}
	const moveKeyValue = input.moveKey;
	const framesValue = input.frames;
	const durability = input.durability;
	const limitsValue = input.limits;
	const bindingValue = input.binding;
	const moveKey = assertMoveKey(moveKeyValue);
	if (!Array.isArray(framesValue)) {
		throw new Error("Invalid custody catalog frame pair");
	}
	const frameCount = framesValue.length;
	if (!Number.isSafeInteger(frameCount) || frameCount < 0 || frameCount > 2) {
		throw new Error("Invalid custody catalog frame pair");
	}
	if (durability !== "strict" && durability !== "memory") {
		throw new Error("Invalid custody catalog durability");
	}
	const limits = normalizeLimits(limitsValue);
	const binding = captureRecordBinding(bindingValue);
	const slots = new Set<CustodyRecordSlot>();
	const valid: LoadedFrame[] = [];
	const errors: unknown[] = [];
	for (let index = 0; index < frameCount; index++) {
		const value = framesValue[index];
		if (!value || typeof value !== "object") {
			throw new Error("Invalid custody catalog frame pair");
		}
		const slot = value.slot;
		const bytesValue = value.bytes;
		if ((slot !== "a" && slot !== "b") || slots.has(slot)) {
			throw new Error("Invalid custody catalog frame pair");
		}
		slots.add(slot);
		const bytes = captureBoundedUint8Array(
			bytesValue,
			1,
			limits.maxFrameBytes,
			"custody catalog frame",
		);
		try {
			valid.push(await decodeFrame(bytes, moveKey, slot, limits, binding));
		} catch (error) {
			errors.push(error);
		}
	}
	if (valid.length === 0) {
		throw new AggregateError(errors, "No valid custody catalog frame remains");
	}
	for (let index = 0; index < valid.length; index++) {
		if (valid[index]!.durability !== durability) {
			throw new Error("Custody catalog frame durability mismatch");
		}
	}
	valid.sort((left, right) =>
		left.sequence < right.sequence
			? -1
			: left.sequence > right.sequence
				? 1
				: left.slot.localeCompare(right.slot),
	);
	if (valid.length === 2) assertCoherentFramePair(valid[0]!, valid[1]!);
	const selected = valid.at(-1)!;
	if (!selected.checksum) throw new Error("Missing custody frame checksum");
	return Object.freeze({
		moveKey,
		recordSequence: selected.sequence,
		slot: selected.slot,
		state: selected.state,
		...(selected.entryHash === undefined
			? {}
			: { entryHash: selected.entryHash }),
		...(selected.handoffId === undefined
			? {}
			: { handoffId: selected.handoffId }),
		frameChecksum: selected.checksum,
	});
};

const snapshotFromFrame = (
	frame: LoadedFrame,
	durability: CustodyStoreDurability,
): CustodyRecordSnapshot =>
	Object.freeze({
		moveKey: frame.moveKey,
		revision: frame.sequence,
		durability,
		state: frame.state,
		...(frame.manifest === undefined ? {} : { manifest: frame.manifest }),
		...(durability === "memory" || frame.receipt === undefined
			? {}
			: { receipt: frame.receipt }),
		...(frame.pin === undefined ? {} : { pin: runtimePinFacts(frame.pin) }),
	});

/**
 * Test/model adapter. Each logical slot replacement is atomic in memory and all
 * byte arrays are defensively copied. It provides no crash durability, keyed
 * database CAS, cross-process lease, enumeration, or entry-to-pin index.
 * Memory terminal phases are reducer simulations only; public memory snapshots
 * withhold receipt bytes and never carry DurableCustodyRecordCommit.
 */
export class MemoryCustodyRecordPersistence
	implements CustodyRecordPersistence
{
	private readonly frames: Map<string, Uint8Array>;

	constructor(frames?: Map<string, Uint8Array>) {
		this.frames = frames ?? new Map();
	}

	async read(moveKey: string, slot: CustodyRecordSlot, maxBytes: number) {
		if (
			!Number.isSafeInteger(maxBytes) ||
			maxBytes <= 0 ||
			maxBytes > DEFAULT_CUSTODY_STORE_LIMITS.maxFrameBytes
		) {
			throw new Error("Invalid custody persistence read bound");
		}
		const value = this.frames.get(this.key(moveKey, slot));
		if (value === undefined) return undefined;
		try {
			return captureBoundedUint8Array(
				value,
				0,
				maxBytes,
				"custody persistence read",
			);
		} catch (error) {
			throw new Error("Custody persistence read exceeds byte bound", {
				cause: error,
			});
		}
	}

	async write(moveKey: string, slot: CustodyRecordSlot, bytes: Uint8Array) {
		let copy: Uint8Array;
		try {
			copy = captureBoundedUint8Array(
				bytes,
				1,
				DEFAULT_CUSTODY_STORE_LIMITS.maxFrameBytes,
				"custody persistence write value",
			);
		} catch {
			throw new Error("Invalid custody persistence write value");
		}
		const key = this.key(moveKey, slot);
		this.frames.set(key, copy);
	}

	async close() {}

	/** A separately leased adapter view used to model a clean reopen. */
	fork() {
		return new MemoryCustodyRecordPersistence(this.frames);
	}

	private key(moveKey: string, slot: CustodyRecordSlot) {
		assertMoveKey(moveKey);
		if (slot !== "a" && slot !== "b") {
			throw new Error("Invalid custody record slot");
		}
		return `${moveKey}:${slot}`;
	}
}

/**
 * Disconnected, bounded-work model for one-entry custody handoff records.
 *
 * Records are loaded by exact move key for each operation and are never kept in
 * a resident map. The Node adapter supplies a leased keyed backend, while the
 * raw transitions here remain a disconnected reducer/test seam. The unwired
 * rebalance bridge establishes its cross-store ordering by durably staging a
 * pending manifest before it invokes a custody callback; direct calls to
 * prepareSource() do not establish that ordering. A future runtime must use the
 * bridge, replace the test evidence issuer, and re-confirm each passive catalog
 * candidate against the exact current lower-store epoch and pin after reopen or
 * storage loss before acting on recovered copied facts. Release and prune
 * authority remain future work.
 *
 * The strict Node adapter may expose the passive bounded catalog pages below;
 * they are discovery hints revalidated by point read, never authority. This
 * store deliberately exposes no unbounded enumeration, release, drop,
 * network, transfer-finalization, or prune-permit API.
 */
export class CustodyRecordStore {
	private tail: Promise<void> = Promise.resolve();
	private closing = false;
	private closed = false;
	private closePromise?: Promise<void>;
	private poisoned = false;
	private poisonCause?: unknown;
	private ownsPersistenceLease = false;
	private pendingOperations = 0;
	private pendingOperationsDrained?: Promise<void>;
	private resolvePendingOperationsDrained?: () => void;
	readonly #sourceReceiptAuthorityValue: CustodySourceReceiptAuthority;

	private constructor(
		private readonly persistence: CustodyRecordPersistence,
		private readonly durability: CustodyStoreDurability,
		private readonly limits: CustodyStoreLimits,
		private readonly binding: CapturedCustodyRecordBinding | undefined,
	) {
		this.#sourceReceiptAuthorityValue = Object.freeze(
			{},
		) as CustodySourceReceiptAuthority;
		if (durability === "strict" && binding?.role === "source") {
			sourceReceiptAuthorities.set(
				this.#sourceReceiptAuthorityValue,
				Object.freeze({
					logId: binding.logId,
					localPublicKey: binding.localPublicKey,
				}),
			);
		}
	}

	static async open(properties: {
		persistence: CustodyRecordPersistence;
		durability: CustodyStoreDurability;
		limits?: Partial<CustodyStoreLimits>;
		binding?: CustodyRecordBinding;
	}): Promise<CustodyRecordStore> {
		if (
			!properties.persistence ||
			typeof properties.persistence.read !== "function" ||
			typeof properties.persistence.write !== "function"
		) {
			throw new Error("Invalid custody record persistence");
		}
		if (
			properties.durability !== "strict" &&
			properties.durability !== "memory"
		) {
			throw new Error("Invalid custody store durability");
		}
		if (
			properties.durability === "strict" &&
			typeof properties.persistence.durableBarrier !== "function"
		) {
			throw new Error(
				"Strict custody persistence requires a physical durability barrier",
			);
		}
		const limits = normalizeLimits(properties.limits);
		const binding = captureRecordBinding(properties.binding);
		if (openPersistenceAdapters.has(properties.persistence)) {
			throw new Error("Custody persistence is already open");
		}
		openPersistenceAdapters.add(properties.persistence);
		const store = new CustodyRecordStore(
			properties.persistence,
			properties.durability,
			limits,
			binding,
		);
		store.ownsPersistenceLease = true;
		return store;
	}

	read(moveKey: string): Promise<CustodyRecordReadResult> {
		let release: (() => void) | undefined;
		let capturedMoveKey: string;
		try {
			release = this.acquireAdmission();
			capturedMoveKey = assertMoveKey(moveKey);
		} catch (error) {
			release?.();
			return Promise.reject(error);
		}
		return this.enqueueAdmitted(async () => {
			const frame = await this.load(capturedMoveKey);
			await this.confirmLoadedFrame(frame);
			return this.result(frame);
		}).finally(release);
	}

	/** Passive bounded catalog status; never custody or prune authority. */
	readCatalogStatus(): Promise<CustodyRecordCatalogStatus> {
		return this.catalogOperation("readCatalogStatus", () => []);
	}

	captureCatalogFence(): Promise<CustodyRecordCatalogFence> {
		return this.catalogOperation("captureCatalogFence", () => []);
	}

	scanRecoveryPage(input: {
		fence: CustodyRecordCatalogFence;
		state: CustodyRecordState;
		after?: CustodyRecordCatalogCursor;
		maxRows?: number;
		maxBytes?: number;
	}): Promise<CustodyRecordCatalogPage> {
		return this.catalogOperation("scanRecoveryPage", () => [
			this.snapshotRecoveryScan(input),
		]);
	}

	scanEntryPinsPage(input: {
		fence: CustodyRecordCatalogFence;
		entryHash: string;
		after?: CustodyRecordCatalogCursor;
		maxRows?: number;
		maxBytes?: number;
	}): Promise<CustodyRecordCatalogPage> {
		return this.catalogOperation("scanEntryPinsPage", () => [
			this.snapshotEntryPinScan(input),
		]);
	}

	readCatalogCandidate(
		candidate: CustodyRecordCatalogCandidate,
		maxBytes?: number,
	): Promise<CustodyRecordCatalogCandidateRead> {
		return this.catalogOperation("readCatalogCandidate", () => [
			this.snapshotCatalogCandidate(candidate),
			this.snapshotOptionalNumber(maxBytes, "catalog candidate byte bound"),
		]);
	}

	migrateCatalogPage(input?: {
		maxMoveKeys?: number;
		maxBytes?: number;
	}): Promise<CustodyRecordCatalogMigrationPage> {
		return this.catalogOperation("migrateCatalogPage", () => [
			this.snapshotMigrationOptions(input),
		]);
	}

	/** @internal Point-read and reissue the exact strict source receipt proof. */
	async readSourceReceipt(
		moveKey: string,
	): Promise<CustodySourceReceiptReadResult> {
		const result = await this.read(moveKey);
		if (
			result.snapshot.state !== "source-receipt-durable" ||
			!result.durableCommit ||
			result.durableCommit.state !== "source-receipt-durable"
		) {
			throw new Error("Custody record is not source-receipt-durable");
		}
		getDurableCustodySourceReceiptFacts(
			result.durableCommit as DurableCustodySourceReceiptCommit,
		);
		return result as CustodySourceReceiptReadResult;
	}

	/** @internal Pair a work cursor with this exact opened source store. */
	sourceReceiptAuthority(): CustodySourceReceiptAuthority {
		this.adoptPersistencePoison();
		if (this.closing || this.closed) {
			throw new Error("Custody record store is closing");
		}
		if (this.binding?.role !== "source") {
			throw new Error(
				"Custody source receipt authority requires a source binding",
			);
		}
		if (this.durability !== "strict") {
			throw new Error(
				"Custody source receipt authority requires strict durability",
			);
		}
		if (this.poisoned) {
			throw new Error("Custody record store is poisoned", {
				cause: this.poisonCause,
			});
		}
		return assertCustodySourceReceiptAuthority(
			this.#sourceReceiptAuthorityValue,
		);
	}

	prepareSource(
		expectedRevision: bigint,
		manifest: Uint8Array,
	): Promise<CustodyRecordReadResult> {
		return this.captureTransition(
			expectedRevision,
			"source-prepared",
			manifest,
		);
	}

	markSourceReceiptDurable(
		expectedRevision: bigint,
		manifest: Uint8Array,
		receipt: Uint8Array,
	): Promise<CustodySourceReceiptReadResult> {
		return this.captureTransition(
			expectedRevision,
			"source-receipt-durable",
			manifest,
			receipt,
		) as Promise<CustodySourceReceiptReadResult>;
	}

	beginDestination(
		expectedRevision: bigint,
		manifest: Uint8Array,
	): Promise<CustodyRecordReadResult> {
		return this.captureTransition(
			expectedRevision,
			"destination-collecting",
			manifest,
		);
	}

	markDestinationPinned(
		expectedRevision: bigint,
		manifest: Uint8Array,
		evidence: CustodyDestinationPinEvidence,
	): Promise<CustodyRecordReadResult> {
		return this.captureTransition(
			expectedRevision,
			"destination-pinned",
			manifest,
			undefined,
			evidence,
		);
	}

	markDestinationReceipted(
		expectedRevision: bigint,
		manifest: Uint8Array,
		receipt: Uint8Array,
	): Promise<CustodyRecordReadResult> {
		return this.captureTransition(
			expectedRevision,
			"destination-receipted",
			manifest,
			receipt,
		);
	}

	close(): Promise<void> {
		if (this.closePromise) return this.closePromise;
		this.closing = true;
		sourceReceiptAuthorities.delete(this.#sourceReceiptAuthorityValue);
		this.closePromise = (async () => {
			await this.waitForPendingOperations();
			await this.tail;
			let closeFailed = false;
			let closeError: unknown;
			try {
				await this.persistence.close?.({ flush: false });
			} catch (error) {
				closeFailed = true;
				closeError = error;
			} finally {
				this.closed = true;
				this.releasePersistenceLease();
			}
			const poisonError = this.poisoned
				? new Error("Custody record store is poisoned", {
						cause: this.poisonCause,
					})
				: undefined;
			if (poisonError && closeFailed) {
				throw new AggregateError(
					[poisonError, closeError],
					"Failed to close poisoned custody record store",
				);
			}
			if (closeFailed) throw closeError;
			if (poisonError) throw poisonError;
		})();
		return this.closePromise;
	}

	private async captureTransition(
		expectedRevision: bigint,
		state: Exclude<CustodyRecordState, "absent">,
		manifestValue: Uint8Array,
		receiptValue?: Uint8Array,
		pinEvidence?: CustodyDestinationPinEvidence,
	): Promise<CustodyRecordReadResult> {
		const release = this.acquireAdmission();
		try {
			return await this.captureTransitionAdmitted(
				expectedRevision,
				state,
				manifestValue,
				receiptValue,
				pinEvidence,
			);
		} finally {
			release();
		}
	}

	private async captureTransitionAdmitted(
		expectedRevision: bigint,
		state: Exclude<CustodyRecordState, "absent">,
		manifestValue: Uint8Array,
		receiptValue?: Uint8Array,
		pinEvidence?: CustodyDestinationPinEvidence,
	): Promise<CustodyRecordReadResult> {
		let capturedRevision: bigint;
		let manifestBytes: Uint8Array;
		let receiptBytes: Uint8Array | undefined;
		try {
			capturedRevision = assertExpectedRevision(expectedRevision);
			assertStateMatchesRecordBinding(state, this.binding);
			manifestBytes = captureArtifact(
				manifestValue,
				"custody manifest artifact",
				this.limits,
			);
			receiptBytes = receiptValue
				? captureArtifact(receiptValue, "custody receipt artifact", this.limits)
				: undefined;
			if (stateHasReceipt(state) !== (receiptBytes !== undefined)) {
				throw new Error("Custody transition has invalid receipt presence");
			}
		} catch (error) {
			throw error;
		}
		const decoded = await decodeAndValidateArtifacts(
			manifestBytes,
			receiptBytes,
		);
		assertArtifactsMatchRecordBinding(decoded, this.binding);
		let pin: StoredPinFacts | undefined;
		if (state === "destination-pinned") {
			pin = pinEvidence
				? destinationPinEvidenceFacts.get(pinEvidence)
				: undefined;
			if (!pin) {
				throw new Error("Invalid custody destination pin evidence");
			}
			assertPinMatchesManifest(pin, decoded);
		} else if (pinEvidence !== undefined) {
			throw new Error("Unexpected custody destination pin evidence");
		}
		let target: CapturedTarget = Object.freeze({
			moveKey: decoded.moveKey,
			state,
			manifest: encodeArtifact(manifestBytes),
			handoffId: decoded.handoffId,
			...(receiptBytes
				? {
						receipt: encodeArtifact(receiptBytes),
						receiptId: decoded.receiptId,
					}
				: {}),
			...(pin ? { pin } : {}),
		});
		// Preflight the largest possible first write before admitting any physical
		// baseline mutation.
		encodeFrame(
			target.moveKey,
			revisionForState(target.state),
			target.state,
			target.manifest,
			target.receipt,
			target.pin,
			this.durability,
			this.limits,
		);
		return this.enqueueAdmitted(async () => {
			let current = await this.load(target.moveKey);
			await this.confirmLoadedFrame(current);
			if (
				current.manifest !== undefined &&
				current.handoffId === target.handoffId
			) {
				target = Object.freeze({
					...target,
					manifest: current.manifest,
				});
			}
			if (target.state === "destination-receipted" && current.pin) {
				assertReceiptMatchesPin(current.pin, decoded);
				target = Object.freeze({ ...target, pin: current.pin });
			}
			if (sameTarget(current, target)) {
				return this.result(current);
			}
			const resumesMaterializedBaseline =
				current.sequence === 1n &&
				current.state === "absent" &&
				capturedRevision === 0n;
			if (
				current.sequence !== capturedRevision &&
				!resumesMaterializedBaseline
			) {
				throw new Error("Stale custody record revision");
			}
			assertTransition(current, target);
			if (current.implicit) {
				current = await this.writeFrame({
					moveKey: target.moveKey,
					sequence: current.sequence + 1n,
					slot: oppositeSlot(current.slot),
					state: "absent",
				});
			}
			const nextSequence = current.sequence + 1n;
			if (nextSequence !== revisionForState(target.state)) {
				throw new Error("Invalid custody record reducer revision");
			}
			const written = await this.writeFrame({
				moveKey: target.moveKey,
				sequence: nextSequence,
				slot: oppositeSlot(current.slot),
				state: target.state,
				manifest: target.manifest,
				receipt: target.receipt,
				pin: target.pin,
				handoffId: target.handoffId,
				receiptId: target.receiptId,
			});
			return this.result(written);
		});
	}

	private enqueueAdmitted<T>(operation: () => Promise<T>): Promise<T> {
		if (this.poisoned) {
			return Promise.reject(
				new Error("Custody record store is poisoned", {
					cause: this.poisonCause,
				}),
			);
		}
		const result = this.tail.then(async () => {
			if (this.poisoned) {
				throw new Error("Custody record store is poisoned", {
					cause: this.poisonCause,
				});
			}
			return operation();
		});
		this.tail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	private snapshotOptionalNumber(value: unknown, name: string) {
		if (value === undefined) return undefined;
		if (typeof value !== "number") throw new Error(`Invalid custody ${name}`);
		return value;
	}

	private snapshotCatalogCursor(value: unknown) {
		if (value === undefined) return undefined;
		if (!value || typeof value !== "object") {
			throw new Error("Invalid custody catalog cursor");
		}
		const cursor = value as Record<string, unknown>;
		const mutationSequence = cursor.mutationSequence;
		const moveKey = cursor.moveKey;
		if (typeof mutationSequence !== "bigint" || typeof moveKey !== "string") {
			throw new Error("Invalid custody catalog cursor");
		}
		return Object.freeze({ mutationSequence, moveKey });
	}

	private snapshotRecoveryScan(value: unknown) {
		if (!value || typeof value !== "object") {
			throw new Error("Invalid custody catalog recovery scan");
		}
		const input = value as Record<string, unknown>;
		const fence = input.fence;
		const state = input.state;
		const after = input.after;
		const maxRows = input.maxRows;
		const maxBytes = input.maxBytes;
		if (typeof state !== "string") {
			throw new Error("Invalid custody catalog recovery state");
		}
		return Object.freeze({
			fence: fence as CustodyRecordCatalogFence,
			state: state as CustodyRecordState,
			...(after === undefined
				? {}
				: { after: this.snapshotCatalogCursor(after)! }),
			...(maxRows === undefined
				? {}
				: {
						maxRows: this.snapshotOptionalNumber(maxRows, "catalog row bound"),
					}),
			...(maxBytes === undefined
				? {}
				: {
						maxBytes: this.snapshotOptionalNumber(
							maxBytes,
							"catalog byte bound",
						),
					}),
		});
	}

	private snapshotEntryPinScan(value: unknown) {
		if (!value || typeof value !== "object") {
			throw new Error("Invalid custody catalog entry pin scan");
		}
		const input = value as Record<string, unknown>;
		const fence = input.fence;
		const entryHash = input.entryHash;
		const after = input.after;
		const maxRows = input.maxRows;
		const maxBytes = input.maxBytes;
		if (typeof entryHash !== "string") {
			throw new Error("Invalid custody catalog entry hash");
		}
		return Object.freeze({
			fence: fence as CustodyRecordCatalogFence,
			entryHash,
			...(after === undefined
				? {}
				: { after: this.snapshotCatalogCursor(after)! }),
			...(maxRows === undefined
				? {}
				: {
						maxRows: this.snapshotOptionalNumber(maxRows, "catalog row bound"),
					}),
			...(maxBytes === undefined
				? {}
				: {
						maxBytes: this.snapshotOptionalNumber(
							maxBytes,
							"catalog byte bound",
						),
					}),
		});
	}

	private snapshotCatalogCandidate(value: unknown) {
		if (!value || typeof value !== "object") {
			throw new Error("Invalid custody catalog candidate");
		}
		const input = value as Record<string, unknown>;
		const catalogEpoch = input.catalogEpoch;
		const mutationSequence = input.mutationSequence;
		const moveKey = input.moveKey;
		const recordSequence = input.recordSequence;
		const slot = input.slot;
		const state = input.state;
		const entryHash = input.entryHash;
		const handoffId = input.handoffId;
		const frameChecksum = input.frameChecksum;
		const domainId = input.domainId;
		const writerEpoch = input.writerEpoch;
		const writerOwner = input.writerOwner;
		if (
			typeof catalogEpoch !== "string" ||
			typeof mutationSequence !== "bigint" ||
			typeof moveKey !== "string" ||
			typeof recordSequence !== "bigint" ||
			typeof slot !== "string" ||
			typeof state !== "string" ||
			(entryHash !== undefined && typeof entryHash !== "string") ||
			(handoffId !== undefined && typeof handoffId !== "string") ||
			typeof frameChecksum !== "string" ||
			typeof domainId !== "string" ||
			typeof writerEpoch !== "bigint" ||
			typeof writerOwner !== "string"
		) {
			throw new Error("Invalid custody catalog candidate");
		}
		return Object.freeze({
			catalogEpoch,
			mutationSequence,
			moveKey,
			recordSequence,
			slot: slot as CustodyRecordSlot,
			state: state as CustodyRecordState,
			...(entryHash === undefined ? {} : { entryHash }),
			...(handoffId === undefined ? {} : { handoffId }),
			frameChecksum,
			domainId,
			writerEpoch,
			writerOwner,
		});
	}

	private snapshotMigrationOptions(value: unknown) {
		if (value === undefined) return Object.freeze({});
		if (!value || typeof value !== "object") {
			throw new Error("Invalid custody catalog migration options");
		}
		const input = value as Record<string, unknown>;
		const maxMoveKeys = input.maxMoveKeys;
		const maxBytes = input.maxBytes;
		return Object.freeze({
			...(maxMoveKeys === undefined
				? {}
				: {
						maxMoveKeys: this.snapshotOptionalNumber(
							maxMoveKeys,
							"catalog migration key bound",
						),
					}),
			...(maxBytes === undefined
				? {}
				: {
						maxBytes: this.snapshotOptionalNumber(
							maxBytes,
							"catalog migration byte bound",
						),
					}),
		});
	}

	private catalogOperation<
		K extends
			| "readCatalogStatus"
			| "captureCatalogFence"
			| "scanRecoveryPage"
			| "scanEntryPinsPage"
			| "readCatalogCandidate"
			| "migrateCatalogPage",
	>(
		name: K,
		captureArgs: () => unknown[],
		poisonOnFailure = false,
	): Promise<
		K extends "readCatalogStatus"
			? CustodyRecordCatalogStatus
			: K extends "captureCatalogFence"
				? CustodyRecordCatalogFence
				: K extends "scanRecoveryPage" | "scanEntryPinsPage"
					? CustodyRecordCatalogPage
					: K extends "readCatalogCandidate"
						? CustodyRecordCatalogCandidateRead
						: CustodyRecordCatalogMigrationPage
	> {
		let release: (() => void) | undefined;
		try {
			release = this.acquireAdmission();
			const method = this.persistence[name];
			if (typeof method !== "function") {
				throw new Error(
					"Custody persistence does not expose a bounded catalog",
				);
			}
			const args = captureArgs();
			const result = this.enqueueAdmitted(async () =>
				Reflect.apply(method, this.persistence, args),
			);
			return result
				.catch((error) => {
					if (poisonOnFailure || this.persistence.isPoisoned?.() === true) {
						this.poison(error);
					}
					throw error;
				})
				.finally(release) as never;
		} catch (error) {
			release?.();
			return Promise.reject(error) as never;
		}
	}

	private acquireAdmission() {
		this.adoptPersistencePoison();
		if (this.closing || this.closed) {
			throw new Error("Custody record store is closing");
		}
		if (this.poisoned) {
			throw new Error("Custody record store is poisoned", {
				cause: this.poisonCause,
			});
		}
		if (this.pendingOperations >= this.limits.maxPendingOperations) {
			throw new Error("Custody pending-operation bound exceeded");
		}
		this.pendingOperations++;
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.pendingOperations--;
			if (this.pendingOperations === 0) {
				this.resolvePendingOperationsDrained?.();
				this.resolvePendingOperationsDrained = undefined;
				this.pendingOperationsDrained = undefined;
			}
		};
	}

	private waitForPendingOperations() {
		if (this.pendingOperations === 0) return Promise.resolve();
		if (!this.pendingOperationsDrained) {
			this.pendingOperationsDrained = new Promise<void>((resolve) => {
				this.resolvePendingOperationsDrained = resolve;
			});
		}
		return this.pendingOperationsDrained;
	}

	private async load(moveKey: string): Promise<LoadedFrame> {
		const reads = await Promise.allSettled(
			CUSTODY_RECORD_SLOTS.map((slot) =>
				this.persistence.read(moveKey, slot, this.limits.maxFrameBytes),
			),
		);
		const valid: LoadedFrame[] = [];
		const errors: unknown[] = [];
		const readErrors = reads
			.filter(
				(read): read is PromiseRejectedResult => read.status === "rejected",
			)
			.map((read) => read.reason);
		if (readErrors.length > 0) {
			throw new AggregateError(
				readErrors,
				"Failed to read every custody record generation",
			);
		}
		for (let index = 0; index < reads.length; index++) {
			const read = reads[index];
			if (read.status === "rejected") continue;
			if (read.value === undefined) continue;
			// A fulfilled adapter value must be bounded real bytes. Keep this
			// contract check outside the corrupt-frame fallback below: a Proxy,
			// wrong type, or oversized value may hide a newer generation.
			const bytes = captureBoundedUint8Array(
				read.value,
				0,
				this.limits.maxFrameBytes,
				"custody persistence read value",
			);
			try {
				valid.push(
					await decodeFrame(
						bytes,
						moveKey,
						CUSTODY_RECORD_SLOTS[index],
						this.limits,
						this.binding,
					),
				);
			} catch (error) {
				errors.push(error);
			}
		}
		if (valid.length === 0) {
			if (errors.length > 0) {
				throw new AggregateError(
					errors,
					"No valid custody record generation remains",
				);
			}
			return {
				moveKey,
				sequence: 0n,
				slot: "a",
				state: "absent",
				implicit: true,
			};
		}
		if (valid.some((frame) => frame.durability !== this.durability)) {
			throw new Error("Custody record durability mode mismatch");
		}
		valid.sort((left, right) =>
			left.sequence < right.sequence
				? -1
				: left.sequence > right.sequence
					? 1
					: left.slot.localeCompare(right.slot),
		);
		if (valid.length === 2) {
			assertCoherentFramePair(valid[0], valid[1]);
		}
		const highest = valid.at(-1)!;
		return highest;
	}

	private async confirmLoadedFrame(frame: LoadedFrame) {
		if (this.durability !== "strict" || frame.implicit || !frame.checksum) {
			return;
		}
		try {
			await this.persistence.durableBarrier!(frame.moveKey, frame.slot);
		} catch (error) {
			this.poison(error);
			throw new Error("Failed to confirm recovered custody record frame", {
				cause: error,
			});
		}
		frame.durabilityConfirmed = true;
	}

	private async writeFrame(input: {
		moveKey: string;
		sequence: bigint;
		slot: CustodyRecordSlot;
		state: CustodyRecordState;
		manifest?: string;
		receipt?: string;
		pin?: StoredPinFacts;
		handoffId?: string;
		receiptId?: string;
	}) {
		const encoded = encodeFrame(
			input.moveKey,
			input.sequence,
			input.state,
			input.manifest,
			input.receipt,
			input.pin,
			this.durability,
			this.limits,
		);
		try {
			await this.persistence.write(input.moveKey, input.slot, encoded.bytes);
			if (this.durability === "strict") {
				await this.persistence.durableBarrier!(input.moveKey, input.slot);
			}
		} catch (error) {
			this.poison(error);
			throw new Error("Failed to persist custody record frame", {
				cause: error,
			});
		}
		return {
			...input,
			checksum: encoded.checksum,
			durability: this.durability,
			durabilityConfirmed: this.durability === "strict",
		} satisfies LoadedFrame;
	}

	private result(frame: LoadedFrame): CustodyRecordReadResult {
		const snapshot = snapshotFromFrame(frame, this.durability);
		if (
			this.durability !== "strict" ||
			!frame.checksum ||
			!frame.durabilityConfirmed
		) {
			return Object.freeze({ snapshot });
		}
		const durableCommit = Object.freeze({
			moveKey: frame.moveKey,
			revision: frame.sequence,
			state: frame.state,
			frameChecksum: frame.checksum,
		}) as DurableCustodyRecordCommit;
		if (
			frame.state === "source-receipt-durable" &&
			frame.sequence === 3n &&
			frame.handoffId !== undefined &&
			frame.receiptId !== undefined
		) {
			const sourceCommit = Object.freeze({
				...durableCommit,
				handoffId: frame.handoffId,
				receiptId: frame.receiptId,
			}) as DurableCustodySourceReceiptCommit;
			sourceReceiptCommitFacts.set(
				sourceCommit,
				Object.freeze({
					moveKey: frame.moveKey,
					handoffId: frame.handoffId,
					receiptId: frame.receiptId,
					authority: this.#sourceReceiptAuthorityValue,
				}),
			);
			return Object.freeze({ snapshot, durableCommit: sourceCommit });
		}
		return Object.freeze({ snapshot, durableCommit });
	}

	private poison(cause: unknown) {
		if (this.poisoned) return;
		this.poisoned = true;
		this.poisonCause = cause;
		sourceReceiptAuthorities.delete(this.#sourceReceiptAuthorityValue);
	}

	private adoptPersistencePoison() {
		if (!this.poisoned && this.persistence.isPoisoned?.() === true) {
			this.poison(new Error("Custody persistence is poisoned"));
		}
	}

	private releasePersistenceLease() {
		if (!this.ownsPersistenceLease) return;
		this.ownsPersistenceLease = false;
		openPersistenceAdapters.delete(this.persistence);
	}
}
