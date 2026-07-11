import {
	NATIVE_DURABILITY_JOURNAL_MAX_PROGRAM_ID_LENGTH,
	type NativeDurabilityCheckpointTransactionState,
	NativeDurabilityJournalCodec,
	type NativeDurabilityJournalRecord,
	type NativeDurabilityJournalScan,
	type NativeDurabilityJournalValidationContext,
	NativeDurabilityPhase,
	createNativeDurabilityJournalCodec,
	isNativeDurabilityJournalCodec,
} from "./codec.js";
import {
	NATIVE_DURABILITY_MAX_U64,
	type NativeDurabilityLease,
} from "./lease.js";
import {
	NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME,
	acquireNativeDurabilityNodeLease,
} from "./node-lease.js";
import {
	NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES,
	NATIVE_DURABILITY_STORAGE_VERSION,
	type NativeDurabilityCheckpoint,
	type NativeDurabilityCheckpointReceipt,
	type NativeDurabilityCheckpointRequest,
	type NativeDurabilityDeleteReceipt,
	type NativeDurabilityDeleteRequest,
	NativeDurabilityDigestMismatchError,
	NativeDurabilityIncompleteTailMismatchError,
	type NativeDurabilityIncompleteTailReconciliationRequest,
	type NativeDurabilityJournalAppendRequest,
	NativeDurabilityJournalOffsetConflictError,
	type NativeDurabilityJournalReceipt,
	type NativeDurabilityJournalReconciliationReceipt,
	NativeDurabilityMigrationRequiredError,
	type NativeDurabilityOperationScope,
	NativeDurabilityOutcomeUnknownError,
	type NativeDurabilityStageReceipt,
	type NativeDurabilityStageRequest,
	type NativeDurabilityStagedBlockReference,
	type NativeDurabilityStagingManifest,
	type NativeDurabilityStorage,
	NativeDurabilityStorageClosedError,
	NativeDurabilityStorageCorruptionError,
	type NativeDurabilityStorageStats,
	NativeDurabilityStorageUnsupportedError,
	assertNativeDurabilityCheckpointRequest,
	assertNativeDurabilityDeleteRequest,
	assertNativeDurabilityFence,
	assertNativeDurabilityJournalAppendRequest,
	assertNativeDurabilityOperationScope,
	assertNativeDurabilityStageRequest,
	copyNativeDurabilityBytes,
	encodeNativeDurabilityCanonical,
	nativeDurabilityBytesEqual,
	nativeDurabilityDigestFromHex,
	nativeDurabilityDigestHex,
} from "./storage.js";

const STORAGE_DIRECTORY_NAME = "native-durability-v1";
const STAGING_DIRECTORY_NAME = "staging";
const CHECKPOINT_DIRECTORY_NAME = "checkpoints";
const JOURNAL_FILE_NAME = "journal.bin";
const STAGING_MANIFEST_FILE_NAME = "manifest.json";
const CHECKPOINT_HIGHWATER_FILE_NAME = "generation-highwater.json";
const CHECKPOINT_MANIFEST_A = "manifest-a.json";
const CHECKPOINT_MANIFEST_B = "manifest-b.json";
const JOURNAL_BASE_MANIFEST = "journal-base.json";

type ManifestEnvelope = { payload: string; checksum: string };

type NodeFs = typeof import("fs/promises");
type NodePath = typeof import("path");
type NodeCreateHash = (typeof import("crypto"))["createHash"];

let nodeFs: NodeFs | undefined;
let nodePath: NodePath | undefined;
let nodeCreateHash: NodeCreateHash | undefined;

const loadNodeModules = async (): Promise<void> => {
	if (nodeFs && nodePath && nodeCreateHash) return;
	const processLike = globalThis as {
		process?: { versions?: { node?: string } };
	};
	if (!processLike.process?.versions?.node) {
		throw new NativeDurabilityStorageUnsupportedError(
			"Native durability strict storage is Node-only; OPFS is not supported",
		);
	}
	const fsModule = "fs/promises";
	const pathModule = "path";
	const cryptoModule = "crypto";
	const [fs, path, crypto] = await Promise.all([
		import(/* @vite-ignore */ fsModule) as Promise<NodeFs>,
		import(/* @vite-ignore */ pathModule) as Promise<NodePath>,
		import(/* @vite-ignore */ cryptoModule) as Promise<typeof import("crypto")>,
	]);
	nodeFs = fs;
	nodePath = path;
	nodeCreateHash = crypto.createHash;
};

const requireNodeFs = (): NodeFs => {
	if (!nodeFs) throw new Error("Node durability modules have not been loaded");
	return nodeFs;
};

const requireNodePath = (): NodePath => {
	if (!nodePath)
		throw new Error("Node durability modules have not been loaded");
	return nodePath;
};

type DiskFence = { epoch: string; ownerId: string; domainId: string };
type DiskScope = {
	transactionId: string;
	txSequence: string;
	recordLsn: string;
};
type DiskBlockReference = {
	ordinal: number;
	cid: string;
	byteLength: number;
	digest: string;
};

type DiskStagingManifest = {
	version: typeof NATIVE_DURABILITY_STORAGE_VERSION;
	scope: DiskScope;
	fence: DiskFence;
	blocks: DiskBlockReference[];
};

type DiskStagingCoverage = {
	transactionId: string;
	txSequence: string;
	coveredThroughLsn: string;
	stagingManifestDigest: string;
};

type DiskRetainedTransaction = {
	txSequence: string;
	transactionId: string;
	phase: number;
	operationKind: number;
	planDigest: string;
};

type DiskCheckpointManifest = {
	version: typeof NATIVE_DURABILITY_STORAGE_VERSION;
	programId: string;
	generation: string;
	checkpointLsn: string;
	txSequenceHighwater: string;
	file: string;
	byteLength: number;
	digest: string;
	originFence: DiskFence;
	stagingCoverage: DiskStagingCoverage[];
	retainedTransactions: DiskRetainedTransaction[];
};

type DiskGenerationIdentity = {
	generation: string;
	requestDigest: string;
	transactionId: string;
	txSequence: string;
};

type DiskGenerationHighwater = {
	version: typeof NATIVE_DURABILITY_STORAGE_VERSION;
	generation: string;
	pending?: DiskGenerationIdentity;
	completed?: DiskGenerationIdentity;
};

type GenerationIdentity = {
	generation: bigint;
	requestDigest: Uint8Array;
	transactionId: string;
	txSequence: bigint;
};

type GenerationState = {
	generation: bigint;
	pending?: GenerationIdentity;
	completed?: GenerationIdentity;
};

export type NativeDurabilityNodeWritable = {
	write: (
		buffer: Uint8Array,
		offset: number,
		length: number,
		position: number,
	) => Promise<{ bytesWritten: number }>;
};

export type NativeDurabilityNodeReadable = {
	read: (
		buffer: Uint8Array,
		offset: number,
		length: number,
		position: number,
	) => Promise<{ bytesRead: number }>;
};

/** `FileHandle.write()` is allowed to complete with a short write. */
export const writeNativeDurabilityBytesFully = async (
	handle: NativeDurabilityNodeWritable,
	bytes: Uint8Array,
	position: number,
): Promise<void> => {
	let written = 0;
	while (written < bytes.byteLength) {
		const result = await handle.write(
			bytes,
			written,
			bytes.byteLength - written,
			position + written,
		);
		if (
			!Number.isSafeInteger(result.bytesWritten) ||
			result.bytesWritten <= 0
		) {
			throw new Error("Native durability write made no forward progress");
		}
		if (result.bytesWritten > bytes.byteLength - written) {
			throw new Error(
				"Native durability write exceeded the requested byte count",
			);
		}
		written += result.bytesWritten;
	}
};

export const readNativeDurabilityBytesFully = async (
	handle: NativeDurabilityNodeReadable,
	byteLength: number,
): Promise<Uint8Array> => {
	if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
		throw new RangeError("Native durability read length must be safe");
	}
	const bytes = new Uint8Array(byteLength);
	let offset = 0;
	while (offset < byteLength) {
		const result = await handle.read(
			bytes,
			offset,
			byteLength - offset,
			offset,
		);
		if (
			!Number.isSafeInteger(result.bytesRead) ||
			result.bytesRead <= 0 ||
			result.bytesRead > byteLength - offset
		) {
			throw new NativeDurabilityStorageCorruptionError(
				"Native durability file changed during an exact read",
			);
		}
		offset += result.bytesRead;
	}
	return bytes;
};

const sha256 = (bytes: Uint8Array): Uint8Array => {
	if (!nodeCreateHash)
		throw new Error("Node durability modules have not been loaded");
	return new Uint8Array(nodeCreateHash("sha256").update(bytes).digest());
};

const sha256Hex = (bytes: Uint8Array): string =>
	nativeDurabilityDigestHex(sha256(bytes));

const bytesToHex = (bytes: Uint8Array): string =>
	Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const bytesFromHex = (value: unknown, subject: string): Uint8Array => {
	if (
		typeof value !== "string" ||
		value.length % 2 !== 0 ||
		!/^[0-9a-f]*$/.test(value)
	) {
		throw new NativeDurabilityStorageCorruptionError(
			`${subject} must be canonical lowercase hexadecimal bytes`,
		);
	}
	const bytes = new Uint8Array(value.length / 2);
	for (let index = 0; index < bytes.byteLength; index++) {
		bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
	}
	return bytes;
};

const encodeManifest = (payload: unknown): Uint8Array => {
	const payloadText = JSON.stringify(payload);
	return new TextEncoder().encode(
		JSON.stringify({
			payload: payloadText,
			checksum: sha256Hex(new TextEncoder().encode(payloadText)),
		}),
	);
};

const decodeManifest = <T>(bytes: Uint8Array, subject: string): T => {
	try {
		const envelope = JSON.parse(
			new TextDecoder().decode(bytes),
		) as ManifestEnvelope;
		if (
			typeof envelope.payload !== "string" ||
			typeof envelope.checksum !== "string" ||
			sha256Hex(new TextEncoder().encode(envelope.payload)) !==
				envelope.checksum
		) {
			throw new Error("checksum mismatch");
		}
		return JSON.parse(envelope.payload) as T;
	} catch (error) {
		throw new NativeDurabilityStorageCorruptionError(
			`Invalid ${subject} manifest`,
			error,
		);
	}
};

const isNotFound = (error: unknown): boolean =>
	(error as { code?: string })?.code === "ENOENT";

const syncDirectory = async (path: string): Promise<void> => {
	const handle = await requireNodeFs().open(path, "r");
	try {
		await handle.sync();
	} finally {
		await handle.close();
	}
};

const ensureDirectory = async (path: string, parent: string): Promise<void> => {
	let created = false;
	try {
		await requireNodeFs().mkdir(path);
		created = true;
	} catch (error) {
		if ((error as { code?: string }).code !== "EEXIST") throw error;
		if (!(await requireNodeFs().stat(path)).isDirectory()) throw error;
	}
	if (created) await syncDirectory(parent);
};

const diskFence = (lease: NativeDurabilityLease): DiskFence => ({
	epoch: lease.fence.epoch.toString(),
	ownerId: lease.fence.ownerId,
	domainId: lease.fence.domainId,
});

const diskScope = (scope: NativeDurabilityOperationScope): DiskScope => ({
	transactionId: scope.transactionId,
	txSequence: scope.txSequence.toString(),
	recordLsn: scope.recordLsn.toString(),
});

const blockFileName = (ordinal: number): string =>
	`${ordinal.toString().padStart(12, "0")}.block`;

const checkpointFileName = (generation: bigint): string =>
	`checkpoint-${generation}.bin`;

const transactionDirectoryName = (transactionId: string): string =>
	`tx-${sha256Hex(new TextEncoder().encode(transactionId))}`;

export type NativeDurabilityNodeStorageOptions = {
	directory: string;
	programId: Uint8Array;
};

const readFileIfExists = async (
	path: string,
): Promise<Uint8Array | undefined> => {
	try {
		return new Uint8Array(await requireNodeFs().readFile(path));
	} catch (error) {
		if (isNotFound(error)) return undefined;
		throw error;
	}
};

const writeNewFileAndSync = async (
	path: string,
	bytes: Uint8Array,
): Promise<void> => {
	const handle = await requireNodeFs().open(path, "wx+");
	try {
		await writeNativeDurabilityBytesFully(handle, bytes, 0);
		await handle.sync();
	} finally {
		await handle.close();
	}
};

const replaceFileAtomicallyAndSync = async (
	path: string,
	bytes: Uint8Array,
	temporarySuffix: string,
): Promise<void> => {
	const parent = requireNodePath().dirname(path);
	const temporary = `${path}.tmp-${temporarySuffix}`;
	await requireNodeFs().rm(temporary, { force: true });
	await writeNewFileAndSync(temporary, bytes);
	await requireNodeFs().rename(temporary, path);
	await syncDirectory(parent);
};

const writeImmutableFileAtomicallyAndSync = async (
	path: string,
	bytes: Uint8Array,
	temporarySuffix: string,
): Promise<void> => {
	const parent = requireNodePath().dirname(path);
	const temporary = `${path}.tmp-${temporarySuffix}`;
	await requireNodeFs().rm(temporary, { force: true });
	await writeNewFileAndSync(temporary, bytes);
	await requireNodeFs().rename(temporary, path);
	await syncDirectory(parent);
};

const parseUnsignedBigint = (value: unknown, subject: string): bigint => {
	if (typeof value !== "string" || !/^(0|[1-9][0-9]*)$/.test(value)) {
		throw new NativeDurabilityStorageCorruptionError(
			`${subject} must be an unsigned decimal bigint`,
		);
	}
	const parsed = BigInt(value);
	if (parsed > NATIVE_DURABILITY_MAX_U64) {
		throw new NativeDurabilityStorageCorruptionError(
			`${subject} exceeds unsigned 64-bit range`,
		);
	}
	return parsed;
};

const parseGenerationIdentity = (
	value: DiskGenerationIdentity | undefined,
	subject: string,
): GenerationIdentity | undefined => {
	if (value == null) return undefined;
	if (
		typeof value.transactionId !== "string" ||
		!value.transactionId ||
		new TextEncoder().encode(value.transactionId).byteLength >
			NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES ||
		typeof value.requestDigest !== "string"
	) {
		throw new NativeDurabilityStorageCorruptionError(
			`${subject} checkpoint identity fields are invalid`,
		);
	}
	const identity = {
		generation: parseUnsignedBigint(
			value.generation,
			`${subject} checkpoint generation`,
		),
		requestDigest: nativeDurabilityDigestFromHex(value.requestDigest),
		transactionId: value.transactionId,
		txSequence: parseUnsignedBigint(
			value.txSequence,
			`${subject} checkpoint transaction sequence`,
		),
	};
	if (identity.txSequence === 0n) {
		throw new NativeDurabilityStorageCorruptionError(
			`${subject} checkpoint transaction sequence must be non-zero`,
		);
	}
	return identity;
};

const diskGenerationIdentity = (
	identity: GenerationIdentity,
): DiskGenerationIdentity => ({
	generation: identity.generation.toString(),
	requestDigest: nativeDurabilityDigestHex(identity.requestDigest),
	transactionId: identity.transactionId,
	txSequence: identity.txSequence.toString(),
});

const generationIdentityMatches = (
	identity: GenerationIdentity,
	request: NativeDurabilityCheckpointRequest,
	requestDigest: Uint8Array,
): boolean =>
	identity.transactionId === request.scope.transactionId &&
	identity.txSequence === request.scope.txSequence &&
	nativeDurabilityBytesEqual(identity.requestDigest, requestDigest);

const parseStagingManifest = (
	bytes: Uint8Array,
	expectedTransactionId?: string,
): NativeDurabilityStagingManifest => {
	const disk = decodeManifest<DiskStagingManifest>(bytes, "staging");
	if (
		disk.version !== NATIVE_DURABILITY_STORAGE_VERSION ||
		!disk.scope ||
		typeof disk.scope.transactionId !== "string" ||
		!disk.scope.transactionId ||
		(expectedTransactionId != null &&
			disk.scope.transactionId !== expectedTransactionId) ||
		!disk.fence ||
		typeof disk.fence.ownerId !== "string" ||
		typeof disk.fence.domainId !== "string" ||
		!Array.isArray(disk.blocks)
	) {
		throw new NativeDurabilityStorageCorruptionError(
			"Staging manifest fields are invalid",
		);
	}
	if (
		new TextEncoder().encode(disk.scope.transactionId).byteLength >
		NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES
	) {
		throw new NativeDurabilityStorageCorruptionError(
			"Staging transaction ID exceeds the journal format limit",
		);
	}
	const blocks = disk.blocks.map((block, index) => {
		if (
			block.ordinal !== index ||
			typeof block.cid !== "string" ||
			!block.cid ||
			!Number.isSafeInteger(block.byteLength) ||
			block.byteLength < 0
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`Staging block reference ${index} is invalid`,
			);
		}
		return {
			ordinal: block.ordinal,
			cid: block.cid,
			byteLength: block.byteLength,
			digest: nativeDurabilityDigestFromHex(block.digest),
		};
	});
	const manifest: NativeDurabilityStagingManifest = {
		version: NATIVE_DURABILITY_STORAGE_VERSION,
		scope: {
			transactionId: disk.scope.transactionId,
			txSequence: parseUnsignedBigint(disk.scope.txSequence, "txSequence"),
			recordLsn: parseUnsignedBigint(disk.scope.recordLsn, "recordLsn"),
		},
		fence: {
			epoch: parseUnsignedBigint(disk.fence.epoch, "fence epoch"),
			ownerId: disk.fence.ownerId,
			domainId: disk.fence.domainId,
		},
		blocks,
		manifestDigest: sha256(bytes),
	};
	if (manifest.scope.txSequence === 0n || manifest.scope.recordLsn === 0n) {
		throw new NativeDurabilityStorageCorruptionError(
			"Staging manifest sequence and LSN must be non-zero",
		);
	}
	try {
		assertNativeDurabilityFence(manifest.fence);
	} catch (error) {
		throw new NativeDurabilityStorageCorruptionError(
			"Staging manifest fence is invalid",
			error,
		);
	}
	return manifest;
};

const parseDiskFence = (
	value: DiskFence | undefined,
	subject: string,
): NativeDurabilityLease["fence"] => {
	if (
		!value ||
		typeof value.ownerId !== "string" ||
		typeof value.domainId !== "string"
	) {
		throw new NativeDurabilityStorageCorruptionError(
			`${subject} fence is invalid`,
		);
	}
	const fence = {
		epoch: parseUnsignedBigint(value.epoch, `${subject} fence epoch`),
		ownerId: value.ownerId,
		domainId: value.domainId,
	};
	try {
		assertNativeDurabilityFence(fence);
	} catch (error) {
		throw new NativeDurabilityStorageCorruptionError(
			`${subject} fence is invalid`,
			error,
		);
	}
	return fence;
};

const parseDiskStagingCoverage = (
	value: unknown,
	checkpointLsn: bigint,
): NativeDurabilityCheckpoint["stagingCoverage"] => {
	if (!Array.isArray(value)) {
		throw new NativeDurabilityStorageCorruptionError(
			"Checkpoint staging coverage is invalid",
		);
	}
	const transactionIds = new Set<string>();
	return value.map((unknownCoverage, index) => {
		const coverage = unknownCoverage as Partial<DiskStagingCoverage>;
		if (
			typeof coverage.transactionId !== "string" ||
			!coverage.transactionId ||
			new TextEncoder().encode(coverage.transactionId).byteLength >
				NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES ||
			transactionIds.has(coverage.transactionId) ||
			typeof coverage.stagingManifestDigest !== "string"
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`Checkpoint staging coverage ${index} is invalid`,
			);
		}
		transactionIds.add(coverage.transactionId);
		const txSequence = parseUnsignedBigint(
			coverage.txSequence,
			`checkpoint staging coverage ${index} transaction sequence`,
		);
		const coveredThroughLsn = parseUnsignedBigint(
			coverage.coveredThroughLsn,
			`checkpoint staging coverage ${index} LSN`,
		);
		if (
			txSequence === 0n ||
			coveredThroughLsn === 0n ||
			coveredThroughLsn > checkpointLsn
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`Checkpoint staging coverage ${index} exceeds its checkpoint`,
			);
		}
		return {
			transactionId: coverage.transactionId,
			txSequence,
			coveredThroughLsn,
			stagingManifestDigest: nativeDurabilityDigestFromHex(
				coverage.stagingManifestDigest,
			),
		};
	});
};

const parseDiskRetainedTransactions = (
	value: unknown,
	txSequenceHighwater: bigint,
): NativeDurabilityCheckpointTransactionState[] => {
	if (!Array.isArray(value)) {
		throw new NativeDurabilityStorageCorruptionError(
			"Checkpoint retained transactions are invalid",
		);
	}
	const transactionIds = new Set<string>();
	const transactionSequences = new Set<bigint>();
	return value.map((unknownRetained, index) => {
		const retained = unknownRetained as Partial<DiskRetainedTransaction>;
		const txSequence = parseUnsignedBigint(
			retained.txSequence,
			`retained transaction ${index} sequence`,
		);
		if (
			txSequence === 0n ||
			txSequence > txSequenceHighwater ||
			typeof retained.transactionId !== "string" ||
			!retained.transactionId ||
			new TextEncoder().encode(retained.transactionId).byteLength >
				NATIVE_DURABILITY_MAX_TRANSACTION_ID_BYTES ||
			transactionIds.has(retained.transactionId) ||
			transactionSequences.has(txSequence) ||
			!Number.isInteger(retained.phase) ||
			(retained.phase as number) < 1 ||
			(retained.phase as number) > 6 ||
			!Number.isInteger(retained.operationKind) ||
			(retained.operationKind as number) < 1 ||
			(retained.operationKind as number) > 5 ||
			typeof retained.planDigest !== "string"
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`Retained transaction ${index} is invalid`,
			);
		}
		transactionIds.add(retained.transactionId);
		transactionSequences.add(txSequence);
		return {
			txSequence,
			transactionId: retained.transactionId,
			phase: retained.phase,
			operationKind: retained.operationKind,
			planDigest: nativeDurabilityDigestFromHex(retained.planDigest),
		} as NativeDurabilityCheckpointTransactionState;
	});
};

const cloneCheckpoint = (
	checkpoint: NativeDurabilityCheckpoint,
): NativeDurabilityCheckpoint => ({
	...checkpoint,
	bytes: copyNativeDurabilityBytes(checkpoint.bytes),
	digest: copyNativeDurabilityBytes(checkpoint.digest),
	originFence: { ...checkpoint.originFence },
	stagingCoverage: checkpoint.stagingCoverage.map((coverage) => ({
		...coverage,
		stagingManifestDigest: copyNativeDurabilityBytes(
			coverage.stagingManifestDigest,
		),
	})),
	retainedTransactions: checkpoint.retainedTransactions.map((retained) => ({
		...retained,
		planDigest: copyNativeDurabilityBytes(retained.planDigest),
	})),
});

const snapshotStageRequest = (
	request: NativeDurabilityStageRequest,
): NativeDurabilityStageRequest => {
	assertNativeDurabilityStageRequest(request);
	return {
		scope: { ...request.scope },
		blocks: [...request.blocks]
			.sort((left, right) => left.ordinal - right.ordinal)
			.map((block) => ({
				ordinal: block.ordinal,
				cid: block.cid,
				bytes: copyNativeDurabilityBytes(block.bytes),
				digest: copyNativeDurabilityBytes(block.digest),
			})),
	};
};

const snapshotJournalRequest = (
	request: NativeDurabilityJournalAppendRequest,
): NativeDurabilityJournalAppendRequest => {
	assertNativeDurabilityJournalAppendRequest(request);
	return {
		...request,
		frames: copyNativeDurabilityBytes(request.frames),
		framesDigest: copyNativeDurabilityBytes(request.framesDigest),
	};
};

const snapshotCheckpointRequest = (
	request: NativeDurabilityCheckpointRequest,
): NativeDurabilityCheckpointRequest => {
	assertNativeDurabilityCheckpointRequest(request);
	return {
		...request,
		scope: { ...request.scope },
		bytes: copyNativeDurabilityBytes(request.bytes),
		digest: copyNativeDurabilityBytes(request.digest),
		stagingCoverage: request.stagingCoverage.map((coverage) => ({
			...coverage,
			stagingManifestDigest: copyNativeDurabilityBytes(
				coverage.stagingManifestDigest,
			),
		})),
		retainedTransactions: request.retainedTransactions.map((retained) => ({
			...retained,
			planDigest: copyNativeDurabilityBytes(retained.planDigest),
		})),
	};
};

const snapshotDeleteRequest = (
	request: NativeDurabilityDeleteRequest,
): NativeDurabilityDeleteRequest => {
	assertNativeDurabilityDeleteRequest(request);
	return {
		scope: { ...request.scope },
		targets: request.targets.map((target) => ({ ...target })),
	};
};

const nodeNativeDurabilityStorageConstructionToken = Symbol(
	"NodeNativeDurabilityStorageConstructionToken",
);

export class NodeNativeDurabilityStorage implements NativeDurabilityStorage {
	readonly version = NATIVE_DURABILITY_STORAGE_VERSION;
	readonly kind = "node-fsync" as const;
	readonly crashSafe = true;
	readonly domainId: string;
	readonly fence: NativeDurabilityLease["fence"];

	private readonly rootDirectory: string;
	private readonly namespaceDirectory: string;
	private readonly stagingDirectory: string;
	private readonly checkpointDirectory: string;
	private readonly journalPath: string;
	private operationTail: Promise<void> = Promise.resolve();
	private closePromise?: Promise<void>;
	private closing = false;
	private closed = false;
	private barrierOrdinal = 0n;
	private strictDeleteCount = 0n;

	private constructor(
		constructionToken: typeof nodeNativeDurabilityStorageConstructionToken,
		private readonly lease: NativeDurabilityLease,
		private readonly journalCodec: NativeDurabilityJournalCodec,
		private readonly programId: Uint8Array,
		rootDirectory: string,
	) {
		if (constructionToken !== nodeNativeDurabilityStorageConstructionToken) {
			throw new TypeError(
				"NodeNativeDurabilityStorage must be created by createNodeNativeDurabilityStorage",
			);
		}
		if (!isNativeDurabilityJournalCodec(journalCodec)) {
			throw new TypeError(
				"NodeNativeDurabilityStorage requires the official native durability journal codec",
			);
		}
		assertNativeDurabilityFence(lease.fence);
		this.domainId = lease.fence.domainId;
		this.fence = Object.freeze({ ...lease.fence });
		const path = requireNodePath();
		const root = path.resolve(rootDirectory);
		this.rootDirectory = root;
		this.namespaceDirectory = path.join(root, STORAGE_DIRECTORY_NAME);
		this.stagingDirectory = path.join(
			this.namespaceDirectory,
			STAGING_DIRECTORY_NAME,
		);
		this.checkpointDirectory = path.join(
			this.namespaceDirectory,
			CHECKPOINT_DIRECTORY_NAME,
		);
		this.journalPath = path.join(this.namespaceDirectory, JOURNAL_FILE_NAME);
	}

	static async create(
		options: NativeDurabilityNodeStorageOptions,
	): Promise<NodeNativeDurabilityStorage> {
		if (
			!(options?.programId instanceof Uint8Array) ||
			options.programId.byteLength === 0 ||
			options.programId.byteLength >
				NATIVE_DURABILITY_JOURNAL_MAX_PROGRAM_ID_LENGTH
		) {
			throw new TypeError(
				`programId must contain 1-${NATIVE_DURABILITY_JOURNAL_MAX_PROGRAM_ID_LENGTH} bytes`,
			);
		}
		if (typeof options.directory !== "string" || !options.directory) {
			throw new TypeError("directory must be a non-empty string");
		}
		const requestedDirectory = options.directory;
		const programId = copyNativeDurabilityBytes(options.programId);
		await loadNodeModules();
		const fs = requireNodeFs();
		const canonicalDirectory = await fs.realpath(requestedDirectory);
		if (!(await fs.stat(canonicalDirectory)).isDirectory()) {
			throw new NativeDurabilityStorageUnsupportedError(
				`Native durability root is not a directory: ${canonicalDirectory}`,
			);
		}
		const lease = await acquireNativeDurabilityNodeLease(canonicalDirectory);
		try {
			const initialContext: NativeDurabilityJournalValidationContext = {
				checkpointLsn: 0n,
				checkpointTxSequenceHighwater: 0n,
				expectedProgramId: programId,
				expectedWriterDomainId: lease.fence.domainId,
				checkpointWriterEpoch: 0n,
				currentWriterEpoch: lease.fence.epoch,
				currentWriterOwnerId: lease.fence.ownerId,
				retainedTransactions: [],
			};
			const codec = await createNativeDurabilityJournalCodec(initialContext);
			const storage = new NodeNativeDurabilityStorage(
				nodeNativeDurabilityStorageConstructionToken,
				lease,
				codec,
				programId,
				canonicalDirectory,
			);
			await storage.enqueue(async () => storage.initialize());
			return storage;
		} catch (error) {
			await lease.close();
			throw error;
		}
	}

	private enqueue<T>(operation: () => Promise<T>): Promise<T> {
		if (this.closing || this.closed) {
			return Promise.reject(new NativeDurabilityStorageClosedError());
		}
		const result = this.operationTail.then(() =>
			this.lease.runWhileHeld(operation),
		);
		const settled = result.then(
			() => undefined,
			() => undefined,
		);
		this.operationTail = settled;
		return result;
	}

	private async initialize(): Promise<void> {
		const fs = requireNodeFs();
		const entries = await fs.readdir(this.rootDirectory);
		const namespaceExists = entries.includes(STORAGE_DIRECTORY_NAME);
		const legacyEntries = entries.filter(
			(entry) =>
				entry !== STORAGE_DIRECTORY_NAME &&
				entry !== NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME,
		);
		if (!namespaceExists && legacyEntries.length > 0) {
			throw new NativeDurabilityMigrationRequiredError(this.rootDirectory);
		}
		if (namespaceExists && legacyEntries.length > 0) {
			const existingCheckpoints = await this.readValidCheckpointsInternal();
			const existingJournalBase = await this.readJournalBaseCheckpoint();
			if (existingCheckpoints.length === 0 || !existingJournalBase) {
				throw new NativeDurabilityMigrationRequiredError(this.rootDirectory);
			}
		}
		await ensureDirectory(this.namespaceDirectory, this.rootDirectory);
		await ensureDirectory(this.stagingDirectory, this.namespaceDirectory);
		await ensureDirectory(this.checkpointDirectory, this.namespaceDirectory);
		const hasEstablishedAuthority = (
			await Promise.all([
				readFileIfExists(this.checkpointHighwaterPath()),
				readFileIfExists(this.checkpointManifestPath("a")),
				readFileIfExists(this.checkpointManifestPath("b")),
				readFileIfExists(
					requireNodePath().join(
						this.checkpointDirectory,
						JOURNAL_BASE_MANIFEST,
					),
				),
			])
		).some((bytes) => bytes != null);
		let journalExists = false;
		try {
			journalExists = (await fs.stat(this.journalPath)).isFile();
		} catch (error) {
			if (!isNotFound(error)) throw error;
		}
		if (!journalExists) {
			if (hasEstablishedAuthority) {
				throw new NativeDurabilityStorageCorruptionError(
					"Established native durability authority is missing journal.bin",
				);
			}
			const handle = await fs.open(this.journalPath, "wx+");
			try {
				await handle.sync();
			} finally {
				await handle.close();
			}
			await syncDirectory(this.namespaceDirectory);
		}
		// This is also a feature probe: crash-safe mode is unavailable when the
		// platform/filesystem cannot fsync a directory.
		await syncDirectory(this.namespaceDirectory);
		await this.ensureGenesisCheckpoint(legacyEntries.length === 0);
		await this.reconcileCheckpointGenerationState();
		const journalBase = await this.readJournalBaseCheckpoint();
		if (!journalBase) {
			throw new NativeDurabilityStorageCorruptionError(
				"Native durability journal base is missing after initialization",
			);
		}
		const journalHandle = await fs.open(this.journalPath, "r+");
		let journalScan: NativeDurabilityJournalScan;
		try {
			const journalLength = Number((await journalHandle.stat()).size);
			if (!Number.isSafeInteger(journalLength) || journalLength < 0) {
				throw new NativeDurabilityStorageCorruptionError(
					"Native durability journal size is not a safe byte offset",
				);
			}
			journalScan = this.journalCodec.scan(
				await readNativeDurabilityBytesFully(journalHandle, journalLength),
				this.checkpointValidationContext(journalBase),
			);
			// A reopen that observes complete prepared frames must cross a fresh
			// barrier before recovery can classify them as durable. This also makes
			// a complete-write/before-sync process death recoverable without the
			// original caller retaining its append request.
			await journalHandle.sync();
		} finally {
			await journalHandle.close();
		}
		const activeCheckpoint = (await this.readValidCheckpointsInternal())[0];
		if (
			!activeCheckpoint ||
			journalScan.lastRecordLsn < activeCheckpoint.checkpointLsn
		) {
			throw new NativeDurabilityStorageCorruptionError(
				"Journal authority ends before the active checkpoint watermark",
			);
		}
		await this.reconcileStagingOrphans(journalScan.records);
	}

	private checkpointManifestPath(slot: "a" | "b"): string {
		return requireNodePath().join(
			this.checkpointDirectory,
			slot === "a" ? CHECKPOINT_MANIFEST_A : CHECKPOINT_MANIFEST_B,
		);
	}

	private checkpointHighwaterPath(): string {
		return requireNodePath().join(
			this.checkpointDirectory,
			CHECKPOINT_HIGHWATER_FILE_NAME,
		);
	}

	private async readGenerationState(): Promise<GenerationState | undefined> {
		const bytes = await readFileIfExists(this.checkpointHighwaterPath());
		if (!bytes) return undefined;
		const disk = decodeManifest<DiskGenerationHighwater>(
			bytes,
			"checkpoint generation highwater",
		);
		if (disk.version !== this.version) {
			throw new NativeDurabilityStorageCorruptionError(
				"Checkpoint generation highwater version is invalid",
			);
		}
		const generation = parseUnsignedBigint(
			disk.generation,
			"checkpoint generation highwater",
		);
		const pending = parseGenerationIdentity(disk.pending, "pending");
		const completed = parseGenerationIdentity(disk.completed, "completed");
		if (
			(pending &&
				(pending.generation !== generation || pending.generation === 0n)) ||
			(completed &&
				(completed.generation > generation || completed.generation === 0n)) ||
			(pending && completed && completed.generation >= pending.generation)
		) {
			throw new NativeDurabilityStorageCorruptionError(
				"Checkpoint request identity exceeds its generation highwater",
			);
		}
		return { generation, pending, completed };
	}

	private async writeGenerationState(state: GenerationState): Promise<void> {
		await replaceFileAtomicallyAndSync(
			this.checkpointHighwaterPath(),
			encodeManifest({
				version: this.version,
				generation: state.generation.toString(),
				pending: state.pending
					? diskGenerationIdentity(state.pending)
					: undefined,
				completed: state.completed
					? diskGenerationIdentity(state.completed)
					: undefined,
			} satisfies DiskGenerationHighwater),
			`${this.lease.fence.epoch}-${this.lease.fence.ownerId}`,
		);
	}

	private async readCheckpointSlot(
		slot: "a" | "b",
	): Promise<NativeDurabilityCheckpoint | undefined> {
		return this.readCheckpointManifest(
			this.checkpointManifestPath(slot),
			slot,
			`checkpoint manifest ${slot}`,
		);
	}

	private async readCheckpointManifest(
		path: string,
		manifestSlot: "a" | "b",
		subject: string,
	): Promise<NativeDurabilityCheckpoint | undefined> {
		const manifestBytes = await readFileIfExists(path);
		if (!manifestBytes) return undefined;
		const disk = decodeManifest<DiskCheckpointManifest>(manifestBytes, subject);
		if (
			disk.version !== this.version ||
			typeof disk.file !== "string" ||
			!Number.isSafeInteger(disk.byteLength) ||
			disk.byteLength < 0 ||
			typeof disk.digest !== "string"
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`${subject} fields are invalid`,
			);
		}
		const persistedProgramId = bytesFromHex(
			disk.programId,
			`${subject} program ID`,
		);
		if (!nativeDurabilityBytesEqual(persistedProgramId, this.programId)) {
			throw new NativeDurabilityStorageCorruptionError(
				`${subject} belongs to another program`,
			);
		}
		const generation = parseUnsignedBigint(
			disk.generation,
			`${subject} generation`,
		);
		const checkpointLsn = parseUnsignedBigint(
			disk.checkpointLsn,
			`${subject} LSN`,
		);
		const txSequenceHighwater = parseUnsignedBigint(
			disk.txSequenceHighwater,
			`${subject} transaction highwater`,
		);
		if (disk.file !== checkpointFileName(generation)) {
			throw new NativeDurabilityStorageCorruptionError(
				`${subject} has a noncanonical generation file`,
			);
		}
		const originFence = parseDiskFence(disk.originFence, subject);
		if (
			originFence.domainId !== this.domainId ||
			originFence.epoch > this.lease.fence.epoch ||
			(originFence.epoch === this.lease.fence.epoch &&
				originFence.ownerId !== this.lease.fence.ownerId)
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`${subject} has an invalid durability fence`,
			);
		}
		const stagingCoverage = parseDiskStagingCoverage(
			disk.stagingCoverage,
			checkpointLsn,
		);
		const retainedTransactions = parseDiskRetainedTransactions(
			disk.retainedTransactions,
			txSequenceHighwater,
		);
		if (
			(generation === 0n &&
				(checkpointLsn !== 0n ||
					txSequenceHighwater !== 0n ||
					stagingCoverage.length !== 0 ||
					retainedTransactions.length !== 0 ||
					disk.byteLength !== 0)) ||
			(generation > 0n &&
				(checkpointLsn === 0n || txSequenceHighwater === 0n)) ||
			stagingCoverage.some(
				(coverage) => coverage.txSequence > txSequenceHighwater,
			)
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`${subject} checkpoint watermarks are inconsistent`,
			);
		}
		for (const coverage of stagingCoverage) {
			const retained = retainedTransactions.find(
				(candidate) => candidate.transactionId === coverage.transactionId,
			);
			if (
				!retained ||
				retained.txSequence !== coverage.txSequence ||
				retained.phase !== NativeDurabilityPhase.Clean
			) {
				throw new NativeDurabilityStorageCorruptionError(
					`${subject} staging coverage lacks an exact retained CLEAN transaction`,
				);
			}
		}
		const checkpointBytes = await readFileIfExists(
			requireNodePath().join(this.checkpointDirectory, disk.file),
		);
		const digest = nativeDurabilityDigestFromHex(disk.digest);
		if (
			!checkpointBytes ||
			checkpointBytes.byteLength !== disk.byteLength ||
			!nativeDurabilityBytesEqual(sha256(checkpointBytes), digest)
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`Checkpoint generation ${generation} is missing or corrupt`,
			);
		}
		return {
			version: this.version,
			generation,
			checkpointLsn,
			txSequenceHighwater,
			bytes: checkpointBytes,
			digest,
			originFence,
			manifestSlot,
			stagingCoverage,
			retainedTransactions,
		};
	}

	private readJournalBaseCheckpoint(): Promise<
		NativeDurabilityCheckpoint | undefined
	> {
		return this.readCheckpointManifest(
			requireNodePath().join(this.checkpointDirectory, JOURNAL_BASE_MANIFEST),
			"a",
			"journal base manifest",
		);
	}

	private async readValidCheckpointsInternal(): Promise<
		NativeDurabilityCheckpoint[]
	> {
		const checkpoints = (
			await Promise.all([
				this.readCheckpointSlot("a"),
				this.readCheckpointSlot("b"),
			])
		).filter(
			(checkpoint): checkpoint is NativeDurabilityCheckpoint =>
				checkpoint != null,
		);
		if (
			checkpoints.length === 2 &&
			checkpoints[0].generation === checkpoints[1].generation
		) {
			throw new NativeDurabilityStorageCorruptionError(
				"Checkpoint manifest slots select the same generation",
			);
		}
		return checkpoints.sort((left, right) =>
			left.generation < right.generation
				? 1
				: left.generation > right.generation
					? -1
					: 0,
		);
	}

	private async ensureGenesisCheckpoint(mayCreate: boolean): Promise<void> {
		const checkpoints = await this.readValidCheckpointsInternal();
		if (checkpoints.length > 0) {
			const state = await this.readGenerationState();
			if (state == null || state.generation < checkpoints[0].generation) {
				throw new NativeDurabilityStorageCorruptionError(
					"Checkpoint generation highwater is missing or regressed",
				);
			}
			const selectedPending = state.pending
				? checkpoints.find(
						(checkpoint) => checkpoint.generation === state.pending?.generation,
					)
				: undefined;
			const expectedActiveGeneration =
				selectedPending?.generation ?? state.completed?.generation ?? 0n;
			if (checkpoints[0].generation !== expectedActiveGeneration) {
				throw new NativeDurabilityStorageCorruptionError(
					`Active checkpoint generation ${checkpoints[0].generation} does not match completed generation ${expectedActiveGeneration}`,
				);
			}
			const journalBase = await this.readJournalBaseCheckpoint();
			if (!journalBase || journalBase.generation !== 0n) {
				throw new NativeDurabilityStorageCorruptionError(
					"Journal base checkpoint is missing or unsupported",
				);
			}
			return;
		}
		if (!mayCreate) {
			throw new NativeDurabilityMigrationRequiredError(this.rootDirectory);
		}
		const journal = await readFileIfExists(this.journalPath);
		if (journal && journal.byteLength !== 0) {
			throw new NativeDurabilityStorageCorruptionError(
				"Cannot create genesis for a nonempty durability journal",
			);
		}
		const existingState = await this.readGenerationState();
		if (
			existingState != null &&
			(existingState.generation !== 0n ||
				existingState.pending != null ||
				existingState.completed != null)
		) {
			throw new NativeDurabilityStorageCorruptionError(
				"Incomplete genesis has a nonzero checkpoint generation highwater",
			);
		}
		if (existingState == null)
			await this.writeGenerationState({ generation: 0n });
		const genesisBytes = new Uint8Array();
		const genesisPath = requireNodePath().join(
			this.checkpointDirectory,
			checkpointFileName(0n),
		);
		const existingGenesis = await readFileIfExists(genesisPath);
		if (existingGenesis) {
			if (existingGenesis.byteLength !== 0) {
				throw new NativeDurabilityStorageCorruptionError(
					"Incomplete genesis checkpoint conflicts with the canonical empty checkpoint",
				);
			}
			const handle = await requireNodeFs().open(genesisPath, "r+");
			try {
				await handle.sync();
			} finally {
				await handle.close();
			}
			await syncDirectory(this.checkpointDirectory);
		} else {
			await writeImmutableFileAtomicallyAndSync(
				genesisPath,
				genesisBytes,
				`${this.lease.fence.epoch}-${this.lease.fence.ownerId}`,
			);
		}
		const manifest: DiskCheckpointManifest = {
			version: this.version,
			programId: bytesToHex(this.programId),
			generation: "0",
			checkpointLsn: "0",
			txSequenceHighwater: "0",
			file: checkpointFileName(0n),
			byteLength: 0,
			digest: sha256Hex(genesisBytes),
			originFence: diskFence(this.lease),
			stagingCoverage: [],
			retainedTransactions: [],
		};
		const journalBasePath = requireNodePath().join(
			this.checkpointDirectory,
			JOURNAL_BASE_MANIFEST,
		);
		const existingJournalBase = await this.readJournalBaseCheckpoint();
		if (existingJournalBase) {
			if (existingJournalBase.generation !== 0n) {
				throw new NativeDurabilityStorageCorruptionError(
					"Incomplete genesis has a nonzero journal base",
				);
			}
		} else {
			await replaceFileAtomicallyAndSync(
				journalBasePath,
				encodeManifest(manifest),
				`${this.lease.fence.epoch}-${this.lease.fence.ownerId}`,
			);
		}
		await replaceFileAtomicallyAndSync(
			this.checkpointManifestPath("a"),
			encodeManifest(manifest),
			`${this.lease.fence.epoch}-${this.lease.fence.ownerId}`,
		);
	}

	private async reconcileCheckpointGenerationState(): Promise<void> {
		const state = await this.readGenerationState();
		if (!state) {
			throw new NativeDurabilityStorageCorruptionError(
				"Checkpoint generation state is missing",
			);
		}
		if (state.pending) {
			const checkpoints = await this.readValidCheckpointsInternal();
			const selected = checkpoints.find(
				(checkpoint) => checkpoint.generation === state.pending?.generation,
			);
			if (selected) {
				if (selected.generation !== checkpoints[0]?.generation) {
					throw new NativeDurabilityStorageCorruptionError(
						"Pending checkpoint generation is selected behind a newer manifest",
					);
				}
				await this.writeGenerationState({
					generation: state.generation,
					completed: state.pending,
				});
			} else {
				await requireNodeFs().rm(
					requireNodePath().join(
						this.checkpointDirectory,
						checkpointFileName(state.pending.generation),
					),
					{ force: true },
				);
				await syncDirectory(this.checkpointDirectory);
				await this.writeGenerationState({
					generation: state.generation,
					completed: state.completed,
				});
			}
		}
		const temporaryPrefixes = [
			CHECKPOINT_HIGHWATER_FILE_NAME,
			CHECKPOINT_MANIFEST_A,
			CHECKPOINT_MANIFEST_B,
			JOURNAL_BASE_MANIFEST,
		].map((name) => `${name}.tmp-`);
		let removedTemporary = false;
		for (const entry of await requireNodeFs().readdir(
			this.checkpointDirectory,
			{ withFileTypes: true },
		)) {
			if (
				entry.isFile() &&
				(temporaryPrefixes.some((prefix) => entry.name.startsWith(prefix)) ||
					/^checkpoint-(0|[1-9][0-9]*)\.bin\.tmp-/.test(entry.name))
			) {
				await requireNodeFs().rm(
					requireNodePath().join(this.checkpointDirectory, entry.name),
					{ force: true },
				);
				removedTemporary = true;
			}
		}
		if (removedTemporary) await syncDirectory(this.checkpointDirectory);
	}

	private async reconcileStagingOrphans(
		records: readonly NativeDurabilityJournalRecord[],
	): Promise<void> {
		const activeCheckpoint = (await this.readValidCheckpointsInternal())[0];
		if (!activeCheckpoint) {
			throw new NativeDurabilityStorageCorruptionError(
				"Staging reconciliation requires an active checkpoint",
			);
		}
		const journalTransactionDirectories = new Set(
			records.map((record) => transactionDirectoryName(record.transactionId)),
		);
		let removed = false;
		for (const entry of await requireNodeFs().readdir(this.stagingDirectory, {
			withFileTypes: true,
		})) {
			if (!entry.isDirectory() || !/^tx-[0-9a-f]{64}$/.test(entry.name)) {
				throw new NativeDurabilityStorageCorruptionError(
					`Unexpected private staging entry ${entry.name}`,
				);
			}
			const transactionDirectory = requireNodePath().join(
				this.stagingDirectory,
				entry.name,
			);
			if (!journalTransactionDirectories.has(entry.name)) {
				await requireNodeFs().rm(transactionDirectory, {
					recursive: true,
					force: true,
				});
				removed = true;
				continue;
			}
			const manifestBytes = await readFileIfExists(
				requireNodePath().join(
					transactionDirectory,
					STAGING_MANIFEST_FILE_NAME,
				),
			);
			if (!manifestBytes) {
				const record = records.findLast(
					(candidate) =>
						transactionDirectoryName(candidate.transactionId) === entry.name,
				);
				if (
					record &&
					this.checkpointAuthorizesStagingDeletion(
						activeCheckpoint,
						record.transactionId,
						records,
					)
				) {
					await requireNodeFs().rm(transactionDirectory, {
						recursive: true,
						force: true,
					});
					removed = true;
					continue;
				}
				throw new NativeDurabilityStorageCorruptionError(
					`Journal-owned staging directory ${entry.name} has no manifest`,
				);
			}
			const parsed = parseStagingManifest(manifestBytes);
			if (entry.name !== transactionDirectoryName(parsed.scope.transactionId)) {
				throw new NativeDurabilityStorageCorruptionError(
					`Staging directory ${entry.name} does not match its manifest`,
				);
			}
			await this.readStagingManifestInternal(parsed.scope.transactionId);
			if (
				this.checkpointAuthorizesStagingDeletion(
					activeCheckpoint,
					parsed.scope.transactionId,
					records,
					parsed,
				)
			) {
				await requireNodeFs().rm(transactionDirectory, {
					recursive: true,
					force: true,
				});
				removed = true;
				continue;
			}
			for (const block of parsed.blocks) {
				const blockBytes = await readFileIfExists(
					requireNodePath().join(
						transactionDirectory,
						blockFileName(block.ordinal),
					),
				);
				if (
					!blockBytes ||
					blockBytes.byteLength !== block.byteLength ||
					!nativeDurabilityBytesEqual(sha256(blockBytes), block.digest)
				) {
					throw new NativeDurabilityStorageCorruptionError(
						`Journal-owned staged block ${block.ordinal} is missing or corrupt`,
					);
				}
			}
		}
		if (removed) await syncDirectory(this.stagingDirectory);
	}

	private checkpointAuthorizesStagingDeletion(
		checkpoint: NativeDurabilityCheckpoint,
		transactionId: string,
		records: readonly NativeDurabilityJournalRecord[],
		staged?: NativeDurabilityStagingManifest,
	): boolean {
		const coverage = checkpoint.stagingCoverage.find(
			(candidate) => candidate.transactionId === transactionId,
		);
		const retained = checkpoint.retainedTransactions.find(
			(candidate) => candidate.transactionId === transactionId,
		);
		const record = records.findLast(
			(candidate) => candidate.transactionId === transactionId,
		);
		return !!(
			coverage &&
			retained &&
			record &&
			retained.phase === NativeDurabilityPhase.Clean &&
			record.phase === NativeDurabilityPhase.Clean &&
			coverage.txSequence === retained.txSequence &&
			coverage.txSequence === record.txSequence &&
			retained.operationKind === record.operationKind &&
			nativeDurabilityBytesEqual(retained.planDigest, record.planDigest) &&
			coverage.coveredThroughLsn >= record.recordLsn &&
			coverage.coveredThroughLsn <= checkpoint.checkpointLsn &&
			(!staged ||
				(staged.scope.txSequence === coverage.txSequence &&
					coverage.coveredThroughLsn >= staged.scope.recordLsn &&
					nativeDurabilityBytesEqual(
						coverage.stagingManifestDigest,
						staged.manifestDigest,
					)))
		);
	}

	private checkpointValidationContext(
		checkpoint: NativeDurabilityCheckpoint,
	): NativeDurabilityJournalValidationContext {
		return {
			checkpointLsn: checkpoint.checkpointLsn,
			checkpointTxSequenceHighwater: checkpoint.txSequenceHighwater,
			expectedProgramId: copyNativeDurabilityBytes(this.programId),
			expectedWriterDomainId: this.domainId,
			checkpointWriterEpoch: checkpoint.originFence.epoch,
			checkpointWriterOwnerId: checkpoint.originFence.ownerId,
			currentWriterEpoch: this.lease.fence.epoch,
			currentWriterOwnerId: this.lease.fence.ownerId,
			retainedTransactions: checkpoint.retainedTransactions.map((retained) => ({
				...retained,
				planDigest: copyNativeDurabilityBytes(retained.planDigest),
			})),
		};
	}

	private transactionDirectory(transactionId: string): string {
		return requireNodePath().join(
			this.stagingDirectory,
			transactionDirectoryName(transactionId),
		);
	}

	private async readStagingManifestInternal(
		transactionId: string,
	): Promise<NativeDurabilityStagingManifest | undefined> {
		const bytes = await readFileIfExists(
			requireNodePath().join(
				this.transactionDirectory(transactionId),
				STAGING_MANIFEST_FILE_NAME,
			),
		);
		if (!bytes) return undefined;
		const manifest = parseStagingManifest(bytes, transactionId);
		if (
			manifest.fence.domainId !== this.lease.fence.domainId ||
			manifest.fence.epoch > this.lease.fence.epoch ||
			(manifest.fence.epoch === this.lease.fence.epoch &&
				manifest.fence.ownerId !== this.lease.fence.ownerId)
		) {
			throw new NativeDurabilityStorageCorruptionError(
				`Staging transaction ${transactionId} has an invalid persisted fence`,
			);
		}
		return manifest;
	}

	async stageAndSync(
		unsafeRequest: NativeDurabilityStageRequest,
	): Promise<NativeDurabilityStageReceipt> {
		const request = snapshotStageRequest(unsafeRequest);
		return this.enqueue(async () => {
			for (const block of request.blocks) {
				if (!nativeDurabilityBytesEqual(sha256(block.bytes), block.digest)) {
					throw new NativeDurabilityDigestMismatchError(
						`staged block ${block.ordinal}`,
					);
				}
			}
			const transactionDirectory = this.transactionDirectory(
				request.scope.transactionId,
			);
			const references: NativeDurabilityStagedBlockReference[] =
				request.blocks.map((block) => ({
					ordinal: block.ordinal,
					cid: block.cid,
					byteLength: block.bytes.byteLength,
					digest: copyNativeDurabilityBytes(block.digest),
				}));
			let started = false;
			try {
				let transactionDirectoryExists = false;
				try {
					transactionDirectoryExists = (
						await requireNodeFs().stat(transactionDirectory)
					).isDirectory();
				} catch (error) {
					if (!isNotFound(error)) throw error;
				}
				const existing = await this.readStagingManifestInternal(
					request.scope.transactionId,
				);
				let manifest: NativeDurabilityStagingManifest;
				if (existing) {
					if (
						existing.scope.txSequence !== request.scope.txSequence ||
						existing.scope.recordLsn !== request.scope.recordLsn ||
						existing.blocks.length !== references.length ||
						existing.blocks.some(
							(block, index) =>
								block.ordinal !== references[index].ordinal ||
								block.cid !== references[index].cid ||
								block.byteLength !== references[index].byteLength ||
								!nativeDurabilityBytesEqual(
									block.digest,
									references[index].digest,
								),
						)
					) {
						throw new NativeDurabilityStorageCorruptionError(
							`Staging transaction ${request.scope.transactionId} conflicts with its durable manifest`,
						);
					}
					for (const block of existing.blocks) {
						const blockPath = requireNodePath().join(
							transactionDirectory,
							blockFileName(block.ordinal),
						);
						const bytes = await readFileIfExists(blockPath);
						if (
							!bytes ||
							bytes.byteLength !== block.byteLength ||
							!nativeDurabilityBytesEqual(sha256(bytes), block.digest)
						) {
							throw new NativeDurabilityStorageCorruptionError(
								`Staged block ${block.ordinal} is missing or corrupt`,
							);
						}
						const handle = await requireNodeFs().open(blockPath, "r+");
						try {
							await handle.sync();
						} finally {
							await handle.close();
						}
					}
					await syncDirectory(transactionDirectory);
					manifest = existing;
				} else {
					started = true;
					if (!transactionDirectoryExists) {
						await ensureDirectory(transactionDirectory, this.stagingDirectory);
					} else {
						const expectedFiles = new Set(
							request.blocks.map((block) => blockFileName(block.ordinal)),
						);
						for (const entry of await requireNodeFs().readdir(
							transactionDirectory,
							{
								withFileTypes: true,
							},
						)) {
							const isManifestTemporary = entry.name.startsWith(
								`${STAGING_MANIFEST_FILE_NAME}.tmp-`,
							);
							const isBlockTemporary = [...expectedFiles].some((name) =>
								entry.name.startsWith(`${name}.tmp-`),
							);
							if (
								!entry.isFile() ||
								(!expectedFiles.has(entry.name) &&
									!isManifestTemporary &&
									!isBlockTemporary)
							) {
								throw new NativeDurabilityStorageCorruptionError(
									`Unexpected private staging entry ${entry.name}`,
								);
							}
							if (isManifestTemporary || isBlockTemporary) {
								await requireNodeFs().rm(
									requireNodePath().join(transactionDirectory, entry.name),
									{ force: true },
								);
							}
						}
						await syncDirectory(transactionDirectory);
					}
					for (const block of request.blocks) {
						const blockPath = requireNodePath().join(
							transactionDirectory,
							blockFileName(block.ordinal),
						);
						const existingBlock = await readFileIfExists(blockPath);
						if (existingBlock) {
							if (
								existingBlock.byteLength !== block.bytes.byteLength ||
								!nativeDurabilityBytesEqual(sha256(existingBlock), block.digest)
							) {
								throw new NativeDurabilityStorageCorruptionError(
									`Orphan staged block ${block.ordinal} conflicts with retry`,
								);
							}
							const handle = await requireNodeFs().open(blockPath, "r+");
							try {
								await handle.sync();
							} finally {
								await handle.close();
							}
						} else {
							await writeImmutableFileAtomicallyAndSync(
								blockPath,
								block.bytes,
								`${this.lease.fence.epoch}-${this.lease.fence.ownerId}`,
							);
						}
					}
					const payload: DiskStagingManifest = {
						version: this.version,
						scope: diskScope(request.scope),
						fence: diskFence(this.lease),
						blocks: references.map((block) => ({
							ordinal: block.ordinal,
							cid: block.cid,
							byteLength: block.byteLength,
							digest: nativeDurabilityDigestHex(block.digest),
						})),
					};
					const manifestBytes = encodeManifest(payload);
					await replaceFileAtomicallyAndSync(
						requireNodePath().join(
							transactionDirectory,
							STAGING_MANIFEST_FILE_NAME,
						),
						manifestBytes,
						`${this.lease.fence.epoch}-${this.lease.fence.ownerId}`,
					);
					manifest = parseStagingManifest(
						manifestBytes,
						request.scope.transactionId,
					);
				}
				this.barrierOrdinal++;
				return {
					version: this.version,
					kind: "stage",
					domainId: this.domainId,
					fence: { ...this.lease.fence },
					transactionId: request.scope.transactionId,
					txSequence: request.scope.txSequence,
					firstRecordLsn: request.scope.recordLsn,
					lastRecordLsn: request.scope.recordLsn,
					scopeDigest: sha256(encodeNativeDurabilityCanonical(request)),
					barrierOrdinal: this.barrierOrdinal,
					blocks: manifest.blocks.map((block) => ({
						...block,
						digest: copyNativeDurabilityBytes(block.digest),
					})),
					manifestDigest: copyNativeDurabilityBytes(manifest.manifestDigest),
				};
			} catch (error) {
				if (!started) throw error;
				throw new NativeDurabilityOutcomeUnknownError(
					"stage",
					request.scope.transactionId,
					request.scope.txSequence,
					error,
				);
			}
		});
	}

	async readStagingManifest(
		transactionId: string,
	): Promise<NativeDurabilityStagingManifest | undefined> {
		assertNativeDurabilityOperationScope({
			transactionId,
			txSequence: 0n,
			recordLsn: 0n,
		});
		return this.enqueue(async () => {
			const manifest = await this.readStagingManifestInternal(transactionId);
			return (
				manifest && {
					...manifest,
					scope: { ...manifest.scope },
					fence: { ...manifest.fence },
					blocks: manifest.blocks.map((block) => ({
						...block,
						digest: copyNativeDurabilityBytes(block.digest),
					})),
					manifestDigest: copyNativeDurabilityBytes(manifest.manifestDigest),
				}
			);
		});
	}

	async readStagedBlock(
		transactionId: string,
		ordinal: number,
	): Promise<Uint8Array | undefined> {
		assertNativeDurabilityOperationScope({
			transactionId,
			txSequence: 0n,
			recordLsn: 0n,
		});
		if (!Number.isSafeInteger(ordinal) || ordinal < 0) {
			return Promise.reject(new RangeError("Invalid staging ordinal"));
		}
		return this.enqueue(async () => {
			const manifest = await this.readStagingManifestInternal(transactionId);
			const reference = manifest?.blocks.find(
				(block) => block.ordinal === ordinal,
			);
			if (!reference) return undefined;
			const bytes = await readFileIfExists(
				requireNodePath().join(
					this.transactionDirectory(transactionId),
					blockFileName(ordinal),
				),
			);
			if (
				!bytes ||
				bytes.byteLength !== reference.byteLength ||
				!nativeDurabilityBytesEqual(sha256(bytes), reference.digest)
			) {
				throw new NativeDurabilityStorageCorruptionError(
					`Staged block ${ordinal} is missing or corrupt`,
				);
			}
			return bytes;
		});
	}

	private async listStagingTransactionIdsInternal(): Promise<string[]> {
		const entries = await requireNodeFs().readdir(this.stagingDirectory, {
			withFileTypes: true,
		});
		const transactionIds: string[] = [];
		for (const entry of entries) {
			if (!entry.isDirectory() || !entry.name.startsWith("tx-")) {
				throw new NativeDurabilityStorageCorruptionError(
					`Unexpected private staging entry ${entry.name}`,
				);
			}
			const bytes = await readFileIfExists(
				requireNodePath().join(
					this.stagingDirectory,
					entry.name,
					STAGING_MANIFEST_FILE_NAME,
				),
			);
			if (!bytes) {
				throw new NativeDurabilityStorageCorruptionError(
					`Incomplete private staging directory ${entry.name}`,
				);
			}
			const transactionId = parseStagingManifest(bytes).scope.transactionId;
			if (entry.name !== transactionDirectoryName(transactionId)) {
				throw new NativeDurabilityStorageCorruptionError(
					`Staging directory ${entry.name} does not match its transaction manifest`,
				);
			}
			await this.readStagingManifestInternal(transactionId);
			transactionIds.push(transactionId);
		}
		return transactionIds.sort();
	}

	async listStagingTransactionIds(): Promise<string[]> {
		return this.enqueue(async () => this.listStagingTransactionIdsInternal());
	}

	async appendJournalAndSync(
		unsafeRequest: NativeDurabilityJournalAppendRequest,
	): Promise<NativeDurabilityJournalReceipt> {
		const request = snapshotJournalRequest(unsafeRequest);
		return this.enqueue(async () => {
			if (
				!nativeDurabilityBytesEqual(
					sha256(request.frames),
					request.framesDigest,
				)
			) {
				throw new NativeDurabilityDigestMismatchError("journal frames");
			}
			const checkpoint = await this.readJournalBaseCheckpoint();
			if (!checkpoint) {
				throw new NativeDurabilityStorageCorruptionError(
					"Native durability journal has no valid checkpoint authority",
				);
			}
			const context = this.checkpointValidationContext(checkpoint);
			const handle = await requireNodeFs().open(this.journalPath, "r+");
			let started = false;
			try {
				const actualOffset = Number((await handle.stat()).size);
				if (!Number.isSafeInteger(actualOffset) || actualOffset < 0) {
					throw new NativeDurabilityStorageCorruptionError(
						"Native durability journal size is not a safe byte offset",
					);
				}
				const endOffset = request.expectedOffset + request.frames.byteLength;
				if (!Number.isSafeInteger(endOffset)) {
					throw new NativeDurabilityStorageCorruptionError(
						"Candidate journal end offset is not safe",
					);
				}
				const alreadyWritten = actualOffset === endOffset;
				if (actualOffset < request.expectedOffset || actualOffset > endOffset) {
					throw new NativeDurabilityJournalOffsetConflictError(
						request.expectedOffset,
						actualOffset,
					);
				}
				const observed = await readNativeDurabilityBytesFully(
					handle,
					actualOffset,
				);
				const prefix = observed.slice(0, request.expectedOffset);
				const existingRequestLength = actualOffset - request.expectedOffset;
				if (
					!nativeDurabilityBytesEqual(
						observed.slice(request.expectedOffset),
						request.frames.slice(0, existingRequestLength),
					)
				) {
					throw new NativeDurabilityStorageCorruptionError(
						"Existing journal suffix conflicts with the exact append retry",
					);
				}
				const prefixScan = this.journalCodec.scan(prefix, context);
				if (
					prefixScan.incompleteTailOffset != null ||
					prefixScan.validLength !== prefix.byteLength
				) {
					throw new NativeDurabilityIncompleteTailMismatchError(
						"Journal must be reconciled before appending another frame",
					);
				}
				const candidate = new Uint8Array(
					prefix.byteLength + request.frames.byteLength,
				);
				candidate.set(prefix);
				candidate.set(request.frames, prefix.byteLength);
				const candidateScan = this.journalCodec.scan(candidate, context);
				if (
					candidateScan.incompleteTailOffset != null ||
					candidateScan.validLength !== candidate.byteLength
				) {
					throw new NativeDurabilityStorageCorruptionError(
						"Candidate journal append is not a complete canonical frame sequence",
					);
				}
				const appendedRecords = candidateScan.records.slice(
					prefixScan.records.length,
				);
				if (appendedRecords.length === 0) {
					throw new NativeDurabilityStorageCorruptionError(
						"Candidate journal append contains no records",
					);
				}
				const firstRecord = appendedRecords[0];
				const lastRecord = appendedRecords[appendedRecords.length - 1];
				if (
					firstRecord.recordLsn !== request.firstRecordLsn ||
					lastRecord.recordLsn !== request.lastRecordLsn ||
					appendedRecords.some(
						(record) =>
							record.transactionId !== request.transactionId ||
							record.txSequence !== request.txSequence ||
							record.writerDomainId !== this.lease.fence.domainId ||
							(existingRequestLength === 0 &&
								(record.writerEpoch !== this.lease.fence.epoch ||
									record.writerOwnerId !== this.lease.fence.ownerId)),
					)
				) {
					throw new NativeDurabilityStorageCorruptionError(
						"Candidate journal metadata does not match the intended transaction or current fence",
					);
				}
				started = true;
				if (!alreadyWritten) {
					await writeNativeDurabilityBytesFully(
						handle,
						request.frames.slice(existingRequestLength),
						actualOffset,
					);
				}
				await handle.sync();
				this.barrierOrdinal++;
				return {
					version: this.version,
					kind: "journal-append",
					domainId: this.domainId,
					fence: { ...this.lease.fence },
					transactionId: firstRecord.transactionId,
					txSequence: firstRecord.txSequence,
					firstRecordLsn: firstRecord.recordLsn,
					lastRecordLsn: lastRecord.recordLsn,
					scopeDigest: sha256(encodeNativeDurabilityCanonical(request)),
					barrierOrdinal: this.barrierOrdinal,
					offset: request.expectedOffset,
					endOffset,
					framesDigest: copyNativeDurabilityBytes(request.framesDigest),
				};
			} catch (error) {
				if (!started) throw error;
				throw new NativeDurabilityOutcomeUnknownError(
					"journal-append",
					request.transactionId,
					request.txSequence,
					error,
				);
			} finally {
				try {
					await handle.close();
				} catch {
					// A completed strict barrier is authoritative. When the body failed,
					// that typed failure is authoritative instead. Close cannot alter either.
				}
			}
		});
	}

	async readJournal(): Promise<Uint8Array> {
		return this.enqueue(
			async () =>
				new Uint8Array(await requireNodeFs().readFile(this.journalPath)),
		);
	}

	async reconcileIncompleteJournalTailAndSync(
		unsafeRequest: NativeDurabilityIncompleteTailReconciliationRequest,
	): Promise<NativeDurabilityJournalReconciliationReceipt> {
		const request = { ...unsafeRequest };
		try {
			assertNativeDurabilityOperationScope({
				...request,
				recordLsn: 0n,
			});
		} catch (error) {
			return Promise.reject(error);
		}
		if (request.txSequence === 0n) {
			return Promise.reject(
				new TypeError("Invalid incomplete-tail reconciliation request"),
			);
		}
		return this.enqueue(async () => {
			const checkpoint = await this.readJournalBaseCheckpoint();
			if (!checkpoint) {
				throw new NativeDurabilityStorageCorruptionError(
					"Native durability journal has no valid checkpoint authority",
				);
			}
			const handle = await requireNodeFs().open(this.journalPath, "r+");
			let started = false;
			try {
				const observedLength = Number((await handle.stat()).size);
				if (!Number.isSafeInteger(observedLength) || observedLength < 0) {
					throw new NativeDurabilityStorageCorruptionError(
						"Native durability journal size is not a safe byte offset",
					);
				}
				const observed = await readNativeDurabilityBytesFully(
					handle,
					observedLength,
				);
				const observedDigest = sha256(observed);
				const scan = this.journalCodec.scan(
					copyNativeDurabilityBytes(observed),
					this.checkpointValidationContext(checkpoint),
				);
				const hasIncompleteTail =
					scan.incompleteTailOffset != null &&
					scan.incompleteTailReason != null &&
					scan.validLength === scan.incompleteTailOffset &&
					scan.validLength < observed.byteLength;
				const isAlreadyComplete =
					scan.incompleteTailOffset == null &&
					scan.incompleteTailReason == null &&
					scan.validLength === observed.byteLength;
				if (!hasIncompleteTail && !isAlreadyComplete) {
					throw new NativeDurabilityIncompleteTailMismatchError(
						"The exact current journal is neither complete nor a structurally incomplete tail",
					);
				}
				const rereadLength = Number((await handle.stat()).size);
				const reread =
					rereadLength === observedLength
						? await readNativeDurabilityBytesFully(handle, rereadLength)
						: new Uint8Array();
				if (
					reread.byteLength !== observed.byteLength ||
					!nativeDurabilityBytesEqual(sha256(reread), observedDigest)
				) {
					throw new NativeDurabilityIncompleteTailMismatchError(
						"Journal changed after incomplete-tail classification",
					);
				}
				started = true;
				if (hasIncompleteTail) await handle.truncate(scan.validLength);
				await handle.sync();
				this.barrierOrdinal++;
				return {
					version: this.version,
					kind: "journal-tail-reconciliation",
					domainId: this.domainId,
					fence: { ...this.lease.fence },
					transactionId: request.transactionId,
					txSequence: request.txSequence,
					firstRecordLsn: scan.lastRecordLsn,
					lastRecordLsn: scan.lastRecordLsn,
					scopeDigest: sha256(
						encodeNativeDurabilityCanonical({
							...request,
							validLength: scan.validLength,
							incompleteTailReason:
								scan.incompleteTailReason ?? "already-complete",
							observedDigest,
						}),
					),
					barrierOrdinal: this.barrierOrdinal,
					previousLength: observed.byteLength,
					validLength: scan.validLength,
					observedDigest,
				};
			} catch (error) {
				if (!started) throw error;
				throw new NativeDurabilityOutcomeUnknownError(
					"journal-tail-reconciliation",
					request.transactionId,
					request.txSequence,
					error,
				);
			} finally {
				try {
					await handle.close();
				} catch {
					// Preserve the strict barrier result or the typed operation failure.
				}
			}
		});
	}

	private checkpointReceipt(
		request: NativeDurabilityCheckpointRequest,
		requestDigest: Uint8Array,
		checkpoint: Pick<
			NativeDurabilityCheckpoint,
			"generation" | "checkpointLsn" | "digest" | "manifestSlot"
		>,
	): NativeDurabilityCheckpointReceipt {
		this.barrierOrdinal++;
		return {
			version: this.version,
			kind: "checkpoint",
			domainId: this.domainId,
			fence: { ...this.lease.fence },
			transactionId: request.scope.transactionId,
			txSequence: request.scope.txSequence,
			firstRecordLsn: request.scope.recordLsn,
			lastRecordLsn: request.scope.recordLsn,
			scopeDigest: copyNativeDurabilityBytes(requestDigest),
			barrierOrdinal: this.barrierOrdinal,
			generation: checkpoint.generation,
			checkpointLsn: checkpoint.checkpointLsn,
			checkpointDigest: copyNativeDurabilityBytes(checkpoint.digest),
			manifestSlot: checkpoint.manifestSlot,
			stagingCoverageDigest: sha256(
				encodeNativeDurabilityCanonical(request.stagingCoverage),
			),
		};
	}

	async writeCheckpointAndSync(
		unsafeRequest: NativeDurabilityCheckpointRequest,
	): Promise<NativeDurabilityCheckpointReceipt> {
		const request = snapshotCheckpointRequest(unsafeRequest);
		return this.enqueue(async () => {
			if (!nativeDurabilityBytesEqual(sha256(request.bytes), request.digest)) {
				throw new NativeDurabilityDigestMismatchError("checkpoint");
			}
			const journalBase = await this.readJournalBaseCheckpoint();
			if (!journalBase) {
				throw new NativeDurabilityStorageCorruptionError(
					"Checkpoint write has no journal base authority",
				);
			}
			const journalBytes = new Uint8Array(
				await requireNodeFs().readFile(this.journalPath),
			);
			const journalScan = this.journalCodec.scan(
				journalBytes,
				this.checkpointValidationContext(journalBase),
			);
			if (
				journalScan.incompleteTailOffset != null ||
				journalScan.validLength !== journalBytes.byteLength ||
				request.checkpointLsn > journalScan.lastRecordLsn
			) {
				throw new NativeDurabilityStorageCorruptionError(
					"Checkpoint cannot cover an incomplete or shorter journal",
				);
			}
			const checkpointScopeRecord = journalScan.records.find(
				(record) => record.recordLsn === request.scope.recordLsn,
			);
			if (
				!checkpointScopeRecord ||
				checkpointScopeRecord.transactionId !== request.scope.transactionId ||
				checkpointScopeRecord.txSequence !== request.scope.txSequence ||
				checkpointScopeRecord.phase !== NativeDurabilityPhase.Clean
			) {
				throw new NativeDurabilityStorageCorruptionError(
					"Checkpoint operation scope does not match the journal authority",
				);
			}
			const latestRecords = new Map<string, NativeDurabilityJournalRecord>();
			let coveredTxSequenceHighwater = journalBase.txSequenceHighwater;
			for (const retained of journalBase.retainedTransactions) {
				latestRecords.set(retained.transactionId, {
					recordLsn: journalBase.checkpointLsn,
					txSequence: retained.txSequence,
					writerEpoch: journalBase.originFence.epoch,
					writerOwnerId: journalBase.originFence.ownerId,
					writerDomainId: journalBase.originFence.domainId,
					phase: retained.phase,
					operationKind: retained.operationKind,
					programId: copyNativeDurabilityBytes(this.programId),
					transactionId: retained.transactionId,
					planDigest: copyNativeDurabilityBytes(retained.planDigest),
					payload: new Uint8Array(),
				});
			}
			for (const record of journalScan.records) {
				if (record.recordLsn <= request.checkpointLsn) {
					latestRecords.set(record.transactionId, record);
					if (record.txSequence > coveredTxSequenceHighwater) {
						coveredTxSequenceHighwater = record.txSequence;
					}
				}
			}
			if (request.txSequenceHighwater < coveredTxSequenceHighwater) {
				throw new NativeDurabilityStorageCorruptionError(
					"Checkpoint transaction highwater regresses below covered journal records",
				);
			}
			for (const retained of request.retainedTransactions) {
				const record = latestRecords.get(retained.transactionId);
				if (
					!record ||
					record.txSequence !== retained.txSequence ||
					record.phase !== retained.phase ||
					record.operationKind !== retained.operationKind ||
					!nativeDurabilityBytesEqual(record.planDigest, retained.planDigest)
				) {
					throw new NativeDurabilityStorageCorruptionError(
						`Checkpoint retained transaction ${retained.transactionId} does not match the journal authority`,
					);
				}
			}
			const requestDigest = sha256(encodeNativeDurabilityCanonical(request));
			let state = await this.readGenerationState();
			if (!state) {
				throw new NativeDurabilityStorageCorruptionError(
					"Checkpoint generation state is missing",
				);
			}
			const currentCheckpoints = await this.readValidCheckpointsInternal();
			const activeCheckpoint = currentCheckpoints[0];
			if (
				!activeCheckpoint ||
				request.checkpointLsn < activeCheckpoint.checkpointLsn ||
				request.txSequenceHighwater < activeCheckpoint.txSequenceHighwater
			) {
				throw new NativeDurabilityStorageCorruptionError(
					"Checkpoint generation cannot regress active checkpoint watermarks",
				);
			}
			if (
				!state.pending &&
				state.completed &&
				state.completed.transactionId === request.scope.transactionId &&
				state.completed.txSequence === request.scope.txSequence
			) {
				if (
					!generationIdentityMatches(state.completed, request, requestDigest)
				) {
					throw new NativeDurabilityStorageCorruptionError(
						"Completed checkpoint scope was reused with different checkpoint content",
					);
				}
				if (
					activeCheckpoint.generation !== state.completed.generation ||
					activeCheckpoint.checkpointLsn !== request.checkpointLsn ||
					activeCheckpoint.txSequenceHighwater !==
						request.txSequenceHighwater ||
					!nativeDurabilityBytesEqual(activeCheckpoint.bytes, request.bytes) ||
					!nativeDurabilityBytesEqual(
						activeCheckpoint.digest,
						request.digest,
					) ||
					activeCheckpoint.stagingCoverage.length !==
						request.stagingCoverage.length ||
					activeCheckpoint.stagingCoverage.some((coverage, index) => {
						const expected = request.stagingCoverage[index];
						return (
							!expected ||
							coverage.transactionId !== expected.transactionId ||
							coverage.txSequence !== expected.txSequence ||
							coverage.coveredThroughLsn !== expected.coveredThroughLsn ||
							!nativeDurabilityBytesEqual(
								coverage.stagingManifestDigest,
								expected.stagingManifestDigest,
							)
						);
					}) ||
					activeCheckpoint.retainedTransactions.length !==
						request.retainedTransactions.length ||
					activeCheckpoint.retainedTransactions.some((retained, index) => {
						const expected = request.retainedTransactions[index];
						return (
							!expected ||
							retained.transactionId !== expected.transactionId ||
							retained.txSequence !== expected.txSequence ||
							retained.phase !== expected.phase ||
							retained.operationKind !== expected.operationKind ||
							!nativeDurabilityBytesEqual(
								retained.planDigest,
								expected.planDigest,
							)
						);
					})
				) {
					throw new NativeDurabilityStorageCorruptionError(
						"Completed checkpoint identity does not match the active generation",
					);
				}
				await syncDirectory(this.checkpointDirectory);
				return this.checkpointReceipt(request, requestDigest, activeCheckpoint);
			}
			for (const coverage of request.stagingCoverage) {
				const staged = await this.readStagingManifestInternal(
					coverage.transactionId,
				);
				const record = latestRecords.get(coverage.transactionId);
				if (
					!staged ||
					!record ||
					record.phase !== NativeDurabilityPhase.Clean ||
					record.txSequence !== coverage.txSequence ||
					staged.scope.txSequence !== coverage.txSequence ||
					coverage.coveredThroughLsn < record.recordLsn ||
					coverage.coveredThroughLsn < staged.scope.recordLsn ||
					!nativeDurabilityBytesEqual(
						staged.manifestDigest,
						coverage.stagingManifestDigest,
					)
				) {
					throw new NativeDurabilityStorageCorruptionError(
						`Checkpoint coverage does not prove CLEAN staging transaction ${coverage.transactionId}`,
					);
				}
			}

			let generation: bigint;
			let started = false;
			try {
				if (state.pending) {
					if (
						state.pending.transactionId !== request.scope.transactionId ||
						state.pending.txSequence !== request.scope.txSequence ||
						!nativeDurabilityBytesEqual(
							state.pending.requestDigest,
							requestDigest,
						)
					) {
						throw new NativeDurabilityStorageCorruptionError(
							`Checkpoint generation ${state.pending.generation} has an unresolved different owner`,
						);
					}
					generation = state.pending.generation;
				} else {
					if (state.generation === NATIVE_DURABILITY_MAX_U64) {
						throw new NativeDurabilityStorageCorruptionError(
							"Checkpoint generation highwater is exhausted",
						);
					}
					generation = state.generation + 1n;
					const targetSlot = generation % 2n === 0n ? "a" : "b";
					if (activeCheckpoint.manifestSlot === targetSlot) {
						if (generation === NATIVE_DURABILITY_MAX_U64) {
							throw new NativeDurabilityStorageCorruptionError(
								"Checkpoint generation highwater is exhausted before a safe manifest slot",
							);
						}
						generation++;
					}
					state = {
						generation,
						completed: state.completed,
						pending: {
							generation,
							requestDigest,
							transactionId: request.scope.transactionId,
							txSequence: request.scope.txSequence,
						},
					};
					started = true;
					await this.writeGenerationState(state);
				}
				const generationPath = requireNodePath().join(
					this.checkpointDirectory,
					checkpointFileName(generation),
				);
				const existingGeneration = await readFileIfExists(generationPath);
				let writeGeneration = existingGeneration == null;
				if (existingGeneration) {
					if (
						existingGeneration.byteLength !== request.bytes.byteLength ||
						!nativeDurabilityBytesEqual(
							sha256(existingGeneration),
							request.digest,
						)
					) {
						started = true;
						await requireNodeFs().rm(generationPath, { force: true });
						await syncDirectory(this.checkpointDirectory);
						writeGeneration = true;
					} else {
						const handle = await requireNodeFs().open(generationPath, "r+");
						try {
							await handle.sync();
						} finally {
							await handle.close();
						}
						await syncDirectory(this.checkpointDirectory);
					}
				}
				if (writeGeneration) {
					started = true;
					await writeImmutableFileAtomicallyAndSync(
						generationPath,
						request.bytes,
						`${this.lease.fence.epoch}-${this.lease.fence.ownerId}`,
					);
				}
				const manifestSlot = generation % 2n === 0n ? "a" : "b";
				const manifest: DiskCheckpointManifest = {
					version: this.version,
					programId: bytesToHex(this.programId),
					generation: generation.toString(),
					checkpointLsn: request.checkpointLsn.toString(),
					txSequenceHighwater: request.txSequenceHighwater.toString(),
					file: checkpointFileName(generation),
					byteLength: request.bytes.byteLength,
					digest: nativeDurabilityDigestHex(request.digest),
					originFence: diskFence(this.lease),
					stagingCoverage: request.stagingCoverage.map((coverage) => ({
						transactionId: coverage.transactionId,
						txSequence: coverage.txSequence.toString(),
						coveredThroughLsn: coverage.coveredThroughLsn.toString(),
						stagingManifestDigest: nativeDurabilityDigestHex(
							coverage.stagingManifestDigest,
						),
					})),
					retainedTransactions: request.retainedTransactions.map(
						(retained) => ({
							txSequence: retained.txSequence.toString(),
							transactionId: retained.transactionId,
							phase: retained.phase,
							operationKind: retained.operationKind,
							planDigest: nativeDurabilityDigestHex(retained.planDigest),
						}),
					),
				};
				started = true;
				await replaceFileAtomicallyAndSync(
					this.checkpointManifestPath(manifestSlot),
					encodeManifest(manifest),
					`${this.lease.fence.epoch}-${this.lease.fence.ownerId}`,
				);
				const completed: GenerationIdentity = {
					generation,
					requestDigest: copyNativeDurabilityBytes(requestDigest),
					transactionId: request.scope.transactionId,
					txSequence: request.scope.txSequence,
				};
				await this.writeGenerationState({ generation, completed });
				return this.checkpointReceipt(request, requestDigest, {
					generation,
					checkpointLsn: request.checkpointLsn,
					digest: request.digest,
					manifestSlot,
				});
			} catch (error) {
				if (!started) throw error;
				throw new NativeDurabilityOutcomeUnknownError(
					"checkpoint",
					request.scope.transactionId,
					request.scope.txSequence,
					error,
				);
			}
		});
	}

	async readLatestCheckpoint(): Promise<
		NativeDurabilityCheckpoint | undefined
	> {
		return this.enqueue(async () => {
			const checkpoint = (await this.readValidCheckpointsInternal())[0];
			return checkpoint && cloneCheckpoint(checkpoint);
		});
	}

	async deleteAndSync(
		unsafeRequest: NativeDurabilityDeleteRequest,
	): Promise<NativeDurabilityDeleteReceipt> {
		const request = snapshotDeleteRequest(unsafeRequest);
		return this.enqueue(async () => {
			const checkpoints = await this.readValidCheckpointsInternal();
			const active = checkpoints[0];
			const previous = checkpoints[1];
			const journalBase = await this.readJournalBaseCheckpoint();
			const generationState = await this.readGenerationState();
			if (!active || !journalBase || !generationState) {
				throw new NativeDurabilityStorageCorruptionError(
					"Strict deletion requires valid checkpoint authorities",
				);
			}
			const journalBytes = new Uint8Array(
				await requireNodeFs().readFile(this.journalPath),
			);
			const journalScan = this.journalCodec.scan(
				journalBytes,
				this.checkpointValidationContext(journalBase),
			);
			const scopeRecord = journalScan.records.find(
				(record) => record.recordLsn === request.scope.recordLsn,
			);
			if (
				journalScan.incompleteTailOffset != null ||
				journalScan.validLength !== journalBytes.byteLength ||
				!scopeRecord ||
				scopeRecord.transactionId !== request.scope.transactionId ||
				scopeRecord.txSequence !== request.scope.txSequence ||
				(scopeRecord.phase !== NativeDurabilityPhase.CleanupPending &&
					scopeRecord.phase !== NativeDurabilityPhase.Clean)
			) {
				throw new NativeDurabilityStorageCorruptionError(
					"Delete operation scope does not match a complete journal authority",
				);
			}
			if (generationState.pending) {
				throw new NativeDurabilityStorageCorruptionError(
					`Cannot delete while checkpoint generation ${generationState.pending.generation} is unresolved`,
				);
			}
			const stagingToDelete: string[] = [];
			const checkpointsToDelete: bigint[] = [];
			for (const target of request.targets) {
				if (target.kind === "staging") {
					const transactionDirectory = this.transactionDirectory(
						target.transactionId,
					);
					let directoryExists = false;
					try {
						directoryExists = (
							await requireNodeFs().stat(transactionDirectory)
						).isDirectory();
					} catch (error) {
						if (!isNotFound(error)) throw error;
					}
					const staged = await this.readStagingManifestInternal(
						target.transactionId,
					);
					if (
						directoryExists &&
						!staged &&
						!this.checkpointAuthorizesStagingDeletion(
							active,
							target.transactionId,
							journalScan.records,
						)
					) {
						throw new NativeDurabilityStorageCorruptionError(
							`Cannot checkpoint-delete incomplete staging transaction ${target.transactionId}`,
						);
					}
					if (
						staged &&
						!this.checkpointAuthorizesStagingDeletion(
							active,
							target.transactionId,
							journalScan.records,
							staged,
						)
					) {
						throw new NativeDurabilityStorageCorruptionError(
							`Active checkpoint does not exactly cover CLEAN staging transaction ${target.transactionId}`,
						);
					}
					stagingToDelete.push(transactionDirectory);
				} else {
					if (
						target.generation === active.generation ||
						target.generation === previous?.generation ||
						target.generation === journalBase.generation
					) {
						throw new NativeDurabilityStorageCorruptionError(
							`Cannot delete active, previous, or journal-base checkpoint generation ${target.generation}`,
						);
					}
					if (target.generation > generationState.generation) {
						throw new NativeDurabilityStorageCorruptionError(
							`Checkpoint generation ${target.generation} exceeds the permanent highwater`,
						);
					}
					checkpointsToDelete.push(target.generation);
				}
			}
			let started = false;
			try {
				for (const directory of stagingToDelete) {
					started = true;
					await requireNodeFs().rm(directory, {
						recursive: true,
						force: true,
					});
					await syncDirectory(this.stagingDirectory);
				}
				for (const generation of checkpointsToDelete) {
					started = true;
					await requireNodeFs().rm(
						requireNodePath().join(
							this.checkpointDirectory,
							checkpointFileName(generation),
						),
						{ force: true },
					);
					await syncDirectory(this.checkpointDirectory);
				}
				this.strictDeleteCount++;
				this.barrierOrdinal++;
				return {
					version: this.version,
					kind: "delete",
					domainId: this.domainId,
					fence: { ...this.lease.fence },
					transactionId: request.scope.transactionId,
					txSequence: request.scope.txSequence,
					firstRecordLsn: request.scope.recordLsn,
					lastRecordLsn: request.scope.recordLsn,
					scopeDigest: sha256(encodeNativeDurabilityCanonical(request)),
					barrierOrdinal: this.barrierOrdinal,
					targets: request.targets.map((target) => ({ ...target })),
				};
			} catch (error) {
				if (!started) throw error;
				throw new NativeDurabilityOutcomeUnknownError(
					"delete",
					request.scope.transactionId,
					request.scope.txSequence,
					error,
				);
			}
		});
	}

	async stats(): Promise<NativeDurabilityStorageStats> {
		return this.enqueue(async () => {
			const transactionIds = await this.listStagingTransactionIdsInternal();
			let stagedBlocks = 0;
			let stagedBytes = 0;
			for (const transactionId of transactionIds) {
				const manifest = await this.readStagingManifestInternal(transactionId);
				if (!manifest) continue;
				stagedBlocks += manifest.blocks.length;
				for (const block of manifest.blocks) stagedBytes += block.byteLength;
			}
			let checkpointGenerations = 0;
			let checkpointBytes = 0;
			for (const entry of await requireNodeFs().readdir(
				this.checkpointDirectory,
				{ withFileTypes: true },
			)) {
				if (
					entry.isFile() &&
					/^checkpoint-(0|[1-9][0-9]*)\.bin$/.test(entry.name)
				) {
					checkpointGenerations++;
					checkpointBytes += Number(
						(
							await requireNodeFs().stat(
								requireNodePath().join(this.checkpointDirectory, entry.name),
							)
						).size,
					);
				}
			}
			const journalBytes = Number(
				(await requireNodeFs().stat(this.journalPath)).size,
			);
			return {
				kind: this.kind,
				domainId: this.domainId,
				strictBarrierCount: this.barrierOrdinal,
				strictDeleteCount: this.strictDeleteCount,
				journalBytes,
				stagingTransactions: transactionIds.length,
				stagedBlocks,
				stagedBytes,
				checkpointGenerations,
				checkpointBytes,
			};
		});
	}

	/** Drains admitted operations before releasing the owned OS lease. */
	close(): Promise<void> {
		if (this.closePromise) return this.closePromise;
		this.closing = true;
		this.closePromise = (async () => {
			let operationError: unknown;
			try {
				await this.operationTail;
			} catch (error) {
				operationError = error;
			}
			let leaseError: unknown;
			try {
				await this.lease.close();
			} catch (error) {
				leaseError = error;
			}
			this.closed = true;
			if (operationError) throw operationError;
			if (leaseError) throw leaseError;
		})();
		return this.closePromise;
	}
}

export const createNodeNativeDurabilityStorage = async (
	options: NativeDurabilityNodeStorageOptions,
): Promise<NodeNativeDurabilityStorage> =>
	NodeNativeDurabilityStorage.create(options);
