import type {
	NativeDurabilityJournalClassification,
	NativeDurabilityJournalClassifier,
} from "./storage.js";

export const NATIVE_DURABILITY_JOURNAL_FORMAT_VERSION = 1 as const;
export const NATIVE_DURABILITY_JOURNAL_MAX_U64 = (1n << 64n) - 1n;
export const NATIVE_DURABILITY_JOURNAL_MAX_BODY_LENGTH = 64 * 1024 * 1024;
export const NATIVE_DURABILITY_JOURNAL_MAX_PROGRAM_ID_LENGTH = 4096;
export const NATIVE_DURABILITY_JOURNAL_MAX_TRANSACTION_ID_LENGTH = 1024;
export const NATIVE_DURABILITY_JOURNAL_MAX_WRITER_OWNER_ID_LENGTH = 1024;
export const NATIVE_DURABILITY_JOURNAL_MAX_WRITER_DOMAIN_ID_LENGTH = 1024;
export const NATIVE_DURABILITY_JOURNAL_CODEC_BRAND = Symbol.for(
	"@peerbit/native-backbone/native-durability-journal-codec/v1",
);

export enum NativeDurabilityPhase {
	DurablePrepared = 1,
	NativeApplied = 2,
	Published = 3,
	Committed = 4,
	CleanupPending = 5,
	Clean = 6,
}

export enum NativeDurabilityOperationKind {
	Append = 1,
	Batch = 2,
	Document = 3,
	Receive = 4,
	Repair = 5,
}

export type NativeDurabilityJournalRecord = {
	recordLsn: bigint;
	txSequence: bigint;
	writerEpoch: bigint;
	writerOwnerId: string;
	writerDomainId: string;
	phase: NativeDurabilityPhase;
	operationKind: NativeDurabilityOperationKind;
	programId: Uint8Array;
	transactionId: string;
	planDigest: Uint8Array;
	payload: Uint8Array;
};

export type NativeDurabilityCheckpointTransactionState = Pick<
	NativeDurabilityJournalRecord,
	"txSequence" | "transactionId" | "phase" | "operationKind" | "planDigest"
>;

export type NativeDurabilityJournalValidationContext = {
	checkpointLsn: bigint;
	checkpointTxSequenceHighwater: bigint;
	expectedProgramId: Uint8Array;
	expectedWriterDomainId: string;
	checkpointWriterEpoch: bigint;
	checkpointWriterOwnerId?: string;
	currentWriterEpoch: bigint;
	currentWriterOwnerId: string;
	retainedTransactions?: readonly NativeDurabilityCheckpointTransactionState[];
};

export type NativeDurabilityIncompleteTailReason =
	| "short-header"
	| "short-body"
	| "short-trailer";

export type NativeDurabilityJournalScan = {
	records: NativeDurabilityJournalRecord[];
	validLength: number;
	incompleteTailOffset?: number;
	incompleteTailReason?: NativeDurabilityIncompleteTailReason;
	lastRecordLsn: bigint;
};

export type NativeDurabilityJournalErrorCode =
	| "ERR_NATIVE_DURABILITY_JOURNAL_INPUT"
	| `ERR_NATIVE_DURABILITY_JOURNAL_${string}`;

export class NativeDurabilityJournalCodecError extends Error {
	constructor(
		readonly code: NativeDurabilityJournalErrorCode,
		message: string,
		readonly byteOffset?: number,
		readonly cause?: unknown,
	) {
		super(message);
		this.name = "NativeDurabilityJournalCodecError";
	}
}

export class NativeDurabilityJournalInputError extends NativeDurabilityJournalCodecError {
	constructor(readonly field: string, message: string) {
		super("ERR_NATIVE_DURABILITY_JOURNAL_INPUT", message);
		this.name = "NativeDurabilityJournalInputError";
	}
}

export class NativeDurabilityJournalCorruptionError extends NativeDurabilityJournalCodecError {
	constructor(
		code: NativeDurabilityJournalErrorCode,
		message: string,
		byteOffset?: number,
		cause?: unknown,
	) {
		super(code, message, byteOffset, cause);
		this.name = "NativeDurabilityJournalCorruptionError";
	}
}

type NativeDurabilityJournalCodecHandle = {
	encodeFrame(
		recordLsn: string,
		txSequence: string,
		writerEpoch: string,
		writerOwnerId: string,
		writerDomainId: string,
		phase: number,
		operationKind: number,
		programId: Uint8Array,
		transactionId: string,
		planDigest: Uint8Array,
		payload: Uint8Array,
	): Uint8Array;
	scan(
		bytes: Uint8Array,
		checkpointLsn: string,
		checkpointTxSequenceHighwater: string,
		expectedProgramId: Uint8Array,
		expectedWriterDomainId: string,
		checkpointWriterEpoch: string,
		checkpointWriterOwnerId: string | undefined,
		currentWriterEpoch: string,
		currentWriterOwnerId: string,
		retainedTransactions: unknown[],
	): unknown[];
};

type NativeBackboneWasmModule = {
	default(input?: unknown): Promise<unknown>;
	initSync(input?: unknown): unknown;
	NativeDurabilityJournalCodec: new () => NativeDurabilityJournalCodecHandle;
};

let wasmModulePromise: Promise<NativeBackboneWasmModule> | undefined;
let wasmInitialization: Promise<NativeBackboneWasmModule> | undefined;
const nativeDurabilityJournalCodecConstructionToken = Symbol();
const trustedNativeDurabilityJournalCodecs = new WeakSet<object>();

const loadWasm = async (): Promise<NativeBackboneWasmModule> => {
	const wasmModulePath = "../../wasm/native_backbone.js";
	wasmModulePromise ??= import(
		/* @vite-ignore */ wasmModulePath
	) as Promise<NativeBackboneWasmModule>;
	wasmInitialization ??= wasmModulePromise.then(async (wasm) => {
		const processLike = globalThis as {
			process?: { versions?: { node?: string } };
		};
		if (processLike.process?.versions?.node) {
			const fsPromises = "fs/promises";
			const { readFile } = (await import(
				/* @vite-ignore */ fsPromises
			)) as typeof import("fs/promises");
			const bytes = await readFile(
				new URL("../../wasm/native_backbone_bg.wasm", import.meta.url),
			);
			wasm.initSync({ module: bytes });
		} else {
			await wasm.default({
				module_or_path: new URL(
					"../../wasm/native_backbone_bg.wasm",
					import.meta.url,
				),
			});
		}
		return wasm;
	});
	return wasmInitialization;
};

let textEncoder: TextEncoder | undefined;

const utf8ByteLength = (value: string): number => {
	textEncoder ??= new TextEncoder();
	return textEncoder.encode(value).byteLength;
};

const assertU64 = (
	value: bigint,
	field: string,
	options: { nonzero?: boolean } = {},
): void => {
	if (
		typeof value !== "bigint" ||
		value < 0n ||
		value > NATIVE_DURABILITY_JOURNAL_MAX_U64 ||
		(options.nonzero === true && value === 0n)
	) {
		throw new NativeDurabilityJournalInputError(
			field,
			`${field} must be ${options.nonzero ? "a non-zero" : "an"} unsigned 64-bit bigint`,
		);
	}
};

const assertBytes = (
	value: Uint8Array,
	field: string,
	options: { exact?: number; min?: number; max?: number } = {},
): void => {
	if (!(value instanceof Uint8Array)) {
		throw new NativeDurabilityJournalInputError(field, `${field} must be Uint8Array`);
	}
	if (options.exact != null && value.byteLength !== options.exact) {
		throw new NativeDurabilityJournalInputError(
			field,
			`${field} must contain exactly ${options.exact} bytes`,
		);
	}
	if (options.min != null && value.byteLength < options.min) {
		throw new NativeDurabilityJournalInputError(
			field,
			`${field} must contain at least ${options.min} bytes`,
		);
	}
	if (options.max != null && value.byteLength > options.max) {
		throw new NativeDurabilityJournalInputError(
			field,
			`${field} exceeds ${options.max} bytes`,
		);
	}
};

const assertBoundedString = (
	value: string,
	field: string,
	maxBytes: number,
): void => {
	if (typeof value !== "string" || value.length === 0) {
		throw new NativeDurabilityJournalInputError(field, `${field} must not be empty`);
	}
	const byteLength = utf8ByteLength(value);
	if (byteLength > maxBytes) {
		throw new NativeDurabilityJournalInputError(
			field,
			`${field} exceeds ${maxBytes} UTF-8 bytes`,
		);
	}
};

const assertPhase = (value: NativeDurabilityPhase, field: string): void => {
	if (!Number.isInteger(value) || value < 1 || value > 6) {
		throw new NativeDurabilityJournalInputError(field, `${field} is invalid`);
	}
};

const assertOperationKind = (
	value: NativeDurabilityOperationKind,
	field: string,
): void => {
	if (!Number.isInteger(value) || value < 1 || value > 5) {
		throw new NativeDurabilityJournalInputError(field, `${field} is invalid`);
	}
};

const assertRecord = (record: NativeDurabilityJournalRecord): void => {
	assertU64(record.recordLsn, "recordLsn", { nonzero: true });
	assertU64(record.txSequence, "txSequence", { nonzero: true });
	assertU64(record.writerEpoch, "writerEpoch", { nonzero: true });
	assertBoundedString(
		record.writerOwnerId,
		"writerOwnerId",
		NATIVE_DURABILITY_JOURNAL_MAX_WRITER_OWNER_ID_LENGTH,
	);
	assertBoundedString(
		record.writerDomainId,
		"writerDomainId",
		NATIVE_DURABILITY_JOURNAL_MAX_WRITER_DOMAIN_ID_LENGTH,
	);
	assertPhase(record.phase, "phase");
	assertOperationKind(record.operationKind, "operationKind");
	assertBytes(record.programId, "programId", {
		min: 1,
		max: NATIVE_DURABILITY_JOURNAL_MAX_PROGRAM_ID_LENGTH,
	});
	assertBoundedString(
		record.transactionId,
		"transactionId",
		NATIVE_DURABILITY_JOURNAL_MAX_TRANSACTION_ID_LENGTH,
	);
	assertBytes(record.planDigest, "planDigest", { exact: 32 });
	assertBytes(record.payload, "payload", {
		max: NATIVE_DURABILITY_JOURNAL_MAX_BODY_LENGTH,
	});
};

const assertContext = (context: NativeDurabilityJournalValidationContext): void => {
	assertU64(context.checkpointLsn, "checkpointLsn");
	assertU64(
		context.checkpointTxSequenceHighwater,
		"checkpointTxSequenceHighwater",
	);
	assertBytes(context.expectedProgramId, "expectedProgramId", {
		min: 1,
		max: NATIVE_DURABILITY_JOURNAL_MAX_PROGRAM_ID_LENGTH,
	});
	assertBoundedString(
		context.expectedWriterDomainId,
		"expectedWriterDomainId",
		NATIVE_DURABILITY_JOURNAL_MAX_WRITER_DOMAIN_ID_LENGTH,
	);
	assertU64(context.checkpointWriterEpoch, "checkpointWriterEpoch");
	assertU64(context.currentWriterEpoch, "currentWriterEpoch", { nonzero: true });
	if (context.checkpointWriterEpoch > context.currentWriterEpoch) {
		throw new NativeDurabilityJournalInputError(
			"checkpointWriterEpoch",
			"checkpointWriterEpoch must not exceed currentWriterEpoch",
		);
	}
	if (context.checkpointWriterEpoch > 0n && context.checkpointWriterOwnerId == null) {
		throw new NativeDurabilityJournalInputError(
			"checkpointWriterOwnerId",
			"checkpointWriterOwnerId is required for a non-genesis checkpoint epoch",
		);
	}
	if (context.checkpointWriterOwnerId != null) {
		assertBoundedString(
			context.checkpointWriterOwnerId,
			"checkpointWriterOwnerId",
			NATIVE_DURABILITY_JOURNAL_MAX_WRITER_OWNER_ID_LENGTH,
		);
	}
	assertBoundedString(
		context.currentWriterOwnerId,
		"currentWriterOwnerId",
		NATIVE_DURABILITY_JOURNAL_MAX_WRITER_OWNER_ID_LENGTH,
	);
	for (const [index, retained] of (context.retainedTransactions ?? []).entries()) {
		assertU64(retained.txSequence, `retainedTransactions[${index}].txSequence`, {
			nonzero: true,
		});
		if (retained.txSequence > context.checkpointTxSequenceHighwater) {
			throw new NativeDurabilityJournalInputError(
				`retainedTransactions[${index}].txSequence`,
				"retained transaction sequence exceeds checkpoint highwater",
			);
		}
		assertBoundedString(
			retained.transactionId,
			`retainedTransactions[${index}].transactionId`,
			NATIVE_DURABILITY_JOURNAL_MAX_TRANSACTION_ID_LENGTH,
		);
		assertPhase(retained.phase, `retainedTransactions[${index}].phase`);
		assertOperationKind(
			retained.operationKind,
			`retainedTransactions[${index}].operationKind`,
		);
		assertBytes(retained.planDigest, `retainedTransactions[${index}].planDigest`, {
			exact: 32,
		});
	}
};

const copyContext = (
	context: NativeDurabilityJournalValidationContext,
): NativeDurabilityJournalValidationContext => ({
	...context,
	expectedProgramId: new Uint8Array(context.expectedProgramId),
	retainedTransactions: (context.retainedTransactions ?? []).map((retained) => ({
		...retained,
		planDigest: new Uint8Array(retained.planDigest),
	})),
});

const parseDecimalU64 = (value: unknown, field: string): bigint => {
	if (typeof value !== "string" || !/^(0|[1-9][0-9]*)$/.test(value)) {
		throw new NativeDurabilityJournalCorruptionError(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_DECIMAL_U64",
			`${field} is not a canonical decimal u64`,
		);
	}
	const parsed = BigInt(value);
	if (parsed > NATIVE_DURABILITY_JOURNAL_MAX_U64) {
		throw new NativeDurabilityJournalCorruptionError(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_DECIMAL_U64",
			`${field} exceeds u64`,
		);
	}
	return parsed;
};

const parseSafeOffset = (
	value: unknown,
	field: string,
	maximum: number,
): number => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value < 0 ||
		value > maximum
	) {
		throw new NativeDurabilityJournalCorruptionError(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_SCAN_ROW",
			`${field} is not a safe byte offset`,
		);
	}
	return value;
};

const bytesFromRow = (value: unknown, field: string): Uint8Array => {
	if (!(value instanceof Uint8Array)) {
		throw new NativeDurabilityJournalCorruptionError(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_SCAN_ROW",
			`${field} is not bytes`,
		);
	}
	return new Uint8Array(value);
};

const stringFromRow = (value: unknown, field: string): string => {
	if (typeof value !== "string") {
		throw new NativeDurabilityJournalCorruptionError(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_SCAN_ROW",
			`${field} is not a string`,
		);
	}
	return value;
};

const enumFromRow = <T extends number>(
	value: unknown,
	field: string,
	min: number,
	max: number,
): T => {
	if (typeof value !== "number" || !Number.isInteger(value) || value < min || value > max) {
		throw new NativeDurabilityJournalCorruptionError(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_SCAN_ROW",
			`${field} is invalid`,
		);
	}
	return value as T;
};

const errorFromWasm = (
	error: unknown,
	kind: "input" | "corruption",
): NativeDurabilityJournalCodecError => {
	if (Array.isArray(error) && typeof error[0] === "string" && typeof error[1] === "string") {
		const offset =
			typeof error[2] === "number" && Number.isSafeInteger(error[2])
				? error[2]
				: undefined;
		if (kind === "corruption") {
			return new NativeDurabilityJournalCorruptionError(
				error[0] as NativeDurabilityJournalErrorCode,
				error[1],
				offset,
				error,
			);
		}
		return new NativeDurabilityJournalCodecError(
			error[0] as NativeDurabilityJournalErrorCode,
			error[1],
			offset,
			error,
		);
	}
	return kind === "corruption"
		? new NativeDurabilityJournalCorruptionError(
				"ERR_NATIVE_DURABILITY_JOURNAL_WASM",
				"Native durability journal codec failed",
				undefined,
				error,
			)
		: new NativeDurabilityJournalCodecError(
				"ERR_NATIVE_DURABILITY_JOURNAL_WASM",
				"Native durability journal codec failed",
				undefined,
				error,
			);
};

const parseScan = (raw: unknown, byteLength: number): NativeDurabilityJournalScan => {
	if (!Array.isArray(raw) || raw.length !== 5 || !Array.isArray(raw[4])) {
		throw new NativeDurabilityJournalCorruptionError(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_SCAN_ROW",
			"Native durability journal scan result is malformed",
		);
	}
	const validLength = parseSafeOffset(raw[0], "validLength", byteLength);
	const incompleteTailOffset =
		raw[1] == null
			? undefined
			: parseSafeOffset(raw[1], "incompleteTailOffset", byteLength);
	const reasonCode = enumFromRow<number>(raw[2], "incompleteTailReason", 0, 3);
	const incompleteTailReason =
		reasonCode === 0
			? undefined
			: reasonCode === 1
				? "short-header"
				: reasonCode === 2
					? "short-body"
					: "short-trailer";
	if ((incompleteTailOffset == null) !== (incompleteTailReason == null)) {
		throw new NativeDurabilityJournalCorruptionError(
			"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_SCAN_ROW",
			"Incomplete-tail offset and reason disagree",
		);
	}
	const records = (raw[4] as unknown[]).map((unknownRow, index) => {
		if (!Array.isArray(unknownRow) || unknownRow.length !== 11) {
			throw new NativeDurabilityJournalCorruptionError(
				"ERR_NATIVE_DURABILITY_JOURNAL_INVALID_SCAN_ROW",
				`Journal record row ${index} is malformed`,
			);
		}
		const row = unknownRow;
		return {
			recordLsn: parseDecimalU64(row[0], `records[${index}].recordLsn`),
			txSequence: parseDecimalU64(row[1], `records[${index}].txSequence`),
			writerEpoch: parseDecimalU64(row[2], `records[${index}].writerEpoch`),
			writerOwnerId: stringFromRow(row[3], `records[${index}].writerOwnerId`),
			writerDomainId: stringFromRow(row[4], `records[${index}].writerDomainId`),
			phase: enumFromRow<NativeDurabilityPhase>(
				row[5],
				`records[${index}].phase`,
				1,
				6,
			),
			operationKind: enumFromRow<NativeDurabilityOperationKind>(
				row[6],
				`records[${index}].operationKind`,
				1,
				5,
			),
			programId: bytesFromRow(row[7], `records[${index}].programId`),
			transactionId: stringFromRow(row[8], `records[${index}].transactionId`),
			planDigest: bytesFromRow(row[9], `records[${index}].planDigest`),
			payload: bytesFromRow(row[10], `records[${index}].payload`),
		} satisfies NativeDurabilityJournalRecord;
	});
	return {
		records,
		validLength,
		incompleteTailOffset,
		incompleteTailReason,
		lastRecordLsn: parseDecimalU64(raw[3], "lastRecordLsn"),
	};
};

export class NativeDurabilityJournalCodec implements NativeDurabilityJournalClassifier {
	readonly formatVersion = NATIVE_DURABILITY_JOURNAL_FORMAT_VERSION;
	readonly [NATIVE_DURABILITY_JOURNAL_CODEC_BRAND] = true as const;
	private readonly validationContext: NativeDurabilityJournalValidationContext;

	private constructor(
		constructionToken: typeof nativeDurabilityJournalCodecConstructionToken,
		private readonly native: NativeDurabilityJournalCodecHandle,
		context: NativeDurabilityJournalValidationContext,
	) {
		if (constructionToken !== nativeDurabilityJournalCodecConstructionToken) {
			throw new TypeError(
				"NativeDurabilityJournalCodec must be created by createNativeDurabilityJournalCodec",
			);
		}
		assertContext(context);
		this.validationContext = copyContext(context);
	}

	static async create(
		context: NativeDurabilityJournalValidationContext,
	): Promise<NativeDurabilityJournalCodec> {
		assertContext(context);
		// Snapshot before the first await so caller mutation cannot change the
		// validation authority while the Wasm module is loading.
		const validationContext = copyContext(context);
		const wasm = await loadWasm();
		const codec = new NativeDurabilityJournalCodec(
			nativeDurabilityJournalCodecConstructionToken,
			new wasm.NativeDurabilityJournalCodec(),
			validationContext,
		);
		trustedNativeDurabilityJournalCodecs.add(codec);
		return codec;
	}

	get context(): NativeDurabilityJournalValidationContext {
		return copyContext(this.validationContext);
	}

	encode(record: NativeDurabilityJournalRecord): Uint8Array {
		assertRecord(record);
		try {
			return new Uint8Array(
				this.native.encodeFrame(
					record.recordLsn.toString(),
					record.txSequence.toString(),
					record.writerEpoch.toString(),
					record.writerOwnerId,
					record.writerDomainId,
					record.phase,
					record.operationKind,
					record.programId,
					record.transactionId,
					record.planDigest,
					record.payload,
				),
			);
		} catch (error) {
			throw errorFromWasm(error, "input");
		}
	}

	scan(
		bytes: Uint8Array,
		context: NativeDurabilityJournalValidationContext = this.validationContext,
	): NativeDurabilityJournalScan {
		assertBytes(bytes, "journalBytes");
		assertContext(context);
		const retainedTransactions = (context.retainedTransactions ?? []).map(
			(retained) => [
				retained.txSequence.toString(),
				retained.transactionId,
				retained.phase,
				retained.operationKind,
				retained.planDigest,
			],
		);
		try {
			return parseScan(
				this.native.scan(
					bytes,
					context.checkpointLsn.toString(),
					context.checkpointTxSequenceHighwater.toString(),
					context.expectedProgramId,
					context.expectedWriterDomainId,
					context.checkpointWriterEpoch.toString(),
					context.checkpointWriterOwnerId,
					context.currentWriterEpoch.toString(),
					context.currentWriterOwnerId,
					retainedTransactions,
				),
				bytes.byteLength,
			);
		} catch (error) {
			if (error instanceof NativeDurabilityJournalCodecError) throw error;
			throw errorFromWasm(error, "corruption");
		}
	}

	classify(bytes: Uint8Array): NativeDurabilityJournalClassification {
		const scan = this.scan(bytes);
		if (scan.incompleteTailOffset != null && scan.incompleteTailReason != null) {
			return {
				kind: "incomplete-tail",
				validLength: scan.validLength,
				lastRecordLsn: scan.lastRecordLsn,
				reason: scan.incompleteTailReason,
			};
		}
		return {
			kind: "complete",
			validLength: scan.validLength,
			lastRecordLsn: scan.lastRecordLsn,
		};
	}
}

export const isNativeDurabilityJournalCodec = (
	value: unknown,
): value is NativeDurabilityJournalCodec =>
	(typeof value === "object" || typeof value === "function") &&
	value !== null &&
	trustedNativeDurabilityJournalCodecs.has(value);

export const createNativeDurabilityJournalCodec = async (
	context: NativeDurabilityJournalValidationContext,
): Promise<NativeDurabilityJournalCodec> =>
	NativeDurabilityJournalCodec.create(context);
