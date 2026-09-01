import {
	type AnyStore,
	type CrashSafeAtomicReplaceDurability,
	type CrashSafeAtomicReplaceStore,
	type CrashSafeDurability,
} from "@peerbit/any-store-interface";
import { SHA256 } from "@stablelib/sha256";
import { copyExactUint8Array, getExactUint8ArrayByteLength } from "./bytes.js";

const textEncoder = new TextEncoder();
const FORMAT_MAGIC = new Uint8Array([
	0x50, 0x42, 0x32, 0x53, 0x43, 0x50, 0x30, 0x31,
]);
const FORMAT_VERSION = 1;
const SLOT_OFFSET = 9;
const GENERATION_OFFSET = 12;
const PAYLOAD_LENGTH_OFFSET = 20;
const SCOPE_DIGEST_OFFSET = 24;
const PREDECESSOR_DIGEST_OFFSET = 56;
const CHECKSUM_OFFSET = 88;
const HEADER_LENGTH = 120;
const DIGEST_LENGTH = 32;
const MAX_U64 = (1n << 64n) - 1n;
const ZERO_DIGEST = new Uint8Array(DIGEST_LENGTH);
const SCOPE_DOMAIN = textEncoder.encode(
	"peerbit:any-store:two-slot-checkpoint:scope:v1\0",
);
const RECORD_DOMAIN = textEncoder.encode(
	"peerbit:any-store:two-slot-checkpoint:record:v1\0",
);

export const MAX_CHECKPOINT_SCOPE_BYTES = 1_024;
export const MAX_CHECKPOINT_PAYLOAD_BYTES = 64 * 1024 * 1024;

const digest = (...chunks: Uint8Array[]): Uint8Array => {
	const hasher = new SHA256();
	try {
		for (const chunk of chunks) hasher.update(chunk);
		return new Uint8Array(hasher.digest());
	} finally {
		hasher.clean();
	}
};

const equalBytes = (left: Uint8Array, right: Uint8Array): boolean => {
	if (left.byteLength !== right.byteLength) return false;
	let difference = 0;
	for (let i = 0; i < left.byteLength; i++) difference |= left[i] ^ right[i];
	return difference === 0;
};

const isZeroDigest = (value: Uint8Array): boolean =>
	equalBytes(value, ZERO_DIGEST);

const checksumRecord = (record: Uint8Array): Uint8Array =>
	digest(
		RECORD_DOMAIN,
		record.subarray(0, CHECKSUM_OFFSET),
		record.subarray(HEADER_LENGTH),
	);

const slotForGeneration = (generation: bigint): 0 | 1 =>
	generation % 2n === 1n ? 0 : 1;

export class CheckpointUnsupportedStoreError extends Error {
	constructor() {
		super(
			"CrashSafeTwoSlotCheckpoint requires crashSafeDurability.atomicReplace",
		);
		this.name = "CheckpointUnsupportedStoreError";
	}
}

export class CheckpointCorruptionError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "CheckpointCorruptionError";
	}
}

export class CheckpointCommitInFlightError extends Error {
	constructor() {
		super("A checkpoint commit is already in flight");
		this.name = "CheckpointCommitInFlightError";
	}
}

export class CheckpointAmbiguousCommitError extends Error {
	readonly cause: unknown;

	constructor(cause: unknown) {
		super(
			"Checkpoint replacement failed with an indeterminate outcome; reopen before reading or committing again",
		);
		this.name = "CheckpointAmbiguousCommitError";
		this.cause = cause;
	}
}

export type CrashSafeCheckpointSnapshot = {
	readonly generation: bigint;
	readonly payload: Uint8Array;
};

export type CrashSafeTwoSlotCheckpointOptions = {
	readonly store: AnyStore;
	/** Stable application/domain bytes that bind every record in the namespace. */
	readonly scope: Uint8Array;
	/** Caller-selected bound. It may not exceed 64 MiB. */
	readonly maxPayloadBytes: number;
};

type DecodedRecord = {
	readonly generation: bigint;
	readonly payload: Uint8Array;
	readonly predecessorDigest: Uint8Array;
	readonly checksum: Uint8Array;
};

type CheckpointState = {
	readonly generation: bigint;
	readonly payload: Uint8Array;
	readonly checksum: Uint8Array;
};

const isCrashSafeAtomicReplaceDurability = (
	durability: CrashSafeDurability | undefined,
): durability is CrashSafeAtomicReplaceDurability =>
	durability?.crashSafe === true &&
	typeof durability.barrier === "function" &&
	typeof durability.atomicReplace === "function";

export const isCrashSafeAtomicReplaceStore = (
	store: AnyStore,
): store is CrashSafeAtomicReplaceStore =>
	isCrashSafeAtomicReplaceDurability(store.crashSafeDurability);

const encodeRecord = (input: {
	generation: bigint;
	slot: 0 | 1;
	payload: Uint8Array;
	scopeDigest: Uint8Array;
	predecessorDigest: Uint8Array;
}): { bytes: Uint8Array; checksum: Uint8Array } => {
	const bytes = new Uint8Array(HEADER_LENGTH + input.payload.byteLength);
	const view = new DataView(bytes.buffer);
	bytes.set(FORMAT_MAGIC, 0);
	bytes[8] = FORMAT_VERSION;
	bytes[SLOT_OFFSET] = input.slot;
	view.setUint16(10, 0, true);
	view.setBigUint64(GENERATION_OFFSET, input.generation, true);
	view.setUint32(PAYLOAD_LENGTH_OFFSET, input.payload.byteLength, true);
	bytes.set(input.scopeDigest, SCOPE_DIGEST_OFFSET);
	bytes.set(input.predecessorDigest, PREDECESSOR_DIGEST_OFFSET);
	bytes.set(input.payload, HEADER_LENGTH);
	const checksum = checksumRecord(bytes);
	bytes.set(checksum, CHECKSUM_OFFSET);
	return { bytes, checksum };
};

const decodeRecord = (
	bytes: Uint8Array,
	expectedSlot: 0 | 1,
	scopeDigest: Uint8Array,
	maxPayloadBytes: number,
): DecodedRecord => {
	const corrupt = (detail: string): never => {
		throw new CheckpointCorruptionError(
			`Checkpoint slot ${expectedSlot === 0 ? "A" : "B"} ${detail}`,
		);
	};

	if (bytes.byteLength < HEADER_LENGTH) corrupt("is truncated");
	if (!equalBytes(bytes.subarray(0, FORMAT_MAGIC.byteLength), FORMAT_MAGIC)) {
		corrupt("has an invalid format marker");
	}
	if (bytes[8] !== FORMAT_VERSION) corrupt("has an unsupported format version");
	if (bytes[SLOT_OFFSET] !== expectedSlot) corrupt("claims the wrong slot");
	const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	if (view.getUint16(10, true) !== 0) corrupt("has non-zero reserved bytes");

	const generation = view.getBigUint64(GENERATION_OFFSET, true);
	if (generation === 0n) corrupt("has generation zero");
	if (slotForGeneration(generation) !== expectedSlot) {
		corrupt("generation has the wrong parity");
	}

	const payloadLength = view.getUint32(PAYLOAD_LENGTH_OFFSET, true);
	if (payloadLength > maxPayloadBytes)
		corrupt("payload exceeds the configured bound");
	if (bytes.byteLength !== HEADER_LENGTH + payloadLength) {
		corrupt("has an invalid encoded length");
	}

	const encodedScopeDigest = bytes.subarray(
		SCOPE_DIGEST_OFFSET,
		SCOPE_DIGEST_OFFSET + DIGEST_LENGTH,
	);
	if (!equalBytes(encodedScopeDigest, scopeDigest)) {
		corrupt("belongs to a different scope");
	}

	const predecessorDigest = bytes.subarray(
		PREDECESSOR_DIGEST_OFFSET,
		PREDECESSOR_DIGEST_OFFSET + DIGEST_LENGTH,
	);
	if (generation === 1n && !isZeroDigest(predecessorDigest)) {
		corrupt("generation one has a predecessor");
	}
	if (generation > 1n && isZeroDigest(predecessorDigest)) {
		corrupt("is missing its predecessor link");
	}

	const encodedChecksum = bytes.subarray(
		CHECKSUM_OFFSET,
		CHECKSUM_OFFSET + DIGEST_LENGTH,
	);
	const actualChecksum = checksumRecord(bytes);
	if (!equalBytes(encodedChecksum, actualChecksum)) {
		corrupt("checksum does not match");
	}

	return {
		generation,
		payload: bytes.subarray(HEADER_LENGTH),
		predecessorDigest,
		checksum: encodedChecksum,
	};
};

const resolveRecords = (
	left: DecodedRecord | undefined,
	right: DecodedRecord | undefined,
): CheckpointState | undefined => {
	const records = [left, right].filter(
		(record): record is DecodedRecord => record !== undefined,
	);
	if (records.length === 0) return undefined;
	if (records.length === 1) {
		if (records[0].generation !== 1n) {
			throw new CheckpointCorruptionError(
				"A lone checkpoint slot is only valid for generation one",
			);
		}
		return {
			generation: records[0].generation,
			payload: records[0].payload,
			checksum: records[0].checksum,
		};
	}

	records.sort((a, b) =>
		a.generation === b.generation ? 0 : a.generation < b.generation ? -1 : 1,
	);
	const [older, newer] = records;
	if (newer.generation !== older.generation + 1n) {
		throw new CheckpointCorruptionError(
			"Checkpoint slots do not contain consecutive generations",
		);
	}
	if (!equalBytes(newer.predecessorDigest, older.checksum)) {
		throw new CheckpointCorruptionError(
			"Checkpoint slots do not form one predecessor chain",
		);
	}
	return {
		generation: newer.generation,
		payload: newer.payload,
		checksum: newer.checksum,
	};
};

/**
 * A single-writer, crash-safe checkpoint backed by exactly two fixed keys in a
 * trusted caller-dedicated store or sublevel. Construct instances with
 * `CrashSafeTwoSlotCheckpoint.open()` so every present slot is validated before
 * a snapshot becomes visible.
 */
export class CrashSafeTwoSlotCheckpoint {
	private readonly durability: CrashSafeAtomicReplaceDurability;
	private readonly slotKeys: readonly [string, string];
	private readonly scopeDigest: Uint8Array;
	private readonly maxPayloadBytes: number;
	private state: CheckpointState | undefined;
	private commitInFlight = false;
	private terminalError: CheckpointAmbiguousCommitError | undefined;

	private constructor(input: {
		durability: CrashSafeAtomicReplaceDurability;
		slotKeys: readonly [string, string];
		scopeDigest: Uint8Array;
		maxPayloadBytes: number;
		state: CheckpointState | undefined;
	}) {
		this.durability = input.durability;
		this.slotKeys = input.slotKeys;
		this.scopeDigest = input.scopeDigest;
		this.maxPayloadBytes = input.maxPayloadBytes;
		this.state = input.state;
	}

	static async open(
		options: CrashSafeTwoSlotCheckpointOptions,
	): Promise<CrashSafeTwoSlotCheckpoint> {
		// Capture accessor-backed options exactly once so validation and I/O cannot
		// cross stores, scopes, or bounds if a caller supplies getters.
		const store = options.store;
		const scopeInput = options.scope;
		const maxPayloadBytes = options.maxPayloadBytes;
		const scopeLength = getExactUint8ArrayByteLength(
			scopeInput,
			"Checkpoint scope",
		);
		if (scopeLength === 0 || scopeLength > MAX_CHECKPOINT_SCOPE_BYTES) {
			throw new RangeError(
				`Checkpoint scope must contain 1-${MAX_CHECKPOINT_SCOPE_BYTES} bytes`,
			);
		}
		const scope = copyExactUint8Array(
			scopeInput,
			"Checkpoint scope",
			scopeLength,
		);
		if (
			!Number.isSafeInteger(maxPayloadBytes) ||
			maxPayloadBytes < 0 ||
			maxPayloadBytes > MAX_CHECKPOINT_PAYLOAD_BYTES
		) {
			throw new RangeError(
				`maxPayloadBytes must be an integer between 0 and ${MAX_CHECKPOINT_PAYLOAD_BYTES}`,
			);
		}
		const durability = store.crashSafeDurability;
		if (!isCrashSafeAtomicReplaceDurability(durability)) {
			throw new CheckpointUnsupportedStoreError();
		}

		const scopeDigest = digest(SCOPE_DOMAIN, scope);
		const slotKeys = [
			"\0peerbit:two-slot-checkpoint:v1:a",
			"\0peerbit:two-slot-checkpoint:v1:b",
		] as const;

		// A fresh physical fence avoids treating process-local write completion as
		// durable authority before inspecting the two slots.
		await durability.barrier();
		const maxRecordBytes = HEADER_LENGTH + maxPayloadBytes;
		const readSlot = async (
			key: string,
			slot: "A" | "B",
			expectedSlot: 0 | 1,
		): Promise<DecodedRecord | undefined> => {
			const value = await store.get(key);
			if (value === undefined) return undefined;
			const name = `Checkpoint slot ${slot}`;
			const byteLength = getExactUint8ArrayByteLength(value, name);
			if (byteLength > maxRecordBytes) {
				throw new CheckpointCorruptionError(
					`${name} payload exceeds the configured bound`,
				);
			}
			const recordCopy = copyExactUint8Array(value, name, byteLength);
			return decodeRecord(
				recordCopy,
				expectedSlot,
				scopeDigest,
				maxPayloadBytes,
			);
		};
		// Decode sequentially so only one backend-returned value is retained at a
		// time. Decoded fields are views into each helper-owned exact record copy.
		const left = await readSlot(slotKeys[0], "A", 0);
		const right = await readSlot(slotKeys[1], "B", 1);

		return new CrashSafeTwoSlotCheckpoint({
			durability,
			slotKeys,
			scopeDigest,
			maxPayloadBytes,
			state: resolveRecords(left, right),
		});
	}

	get current(): CrashSafeCheckpointSnapshot | undefined {
		if (this.terminalError) throw this.terminalError;
		if (!this.state) return undefined;
		return {
			generation: this.state.generation,
			payload: new Uint8Array(this.state.payload),
		};
	}

	async commit(payload: Uint8Array): Promise<CrashSafeCheckpointSnapshot> {
		if (this.terminalError) throw this.terminalError;
		if (this.commitInFlight) throw new CheckpointCommitInFlightError();

		const payloadLength = getExactUint8ArrayByteLength(
			payload,
			"Checkpoint payload",
		);
		if (payloadLength > this.maxPayloadBytes) {
			throw new RangeError(
				`Checkpoint payload exceeds the ${this.maxPayloadBytes}-byte configured bound`,
			);
		}
		const payloadCopy = copyExactUint8Array(
			payload,
			"Checkpoint payload",
			payloadLength,
		);
		const generation = (this.state?.generation ?? 0n) + 1n;
		if (generation > MAX_U64) {
			throw new RangeError("Checkpoint generation exceeds u64");
		}
		const slot = slotForGeneration(generation);
		const record = encodeRecord({
			generation,
			slot,
			payload: payloadCopy,
			scopeDigest: this.scopeDigest,
			predecessorDigest: this.state?.checksum ?? ZERO_DIGEST,
		});
		// Keep allocations and byte copies before durable publication so an ordinary
		// post-publication return path cannot look like an ambiguous write.
		const nextState: CheckpointState = {
			generation,
			payload: payloadCopy,
			checksum: record.checksum,
		};
		const result: CrashSafeCheckpointSnapshot = {
			generation,
			payload: new Uint8Array(payloadCopy),
		};

		this.commitInFlight = true;
		try {
			await this.durability.atomicReplace(this.slotKeys[slot], record.bytes);
		} catch (error) {
			const terminalError = new CheckpointAmbiguousCommitError(error);
			this.terminalError = terminalError;
			throw terminalError;
		} finally {
			this.commitInFlight = false;
		}

		this.state = nextState;
		return result;
	}
}
