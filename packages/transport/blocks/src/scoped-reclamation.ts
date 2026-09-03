import type {
	AnyStore,
	CrashSafeAtomicReplaceDurability,
	CrashSafeDurability,
} from "@peerbit/any-store-interface";
import {
	type ScopedBlockReclamationFaultCode,
	type ScopedBlockReclamationHealth,
	type ScopedBlockReclamationLimits,
	type ScopedBlockReclamationScopeV1,
	type ScopedBlockReclamationV1,
	type ScopedBlockReleaseResult,
	calculateRawCid,
	cidifyString,
	stringifyCid,
	verifyBlockBytes,
} from "@peerbit/blocks-interface";
import { createSHA256 } from "@peerbit/crypto";
import { AnyBlockStore } from "./any-blockstore.js";

const textEncoder = new TextEncoder();
const NAMESPACE = "peerbit-scoped-reclamation-v1";
const ROOT_SUBLEVEL_PREFIX = `!${NAMESPACE}!`;
const DATA_LEVEL = "data";
const REFERENCE_LEVEL = "references";
const RECORD_MAGIC = new Uint8Array([
	0x50, 0x42, 0x53, 0x52, 0x45, 0x46, 0x31, 0,
]);
const RECORD_VERSION = 1;
const GENERATION_OFFSET = 12;
const COUNT_OFFSET = 20;
const RECORD_PREFIX_BYTES = 24;
const DIGEST_BYTES = 32;
const CHECKSUM_BYTES = 32;
const MAX_GENERATION = (1n << 64n) - 1n;
// Raw Blocks accepted arbitrary valid CID encodings before this opt-in wrapper
// existed. Keep pathological-but-valid CIDs usable while placing a generous,
// private ceiling on parser work. Managed CIDs retain their tighter public cap.
const MAX_RAW_BLOCK_CID_BYTES = 64 * 1024;
const SCOPE_DOMAIN = textEncoder.encode("peerbit:blocks:scope:v1\0");
const RECORD_DOMAIN = textEncoder.encode("peerbit:blocks:references:v1\0");
const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
const typedArrayByteLength = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	"byteLength",
)!.get!;
const typedArrayBuffer = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	"buffer",
)!.get!;
const typedArrayTag = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	Symbol.toStringTag,
)!.get!;
const typedArraySet = Uint8Array.prototype.set;
const SCOPED_BLOCK_RECLAMATION_LIMITS: ScopedBlockReclamationLimits =
	Object.freeze({
		maxBlockBytes: 16 * 1024 * 1024,
		maxCidBytes: 512,
		scopeKeyBytes: 32,
		maxReferencesPerBlock: 1_024,
		maxPendingOperations: 1_024,
		maxPendingBytes: 64 * 1024 * 1024,
	});

const OPENING_HEALTH: ScopedBlockReclamationHealth = Object.freeze({
	status: "opening",
});
const READY_HEALTH: ScopedBlockReclamationHealth = Object.freeze({
	status: "ready",
});
const CLOSED_HEALTH: ScopedBlockReclamationHealth = Object.freeze({
	status: "closed",
});

const isAtomicDurability = (
	value: CrashSafeDurability | undefined,
): value is CrashSafeAtomicReplaceDurability =>
	value?.crashSafe === true &&
	typeof value.barrier === "function" &&
	typeof value.atomicReplace === "function";

const equalBytes = (left: Uint8Array, right: Uint8Array): boolean => {
	if (left.byteLength !== right.byteLength) return false;
	let difference = 0;
	for (let i = 0; i < left.byteLength; i++) difference |= left[i] ^ right[i];
	return difference === 0;
};

const compareBytes = (left: Uint8Array, right: Uint8Array): number => {
	for (let i = 0; i < left.byteLength; i++) {
		if (left[i] !== right[i]) return left[i]! - right[i]!;
	}
	return left.byteLength - right.byteLength;
};

const hash = (...values: Uint8Array[]): Uint8Array => {
	const hasher = createSHA256();
	for (const value of values) hasher.update(value);
	return new Uint8Array(hasher.digest());
};

const exactByteLength = (value: Uint8Array, name: string): number => {
	let byteLength: number;
	let tag: string | undefined;
	try {
		byteLength = typedArrayByteLength.call(value);
		tag = typedArrayTag.call(value);
	} catch {
		throw new TypeError(`${name} must be a genuine Uint8Array`);
	}
	if (tag !== "Uint8Array") throw new TypeError(`${name} must be a Uint8Array`);
	if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
		throw new RangeError(`${name} has an invalid byte length`);
	}
	return byteLength;
};

const exactCopy = (
	value: Uint8Array,
	name: string,
	maximumBytes: number,
	allowEmpty: boolean,
): Uint8Array => {
	const byteLength = exactByteLength(value, name);
	let buffer: ArrayBufferLike;
	try {
		buffer = typedArrayBuffer.call(value);
	} catch {
		throw new TypeError(`${name} must be an attached Uint8Array`);
	}
	if (
		typeof SharedArrayBuffer !== "undefined" &&
		buffer instanceof SharedArrayBuffer
	) {
		throw new TypeError(`${name} may not use shared memory`);
	}
	if ((!allowEmpty && byteLength === 0) || byteLength > maximumBytes) {
		throw new RangeError(
			allowEmpty
				? `${name} must contain at most ${maximumBytes} bytes`
				: `${name} must contain exactly ${maximumBytes} bytes`,
		);
	}
	const copy = new Uint8Array(byteLength);
	try {
		typedArraySet.call(copy, value);
		return copy;
	} catch {
		throw new TypeError(`${name} must be an attached Uint8Array`);
	}
};

const copyScopeKey = (value: Uint8Array): Uint8Array => {
	const copy = exactCopy(
		value,
		"Scoped block reclamation scope key",
		SCOPED_BLOCK_RECLAMATION_LIMITS.scopeKeyBytes,
		false,
	);
	if (copy.byteLength !== SCOPED_BLOCK_RECLAMATION_LIMITS.scopeKeyBytes) {
		throw new RangeError(
			`Scoped block reclamation scope key must contain exactly ${SCOPED_BLOCK_RECLAMATION_LIMITS.scopeKeyBytes} bytes`,
		);
	}
	return copy;
};

const encodeBoundedCidText = (
	value: string,
	name: "Raw block CID" | "Scoped block CID",
	maximumBytes: number,
): Uint8Array => {
	if (typeof value !== "string" || value.length === 0) {
		throw new TypeError(`${name} must be a non-empty string`);
	}
	// A CID is ASCII, so the code-unit check rejects oversized valid inputs
	// without asking the parser or TextEncoder to process attacker-sized text.
	if (value.length > maximumBytes) {
		throw new RangeError(`${name} exceeds ${maximumBytes} bytes`);
	}
	const bytes = textEncoder.encode(value);
	if (bytes.byteLength > maximumBytes) {
		throw new RangeError(`${name} exceeds ${maximumBytes} bytes`);
	}
	return bytes;
};

const validateCidInput = (value: string): void => {
	encodeBoundedCidText(
		value,
		"Scoped block CID",
		SCOPED_BLOCK_RECLAMATION_LIMITS.maxCidBytes,
	);
};

// ClassicLevel's root view can address the encoded keys of its sublevels. The
// raw Blocks surface is therefore a CID-only authority boundary, not merely a
// convenience validation: invalid keys must never reach the root store.
const isRawBlockCid = (value: unknown): value is string => {
	if (typeof value !== "string" || value.length === 0) return false;
	if (value.startsWith(ROOT_SUBLEVEL_PREFIX)) return false;
	try {
		encodeBoundedCidText(value, "Raw block CID", MAX_RAW_BLOCK_CID_BYTES);
		const parsed = cidifyString(value);
		encodeBoundedCidText(
			stringifyCid(parsed),
			"Raw block CID",
			MAX_RAW_BLOCK_CID_BYTES,
		);
		return true;
	} catch {
		return false;
	}
};

const requireRawBlockCid: (value: unknown) => asserts value is string = (
	value,
) => {
	if (typeof value !== "string" || value.length === 0) {
		throw new TypeError("Raw block key must be a valid CID string");
	}
	if (value.startsWith(ROOT_SUBLEVEL_PREFIX)) {
		throw new TypeError("Raw block key must be a valid CID string");
	}
	encodeBoundedCidText(value, "Raw block CID", MAX_RAW_BLOCK_CID_BYTES);
	let parsed: ReturnType<typeof cidifyString>;
	try {
		parsed = cidifyString(value);
	} catch {
		throw new TypeError("Raw block key must be a valid CID string");
	}
	encodeBoundedCidText(
		stringifyCid(parsed),
		"Raw block CID",
		MAX_RAW_BLOCK_CID_BYTES,
	);
};

const canonicalCid = (value: string): { cid: string; bytes: Uint8Array } => {
	validateCidInput(value);
	const parsed = cidifyString(value);
	const cid = stringifyCid(parsed);
	const bytes = encodeBoundedCidText(
		cid,
		"Scoped block CID",
		SCOPED_BLOCK_RECLAMATION_LIMITS.maxCidBytes,
	);
	return { cid, bytes };
};

type ReferenceState = Readonly<{
	generation: bigint;
	references: readonly Uint8Array[];
}>;

type ManagedIteratorState = {
	readonly iterator: AsyncIterator<[string, Uint8Array], void, void>;
	closing?: Promise<void>;
};

const encodeReferenceState = (
	cidBytes: Uint8Array,
	state: ReferenceState,
): Uint8Array => {
	const bodyLength =
		RECORD_PREFIX_BYTES + state.references.length * DIGEST_BYTES;
	const bytes = new Uint8Array(bodyLength + CHECKSUM_BYTES);
	const view = new DataView(bytes.buffer);
	bytes.set(RECORD_MAGIC, 0);
	bytes[8] = RECORD_VERSION;
	view.setBigUint64(GENERATION_OFFSET, state.generation, true);
	view.setUint32(COUNT_OFFSET, state.references.length, true);
	for (let i = 0; i < state.references.length; i++) {
		bytes.set(state.references[i]!, RECORD_PREFIX_BYTES + i * DIGEST_BYTES);
	}
	bytes.set(
		hash(RECORD_DOMAIN, cidBytes, bytes.subarray(0, bodyLength)),
		bodyLength,
	);
	return bytes;
};

const decodeReferenceState = (
	cidBytes: Uint8Array,
	input: Uint8Array,
): ReferenceState => {
	const corrupt = (detail: string): never => {
		throw new ScopedBlockReclamationCorruptionError(
			`Scoped block reference state ${detail}`,
		);
	};
	const maxBytes =
		RECORD_PREFIX_BYTES +
		SCOPED_BLOCK_RECLAMATION_LIMITS.maxReferencesPerBlock * DIGEST_BYTES +
		CHECKSUM_BYTES;
	let bytes: Uint8Array;
	try {
		bytes = exactCopy(input, "Scoped block reference state", maxBytes, true);
	} catch (error) {
		corrupt(
			`is invalid: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	if (bytes.byteLength < RECORD_PREFIX_BYTES + CHECKSUM_BYTES) {
		corrupt("is truncated");
	}
	if (!equalBytes(bytes.subarray(0, RECORD_MAGIC.byteLength), RECORD_MAGIC)) {
		corrupt("has an invalid format marker");
	}
	if (bytes[8] !== RECORD_VERSION) corrupt("has an unsupported version");
	if (bytes[9] !== 0 || bytes[10] !== 0 || bytes[11] !== 0) {
		corrupt("has non-zero reserved bytes");
	}
	const view = new DataView(bytes.buffer);
	const generation = view.getBigUint64(GENERATION_OFFSET, true);
	if (generation === 0n) corrupt("has generation zero");
	const count = view.getUint32(COUNT_OFFSET, true);
	if (count > SCOPED_BLOCK_RECLAMATION_LIMITS.maxReferencesPerBlock) {
		corrupt("contains too many references");
	}
	const bodyLength = RECORD_PREFIX_BYTES + count * DIGEST_BYTES;
	if (bytes.byteLength !== bodyLength + CHECKSUM_BYTES) {
		corrupt("has an invalid encoded length");
	}
	const expectedChecksum = hash(
		RECORD_DOMAIN,
		cidBytes,
		bytes.subarray(0, bodyLength),
	);
	if (!equalBytes(bytes.subarray(bodyLength), expectedChecksum)) {
		corrupt("checksum does not match");
	}
	const references: Uint8Array[] = [];
	for (let i = 0; i < count; i++) {
		const reference = bytes.slice(
			RECORD_PREFIX_BYTES + i * DIGEST_BYTES,
			RECORD_PREFIX_BYTES + (i + 1) * DIGEST_BYTES,
		);
		if (
			references.length > 0 &&
			compareBytes(references[references.length - 1]!, reference) >= 0
		) {
			corrupt("references are not strictly ordered");
		}
		references.push(reference);
	}
	return { generation, references };
};

class ScopedBlockReclamationCorruptionError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ScopedBlockReclamationCorruptionError";
	}
}

class ScopedBlockReclamationFaultError extends Error {
	readonly cause: unknown;
	readonly reason: ScopedBlockReclamationFaultCode;

	constructor(reason: ScopedBlockReclamationFaultCode, cause: unknown) {
		super(`Scoped block reclamation faulted: ${reason}`);
		this.name = "ScopedBlockReclamationFaultError";
		this.reason = reason;
		this.cause = cause;
	}
}

class ScopedBlockReclamationLifecycleError extends Error {
	constructor() {
		super(
			"Scoped block reclamation handle is not valid for this service lifecycle",
		);
		this.name = "ScopedBlockReclamationLifecycleError";
	}
}

class ScopedBlockReclamationController implements ScopedBlockReclamationV1 {
	declare readonly kind: "scoped-references-v1";
	declare readonly limits: ScopedBlockReclamationLimits;
	private currentHealth: ScopedBlockReclamationHealth = OPENING_HEALTH;
	private dataStore?: AnyStore;
	private referenceStore?: AnyStore;
	private dataDurability?: CrashSafeDurability;
	private referenceDurability?: CrashSafeAtomicReplaceDurability;
	private lifecycleGeneration = 0;
	private tail: Promise<void> = Promise.resolve();
	private pendingOperations = 0;
	private pendingBytes = 0;
	private activeReadLeases = 0;
	private readonly readDrainWaiters = new Set<() => void>();
	private readonly managedIterators = new Set<ManagedIteratorState>();

	constructor(private readonly rootStore: AnyStore) {
		Object.defineProperties(this, {
			kind: {
				value: "scoped-references-v1",
				enumerable: true,
				writable: false,
				configurable: false,
			},
			limits: {
				value: SCOPED_BLOCK_RECLAMATION_LIMITS,
				enumerable: true,
				writable: false,
				configurable: false,
			},
		});
	}

	health(): ScopedBlockReclamationHealth {
		return this.currentHealth;
	}

	async start(): Promise<void> {
		this.currentHealth = OPENING_HEALTH;
		try {
			if ((await this.rootStore.persisted()) !== true) {
				throw new Error("Backing store is not persistent");
			}
			const namespace = await this.rootStore.sublevel(NAMESPACE);
			const dataStore = await namespace.sublevel(DATA_LEVEL);
			const referenceStore = await namespace.sublevel(REFERENCE_LEVEL);
			const dataDurability = dataStore.crashSafeDurability;
			const referenceDurability = referenceStore.crashSafeDurability;
			if (
				dataDurability?.crashSafe !== true ||
				typeof dataDurability.barrier !== "function" ||
				!isAtomicDurability(referenceDurability)
			) {
				throw new Error("Backing sublevels lack crash-safe durability");
			}
			await dataDurability.barrier();
			await referenceDurability.barrier();
			this.dataStore = dataStore;
			this.referenceStore = referenceStore;
			this.dataDurability = dataDurability;
			this.referenceDurability = referenceDurability;
			this.lifecycleGeneration += 1;
			this.currentHealth = READY_HEALTH;
		} catch (error) {
			this.fault("storage-failure", error);
		}
	}

	async stop(): Promise<void> {
		this.currentHealth = CLOSED_HEALTH;
		await this.tail;
		await this.waitForReadLeases();
		await Promise.all(
			[...this.managedIterators].map((state) =>
				this.closeManagedIterator(state),
			),
		);
		this.dataStore = undefined;
		this.referenceStore = undefined;
		this.dataDurability = undefined;
		this.referenceDurability = undefined;
	}

	failStartup(cause: unknown): never {
		this.fault("storage-failure", cause);
	}

	openScope(scopeKey: Uint8Array): ScopedBlockReclamationScopeV1 {
		this.requireReady(this.lifecycleGeneration);
		const key = copyScopeKey(scopeKey);
		const digest = hash(SCOPE_DOMAIN, key);
		const generation = this.lifecycleGeneration;
		return Object.freeze({
			put: (bytes: Uint8Array) => this.put(generation, digest, bytes),
			retain: (cid: string, bytes: Uint8Array) =>
				this.retain(generation, digest, cid, bytes),
			release: (cid: string) => this.release(generation, digest, cid),
		});
	}

	private put(
		generation: number,
		scopeDigest: Uint8Array,
		input: Uint8Array,
	): Promise<string> {
		return this.enqueueWithBytes(generation, input, async (bytes) => {
			const cid = (await calculateRawCid(bytes)).cid;
			return this.retainPrepared(scopeDigest, cid, bytes);
		});
	}

	private retain(
		generation: number,
		scopeDigest: Uint8Array,
		inputCid: string,
		input: Uint8Array,
	): Promise<string> {
		let cid: string;
		try {
			cid = canonicalCid(inputCid).cid;
		} catch (error) {
			return Promise.reject(error);
		}
		return this.enqueueWithBytes(generation, input, async (bytes) => {
			await verifyBlockBytes(cid, bytes);
			return this.retainPrepared(scopeDigest, cid, bytes);
		});
	}

	private async retainPrepared(
		scopeDigest: Uint8Array,
		cid: string,
		bytes: Uint8Array,
	): Promise<string> {
		const cidBytes = textEncoder.encode(cid);
		const state = await this.readReferenceState(cid, cidBytes);
		const stored = await this.readManagedBytes(cid);
		if (state && state.references.length > 0 && stored === undefined) {
			this.fault(
				"corrupt-state",
				new ScopedBlockReclamationCorruptionError(
					"Scoped block reference state points to missing bytes",
				),
			);
		}
		if (stored !== undefined) {
			await this.verifyManagedBytes(cid, stored);
			if (!equalBytes(stored, bytes)) {
				this.fault(
					"corrupt-state",
					new ScopedBlockReclamationCorruptionError(
						"Scoped managed block bytes disagree with retained bytes",
					),
				);
			}
		} else {
			await this.mutate(async () => {
				await this.dataStore!.put(cid, bytes);
				await this.dataDurability!.barrier();
			});
		}

		const references = [...(state?.references ?? [])];
		if (references.some((reference) => equalBytes(reference, scopeDigest))) {
			return cid;
		}
		if (
			references.length >= SCOPED_BLOCK_RECLAMATION_LIMITS.maxReferencesPerBlock
		) {
			throw new RangeError(
				`Scoped block reference count exceeds ${SCOPED_BLOCK_RECLAMATION_LIMITS.maxReferencesPerBlock}`,
			);
		}
		references.push(scopeDigest);
		references.sort(compareBytes);
		await this.writeReferenceState(cid, cidBytes, {
			generation: this.nextGeneration(state),
			references,
		});
		return cid;
	}

	private async release(
		generation: number,
		scopeDigest: Uint8Array,
		inputCid: string,
	): Promise<ScopedBlockReleaseResult> {
		try {
			validateCidInput(inputCid);
		} catch (error) {
			return Promise.reject(error);
		}
		return this.enqueue(generation, async () => {
			const { cid, bytes: cidBytes } = canonicalCid(inputCid);
			const state = await this.readReferenceState(cid, cidBytes);
			if (!state || state.references.length === 0) {
				await this.cleanupUnreferenced(cid, state !== undefined);
				return "not-retained";
			}
			const stored = await this.readManagedBytes(cid);
			if (stored === undefined) {
				this.fault(
					"corrupt-state",
					new ScopedBlockReclamationCorruptionError(
						"Scoped block reference state points to missing bytes",
					),
				);
			}
			await this.verifyManagedBytes(cid, stored);
			const index = state.references.findIndex((reference) =>
				equalBytes(reference, scopeDigest),
			);
			if (index === -1) return "not-retained";
			const references = state.references.filter((_, i) => i !== index);
			await this.writeReferenceState(cid, cidBytes, {
				generation: this.nextGeneration(state),
				references,
			});
			if (references.length > 0) return "retained";
			await this.cleanupUnreferenced(cid, true);
			return "reclaimed";
		});
	}

	private nextGeneration(state: ReferenceState | undefined): bigint {
		const generation = (state?.generation ?? 0n) + 1n;
		if (generation > MAX_GENERATION) {
			throw new RangeError("Scoped block reference generation exceeds u64");
		}
		return generation;
	}

	private async readReferenceState(
		cid: string,
		cidBytes: Uint8Array,
	): Promise<ReferenceState | undefined> {
		let bytes: Uint8Array | undefined;
		try {
			bytes = await this.referenceStore!.get(cid);
		} catch (error) {
			this.fault("storage-failure", error);
		}
		if (bytes === undefined) return undefined;
		try {
			return decodeReferenceState(cidBytes, bytes);
		} catch (error) {
			this.fault("corrupt-state", error);
		}
	}

	private async writeReferenceState(
		cid: string,
		cidBytes: Uint8Array,
		state: ReferenceState,
	): Promise<void> {
		const bytes = encodeReferenceState(cidBytes, state);
		try {
			await this.referenceDurability!.atomicReplace(cid, bytes);
		} catch (error) {
			this.fault("ambiguous-mutation", error);
		}
	}

	private async readManagedBytes(cid: string): Promise<Uint8Array | undefined> {
		let value: Uint8Array | undefined;
		try {
			value = await this.dataStore!.get(cid);
		} catch (error) {
			this.fault("storage-failure", error);
		}
		if (value === undefined) return undefined;
		return this.copyManagedBytes(value);
	}

	private copyManagedBytes(value: Uint8Array): Uint8Array {
		try {
			return exactCopy(
				value,
				"Scoped managed block bytes",
				SCOPED_BLOCK_RECLAMATION_LIMITS.maxBlockBytes,
				true,
			);
		} catch (error) {
			this.fault(
				"corrupt-state",
				new ScopedBlockReclamationCorruptionError(
					`Scoped managed block bytes are invalid: ${error instanceof Error ? error.message : String(error)}`,
				),
			);
		}
	}

	private async verifyManagedBytes(
		cid: string,
		bytes: Uint8Array,
	): Promise<void> {
		try {
			await verifyBlockBytes(cid, bytes);
		} catch (error) {
			this.fault("corrupt-state", error);
		}
	}

	private async cleanupUnreferenced(
		cid: string,
		hasZeroReferenceRecord: boolean,
	): Promise<void> {
		const stored = await this.readManagedBytes(cid);
		if (stored !== undefined) {
			await this.mutate(async () => {
				await this.dataStore!.del(cid);
				await this.dataDurability!.barrier();
			});
		}
		if (hasZeroReferenceRecord) {
			await this.mutate(async () => {
				await this.referenceStore!.del(cid);
				await this.referenceDurability!.barrier();
			});
		}
	}

	private async mutate(operation: () => Promise<void>): Promise<void> {
		try {
			await operation();
		} catch (error) {
			this.fault("ambiguous-mutation", error);
		}
	}

	private enqueue<T>(
		generation: number,
		operation: () => Promise<T>,
		reservedBytes = 0,
	): Promise<T> {
		if (
			this.pendingOperations >=
				SCOPED_BLOCK_RECLAMATION_LIMITS.maxPendingOperations ||
			this.pendingBytes + reservedBytes >
				SCOPED_BLOCK_RECLAMATION_LIMITS.maxPendingBytes
		) {
			return Promise.reject(
				new RangeError("Scoped block reclamation pending-work limit exceeded"),
			);
		}
		this.pendingOperations += 1;
		this.pendingBytes += reservedBytes;
		const result = this.tail.then(async () => {
			this.requireReady(generation);
			return operation();
		});
		this.tail = result.then(
			(): void => undefined,
			(): void => undefined,
		);
		return result.finally(() => {
			this.pendingOperations -= 1;
			this.pendingBytes -= reservedBytes;
		});
	}

	private enqueueWithBytes<T>(
		generation: number,
		input: Uint8Array,
		operation: (bytes: Uint8Array) => Promise<T>,
	): Promise<T> {
		let byteLength: number;
		try {
			byteLength = exactByteLength(input, "Scoped block bytes");
		} catch (error) {
			return Promise.reject(error);
		}
		if (byteLength > SCOPED_BLOCK_RECLAMATION_LIMITS.maxBlockBytes) {
			return Promise.reject(
				new RangeError(
					`Scoped block bytes must contain at most ${SCOPED_BLOCK_RECLAMATION_LIMITS.maxBlockBytes} bytes`,
				),
			);
		}
		if (
			this.pendingOperations >=
				SCOPED_BLOCK_RECLAMATION_LIMITS.maxPendingOperations ||
			this.pendingBytes + byteLength >
				SCOPED_BLOCK_RECLAMATION_LIMITS.maxPendingBytes
		) {
			return Promise.reject(
				new RangeError("Scoped block reclamation pending-work limit exceeded"),
			);
		}
		let bytes: Uint8Array;
		try {
			bytes = exactCopy(
				input,
				"Scoped block bytes",
				SCOPED_BLOCK_RECLAMATION_LIMITS.maxBlockBytes,
				true,
			);
		} catch (error) {
			return Promise.reject(error);
		}
		return this.enqueue(generation, () => operation(bytes), byteLength);
	}

	private requireReady(generation: number): void {
		if (
			this.currentHealth.status !== "ready" ||
			generation !== this.lifecycleGeneration
		) {
			throw new ScopedBlockReclamationLifecycleError();
		}
	}

	private fault(
		reason: ScopedBlockReclamationFaultCode,
		cause: unknown,
	): never {
		// Stop wins the lifecycle race. A read already admitted before close may
		// still reject, but it must not resurrect a closed controller as faulted.
		if (this.currentHealth.status !== "closed") {
			this.currentHealth = Object.freeze({ status: "faulted", reason });
		}
		if (cause instanceof ScopedBlockReclamationFaultError) throw cause;
		throw new ScopedBlockReclamationFaultError(reason, cause);
	}

	private acquireReadLease(): (() => void) | undefined {
		if (this.currentHealth.status === "closed" || !this.dataStore) {
			return undefined;
		}
		this.activeReadLeases += 1;
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.activeReadLeases -= 1;
			if (this.activeReadLeases !== 0) return;
			for (const resolve of this.readDrainWaiters) resolve();
			this.readDrainWaiters.clear();
		};
	}

	private waitForReadLeases(): Promise<void> {
		if (this.activeReadLeases === 0) return Promise.resolve();
		return new Promise((resolve) => this.readDrainWaiters.add(resolve));
	}

	private closeManagedIterator(state: ManagedIteratorState): Promise<void> {
		if (!state.closing) {
			state.closing = (async () => {
				try {
					await state.iterator.return?.();
				} catch (error) {
					if (this.currentHealth.status !== "closed") {
						this.fault("storage-failure", error);
					}
				} finally {
					this.managedIterators.delete(state);
				}
			})();
		}
		return state.closing;
	}

	async readForBlocks(inputCid: string): Promise<Uint8Array | undefined> {
		let cid: string;
		try {
			cid = canonicalCid(inputCid).cid;
		} catch {
			return undefined;
		}
		const release = this.acquireReadLease();
		if (!release) return undefined;
		try {
			return await this.readManagedBytes(cid);
		} finally {
			release();
		}
	}

	async *iterateManaged(): AsyncGenerator<[string, Uint8Array], void, void> {
		const dataStore = this.dataStore;
		if (!dataStore || this.currentHealth.status === "closed") return;
		const iterator = dataStore.iterator()[Symbol.asyncIterator]();
		const state: ManagedIteratorState = { iterator };
		this.managedIterators.add(state);
		try {
			while (true) {
				const release = this.acquireReadLease();
				if (!release) return;
				let entry: [string, Uint8Array] | undefined;
				try {
					const next = await iterator.next();
					if (next.done) return;
					const [inputCid, inputBytes] = next.value as [string, Uint8Array];
					let cid: string;
					try {
						cid = canonicalCid(inputCid).cid;
						if (cid !== inputCid) {
							throw new Error("Managed block key is not canonical");
						}
					} catch (error) {
						this.fault("corrupt-state", error);
					}
					const bytes = this.copyManagedBytes(inputBytes);
					// Resolve raw aliases while the lease is held. The outer block-store
					// iterator must not perform an untracked root read after close begins.
					if ((await this.rootStore.get(cid)) === undefined) {
						entry = [cid, bytes];
					}
				} catch (error) {
					if (error instanceof ScopedBlockReclamationFaultError) throw error;
					this.fault("storage-failure", error);
				} finally {
					release();
				}
				if (entry) yield entry;
			}
		} finally {
			await this.closeManagedIterator(state);
		}
	}
}

class ScopedReclamationBlockStore extends AnyBlockStore {
	declare readonly localReclamation: ScopedBlockReclamationV1;

	constructor(
		store: AnyStore,
		private readonly reclamation: ScopedBlockReclamationController,
	) {
		super(store);
		Object.defineProperty(this, "localReclamation", {
			value: reclamation,
			enumerable: true,
			writable: false,
			configurable: false,
		});
	}

	override async start(): Promise<void> {
		try {
			await super.start();
			await this.reclamation.start();
		} catch (error) {
			try {
				await super.stop();
			} catch {
				// Preserve the startup fault; the service was never made ready.
			}
			this.reclamation.failStartup(error);
		}
	}

	override async stop(): Promise<void> {
		await this.reclamation.stop();
		await super.stop();
	}

	override async get(
		cid: string,
		options?: Parameters<AnyBlockStore["get"]>[1],
	): Promise<Uint8Array | undefined> {
		if (!isRawBlockCid(cid)) return undefined;
		const raw = await super.get(cid, options);
		if (raw !== undefined) return raw;
		const managed = await this.reclamation.readForBlocks(cid);
		return managed === undefined
			? undefined
			: this.decodeStoredBytes(cid, managed, options);
	}

	override async getMany(
		cids: string[],
		options?: Parameters<AnyBlockStore["getMany"]>[1],
	): Promise<Array<Uint8Array | undefined>> {
		const length = cids.length;
		const values = new Array<Uint8Array | undefined>(length).fill(undefined);
		const validIndexes: number[] = [];
		const validCids: string[] = [];
		for (let index = 0; index < length; index++) {
			const cid = cids[index];
			if (!isRawBlockCid(cid)) continue;
			validIndexes.push(index);
			validCids.push(cid);
		}
		const rawValues = await super.getMany(validCids, options);
		for (let index = 0; index < validIndexes.length; index++) {
			values[validIndexes[index]!] = rawValues[index];
		}
		await Promise.all(
			validIndexes.map(async (index, validIndex) => {
				const value = values[index];
				if (value !== undefined) return;
				const cid = validCids[validIndex]!;
				const managed = await this.reclamation.readForBlocks(cid);
				if (managed !== undefined) {
					values[index] = await this.decodeStoredBytes(cid, managed, options);
				}
			}),
		);
		return values;
	}

	override async has(cid: string): Promise<boolean> {
		if (!isRawBlockCid(cid)) return false;
		return (
			(await super.has(cid)) ||
			(await this.reclamation.readForBlocks(cid)) !== undefined
		);
	}

	override async hasMany(cids: string[]): Promise<boolean[]> {
		const length = cids.length;
		const values = new Array<boolean>(length).fill(false);
		const validIndexes: number[] = [];
		const validCids: string[] = [];
		for (let index = 0; index < length; index++) {
			const cid = cids[index];
			if (!isRawBlockCid(cid)) continue;
			validIndexes.push(index);
			validCids.push(cid);
		}
		const rawValues = await super.hasMany(validCids);
		for (let index = 0; index < validIndexes.length; index++) {
			values[validIndexes[index]!] = rawValues[index]!;
		}
		await Promise.all(
			validIndexes.map(async (index, validIndex) => {
				const value = values[index];
				if (!value) {
					values[index] =
						(await this.reclamation.readForBlocks(validCids[validIndex]!)) !==
						undefined;
				}
			}),
		);
		return values;
	}

	override async put(
		input: Parameters<AnyBlockStore["put"]>[0],
	): Promise<string> {
		if (input instanceof Uint8Array) return super.put(input);
		const cid = input?.cid;
		requireRawBlockCid(cid);
		const block = input.block;
		return super.put({ cid, block });
	}

	override async putMany(
		inputs: Parameters<AnyBlockStore["putMany"]>[0],
	): Promise<string[]> {
		const stableInputs: Parameters<AnyBlockStore["putMany"]>[0] = [];
		const length = inputs.length;
		for (let index = 0; index < length; index++) {
			const input = inputs[index]!;
			if (input instanceof Uint8Array) {
				stableInputs.push(input);
				continue;
			}
			const cid = input?.cid;
			requireRawBlockCid(cid);
			const block = input.block;
			stableInputs.push({ cid, block });
		}
		return super.putMany(stableInputs);
	}

	override putKnown(cid: string, bytes: Uint8Array): Promise<string> | string {
		requireRawBlockCid(cid);
		return super.putKnown(cid, bytes);
	}

	override putKnownMany(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): Promise<string[]> | string[] {
		const stableBlocks: Array<readonly [string, Uint8Array]> = [];
		const length = blocks.length;
		for (let index = 0; index < length; index++) {
			const block = blocks[index]!;
			const cid = block[0];
			requireRawBlockCid(cid);
			stableBlocks.push([cid, block[1]]);
		}
		return super.putKnownMany(stableBlocks);
	}

	override async rm(cid: string): Promise<void> {
		requireRawBlockCid(cid);
		return super.rm(cid);
	}

	override async rmMany(cids: string[]): Promise<number> {
		const stableCids: string[] = [];
		const length = cids.length;
		for (let index = 0; index < length; index++) {
			const cid = cids[index];
			requireRawBlockCid(cid);
			stableCids.push(cid);
		}
		return super.rmMany(stableCids);
	}

	override async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for await (const entry of super.iterator()) {
			if (isRawBlockCid(entry[0])) yield entry;
		}
		for await (const [cid, bytes] of this.reclamation.iterateManaged()) {
			yield [cid, bytes];
		}
	}
}

/** @internal Used only after DirectBlock has proved this is its built-in store. */
export const createBuiltInScopedReclamationBlockStore = (
	store: AnyStore,
): AnyBlockStore => {
	if (!isAtomicDurability(store.crashSafeDurability)) {
		return new AnyBlockStore(store);
	}
	const reclamation = new ScopedBlockReclamationController(store);
	return new ScopedReclamationBlockStore(store, reclamation);
};
