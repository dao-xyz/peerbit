import { deserialize, serialize } from "@dao-xyz/borsh";
import type { CrashSafeAtomicReplaceStore } from "@peerbit/any-store-interface";
import { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";
import { sha256Sync } from "@peerbit/crypto";
import { concat, equals } from "uint8arrays";
import {
	type ClassifiedResourceOperationLeaseV2,
	type ClassifiedResourceOperationResultV2,
	type ResourceOperationAuthorizationOptionsV2,
	TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_OPERATION_AUTHORIZATIONS,
	TrustedNetworkV2ResourceOperationEngine,
	type TrustedNetworkV2ResourceOperationEngineProperties,
} from "./v2-resource-operation-engine.js";
import {
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES,
	authenticateResourceOperationEntryV2,
} from "./v2-resource-operation-entry.js";
import {
	NetworkDescriptorV2,
	assertNetworkDescriptorV2,
	copyUint8ArrayWithLengthV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

export const TRUSTED_NETWORK_V2_MAX_RETAINED_RESOURCE_OPERATIONS = 256;
export const TRUSTED_NETWORK_V2_MAX_RETAINED_RESOURCE_OPERATION_BYTES =
	16 * 1024 * 1024;

const MAGIC = Uint8Array.of(0x50, 0x42, 0x52, 1);
const HEADER_BYTES = MAGIC.byteLength + 4;
const SCOPE_DOMAIN = new TextEncoder().encode(
	"peerbit/trusted-network/v2/resource-operation-journal/v1\0",
);

const u32 = (value: number): Uint8Array => {
	const bytes = new Uint8Array(4);
	new DataView(bytes.buffer).setUint32(0, value, true);
	return bytes;
};

const bound = (value: number, maximum: number, name: string): number => {
	if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
		throw new RangeError(`${name} must be between 1 and ${maximum}`);
	}
	return value;
};

export type RetainedResourceOperationResultV2 =
	| Readonly<{ status: "retained"; entryCid: string; duplicate: boolean }>
	| Readonly<{
			status: "rejected" | "capacity" | "unavailable" | "halted";
			reason: string;
	  }>;

export type TrustedNetworkV2ResourceOperationJournalProperties =
	TrustedNetworkV2ResourceOperationEngineProperties &
		Readonly<{
			/** Already-open, dedicated store/sublevel with one owning journal. */
			store: CrashSafeAtomicReplaceStore;
			maxEntries?: number;
			maxRetainedBytes?: number;
		}>;

/**
 * Internal recovery journal for explicitly submitted signed operations.
 * Retention authenticates bytes/scope, not WRITER authority or acceptance.
 * It never evicts, re-signs or applies an operation. Classification is always
 * fresh and uses the existing policy -> fence leases through the caller's use.
 * This is not a complete resource-log inventory, projection or compaction API.
 */
export class TrustedNetworkV2ResourceOperationJournal {
	private records = new Map<string, Uint8Array>();
	private retainedBytes = 0;
	private pending?: Promise<RetainedResourceOperationResultV2>;
	private readonly classifications = new Set<Promise<unknown>>();
	private closed = false;
	private terminalError?: unknown;

	private constructor(
		private readonly checkpoint: CrashSafeTwoSlotCheckpoint,
		private readonly engine: TrustedNetworkV2ResourceOperationEngine,
		private readonly scope: {
			descriptor: NetworkDescriptorV2;
			expectedResourceId: Uint8Array;
			expectedGid: string;
		},
		private readonly maxEntries: number,
		private readonly maxRetainedBytes: number,
		private readonly signal?: AbortSignal,
	) {}

	static async open(
		properties: TrustedNetworkV2ResourceOperationJournalProperties,
	): Promise<TrustedNetworkV2ResourceOperationJournal> {
		const signal = properties.signal;
		const suppliedResourceId = properties.expectedResourceId;
		assertNetworkDescriptorV2(properties.descriptor);
		const descriptor = deserialize(
			serialize(properties.descriptor),
			NetworkDescriptorV2,
		);
		if (exactUint8ArrayByteLengthV2(suppliedResourceId) !== 32) {
			throw new Error("Expected resource id must contain exactly 32 bytes");
		}
		const expectedResourceId = copyUint8ArrayWithLengthV2(
			suppliedResourceId,
			32,
		);
		const expectedGid = properties.expectedGid;
		if (typeof expectedGid !== "string" || expectedGid.length > 1_024) {
			throw new TypeError(
				"Resource journal gid must be at most 1,024 characters",
			);
		}
		const maxEntries = bound(
			properties.maxEntries ??
				TRUSTED_NETWORK_V2_MAX_RETAINED_RESOURCE_OPERATIONS,
			TRUSTED_NETWORK_V2_MAX_RETAINED_RESOURCE_OPERATIONS,
			"maxEntries",
		);
		const maxRetainedBytes = bound(
			properties.maxRetainedBytes ??
				TRUSTED_NETWORK_V2_MAX_RETAINED_RESOURCE_OPERATION_BYTES,
			TRUSTED_NETWORK_V2_MAX_RETAINED_RESOURCE_OPERATION_BYTES,
			"maxRetainedBytes",
		);
		const scope = { descriptor, expectedResourceId, expectedGid };
		const descriptorBytes = serialize(descriptor);
		const gidBytes = new TextEncoder().encode(expectedGid);
		const engine = new TrustedNetworkV2ResourceOperationEngine({
			...properties,
			...scope,
			signal,
		});
		try {
			if (signal?.aborted) throw new Error("Journal open aborted");
			const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
				store: properties.store,
				scope: sha256Sync(
					concat([
						SCOPE_DOMAIN,
						u32(descriptorBytes.byteLength),
						descriptorBytes,
						expectedResourceId,
						u32(gidBytes.byteLength),
						gidBytes,
					]),
				),
				maxPayloadBytes: HEADER_BYTES + maxEntries * 4 + maxRetainedBytes,
			});
			const journal = new TrustedNetworkV2ResourceOperationJournal(
				checkpoint,
				engine,
				scope,
				maxEntries,
				maxRetainedBytes,
				signal,
			);
			const saved = checkpoint.current;
			if (saved) await journal.restore(saved.payload);
			journal.assertReadable();
			return journal;
		} catch (error) {
			engine.abort();
			throw error;
		}
	}

	get size(): number {
		this.assertReadable();
		return this.records.size;
	}

	get byteLength(): number {
		this.assertReadable();
		return this.retainedBytes;
	}

	/** Bounded, copied inventory. It says nothing about unsubmitted remote work. */
	entries(): readonly string[] {
		this.assertReadable();
		return [...this.records.keys()].sort();
	}

	/** Exact signed bytes for recovery/export; never an authorization token. */
	get(entryCid: string): Uint8Array | undefined {
		this.assertReadable();
		const bytes = this.records.get(entryCid);
		return bytes === undefined ? undefined : new Uint8Array(bytes);
	}

	retain(
		entryBytes: Uint8Array,
		options?: Readonly<{ signal?: AbortSignal }>,
	): Promise<RetainedResourceOperationResultV2> {
		if (
			this.closed ||
			this.signal?.aborted ||
			this.terminalError !== undefined
		) {
			return Promise.resolve({
				status: "halted",
				reason: "Journal is closed or faulted",
			});
		}
		if (this.pending !== undefined) {
			return Promise.resolve({
				status: "capacity",
				reason: "A retention is already in flight",
			});
		}
		if (options?.signal?.aborted) {
			return Promise.resolve({
				status: "unavailable",
				reason: "Retention was cancelled",
			});
		}
		let captured: Uint8Array;
		try {
			const length = exactUint8ArrayByteLengthV2(entryBytes);
			bound(
				length,
				TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES,
				"entryBytes",
			);
			captured = copyUint8ArrayWithLengthV2(entryBytes, length);
		} catch {
			return Promise.resolve({
				status: "rejected",
				reason: "Invalid bounded operation bytes",
			});
		}
		const pending = this.retainCaptured(captured, options?.signal);
		this.pending = pending;
		const settled = () => {
			if (this.pending === pending) this.pending = undefined;
		};
		void pending.then(settled, settled);
		return pending;
	}

	/** The callback must not await close() or reenter either anchor. */
	withClassifiedOperation<T>(
		entryCid: string,
		use: (lease: ClassifiedResourceOperationLeaseV2) => T | Promise<T>,
		options?: ResourceOperationAuthorizationOptionsV2,
	): Promise<ClassifiedResourceOperationResultV2<T>> {
		if (
			this.closed ||
			this.signal?.aborted ||
			this.terminalError !== undefined
		) {
			return Promise.resolve({
				status: "halted",
				reason: "Journal is closed or faulted",
				fetchHints: [],
			});
		}
		const bytes = this.records.get(entryCid);
		if (bytes === undefined) {
			return Promise.resolve({
				status: "unavailable",
				reason: "Operation is not retained",
				fetchHints: [],
			});
		}
		if (
			this.classifications.size >=
			TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_OPERATION_AUTHORIZATIONS
		) {
			return Promise.resolve({
				status: "unavailable",
				reason: "Journal classification capacity reached",
				fetchHints: [],
			});
		}
		const work = this.engine.withClassifiedOperation(bytes, use, options);
		this.classifications.add(work);
		const settled = () => {
			this.classifications.delete(work);
		};
		void work.then(settled, settled);
		return work;
	}

	/** Drains entered work without closing the caller-owned store. */
	async close(): Promise<void> {
		this.closed = true;
		this.engine.abort();
		// Failures remain on their original operation promises. Closing must still
		// wait for every started commit before callers may close the shared store.
		await Promise.allSettled([
			...this.classifications,
			...(this.pending === undefined ? [] : [this.pending]),
		]);
		this.records.clear();
		this.retainedBytes = 0;
	}

	private assertReadable(): void {
		if (this.terminalError !== undefined) throw this.terminalError;
		if (this.closed || this.signal?.aborted)
			throw new Error("Journal is closed");
	}

	private async retainCaptured(
		bytes: Uint8Array,
		signal: AbortSignal | undefined,
	): Promise<RetainedResourceOperationResultV2> {
		let entryCid: string;
		try {
			const authenticated = await authenticateResourceOperationEntryV2({
				entryBytes: bytes,
				...this.scope,
			});
			entryCid = authenticated.entryCid;
		} catch {
			return {
				status: "rejected",
				reason: "Operation signature or scope is invalid",
			};
		}
		if (this.closed || this.signal?.aborted || signal?.aborted) {
			return { status: "unavailable", reason: "Retention was cancelled" };
		}
		if (this.records.has(entryCid)) {
			return { status: "retained", entryCid, duplicate: true };
		}
		if (
			this.records.size >= this.maxEntries ||
			this.retainedBytes + bytes.byteLength > this.maxRetainedBytes
		) {
			return {
				status: "capacity",
				reason: "Retained operation limit reached; caller must keep the input",
			};
		}
		const records = new Map(this.records).set(entryCid, bytes);
		const retainedBytes = this.retainedBytes + bytes.byteLength;
		const payload = this.encode(records, retainedBytes);
		try {
			await this.checkpoint.commit(payload);
		} catch (error) {
			this.terminalError = error;
			this.engine.abort();
			throw error;
		}
		// Cancellation cannot turn a completed durable replacement into a refusal.
		this.records = records;
		this.retainedBytes = retainedBytes;
		return { status: "retained", entryCid, duplicate: false };
	}

	private encode(
		records: Map<string, Uint8Array>,
		totalBytes: number,
	): Uint8Array {
		const payload = new Uint8Array(
			HEADER_BYTES + records.size * 4 + totalBytes,
		);
		payload.set(MAGIC);
		const view = new DataView(payload.buffer);
		view.setUint32(MAGIC.byteLength, records.size, true);
		let offset = HEADER_BYTES;
		for (const cid of [...records.keys()].sort()) {
			const bytes = records.get(cid)!;
			view.setUint32(offset, bytes.byteLength, true);
			offset += 4;
			payload.set(bytes, offset);
			offset += bytes.byteLength;
		}
		return payload;
	}

	private async restore(payload: Uint8Array): Promise<void> {
		if (
			payload.byteLength < HEADER_BYTES ||
			!equals(payload.subarray(0, MAGIC.byteLength), MAGIC)
		) {
			throw new Error("Invalid resource operation journal header");
		}
		const view = new DataView(
			payload.buffer,
			payload.byteOffset,
			payload.byteLength,
		);
		const count = view.getUint32(MAGIC.byteLength, true);
		if (count > this.maxEntries)
			throw new Error("Resource operation journal exceeds entry limit");
		let offset = HEADER_BYTES;
		let previous: string | undefined;
		for (let i = 0; i < count; i++) {
			this.assertReadable();
			if (offset + 4 > payload.byteLength)
				throw new Error("Truncated resource operation journal");
			const length = view.getUint32(offset, true);
			offset += 4;
			if (
				length < 1 ||
				length > TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES ||
				offset + length > payload.byteLength ||
				this.retainedBytes + length > this.maxRetainedBytes
			)
				throw new Error("Invalid resource operation journal entry length");
			const bytes = payload.slice(offset, offset + length);
			offset += length;
			const { entryCid } = await authenticateResourceOperationEntryV2({
				entryBytes: bytes,
				...this.scope,
			});
			if (previous !== undefined && previous >= entryCid)
				throw new Error(
					"Resource operation journal entries are not unique and ordered",
				);
			previous = entryCid;
			this.records.set(entryCid, bytes);
			this.retainedBytes += length;
		}
		if (offset !== payload.byteLength)
			throw new Error("Trailing resource operation journal bytes");
	}
}
