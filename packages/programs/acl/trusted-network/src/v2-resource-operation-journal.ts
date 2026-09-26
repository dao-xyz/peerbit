import { deserialize, serialize } from "@dao-xyz/borsh";
import type { CrashSafeAtomicReplaceStore } from "@peerbit/any-store-interface";
import { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";
import { sha256Sync } from "@peerbit/crypto";
import { concat, equals } from "uint8arrays";
import type { PolicyLeaseResultV2 } from "./v2-policy-anchor.js";
import type {
	ExactResourceFenceHeadLeaseV2,
	TrustedNetworkV2DurableResourceFenceReducer,
} from "./v2-resource-fence-anchor.js";
import {
	ResourceCausalWorkBudgetV2,
	type ResourceCausalWorkLimitsV2,
	TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS,
	captureCanonicalResourceFenceCidV2,
} from "./v2-resource-fence-engine.js";
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

const MAGIC = Uint8Array.of(0x50, 0x42, 0x52, 2);
const HEADER_BYTES = MAGIC.byteLength + 4;
// Presence, policy u64/digest, framed canonical fence CID/digest, inventory digest.
const MAX_PROJECTION_HEADER_BYTES =
	1 +
	8 +
	32 +
	4 +
	TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS +
	32 +
	32;
const REPLAY_TIMEOUT_MS = 10_000;
const PROJECTION_STATUSES = [
	"policy-final",
	"provisional",
	"rejected",
] as const;
const INVENTORY_DOMAIN = new TextEncoder().encode(
	"peerbit/trusted-network/v2/resource-operation-inventory/v1\0",
);
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

/** Historical local replay evidence, never a reusable authorization token. */
export type RetainedResourceProjectionV2 = Readonly<{
	acceptedPolicyHead: Readonly<{ sequence: bigint; digest: Uint8Array }>;
	acceptedFenceHead: Readonly<{ entryCid: string; digest: Uint8Array }>;
	inventoryDigest: Uint8Array;
	entries: readonly Readonly<{
		entryCid: string;
		status: (typeof PROJECTION_STATUSES)[number];
	}>[];
}>;

export type ReplayedResourceOperationsV2 = Omit<
	RetainedResourceProjectionV2,
	"entries"
> & {
	readonly entries: readonly (RetainedResourceProjectionV2["entries"][number] & {
		readonly applicationPayload?: Uint8Array;
	})[];
};

const copyProjection = (
	projection: RetainedResourceProjectionV2,
): RetainedResourceProjectionV2 => ({
	acceptedPolicyHead: {
		sequence: projection.acceptedPolicyHead.sequence,
		digest: new Uint8Array(projection.acceptedPolicyHead.digest),
	},
	acceptedFenceHead: {
		entryCid: projection.acceptedFenceHead.entryCid,
		digest: new Uint8Array(projection.acceptedFenceHead.digest),
	},
	inventoryDigest: new Uint8Array(projection.inventoryDigest),
	entries: projection.entries.map(({ entryCid, status }) => ({
		entryCid,
		status,
	})),
});

const sameHeads = (
	left: Pick<
		RetainedResourceProjectionV2,
		"acceptedPolicyHead" | "acceptedFenceHead"
	>,
	right: Pick<
		RetainedResourceProjectionV2,
		"acceptedPolicyHead" | "acceptedFenceHead"
	>,
): boolean =>
	left.acceptedPolicyHead.sequence === right.acceptedPolicyHead.sequence &&
	equals(left.acceptedPolicyHead.digest, right.acceptedPolicyHead.digest) &&
	left.acceptedFenceHead.entryCid === right.acceptedFenceHead.entryCid &&
	equals(left.acceptedFenceHead.digest, right.acceptedFenceHead.digest);

const headsFromLease = (lease: ExactResourceFenceHeadLeaseV2) => ({
	acceptedPolicyHead: {
		sequence: lease.acceptedHead.sequence,
		digest: new Uint8Array(lease.acceptedHead.digest),
	},
	acceptedFenceHead: {
		entryCid: lease.fence.entryCid,
		digest: new Uint8Array(lease.fence.digest),
	},
});

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
 * Its replay projection covers only this retained set, not complete remote
 * history or application-specific conflict resolution. No compaction API.
 */
export class TrustedNetworkV2ResourceOperationJournal {
	private records = new Map<string, Uint8Array>();
	private retainedBytes = 0;
	private pending?: Promise<unknown>;
	private readonly classifications = new Set<Promise<unknown>>();
	private readonly lifecycle = new AbortController();
	private projection?: RetainedResourceProjectionV2;
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
		private readonly fenceAnchor: TrustedNetworkV2DurableResourceFenceReducer,
		private readonly causalWorkLimits: Readonly<ResourceCausalWorkLimitsV2>,
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
		const causalWorkLimits = new ResourceCausalWorkBudgetV2(
			properties.causalWorkLimits,
		).remaining;
		const fenceAnchor = properties.fenceAnchor;
		const descriptorBytes = serialize(descriptor);
		const gidBytes = new TextEncoder().encode(expectedGid);
		const engine = new TrustedNetworkV2ResourceOperationEngine({
			...properties,
			...scope,
			fenceAnchor,
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
				maxPayloadBytes:
					HEADER_BYTES +
					maxEntries * 5 +
					maxRetainedBytes +
					MAX_PROJECTION_HEADER_BYTES,
			});
			const journal = new TrustedNetworkV2ResourceOperationJournal(
				checkpoint,
				engine,
				scope,
				maxEntries,
				maxRetainedBytes,
				fenceAnchor,
				causalWorkLimits,
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

	/** Diagnostic only, including after reopen. Authoritative use requires replay. */
	get persistedProjection(): RetainedResourceProjectionV2 | undefined {
		this.assertReadable();
		return this.projection === undefined
			? undefined
			: copyProjection(this.projection);
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
				reason: "A journal mutation is already in flight",
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
		return this.trackMutation(this.retainCaptured(captured, options?.signal));
	}

	/**
	 * Replay this entire local retained set at one exact head pair, checkpoint the
	 * authorization projection, then hold both anchors through use. CID order is
	 * canonical set order, not a causal/application order. No automatic retries.
	 * The consumer must not reenter the journal/anchors or await close().
	 */
	withReplayedOperations<T>(
		fenceEntryCid: string,
		use: (view: ReplayedResourceOperationsV2) => T | Promise<T>,
		options?: Omit<ResourceOperationAuthorizationOptionsV2, "causalWork">,
	): Promise<PolicyLeaseResultV2<T>> {
		if (this.closed || this.signal?.aborted || this.terminalError !== undefined)
			return Promise.resolve({
				status: "halted",
				reason: "Journal is closed or faulted",
			});
		if (this.pending !== undefined)
			return Promise.resolve({
				status: "capacity",
				reason: "A journal mutation is already in flight",
			});
		let deadline: number;
		let signal: AbortSignal;
		try {
			fenceEntryCid = captureCanonicalResourceFenceCidV2(fenceEntryCid);
			const timeout = options?.timeoutMs ?? REPLAY_TIMEOUT_MS;
			const suppliedDeadline = options?.deadline;
			if (
				typeof use !== "function" ||
				!Number.isSafeInteger(timeout) ||
				timeout < 0 ||
				timeout > REPLAY_TIMEOUT_MS ||
				(suppliedDeadline !== undefined &&
					(!Number.isSafeInteger(suppliedDeadline) || suppliedDeadline < 0))
			)
				throw new Error("Invalid replay options");
			deadline = Math.min(Date.now() + timeout, suppliedDeadline ?? Infinity);
			const callerSignal = options?.signal;
			signal = AbortSignal.any([
				this.lifecycle.signal,
				...(this.signal === undefined ? [] : [this.signal]),
				...(callerSignal === undefined ? [] : [callerSignal]),
			]);
		} catch {
			return Promise.resolve({
				status: "rejected",
				reason: "Invalid replay requirement",
			});
		}
		return this.trackMutation(
			this.replay(fenceEntryCid, use, deadline, signal),
		);
	}

	private trackMutation<T>(pending: Promise<T>): Promise<T> {
		this.pending = pending;
		const settled = () => {
			if (this.pending === pending) this.pending = undefined;
		};
		void pending.then(settled, settled);
		return pending;
	}

	private async replay<T>(
		fenceEntryCid: string,
		use: (view: ReplayedResourceOperationsV2) => T | Promise<T>,
		deadline: number,
		signal: AbortSignal,
	): Promise<PolicyLeaseResultV2<T>> {
		const stop = ():
			| Exclude<PolicyLeaseResultV2<T>, { status: "completed" }>
			| undefined =>
			this.closed || this.terminalError !== undefined || this.signal?.aborted
				? { status: "halted", reason: "Journal is closed or faulted" }
				: signal.aborted || Date.now() >= deadline
					? { status: "unavailable", reason: "Replay was cancelled or expired" }
					: undefined;
		const initialStop = stop();
		if (initialStop) return initialStop;
		const inventory = [...this.records.keys()].sort();
		const requirement = { fenceEntryCid, deadline, signal };
		const captured = await this.fenceAnchor.withExactResourceFenceHead(
			requirement,
			headsFromLease,
		);
		if (captured.status !== "completed") return captured;
		const entries: ReplayedResourceOperationsV2["entries"][number][] = [];
		const causalWork = new ResourceCausalWorkBudgetV2(this.causalWorkLimits);
		const drift = {
			status: "unavailable" as const,
			reason: "Replay policy or resource head changed",
		};
		for (const entryCid of inventory) {
			const stopped = stop();
			if (stopped) return stopped;
			const classified = await this.engine.withClassifiedOperation(
				this.records.get(entryCid)!,
				(lease) =>
					sameHeads(captured.value, lease)
						? {
								entryCid,
								status: lease.classification.status,
								applicationPayload: lease.classification.applicationPayload,
							}
						: undefined,
				{ deadline, signal, causalWork },
			);
			if (classified.status !== "completed")
				return {
					status: classified.status,
					reason: classified.reason ?? "Replay classification unavailable",
				};
			if (classified.value === undefined) return drift;
			entries.push(classified.value);
		}
		const afterClassification = stop();
		if (afterClassification) return afterClassification;
		const projection: RetainedResourceProjectionV2 = {
			...captured.value,
			inventoryDigest: this.inventoryDigest(),
			entries: entries.map(({ entryCid, status }) => ({ entryCid, status })),
		};
		const final = await this.fenceAnchor.withExactResourceFenceHead(
			requirement,
			async (lease): Promise<PolicyLeaseResultV2<T>> => {
				const stopped = stop();
				if (stopped) return stopped;
				if (!sameHeads(projection, headsFromLease(lease))) return drift;
				// Fresh replay is mandatory, but an identical durable projection does
				// not need another full journal replacement/fsync.
				if (
					!equals(
						this.encodeProjection(this.projection),
						this.encodeProjection(projection),
					)
				) {
					await this.commit(
						this.encode(this.records, this.retainedBytes, projection),
					);
				}
				this.projection = projection;
				// Publication must finish, but cancellation can still prevent a consumer
				// which has not entered. Once entered its actual result/error is retained.
				const beforeUse = stop();
				if (beforeUse) return beforeUse;
				return {
					status: "completed",
					value: await use({ ...copyProjection(projection), entries }),
				};
			},
		);
		return final.status === "completed" ? final.value : final;
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
		this.lifecycle.abort();
		this.engine.abort();
		// Failures remain on their original operation promises. Closing must still
		// wait for every started commit before callers may close the shared store.
		await Promise.allSettled([
			...this.classifications,
			...(this.pending === undefined ? [] : [this.pending]),
		]);
		this.records.clear();
		this.retainedBytes = 0;
		this.projection = undefined;
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
		await this.commit(payload);
		// Cancellation cannot turn a completed durable replacement into a refusal.
		this.records = records;
		this.retainedBytes = retainedBytes;
		this.projection = undefined;
		return { status: "retained", entryCid, duplicate: false };
	}

	private async commit(payload: Uint8Array): Promise<void> {
		try {
			await this.checkpoint.commit(payload);
		} catch (error) {
			this.terminalError = error;
			this.lifecycle.abort();
			this.engine.abort();
			throw error;
		}
	}

	private inventoryDigest(): Uint8Array {
		const cids = [...this.records.keys()]
			.sort()
			.map((cid) => new TextEncoder().encode(cid));
		return sha256Sync(
			concat([
				INVENTORY_DOMAIN,
				u32(cids.length),
				...cids.flatMap((cid) => [u32(cid.byteLength), cid]),
			]),
		);
	}

	private encode(
		records: Map<string, Uint8Array>,
		totalBytes: number,
		projection?: RetainedResourceProjectionV2,
	): Uint8Array {
		const suffix = this.encodeProjection(projection);
		const payload = new Uint8Array(
			HEADER_BYTES + records.size * 4 + totalBytes + suffix.byteLength,
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
		payload.set(suffix, offset);
		return payload;
	}

	private encodeProjection(
		projection?: RetainedResourceProjectionV2,
	): Uint8Array {
		if (projection === undefined) return Uint8Array.of(0);
		const sequence = new Uint8Array(8);
		new DataView(sequence.buffer).setBigUint64(
			0,
			projection.acceptedPolicyHead.sequence,
			true,
		);
		const cid = new TextEncoder().encode(projection.acceptedFenceHead.entryCid);
		return concat([
			Uint8Array.of(1),
			sequence,
			projection.acceptedPolicyHead.digest,
			u32(cid.byteLength),
			cid,
			projection.acceptedFenceHead.digest,
			projection.inventoryDigest,
			Uint8Array.from(
				projection.entries.map(({ status }) =>
					PROJECTION_STATUSES.indexOf(status),
				),
			),
		]);
	}

	private restoreProjection(bytes: Uint8Array): void {
		if (bytes.byteLength === 1 && bytes[0] === 0) return;
		// Fixed framing: present[1], policy[8+32], CID length[4], CID,
		// fence digest[32], inventory digest[32], one status per retained record.
		if (bytes.byteLength < 109 || bytes[0] !== 1)
			throw new Error("Invalid retained projection framing");
		const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
		const cidLength = view.getUint32(41, true);
		const statusOffset = 45 + cidLength + 64;
		if (
			cidLength > TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS ||
			statusOffset + this.records.size !== bytes.byteLength
		)
			throw new Error("Invalid retained projection length");
		const entryCid = captureCanonicalResourceFenceCidV2(
			new TextDecoder("utf-8", { fatal: true }).decode(
				bytes.subarray(45, 45 + cidLength),
			),
		);
		const inventoryDigest = bytes.slice(77 + cidLength, statusOffset);
		if (!equals(inventoryDigest, this.inventoryDigest()))
			throw new Error("Retained projection inventory mismatch");
		this.projection = {
			acceptedPolicyHead: {
				sequence: view.getBigUint64(1, true),
				digest: bytes.slice(9, 41),
			},
			acceptedFenceHead: {
				entryCid,
				digest: bytes.slice(45 + cidLength, 77 + cidLength),
			},
			inventoryDigest,
			entries: this.entries().map((entryCid, i) => {
				const status = PROJECTION_STATUSES[bytes[statusOffset + i]];
				if (status === undefined)
					throw new Error("Invalid retained projection status");
				return { entryCid, status };
			}),
		};
	}

	private async restore(payload: Uint8Array): Promise<void> {
		if (
			payload.byteLength < HEADER_BYTES ||
			!equals(payload.subarray(0, 3), MAGIC.subarray(0, 3)) ||
			(payload[3] !== 1 && payload[3] !== 2)
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
		// Retention-only v1 checkpoints remain readable; all new writes use v2.
		if (payload[3] === 2) this.restoreProjection(payload.subarray(offset));
		else if (offset !== payload.byteLength)
			throw new Error("Trailing resource operation journal bytes");
	}
}
