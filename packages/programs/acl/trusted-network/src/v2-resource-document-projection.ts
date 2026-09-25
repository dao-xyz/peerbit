import {
	deserialize,
	field,
	fixedArray,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import type { CrashSafeAtomicReplaceStore } from "@peerbit/any-store-interface";
import { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";
import { sha256Sync } from "@peerbit/crypto";
import { concat, equals } from "uint8arrays";
import type { PolicyLeaseResultV2 } from "./v2-policy-anchor.js";
import type { ResourceOperationAuthorizationOptionsV2 } from "./v2-resource-operation-engine.js";
import {
	type ReplayedResourceOperationsV2,
	type RetainedResourceOperationResultV2,
	TRUSTED_NETWORK_V2_MAX_RETAINED_RESOURCE_OPERATION_BYTES,
	TrustedNetworkV2ResourceOperationJournal,
	type TrustedNetworkV2ResourceOperationJournalProperties,
} from "./v2-resource-operation-journal.js";
import {
	NetworkDescriptorV2,
	copyUint8ArrayWithLengthV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

/** Internal fixed application profile; each operation CID remains its identity. */
@variant([2, 5])
export class ImmutableResourceDocumentV2 {
	@field({ type: "string" })
	key: string;

	@field({ type: Uint8Array })
	value: Uint8Array;

	constructor(properties: { key: string; value: Uint8Array }) {
		this.key = properties.key;
		this.value = properties.value;
	}
}

const decodeDocument = (bytes: Uint8Array): ImmutableResourceDocumentV2 => {
	// Decode only this bounded Borsh wire profile. Generic schemas can contain
	// vectors whose tiny framing declares unbounded allocation/work.
	if (
		bytes.byteLength < 10 ||
		bytes.byteLength > 10 + 1024 + 60 * 1024 ||
		bytes[0] !== 2 ||
		bytes[1] !== 5
	)
		throw new Error("Invalid document framing");
	const frame = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	const keyLength = frame.getUint32(2, true);
	if (keyLength > 1024 || 10 + keyLength > bytes.byteLength)
		throw new Error("Invalid document key length");
	const valueLength = frame.getUint32(6 + keyLength, true);
	if (
		valueLength > 60 * 1024 ||
		10 + keyLength + valueLength !== bytes.byteLength
	)
		throw new Error("Invalid document value length");
	const key = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
		bytes.subarray(6, 6 + keyLength),
	);
	const value = new ImmutableResourceDocumentV2({
		key,
		value: bytes.slice(10 + keyLength),
	});
	if (!equals(bytes, serialize(value)))
		throw new Error("Noncanonical document");
	return value;
};

// Local cache format only: not a signed snapshot or a public resource protocol.
@variant(0)
class StoredDocumentV2 {
	@field({ type: "string" })
	entryCid: string;

	@field({ type: "u8" })
	status: number;

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(entryCid: string, status: number, data: Uint8Array) {
		this.entryCid = entryCid;
		this.status = status;
		this.data = data;
	}
}

@variant(0)
class StoredDocumentProjectionV2 {
	@field({ type: "u64" })
	policySequence: bigint;

	@field({ type: fixedArray("u8", 32) })
	policyDigest: Uint8Array;

	@field({ type: "string" })
	fenceEntryCid: string;

	@field({ type: fixedArray("u8", 32) })
	fenceDigest: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	inventoryDigest: Uint8Array;

	@field({ type: vec(StoredDocumentV2) })
	documents: StoredDocumentV2[];

	constructor(
		view: ReplayedResourceOperationsV2,
		documents: StoredDocumentV2[],
	) {
		this.policySequence = view.acceptedPolicyHead.sequence;
		this.policyDigest = view.acceptedPolicyHead.digest;
		this.fenceEntryCid = view.acceptedFenceHead.entryCid;
		this.fenceDigest = view.acceptedFenceHead.digest;
		this.inventoryDigest = view.inventoryDigest;
		this.documents = documents;
	}
}

export type ProtectedDocumentViewV2 = Omit<
	ReplayedResourceOperationsV2,
	"entries"
> & {
	readonly documents: readonly Readonly<{
		entryCid: string;
		status: "policy-final" | "provisional";
		value: ImmutableResourceDocumentV2;
	}>[];
};

export type TrustedNetworkV2ResourceDocumentProjectionProperties =
	TrustedNetworkV2ResourceOperationJournalProperties & {
		/** Dedicated, already-open store, separate from the journal and anchors. */
		projectionStore: CrashSafeAtomicReplaceStore;
		/** Stable 32-byte application value interpretation/version identifier. */
		documentProfileId: Uint8Array;
	};

const frame = (bytes: Uint8Array): Uint8Array => {
	const length = new Uint8Array(4);
	new DataView(length.buffer).setUint32(0, bytes.byteLength, true);
	return concat([length, bytes]);
};

/**
 * Internal immutable document-set consumer of retained-operation replay.
 * Each signed operation CID is one independent document identity. CID sorting
 * is set order, never mutable Documents conflict resolution. All retained
 * operations must use the selected canonical document codec.
 *
 * Every read replays the complete local inventory, atomically replaces rows and
 * their watermark, then holds both authorization anchors through the consumer.
 * Recovered cache bytes never authorize a read. No network completeness,
 * confidentiality, mutable put/delete, snapshot import or history truncation.
 */
export class TrustedNetworkV2ResourceDocumentProjection {
	private readonly lifecycle = new AbortController();
	private closed = false;
	private faulted = false;
	private persistedBytes?: Uint8Array;

	private constructor(
		private readonly journal: TrustedNetworkV2ResourceOperationJournal,
		private readonly checkpoint: CrashSafeTwoSlotCheckpoint,
		private readonly signal?: AbortSignal,
	) {
		// Only used to avoid an identical replacement AFTER fresh authenticated
		// replay. Never decode or serve the old projection as current authority.
		this.persistedBytes = checkpoint.current?.payload;
	}

	static async open(
		properties: TrustedNetworkV2ResourceDocumentProjectionProperties,
	): Promise<TrustedNetworkV2ResourceDocumentProjection> {
		const captured = { ...properties };
		if (captured.store === captured.projectionStore)
			throw new Error(
				"Journal and document projection require separate stores",
			);
		if (
			exactUint8ArrayByteLengthV2(captured.expectedResourceId) !== 32 ||
			exactUint8ArrayByteLengthV2(captured.documentProfileId) !== 32
		)
			throw new Error(
				"Resource and document profile ids must contain exactly 32 bytes",
			);
		const descriptor = deserialize(
			serialize(captured.descriptor),
			NetworkDescriptorV2,
		);
		const expectedResourceId = copyUint8ArrayWithLengthV2(
			captured.expectedResourceId,
			32,
		);
		const profile = copyUint8ArrayWithLengthV2(captured.documentProfileId, 32);
		const journal = await TrustedNetworkV2ResourceOperationJournal.open({
			...captured,
			descriptor,
			expectedResourceId,
		});
		try {
			const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
				store: captured.projectionStore,
				scope: sha256Sync(
					concat([
						new TextEncoder().encode(
							"peerbit/trusted-network/v2/immutable-document-projection/v1\0",
						),
						frame(serialize(descriptor)),
						expectedResourceId,
						frame(new TextEncoder().encode(captured.expectedGid)),
						profile,
					]),
				),
				// Original entry bytes bound document bytes; allow framing for at most
				// 256 canonical CIDs/statuses plus the fixed head/inventory watermark.
				maxPayloadBytes:
					(captured.maxRetainedBytes ??
						TRUSTED_NETWORK_V2_MAX_RETAINED_RESOURCE_OPERATION_BYTES) +
					128 * 1024,
			});
			if (captured.signal?.aborted)
				throw new Error("Document projection open aborted");
			return new TrustedNetworkV2ResourceDocumentProjection(
				journal,
				checkpoint,
				captured.signal,
			);
		} catch (error) {
			await journal.close();
			throw error;
		}
	}

	retain(
		bytes: Uint8Array,
		options?: Readonly<{ signal?: AbortSignal }>,
	): Promise<RetainedResourceOperationResultV2> {
		if (this.closed || this.faulted || this.signal?.aborted)
			return Promise.resolve({
				status: "halted",
				reason: "Document projection is closed or faulted",
			});
		return this.journal.retain(bytes, options);
	}

	/** Original signed bytes only; retention is not document acceptance. */
	get(entryCid: string): Uint8Array | undefined {
		return this.journal.get(entryCid);
	}

	entries(): readonly string[] {
		return this.journal.entries();
	}

	/** The consumer must not reenter this resource/its anchors or await close. */
	async withDocuments<R>(
		fenceEntryCid: string,
		use: (view: ProtectedDocumentViewV2) => R | Promise<R>,
		options?: Omit<ResourceOperationAuthorizationOptionsV2, "causalWork">,
	): Promise<PolicyLeaseResultV2<R>> {
		if (this.closed || this.faulted || this.signal?.aborted)
			return {
				status: "halted",
				reason: "Document projection is closed or faulted",
			};
		if (typeof use !== "function")
			return { status: "rejected", reason: "Invalid document consumer" };
		const captured = { ...options };
		const timeout = captured.timeoutMs ?? 10_000;
		if (
			!Number.isSafeInteger(timeout) ||
			timeout < 0 ||
			timeout > 10_000 ||
			(captured.deadline !== undefined &&
				(!Number.isSafeInteger(captured.deadline) || captured.deadline < 0))
		)
			return { status: "rejected", reason: "Invalid document replay deadline" };
		const signal = AbortSignal.any([
			this.lifecycle.signal,
			...(this.signal ? [this.signal] : []),
			...(captured.signal ? [captured.signal] : []),
		]);
		const deadline = Math.min(
			Date.now() + timeout,
			captured.deadline ?? Infinity,
		);
		const result = await this.journal.withReplayedOperations(
			fenceEntryCid,
			async (view): Promise<PolicyLeaseResultV2<R>> => {
				const rows: StoredDocumentV2[] = [];
				const documents: ProtectedDocumentViewV2["documents"][number][] = [];
				try {
					for (const entry of view.entries) {
						if (entry.status === "rejected") continue;
						const data = entry.applicationPayload!;
						const value = decodeDocument(data);
						rows.push(
							new StoredDocumentV2(
								entry.entryCid,
								entry.status === "policy-final" ? 0 : 1,
								data,
							),
						);
						documents.push({
							entryCid: entry.entryCid,
							status: entry.status,
							value,
						});
					}
				} catch {
					return {
						status: "rejected",
						reason:
							"Retained operation has an invalid canonical document payload",
					};
				}
				const payload = serialize(new StoredDocumentProjectionV2(view, rows));
				if (signal.aborted || Date.now() >= deadline)
					return {
						status: "unavailable",
						reason: "Document projection was cancelled or expired",
					};
				if (!this.persistedBytes || !equals(this.persistedBytes, payload)) {
					try {
						await this.checkpoint.commit(payload);
						this.persistedBytes = payload;
					} catch (error) {
						this.faulted = true;
						throw error;
					}
				}
				// Finish started publication even on cancellation, but never enter a
				// reader after its deadline/close. An entered reader keeps its outcome.
				if (signal.aborted || Date.now() >= deadline)
					return {
						status: "unavailable",
						reason: "Document projection was cancelled or expired",
					};
				const { entries: _entries, ...watermark } = view;
				return {
					status: "completed",
					value: await use({ ...watermark, documents }),
				};
			},
			{ ...captured, deadline, signal },
		);
		return result.status === "completed" ? result.value : result;
	}

	/** Drains started journal/projection publication and entered consumers. */
	async close(): Promise<void> {
		this.closed = true;
		this.lifecycle.abort();
		await this.journal.close();
		this.persistedBytes = undefined;
	}
}
