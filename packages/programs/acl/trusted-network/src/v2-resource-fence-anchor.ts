import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import type { CrashSafeAtomicReplaceStore } from "@peerbit/any-store-interface";
import { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";
import { sha256Sync } from "@peerbit/crypto";
import { concat, equals } from "uint8arrays";
import type {
	AcceptedPolicyLeaseV2,
	PolicyLeaseReferenceV2,
	PolicyLeaseResultV2,
	TrustedNetworkV2DurablePolicyReducer,
} from "./v2-policy-anchor.js";
import {
	type AcceptedResourceFencePolicyV2,
	type PreparedResourceFenceCandidateV2,
	type ResourceFenceAdmissionResultV2,
	type ResourceFenceAdmissionStatusV2,
	type ResourceFenceForkEvidenceV2,
	type ResourceFenceHeadProjectionV2,
	type ResourceFencePolicyReferenceV2,
	type ResourceFenceReducerDurableStateV2,
	TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS,
	TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_FENCES,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_MISSING_CIDS,
	TrustedNetworkV2ResourceFenceReducer,
	type TrustedNetworkV2ResourceFenceReducerProperties,
	captureCanonicalResourceFenceCidV2,
} from "./v2-resource-fence-engine.js";
import {
	NetworkDescriptorV2,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES,
	assertNetworkDescriptorV2,
} from "./v2.js";

/**
 * Internal crash-safe resource-fence publication wrapper.
 *
 * This module deliberately remains absent from the package entry point. The
 * supplied store must be an already-open, dedicated store or sublevel, owned
 * by one writer. The generic two-slot checkpoint is the durable publication
 * boundary; a successful log append or `persisted()` call is not substituted
 * for atomic replacement.
 */

export const TRUSTED_NETWORK_V2_RESOURCE_FENCE_ANCHOR_STORE_OWNER =
	"peerbit/trusted-network/v2/resource-fence-anchor/v1";

const RESOURCE_FENCE_REDUCER_DURABLE_FORMAT_VERSION = 1;
const MAX_UNAVAILABLE_REASON_CHARACTERS = 512;
export const TRUSTED_NETWORK_V2_MAX_QUEUED_RESOURCE_FENCE_INPUT_BYTES =
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES * 4;
const DEFAULT_POLICY_LEASE_MAX_STEPS = 1_024;
const MAX_POLICY_LEASE_STEPS = 4_096;
const DEFAULT_POLICY_LEASE_TIMEOUT_MS = 10_000;
const MAX_POLICY_LEASE_TIMEOUT_MS = 60_000;
const DEFAULT_RESOURCE_OPERATION_TIMEOUT_MS = 10_000;
const MAX_RESOURCE_OPERATION_TIMEOUT_MS = 60_000;
const MAX_AUTOMATIC_PENDING_RETRIES =
	TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_FENCES;
const MAX_EXACT_RESOURCE_FENCE_TIMEOUT_MS = 10_000;
const CHECKPOINT_SCOPE_DOMAIN = new TextEncoder().encode(
	"peerbit/trusted-network/v2/resource-fence-anchor/checkpoint/v1\0",
);
const ANCHOR_STATE = Object.freeze({
	ACTIVE: 1,
	UNAVAILABLE: 2,
	FORKED: 3,
} as const);

// Three maximum EntryV0 envelopes cover active parent/head/candidate state or
// the common parent and two canonical fork children. The remaining allowance
// covers the bounded canonical missing-CID set, reason text, and Borsh framing
// without coupling this wrapper to a backend's generic 64 MiB ceiling.
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ANCHOR_CHECKPOINT_PAYLOAD_BYTES =
	64 * 1024;

export type CrashSafeResourceFenceAnchorStoreV2 = CrashSafeAtomicReplaceStore;

/** The policy wrapper is deliberately concrete so every fence uses its lease. */
export type ResourceFencePolicyAnchorV2 = TrustedNetworkV2DurablePolicyReducer;

export type ExactResourceFenceHeadRequirementV2 = {
	fenceEntryCid: string;
	/** Queue-inclusive acquisition timeout; callers may only shorten 10 seconds. */
	timeoutMs?: number;
	deadline?: number;
	signal?: AbortSignal;
};

export type ExactResourceFenceHeadLeaseV2 = AcceptedPolicyLeaseV2 & {
	fence: ResourceFenceHeadProjectionV2;
};

@variant([2, 16, 6])
class ResourceFenceAnchorCheckpointPayloadV2 {
	@field({ type: "u8" })
	state: number;

	@field({ type: Uint8Array })
	acceptedHeadEntryBytes: Uint8Array;

	@field({ type: Uint8Array })
	acceptedParentEntryBytes: Uint8Array;

	@field({ type: Uint8Array })
	comparisonCandidateEntryBytes: Uint8Array;

	@field({ type: vec("string") })
	missingCids: string[];

	@field({ type: Uint8Array })
	missingPredecessorDigest: Uint8Array;

	@field({ type: "string" })
	unavailableReason: string;

	@field({ type: vec(Uint8Array, "u8") })
	forkChildEntryBytes: Uint8Array[];

	constructor(properties?: {
		state: number;
		acceptedHeadEntryBytes: Uint8Array;
		acceptedParentEntryBytes: Uint8Array;
		comparisonCandidateEntryBytes: Uint8Array;
		missingCids: string[];
		missingPredecessorDigest: Uint8Array;
		unavailableReason: string;
		forkChildEntryBytes: Uint8Array[];
	}) {
		if (properties) Object.assign(this, properties);
	}
}

const TYPED_ARRAY_PROTOTYPE = Object.getPrototypeOf(Uint8Array.prototype);
const TYPED_ARRAY_BYTE_LENGTH = Object.getOwnPropertyDescriptor(
	TYPED_ARRAY_PROTOTYPE,
	"byteLength",
)!.get!;
const TYPED_ARRAY_TAG = Object.getOwnPropertyDescriptor(
	TYPED_ARRAY_PROTOTYPE,
	Symbol.toStringTag,
)!.get!;
const ARRAY_BUFFER_IS_VIEW = ArrayBuffer.isView;
const UINT8_ARRAY_SET = Uint8Array.prototype.set;

const exactUint8ArrayByteLength = (input: unknown): number => {
	if (
		!ARRAY_BUFFER_IS_VIEW(input) ||
		TYPED_ARRAY_TAG.call(input) !== "Uint8Array"
	) {
		throw new TypeError("Expected a genuine Uint8Array");
	}
	const byteLength = TYPED_ARRAY_BYTE_LENGTH.call(input) as number;
	if (byteLength === 0) {
		// Distinguish a detached view without consulting caller properties.
		UINT8_ARRAY_SET.call(new Uint8Array(0), input as Uint8Array);
	}
	return byteLength;
};

const copyBytesWithLength = (
	bytes: Uint8Array,
	byteLength: number,
): Uint8Array => {
	const copy = new Uint8Array(byteLength);
	UINT8_ARRAY_SET.call(copy, bytes);
	return copy;
};

const copyBytes = (bytes: Uint8Array): Uint8Array =>
	copyBytesWithLength(bytes, exactUint8ArrayByteLength(bytes));

const hasExactUint8ArrayByteLength = (
	input: unknown,
	expected: number,
): input is Uint8Array => {
	try {
		return exactUint8ArrayByteLength(input) === expected;
	} catch {
		return false;
	}
};

const u32Bytes = (value: number): Uint8Array => {
	const bytes = new Uint8Array(4);
	new DataView(bytes.buffer).setUint32(0, value, false);
	return bytes;
};

const captureResourceId = (resourceId: Uint8Array): Uint8Array => {
	if (!hasExactUint8ArrayByteLength(resourceId, 32)) {
		throw new Error("Resource-fence anchor resource id must contain 32 bytes");
	}
	return copyBytesWithLength(resourceId, 32);
};

const captureGid = (gid: string): string => {
	if (typeof gid !== "string") {
		throw new TypeError("Resource-fence anchor gid must be a string");
	}
	return gid;
};

/**
 * Return a fixed-size scope while committing to every immutable interpretation
 * input. Length-prefixing prevents descriptor/resource/gid boundary ambiguity;
 * the domain embeds the wrapper/checkpoint format version.
 */
const checkpointScope = (
	descriptor: NetworkDescriptorV2,
	resourceId: Uint8Array,
	gid: string,
): Uint8Array => {
	const descriptorBytes = serialize(descriptor);
	const gidBytes = new TextEncoder().encode(gid);
	if (gidBytes.byteLength > 0xffffffff) {
		throw new RangeError("Resource-fence anchor gid exceeds u32 framing");
	}
	return sha256Sync(
		concat([
			CHECKPOINT_SCOPE_DOMAIN,
			u32Bytes(descriptorBytes.byteLength),
			descriptorBytes,
			resourceId,
			u32Bytes(gidBytes.byteLength),
			gidBytes,
		]),
	);
};

const assertOpenNotAborted = (signal: AbortSignal | undefined): void => {
	if (signal?.aborted) {
		throw new Error(
			"TrustedNetwork v2 durable resource-fence open was aborted",
		);
	}
};

const assertEntryBytes = (bytes: Uint8Array, label: string): void => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLength(bytes);
	} catch {
		byteLength = -1;
	}
	if (
		byteLength < 1 ||
		byteLength > TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES
	) {
		throw new Error(
			`${label} must contain 1-${TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES} bytes`,
		);
	}
};

const assertEmptyBytes = (bytes: Uint8Array, label: string): void => {
	if (!hasExactUint8ArrayByteLength(bytes, 0)) {
		throw new Error(`${label} must be empty`);
	}
};

const optionalEntryBytes = (
	bytes: Uint8Array,
	label: string,
): Uint8Array | undefined => {
	const byteLength = exactUint8ArrayByteLength(bytes);
	if (byteLength === 0) return undefined;
	assertEntryBytes(bytes, label);
	return copyBytesWithLength(bytes, byteLength);
};

const assertCanonicalMissingCids = (missingCids: readonly string[]): void => {
	if (missingCids.length > TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_MISSING_CIDS) {
		throw new Error(
			`Unavailable resource-fence state may contain at most ${TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_MISSING_CIDS} missing CIDs`,
		);
	}
	let previous: string | undefined;
	for (const cid of missingCids) {
		if (
			typeof cid !== "string" ||
			cid.length === 0 ||
			cid.length > TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS ||
			(previous !== undefined && previous >= cid)
		) {
			throw new Error(
				"Unavailable resource-fence missing CIDs must be sorted, unique, and bounded",
			);
		}
		previous = cid;
	}
};

const decodeCanonicalCheckpointPayload = (
	input: Uint8Array,
): ResourceFenceAnchorCheckpointPayloadV2 => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLength(input);
	} catch {
		byteLength = -1;
	}
	if (
		byteLength < 1 ||
		byteLength >
			TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ANCHOR_CHECKPOINT_PAYLOAD_BYTES
	) {
		throw new Error(
			"Resource-fence anchor checkpoint payload exceeds its byte ceiling",
		);
	}
	const payload = deserialize(input, ResourceFenceAnchorCheckpointPayloadV2);
	if (!equals(input, serialize(payload))) {
		throw new Error(
			"Resource-fence anchor checkpoint payload is not canonical",
		);
	}
	return payload;
};

const checkpointPayloadFromState = (
	state: Exclude<ResourceFenceReducerDurableStateV2, { state: "EMPTY" }>,
): ResourceFenceAnchorCheckpointPayloadV2 => {
	if (state.state === "ACTIVE") {
		return new ResourceFenceAnchorCheckpointPayloadV2({
			state: ANCHOR_STATE.ACTIVE,
			acceptedHeadEntryBytes: copyBytes(state.acceptedHeadEntryBytes),
			acceptedParentEntryBytes:
				state.acceptedParentEntryBytes === undefined
					? new Uint8Array(0)
					: copyBytes(state.acceptedParentEntryBytes),
			comparisonCandidateEntryBytes: new Uint8Array(0),
			missingCids: [],
			missingPredecessorDigest: new Uint8Array(0),
			unavailableReason: "",
			forkChildEntryBytes: [],
		});
	}
	if (state.state === "UNAVAILABLE") {
		return new ResourceFenceAnchorCheckpointPayloadV2({
			state: ANCHOR_STATE.UNAVAILABLE,
			acceptedHeadEntryBytes: copyBytes(state.acceptedHeadEntryBytes),
			acceptedParentEntryBytes:
				state.acceptedParentEntryBytes === undefined
					? new Uint8Array(0)
					: copyBytes(state.acceptedParentEntryBytes),
			comparisonCandidateEntryBytes: copyBytes(
				state.comparisonCandidateEntryBytes,
			),
			missingCids: [...state.missingCids],
			missingPredecessorDigest:
				state.missingPredecessorDigest === undefined
					? new Uint8Array(0)
					: copyBytes(state.missingPredecessorDigest),
			unavailableReason: state.reason,
			forkChildEntryBytes: [],
		});
	}
	return new ResourceFenceAnchorCheckpointPayloadV2({
		state: ANCHOR_STATE.FORKED,
		acceptedHeadEntryBytes:
			state.commonParentEntryBytes === undefined
				? new Uint8Array(0)
				: copyBytes(state.commonParentEntryBytes),
		acceptedParentEntryBytes: new Uint8Array(0),
		comparisonCandidateEntryBytes: new Uint8Array(0),
		missingCids: [],
		missingPredecessorDigest: new Uint8Array(0),
		unavailableReason: "",
		forkChildEntryBytes: state.childEntryBytes.map(copyBytes),
	});
};

const durableStateFromCheckpointPayload = (
	payload: ResourceFenceAnchorCheckpointPayloadV2,
): Exclude<ResourceFenceReducerDurableStateV2, { state: "EMPTY" }> => {
	if (payload.state === ANCHOR_STATE.ACTIVE) {
		assertEntryBytes(payload.acceptedHeadEntryBytes, "accepted fence head");
		const acceptedParentEntryBytes = optionalEntryBytes(
			payload.acceptedParentEntryBytes,
			"accepted fence parent",
		);
		assertEmptyBytes(
			payload.comparisonCandidateEntryBytes,
			"active comparison candidate",
		);
		if (
			payload.missingCids.length !== 0 ||
			payload.missingPredecessorDigest.byteLength !== 0 ||
			payload.unavailableReason !== "" ||
			payload.forkChildEntryBytes.length !== 0
		) {
			throw new Error("Active resource-fence checkpoint has unrelated state");
		}
		return {
			formatVersion: RESOURCE_FENCE_REDUCER_DURABLE_FORMAT_VERSION,
			state: "ACTIVE",
			acceptedHeadEntryBytes: copyBytes(payload.acceptedHeadEntryBytes),
			...(acceptedParentEntryBytes === undefined
				? {}
				: { acceptedParentEntryBytes }),
		};
	}
	if (payload.state === ANCHOR_STATE.UNAVAILABLE) {
		assertEntryBytes(payload.acceptedHeadEntryBytes, "accepted fence head");
		const acceptedParentEntryBytes = optionalEntryBytes(
			payload.acceptedParentEntryBytes,
			"accepted fence parent",
		);
		assertEntryBytes(
			payload.comparisonCandidateEntryBytes,
			"unavailable comparison candidate",
		);
		assertCanonicalMissingCids(payload.missingCids);
		const missingPredecessorDigest = optionalEntryBytes(
			payload.missingPredecessorDigest,
			"unavailable missing predecessor digest",
		);
		if (
			missingPredecessorDigest !== undefined &&
			missingPredecessorDigest.byteLength !== 32
		) {
			throw new Error(
				"Unavailable resource-fence predecessor digest must contain 32 bytes",
			);
		}
		if (
			payload.unavailableReason.length === 0 ||
			payload.unavailableReason.length > MAX_UNAVAILABLE_REASON_CHARACTERS
		) {
			throw new Error(
				"Unavailable resource-fence reason is empty or exceeds its ceiling",
			);
		}
		if (payload.forkChildEntryBytes.length !== 0) {
			throw new Error(
				"Unavailable resource-fence checkpoint must not contain fork children",
			);
		}
		return {
			formatVersion: RESOURCE_FENCE_REDUCER_DURABLE_FORMAT_VERSION,
			state: "UNAVAILABLE",
			acceptedHeadEntryBytes: copyBytes(payload.acceptedHeadEntryBytes),
			...(acceptedParentEntryBytes === undefined
				? {}
				: { acceptedParentEntryBytes }),
			comparisonCandidateEntryBytes: copyBytes(
				payload.comparisonCandidateEntryBytes,
			),
			missingCids: [...payload.missingCids],
			...(missingPredecessorDigest === undefined
				? {}
				: { missingPredecessorDigest }),
			reason: payload.unavailableReason,
		};
	}
	if (payload.state !== ANCHOR_STATE.FORKED) {
		throw new Error("Unknown durable resource-fence anchor state");
	}
	const commonParentEntryBytes = optionalEntryBytes(
		payload.acceptedHeadEntryBytes,
		"resource-fence fork common parent",
	);
	assertEmptyBytes(payload.acceptedParentEntryBytes, "forked accepted parent");
	assertEmptyBytes(
		payload.comparisonCandidateEntryBytes,
		"forked comparison candidate",
	);
	if (
		payload.missingCids.length !== 0 ||
		payload.missingPredecessorDigest.byteLength !== 0 ||
		payload.unavailableReason !== "" ||
		payload.forkChildEntryBytes.length !== 2
	) {
		throw new Error(
			"Forked resource-fence checkpoint must contain exactly two children",
		);
	}
	for (const bytes of payload.forkChildEntryBytes) {
		assertEntryBytes(bytes, "resource-fence fork child");
	}
	return {
		formatVersion: RESOURCE_FENCE_REDUCER_DURABLE_FORMAT_VERSION,
		state: "FORKED",
		...(commonParentEntryBytes === undefined ? {} : { commonParentEntryBytes }),
		childEntryBytes: [
			copyBytes(payload.forkChildEntryBytes[0]!),
			copyBytes(payload.forkChildEntryBytes[1]!),
		],
	};
};

const coreIdentityBytesFromState = (
	state: Exclude<ResourceFenceReducerDurableStateV2, { state: "EMPTY" }>,
): Uint8Array => serialize(checkpointPayloadFromState(state));

const copyPolicyReference = (
	reference: ResourceFencePolicyReferenceV2,
): ResourceFencePolicyReferenceV2 => ({
	sequence: reference.sequence,
	digest: copyBytes(reference.digest),
});

const copyHead = (
	head: ResourceFenceHeadProjectionV2 | undefined,
): ResourceFenceHeadProjectionV2 | undefined =>
	head === undefined
		? undefined
		: {
				sequence: head.sequence,
				digest: copyBytes(head.digest),
				entryCid: head.entryCid,
				previousFenceDigest: copyBytes(head.previousFenceDigest),
				policy: copyPolicyReference(head.policy),
				contentEpoch: head.contentEpoch,
				epochManifestDigest: copyBytes(head.epochManifestDigest),
				causalFrontier: head.causalFrontier.map((parent) => ({
					cid: parent.cid,
					digest: copyBytes(parent.digest),
				})),
			};

const copyForkEvidence = (
	evidence: ResourceFenceForkEvidenceV2 | undefined,
): ResourceFenceForkEvidenceV2 | undefined => {
	if (evidence === undefined) return undefined;
	const commonParent =
		evidence.commonParent.kind === "initial"
			? {
					kind: "initial" as const,
					digest: copyBytes(evidence.commonParent.digest),
				}
			: {
					kind: "fence" as const,
					head: copyHead(evidence.commonParent.head)!,
				};
	return {
		commonParent,
		children: [
			{
				sequence: evidence.children[0].sequence,
				digest: copyBytes(evidence.children[0].digest),
				entryCid: evidence.children[0].entryCid,
				entryBytes: copyBytes(evidence.children[0].entryBytes),
			},
			{
				sequence: evidence.children[1].sequence,
				digest: copyBytes(evidence.children[1].digest),
				entryCid: evidence.children[1].entryCid,
				entryBytes: copyBytes(evidence.children[1].entryBytes),
			},
		],
	};
};

type PublishedResourceFenceProjectionV2 = {
	state: "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED";
	head?: ResourceFenceHeadProjectionV2;
	forkEvidence?: ResourceFenceForkEvidenceV2;
};

const publishedFromReducer = (
	core: TrustedNetworkV2ResourceFenceReducer,
): PublishedResourceFenceProjectionV2 => {
	if (core.state === "HALTED") {
		throw new Error("Cannot publish a halted resource-fence reducer");
	}
	return {
		state: core.state,
		head: copyHead(core.head),
		forkEvidence: copyForkEvidence(core.forkEvidence),
	};
};

const assertCanonicalCoreRestore = (
	core: TrustedNetworkV2ResourceFenceReducer,
	expected: Exclude<ResourceFenceReducerDurableStateV2, { state: "EMPTY" }>,
): void => {
	const restored = core.exportDurableState();
	if (restored.state === "EMPTY" || restored.state !== expected.state) {
		throw new Error(
			"Restored resource-fence anchor state is unexpectedly empty",
		);
	}
	if (
		!equals(
			coreIdentityBytesFromState(restored),
			coreIdentityBytesFromState(expected),
		)
	) {
		throw new Error("Restored resource-fence anchor state is not canonical");
	}
};

type ResourceFenceCoreOptions = Omit<
	TrustedNetworkV2ResourceFenceReducerProperties,
	"descriptor" | "expectedResourceId" | "expectedGid" | "signal"
>;

export type TrustedNetworkV2DurableResourceFenceReducerOptions =
	ResourceFenceCoreOptions & {
		descriptor: NetworkDescriptorV2;
		expectedResourceId: Uint8Array;
		expectedGid: string;
		policyAnchor: ResourceFencePolicyAnchorV2;
		store: CrashSafeResourceFenceAnchorStoreV2;
		policyLeaseMaxSteps?: number;
		policyLeaseTimeoutMs?: number;
		signal?: AbortSignal;
	};

type PreparedAdmissionV2 = {
	candidate: PreparedResourceFenceCandidateV2;
};

type ResourceFenceOperationBudgetV2 = {
	signal: AbortSignal;
	deadline: number;
	dispose: () => void;
};

type LeasedAdmissionV2 = {
	result: ResourceFenceAdmissionResultV2;
	leaseCompleted: boolean;
};

const acceptedPolicyFromLease = (policy: {
	sequence: bigint;
	digest: Uint8Array;
}): AcceptedResourceFencePolicyV2 => ({
	sequence: policy.sequence,
	digest: copyBytes(policy.digest),
});

const policyFailureStatus = (
	status: "unavailable" | "rejected" | "capacity" | "halted",
): ResourceFenceAdmissionStatusV2 =>
	status === "unavailable" ? "unavailable" : status;

/**
 * Crash-safe publication wrapper for one internal resource-fence reducer.
 *
 * `prepare()` performs bounded immutable authentication without mutating the
 * reducer. Every mutation then acquires the referenced accepted-policy lease
 * first and holds it through resource serialization and checkpoint commit.
 * Getters expose only the last post-commit projection. This internal anchor is
 * not, by itself, a revocation-safety or authorization gate: a consumer must
 * also complete protected-log replay, prove that no input remains unresolved,
 * and revalidate each protected operation against the resulting fence.
 */
export class TrustedNetworkV2DurableResourceFenceReducer {
	private checkpoint?: CrashSafeTwoSlotCheckpoint;
	private readonly core: TrustedNetworkV2ResourceFenceReducer;
	private readonly policyAnchor: ResourceFencePolicyAnchorV2;
	private readonly lifecycleController: AbortController;
	private readonly policyLeaseMaxSteps: number;
	private readonly policyLeaseTimeoutMs: number;
	private readonly operationTimeoutMs: number;
	private readonly resolveEntryV0: ResourceFenceCoreOptions["resolveEntryV0"];
	private externalSignal?: AbortSignal;
	private externalAbortListener?: () => void;
	private published: PublishedResourceFenceProjectionV2;
	private durableCoreIdentityBytes?: Uint8Array;
	private operationTail: Promise<void> = Promise.resolve();
	private exactPreparationTail: Promise<void> = Promise.resolve();
	private publicationFences = 0;
	private recoveryFences = 0;
	private bufferedOperations = 0;
	private bufferedOperationInputBytes = 0;
	private terminalError?: Error;

	private constructor(properties: {
		checkpoint?: CrashSafeTwoSlotCheckpoint;
		core: TrustedNetworkV2ResourceFenceReducer;
		policyAnchor: ResourceFencePolicyAnchorV2;
		lifecycleController: AbortController;
		policyLeaseMaxSteps: number;
		policyLeaseTimeoutMs: number;
		operationTimeoutMs: number;
		resolveEntryV0: ResourceFenceCoreOptions["resolveEntryV0"];
		externalSignal?: AbortSignal;
		externalAbortListener?: () => void;
		durableCoreIdentityBytes?: Uint8Array;
	}) {
		this.checkpoint = properties.checkpoint;
		this.core = properties.core;
		this.policyAnchor = properties.policyAnchor;
		this.lifecycleController = properties.lifecycleController;
		this.policyLeaseMaxSteps = properties.policyLeaseMaxSteps;
		this.policyLeaseTimeoutMs = properties.policyLeaseTimeoutMs;
		this.operationTimeoutMs = properties.operationTimeoutMs;
		this.resolveEntryV0 = properties.resolveEntryV0;
		this.externalSignal = properties.externalSignal;
		this.externalAbortListener = properties.externalAbortListener;
		this.published = publishedFromReducer(this.core);
		this.durableCoreIdentityBytes =
			properties.durableCoreIdentityBytes === undefined
				? undefined
				: copyBytes(properties.durableCoreIdentityBytes);
	}

	static async open(
		options: TrustedNetworkV2DurableResourceFenceReducerOptions,
	): Promise<TrustedNetworkV2DurableResourceFenceReducer> {
		const descriptorInput = options.descriptor;
		const resourceIdInput = options.expectedResourceId;
		const gidInput = options.expectedGid;
		const policyAnchor = options.policyAnchor;
		const store = options.store;
		const externalSignal = options.signal;
		const policyLeaseMaxSteps =
			options.policyLeaseMaxSteps ?? DEFAULT_POLICY_LEASE_MAX_STEPS;
		const policyLeaseTimeoutMs =
			options.policyLeaseTimeoutMs ?? DEFAULT_POLICY_LEASE_TIMEOUT_MS;
		const operationTimeoutMs =
			options.operationTimeoutMs ?? DEFAULT_RESOURCE_OPERATION_TIMEOUT_MS;
		if (
			!Number.isSafeInteger(policyLeaseMaxSteps) ||
			policyLeaseMaxSteps < 0 ||
			policyLeaseMaxSteps > MAX_POLICY_LEASE_STEPS
		) {
			throw new RangeError(
				`policyLeaseMaxSteps must be between 0 and ${MAX_POLICY_LEASE_STEPS}`,
			);
		}
		if (
			!Number.isSafeInteger(policyLeaseTimeoutMs) ||
			policyLeaseTimeoutMs < 1 ||
			policyLeaseTimeoutMs > MAX_POLICY_LEASE_TIMEOUT_MS
		) {
			throw new RangeError(
				`policyLeaseTimeoutMs must be between 1 and ${MAX_POLICY_LEASE_TIMEOUT_MS}`,
			);
		}
		if (
			!Number.isSafeInteger(operationTimeoutMs) ||
			operationTimeoutMs < 1 ||
			operationTimeoutMs > MAX_RESOURCE_OPERATION_TIMEOUT_MS
		) {
			throw new RangeError(
				`operationTimeoutMs must be between 1 and ${MAX_RESOURCE_OPERATION_TIMEOUT_MS}`,
			);
		}
		assertNetworkDescriptorV2(descriptorInput);
		const descriptor = deserialize(
			serialize(descriptorInput),
			NetworkDescriptorV2,
		);
		const expectedResourceId = captureResourceId(resourceIdInput);
		const expectedGid = captureGid(gidInput);
		if (
			policyAnchor === null ||
			typeof policyAnchor !== "object" ||
			typeof policyAnchor.withAcceptedPolicyLease !== "function" ||
			typeof policyAnchor.isUsable !== "function"
		) {
			throw new TypeError(
				"Resource-fence anchor requires a durable policy lease provider",
			);
		}
		const lifecycleController = new AbortController();
		let externalAbortListener: (() => void) | undefined;
		if (externalSignal?.aborted) {
			lifecycleController.abort();
		} else if (externalSignal !== undefined) {
			externalAbortListener = (): void => lifecycleController.abort();
			externalSignal.addEventListener("abort", externalAbortListener, {
				once: true,
			});
		}
		const signal = lifecycleController.signal;
		let core: TrustedNetworkV2ResourceFenceReducer | undefined;
		try {
			const coreProperties: TrustedNetworkV2ResourceFenceReducerProperties = {
				descriptor,
				expectedResourceId,
				expectedGid,
				resolveFenceEntry: options.resolveFenceEntry,
				resolveEntryV0: options.resolveEntryV0,
				causalLimits: options.causalLimits,
				causalTimeoutMs: options.causalTimeoutMs,
				operationTimeoutMs,
				maxPending: options.maxPending,
				maxPendingBytes: options.maxPendingBytes,
				maxFenceAncestrySteps: options.maxFenceAncestrySteps,
				signal,
			};
			assertOpenNotAborted(signal);
			const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
				store,
				scope: checkpointScope(descriptor, expectedResourceId, expectedGid),
				maxPayloadBytes:
					TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ANCHOR_CHECKPOINT_PAYLOAD_BYTES,
			});
			assertOpenNotAborted(signal);

			// No append-only resource-anchor format was ever activated. Refuse any
			// record in its reserved namespace instead of inventing a fallback that can
			// silently lower the durable high-water mark.
			for await (const [key] of store.iterator()) {
				assertOpenNotAborted(signal);
				if (
					key === TRUSTED_NETWORK_V2_RESOURCE_FENCE_ANCHOR_STORE_OWNER ||
					key.startsWith(
						`${TRUSTED_NETWORK_V2_RESOURCE_FENCE_ANCHOR_STORE_OWNER}/`,
					)
				) {
					throw new Error(
						"Legacy resource-fence anchor records are not supported; reset the dedicated store",
					);
				}
			}
			assertOpenNotAborted(signal);

			const current = checkpoint.current;
			if (current === undefined) {
				core = new TrustedNetworkV2ResourceFenceReducer(coreProperties);
				return new TrustedNetworkV2DurableResourceFenceReducer({
					checkpoint,
					core,
					policyAnchor,
					lifecycleController,
					policyLeaseMaxSteps,
					policyLeaseTimeoutMs,
					operationTimeoutMs,
					resolveEntryV0: coreProperties.resolveEntryV0,
					externalSignal,
					externalAbortListener,
				});
			}

			const payload = decodeCanonicalCheckpointPayload(current.payload);
			const durableState = durableStateFromCheckpointPayload(payload);
			core = await TrustedNetworkV2ResourceFenceReducer.restore({
				...coreProperties,
				durableState,
				isAcceptedPolicy: async (reference) => {
					const leased = await policyAnchor.withAcceptedPolicyLease(
						{
							...reference,
							maxSteps: policyLeaseMaxSteps,
							timeoutMs: policyLeaseTimeoutMs,
							signal,
						},
						({ policy }) =>
							policy.sequence === reference.sequence &&
							equals(policy.digest, reference.digest),
					);
					return leased.status === "completed" && leased.value;
				},
			});
			assertOpenNotAborted(signal);
			assertCanonicalCoreRestore(core, durableState);
			const forked = durableState.state === "FORKED";
			return new TrustedNetworkV2DurableResourceFenceReducer({
				checkpoint: forked ? undefined : checkpoint,
				core,
				policyAnchor,
				lifecycleController,
				policyLeaseMaxSteps,
				policyLeaseTimeoutMs,
				operationTimeoutMs,
				resolveEntryV0: coreProperties.resolveEntryV0,
				externalSignal,
				externalAbortListener,
				durableCoreIdentityBytes: coreIdentityBytesFromState(durableState),
			});
		} catch (error) {
			core?.abort();
			lifecycleController.abort();
			if (externalSignal !== undefined && externalAbortListener !== undefined) {
				externalSignal.removeEventListener("abort", externalAbortListener);
			}
			throw error;
		}
	}

	get state(): "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED" | "HALTED" {
		if (this.terminalError !== undefined) return "HALTED";
		if (this.published.state === "FORKED") return "FORKED";
		if (this.core.state === "HALTED") return "HALTED";
		return this.published.state;
	}

	get head(): ResourceFenceHeadProjectionV2 | undefined {
		return copyHead(this.published.head);
	}

	get forkEvidence(): ResourceFenceForkEvidenceV2 | undefined {
		return copyForkEvidence(this.published.forkEvidence);
	}

	get pendingCount(): number {
		return this.published.state === "FORKED" ? 0 : this.core.pendingCount;
	}

	get pendingBytes(): number {
		return this.published.state === "FORKED" ? 0 : this.core.pendingBytes;
	}

	get pendingPolicyReferences(): ReadonlyArray<
		Readonly<{
			entryCid: string;
			policyReference: ResourceFencePolicyReferenceV2;
		}>
	> {
		return this.published.state === "FORKED"
			? []
			: this.core.pendingPolicyReferences.map(
					({ entryCid, policyReference }) => ({
						entryCid,
						policyReference: copyPolicyReference(policyReference),
					}),
				);
	}

	/** Internal diagnostics for the fixed outer operation-queue bound. */
	get bufferedAdmissionCount(): number {
		return this.bufferedOperations;
	}

	/** Exact captured input bytes retained by unsettled outer operations. */
	get bufferedAdmissionBytes(): number {
		return this.bufferedOperationInputBytes;
	}

	/**
	 * Non-security diagnostic: the local committed projection is quiescent.
	 * This does not prove complete-log replay or authorize a protected operation.
	 */
	isCommittedHeadStable(): boolean {
		return (
			this.publicationFences === 0 &&
			this.recoveryFences === 0 &&
			this.pendingCount === 0 &&
			this.state === "ACTIVE" &&
			this.policyAnchor.isUsable()
		);
	}

	abort(): void {
		this.detachExternalAbortListener();
		this.lifecycleController.abort();
		this.core.abort();
	}

	/**
	 * Authenticate and durably admit one exact fence, then hold policy followed
	 * by resource serialization through the callback. This proves the named local
	 * head only, not remote freshness, complete resource replay, or authorization.
	 * Callbacks must not await another operation on either anchor. Cancellation
	 * after callback entry does not retract the lease or release its queue slots.
	 */
	withExactResourceFenceHead<T>(
		requirement: ExactResourceFenceHeadRequirementV2,
		use: (lease: ExactResourceFenceHeadLeaseV2) => T | Promise<T>,
	): Promise<PolicyLeaseResultV2<T>> {
		const halted = (): PolicyLeaseResultV2<T> | undefined =>
			this.terminalError !== undefined ||
			this.lifecycleController.signal.aborted ||
			this.state === "FORKED"
				? { status: "halted", reason: "Resource-fence anchor is halted" }
				: undefined;
		const terminal = halted();
		if (terminal) return Promise.resolve(terminal);
		let cid: string;
		let deadline: number;
		let interruptionSignal: AbortSignal;
		try {
			cid = captureCanonicalResourceFenceCidV2(requirement.fenceEntryCid);
			const timeout =
				requirement.timeoutMs ?? MAX_EXACT_RESOURCE_FENCE_TIMEOUT_MS;
			const suppliedDeadline = requirement.deadline;
			if (
				!Number.isSafeInteger(timeout) ||
				timeout < 0 ||
				timeout > MAX_EXACT_RESOURCE_FENCE_TIMEOUT_MS ||
				(suppliedDeadline !== undefined &&
					(!Number.isSafeInteger(suppliedDeadline) || suppliedDeadline < 0)) ||
				typeof use !== "function"
			)
				throw new Error("Invalid exact fence requirement");
			deadline = Math.min(Date.now() + timeout, suppliedDeadline ?? Infinity);
			const signal = requirement.signal;
			interruptionSignal = AbortSignal.any(
				signal === undefined
					? [this.lifecycleController.signal]
					: [this.lifecycleController.signal, signal],
			);
		} catch {
			return Promise.resolve({
				status: "rejected",
				reason: "Exact resource-fence requirement is invalid",
			});
		}

		// Reserve the full bounded response before starting a resolver. Retain the
		// reservation until actual work settles even if cancellation settles outward.
		const inputBytes =
			TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES + cid.length;
		if (!this.reserveOperation(inputBytes)) {
			return Promise.resolve({
				status: "capacity",
				reason: "Exact resource-fence queue is at its fixed capacity",
			});
		}
		const controller = new AbortController();
		let callbackEntered = false;
		let interrupted: PolicyLeaseResultV2<T> | undefined;
		let resolveEarly!: (result: PolicyLeaseResultV2<T>) => void;
		const early = new Promise<PolicyLeaseResultV2<T>>((resolve) => {
			resolveEarly = resolve;
		});
		let timer: ReturnType<typeof setTimeout> | undefined;
		const dispose = () => {
			clearTimeout(timer);
			interruptionSignal.removeEventListener("abort", interrupt);
		};
		const interrupt = () => {
			if (callbackEntered || interrupted) return;
			interrupted = halted() ?? {
				status: "unavailable",
				reason: "Exact resource-fence acquisition was cancelled or expired",
			};
			controller.abort();
			dispose();
			resolveEarly(interrupted);
		};
		const stop = (): PolicyLeaseResultV2<T> | undefined => {
			if (
				!callbackEntered &&
				(interruptionSignal.aborted || Date.now() >= deadline)
			)
				interrupt();
			return interrupted ?? halted();
		};
		interruptionSignal.addEventListener("abort", interrupt, { once: true });
		if (interruptionSignal.aborted || Date.now() >= deadline) interrupt();
		else timer = setTimeout(interrupt, deadline - Date.now());

		// One preparation lane bounds ignored cancellation in resolver/signature
		// work. Expired queued closures skip resolution when they reach the lane.
		const preparation = this.exactPreparationTail.then(async () => {
			const stopped = stop();
			if (stopped) return stopped;
			let bytes: Uint8Array | undefined;
			try {
				const resolved = await this.resolveEntryV0([cid], {
					signal: controller.signal,
				});
				if (stop()) return stop()!;
				bytes = resolved.get(cid);
			} catch {
				return (
					stop() ?? {
						status: "unavailable" as const,
						reason: "Exact resource-fence CID resolution failed",
					}
				);
			}
			if (bytes === undefined)
				return {
					status: "unavailable" as const,
					reason: "Exact resource-fence entry is missing",
				};
			const prepared = await this.core.prepare(bytes);
			if (stop()) return stop()!;
			if (prepared.status !== "prepared") return prepared;
			if (prepared.candidate.entryCid !== cid) {
				return {
					status: "rejected" as const,
					reason: "Resolved resource-fence bytes do not match the exact CID",
				};
			}
			return prepared;
		});
		this.exactPreparationTail = preparation.then(
			() => {},
			() => {},
		);
		this.recoveryFences += 1;
		const completed = preparation
			.then(async (prepared): Promise<PolicyLeaseResultV2<T>> => {
				if (prepared.status !== "prepared") return prepared;
				const stopped = stop();
				if (stopped) return stopped;
				const budget = { signal: controller.signal, deadline, dispose };
				const leased = await this.policyAnchor.withAcceptedPolicyLease(
					this.policyLeaseReference(prepared.candidate.policyReference, budget),
					(policyLease) =>
						this.enqueueResourceOperation(
							async (): Promise<PolicyLeaseResultV2<T>> => {
								const queuedStop = stop();
								if (queuedStop) return queuedStop;
								let admission: ResourceFenceAdmissionResultV2;
								try {
									admission = await this.core.ingestPrepared(
										prepared.candidate,
										acceptedPolicyFromLease(policyLease.policy),
										{
											signal: controller.signal,
											cancelBeforePublication: true,
										},
									);
									// After evaluation may have changed core state, replacement must
									// settle under both leases even if the caller already cancelled.
									await this.persistCorePublication(admission);
								} catch (error) {
									throw this.halt(error);
								}
								const committedStop = stop();
								if (committedStop) return committedStop;
								if (
									admission.status !== "accepted" &&
									admission.status !== "duplicate"
								) {
									return {
										status:
											admission.status === "rejected" ||
											admission.status === "capacity"
												? admission.status
												: "unavailable",
										reason:
											admission.reason ??
											"Exact resource-fence admission is unavailable",
									};
								}
								if (
									this.state !== "ACTIVE" ||
									this.published.head?.entryCid !== cid
								) {
									return {
										status: "unavailable",
										reason:
											"Exact resource fence is not the durable current head",
									};
								}
								const fence = copyHead(this.published.head)!;
								// No await between the final lifecycle check and user code.
								callbackEntered = true;
								dispose();
								return {
									status: "completed",
									value: await use({ ...policyLease, fence }),
								};
							},
							false,
						),
				);
				return leased.status === "completed"
					? leased.value
					: (stop() ?? leased);
			})
			.finally(() => {
				dispose();
				this.recoveryFences -= 1;
				this.releaseOperation(inputBytes);
			});
		return Promise.race([completed, early]);
	}

	async ingest(
		entryBytes: Uint8Array,
	): Promise<ResourceFenceAdmissionResultV2> {
		if (this.terminalError !== undefined) throw this.terminalError;
		if (this.published.state === "FORKED") return this.forkFailStopResult();
		if (this.core.state === "HALTED") return this.lifecycleHaltedResult();

		let inputBytes: number;
		try {
			inputBytes = exactUint8ArrayByteLength(entryBytes);
		} catch {
			return this.immediateResult(
				"rejected",
				"Resource fence entry must be a Uint8Array",
			);
		}
		if (
			inputBytes < 1 ||
			inputBytes > TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES
		) {
			return this.immediateResult(
				"rejected",
				`Resource fence entry must contain 1-${TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES} bytes`,
			);
		}
		if (!this.reserveOperation(inputBytes)) {
			return this.immediateResult(
				"capacity",
				"Durable resource-fence operation queue is at its fixed capacity",
			);
		}

		let captured: Uint8Array;
		try {
			captured = copyBytesWithLength(entryBytes, inputBytes);
		} catch (error) {
			this.releaseOperation(inputBytes);
			throw error;
		}

		const budget = this.beginOperationBudget();
		this.recoveryFences += 1;
		try {
			const prepared = await this.core.prepare(captured);
			if (prepared.status !== "prepared") {
				return this.immediateResult(prepared.status, prepared.reason);
			}
			const admission = await this.withCandidatePolicyLease(
				{ candidate: prepared.candidate },
				budget,
			);
			return await this.drainPendingAfterAcceptance(admission, budget);
		} finally {
			budget.dispose();
			this.recoveryFences -= 1;
			this.releaseOperation(inputBytes);
		}
	}

	async retryPending(
		entryCid: string,
	): Promise<ResourceFenceAdmissionResultV2> {
		if (this.terminalError !== undefined) throw this.terminalError;
		if (this.published.state === "FORKED") return this.forkFailStopResult();
		if (this.core.state === "HALTED") return this.lifecycleHaltedResult();
		if (
			typeof entryCid !== "string" ||
			entryCid.length === 0 ||
			entryCid.length > TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS
		) {
			return this.immediateResult(
				"rejected",
				"Pending resource-fence CID is invalid",
			);
		}
		const pending = this.core.pendingPolicyReferences.find(
			(candidate) => candidate.entryCid === entryCid,
		);
		if (pending === undefined) {
			return this.immediateResult("rejected", "Resource fence is not pending");
		}
		const inputBytes = new TextEncoder().encode(entryCid).byteLength;
		if (!this.reserveOperation(inputBytes)) {
			return this.immediateResult(
				"capacity",
				"Durable resource-fence operation queue is at its fixed capacity",
			);
		}
		const budget = this.beginOperationBudget();
		this.recoveryFences += 1;
		try {
			const admission = await this.retryPendingUnderPolicyLease(
				entryCid,
				pending.policyReference,
				budget,
			);
			return await this.drainPendingAfterAcceptance(admission, budget);
		} finally {
			budget.dispose();
			this.recoveryFences -= 1;
			this.releaseOperation(inputBytes);
		}
	}

	private async withCandidatePolicyLease(
		prepared: PreparedAdmissionV2,
		budget: ResourceFenceOperationBudgetV2,
	): Promise<LeasedAdmissionV2> {
		const leased = await this.policyAnchor.withAcceptedPolicyLease(
			this.policyLeaseReference(prepared.candidate.policyReference, budget),
			({ policy }) =>
				this.enqueueResourceOperation(async () => {
					if (this.published.state === "FORKED") {
						return this.forkFailStopResult();
					}
					if (this.core.state === "HALTED") {
						return this.lifecycleHaltedResult();
					}
					const result = await this.core.ingestPrepared(
						prepared.candidate,
						acceptedPolicyFromLease(policy),
						{ signal: budget.signal },
					);
					await this.persistCorePublication(result);
					return result;
				}),
		);
		if (leased.status === "completed") {
			return { result: leased.value, leaseCompleted: true };
		}
		if (this.lifecycleController.signal.aborted) {
			return {
				result: this.lifecycleHaltedResult(),
				leaseCompleted: false,
			};
		}
		return {
			result: this.immediateResult(
				policyFailureStatus(leased.status),
				leased.reason,
			),
			leaseCompleted: false,
		};
	}

	private policyLeaseReference(
		reference: ResourceFencePolicyReferenceV2,
		budget: ResourceFenceOperationBudgetV2,
	): PolicyLeaseReferenceV2 {
		return {
			sequence: reference.sequence,
			digest: copyBytes(reference.digest),
			maxSteps: this.policyLeaseMaxSteps,
			timeoutMs: this.policyLeaseTimeoutMs,
			deadline: budget.deadline,
			signal: budget.signal,
		};
	}

	private async retryPendingUnderPolicyLease(
		entryCid: string,
		reference: ResourceFencePolicyReferenceV2,
		budget: ResourceFenceOperationBudgetV2,
	): Promise<LeasedAdmissionV2> {
		const leased = await this.policyAnchor.withAcceptedPolicyLease(
			this.policyLeaseReference(reference, budget),
			({ policy }) =>
				this.enqueueResourceOperation(async () => {
					if (this.published.state === "FORKED") {
						return this.forkFailStopResult();
					}
					if (this.core.state === "HALTED") {
						return this.lifecycleHaltedResult();
					}
					const result = await this.core.retryPending(
						entryCid,
						acceptedPolicyFromLease(policy),
						{ signal: budget.signal },
					);
					await this.persistCorePublication(result);
					return result;
				}),
		);
		if (leased.status === "completed") {
			return { result: leased.value, leaseCompleted: true };
		}
		if (this.lifecycleController.signal.aborted) {
			return {
				result: this.lifecycleHaltedResult(),
				leaseCompleted: false,
			};
		}
		return {
			result: this.immediateResult(
				policyFailureStatus(leased.status),
				leased.reason,
			),
			leaseCompleted: false,
		};
	}

	/**
	 * Retry newly actionable candidates only after the lease that accepted the
	 * predecessor has been released. The CID/head pair prevents blind retries;
	 * the fixed attempt ceiling and one shared deadline bound the whole drain.
	 */
	private async drainPendingAfterAcceptance(
		initial: LeasedAdmissionV2,
		budget: ResourceFenceOperationBudgetV2,
	): Promise<ResourceFenceAdmissionResultV2> {
		if (!initial.leaseCompleted || initial.result.status !== "accepted") {
			return initial.result;
		}
		let outcome = initial.result;
		const attemptedAtHead = new Set<string>();
		for (let attempts = 0; attempts < MAX_AUTOMATIC_PENDING_RETRIES; ) {
			if (this.lifecycleController.signal.aborted) {
				return this.lifecycleHaltedResult();
			}
			if (budget.signal.aborted) {
				return this.immediateResult(
					"unavailable",
					"Resource-fence pending recovery deadline elapsed",
				);
			}
			if (this.core.state === "FORKED") return this.forkFailStopResult();
			if (this.core.state === "HALTED") return this.lifecycleHaltedResult();
			if (this.core.state === "UNAVAILABLE") return outcome;

			const headCid = this.core.head?.entryCid ?? "";
			const pending = this.core.pendingPolicyReferences;
			const next = pending.find(
				({ entryCid }) => !attemptedAtHead.has(`${headCid}\0${entryCid}`),
			);
			if (next === undefined) return outcome;
			attemptedAtHead.add(`${headCid}\0${next.entryCid}`);
			attempts += 1;
			const retried = await this.retryPendingUnderPolicyLease(
				next.entryCid,
				next.policyReference,
				budget,
			);
			if (!retried.leaseCompleted) return retried.result;
			switch (retried.result.status) {
				case "accepted":
					outcome = retried.result;
					break;
				case "forked":
				case "unavailable":
				case "halted":
				case "capacity":
					return retried.result;
				default:
					break;
			}
		}
		return this.core.pendingCount === 0
			? outcome
			: this.immediateResult(
					"unavailable",
					"Resource-fence pending recovery reached its fixed work ceiling",
				);
	}

	private beginOperationBudget(): ResourceFenceOperationBudgetV2 {
		const controller = new AbortController();
		const lifecycleSignal = this.lifecycleController.signal;
		const abort = (): void => controller.abort();
		lifecycleSignal.addEventListener("abort", abort, { once: true });
		if (lifecycleSignal.aborted) controller.abort();
		const deadline = Date.now() + this.operationTimeoutMs;
		const timeout = setTimeout(abort, this.operationTimeoutMs);
		let disposed = false;
		return {
			signal: controller.signal,
			deadline,
			dispose: (): void => {
				if (disposed) return;
				disposed = true;
				clearTimeout(timeout);
				lifecycleSignal.removeEventListener("abort", abort);
			},
		};
	}

	private reserveOperation(inputBytes: number): boolean {
		if (
			this.bufferedOperations >=
				TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_FENCES ||
			inputBytes >
				TRUSTED_NETWORK_V2_MAX_QUEUED_RESOURCE_FENCE_INPUT_BYTES -
					this.bufferedOperationInputBytes
		) {
			return false;
		}
		this.bufferedOperations += 1;
		this.bufferedOperationInputBytes += inputBytes;
		return true;
	}

	private releaseOperation(inputBytes: number): void {
		this.bufferedOperations -= 1;
		this.bufferedOperationInputBytes -= inputBytes;
	}

	private enqueueResourceOperation<T>(
		operation: () => Promise<T>,
		haltOnError = true,
	): Promise<T> {
		this.publicationFences += 1;
		const result = this.operationTail.then(async () => {
			if (this.terminalError !== undefined) throw this.terminalError;
			try {
				return await operation();
			} catch (error) {
				throw haltOnError ? this.halt(error) : error;
			}
		});
		this.operationTail = result.then(
			(): void => {},
			(): void => {},
		);
		return result.finally(() => {
			this.publicationFences -= 1;
		});
	}

	private halt(error: unknown): Error {
		if (this.terminalError !== undefined) return this.terminalError;
		const cause = error instanceof Error ? error : new Error(String(error));
		const terminal = new Error(
			`TrustedNetwork v2 durable resource-fence publication is ambiguous and halted: ${cause.message}`,
		);
		terminal.cause = cause;
		this.terminalError = terminal;
		this.detachExternalAbortListener();
		this.lifecycleController.abort();
		this.core.abort();
		return terminal;
	}

	private detachExternalAbortListener(): void {
		if (
			this.externalSignal !== undefined &&
			this.externalAbortListener !== undefined
		) {
			this.externalSignal.removeEventListener(
				"abort",
				this.externalAbortListener,
			);
		}
		this.externalSignal = undefined;
		this.externalAbortListener = undefined;
	}

	private immediateResult(
		status: ResourceFenceAdmissionStatusV2,
		reason: string,
	): ResourceFenceAdmissionResultV2 {
		return {
			status,
			reason,
			head: this.head,
			fetchHints: [],
			pendingCount: this.pendingCount,
			pendingBytes: this.pendingBytes,
		};
	}

	private forkFailStopResult(): ResourceFenceAdmissionResultV2 {
		return this.immediateResult(
			"halted",
			"Resource-fence reducer is halted by authority equivocation",
		);
	}

	private lifecycleHaltedResult(): ResourceFenceAdmissionResultV2 {
		return this.immediateResult(
			"halted",
			"Resource-fence reducer lifecycle is aborted",
		);
	}

	private async persistCorePublication(
		result: ResourceFenceAdmissionResultV2,
	): Promise<void> {
		const durableState = this.core.exportDurableState();
		if (durableState.state === "EMPTY") {
			if (this.durableCoreIdentityBytes !== undefined) {
				throw new Error("Durable resource-fence state cannot return to EMPTY");
			}
			if ((result.forkObservations?.length ?? 0) !== 0) {
				throw new Error("An empty resource-fence anchor cannot contain a fork");
			}
			return;
		}

		const coreIdentityBytes = coreIdentityBytesFromState(durableState);
		const stateChanged =
			this.durableCoreIdentityBytes === undefined ||
			!equals(this.durableCoreIdentityBytes, coreIdentityBytes);
		if (durableState.state !== "FORKED") {
			if ((result.forkObservations?.length ?? 0) !== 0) {
				throw new Error(
					"Only a forked resource-fence anchor may contain observations",
				);
			}
			if (!stateChanged) return;
		} else if (this.published.state === "FORKED" || !stateChanged) {
			throw new Error(
				"A durable resource-fence fork transition may be published only once",
			);
		}

		const nextPublished = publishedFromReducer(this.core);
		const payloadBytes = serialize(checkpointPayloadFromState(durableState));
		if (
			payloadBytes.byteLength >
			TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ANCHOR_CHECKPOINT_PAYLOAD_BYTES
		) {
			throw new Error(
				"Resource-fence anchor checkpoint payload exceeds its byte ceiling",
			);
		}

		// Allocate all post-commit values before durable replacement. Once commit
		// resolves, publication consists only of non-throwing assignments.
		const nextDurableCoreIdentityBytes =
			durableState.state === "FORKED"
				? undefined
				: copyBytes(coreIdentityBytes);
		const checkpoint = this.checkpoint;
		if (checkpoint === undefined) {
			throw new Error("Resource-fence anchor checkpoint is unavailable");
		}
		await checkpoint.commit(payloadBytes);
		// A replacement that already crossed its commit boundary can be recovered
		// by a fresh opener, but the aborted instance must not publish that head.
		if (this.lifecycleController.signal.aborted) return;

		this.durableCoreIdentityBytes = nextDurableCoreIdentityBytes;
		this.published = nextPublished;
		if (durableState.state === "FORKED") this.checkpoint = undefined;
	}
}
