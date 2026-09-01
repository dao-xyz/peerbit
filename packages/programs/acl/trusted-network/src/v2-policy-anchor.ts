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
import { PublicSignKey, sha256Sync } from "@peerbit/crypto";
import { compare, concat, equals } from "uint8arrays";
import type {
	PolicyAdmissionResultV2,
	PolicyForkEvidenceV2,
	PolicyHeadProjectionV2,
	PolicyReducerDurableStateV2,
	PolicySnapshotResolverV2,
} from "./v2-policy-engine.js";
import {
	TRUSTED_NETWORK_V2_MAX_PENDING_POLICIES,
	TrustedNetworkV2PolicyReducer,
	authenticatePolicySnapshotEntryV2,
} from "./v2-policy-engine.js";
import {
	NetworkDescriptorV2,
	PolicySubjectBindingV2,
	TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS,
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
	assertNetworkDescriptorV2,
} from "./v2.js";

/**
 * Internal crash-safe policy-anchor storage format.
 *
 * This module is intentionally absent from the package entry point. Its store
 * must already be open and dedicated to this anchor and descriptor. Exactly
 * one wrapper may write that scope: the two-slot checkpoint deliberately does
 * not pretend that atomic replacement is a multi-process compare-and-swap.
 */

/** The namespace owned by the superseded append-only anchor format. */
export const TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER =
	"peerbit/trusted-network/v2/policy-anchor/v1";

const REDUCER_DURABLE_FORMAT_VERSION = 1;
const MAX_UNAVAILABLE_REASON_LENGTH = 512;
// The core admits at most 64 pending policies. A fork transition can surface
// that complete set plus its accepted and triggering children. Once the
// transition is durable the wrapper stops policy admission entirely.
const MAX_FORK_OBSERVATIONS_V2 = TRUSTED_NETWORK_V2_MAX_PENDING_POLICIES + 2;
// Variant/state and length framing for the parent, empty candidate, digest,
// empty reason, u8 vector count, and every u32-framed observation.
const MAX_CHECKPOINT_PAYLOAD_FRAMING_BYTES_V2 = 313;
/** Exact maximum encoded application payload accepted by the checkpoint. */
export const TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES =
	(MAX_FORK_OBSERVATIONS_V2 + 1) * TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES +
	MAX_CHECKPOINT_PAYLOAD_FRAMING_BYTES_V2;
const MAX_QUEUED_POLICY_ENTRY_BYTES_V2 =
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES * 2;
const ZERO_DIGEST = new Uint8Array(32);
const textEncoder = new TextEncoder();
const CHECKPOINT_SCOPE_DOMAIN = textEncoder.encode(
	"peerbit/trusted-network/v2/policy-anchor/checkpoint/v1\0",
);
const ANCHOR_STATE = Object.freeze({
	ACTIVE: 1,
	UNAVAILABLE: 2,
	FORKED: 3,
} as const);

export type CrashSafePolicyAnchorStoreV2 = CrashSafeAtomicReplaceStore;

type DurableReducerOptionsV2 = {
	descriptor: NetworkDescriptorV2;
	resolvePolicyEntry: PolicySnapshotResolverV2;
	resolveTimeoutMs?: number;
	signal?: AbortSignal;
	maxPending?: number;
	maxPendingPolicyBytes?: number;
};

export type TrustedNetworkV2DurablePolicyReducerOptions =
	DurableReducerOptionsV2 & {
		store: CrashSafePolicyAnchorStoreV2;
	};

/**
 * One self-contained application snapshot. The enclosing generic checkpoint
 * supplies generation, scope, predecessor, and checksum authentication.
 */
@variant([2, 16, 5])
class PolicyAnchorCheckpointPayloadV2 {
	@field({ type: "u8" })
	state: number;

	@field({ type: Uint8Array })
	acceptedHeadEntryBytes: Uint8Array;

	@field({ type: Uint8Array })
	comparisonCandidateEntryBytes: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	acceptedAncestorDigest: Uint8Array;

	@field({ type: "string" })
	unavailableReason: string;

	@field({ type: vec(Uint8Array, "u8") })
	forkObservationEntryBytes: Uint8Array[];

	constructor(properties?: {
		state: number;
		acceptedHeadEntryBytes: Uint8Array;
		comparisonCandidateEntryBytes: Uint8Array;
		acceptedAncestorDigest: Uint8Array;
		unavailableReason: string;
		forkObservationEntryBytes: Uint8Array[];
	}) {
		if (properties) Object.assign(this, properties);
	}
}

type CanonicalForkChildV2 = {
	digest: Uint8Array;
	entryBytes: Uint8Array;
};

type PublishedProjectionV2 = {
	state: "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED";
	head?: PolicyHeadProjectionV2;
	forkEvidence?: PolicyForkEvidenceV2;
	roles: Map<string, number>;
};

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
		// The intrinsic getter reports zero for both empty and detached views. The
		// intrinsic set distinguishes them without consulting caller properties.
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

const copyBytes = (bytes: Uint8Array): Uint8Array => {
	const byteLength = exactUint8ArrayByteLength(bytes);
	return copyBytesWithLength(bytes, byteLength);
};

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

const bytesKey = (bytes: Uint8Array): string => {
	let key = "";
	for (const byte of bytes) key += byte.toString(16).padStart(2, "0");
	return key;
};

const copyBinding = (binding: PolicySubjectBindingV2): PolicySubjectBindingV2 =>
	deserialize(serialize(binding), PolicySubjectBindingV2);

const copyHead = (
	head: PolicyHeadProjectionV2 | undefined,
): PolicyHeadProjectionV2 | undefined =>
	head === undefined
		? undefined
		: {
				sequence: head.sequence,
				digest: copyBytes(head.digest),
				bindings: head.bindings.map(copyBinding),
			};

const copyForkEvidence = (
	evidence: PolicyForkEvidenceV2 | undefined,
): PolicyForkEvidenceV2 | undefined =>
	evidence === undefined
		? undefined
		: {
				commonParent: copyHead(evidence.commonParent)!,
				children: [
					{
						sequence: evidence.children[0].sequence,
						digest: copyBytes(evidence.children[0].digest),
						entryBytes: copyBytes(evidence.children[0].entryBytes),
					},
					{
						sequence: evidence.children[1].sequence,
						digest: copyBytes(evidence.children[1].digest),
						entryBytes: copyBytes(evidence.children[1].entryBytes),
					},
				],
			};

const keyId = (key: PublicSignKey): string => bytesKey(serialize(key));

const observationContentHash = (entryBytes: Uint8Array): Uint8Array =>
	sha256Sync(entryBytes);

const u32Bytes = (value: number): Uint8Array => {
	const bytes = new Uint8Array(4);
	new DataView(bytes.buffer).setUint32(0, value, false);
	return bytes;
};

const checkpointScope = (descriptor: NetworkDescriptorV2): Uint8Array => {
	const descriptorBytes = serialize(descriptor);
	return concat([
		CHECKPOINT_SCOPE_DOMAIN,
		u32Bytes(descriptorBytes.byteLength),
		descriptorBytes,
	]);
};

const assertOpenNotAborted = (signal: AbortSignal | undefined): void => {
	if (signal?.aborted) {
		throw new Error("TrustedNetwork v2 durable policy open was aborted");
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
		byteLength > TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES
	) {
		throw new Error(
			`${label} must contain 1-${TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES} bytes`,
		);
	}
};

const canonicalForkObservationEntries = (
	entries: Iterable<Uint8Array>,
): Uint8Array[] => {
	const observed = new Map<string, Uint8Array>();
	const hashed: Array<{ entryBytes: Uint8Array; hash: Uint8Array }> = [];
	for (const entryBytes of entries) {
		assertEntryBytes(entryBytes, "fork observation entry");
		const hash = observationContentHash(entryBytes);
		const hashKey = bytesKey(hash);
		const existing = observed.get(hashKey);
		if (existing !== undefined) {
			if (equals(existing, entryBytes)) {
				throw new Error("Duplicate policy fork observation");
			}
			throw new Error("Policy fork-observation content hash collision");
		}
		observed.set(hashKey, entryBytes);
		hashed.push({ entryBytes, hash });
	}
	return hashed
		.sort((left, right) => {
			const hashOrder = compare(left.hash, right.hash);
			return hashOrder === 0
				? compare(left.entryBytes, right.entryBytes)
				: hashOrder;
		})
		.map(({ entryBytes }) => entryBytes);
};

const assertEmptyBytes = (bytes: Uint8Array, label: string): void => {
	if (!hasExactUint8ArrayByteLength(bytes, 0)) {
		throw new Error(`${label} must be empty`);
	}
};

const retainCanonicalForkChild = (
	children: CanonicalForkChildV2[],
	candidate: CanonicalForkChildV2,
): void => {
	const existingIndex = children.findIndex(({ digest }) =>
		equals(digest, candidate.digest),
	);
	if (existingIndex >= 0) {
		if (
			compare(candidate.entryBytes, children[existingIndex]!.entryBytes) >= 0
		) {
			return;
		}
		children.splice(existingIndex, 1);
	}
	children.push({
		digest: copyBytes(candidate.digest),
		entryBytes: copyBytes(candidate.entryBytes),
	});
	children.sort((left, right) => {
		const digestOrder = compare(left.digest, right.digest);
		return digestOrder === 0
			? compare(left.entryBytes, right.entryBytes)
			: digestOrder;
	});
	if (children.length > 2) children.length = 2;
};

const assertCanonicalObservationOrder = (
	entries: readonly Uint8Array[],
): void => {
	const canonical = canonicalForkObservationEntries(entries);
	if (
		canonical.length !== entries.length ||
		canonical.some((entryBytes, index) => !equals(entryBytes, entries[index]!))
	) {
		throw new Error("Policy fork observations are not in canonical order");
	}
};

const checkpointPayloadFromState = (
	state: Exclude<PolicyReducerDurableStateV2, { state: "EMPTY" }>,
	forkObservationEntryBytes: readonly Uint8Array[] = [],
): PolicyAnchorCheckpointPayloadV2 => {
	if (state.state === "ACTIVE") {
		return new PolicyAnchorCheckpointPayloadV2({
			state: ANCHOR_STATE.ACTIVE,
			acceptedHeadEntryBytes: copyBytes(state.acceptedHeadEntryBytes),
			comparisonCandidateEntryBytes: new Uint8Array(0),
			acceptedAncestorDigest: copyBytes(ZERO_DIGEST),
			unavailableReason: "",
			forkObservationEntryBytes: [],
		});
	}
	if (state.state === "UNAVAILABLE") {
		return new PolicyAnchorCheckpointPayloadV2({
			state: ANCHOR_STATE.UNAVAILABLE,
			acceptedHeadEntryBytes: copyBytes(state.acceptedHeadEntryBytes),
			comparisonCandidateEntryBytes: copyBytes(
				state.comparisonCandidateEntryBytes,
			),
			acceptedAncestorDigest: copyBytes(state.acceptedAncestorDigest),
			unavailableReason: state.reason,
			forkObservationEntryBytes: [],
		});
	}
	return new PolicyAnchorCheckpointPayloadV2({
		state: ANCHOR_STATE.FORKED,
		acceptedHeadEntryBytes: copyBytes(state.commonParentEntryBytes),
		comparisonCandidateEntryBytes: new Uint8Array(0),
		acceptedAncestorDigest: copyBytes(ZERO_DIGEST),
		unavailableReason: "",
		forkObservationEntryBytes: forkObservationEntryBytes.map(copyBytes),
	});
};

const coreIdentityBytesFromState = (
	state: Exclude<PolicyReducerDurableStateV2, { state: "EMPTY" }>,
): Uint8Array =>
	// Fork observations are deliberately excluded from this transient identity.
	// A FORKED identity is only compared with the preceding non-forked snapshot;
	// after publication, the wrapper admits no more work and retains no copy.
	serialize(checkpointPayloadFromState(state));

const decodeCanonicalCheckpointPayload = (
	input: Uint8Array,
): PolicyAnchorCheckpointPayloadV2 => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLength(input);
	} catch {
		byteLength = -1;
	}
	if (
		byteLength < 1 ||
		byteLength > TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES
	) {
		throw new Error(
			"Policy-anchor checkpoint payload exceeds its byte ceiling",
		);
	}
	// CrashSafeTwoSlotCheckpoint.current already returns a fresh genuine exact
	// copy, so retaining that input while deserializing avoids another full-size
	// restore copy without exposing caller-owned bytes.
	const payload = deserialize(input, PolicyAnchorCheckpointPayloadV2);
	if (!equals(input, serialize(payload))) {
		throw new Error("Policy-anchor checkpoint payload is not canonical");
	}
	return payload;
};

type DecodedCheckpointPayloadV2 = {
	durableState: Exclude<PolicyReducerDurableStateV2, { state: "EMPTY" }>;
	coreIdentityBytes?: Uint8Array;
};

const durableStateFromCheckpointPayload = async (
	payload: PolicyAnchorCheckpointPayloadV2,
	descriptor: NetworkDescriptorV2,
	signal: AbortSignal | undefined,
): Promise<DecodedCheckpointPayloadV2> => {
	assertEntryBytes(payload.acceptedHeadEntryBytes, "accepted head entry");
	if (payload.state === ANCHOR_STATE.ACTIVE) {
		assertEmptyBytes(
			payload.comparisonCandidateEntryBytes,
			"active comparison candidate",
		);
		if (!equals(payload.acceptedAncestorDigest, ZERO_DIGEST)) {
			throw new Error("Active accepted-ancestor digest must be zero");
		}
		if (payload.unavailableReason !== "") {
			throw new Error("Active unavailable reason must be empty");
		}
		if (payload.forkObservationEntryBytes.length !== 0) {
			throw new Error(
				"Active policy-anchor state must not contain fork evidence",
			);
		}
		const durableState = {
			formatVersion: REDUCER_DURABLE_FORMAT_VERSION,
			state: "ACTIVE",
			acceptedHeadEntryBytes: copyBytes(payload.acceptedHeadEntryBytes),
		} as const;
		return {
			durableState,
			coreIdentityBytes: coreIdentityBytesFromState(durableState),
		};
	}
	if (payload.state === ANCHOR_STATE.UNAVAILABLE) {
		assertEntryBytes(
			payload.comparisonCandidateEntryBytes,
			"unavailable comparison candidate",
		);
		if (!hasExactUint8ArrayByteLength(payload.acceptedAncestorDigest, 32)) {
			throw new Error("Unavailable accepted-ancestor digest must be 32 bytes");
		}
		if (
			payload.unavailableReason.length === 0 ||
			payload.unavailableReason.length > MAX_UNAVAILABLE_REASON_LENGTH
		) {
			throw new Error(
				"Unavailable reason is empty or exceeds its character ceiling",
			);
		}
		if (payload.forkObservationEntryBytes.length !== 0) {
			throw new Error(
				"Unavailable policy-anchor state must not contain fork evidence",
			);
		}
		const durableState = {
			formatVersion: REDUCER_DURABLE_FORMAT_VERSION,
			state: "UNAVAILABLE",
			acceptedHeadEntryBytes: copyBytes(payload.acceptedHeadEntryBytes),
			comparisonCandidateEntryBytes: copyBytes(
				payload.comparisonCandidateEntryBytes,
			),
			acceptedAncestorDigest: copyBytes(payload.acceptedAncestorDigest),
			reason: payload.unavailableReason,
		} as const;
		return {
			durableState,
			coreIdentityBytes: coreIdentityBytesFromState(durableState),
		};
	}
	if (payload.state !== ANCHOR_STATE.FORKED) {
		throw new Error("Unknown durable policy-anchor state");
	}
	assertEmptyBytes(
		payload.comparisonCandidateEntryBytes,
		"forked comparison candidate",
	);
	if (!equals(payload.acceptedAncestorDigest, ZERO_DIGEST)) {
		throw new Error("Forked accepted-ancestor digest must be zero");
	}
	if (payload.unavailableReason !== "") {
		throw new Error("Forked unavailable reason must be empty");
	}
	if (
		payload.forkObservationEntryBytes.length < 2 ||
		payload.forkObservationEntryBytes.length > MAX_FORK_OBSERVATIONS_V2
	) {
		throw new Error(
			`Forked policy-anchor state must contain 2-${MAX_FORK_OBSERVATIONS_V2} observations`,
		);
	}
	assertCanonicalObservationOrder(payload.forkObservationEntryBytes);

	const commonParent = await authenticatePolicySnapshotEntryV2(
		payload.acceptedHeadEntryBytes,
		descriptor,
	);
	assertOpenNotAborted(signal);
	const canonicalChildren: CanonicalForkChildV2[] = [];
	for (const entryBytes of payload.forkObservationEntryBytes) {
		const authenticated = await authenticatePolicySnapshotEntryV2(
			entryBytes,
			descriptor,
		);
		assertOpenNotAborted(signal);
		if (
			authenticated.body.sequence !== commonParent.body.sequence + 1n ||
			!equals(authenticated.body.previousPolicyDigest, commonParent.digest)
		) {
			throw new Error(
				"Stored policy fork observation is not a direct child of the common parent",
			);
		}
		retainCanonicalForkChild(canonicalChildren, {
			digest: authenticated.digest,
			entryBytes,
		});
	}
	if (canonicalChildren.length !== 2) {
		throw new Error(
			"Stored policy fork evidence has fewer than two distinct children",
		);
	}
	return {
		durableState: {
			formatVersion: REDUCER_DURABLE_FORMAT_VERSION,
			state: "FORKED",
			commonParentEntryBytes: copyBytes(payload.acceptedHeadEntryBytes),
			childEntryBytes: [
				copyBytes(canonicalChildren[0]!.entryBytes),
				copyBytes(canonicalChildren[1]!.entryBytes),
			],
		},
	};
};

const publishedFromReducer = (
	reducer: TrustedNetworkV2PolicyReducer,
): PublishedProjectionV2 => {
	if (reducer.state === "HALTED") {
		throw new Error("Cannot publish a halted policy reducer");
	}
	const head = copyHead(reducer.head);
	const roles = new Map<string, number>();
	for (const binding of head?.bindings ?? []) {
		roles.set(keyId(binding.signingKey), binding.roles);
	}
	return {
		state: reducer.state,
		head,
		forkEvidence: copyForkEvidence(reducer.forkEvidence),
		roles,
	};
};

const assertCanonicalCoreRestore = (
	reducer: TrustedNetworkV2PolicyReducer,
	expectedState: Exclude<PolicyReducerDurableStateV2, { state: "EMPTY" }>,
): void => {
	const restoredState = reducer.exportDurableState();
	if (
		restoredState.state === "EMPTY" ||
		restoredState.state !== expectedState.state
	) {
		throw new Error("Restored policy-anchor state is unexpectedly empty");
	}
	let exact = false;
	if (restoredState.state === "ACTIVE" && expectedState.state === "ACTIVE") {
		exact = equals(
			restoredState.acceptedHeadEntryBytes,
			expectedState.acceptedHeadEntryBytes,
		);
	} else if (
		restoredState.state === "UNAVAILABLE" &&
		expectedState.state === "UNAVAILABLE"
	) {
		exact =
			equals(
				restoredState.acceptedHeadEntryBytes,
				expectedState.acceptedHeadEntryBytes,
			) &&
			equals(
				restoredState.comparisonCandidateEntryBytes,
				expectedState.comparisonCandidateEntryBytes,
			) &&
			equals(
				restoredState.acceptedAncestorDigest,
				expectedState.acceptedAncestorDigest,
			) &&
			restoredState.reason === expectedState.reason;
	} else if (
		restoredState.state === "FORKED" &&
		expectedState.state === "FORKED"
	) {
		exact =
			equals(
				restoredState.commonParentEntryBytes,
				expectedState.commonParentEntryBytes,
			) &&
			equals(
				restoredState.childEntryBytes[0],
				expectedState.childEntryBytes[0],
			) &&
			equals(
				restoredState.childEntryBytes[1],
				expectedState.childEntryBytes[1],
			);
	}
	if (!exact) {
		throw new Error("Restored policy-anchor state is not canonical");
	}
};

const canonicalForkEntriesForCommit = (
	result: PolicyAdmissionResultV2,
	durableState: Extract<PolicyReducerDurableStateV2, { state: "FORKED" }>,
	forkEvidence: PolicyForkEvidenceV2 | undefined,
): Uint8Array[] => {
	if (forkEvidence === undefined) {
		throw new Error("Forked policy reducer has no fork evidence");
	}
	const observations = result.forkObservations ?? [];
	if (
		observations.length < 2 ||
		observations.length > MAX_FORK_OBSERVATIONS_V2
	) {
		throw new Error(
			`A durable fork transition must contain 2-${MAX_FORK_OBSERVATIONS_V2} observations`,
		);
	}
	const observationEntries: Uint8Array[] = [];
	const canonicalChildren: CanonicalForkChildV2[] = [];
	for (const proof of observations) {
		assertEntryBytes(proof.entryBytes, "fork observation entry");
		if (!hasExactUint8ArrayByteLength(proof.digest, 32)) {
			throw new Error("Policy fork-observation digest must be 32 bytes");
		}
		if (proof.sequence !== forkEvidence.commonParent.sequence + 1n) {
			throw new Error(
				"Policy fork observation is not a direct child of the common parent",
			);
		}
		// The reducer result owns this genuine exact copy and is not exposed until
		// publication settles, so retaining it through this synchronous validation
		// avoids another full fork-evidence copy.
		observationEntries.push(proof.entryBytes);
		retainCanonicalForkChild(canonicalChildren, proof);
	}
	if (canonicalChildren.length !== 2) {
		throw new Error(
			"A durable fork transition requires two distinct policy children",
		);
	}
	for (let index = 0; index < canonicalChildren.length; index++) {
		if (
			!equals(
				canonicalChildren[index]!.entryBytes,
				durableState.childEntryBytes[index]!,
			)
		) {
			throw new Error(
				"Published fork state does not contain the lowest canonical children",
			);
		}
	}
	return canonicalForkObservationEntries(observationEntries);
};

/**
 * Crash-safe publication wrapper for the internal v2 reducer.
 *
 * The mutable reducer is never exposed. Authorization reads use only the last
 * projection published after one atomic checkpoint replacement. An admission
 * places a fail-closed fence around both reducer mutation and publication.
 */
export class TrustedNetworkV2DurablePolicyReducer {
	private checkpoint?: CrashSafeTwoSlotCheckpoint;
	private core: TrustedNetworkV2PolicyReducer;
	private published: PublishedProjectionV2;
	private durableCoreIdentityBytes?: Uint8Array;
	private operationTail: Promise<void> = Promise.resolve();
	private authorizationFences = 0;
	private bufferedAdmissions = 0;
	private bufferedAdmissionEntryBytes = 0;
	private terminalError?: Error;

	private constructor(properties: {
		checkpoint?: CrashSafeTwoSlotCheckpoint;
		core: TrustedNetworkV2PolicyReducer;
		durableCoreIdentityBytes?: Uint8Array;
	}) {
		this.checkpoint = properties.checkpoint;
		this.core = properties.core;
		this.published = publishedFromReducer(this.core);
		this.durableCoreIdentityBytes =
			properties.durableCoreIdentityBytes === undefined
				? undefined
				: copyBytes(properties.durableCoreIdentityBytes);
	}

	static async open(
		options: TrustedNetworkV2DurablePolicyReducerOptions,
	): Promise<TrustedNetworkV2DurablePolicyReducer> {
		assertNetworkDescriptorV2(options.descriptor);
		const descriptor = deserialize(
			serialize(options.descriptor),
			NetworkDescriptorV2,
		);
		const signal = options.signal;
		const store = options.store;
		const coreProperties: DurableReducerOptionsV2 = {
			descriptor,
			resolvePolicyEntry: options.resolvePolicyEntry,
			resolveTimeoutMs: options.resolveTimeoutMs,
			signal,
			maxPending: options.maxPending,
			maxPendingPolicyBytes: options.maxPendingPolicyBytes,
		};
		assertOpenNotAborted(signal);
		const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
			store,
			scope: checkpointScope(descriptor),
			maxPayloadBytes:
				TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES,
		});
		assertOpenNotAborted(signal);

		// Never let the old append-only format look like an empty or newer
		// checkpoint. It was internal and never activated, so migration or fallback
		// would add rollback surface without preserving a public compatibility need.
		for await (const [key] of store.iterator()) {
			assertOpenNotAborted(signal);
			if (
				key === TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER ||
				key.startsWith(`${TRUSTED_NETWORK_V2_POLICY_ANCHOR_STORE_OWNER}/`)
			) {
				throw new Error(
					"Legacy append-only policy-anchor records are not supported; reset the dedicated store",
				);
			}
		}
		assertOpenNotAborted(signal);

		const current = checkpoint.current;
		let core: TrustedNetworkV2PolicyReducer | undefined;
		try {
			if (current === undefined) {
				core = new TrustedNetworkV2PolicyReducer(coreProperties);
				return new TrustedNetworkV2DurablePolicyReducer({
					checkpoint,
					core,
				});
			}

			const payload = decodeCanonicalCheckpointPayload(current.payload);
			const decoded = await durableStateFromCheckpointPayload(
				payload,
				descriptor,
				signal,
			);
			assertOpenNotAborted(signal);
			core = await TrustedNetworkV2PolicyReducer.restore({
				...coreProperties,
				durableState: decoded.durableState,
			});
			assertOpenNotAborted(signal);
			assertCanonicalCoreRestore(core, decoded.durableState);
			const forked = decoded.durableState.state === "FORKED";
			return new TrustedNetworkV2DurablePolicyReducer({
				// The helper retains its latest payload. A terminal fork no longer needs
				// storage, so release that potentially 8.78 MiB snapshot immediately.
				checkpoint: forked ? undefined : checkpoint,
				core,
				durableCoreIdentityBytes: decoded.coreIdentityBytes,
			});
		} catch (error) {
			core?.abort();
			throw error;
		}
	}

	get state(): "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED" | "HALTED" {
		if (this.terminalError !== undefined) {
			return "HALTED";
		}
		if (this.published.state === "FORKED") return "FORKED";
		if (this.core.state === "HALTED") return "HALTED";
		return this.published.state;
	}

	get head(): PolicyHeadProjectionV2 | undefined {
		return copyHead(this.published.head);
	}

	get forkEvidence(): PolicyForkEvidenceV2 | undefined {
		return copyForkEvidence(this.published.forkEvidence);
	}

	get pendingCount(): number {
		return this.published.state === "FORKED" ? 0 : this.core.pendingCount;
	}

	get pendingBytes(): number {
		return this.published.state === "FORKED" ? 0 : this.core.pendingBytes;
	}

	get pendingDigests(): Uint8Array[] {
		return this.published.state === "FORKED"
			? []
			: this.core.pendingDigests.map(copyBytes);
	}

	/** Internal diagnostics for the fixed pre-publication admission bound. */
	get bufferedAdmissionCount(): number {
		return this.bufferedAdmissions;
	}

	/** Internal diagnostics for copied entry bytes awaiting settlement. */
	get bufferedAdmissionBytes(): number {
		return this.bufferedAdmissionEntryBytes;
	}

	/** Projection query only; use isAuthorized() for the fail-closed gate. */
	rolesFor(subject: PublicSignKey): number {
		return this.published.roles.get(keyId(subject)) ?? 0;
	}

	isAuthorized(subject: PublicSignKey, roles: number): boolean {
		if (
			this.authorizationFences !== 0 ||
			this.state !== "ACTIVE" ||
			!Number.isInteger(roles) ||
			roles === 0 ||
			(roles & ~TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS) !== 0
		) {
			return false;
		}
		return (this.rolesFor(subject) & roles) === roles;
	}

	abort(): void {
		this.core.abort();
	}

	ingest(entryBytes: Uint8Array): Promise<PolicyAdmissionResultV2> {
		if (this.terminalError !== undefined) {
			return Promise.reject(this.terminalError);
		}
		if (this.published.state === "FORKED") {
			return Promise.resolve(this.forkFailStopResult());
		}
		if (this.core.state === "HALTED") {
			return Promise.resolve(this.lifecycleHaltedResult());
		}
		let reservedEntryBytes: number;
		try {
			reservedEntryBytes = exactUint8ArrayByteLength(entryBytes);
		} catch {
			return Promise.resolve(
				this.immediateAdmissionResult(
					"rejected",
					"Policy snapshot entry must be a Uint8Array",
				),
			);
		}
		if (reservedEntryBytes > TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES) {
			return Promise.resolve(
				this.immediateAdmissionResult(
					"rejected",
					`Policy snapshot entry exceeds ${TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES} bytes`,
				),
			);
		}
		if (!this.reserveAdmission(reservedEntryBytes)) {
			return Promise.resolve(
				this.immediateAdmissionResult(
					"capacity",
					"Durable policy admission queue is at its fixed capacity",
					this.core.pendingCount,
					this.core.pendingBytes,
				),
			);
		}
		let captured: Uint8Array;
		try {
			captured = copyBytesWithLength(entryBytes, reservedEntryBytes);
		} catch (error) {
			this.releaseAdmission(reservedEntryBytes);
			return Promise.reject(error);
		}
		return this.enqueue(async () => {
			if (this.published.state === "FORKED") {
				return this.forkFailStopResult();
			}
			if (this.core.state === "HALTED") {
				return this.lifecycleHaltedResult();
			}
			const result = await this.core.ingest(captured);
			await this.persistCorePublication(result);
			return result;
		}, reservedEntryBytes);
	}

	retryUnavailable(): Promise<PolicyAdmissionResultV2> {
		if (this.terminalError !== undefined) {
			return Promise.reject(this.terminalError);
		}
		if (this.published.state === "FORKED") {
			return Promise.resolve(this.forkFailStopResult());
		}
		if (this.core.state === "HALTED") {
			return Promise.resolve(this.lifecycleHaltedResult());
		}
		if (!this.reserveAdmission(0)) {
			return Promise.resolve(
				this.immediateAdmissionResult(
					"capacity",
					"Durable policy admission queue is at its fixed capacity",
					this.core.pendingCount,
					this.core.pendingBytes,
				),
			);
		}
		return this.enqueue(async () => {
			if (this.published.state === "FORKED") {
				return this.forkFailStopResult();
			}
			if (this.core.state === "HALTED") {
				return this.lifecycleHaltedResult();
			}
			const result = await this.core.retryUnavailable();
			await this.persistCorePublication(result);
			return result;
		}, 0);
	}

	private forkFailStopResult(): PolicyAdmissionResultV2 {
		return {
			status: "halted",
			reason: "Policy reducer is halted by authority equivocation",
			fetchHints: [],
			pendingCount: 0,
			pendingBytes: 0,
		};
	}

	private lifecycleHaltedResult(): PolicyAdmissionResultV2 {
		return {
			status: "halted",
			reason: "Policy reducer lifecycle is aborted",
			fetchHints: [],
			pendingCount: this.core.pendingCount,
			pendingBytes: this.core.pendingBytes,
		};
	}

	private immediateAdmissionResult(
		status: "capacity" | "rejected",
		reason: string,
		pendingCount = 0,
		pendingBytes = 0,
	): PolicyAdmissionResultV2 {
		return { status, reason, fetchHints: [], pendingCount, pendingBytes };
	}

	private reserveAdmission(entryBytes: number): boolean {
		if (
			this.bufferedAdmissions >= TRUSTED_NETWORK_V2_MAX_PENDING_POLICIES ||
			entryBytes >
				MAX_QUEUED_POLICY_ENTRY_BYTES_V2 - this.bufferedAdmissionEntryBytes
		) {
			return false;
		}
		this.bufferedAdmissions += 1;
		this.bufferedAdmissionEntryBytes += entryBytes;
		return true;
	}

	private releaseAdmission(entryBytes: number): void {
		this.bufferedAdmissions -= 1;
		this.bufferedAdmissionEntryBytes -= entryBytes;
	}

	private enqueue<T>(
		operation: () => Promise<T>,
		bufferedEntryBytes: number,
	): Promise<T> {
		this.authorizationFences++;
		const result = this.operationTail.then(async () => {
			if (this.terminalError !== undefined) throw this.terminalError;
			try {
				return await operation();
			} catch (error) {
				throw this.halt(error);
			}
		});
		this.operationTail = result.then(
			(): void => {},
			(): void => {},
		);
		return result.finally(() => {
			this.authorizationFences--;
			this.releaseAdmission(bufferedEntryBytes);
		});
	}

	private halt(error: unknown): Error {
		if (this.terminalError !== undefined) return this.terminalError;
		const cause = error instanceof Error ? error : new Error(String(error));
		const terminal = new Error(
			`TrustedNetwork v2 durable policy publication is ambiguous and halted: ${cause.message}`,
		);
		terminal.cause = cause;
		this.terminalError = terminal;
		this.core.abort();
		return terminal;
	}

	private async persistCorePublication(
		result: PolicyAdmissionResultV2,
	): Promise<void> {
		const durableState = this.core.exportDurableState();
		if (durableState.state === "EMPTY") {
			if (this.durableCoreIdentityBytes !== undefined) {
				throw new Error("Durable policy state cannot return to EMPTY");
			}
			if ((result.forkObservations?.length ?? 0) !== 0) {
				throw new Error("An empty policy anchor cannot contain fork evidence");
			}
			return;
		}

		const coreIdentityBytes = coreIdentityBytesFromState(durableState);
		const stateChanged =
			this.durableCoreIdentityBytes === undefined ||
			!equals(this.durableCoreIdentityBytes, coreIdentityBytes);
		if (durableState.state !== "FORKED") {
			if ((result.forkObservations?.length ?? 0) !== 0) {
				throw new Error("Only a forked policy anchor may contain observations");
			}
			if (!stateChanged) return;
		} else if (this.published.state === "FORKED" || !stateChanged) {
			throw new Error("A durable fork transition may be published only once");
		}

		const nextPublished = publishedFromReducer(this.core);
		const observationEntries =
			durableState.state === "FORKED"
				? canonicalForkEntriesForCommit(
						result,
						durableState,
						nextPublished.forkEvidence,
					)
				: [];
		const payloadBytes = serialize(
			checkpointPayloadFromState(durableState, observationEntries),
		);
		if (
			payloadBytes.byteLength >
			TRUSTED_NETWORK_V2_MAX_POLICY_ANCHOR_CHECKPOINT_PAYLOAD_BYTES
		) {
			throw new Error(
				"Policy-anchor checkpoint payload exceeds its byte ceiling",
			);
		}

		// Allocate every post-commit publication value before durable replacement.
		// Once commit resolves, the remaining operations are non-throwing assignments.
		const nextDurableCoreIdentityBytes =
			durableState.state === "FORKED"
				? undefined
				: copyBytes(coreIdentityBytes);
		const checkpoint = this.checkpoint;
		if (checkpoint === undefined) {
			throw new Error("Policy-anchor checkpoint is unavailable");
		}
		await checkpoint.commit(payloadBytes);

		this.durableCoreIdentityBytes = nextDurableCoreIdentityBytes;
		this.published = nextPublished;
		if (durableState.state === "FORKED") {
			// The terminal projection is self-contained; drop the helper's retained
			// copy of the potentially maximum-sized checkpoint payload.
			this.checkpoint = undefined;
		}
	}
}
