import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	cidifyString,
	codecMap,
	defaultHasher,
	stringifyCid,
} from "@peerbit/blocks-interface";
import {
	type BoundedEntryV0CausalReachabilityLimits,
	type BoundedEntryV0CausalReachabilityResolver,
	checkBoundedEntryV0CausalReachability,
} from "@peerbit/log";
import { compare, equals } from "uint8arrays";
import {
	type AuthenticatedResourceFenceEntryV2,
	authenticateResourceFenceEntryV2,
} from "./v2-resource-fence-entry.js";
import {
	NetworkDescriptorV2,
	ResourceFenceV2,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES,
	assertNetworkDescriptorV2,
} from "./v2.js";

/** Internal ceilings for the non-activatable resource-fence reducer. */
export const TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_FENCES = 64;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_MISSING_CIDS = 64;
export const TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS = 128;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ANCESTRY_STEPS = 64;

const PENDING_ACCOUNTING_OVERHEAD = 64;
const MAX_PENDING_BYTES =
	(TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES +
		PENDING_ACCOUNTING_OVERHEAD) *
	TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_FENCES;
const MAX_CAUSAL_ENTRY_BYTES = 128 * 1024;
const MAX_CAUSAL_TOTAL_BYTES = 16 * 1024 * 1024;
const MAX_CAUSAL_PARENT_LINKS = 64 * 1024;
const DEFAULT_CAUSAL_TIMEOUT_MS = 10 * 1000;
const MAX_TIMER_DELAY_MS = 0x7fffffff;
const MAX_REASON_LENGTH = 512;

const DEFAULT_CAUSAL_LIMITS: Readonly<BoundedEntryV0CausalReachabilityLimits> =
	Object.freeze({
		maxEntryBytes: MAX_CAUSAL_ENTRY_BYTES,
		maxDirectParents: TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS,
		maxVisitedEntries: 1024,
		maxTotalBytes: MAX_CAUSAL_TOTAL_BYTES,
		maxParentLinks: MAX_CAUSAL_PARENT_LINKS,
		maxResolveBatchSize: TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_DIRECT_PARENTS,
	});

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

const exactByteLength = (value: unknown): number => {
	if (
		!ARRAY_BUFFER_IS_VIEW(value) ||
		TYPED_ARRAY_TAG.call(value) !== "Uint8Array"
	) {
		throw new TypeError("Expected a genuine Uint8Array");
	}
	const byteLength = TYPED_ARRAY_BYTE_LENGTH.call(value) as number;
	if (byteLength === 0) {
		UINT8_ARRAY_SET.call(new Uint8Array(0), value as Uint8Array);
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
	copyBytesWithLength(bytes, exactByteLength(bytes));

const captureBytes32 = (bytes: Uint8Array, label: string): Uint8Array => {
	let byteLength: number;
	try {
		byteLength = exactByteLength(bytes);
	} catch {
		throw new Error(`${label} must contain exactly 32 bytes`);
	}
	if (byteLength !== 32) {
		throw new Error(`${label} must contain exactly 32 bytes`);
	}
	return copyBytesWithLength(bytes, byteLength);
};

const bytesKey = (bytes: Uint8Array): string => {
	let key = "";
	for (const byte of bytes) key += byte.toString(16).padStart(2, "0");
	return key;
};

const boundedReason = (reason: string): string =>
	reason.slice(0, MAX_REASON_LENGTH);

const validationMessage = (error: unknown): string =>
	error instanceof Error ? error.message : String(error);

const copyBody = (body: ResourceFenceV2): ResourceFenceV2 =>
	new ResourceFenceV2({
		networkId: copyBytes(body.networkId),
		resourceId: copyBytes(body.resourceId),
		fenceSequence: body.fenceSequence,
		previousFenceDigest: copyBytes(body.previousFenceDigest),
		policySequence: body.policySequence,
		policyDigest: copyBytes(body.policyDigest),
		contentEpoch: body.contentEpoch,
		epochManifestDigest: copyBytes(body.epochManifestDigest),
	});

type ValidatedResourceFenceV2 = {
	entryBytes: Uint8Array;
	entryCid: string;
	digest: Uint8Array;
	body: ResourceFenceV2;
	directParents: Array<{ cid: string; digest: Uint8Array }>;
	accountedBytes: number;
};

const snapshotFromAuthenticated = (
	authenticated: AuthenticatedResourceFenceEntryV2,
): ValidatedResourceFenceV2 => ({
	entryBytes: copyBytes(authenticated.entryBytes),
	entryCid: authenticated.entryCid,
	digest: copyBytes(authenticated.digest),
	body: copyBody(authenticated.body),
	directParents: authenticated.directParents.map((parent) => ({
		cid: parent.cid,
		digest: copyBytes(parent.digest),
	})),
	accountedBytes:
		authenticated.entryBytes.byteLength + PENDING_ACCOUNTING_OVERHEAD,
});

const copySnapshot = (
	snapshot: ValidatedResourceFenceV2,
): ValidatedResourceFenceV2 => ({
	entryBytes: copyBytes(snapshot.entryBytes),
	entryCid: snapshot.entryCid,
	digest: copyBytes(snapshot.digest),
	body: copyBody(snapshot.body),
	directParents: snapshot.directParents.map((parent) => ({
		cid: parent.cid,
		digest: copyBytes(parent.digest),
	})),
	accountedBytes: snapshot.accountedBytes,
});

export type ResourceFencePolicyReferenceV2 = Readonly<{
	sequence: bigint;
	digest: Uint8Array;
}>;

export type AcceptedResourceFencePolicyV2 = ResourceFencePolicyReferenceV2;

export type ResourceFenceHeadProjectionV2 = Readonly<{
	sequence: bigint;
	digest: Uint8Array;
	entryCid: string;
	previousFenceDigest: Uint8Array;
	policy: ResourceFencePolicyReferenceV2;
	contentEpoch: bigint;
	epochManifestDigest: Uint8Array;
	/** Exact signed EntryV0 `meta.next`; an empty array is an explicit frontier. */
	causalFrontier: ReadonlyArray<Readonly<{ cid: string; digest: Uint8Array }>>;
}>;

export type ResourceFenceForkChildProofV2 = Readonly<{
	sequence: bigint;
	digest: Uint8Array;
	entryCid: string;
	entryBytes: Uint8Array;
}>;

export type ResourceFenceForkParentV2 =
	| Readonly<{ kind: "initial"; digest: Uint8Array }>
	| Readonly<{ kind: "fence"; head: ResourceFenceHeadProjectionV2 }>;

export type ResourceFenceForkEvidenceV2 = Readonly<{
	commonParent: ResourceFenceForkParentV2;
	children: readonly [
		ResourceFenceForkChildProofV2,
		ResourceFenceForkChildProofV2,
	];
}>;

export type ResourceFenceFetchHintV2 =
	| Readonly<{ kind: "resource-fence-predecessor"; digest: Uint8Array }>
	| Readonly<{ kind: "causal-entry"; cid: string }>;

export type ResourceFenceAdmissionStatusV2 =
	| "accepted"
	| "duplicate"
	| "pending"
	| "unavailable"
	| "capacity"
	| "rejected"
	| "forked"
	| "halted";

export type ResourceFenceAdmissionResultV2 = Readonly<{
	status: ResourceFenceAdmissionStatusV2;
	reason?: string;
	head?: ResourceFenceHeadProjectionV2;
	forkObservations?: readonly ResourceFenceForkChildProofV2[];
	fetchHints: readonly ResourceFenceFetchHintV2[];
	pendingCount: number;
	pendingBytes: number;
	evictedEntryCids?: readonly string[];
}>;

export type PreparedResourceFenceCandidateV2 = Readonly<{
	entryCid: string;
	policyReference: ResourceFencePolicyReferenceV2;
}>;

export type ResourceFencePreparationResultV2 =
	| Readonly<{
			status: "prepared";
			candidate: PreparedResourceFenceCandidateV2;
	  }>
	| Readonly<{ status: "rejected" | "halted"; reason: string }>;

export type ResourceFenceReducerDurableStateV2 =
	| Readonly<{ formatVersion: 1; state: "EMPTY" }>
	| Readonly<{
			formatVersion: 1;
			state: "ACTIVE";
			acceptedHeadEntryBytes: Uint8Array;
			acceptedParentEntryBytes?: Uint8Array;
	  }>
	| Readonly<{
			formatVersion: 1;
			state: "UNAVAILABLE";
			acceptedHeadEntryBytes: Uint8Array;
			acceptedParentEntryBytes?: Uint8Array;
			comparisonCandidateEntryBytes: Uint8Array;
			missingCids: readonly string[];
			/** Exact accepted-chain predecessor blocked during comparison. */
			missingPredecessorDigest?: Uint8Array;
			reason: string;
	  }>
	| Readonly<{
			formatVersion: 1;
			state: "FORKED";
			commonParentEntryBytes?: Uint8Array;
			childEntryBytes: readonly [Uint8Array, Uint8Array];
	  }>;

export type ResourceFenceAcceptedPolicyCheckV2 = (
	reference: ResourceFencePolicyReferenceV2,
) => boolean | Promise<boolean>;

export type ResourceFenceEntryResolverV2 = (
	digest: Uint8Array,
	options: { signal: AbortSignal },
) => Uint8Array | undefined | Promise<Uint8Array | undefined>;

export type ResourceFenceAdmissionOptionsV2 = Readonly<{
	/** Per-call cancellation; it does not abort the reducer lifecycle. */
	signal?: AbortSignal;
}>;

export type TrustedNetworkV2ResourceFenceReducerProperties = Readonly<{
	descriptor: NetworkDescriptorV2;
	expectedResourceId: Uint8Array;
	expectedGid: string;
	resolveFenceEntry: ResourceFenceEntryResolverV2;
	resolveEntryV0: BoundedEntryV0CausalReachabilityResolver;
	causalLimits?: BoundedEntryV0CausalReachabilityLimits;
	causalTimeoutMs?: number;
	operationTimeoutMs?: number;
	maxPending?: number;
	maxPendingBytes?: number;
	maxFenceAncestrySteps?: number;
	signal?: AbortSignal;
}>;

type UnavailableComparisonV2 = {
	candidate: ValidatedResourceFenceV2;
	missingCids: string[];
	missingPredecessorDigest?: Uint8Array;
	reason: string;
};

type EvaluationV2 =
	| { status: "accept" }
	| { status: "duplicate"; reason?: string }
	| { status: "pending"; reason: string }
	| { status: "reject"; reason: string }
	| {
			status: "unavailable";
			missingCids: string[];
			missingPredecessorDigest?: Uint8Array;
			reason: string;
	  }
	| {
			status: "fork";
			commonParent?: ValidatedResourceFenceV2;
			acceptedChild: ValidatedResourceFenceV2;
			candidateChild: ValidatedResourceFenceV2;
	  };

type NamedPredecessorResolutionV2 =
	| { status: "found"; predecessor: ValidatedResourceFenceV2 }
	| { status: "missing"; reason: string }
	| { status: "unavailable"; reason: string }
	| { status: "reject"; reason: string };

type FenceChainResolutionV2 =
	| { status: "found"; fence: ValidatedResourceFenceV2 }
	| {
			status: "unavailable";
			missingCids: string[];
			missingPredecessorDigest?: Uint8Array;
			reason: string;
	  };

type PreparedInternalV2 = {
	owner: TrustedNetworkV2ResourceFenceReducer;
	snapshot: ValidatedResourceFenceV2;
};

const PREPARED = new WeakMap<object, PreparedInternalV2>();

const captureFenceEntryBytes = (entryBytes: Uint8Array): Uint8Array => {
	let byteLength: number;
	try {
		byteLength = exactByteLength(entryBytes);
	} catch {
		throw new Error("Resource fence must use canonical EntryV0 bytes");
	}
	if (
		byteLength < 1 ||
		byteLength > TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES
	) {
		throw new Error(
			`Resource fence entry must contain 1-${TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES} bytes`,
		);
	}
	return copyBytesWithLength(entryBytes, byteLength);
};

const capturePolicy = (
	policy: AcceptedResourceFencePolicyV2,
): AcceptedResourceFencePolicyV2 => {
	let sequence: unknown;
	let digest: unknown;
	try {
		sequence = policy?.sequence;
		digest = policy?.digest;
	} catch {
		throw new Error("Accepted policy reference is invalid");
	}
	if (
		policy === null ||
		typeof policy !== "object" ||
		typeof sequence !== "bigint" ||
		sequence < 0n ||
		sequence > 0xffffffffffffffffn
	) {
		throw new Error("Accepted policy sequence must be a u64");
	}
	return {
		sequence,
		digest: captureBytes32(digest as Uint8Array, "Accepted policy digest"),
	};
};

const policyReference = (
	snapshot: ValidatedResourceFenceV2,
): ResourceFencePolicyReferenceV2 => ({
	sequence: snapshot.body.policySequence,
	digest: copyBytes(snapshot.body.policyDigest),
});

const policyMatches = (
	snapshot: ValidatedResourceFenceV2,
	policy: AcceptedResourceFencePolicyV2,
): boolean =>
	snapshot.body.policySequence === policy.sequence &&
	equals(snapshot.body.policyDigest, policy.digest);

const projectionFromSnapshot = (
	snapshot: ValidatedResourceFenceV2,
): ResourceFenceHeadProjectionV2 => ({
	sequence: snapshot.body.fenceSequence,
	digest: copyBytes(snapshot.digest),
	entryCid: snapshot.entryCid,
	previousFenceDigest: copyBytes(snapshot.body.previousFenceDigest),
	policy: policyReference(snapshot),
	contentEpoch: snapshot.body.contentEpoch,
	epochManifestDigest: copyBytes(snapshot.body.epochManifestDigest),
	causalFrontier: snapshot.directParents.map((parent) => ({
		cid: parent.cid,
		digest: copyBytes(parent.digest),
	})),
});

const forkProofFromSnapshot = (
	snapshot: ValidatedResourceFenceV2,
): ResourceFenceForkChildProofV2 => ({
	sequence: snapshot.body.fenceSequence,
	digest: copyBytes(snapshot.digest),
	entryCid: snapshot.entryCid,
	entryBytes: copyBytes(snapshot.entryBytes),
});

const compareForkProofs = (
	left: ResourceFenceForkChildProofV2,
	right: ResourceFenceForkChildProofV2,
): number => {
	const digestOrder = compare(left.digest, right.digest);
	return digestOrder === 0
		? compare(left.entryBytes, right.entryBytes)
		: digestOrder;
};

const copyForkProof = (
	proof: ResourceFenceForkChildProofV2,
): ResourceFenceForkChildProofV2 => ({
	sequence: proof.sequence,
	digest: copyBytes(proof.digest),
	entryCid: proof.entryCid,
	entryBytes: copyBytes(proof.entryBytes),
});

const copyHeadProjection = (
	head: ResourceFenceHeadProjectionV2,
): ResourceFenceHeadProjectionV2 => ({
	sequence: head.sequence,
	digest: copyBytes(head.digest),
	entryCid: head.entryCid,
	previousFenceDigest: copyBytes(head.previousFenceDigest),
	policy: {
		sequence: head.policy.sequence,
		digest: copyBytes(head.policy.digest),
	},
	contentEpoch: head.contentEpoch,
	epochManifestDigest: copyBytes(head.epochManifestDigest),
	causalFrontier: head.causalFrontier.map((parent) => ({
		cid: parent.cid,
		digest: copyBytes(parent.digest),
	})),
});

const copyForkEvidence = (
	evidence: ResourceFenceForkEvidenceV2,
): ResourceFenceForkEvidenceV2 => ({
	commonParent:
		evidence.commonParent.kind === "initial"
			? {
					kind: "initial",
					digest: copyBytes(evidence.commonParent.digest),
				}
			: {
					kind: "fence",
					head: copyHeadProjection(evidence.commonParent.head),
				},
	children: [
		copyForkProof(evidence.children[0]),
		copyForkProof(evidence.children[1]),
	],
});

const captureCanonicalCid = (cid: unknown): string => {
	if (
		typeof cid !== "string" ||
		cid.length < 1 ||
		cid.length > TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS
	) {
		throw new Error("Missing causal entry must use a bounded canonical CID");
	}
	let parsed: ReturnType<typeof cidifyString>;
	try {
		parsed = cidifyString(cid);
	} catch {
		throw new Error("Missing causal entry must use a canonical CID");
	}
	if (
		parsed.version !== 1 ||
		parsed.code !== codecMap.raw.code ||
		parsed.multihash.code !== defaultHasher.code ||
		parsed.multihash.digest.byteLength !== 32 ||
		stringifyCid(parsed) !== cid
	) {
		throw new Error(
			"Missing causal entry must use canonical CIDv1/raw/sha2-256",
		);
	}
	return cid;
};

const captureMissingCids = (cids: readonly string[]): string[] => {
	if (
		!Array.isArray(cids) ||
		cids.length > TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_MISSING_CIDS
	) {
		throw new Error(
			`Unavailable resource fence may retain at most ${TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_MISSING_CIDS} missing CIDs`,
		);
	}
	const captured = cids.map(captureCanonicalCid);
	const sorted = [...captured].sort();
	for (let index = 0; index < sorted.length; index++) {
		if (
			sorted[index] !== captured[index] ||
			(index > 0 && sorted[index] === sorted[index - 1])
		) {
			throw new Error(
				"Unavailable resource-fence missing CIDs must be sorted and unique",
			);
		}
	}
	return captured;
};

const captureOptionalFenceEntryBytes = (
	entryBytes: Uint8Array | undefined,
): Uint8Array | undefined =>
	entryBytes === undefined ? undefined : captureFenceEntryBytes(entryBytes);

const captureDurableState = (
	state: ResourceFenceReducerDurableStateV2,
): ResourceFenceReducerDurableStateV2 => {
	if (
		state === null ||
		typeof state !== "object" ||
		state.formatVersion !== 1
	) {
		throw new Error(
			"Unsupported TrustedNetwork v2 resource-fence state format",
		);
	}
	switch (state.state) {
		case "EMPTY":
			return { formatVersion: 1, state: "EMPTY" };
		case "ACTIVE":
			return {
				formatVersion: 1,
				state: "ACTIVE",
				acceptedHeadEntryBytes: captureFenceEntryBytes(
					state.acceptedHeadEntryBytes,
				),
				acceptedParentEntryBytes: captureOptionalFenceEntryBytes(
					state.acceptedParentEntryBytes,
				),
			};
		case "UNAVAILABLE": {
			if (
				typeof state.reason !== "string" ||
				state.reason.length < 1 ||
				state.reason.length > MAX_REASON_LENGTH
			) {
				throw new Error(
					`Unavailable reason must contain 1-${MAX_REASON_LENGTH} characters`,
				);
			}
			return {
				formatVersion: 1,
				state: "UNAVAILABLE",
				acceptedHeadEntryBytes: captureFenceEntryBytes(
					state.acceptedHeadEntryBytes,
				),
				acceptedParentEntryBytes: captureOptionalFenceEntryBytes(
					state.acceptedParentEntryBytes,
				),
				comparisonCandidateEntryBytes: captureFenceEntryBytes(
					state.comparisonCandidateEntryBytes,
				),
				missingCids: captureMissingCids(state.missingCids),
				missingPredecessorDigest:
					state.missingPredecessorDigest === undefined
						? undefined
						: captureBytes32(
								state.missingPredecessorDigest,
								"Unavailable predecessor digest",
							),
				reason: state.reason,
			};
		}
		case "FORKED":
			if (
				!Array.isArray(state.childEntryBytes) ||
				state.childEntryBytes.length !== 2
			) {
				throw new Error("Fork evidence must contain exactly two children");
			}
			return {
				formatVersion: 1,
				state: "FORKED",
				commonParentEntryBytes: captureOptionalFenceEntryBytes(
					state.commonParentEntryBytes,
				),
				childEntryBytes: [
					captureFenceEntryBytes(state.childEntryBytes[0]),
					captureFenceEntryBytes(state.childEntryBytes[1]),
				],
			};
		default:
			throw new Error("Unsupported TrustedNetwork v2 resource-fence state");
	}
};

export class TrustedNetworkV2ResourceFenceReducer {
	private readonly descriptor: NetworkDescriptorV2;
	private readonly expectedResourceId: Uint8Array;
	private readonly expectedGid: string;
	private readonly resolveFenceEntry: ResourceFenceEntryResolverV2;
	private readonly resolveEntryV0: BoundedEntryV0CausalReachabilityResolver;
	private readonly causalLimits: BoundedEntryV0CausalReachabilityLimits;
	private readonly causalTimeoutMs: number;
	private readonly operationTimeoutMs: number;
	private readonly maxPending: number;
	private readonly maxPendingBytes: number;
	private readonly maxFenceAncestrySteps: number;
	private readonly lifecycleController = new AbortController();
	private externalSignal?: AbortSignal;
	private externalAbortListener?: () => void;
	private acceptedHead?: ValidatedResourceFenceV2;
	private acceptedParent?: ValidatedResourceFenceV2;
	private unavailable?: UnavailableComparisonV2;
	private fork?: ResourceFenceForkEvidenceV2;
	private readonly pending = new Map<string, ValidatedResourceFenceV2>();
	private admissionTail: Promise<void> = Promise.resolve();
	private activeOperationSignal?: AbortSignal;

	constructor(properties: TrustedNetworkV2ResourceFenceReducerProperties) {
		assertNetworkDescriptorV2(properties.descriptor);
		if (typeof properties.expectedGid !== "string") {
			throw new Error("Expected resource gid must be a string");
		}
		if (typeof properties.resolveEntryV0 !== "function") {
			throw new Error("resolveEntryV0 must be a function");
		}
		if (typeof properties.resolveFenceEntry !== "function") {
			throw new Error("resolveFenceEntry must be a function");
		}
		this.descriptor = deserialize(
			serialize(properties.descriptor),
			NetworkDescriptorV2,
		);
		this.expectedResourceId = captureBytes32(
			properties.expectedResourceId,
			"Expected resource id",
		);
		this.expectedGid = properties.expectedGid;
		this.resolveFenceEntry = properties.resolveFenceEntry;
		this.resolveEntryV0 = properties.resolveEntryV0;
		this.causalLimits = this.captureCausalLimits(
			properties.causalLimits ?? DEFAULT_CAUSAL_LIMITS,
		);
		this.causalTimeoutMs =
			properties.causalTimeoutMs ?? DEFAULT_CAUSAL_TIMEOUT_MS;
		if (
			!Number.isSafeInteger(this.causalTimeoutMs) ||
			this.causalTimeoutMs < 1 ||
			this.causalTimeoutMs > MAX_TIMER_DELAY_MS
		) {
			throw new Error("causalTimeoutMs must be a positive safe timer delay");
		}
		this.operationTimeoutMs =
			properties.operationTimeoutMs ?? DEFAULT_CAUSAL_TIMEOUT_MS;
		if (
			!Number.isSafeInteger(this.operationTimeoutMs) ||
			this.operationTimeoutMs < 1 ||
			this.operationTimeoutMs > MAX_TIMER_DELAY_MS
		) {
			throw new Error("operationTimeoutMs must be a positive safe timer delay");
		}
		this.maxPending =
			properties.maxPending ?? TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_FENCES;
		if (
			!Number.isSafeInteger(this.maxPending) ||
			this.maxPending < 1 ||
			this.maxPending > TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_FENCES
		) {
			throw new Error(
				`maxPending must be between 1 and ${TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_FENCES}`,
			);
		}
		this.maxPendingBytes = properties.maxPendingBytes ?? MAX_PENDING_BYTES;
		if (
			!Number.isSafeInteger(this.maxPendingBytes) ||
			this.maxPendingBytes <
				TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ENTRY_BYTES +
					PENDING_ACCOUNTING_OVERHEAD ||
			this.maxPendingBytes > MAX_PENDING_BYTES
		) {
			throw new Error(
				`maxPendingBytes must retain one maximum-size resource fence and be no greater than ${MAX_PENDING_BYTES}`,
			);
		}
		this.maxFenceAncestrySteps =
			properties.maxFenceAncestrySteps ??
			TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ANCESTRY_STEPS;
		if (
			!Number.isSafeInteger(this.maxFenceAncestrySteps) ||
			this.maxFenceAncestrySteps < 1 ||
			this.maxFenceAncestrySteps >
				TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ANCESTRY_STEPS
		) {
			throw new Error(
				`maxFenceAncestrySteps must be between 1 and ${TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_ANCESTRY_STEPS}`,
			);
		}
		if (properties.signal?.aborted) {
			this.lifecycleController.abort();
		} else if (properties.signal !== undefined) {
			this.externalSignal = properties.signal;
			this.externalAbortListener = (): void => {
				this.lifecycleController.abort();
			};
			properties.signal.addEventListener("abort", this.externalAbortListener, {
				once: true,
			});
		}
	}

	get state(): "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED" | "HALTED" {
		if (this.lifecycleController.signal.aborted) return "HALTED";
		if (this.fork !== undefined) return "FORKED";
		if (this.unavailable !== undefined) return "UNAVAILABLE";
		return this.acceptedHead === undefined ? "EMPTY" : "ACTIVE";
	}

	get head(): ResourceFenceHeadProjectionV2 | undefined {
		return this.acceptedHead === undefined
			? undefined
			: projectionFromSnapshot(this.acceptedHead);
	}

	get pendingCount(): number {
		return this.pending.size + (this.unavailable === undefined ? 0 : 1);
	}

	get pendingBytes(): number {
		let total = this.unavailable?.candidate.accountedBytes ?? 0;
		for (const candidate of this.pending.values())
			total += candidate.accountedBytes;
		return total;
	}

	get pendingPolicyReferences(): ReadonlyArray<
		Readonly<{
			entryCid: string;
			policyReference: ResourceFencePolicyReferenceV2;
		}>
	> {
		const candidates = [...this.pending.values()];
		if (this.unavailable !== undefined)
			candidates.push(this.unavailable.candidate);
		return candidates
			.sort((left, right) =>
				left.entryCid < right.entryCid
					? -1
					: left.entryCid > right.entryCid
						? 1
						: 0,
			)
			.map((candidate) => ({
				entryCid: candidate.entryCid,
				policyReference: policyReference(candidate),
			}));
	}

	abort(): void {
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
		this.lifecycleController.abort();
	}

	get forkEvidence(): ResourceFenceForkEvidenceV2 | undefined {
		return this.fork === undefined ? undefined : copyForkEvidence(this.fork);
	}

	static async restore(
		properties: TrustedNetworkV2ResourceFenceReducerProperties & {
			durableState: ResourceFenceReducerDurableStateV2;
			isAcceptedPolicy: ResourceFenceAcceptedPolicyCheckV2;
		},
	): Promise<TrustedNetworkV2ResourceFenceReducer> {
		// Persistence adapters may reuse their read buffers. Capture the complete
		// state before authentication or policy lookup performs its first await.
		const durableState = captureDurableState(properties.durableState);
		if (typeof properties.isAcceptedPolicy !== "function") {
			throw new Error("isAcceptedPolicy must be a function");
		}
		const reducer = new TrustedNetworkV2ResourceFenceReducer(properties);
		const authenticate = (entryBytes: Uint8Array) =>
			reducer.authenticateCaptured(entryBytes);
		const requireAcceptedPolicy = async (
			snapshot: ValidatedResourceFenceV2,
		): Promise<void> => {
			const reference = policyReference(snapshot);
			let accepted: boolean;
			try {
				accepted = await properties.isAcceptedPolicy(reference);
			} catch (error) {
				throw new Error(
					`Resource-fence policy validation failed: ${validationMessage(error)}`,
				);
			}
			if (!accepted) {
				throw new Error(
					"Durable resource fence does not reference an accepted policy prefix",
				);
			}
		};
		const restoreActive = async (
			headEntryBytes: Uint8Array,
			parentEntryBytes?: Uint8Array,
		): Promise<void> => {
			const head = await authenticate(headEntryBytes);
			await requireAcceptedPolicy(head);
			if (head.body.fenceSequence === 0n) {
				if (parentEntryBytes !== undefined) {
					throw new Error("Initial resource fence cannot retain a predecessor");
				}
				reducer.acceptedHead = copySnapshot(head);
				return;
			}
			if (parentEntryBytes === undefined) {
				throw new Error(
					"Non-initial resource fence is missing its predecessor",
				);
			}
			const parent = await authenticate(parentEntryBytes);
			await requireAcceptedPolicy(parent);
			const causal = await reducer.validateCausalTransition(parent, head);
			if (causal.status !== "found") {
				throw new Error(
					`Durable resource-fence head is not causally linked to its predecessor: ${causal.reason}`,
				);
			}
			reducer.acceptedParent = copySnapshot(parent);
			reducer.acceptedHead = copySnapshot(head);
		};

		try {
			switch (durableState.state) {
				case "EMPTY":
					return reducer;
				case "ACTIVE":
					await restoreActive(
						durableState.acceptedHeadEntryBytes,
						durableState.acceptedParentEntryBytes,
					);
					return reducer;
				case "UNAVAILABLE": {
					await restoreActive(
						durableState.acceptedHeadEntryBytes,
						durableState.acceptedParentEntryBytes,
					);
					const candidate = await authenticate(
						durableState.comparisonCandidateEntryBytes,
					);
					await requireAcceptedPolicy(candidate);
					if (candidate.entryCid === reducer.acceptedHead!.entryCid) {
						throw new Error(
							"Unavailable comparison candidate must differ from the accepted head",
						);
					}
					reducer.unavailable = {
						candidate: copySnapshot(candidate),
						missingCids: [...durableState.missingCids],
						missingPredecessorDigest:
							durableState.missingPredecessorDigest === undefined
								? undefined
								: copyBytes(durableState.missingPredecessorDigest),
						reason: durableState.reason,
					};
					return reducer;
				}
				case "FORKED": {
					const [first, second] = await Promise.all([
						authenticate(durableState.childEntryBytes[0]),
						authenticate(durableState.childEntryBytes[1]),
					]);
					await requireAcceptedPolicy(first);
					await requireAcceptedPolicy(second);
					if (
						first.entryCid === second.entryCid ||
						first.body.fenceSequence !== second.body.fenceSequence ||
						!equals(
							first.body.previousFenceDigest,
							second.body.previousFenceDigest,
						)
					) {
						throw new Error(
							"Fork evidence must contain distinct sibling resource fences",
						);
					}
					let commonParent: ResourceFenceForkParentV2;
					if (first.body.fenceSequence === 0n) {
						if (durableState.commonParentEntryBytes !== undefined) {
							throw new Error(
								"Initial resource-fence fork cannot retain a predecessor",
							);
						}
						commonParent = {
							kind: "initial",
							digest: new Uint8Array(32),
						};
					} else {
						if (durableState.commonParentEntryBytes === undefined) {
							throw new Error(
								"Non-initial resource-fence fork is missing its common parent",
							);
						}
						const parent = await authenticate(
							durableState.commonParentEntryBytes,
						);
						await requireAcceptedPolicy(parent);
						const [firstCausal, secondCausal] = await Promise.all([
							reducer.validateCausalTransition(parent, first),
							reducer.validateCausalTransition(parent, second),
						]);
						if (
							firstCausal.status !== "found" ||
							secondCausal.status !== "found"
						) {
							throw new Error(
								"Durable resource-fence fork is not causally linked to its common parent",
							);
						}
						reducer.acceptedHead = copySnapshot(parent);
						commonParent = {
							kind: "fence",
							head: projectionFromSnapshot(parent),
						};
					}
					const children = [
						forkProofFromSnapshot(first),
						forkProofFromSnapshot(second),
					].sort(compareForkProofs) as [
						ResourceFenceForkChildProofV2,
						ResourceFenceForkChildProofV2,
					];
					if (
						compareForkProofs(
							forkProofFromSnapshot(first),
							forkProofFromSnapshot(second),
						) > 0
					) {
						throw new Error(
							"Durable resource-fence fork children are not canonical",
						);
					}
					reducer.fork = { commonParent, children };
					return reducer;
				}
			}
		} catch (error) {
			reducer.abort();
			throw error;
		}
	}

	exportDurableState(): ResourceFenceReducerDurableStateV2 {
		if (this.fork !== undefined) {
			return {
				formatVersion: 1,
				state: "FORKED",
				commonParentEntryBytes:
					this.fork.commonParent.kind === "fence" &&
					this.acceptedHead !== undefined
						? copyBytes(this.acceptedHead.entryBytes)
						: undefined,
				childEntryBytes: [
					copyBytes(this.fork.children[0].entryBytes),
					copyBytes(this.fork.children[1].entryBytes),
				],
			};
		}
		if (this.unavailable !== undefined) {
			if (this.acceptedHead === undefined) {
				throw new Error("Unavailable reducer is missing its accepted head");
			}
			return {
				formatVersion: 1,
				state: "UNAVAILABLE",
				acceptedHeadEntryBytes: copyBytes(this.acceptedHead.entryBytes),
				acceptedParentEntryBytes:
					this.acceptedParent === undefined
						? undefined
						: copyBytes(this.acceptedParent.entryBytes),
				comparisonCandidateEntryBytes: copyBytes(
					this.unavailable.candidate.entryBytes,
				),
				missingCids: [...this.unavailable.missingCids],
				missingPredecessorDigest:
					this.unavailable.missingPredecessorDigest === undefined
						? undefined
						: copyBytes(this.unavailable.missingPredecessorDigest),
				reason: this.unavailable.reason,
			};
		}
		if (this.acceptedHead === undefined) {
			return { formatVersion: 1, state: "EMPTY" };
		}
		return {
			formatVersion: 1,
			state: "ACTIVE",
			acceptedHeadEntryBytes: copyBytes(this.acceptedHead.entryBytes),
			acceptedParentEntryBytes:
				this.acceptedParent === undefined
					? undefined
					: copyBytes(this.acceptedParent.entryBytes),
		};
	}

	async prepare(
		entryBytes: Uint8Array,
	): Promise<ResourceFencePreparationResultV2> {
		const terminal = this.terminalReason();
		if (terminal !== undefined) return { status: "halted", reason: terminal };
		let captured: Uint8Array;
		try {
			// Capture before authentication's first await so queued callers cannot
			// mutate or detach the authority-signed evidence under validation.
			captured = captureFenceEntryBytes(entryBytes);
		} catch (error) {
			return { status: "rejected", reason: validationMessage(error) };
		}
		let snapshot: ValidatedResourceFenceV2;
		try {
			snapshot = await this.authenticateCaptured(captured);
		} catch (error) {
			return { status: "rejected", reason: validationMessage(error) };
		}
		const afterAuthentication = this.terminalReason();
		if (afterAuthentication !== undefined) {
			return { status: "halted", reason: afterAuthentication };
		}
		const outward: PreparedResourceFenceCandidateV2 = Object.freeze({
			entryCid: snapshot.entryCid,
			policyReference: Object.freeze(policyReference(snapshot)),
		});
		PREPARED.set(outward, { owner: this, snapshot: copySnapshot(snapshot) });
		return { status: "prepared", candidate: outward };
	}

	ingestPrepared(
		candidate: PreparedResourceFenceCandidateV2,
		acceptedPolicy: AcceptedResourceFencePolicyV2,
		options?: ResourceFenceAdmissionOptionsV2,
	): Promise<ResourceFenceAdmissionResultV2> {
		const prepared =
			candidate !== null && typeof candidate === "object"
				? PREPARED.get(candidate)
				: undefined;
		let policy: AcceptedResourceFencePolicyV2;
		try {
			policy = capturePolicy(acceptedPolicy);
		} catch (error) {
			return this.enqueue(
				async () => this.result("rejected", validationMessage(error)),
				options,
			);
		}
		if (prepared === undefined || prepared.owner !== this) {
			return this.enqueue(
				async () =>
					this.result(
						"rejected",
						"Prepared resource fence does not belong to this reducer",
					),
				options,
			);
		}
		const snapshot = copySnapshot(prepared.snapshot);
		return this.enqueue(() => this.ingestSnapshot(snapshot, policy), options);
	}

	retryPending(
		entryCid: string,
		acceptedPolicy: AcceptedResourceFencePolicyV2,
		options?: ResourceFenceAdmissionOptionsV2,
	): Promise<ResourceFenceAdmissionResultV2> {
		let policy: AcceptedResourceFencePolicyV2;
		try {
			if (
				typeof entryCid !== "string" ||
				entryCid.length < 1 ||
				entryCid.length > TRUSTED_NETWORK_V2_MAX_CANONICAL_ENTRY_CID_CHARACTERS
			) {
				throw new Error("Pending resource fence CID is invalid");
			}
			policy = capturePolicy(acceptedPolicy);
		} catch (error) {
			return this.enqueue(
				async () => this.result("rejected", validationMessage(error)),
				options,
			);
		}
		return this.enqueue(() => this.retryPendingOne(entryCid, policy), options);
	}

	private async authenticateCaptured(
		entryBytes: Uint8Array,
	): Promise<ValidatedResourceFenceV2> {
		const authenticated = await authenticateResourceFenceEntryV2({
			entryBytes,
			descriptor: this.descriptor,
			expectedResourceId: this.expectedResourceId,
			expectedGid: this.expectedGid,
		});
		return snapshotFromAuthenticated(authenticated);
	}

	private terminalReason(): string | undefined {
		if (this.lifecycleController.signal.aborted) {
			return "Resource-fence reducer lifecycle is aborted";
		}
		if (this.fork !== undefined) {
			return "Resource-fence reducer is halted by authority equivocation";
		}
		return undefined;
	}

	private enqueue(
		operation: () => Promise<ResourceFenceAdmissionResultV2>,
		options?: ResourceFenceAdmissionOptionsV2,
	): Promise<ResourceFenceAdmissionResultV2> {
		const result = this.admissionTail.then(async () => {
			const terminal = this.terminalReason();
			if (terminal !== undefined) return this.result("halted", terminal);
			const operationController = new AbortController();
			const externalSignal = options?.signal;
			const abortOperation = (): void => operationController.abort();
			this.lifecycleController.signal.addEventListener(
				"abort",
				abortOperation,
				{ once: true },
			);
			externalSignal?.addEventListener("abort", abortOperation, { once: true });
			if (this.lifecycleController.signal.aborted || externalSignal?.aborted) {
				operationController.abort();
			}
			const timeout = setTimeout(abortOperation, this.operationTimeoutMs);
			this.activeOperationSignal = operationController.signal;
			try {
				if (operationController.signal.aborted) {
					return this.result(
						this.lifecycleController.signal.aborted ? "halted" : "unavailable",
						this.lifecycleController.signal.aborted
							? "Resource-fence reducer lifecycle is aborted"
							: "Resource-fence admission was cancelled",
					);
				}
				const admission = await operation();
				const afterOperation = this.terminalReason();
				return afterOperation !== undefined && admission.status !== "forked"
					? this.result("halted", afterOperation)
					: admission;
			} finally {
				clearTimeout(timeout);
				this.activeOperationSignal = undefined;
				this.lifecycleController.signal.removeEventListener(
					"abort",
					abortOperation,
				);
				externalSignal?.removeEventListener("abort", abortOperation);
			}
		});
		this.admissionTail = result.then(
			(): void => {},
			(_reason: unknown): void => {},
		);
		return result;
	}

	private fetchHints(): ResourceFenceFetchHintV2[] {
		const predecessors = new Map<string, Uint8Array>();
		for (const candidate of this.pending.values()) {
			if (candidate.body.fenceSequence === 0n) continue;
			predecessors.set(
				bytesKey(candidate.body.previousFenceDigest),
				candidate.body.previousFenceDigest,
			);
		}
		if (
			this.unavailable !== undefined &&
			this.unavailable.candidate.body.fenceSequence !== 0n
		) {
			predecessors.set(
				bytesKey(this.unavailable.candidate.body.previousFenceDigest),
				this.unavailable.candidate.body.previousFenceDigest,
			);
		}
		if (this.unavailable?.missingPredecessorDigest !== undefined) {
			predecessors.set(
				bytesKey(this.unavailable.missingPredecessorDigest),
				this.unavailable.missingPredecessorDigest,
			);
		}
		const hints: ResourceFenceFetchHintV2[] = [...predecessors.entries()]
			.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
			.map(([, digest]) => ({
				kind: "resource-fence-predecessor" as const,
				digest: copyBytes(digest),
			}));
		if (this.unavailable !== undefined) {
			for (const cid of this.unavailable.missingCids) {
				hints.push({ kind: "causal-entry", cid });
			}
		}
		return hints;
	}

	private result(
		status: ResourceFenceAdmissionStatusV2,
		reason?: string,
		evictedEntryCids?: string[],
		forkObservations?: ResourceFenceForkChildProofV2[],
	): ResourceFenceAdmissionResultV2 {
		return {
			status,
			reason,
			head: this.head,
			forkObservations:
				forkObservations === undefined
					? undefined
					: forkObservations.map(copyForkProof),
			fetchHints: this.fetchHints(),
			pendingCount: this.pendingCount,
			pendingBytes: this.pendingBytes,
			evictedEntryCids:
				evictedEntryCids === undefined ? undefined : [...evictedEntryCids],
		};
	}

	private assertDirectTransition(
		parent: ValidatedResourceFenceV2,
		child: ValidatedResourceFenceV2,
	): void {
		if (
			child.body.fenceSequence !== parent.body.fenceSequence + 1n ||
			!equals(child.body.previousFenceDigest, parent.digest)
		) {
			throw new Error(
				"Resource fence is not the exact sequence successor of its predecessor",
			);
		}
		if (child.body.policySequence < parent.body.policySequence) {
			throw new Error("Resource-fence policy sequence cannot decrease");
		}
		if (
			child.body.policySequence === parent.body.policySequence &&
			!equals(child.body.policyDigest, parent.body.policyDigest)
		) {
			throw new Error(
				"Resource fence cannot change policy digest without advancing policy sequence",
			);
		}
		if (child.body.contentEpoch < parent.body.contentEpoch) {
			throw new Error("Resource-fence content epoch cannot decrease");
		}
	}

	private async resolveNamedPredecessor(
		candidate: ValidatedResourceFenceV2,
	): Promise<NamedPredecessorResolutionV2> {
		if (candidate.body.fenceSequence === 0n) {
			return {
				status: "reject",
				reason: "Initial resource fence has no named predecessor",
			};
		}
		const digest = copyBytes(candidate.body.previousFenceDigest);
		const attempt = new AbortController();
		let timedOut = false;
		const abortFromLifecycle = (): void => attempt.abort();
		const operationSignal = this.activeOperationSignal;
		const abortFromOperation = (): void => attempt.abort();
		this.lifecycleController.signal.addEventListener(
			"abort",
			abortFromLifecycle,
			{ once: true },
		);
		operationSignal?.addEventListener("abort", abortFromOperation, {
			once: true,
		});
		if (operationSignal?.aborted) attempt.abort();
		const timeout = setTimeout(() => {
			timedOut = true;
			attempt.abort();
		}, this.causalTimeoutMs);
		const unavailable = (): NamedPredecessorResolutionV2 => ({
			status: "unavailable",
			reason: timedOut
				? `Resource-fence predecessor resolver timed out after ${this.causalTimeoutMs} ms`
				: "Resource-fence predecessor resolver was aborted",
		});
		let rejectOnAbort: (() => void) | undefined;
		const abortPromise = new Promise<never>((_resolve, reject) => {
			rejectOnAbort = (): void => reject(new Error("resolver-aborted"));
			attempt.signal.addEventListener("abort", rejectOnAbort, { once: true });
			if (attempt.signal.aborted) rejectOnAbort();
		});
		const resolution = Promise.resolve().then(async () => {
			if (attempt.signal.aborted) throw new Error("resolver-aborted");
			const supplied = await this.resolveFenceEntry(copyBytes(digest), {
				signal: attempt.signal,
			});
			if (attempt.signal.aborted) throw new Error("resolver-aborted");
			if (supplied === undefined) return undefined;
			const captured = captureFenceEntryBytes(supplied);
			if (attempt.signal.aborted) throw new Error("resolver-aborted");
			return this.authenticateCaptured(captured);
		});
		void resolution.then(
			(): void => undefined,
			(): void => undefined,
		);
		try {
			let predecessor: ValidatedResourceFenceV2 | undefined;
			try {
				predecessor = await Promise.race([resolution, abortPromise]);
			} catch (error) {
				if (attempt.signal.aborted) return unavailable();
				return {
					status: "unavailable",
					reason: boundedReason(
						`Resource-fence predecessor dependency is unavailable: ${validationMessage(error)}`,
					),
				};
			}
			if (predecessor === undefined) {
				return {
					status: "missing",
					reason: "Resource-fence predecessor is missing",
				};
			}
			if (!equals(predecessor.digest, digest)) {
				return {
					status: "unavailable",
					reason:
						"Resource-fence predecessor resolver returned the wrong digest",
				};
			}
			try {
				this.assertDirectTransition(predecessor, candidate);
			} catch (error) {
				return { status: "reject", reason: validationMessage(error) };
			}
			return { status: "found", predecessor };
		} finally {
			clearTimeout(timeout);
			this.lifecycleController.signal.removeEventListener(
				"abort",
				abortFromLifecycle,
			);
			operationSignal?.removeEventListener("abort", abortFromOperation);
			if (rejectOnAbort !== undefined) {
				attempt.signal.removeEventListener("abort", rejectOnAbort);
			}
		}
	}

	private async causalRelation(
		ancestor: ValidatedResourceFenceV2,
		descendant: ValidatedResourceFenceV2,
	) {
		const attempt = new AbortController();
		let timedOut = false;
		const abortFromLifecycle = (): void => attempt.abort();
		const operationSignal = this.activeOperationSignal;
		const abortFromOperation = (): void => attempt.abort();
		this.lifecycleController.signal.addEventListener(
			"abort",
			abortFromLifecycle,
			{ once: true },
		);
		operationSignal?.addEventListener("abort", abortFromOperation, {
			once: true,
		});
		if (operationSignal?.aborted) attempt.abort();
		const timeout = setTimeout(() => {
			timedOut = true;
			attempt.abort();
		}, this.causalTimeoutMs);
		try {
			const result = await checkBoundedEntryV0CausalReachability({
				ancestorCid: ancestor.entryCid,
				descendant: {
					cid: descendant.entryCid,
					bytes: descendant.entryBytes,
				},
				resolve: this.resolveEntryV0,
				limits: this.causalLimits,
				signal: attempt.signal,
			});
			return { result, timedOut };
		} finally {
			clearTimeout(timeout);
			this.lifecycleController.signal.removeEventListener(
				"abort",
				abortFromLifecycle,
			);
			operationSignal?.removeEventListener("abort", abortFromOperation);
		}
	}

	private relationEvaluation(
		relation: Awaited<ReturnType<typeof this.causalRelation>>,
		success: "accept" | "duplicate",
	): EvaluationV2 {
		if (relation.result.status === "ancestor") return { status: success };
		if (relation.result.status === "not-ancestor") {
			return {
				status: "reject",
				reason: "Resource fence does not causally descend from its predecessor",
			};
		}
		const allMissing =
			relation.result.status === "incomplete"
				? [...relation.result.missingCids].sort()
				: [];
		const missingCids = allMissing.slice(
			0,
			TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_MISSING_CIDS,
		);
		return {
			status: "unavailable",
			missingCids,
			reason: boundedReason(
				relation.timedOut
					? `Resource-fence causal ancestry timed out after ${this.causalTimeoutMs} ms`
					: relation.result.status === "capacity"
						? "Resource-fence causal ancestry exceeded configured capacity"
						: allMissing.length > missingCids.length
							? "Resource-fence causal ancestry is unavailable; missing hints were truncated"
							: "Resource-fence causal ancestry is unavailable",
			),
		};
	}

	private async predecessorOf(
		child: ValidatedResourceFenceV2,
		preferRetainedAcceptedParent: boolean,
	): Promise<NamedPredecessorResolutionV2> {
		if (
			preferRetainedAcceptedParent &&
			this.acceptedHead?.entryCid === child.entryCid &&
			this.acceptedParent !== undefined &&
			equals(child.body.previousFenceDigest, this.acceptedParent.digest)
		) {
			return {
				status: "found",
				predecessor: copySnapshot(this.acceptedParent),
			};
		}
		return this.resolveNamedPredecessor(child);
	}

	private async validateCausalTransition(
		parent: ValidatedResourceFenceV2,
		child: ValidatedResourceFenceV2,
	): Promise<FenceChainResolutionV2> {
		let transitionError: string | undefined;
		try {
			this.assertDirectTransition(parent, child);
		} catch (error) {
			transitionError = validationMessage(error);
		}
		if (transitionError !== undefined) {
			return {
				status: "unavailable",
				missingCids: [],
				reason: `Accepted resource-fence ancestry is invalid: ${transitionError}`,
			};
		}
		const relation = await this.causalRelation(parent, child);
		const evaluated = this.relationEvaluation(relation, "duplicate");
		if (evaluated.status === "duplicate") {
			return { status: "found", fence: copySnapshot(parent) };
		}
		return {
			status: "unavailable",
			missingCids:
				evaluated.status === "unavailable" ? evaluated.missingCids : [],
			reason:
				evaluated.status === "unavailable"
					? evaluated.reason
					: "Accepted resource-fence ancestry is not causally linked",
		};
	}

	private async acceptedFenceAtSequence(
		sequence: bigint,
	): Promise<FenceChainResolutionV2> {
		if (
			this.acceptedHead === undefined ||
			sequence > this.acceptedHead.body.fenceSequence
		) {
			return {
				status: "unavailable",
				missingCids: [],
				reason: "Requested accepted resource-fence sequence is unavailable",
			};
		}
		let cursor = copySnapshot(this.acceptedHead);
		let steps = 0;
		while (cursor.body.fenceSequence > sequence) {
			if (steps++ >= this.maxFenceAncestrySteps) {
				return {
					status: "unavailable",
					missingCids: [],
					reason:
						"Accepted resource-fence ancestry exceeded configured step capacity",
				};
			}
			const resolved = await this.predecessorOf(cursor, steps === 1);
			if (resolved.status !== "found") {
				return {
					status: "unavailable",
					missingCids: [],
					missingPredecessorDigest: copyBytes(cursor.body.previousFenceDigest),
					reason: `Accepted resource-fence ancestry is unavailable: ${resolved.reason}`,
				};
			}
			const causal = await this.validateCausalTransition(
				resolved.predecessor,
				cursor,
			);
			if (causal.status !== "found") return causal;
			cursor = causal.fence;
		}
		return { status: "found", fence: cursor };
	}

	private async compareAtAcceptedSequence(
		candidate: ValidatedResourceFenceV2,
		acceptedAtSequence: ValidatedResourceFenceV2,
	): Promise<EvaluationV2> {
		if (candidate.entryCid === acceptedAtSequence.entryCid) {
			return {
				status: "duplicate",
				reason:
					"Stale accepted resource fence cannot roll back the current head",
			};
		}
		if (acceptedAtSequence.body.fenceSequence === 0n) {
			return {
				status: "fork",
				acceptedChild: acceptedAtSequence,
				candidateChild: candidate,
			};
		}
		if (
			!equals(
				candidate.body.previousFenceDigest,
				acceptedAtSequence.body.previousFenceDigest,
			)
		) {
			// Only the prepared candidate's policy reference is covered by the
			// caller's lease. Do not resolve and promote an unleased candidate-chain
			// ancestor into fork evidence; retain this candidate until that exact
			// predecessor is separately prepared and leased.
			return {
				status: "pending",
				reason:
					"Competing resource-fence predecessor must be separately admitted",
			};
		}
		const acceptedParentResolution = await this.predecessorOf(
			acceptedAtSequence,
			acceptedAtSequence.entryCid === this.acceptedHead?.entryCid,
		);
		if (acceptedParentResolution.status !== "found") {
			return {
				status: "unavailable",
				missingCids: [],
				missingPredecessorDigest: copyBytes(
					acceptedAtSequence.body.previousFenceDigest,
				),
				reason: `Accepted resource-fence ancestry is unavailable: ${acceptedParentResolution.reason}`,
			};
		}
		const acceptedParentCausal = await this.validateCausalTransition(
			acceptedParentResolution.predecessor,
			acceptedAtSequence,
		);
		if (acceptedParentCausal.status !== "found") {
			return acceptedParentCausal;
		}
		try {
			this.assertDirectTransition(acceptedParentCausal.fence, candidate);
		} catch (error) {
			return { status: "reject", reason: validationMessage(error) };
		}
		const candidateRelation = await this.causalRelation(
			acceptedParentCausal.fence,
			candidate,
		);
		const candidateCausal = this.relationEvaluation(
			candidateRelation,
			"duplicate",
		);
		if (candidateCausal.status !== "duplicate") return candidateCausal;
		return {
			status: "fork",
			commonParent: acceptedParentCausal.fence,
			acceptedChild: acceptedAtSequence,
			candidateChild: candidate,
		};
	}

	private async evaluate(
		candidate: ValidatedResourceFenceV2,
	): Promise<EvaluationV2> {
		const head = this.acceptedHead;
		if (head === undefined) {
			if (candidate.body.fenceSequence === 0n) return { status: "accept" };
			const predecessor = await this.resolveNamedPredecessor(candidate);
			return predecessor.status === "reject"
				? { status: "reject", reason: predecessor.reason }
				: {
						status: "pending",
						reason:
							predecessor.status === "found"
								? "Authenticated predecessor is not yet accepted"
								: predecessor.reason,
					};
		}
		if (candidate.entryCid === head.entryCid) return { status: "duplicate" };

		if (
			candidate.body.fenceSequence === head.body.fenceSequence + 1n &&
			equals(candidate.body.previousFenceDigest, head.digest)
		) {
			try {
				this.assertDirectTransition(head, candidate);
			} catch (error) {
				return { status: "reject", reason: validationMessage(error) };
			}
			const relation = await this.causalRelation(head, candidate);
			if (this.lifecycleController.signal.aborted) {
				return {
					status: "unavailable",
					missingCids: [],
					reason: "Resource-fence reducer lifecycle is aborted",
				};
			}
			return this.relationEvaluation(relation, "accept");
		}

		if (candidate.body.fenceSequence <= head.body.fenceSequence) {
			const acceptedAtSequence = await this.acceptedFenceAtSequence(
				candidate.body.fenceSequence,
			);
			if (acceptedAtSequence.status !== "found") {
				return {
					status: "unavailable",
					missingCids: acceptedAtSequence.missingCids,
					missingPredecessorDigest: acceptedAtSequence.missingPredecessorDigest,
					reason: acceptedAtSequence.reason,
				};
			}
			return this.compareAtAcceptedSequence(
				candidate,
				acceptedAtSequence.fence,
			);
		}

		const predecessor = await this.resolveNamedPredecessor(candidate);
		if (predecessor.status === "reject") {
			return { status: "reject", reason: predecessor.reason };
		}
		return {
			status: "pending",
			reason:
				predecessor.status === "found"
					? "Authenticated predecessor is not yet accepted"
					: predecessor.reason,
		};
	}

	private project(candidate: ValidatedResourceFenceV2): void {
		this.acceptedParent =
			this.acceptedHead === undefined
				? undefined
				: copySnapshot(this.acceptedHead);
		this.acceptedHead = copySnapshot(candidate);
	}

	private setFork(
		evaluation: Extract<EvaluationV2, { status: "fork" }>,
	): ResourceFenceForkChildProofV2[] {
		const children = [
			forkProofFromSnapshot(evaluation.acceptedChild),
			forkProofFromSnapshot(evaluation.candidateChild),
		].sort(compareForkProofs) as [
			ResourceFenceForkChildProofV2,
			ResourceFenceForkChildProofV2,
		];
		const commonParentProjection: ResourceFenceForkParentV2 =
			evaluation.commonParent === undefined
				? { kind: "initial", digest: new Uint8Array(32) }
				: {
						kind: "fence",
						head: projectionFromSnapshot(evaluation.commonParent),
					};
		this.acceptedHead =
			evaluation.commonParent === undefined
				? undefined
				: copySnapshot(evaluation.commonParent);
		this.acceptedParent = undefined;
		this.unavailable = undefined;
		this.pending.clear();
		this.fork = {
			commonParent: commonParentProjection,
			children,
		};
		return children.map(copyForkProof);
	}

	private retainPending(candidate: ValidatedResourceFenceV2): {
		retained: boolean;
		evictedEntryCids: string[];
	} {
		const candidates = new Map(this.pending);
		candidates.set(candidate.entryCid, copySnapshot(candidate));
		const reservedCount = this.unavailable === undefined ? 0 : 1;
		const reservedBytes = this.unavailable?.candidate.accountedBytes ?? 0;
		const ordered = [...candidates.values()].sort((left, right) =>
			left.entryCid < right.entryCid
				? -1
				: left.entryCid > right.entryCid
					? 1
					: 0,
		);
		const retained = new Map<string, ValidatedResourceFenceV2>();
		let bytes = reservedBytes;
		for (const value of ordered) {
			if (
				retained.size + reservedCount >= this.maxPending ||
				bytes + value.accountedBytes > this.maxPendingBytes
			) {
				continue;
			}
			retained.set(value.entryCid, copySnapshot(value));
			bytes += value.accountedBytes;
		}
		const evictedEntryCids = [...candidates.keys()]
			.filter((entryCid) => !retained.has(entryCid))
			.sort();
		this.pending.clear();
		for (const [entryCid, value] of retained) this.pending.set(entryCid, value);
		return {
			retained: retained.has(candidate.entryCid),
			evictedEntryCids,
		};
	}

	private enterUnavailable(
		candidate: ValidatedResourceFenceV2,
		evaluation: Extract<EvaluationV2, { status: "unavailable" }>,
	): string[] {
		this.pending.delete(candidate.entryCid);
		this.unavailable = {
			candidate: copySnapshot(candidate),
			missingCids: evaluation.missingCids
				.map(captureCanonicalCid)
				.sort()
				.slice(0, TRUSTED_NETWORK_V2_MAX_RESOURCE_FENCE_MISSING_CIDS),
			missingPredecessorDigest:
				evaluation.missingPredecessorDigest === undefined
					? undefined
					: copyBytes(evaluation.missingPredecessorDigest),
			reason: boundedReason(evaluation.reason),
		};
		const evicted: string[] = [];
		const ordered = [...this.pending.values()].sort((left, right) =>
			left.entryCid < right.entryCid
				? -1
				: left.entryCid > right.entryCid
					? 1
					: 0,
		);
		this.pending.clear();
		let bytes = candidate.accountedBytes;
		for (const pending of ordered) {
			if (
				this.pending.size + 1 >= this.maxPending ||
				bytes + pending.accountedBytes > this.maxPendingBytes
			) {
				evicted.push(pending.entryCid);
				continue;
			}
			this.pending.set(pending.entryCid, pending);
			bytes += pending.accountedBytes;
		}
		return evicted.sort();
	}

	private async ingestSnapshot(
		candidate: ValidatedResourceFenceV2,
		acceptedPolicy: AcceptedResourceFencePolicyV2,
	): Promise<ResourceFenceAdmissionResultV2> {
		if (!policyMatches(candidate, acceptedPolicy)) {
			return this.result(
				"rejected",
				"Supplied accepted policy does not match the resource fence",
			);
		}
		if (this.unavailable !== undefined) {
			if (candidate.entryCid === this.unavailable.candidate.entryCid) {
				return this.retryUnavailableCandidate(candidate, acceptedPolicy);
			}
			const retention = this.retainPending(candidate);
			return this.result(
				retention.retained ? "unavailable" : "capacity",
				retention.retained
					? this.unavailable.reason
					: "Resource-fence pending capacity did not retain this candidate",
				retention.evictedEntryCids,
			);
		}
		const existing = this.pending.get(candidate.entryCid);
		if (existing !== undefined) this.pending.delete(candidate.entryCid);
		const evaluation = await this.evaluate(candidate);
		if (this.lifecycleController.signal.aborted) {
			return this.result(
				"halted",
				"Resource-fence reducer lifecycle is aborted",
			);
		}
		return this.applyEvaluation(candidate, evaluation);
	}

	private async retryPendingOne(
		entryCid: string,
		acceptedPolicy: AcceptedResourceFencePolicyV2,
	): Promise<ResourceFenceAdmissionResultV2> {
		if (
			this.unavailable !== undefined &&
			this.unavailable.candidate.entryCid === entryCid
		) {
			return this.retryUnavailableCandidate(
				this.unavailable.candidate,
				acceptedPolicy,
			);
		}
		const candidate = this.pending.get(entryCid);
		if (candidate === undefined) {
			return this.result(
				"rejected",
				"No pending resource fence has the requested CID",
			);
		}
		if (!policyMatches(candidate, acceptedPolicy)) {
			return this.result(
				"rejected",
				"Supplied accepted policy does not match the pending resource fence",
			);
		}
		if (this.unavailable !== undefined) {
			return this.result("unavailable", this.unavailable.reason);
		}
		this.pending.delete(entryCid);
		const evaluation = await this.evaluate(candidate);
		if (this.lifecycleController.signal.aborted) {
			return this.result(
				"halted",
				"Resource-fence reducer lifecycle is aborted",
			);
		}
		return this.applyEvaluation(candidate, evaluation);
	}

	private async retryUnavailableCandidate(
		candidate: ValidatedResourceFenceV2,
		acceptedPolicy: AcceptedResourceFencePolicyV2,
	): Promise<ResourceFenceAdmissionResultV2> {
		if (!policyMatches(candidate, acceptedPolicy)) {
			return this.result(
				"rejected",
				"Supplied accepted policy does not match the unavailable resource fence",
			);
		}
		const evaluation = await this.evaluate(candidate);
		if (this.lifecycleController.signal.aborted) {
			return this.result(
				"halted",
				"Resource-fence reducer lifecycle is aborted",
			);
		}
		this.unavailable = undefined;
		return this.applyEvaluation(candidate, evaluation);
	}

	private applyEvaluation(
		candidate: ValidatedResourceFenceV2,
		evaluation: EvaluationV2,
	): ResourceFenceAdmissionResultV2 {
		switch (evaluation.status) {
			case "accept":
				this.project(candidate);
				return this.result("accepted");
			case "duplicate":
				return this.result("duplicate", evaluation.reason);
			case "reject":
				return this.result("rejected", evaluation.reason);
			case "fork": {
				const observations = this.setFork(evaluation);
				return this.result(
					"forked",
					"Resource-fence authority signed competing children",
					undefined,
					observations,
				);
			}
			case "unavailable": {
				const evicted = this.enterUnavailable(candidate, evaluation);
				return this.result("unavailable", evaluation.reason, evicted);
			}
			case "pending": {
				const retention = this.retainPending(candidate);
				return this.result(
					retention.retained ? "pending" : "capacity",
					retention.retained
						? evaluation.reason
						: "Resource-fence pending capacity did not retain this candidate",
					retention.evictedEntryCids,
				);
			}
		}
	}

	private captureCausalLimits(
		limits: BoundedEntryV0CausalReachabilityLimits,
	): BoundedEntryV0CausalReachabilityLimits {
		const captured = { ...limits };
		const maxima = DEFAULT_CAUSAL_LIMITS;
		for (const key of Object.keys(maxima) as Array<keyof typeof maxima>) {
			const value = captured[key];
			if (!Number.isSafeInteger(value) || value < 1 || value > maxima[key]) {
				throw new Error(
					`${String(key)} must be a positive bounded safe integer`,
				);
			}
		}
		return captured;
	}
}
