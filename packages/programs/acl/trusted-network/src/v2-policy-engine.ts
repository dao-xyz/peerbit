import { deserialize, serialize } from "@dao-xyz/borsh";
import { PublicSignKey } from "@peerbit/crypto";
import { compare, equals } from "uint8arrays";
import { authenticateCapturedAuthorityEntryV0V2 } from "./v2-authority-entry.js";
import {
	NetworkDescriptorV2,
	PolicySnapshotBodyV2,
	PolicySubjectBindingV2,
	TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS,
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES,
	assertNetworkDescriptorV2,
	decodePolicySnapshotBodyV2,
	digestPolicySnapshotBodyV2,
} from "./v2.js";

/**
 * Internal policy reducer for the non-activatable TrustedNetwork v2 scaffold.
 *
 * The reducer intentionally is not exported from the package entry point. It
 * retains one accepted snapshot, a bounded pending working set, and (after
 * equivocation) two child proofs. Historical snapshots are supplied by the
 * resolver instead of being accumulated in memory.
 */

/** Internal protocol ceiling; this module is not part of the package root. */
export const TRUSTED_NETWORK_V2_MAX_PENDING_POLICIES = 64;
const DEFAULT_MAX_PENDING_POLICIES_V2 = TRUSTED_NETWORK_V2_MAX_PENDING_POLICIES;
const PENDING_POLICY_ACCOUNTING_OVERHEAD_V2 = 64;
const MAX_PENDING_POLICY_ACCOUNTED_BYTES_V2 =
	TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES * 2 +
	PENDING_POLICY_ACCOUNTING_OVERHEAD_V2;
const DEFAULT_MAX_PENDING_POLICY_BYTES_V2 =
	MAX_PENDING_POLICY_ACCOUNTED_BYTES_V2;
const DEFAULT_POLICY_RESOLUTION_TIMEOUT_MS_V2 = 10 * 1000;
const MAX_TIMER_DELAY_MS_V2 = 0x7fffffff;
const MAX_UNAVAILABLE_REASON_LENGTH_V2 = 512;

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

const isAbortSignalV2 = (value: unknown): value is AbortSignal => {
	if (value === null || typeof value !== "object") return false;
	try {
		const signal = value as AbortSignal;
		return (
			typeof signal.aborted === "boolean" &&
			typeof signal.addEventListener === "function" &&
			typeof signal.removeEventListener === "function"
		);
	} catch {
		return false;
	}
};

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

const compareKeys = (left: string, right: string): number =>
	left < right ? -1 : left > right ? 1 : 0;

const copyPublicKey = (key: PublicSignKey): PublicSignKey =>
	deserialize(serialize(key), PublicSignKey);

const publicKeyId = (key: PublicSignKey): string => bytesKey(serialize(key));

type ValidatedPolicySnapshotV2 = {
	body: PolicySnapshotBodyV2;
	digest: Uint8Array;
	digestKey: string;
	entryBytes: Uint8Array;
	accountedBytes: number;
};

const copyBinding = (binding: PolicySubjectBindingV2): PolicySubjectBindingV2 =>
	new PolicySubjectBindingV2({
		signingKey: copyPublicKey(binding.signingKey),
		roles: binding.roles,
	});

const copyBody = (body: PolicySnapshotBodyV2): PolicySnapshotBodyV2 =>
	new PolicySnapshotBodyV2({
		networkId: copyBytes(body.networkId),
		sequence: body.sequence,
		previousPolicyDigest: copyBytes(body.previousPolicyDigest),
		bindings: body.bindings.map(copyBinding),
	});

const copySnapshot = (
	snapshot: ValidatedPolicySnapshotV2,
): ValidatedPolicySnapshotV2 => ({
	body: copyBody(snapshot.body),
	digest: copyBytes(snapshot.digest),
	digestKey: snapshot.digestKey,
	entryBytes: copyBytes(snapshot.entryBytes),
	accountedBytes: snapshot.accountedBytes,
});

export type PolicySnapshotResolverV2 = (
	digest: Uint8Array,
	options: { signal: AbortSignal },
) => Uint8Array | undefined | Promise<Uint8Array | undefined>;

export type PolicyParentFetchHintV2 = {
	kind: "policy-parent";
	digest: Uint8Array;
};

export type PolicyHeadProjectionV2 = {
	sequence: bigint;
	digest: Uint8Array;
	bindings: PolicySubjectBindingV2[];
};

/** Internal read-only resolution used by the durable policy-prefix lease. */
export type AcceptedPolicyPrefixResolutionV2 =
	| {
			status: "resolved";
			policy: PolicyHeadProjectionV2;
			acceptedHead: PolicyHeadProjectionV2;
	  }
	| {
			status: "unavailable" | "rejected" | "halted";
			reason: string;
	  };

/** Per-call bounds for the internal accepted-prefix history walk. */
export type AcceptedPolicyPrefixTraversalV2 = {
	/** Maximum number of authenticated parent edges that may be resolved. */
	maxSteps?: number;
	/** Absolute wall-clock deadline, in milliseconds since the Unix epoch. */
	deadline?: number;
	/** Cancels this resolution without halting the reducer. */
	signal?: AbortSignal;
};

export type PolicyForkChildProofV2 = {
	sequence: bigint;
	digest: Uint8Array;
	entryBytes: Uint8Array;
};

export type PolicyForkEvidenceV2 = {
	commonParent: PolicyHeadProjectionV2;
	children: [PolicyForkChildProofV2, PolicyForkChildProofV2];
};

export type PolicyAdmissionStatusV2 =
	| "accepted"
	| "duplicate"
	| "pending"
	| "unavailable"
	| "capacity"
	| "rejected"
	| "forked"
	| "halted";

export type PolicyAdmissionResultV2 = {
	status: PolicyAdmissionStatusV2;
	reason?: string;
	head?: PolicyHeadProjectionV2;
	/**
	 * Authenticated direct children surfaced while entering or already in
	 * FORKED. The outer durable layer can persist these proofs without decoding
	 * or verifying them a second time. They are bounded by one admission plus the
	 * pending working set and are not accumulated by this reducer.
	 */
	forkObservations?: PolicyForkChildProofV2[];
	fetchHints: PolicyParentFetchHintV2[];
	pendingCount: number;
	pendingBytes: number;
	evictedPolicyDigests?: Uint8Array[];
};

export type PolicyReducerDurableStateV2 =
	| { formatVersion: 1; state: "EMPTY" }
	| {
			formatVersion: 1;
			state: "ACTIVE";
			acceptedHeadEntryBytes: Uint8Array;
	  }
	| {
			formatVersion: 1;
			state: "UNAVAILABLE";
			acceptedHeadEntryBytes: Uint8Array;
			comparisonCandidateEntryBytes: Uint8Array;
			acceptedAncestorDigest: Uint8Array;
			reason: string;
	  }
	| {
			formatVersion: 1;
			state: "FORKED";
			commonParentEntryBytes: Uint8Array;
			childEntryBytes: [Uint8Array, Uint8Array];
	  };

export type TrustedNetworkV2PolicyReducerProperties = {
	descriptor: NetworkDescriptorV2;
	resolvePolicyEntry: PolicySnapshotResolverV2;
	resolveTimeoutMs?: number;
	signal?: AbortSignal;
	maxPending?: number;
	maxPendingPolicyBytes?: number;
};

type PendingPolicySnapshotV2 = {
	snapshot: ValidatedPolicySnapshotV2;
	missingParentDigest: Uint8Array;
};

type UnavailablePolicyComparisonV2 = {
	acceptedAncestorDigest: Uint8Array;
	comparisonCandidate: ValidatedPolicySnapshotV2;
	reason: string;
};

type ParentResolutionV2 =
	| { status: "found"; parent: ValidatedPolicySnapshotV2 }
	| { status: "missing"; digest: Uint8Array }
	| { status: "unavailable"; digest: Uint8Array; reason: string }
	| { status: "reject"; digest: Uint8Array; reason: string };

type SnapshotResolutionCacheV2 = Map<
	string,
	Promise<ValidatedPolicySnapshotV2 | undefined>
>;

type EvaluationV2 =
	| { status: "accept" }
	| { status: "duplicate" }
	| { status: "missing"; digest: Uint8Array; reason?: string }
	| { status: "reject"; reason: string }
	| { status: "unavailable"; digest: Uint8Array; reason: string }
	| {
			status: "fork";
			commonParent: ValidatedPolicySnapshotV2;
			candidateChild: ValidatedPolicySnapshotV2;
			acceptedChild: ValidatedPolicySnapshotV2;
	  };

type PendingDrainOutcomeV2 =
	| { status: "accepted" }
	| { status: "forked"; forkObservations: PolicyForkChildProofV2[] }
	| { status: "halted" }
	| {
			status: "unavailable";
			retained: boolean;
			evictedPolicyDigests: Uint8Array[];
	  };

const validationMessage = (error: unknown): string =>
	error instanceof Error ? error.message : String(error);

const boundedUnavailableReason = (reason: string): string =>
	reason.slice(0, MAX_UNAVAILABLE_REASON_LENGTH_V2);

class PolicyDependencyUnavailableErrorV2 extends Error {
	constructor(message: string) {
		super(message);
		this.name = "PolicyDependencyUnavailableErrorV2";
	}
}

const capturePolicySnapshotEntryBytesV2 = (
	entryBytes: Uint8Array,
): Uint8Array => {
	let byteLength: number;
	try {
		byteLength = exactUint8ArrayByteLength(entryBytes);
	} catch {
		throw new Error("Policy snapshot must use canonical EntryV0 bytes");
	}
	if (byteLength === 0) {
		throw new Error("Policy snapshot must use EntryV0");
	}
	if (byteLength > TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES) {
		throw new Error(
			`Policy snapshot entry must contain 1-${TRUSTED_NETWORK_V2_MAX_POLICY_ENTRY_BYTES} bytes`,
		);
	}

	// Apply the protocol byte ceiling before this first copy, decode, or crypto
	// operation. The retained copy also prevents caller mutation during awaits.
	return copyBytesWithLength(entryBytes, byteLength);
};

const authenticateCapturedPolicySnapshotEntryV2 = async (
	canonicalEntryBytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
): Promise<ValidatedPolicySnapshotV2> => {
	const authenticated = await authenticateCapturedAuthorityEntryV0V2(
		canonicalEntryBytes,
		descriptor,
	);
	const canonicalPayload = authenticated.payloadBytes;
	const body = decodePolicySnapshotBodyV2(canonicalPayload, descriptor);
	const digest = digestPolicySnapshotBodyV2(body);
	return {
		body: copyBody(body),
		digest: copyBytes(digest),
		digestKey: bytesKey(digest),
		entryBytes: canonicalEntryBytes,
		accountedBytes:
			canonicalEntryBytes.byteLength +
			serialize(body).byteLength +
			PENDING_POLICY_ACCOUNTING_OVERHEAD_V2,
	};
};

export const authenticatePolicySnapshotEntryV2 = async (
	entryBytes: Uint8Array,
	descriptor: NetworkDescriptorV2,
): Promise<ValidatedPolicySnapshotV2> => {
	assertNetworkDescriptorV2(descriptor);
	return authenticateCapturedPolicySnapshotEntryV2(
		capturePolicySnapshotEntryBytesV2(entryBytes),
		descriptor,
	);
};

const projectionFromSnapshot = (
	snapshot: ValidatedPolicySnapshotV2,
): PolicyHeadProjectionV2 => ({
	sequence: snapshot.body.sequence,
	digest: copyBytes(snapshot.digest),
	bindings: snapshot.body.bindings.map(copyBinding),
});

const forkProofFromSnapshot = (
	snapshot: ValidatedPolicySnapshotV2,
): PolicyForkChildProofV2 => ({
	sequence: snapshot.body.sequence,
	digest: copyBytes(snapshot.digest),
	entryBytes: copyBytes(snapshot.entryBytes),
});

const copyForkChildProof = (
	proof: PolicyForkChildProofV2,
): PolicyForkChildProofV2 => ({
	sequence: proof.sequence,
	digest: copyBytes(proof.digest),
	entryBytes: copyBytes(proof.entryBytes),
});

const compareForkChildProofs = (
	left: PolicyForkChildProofV2,
	right: PolicyForkChildProofV2,
): number => {
	const digestOrder = compare(left.digest, right.digest);
	return digestOrder === 0
		? compare(left.entryBytes, right.entryBytes)
		: digestOrder;
};

const copyProjection = (
	projection: PolicyHeadProjectionV2,
): PolicyHeadProjectionV2 => ({
	sequence: projection.sequence,
	digest: copyBytes(projection.digest),
	bindings: projection.bindings.map(copyBinding),
});

const copyForkEvidence = (
	evidence: PolicyForkEvidenceV2,
): PolicyForkEvidenceV2 => ({
	commonParent: copyProjection(evidence.commonParent),
	children: [
		copyForkChildProof(evidence.children[0]),
		copyForkChildProof(evidence.children[1]),
	],
});

const captureDurableStateV2 = (
	durableState: PolicyReducerDurableStateV2,
): PolicyReducerDurableStateV2 => {
	if (
		durableState === null ||
		typeof durableState !== "object" ||
		durableState.formatVersion !== 1
	) {
		throw new Error("Unsupported TrustedNetwork v2 reducer state format");
	}

	switch (durableState.state) {
		case "EMPTY":
			return { formatVersion: 1, state: "EMPTY" };
		case "ACTIVE":
			return {
				formatVersion: 1,
				state: "ACTIVE",
				acceptedHeadEntryBytes: capturePolicySnapshotEntryBytesV2(
					durableState.acceptedHeadEntryBytes,
				),
			};
		case "UNAVAILABLE": {
			if (
				!hasExactUint8ArrayByteLength(durableState.acceptedAncestorDigest, 32)
			) {
				throw new Error(
					"Unavailable accepted ancestor digest must contain exactly 32 bytes",
				);
			}
			if (
				typeof durableState.reason !== "string" ||
				durableState.reason.length === 0 ||
				durableState.reason.length > MAX_UNAVAILABLE_REASON_LENGTH_V2
			) {
				throw new Error(
					`Unavailable reason must contain 1-${MAX_UNAVAILABLE_REASON_LENGTH_V2} characters`,
				);
			}
			return {
				formatVersion: 1,
				state: "UNAVAILABLE",
				acceptedHeadEntryBytes: capturePolicySnapshotEntryBytesV2(
					durableState.acceptedHeadEntryBytes,
				),
				comparisonCandidateEntryBytes: capturePolicySnapshotEntryBytesV2(
					durableState.comparisonCandidateEntryBytes,
				),
				acceptedAncestorDigest: copyBytes(durableState.acceptedAncestorDigest),
				reason: durableState.reason,
			};
		}
		case "FORKED":
			if (
				!Array.isArray(durableState.childEntryBytes) ||
				durableState.childEntryBytes.length !== 2
			) {
				throw new Error(
					"Forked reducer state must contain exactly two children",
				);
			}
			return {
				formatVersion: 1,
				state: "FORKED",
				commonParentEntryBytes: capturePolicySnapshotEntryBytesV2(
					durableState.commonParentEntryBytes,
				),
				childEntryBytes: [
					capturePolicySnapshotEntryBytesV2(durableState.childEntryBytes[0]),
					capturePolicySnapshotEntryBytesV2(durableState.childEntryBytes[1]),
				],
			};
		default:
			throw new Error("Unsupported TrustedNetwork v2 reducer state");
	}
};

export class TrustedNetworkV2PolicyReducer {
	private readonly descriptor: NetworkDescriptorV2;
	private readonly resolvePolicyEntry: PolicySnapshotResolverV2;
	private readonly resolveTimeoutMs: number;
	private readonly maxPending: number;
	private readonly maxPendingPolicyBytes: number;
	private readonly lifecycleController = new AbortController();
	private externalSignal?: AbortSignal;
	private externalAbortListener?: () => void;
	private acceptedHead?: ValidatedPolicySnapshotV2;
	private projectedRoles = new Map<string, number>();
	private readonly pending = new Map<string, PendingPolicySnapshotV2>();
	// Recovery is bound to this exact candidate. Unrelated admissions may enter
	// the bounded pending set but can never restore ACTIVE authorization.
	private unavailable?: UnavailablePolicyComparisonV2;
	private fork?: PolicyForkEvidenceV2;
	private admissionTail: Promise<void> = Promise.resolve();

	constructor(properties: TrustedNetworkV2PolicyReducerProperties) {
		assertNetworkDescriptorV2(properties.descriptor);
		const maxPending = properties.maxPending ?? DEFAULT_MAX_PENDING_POLICIES_V2;
		if (
			!Number.isSafeInteger(maxPending) ||
			maxPending < 1 ||
			maxPending > TRUSTED_NETWORK_V2_MAX_PENDING_POLICIES
		) {
			throw new Error(
				`maxPending must be a positive safe integer no greater than ${TRUSTED_NETWORK_V2_MAX_PENDING_POLICIES}`,
			);
		}
		const maxPendingPolicyBytes =
			properties.maxPendingPolicyBytes ?? DEFAULT_MAX_PENDING_POLICY_BYTES_V2;
		if (
			!Number.isSafeInteger(maxPendingPolicyBytes) ||
			maxPendingPolicyBytes < 1 ||
			maxPendingPolicyBytes > MAX_PENDING_POLICY_ACCOUNTED_BYTES_V2
		) {
			throw new Error(
				`maxPendingPolicyBytes must be a positive safe integer no greater than ${MAX_PENDING_POLICY_ACCOUNTED_BYTES_V2}`,
			);
		}
		const resolveTimeoutMs =
			properties.resolveTimeoutMs ?? DEFAULT_POLICY_RESOLUTION_TIMEOUT_MS_V2;
		if (
			!Number.isSafeInteger(resolveTimeoutMs) ||
			resolveTimeoutMs < 1 ||
			resolveTimeoutMs > MAX_TIMER_DELAY_MS_V2
		) {
			throw new Error(
				`resolveTimeoutMs must be a positive safe integer no greater than ${MAX_TIMER_DELAY_MS_V2}`,
			);
		}
		this.descriptor = deserialize(
			serialize(properties.descriptor),
			NetworkDescriptorV2,
		);
		this.resolvePolicyEntry = properties.resolvePolicyEntry;
		this.resolveTimeoutMs = resolveTimeoutMs;
		this.maxPending = maxPending;
		this.maxPendingPolicyBytes = maxPendingPolicyBytes;

		if (properties.signal?.aborted) {
			this.lifecycleController.abort();
		} else if (properties.signal !== undefined) {
			this.externalSignal = properties.signal;
			this.externalAbortListener = (): void => {
				this.externalSignal = undefined;
				this.externalAbortListener = undefined;
				this.lifecycleController.abort();
			};
			this.externalSignal.addEventListener(
				"abort",
				this.externalAbortListener,
				{ once: true },
			);
		}
	}

	static async restore(
		properties: TrustedNetworkV2PolicyReducerProperties & {
			durableState: PolicyReducerDurableStateV2;
		},
	): Promise<TrustedNetworkV2PolicyReducer> {
		// Capture the complete checkpoint before the first await. Persistence
		// adapters commonly reuse read buffers, and mutation during authentication
		// must not change what is restored.
		const durableState = captureDurableStateV2(properties.durableState);
		const reducer = new TrustedNetworkV2PolicyReducer(properties);
		const authenticate = (
			entryBytes: Uint8Array,
		): Promise<ValidatedPolicySnapshotV2> =>
			authenticateCapturedPolicySnapshotEntryV2(entryBytes, reducer.descriptor);

		try {
			switch (durableState.state) {
				case "EMPTY":
					return reducer;
				case "ACTIVE": {
					const acceptedHead = await authenticate(
						durableState.acceptedHeadEntryBytes,
					);
					// A durable ACTIVE head is a trusted prior-validation checkpoint. Its
					// authority signature and network binding are re-authenticated above,
					// but restore deliberately does not require historical resolver data.
					reducer.project(acceptedHead);
					return reducer;
				}
				case "UNAVAILABLE": {
					const [acceptedHead, comparisonCandidate] = await Promise.all([
						authenticate(durableState.acceptedHeadEntryBytes),
						authenticate(durableState.comparisonCandidateEntryBytes),
					]);
					if (equals(acceptedHead.digest, comparisonCandidate.digest)) {
						throw new Error(
							"Unavailable comparison candidate must differ from the accepted head",
						);
					}
					reducer.project(acceptedHead);
					reducer.addPending(
						comparisonCandidate,
						durableState.acceptedAncestorDigest,
					);
					reducer.unavailable = {
						acceptedAncestorDigest: copyBytes(
							durableState.acceptedAncestorDigest,
						),
						comparisonCandidate: copySnapshot(comparisonCandidate),
						reason: durableState.reason,
					};
					return reducer;
				}
				case "FORKED": {
					const [commonParent, firstChild, secondChild] = await Promise.all([
						authenticate(durableState.commonParentEntryBytes),
						authenticate(durableState.childEntryBytes[0]),
						authenticate(durableState.childEntryBytes[1]),
					]);
					for (const child of [firstChild, secondChild]) {
						if (
							child.body.sequence !== commonParent.body.sequence + 1n ||
							!equals(child.body.previousPolicyDigest, commonParent.digest)
						) {
							throw new Error(
								"Fork child must be a direct successor of the common parent",
							);
						}
					}
					if (equals(firstChild.digest, secondChild.digest)) {
						throw new Error("Fork children must have distinct policy digests");
					}

					const children = [
						forkProofFromSnapshot(firstChild),
						forkProofFromSnapshot(secondChild),
					].sort(compareForkChildProofs) as [
						PolicyForkChildProofV2,
						PolicyForkChildProofV2,
					];
					reducer.project(commonParent);
					reducer.fork = {
						commonParent: projectionFromSnapshot(commonParent),
						children,
					};
					return reducer;
				}
			}
		} catch (error) {
			// Do not retain a caller-owned AbortSignal listener when restore rejects.
			reducer.abort();
			throw error;
		}
	}

	get state(): "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED" | "HALTED" {
		if (this.lifecycleController.signal.aborted) return "HALTED";
		if (this.fork !== undefined) return "FORKED";
		if (this.unavailable !== undefined) return "UNAVAILABLE";
		return this.acceptedHead === undefined ? "EMPTY" : "ACTIVE";
	}

	get head(): PolicyHeadProjectionV2 | undefined {
		return this.acceptedHead === undefined
			? undefined
			: projectionFromSnapshot(this.acceptedHead);
	}

	get forkEvidence(): PolicyForkEvidenceV2 | undefined {
		return this.fork === undefined ? undefined : copyForkEvidence(this.fork);
	}

	get pendingCount(): number {
		return this.pending.size;
	}

	get pendingBytes(): number {
		let total = 0;
		for (const { snapshot } of this.pending.values()) {
			total += snapshot.accountedBytes;
		}
		return total;
	}

	get pendingDigests(): Uint8Array[] {
		return [...this.pending.values()]
			.sort((a, b) => compareKeys(a.snapshot.digestKey, b.snapshot.digestKey))
			.map(({ snapshot }) => copyBytes(snapshot.digest));
	}

	exportDurableState(): PolicyReducerDurableStateV2 {
		// Lifecycle cancellation is process-local. Export the underlying protocol
		// safety state so a replacement process cannot erase UNAVAILABLE/FORKED.
		if (this.fork !== undefined) {
			if (this.acceptedHead === undefined) {
				throw new Error("Forked reducer is missing its common-parent entry");
			}
			return {
				formatVersion: 1,
				state: "FORKED",
				commonParentEntryBytes: copyBytes(this.acceptedHead.entryBytes),
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
				comparisonCandidateEntryBytes: copyBytes(
					this.unavailable.comparisonCandidate.entryBytes,
				),
				acceptedAncestorDigest: copyBytes(
					this.unavailable.acceptedAncestorDigest,
				),
				reason: this.unavailable.reason,
			};
		}
		if (this.acceptedHead !== undefined) {
			return {
				formatVersion: 1,
				state: "ACTIVE",
				acceptedHeadEntryBytes: copyBytes(this.acceptedHead.entryBytes),
			};
		}
		return { formatVersion: 1, state: "EMPTY" };
	}

	rolesFor(subject: PublicSignKey): number {
		return this.projectedRoles.get(publicKeyId(subject)) ?? 0;
	}

	isAuthorized(subject: PublicSignKey, roles: number): boolean {
		if (
			this.state !== "ACTIVE" ||
			!Number.isInteger(roles) ||
			roles === 0 ||
			(roles & ~TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS) !== 0
		) {
			return false;
		}
		return (this.rolesFor(subject) & roles) === roles;
	}

	/**
	 * Resolve one exact digest on the currently accepted policy prefix.
	 *
	 * The durable wrapper serializes this read with publication and owns the
	 * callback lifetime. The core still serializes the resolver work with direct
	 * users of this internal class. Historical snapshots remain resolver-backed;
	 * this walk retains only one authenticated cursor at a time.
	 */
	resolveAcceptedPolicyPrefix(
		reference: {
			sequence: bigint;
			digest: Uint8Array;
		},
		traversal?: AcceptedPolicyPrefixTraversalV2,
	): Promise<AcceptedPolicyPrefixResolutionV2> {
		let capturedSequence: bigint;
		let capturedDigest: Uint8Array;
		let capturedTraversal: AcceptedPolicyPrefixTraversalV2;
		try {
			capturedSequence = reference.sequence;
			if (
				typeof capturedSequence !== "bigint" ||
				capturedSequence < 0n ||
				capturedSequence > 0xffffffffffffffffn
			) {
				return Promise.resolve({
					status: "rejected",
					reason: "Policy reference sequence must be a u64",
				});
			}
			const suppliedDigest = reference.digest;
			if (!hasExactUint8ArrayByteLength(suppliedDigest, 32)) {
				throw new Error("invalid digest");
			}
			capturedDigest = copyBytes(suppliedDigest);
			const maxSteps = traversal?.maxSteps;
			if (
				maxSteps !== undefined &&
				(!Number.isSafeInteger(maxSteps) || maxSteps < 0)
			) {
				return Promise.resolve({
					status: "rejected",
					reason: "Policy prefix maxSteps must be a non-negative safe integer",
				});
			}
			const deadline = traversal?.deadline;
			if (
				deadline !== undefined &&
				(!Number.isSafeInteger(deadline) || deadline < 0)
			) {
				return Promise.resolve({
					status: "rejected",
					reason: "Policy prefix deadline must be a non-negative safe integer",
				});
			}
			const signal = traversal?.signal;
			if (signal !== undefined && !isAbortSignalV2(signal)) {
				return Promise.resolve({
					status: "rejected",
					reason: "Policy prefix signal must be an AbortSignal",
				});
			}
			capturedTraversal = { maxSteps, deadline, signal };
		} catch {
			return Promise.resolve({
				status: "rejected",
				reason: "Policy reference digest must contain exactly 32 bytes",
			});
		}
		const result = this.admissionTail.then(() =>
			this.resolveAcceptedPolicyPrefixOne(
				{
					sequence: capturedSequence,
					digest: capturedDigest,
				},
				capturedTraversal,
			),
		);
		this.admissionTail = result.then(
			(): void => {},
			(_reason: unknown): void => {},
		);
		return result;
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

	private async resolveAcceptedPolicyPrefixOne(
		reference: {
			sequence: bigint;
			digest: Uint8Array;
		},
		traversal: AcceptedPolicyPrefixTraversalV2,
	): Promise<AcceptedPolicyPrefixResolutionV2> {
		const interrupted = this.acceptedPrefixInterruption(traversal);
		if (interrupted !== undefined) return interrupted;
		if (this.unavailable !== undefined) {
			return {
				status: "unavailable",
				reason: "Policy reducer has unresolved accepted ancestry",
			};
		}
		if (this.acceptedHead === undefined) {
			return {
				status: "unavailable",
				reason: "Policy reducer has no accepted head",
			};
		}
		if (reference.sequence > this.acceptedHead.body.sequence) {
			return {
				status: "unavailable",
				reason: "Policy reference is newer than the accepted head",
			};
		}

		// The admission queue owns the core for this complete walk, so the internal
		// accepted snapshot cannot change until the resolution settles.
		let cursor = this.acceptedHead;
		let steps = 0;
		while (cursor.body.sequence > reference.sequence) {
			const loopInterruption = this.acceptedPrefixInterruption(traversal);
			if (loopInterruption !== undefined) return loopInterruption;
			if (
				cursor.body.sequence === reference.sequence + 1n &&
				!equals(cursor.body.previousPolicyDigest, reference.digest)
			) {
				return {
					status: "rejected",
					reason: "Policy reference is not an accepted prefix",
				};
			}
			if (traversal.maxSteps !== undefined && steps >= traversal.maxSteps) {
				return {
					status: "unavailable",
					reason: "Accepted policy prefix traversal reached maxSteps",
				};
			}
			steps += 1;
			const parent = await this.parentOf(cursor, undefined, traversal);
			const resolutionInterruption = this.acceptedPrefixInterruption(traversal);
			if (resolutionInterruption !== undefined) return resolutionInterruption;
			if (parent.status !== "found") {
				return {
					status: "unavailable",
					reason: "Accepted policy prefix ancestry is unavailable",
				};
			}
			cursor = parent.parent;
		}
		const completedInterruption = this.acceptedPrefixInterruption(traversal);
		if (completedInterruption !== undefined) return completedInterruption;

		if (!equals(cursor.digest, reference.digest)) {
			return {
				status: "rejected",
				reason: "Policy reference is not an accepted prefix",
			};
		}
		return {
			status: "resolved",
			policy: projectionFromSnapshot(cursor),
			acceptedHead: projectionFromSnapshot(this.acceptedHead),
		};
	}

	private acceptedPrefixInterruption(
		traversal: AcceptedPolicyPrefixTraversalV2,
	):
		| Exclude<AcceptedPolicyPrefixResolutionV2, { status: "resolved" }>
		| undefined {
		if (this.lifecycleController.signal.aborted) {
			return {
				status: "halted",
				reason: "Policy reducer lifecycle is aborted",
			};
		}
		if (this.fork !== undefined) {
			return {
				status: "halted",
				reason: "Policy reducer is halted by authority equivocation",
			};
		}
		if (traversal.signal?.aborted) {
			return {
				status: "unavailable",
				reason: "Policy prefix resolution was aborted by the caller",
			};
		}
		if (traversal.deadline !== undefined && Date.now() >= traversal.deadline) {
			return {
				status: "unavailable",
				reason: "Policy prefix resolution deadline elapsed",
			};
		}
		return undefined;
	}

	private fetchHints(): PolicyParentFetchHintV2[] {
		const unique = new Map<string, Uint8Array>();
		for (const { missingParentDigest } of this.pending.values()) {
			unique.set(bytesKey(missingParentDigest), missingParentDigest);
		}
		if (this.unavailable !== undefined) {
			unique.set(
				bytesKey(this.unavailable.acceptedAncestorDigest),
				this.unavailable.acceptedAncestorDigest,
			);
		}
		return [...unique.entries()]
			.sort(([a], [b]) => compareKeys(a, b))
			.map(([, digest]) => ({
				kind: "policy-parent" as const,
				digest: copyBytes(digest),
			}));
	}

	private result(
		status: PolicyAdmissionStatusV2,
		reason?: string,
		evictedPolicyDigests?: Uint8Array[],
		forkObservations?: PolicyForkChildProofV2[],
	): PolicyAdmissionResultV2 {
		return {
			status,
			reason,
			head: this.head,
			forkObservations:
				forkObservations === undefined
					? undefined
					: forkObservations.map(copyForkChildProof),
			fetchHints: this.fetchHints(),
			pendingCount: this.pending.size,
			pendingBytes: this.pendingBytes,
			evictedPolicyDigests:
				evictedPolicyDigests === undefined
					? undefined
					: evictedPolicyDigests.map(copyBytes),
		};
	}

	private forkedResult(
		forkObservations?: PolicyForkChildProofV2[],
	): PolicyAdmissionResultV2 {
		return this.result(
			"forked",
			"Policy authority signed competing children",
			undefined,
			forkObservations,
		);
	}

	private haltedResult(): PolicyAdmissionResultV2 {
		return this.result("halted", "Policy reducer lifecycle is aborted");
	}

	private unavailableResult(retention?: {
		retained: boolean;
		evictedPolicyDigests: Uint8Array[];
	}): PolicyAdmissionResultV2 {
		const blockedCandidateRetained =
			this.unavailable !== undefined &&
			this.pending.has(this.unavailable.comparisonCandidate.digestKey);
		const recoverable =
			(retention?.retained ?? true) && blockedCandidateRetained;
		const reason = recoverable
			? this.unavailable!.reason
			: blockedCandidateRetained
				? "Policy pending capacity did not retain this candidate"
				: "Policy reducer remains unavailable because pending capacity did not retain the blocked candidate";
		return this.result(
			recoverable ? "unavailable" : "capacity",
			reason,
			retention?.evictedPolicyDigests,
		);
	}

	private completedDrainResult(
		outcome: PendingDrainOutcomeV2 | undefined,
	): PolicyAdmissionResultV2 | undefined {
		if (outcome?.status === "forked") {
			return this.forkedResult(outcome.forkObservations);
		}
		if (outcome?.status === "halted") return this.haltedResult();
		return outcome?.status === "unavailable"
			? this.unavailableResult(outcome)
			: undefined;
	}

	private project(snapshot: ValidatedPolicySnapshotV2): void {
		this.acceptedHead = copySnapshot(snapshot);
		this.projectedRoles = new Map(
			snapshot.body.bindings.map((binding) => [
				publicKeyId(binding.signingKey),
				binding.roles,
			]),
		);
	}

	private retainCanonicalHeadEntry(snapshot: ValidatedPolicySnapshotV2): void {
		if (
			this.acceptedHead?.digestKey === snapshot.digestKey &&
			compare(snapshot.entryBytes, this.acceptedHead.entryBytes) < 0
		) {
			this.acceptedHead = copySnapshot(snapshot);
		}
	}

	private async resolveExternalSnapshot(
		digest: Uint8Array,
		traversal?: AcceptedPolicyPrefixTraversalV2,
	): Promise<ValidatedPolicySnapshotV2 | undefined> {
		if (this.lifecycleController.signal.aborted) {
			throw new PolicyDependencyUnavailableErrorV2(
				"Policy resolver lifecycle is aborted",
			);
		}
		if (traversal?.signal?.aborted) {
			throw new PolicyDependencyUnavailableErrorV2(
				"Policy resolver attempt was aborted by the caller",
			);
		}
		const deadlineRemaining =
			traversal?.deadline === undefined
				? undefined
				: traversal.deadline - Date.now();
		const deadlineLimitsAttempt =
			deadlineRemaining !== undefined &&
			deadlineRemaining <= this.resolveTimeoutMs;
		const remaining =
			deadlineRemaining === undefined
				? this.resolveTimeoutMs
				: Math.min(this.resolveTimeoutMs, deadlineRemaining);
		if (remaining <= 0) {
			throw new PolicyDependencyUnavailableErrorV2(
				"Policy resolver deadline elapsed",
			);
		}

		const attemptController = new AbortController();
		let timedOut = false;
		let callerAborted = false;
		const abortFromLifecycle = (): void => attemptController.abort();
		const abortFromCaller = (): void => {
			callerAborted = true;
			attemptController.abort();
		};
		this.lifecycleController.signal.addEventListener(
			"abort",
			abortFromLifecycle,
			{ once: true },
		);
		traversal?.signal?.addEventListener("abort", abortFromCaller, {
			once: true,
		});
		const timeout = setTimeout(() => {
			timedOut = true;
			attemptController.abort();
		}, remaining);
		const abortError = (): PolicyDependencyUnavailableErrorV2 =>
			new PolicyDependencyUnavailableErrorV2(
				callerAborted
					? "Policy resolver attempt was aborted by the caller"
					: timedOut && deadlineLimitsAttempt
						? "Policy resolver deadline elapsed"
						: timedOut
							? `Policy resolver timed out after ${this.resolveTimeoutMs} ms`
							: "Policy resolver attempt was aborted",
			);

		let rejectOnAbort: (() => void) | undefined;

		try {
			return await new Promise<ValidatedPolicySnapshotV2 | undefined>(
				(resolve, reject) => {
					let settled = false;
					const settle = (complete: () => void): void => {
						if (settled) return;
						settled = true;
						complete();
					};
					rejectOnAbort = (): void => settle(() => reject(abortError()));
					attemptController.signal.addEventListener("abort", rejectOnAbort, {
						once: true,
					});
					void Promise.resolve()
						.then(async () => {
							if (attemptController.signal.aborted) throw abortError();
							const entryBytes = await this.resolvePolicyEntry(
								copyBytes(digest),
								{
									signal: attemptController.signal,
								},
							);
							// A resolver may ignore cancellation. Do not spend decode or signature
							// verification work on bytes that arrive after this attempt expired.
							if (attemptController.signal.aborted) throw abortError();
							if (entryBytes === undefined) return undefined;
							const snapshot = await authenticatePolicySnapshotEntryV2(
								entryBytes,
								this.descriptor,
							);
							if (!equals(snapshot.digest, digest)) {
								throw new Error(
									"Policy resolver returned the wrong body digest",
								);
							}
							return snapshot;
						})
						.then(
							(value) => settle(() => resolve(value)),
							(error: unknown) => settle(() => reject(error)),
						);
				},
			);
		} catch (error) {
			if (error instanceof PolicyDependencyUnavailableErrorV2) throw error;
			throw new PolicyDependencyUnavailableErrorV2(
				`Policy resolver dependency is unavailable: ${validationMessage(error)}`,
			);
		} finally {
			clearTimeout(timeout);
			this.lifecycleController.signal.removeEventListener(
				"abort",
				abortFromLifecycle,
			);
			traversal?.signal?.removeEventListener("abort", abortFromCaller);
			if (rejectOnAbort !== undefined) {
				attemptController.signal.removeEventListener("abort", rejectOnAbort);
			}
		}
	}

	private async resolveSnapshot(
		digest: Uint8Array,
		cache?: SnapshotResolutionCacheV2,
		traversal?: AcceptedPolicyPrefixTraversalV2,
	): Promise<ValidatedPolicySnapshotV2 | undefined> {
		const digestKey = bytesKey(digest);
		if (this.acceptedHead?.digestKey === digestKey) {
			return copySnapshot(this.acceptedHead);
		}
		const pending = this.pending.get(digestKey);
		if (pending !== undefined) return copySnapshot(pending.snapshot);
		let resolution = cache?.get(digestKey);
		if (resolution === undefined) {
			resolution = this.resolveExternalSnapshot(digest, traversal);
			cache?.set(digestKey, resolution);
		}
		const snapshot = await resolution;
		return snapshot === undefined ? undefined : copySnapshot(snapshot);
	}

	private async parentOf(
		child: ValidatedPolicySnapshotV2,
		cache?: SnapshotResolutionCacheV2,
		traversal?: AcceptedPolicyPrefixTraversalV2,
	): Promise<ParentResolutionV2> {
		if (child.body.sequence === 0n) {
			return {
				status: "reject",
				digest: copyBytes(child.body.previousPolicyDigest),
				reason: "Genesis policy has no parent",
			};
		}
		let parent: ValidatedPolicySnapshotV2 | undefined;
		try {
			parent = await this.resolveSnapshot(
				child.body.previousPolicyDigest,
				cache,
				traversal,
			);
		} catch (error) {
			return {
				status: "unavailable",
				digest: copyBytes(child.body.previousPolicyDigest),
				reason: `Policy parent dependency is unavailable: ${validationMessage(error)}`,
			};
		}
		if (parent === undefined) {
			return {
				status: "missing",
				digest: copyBytes(child.body.previousPolicyDigest),
			};
		}
		if (child.body.sequence !== parent.body.sequence + 1n) {
			return {
				status: "reject",
				digest: copyBytes(child.body.previousPolicyDigest),
				reason: "Policy sequence is not contiguous with its parent",
			};
		}
		return { status: "found", parent };
	}

	private candidateAncestryResult(
		resolution: Exclude<ParentResolutionV2, { status: "found" }>,
	): EvaluationV2 {
		return resolution.status === "unavailable"
			? {
					status: "missing",
					digest: copyBytes(resolution.digest),
					reason: resolution.reason,
				}
			: resolution;
	}

	private acceptedAncestryUnavailable(
		resolution: Exclude<ParentResolutionV2, { status: "found" }>,
	): Extract<EvaluationV2, { status: "unavailable" }> {
		return {
			status: "unavailable",
			digest: copyBytes(resolution.digest),
			reason:
				resolution.status === "missing"
					? "Accepted policy ancestry is unavailable from the resolver"
					: resolution.status === "unavailable"
						? `Accepted policy ancestry is unavailable: ${resolution.reason}`
						: `Accepted policy ancestry validation failed: ${resolution.reason}`,
		};
	}

	private async evaluate(
		candidate: ValidatedPolicySnapshotV2,
	): Promise<EvaluationV2> {
		const resolutionCache: SnapshotResolutionCacheV2 = new Map();
		if (this.acceptedHead === undefined) {
			let cursor = candidate;
			while (cursor.body.sequence !== 0n) {
				const parent = await this.parentOf(cursor, resolutionCache);
				if (parent.status !== "found") {
					return this.candidateAncestryResult(parent);
				}
				cursor = parent.parent;
			}
			return { status: "accept" };
		}

		let candidateCursor = candidate;
		let acceptedCursor = this.acceptedHead;
		let candidateChild: ValidatedPolicySnapshotV2 | undefined;
		let acceptedChild: ValidatedPolicySnapshotV2 | undefined;

		while (candidateCursor.body.sequence > acceptedCursor.body.sequence) {
			candidateChild = candidateCursor;
			const parent = await this.parentOf(candidateCursor, resolutionCache);
			if (parent.status !== "found") {
				return this.candidateAncestryResult(parent);
			}
			candidateCursor = parent.parent;
		}
		while (acceptedCursor.body.sequence > candidateCursor.body.sequence) {
			acceptedChild = acceptedCursor;
			const parent = await this.parentOf(acceptedCursor, resolutionCache);
			if (parent.status !== "found") {
				return this.acceptedAncestryUnavailable(parent);
			}
			acceptedCursor = parent.parent;
		}

		while (!equals(candidateCursor.digest, acceptedCursor.digest)) {
			if (
				candidateCursor.body.sequence === 0n ||
				acceptedCursor.body.sequence === 0n
			) {
				return {
					status: "reject",
					reason: "Policy branches do not share the descriptor genesis",
				};
			}
			candidateChild = candidateCursor;
			acceptedChild = acceptedCursor;
			const [candidateParent, acceptedParent] = await Promise.all([
				this.parentOf(candidateCursor, resolutionCache),
				this.parentOf(acceptedCursor, resolutionCache),
			]);
			if (acceptedParent.status !== "found") {
				return this.acceptedAncestryUnavailable(acceptedParent);
			}
			if (candidateParent.status !== "found") {
				return this.candidateAncestryResult(candidateParent);
			}
			candidateCursor = candidateParent.parent;
			acceptedCursor = acceptedParent.parent;
		}

		if (candidateChild === undefined) {
			return { status: "duplicate" };
		}
		if (acceptedChild === undefined) {
			return { status: "accept" };
		}
		if (equals(candidateChild.digest, acceptedChild.digest)) {
			return { status: "duplicate" };
		}
		return {
			status: "fork",
			commonParent: candidateCursor,
			candidateChild,
			acceptedChild,
		};
	}

	private setFork(
		evaluation: Extract<EvaluationV2, { status: "fork" }>,
	): PolicyForkChildProofV2[] {
		// Pending snapshots were authenticated when admitted. Combine every one
		// that is already provably a direct child with the pair that first exposed
		// the fork. Canonical selection below may displace either initial child, so
		// the durable layer needs the complete bounded observation set and removes
		// whichever final pair is carried by exportDurableState().
		const pendingForkObservationSnapshots = [...this.pending.values()]
			.map(({ snapshot }) => snapshot)
			.filter(
				(snapshot) =>
					snapshot.body.sequence ===
						evaluation.commonParent.body.sequence + 1n &&
					equals(
						snapshot.body.previousPolicyDigest,
						evaluation.commonParent.digest,
					),
			);
		const forkObservationSnapshots = [
			evaluation.candidateChild,
			evaluation.acceptedChild,
			...pendingForkObservationSnapshots,
		];
		const forkObservations = forkObservationSnapshots
			.map(forkProofFromSnapshot)
			.sort(compareForkChildProofs)
			.filter(
				(proof, index, observations) =>
					index === 0 ||
					!equals(proof.entryBytes, observations[index - 1]!.entryBytes),
			);

		this.project(evaluation.commonParent);
		const children = [
			forkProofFromSnapshot(evaluation.candidateChild),
			forkProofFromSnapshot(evaluation.acceptedChild),
		].sort(compareForkChildProofs) as [
			PolicyForkChildProofV2,
			PolicyForkChildProofV2,
		];
		this.fork = {
			commonParent: projectionFromSnapshot(evaluation.commonParent),
			children,
		};
		for (const snapshot of forkObservationSnapshots) {
			this.retainCanonicalForkChild(snapshot);
		}
		this.unavailable = undefined;
		this.pending.clear();
		return forkObservations;
	}

	private retainCanonicalForkChild(snapshot: ValidatedPolicySnapshotV2): void {
		if (this.fork === undefined) return;
		const byDigest = new Map<string, PolicyForkChildProofV2>();
		for (const proof of [
			...this.fork.children,
			forkProofFromSnapshot(snapshot),
		]) {
			const digestKey = bytesKey(proof.digest);
			const retained = byDigest.get(digestKey);
			if (
				retained === undefined ||
				compare(proof.entryBytes, retained.entryBytes) < 0
			) {
				byDigest.set(digestKey, copyForkChildProof(proof));
			}
		}
		const canonical = [...byDigest.values()]
			.sort(compareForkChildProofs)
			.slice(0, 2);
		if (canonical.length !== 2) return;
		this.fork.children = [canonical[0]!, canonical[1]!];
	}

	private observeAfterFork(
		snapshot: ValidatedPolicySnapshotV2,
	): PolicyForkChildProofV2 | undefined {
		if (this.fork === undefined || this.acceptedHead === undefined) {
			return undefined;
		}
		const commonParent = this.acceptedHead;
		if (
			snapshot.body.sequence !== commonParent.body.sequence + 1n ||
			!equals(snapshot.body.previousPolicyDigest, commonParent.digest)
		) {
			return undefined;
		}

		// This bounded kernel retains only the canonical two direct child proofs.
		// Return this already-authenticated observation so the durable outer layer
		// can retain every proof without verifying it twice.
		const observation = forkProofFromSnapshot(snapshot);
		this.retainCanonicalForkChild(snapshot);
		return observation;
	}

	private addPending(
		snapshot: ValidatedPolicySnapshotV2,
		missingParentDigest: Uint8Array,
	): { retained: boolean; evictedPolicyDigests: Uint8Array[] } {
		const existing = this.pending.get(snapshot.digestKey);
		if (existing !== undefined) {
			existing.missingParentDigest = copyBytes(missingParentDigest);
			if (
				snapshot.accountedBytes <= this.maxPendingPolicyBytes &&
				compare(snapshot.entryBytes, existing.snapshot.entryBytes) < 0
			) {
				existing.snapshot = copySnapshot(snapshot);
			}
			return { retained: true, evictedPolicyDigests: [] };
		}
		if (snapshot.accountedBytes > this.maxPendingPolicyBytes) {
			return {
				retained: false,
				evictedPolicyDigests: [copyBytes(snapshot.digest)],
			};
		}
		this.pending.set(snapshot.digestKey, {
			snapshot: copySnapshot(snapshot),
			missingParentDigest: copyBytes(missingParentDigest),
		});
		const ordered = [...this.pending.entries()].sort(([a], [b]) =>
			compareKeys(a, b),
		);
		const retainedKeys = new Set(
			ordered.slice(0, this.maxPending).map(([key]) => key),
		);
		const evictedPolicyDigests: Uint8Array[] = [];
		for (const [key, pending] of ordered) {
			if (retainedKeys.has(key)) continue;
			evictedPolicyDigests.push(copyBytes(pending.snapshot.digest));
			this.pending.delete(key);
		}
		return {
			retained: retainedKeys.has(snapshot.digestKey),
			evictedPolicyDigests,
		};
	}

	private enterUnavailable(
		snapshot: ValidatedPolicySnapshotV2,
		evaluation: Extract<EvaluationV2, { status: "unavailable" }>,
	): { retained: boolean; evictedPolicyDigests: Uint8Array[] } {
		const retention = this.addPending(snapshot, evaluation.digest);
		this.unavailable = {
			acceptedAncestorDigest: copyBytes(evaluation.digest),
			comparisonCandidate: copySnapshot(snapshot),
			reason: boundedUnavailableReason(evaluation.reason),
		};
		return retention;
	}

	private async drainPending(): Promise<PendingDrainOutcomeV2 | undefined> {
		if (this.lifecycleController.signal.aborted) {
			return { status: "halted" };
		}
		let accepted = false;
		let progress = true;
		while (
			progress &&
			this.pending.size > 0 &&
			this.fork === undefined &&
			this.unavailable === undefined
		) {
			progress = false;
			const ordered = [...this.pending.values()].sort((a, b) =>
				compareKeys(a.snapshot.digestKey, b.snapshot.digestKey),
			);
			for (const pending of ordered) {
				if (!this.pending.has(pending.snapshot.digestKey)) continue;
				const evaluation = await this.evaluate(pending.snapshot);
				if (this.lifecycleController.signal.aborted) {
					return { status: "halted" };
				}
				if (evaluation.status === "missing") {
					pending.missingParentDigest = copyBytes(evaluation.digest);
					continue;
				}
				if (evaluation.status === "unavailable") {
					const retention = this.enterUnavailable(pending.snapshot, evaluation);
					return { status: "unavailable", ...retention };
				}
				this.pending.delete(pending.snapshot.digestKey);
				progress = true;
				if (evaluation.status === "accept") {
					this.project(pending.snapshot);
					accepted = true;
				} else if (evaluation.status === "fork") {
					return {
						status: "forked",
						forkObservations: this.setFork(evaluation),
					};
				}
			}
		}
		return accepted ? { status: "accepted" } : undefined;
	}

	private enqueueAdmission(
		operation: () => Promise<PolicyAdmissionResultV2>,
	): Promise<PolicyAdmissionResultV2> {
		const result = this.admissionTail.then(async () => {
			if (this.lifecycleController.signal.aborted) {
				return this.haltedResult();
			}
			const admission = await operation();
			return this.lifecycleController.signal.aborted
				? this.haltedResult()
				: admission;
		});
		this.admissionTail = result.then(
			(): void => {},
			(_reason: unknown): void => {},
		);
		return result;
	}

	ingest(entryBytes: Uint8Array): Promise<PolicyAdmissionResultV2> {
		if (this.lifecycleController.signal.aborted) {
			return Promise.resolve(this.haltedResult());
		}
		let capturedEntryBytes: Uint8Array;
		try {
			// Capture at the API boundary, before this admission waits behind earlier
			// work. Otherwise a caller could mutate a queued entry before validation.
			capturedEntryBytes = capturePolicySnapshotEntryBytesV2(entryBytes);
		} catch (error) {
			const reason = validationMessage(error);
			return this.enqueueAdmission(async () => this.result("rejected", reason));
		}
		return this.enqueueAdmission(() => this.ingestOne(capturedEntryBytes));
	}

	retryUnavailable(): Promise<PolicyAdmissionResultV2> {
		return this.enqueueAdmission(() => this.retryUnavailableOne());
	}

	private async ingestOne(
		entryBytes: Uint8Array,
	): Promise<PolicyAdmissionResultV2> {
		let snapshot: ValidatedPolicySnapshotV2;
		try {
			snapshot = await authenticateCapturedPolicySnapshotEntryV2(
				entryBytes,
				this.descriptor,
			);
		} catch (error) {
			return this.result("rejected", validationMessage(error));
		}
		if (this.lifecycleController.signal.aborted) return this.haltedResult();

		if (this.fork !== undefined) {
			const forkObservation = this.observeAfterFork(snapshot);
			return this.result(
				"halted",
				"Policy reducer is halted by authority equivocation",
				undefined,
				forkObservation === undefined ? undefined : [forkObservation],
			);
		}
		this.retainCanonicalHeadEntry(snapshot);

		if (this.unavailable !== undefined) {
			if (this.acceptedHead?.digestKey === snapshot.digestKey) {
				return this.result("unavailable", this.unavailable.reason);
			}
			if (
				this.unavailable.comparisonCandidate.digestKey === snapshot.digestKey &&
				compare(
					snapshot.entryBytes,
					this.unavailable.comparisonCandidate.entryBytes,
				) < 0
			) {
				this.unavailable.comparisonCandidate = copySnapshot(snapshot);
			}
			const existingPending = this.pending.get(snapshot.digestKey);
			if (existingPending !== undefined) {
				this.addPending(snapshot, existingPending.missingParentDigest);
				return this.result("unavailable", this.unavailable.reason);
			}
			const retention = this.addPending(
				snapshot,
				this.unavailable.acceptedAncestorDigest,
			);
			return this.unavailableResult(retention);
		}

		const existingPending = this.pending.get(snapshot.digestKey);
		if (existingPending !== undefined) {
			this.addPending(snapshot, existingPending.missingParentDigest);
			return this.result("pending", "Policy snapshot is already pending");
		}

		const evaluation = await this.evaluate(snapshot);
		if (this.lifecycleController.signal.aborted) return this.haltedResult();
		if (evaluation.status === "reject") {
			return this.result("rejected", evaluation.reason);
		}
		if (evaluation.status === "duplicate") {
			return this.result("duplicate");
		}
		if (evaluation.status === "fork") {
			return this.forkedResult(this.setFork(evaluation));
		}
		if (evaluation.status === "unavailable") {
			const retention = this.enterUnavailable(snapshot, evaluation);
			return this.unavailableResult(retention);
		}
		if (evaluation.status === "missing") {
			const pending = this.addPending(snapshot, evaluation.digest);
			return this.result(
				pending.retained ? "pending" : "capacity",
				pending.retained
					? (evaluation.reason ?? "Policy parent is missing")
					: "Policy pending capacity did not retain this candidate",
				pending.evictedPolicyDigests,
			);
		}

		this.project(snapshot);
		return (
			this.completedDrainResult(await this.drainPending()) ??
			this.result("accepted")
		);
	}

	private async retryUnavailableOne(): Promise<PolicyAdmissionResultV2> {
		if (this.lifecycleController.signal.aborted) return this.haltedResult();
		if (this.fork !== undefined) {
			return this.result(
				"halted",
				"Policy reducer is halted by authority equivocation",
			);
		}
		const unavailable = this.unavailable;
		if (unavailable === undefined) {
			return this.result("duplicate", "Policy reducer is not unavailable");
		}
		const pending = this.pending.get(unavailable.comparisonCandidate.digestKey);
		if (pending === undefined) {
			return this.result(
				"capacity",
				"Unavailable comparison candidate is not retained; re-ingest it before retrying",
			);
		}

		const evaluation = await this.evaluate(pending.snapshot);
		if (this.lifecycleController.signal.aborted) return this.haltedResult();
		if (evaluation.status === "unavailable") {
			pending.missingParentDigest = copyBytes(evaluation.digest);
			this.unavailable = {
				acceptedAncestorDigest: copyBytes(evaluation.digest),
				comparisonCandidate: copySnapshot(pending.snapshot),
				reason: boundedUnavailableReason(evaluation.reason),
			};
			return this.result("unavailable", this.unavailable.reason);
		}

		this.unavailable = undefined;
		let status: PolicyAdmissionStatusV2;
		let reason: string | undefined;
		if (evaluation.status === "missing") {
			pending.missingParentDigest = copyBytes(evaluation.digest);
			status = "pending";
			reason = evaluation.reason ?? "Policy parent is missing";
		} else {
			this.pending.delete(pending.snapshot.digestKey);
			if (evaluation.status === "fork") {
				return this.forkedResult(this.setFork(evaluation));
			}
			if (evaluation.status === "accept") {
				this.project(pending.snapshot);
				status = "accepted";
			} else if (evaluation.status === "duplicate") {
				status = "duplicate";
			} else {
				status = "rejected";
				reason = evaluation.reason;
			}
		}

		return (
			this.completedDrainResult(await this.drainPending()) ??
			this.result(status, reason)
		);
	}
}
