import { deserialize, serialize } from "@dao-xyz/borsh";
import { DecryptedThing, PublicSignKey, verify } from "@peerbit/crypto";
import { Entry, EntryV0, NO_ENCODING } from "@peerbit/log";
import { compare, equals } from "uint8arrays";
import {
	NetworkDescriptorV2,
	PolicySnapshotBodyV2,
	PolicySubjectBindingV2,
	TRUSTED_NETWORK_V2_KNOWN_ROLE_BITS,
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

const DEFAULT_MAX_PENDING_POLICIES_V2 = 64;
const DEFAULT_MAX_PENDING_POLICY_BYTES_V2 = 256 * 1024;
const PENDING_POLICY_ACCOUNTING_OVERHEAD_V2 = 64;
const MAX_UNAVAILABLE_REASON_LENGTH_V2 = 512;

const copyBytes = (bytes: Uint8Array): Uint8Array => Uint8Array.from(bytes);

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
) => EntryV0<Uint8Array> | undefined | Promise<EntryV0<Uint8Array> | undefined>;

export type PolicyParentFetchHintV2 = {
	kind: "policy-parent";
	digest: Uint8Array;
};

export type PolicyHeadProjectionV2 = {
	sequence: bigint;
	digest: Uint8Array;
	bindings: PolicySubjectBindingV2[];
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
	fetchHints: PolicyParentFetchHintV2[];
	pendingCount: number;
	pendingBytes: number;
	evictedPolicyDigests?: Uint8Array[];
};

type PendingPolicySnapshotV2 = {
	snapshot: ValidatedPolicySnapshotV2;
	missingParentDigest: Uint8Array;
};

type UnavailablePolicyComparisonV2 = {
	acceptedAncestorDigest: Uint8Array;
	candidateDigestKey: string;
	reason: string;
};

type ParentResolutionV2 =
	| { status: "found"; parent: ValidatedPolicySnapshotV2 }
	| { status: "missing"; digest: Uint8Array }
	| { status: "reject"; digest: Uint8Array; reason: string };

type SnapshotResolutionCacheV2 = Map<
	string,
	Promise<ValidatedPolicySnapshotV2 | undefined>
>;

type EvaluationV2 =
	| { status: "accept" }
	| { status: "duplicate" }
	| { status: "missing"; digest: Uint8Array }
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
	| { status: "forked" }
	| {
			status: "unavailable";
			retained: boolean;
			evictedPolicyDigests: Uint8Array[];
	  };

const validationMessage = (error: unknown): string =>
	error instanceof Error ? error.message : String(error);

const boundedUnavailableReason = (reason: string): string =>
	reason.slice(0, MAX_UNAVAILABLE_REASON_LENGTH_V2);

export const authenticatePolicySnapshotEntryV2 = async (
	entry: unknown,
	descriptor: NetworkDescriptorV2,
): Promise<ValidatedPolicySnapshotV2> => {
	assertNetworkDescriptorV2(descriptor);
	if (!(entry instanceof EntryV0)) {
		throw new Error("Policy snapshot must use EntryV0");
	}
	if (!(entry._meta instanceof DecryptedThing)) {
		throw new Error("Policy snapshot metadata must be public");
	}
	if (!(entry._payload instanceof DecryptedThing)) {
		throw new Error("Policy snapshot payload must be public");
	}
	if (
		entry._signatures === undefined ||
		entry._signatures.signatures.length !== 1
	) {
		throw new Error("Policy snapshot must contain exactly one signature");
	}
	if (!(entry._signatures.signatures[0] instanceof DecryptedThing)) {
		throw new Error("Policy snapshot signature must be public");
	}
	const entryBytes = serialize(entry);
	const authenticatedEntry = deserialize(entryBytes, Entry);
	if (!(authenticatedEntry instanceof EntryV0)) {
		throw new Error("Policy snapshot must decode as EntryV0");
	}
	authenticatedEntry.init({ encoding: NO_ENCODING });

	const signatures = await authenticatedEntry.getSignatures();
	if (signatures.length !== 1) {
		throw new Error("Policy snapshot must resolve exactly one signature");
	}
	const signature = signatures[0]!;
	if (
		!equals(
			serialize(signature.publicKey),
			serialize(descriptor.policyAuthority),
		)
	) {
		throw new Error("Policy snapshot signer is not the policy authority");
	}
	if (!(await verify(signature, authenticatedEntry.getSignableBytes()))) {
		throw new Error("Policy snapshot authority signature is invalid");
	}

	const payload = await authenticatedEntry.getPayloadValue();
	if (!(payload instanceof Uint8Array)) {
		throw new Error(
			"Policy snapshot payload must contain canonical body bytes",
		);
	}
	const body = decodePolicySnapshotBodyV2(copyBytes(payload), descriptor);
	const digest = digestPolicySnapshotBodyV2(body);
	return {
		body: copyBody(body),
		digest: copyBytes(digest),
		digestKey: bytesKey(digest),
		entryBytes,
		accountedBytes:
			entryBytes.byteLength +
			serialize(body).byteLength +
			PENDING_POLICY_ACCOUNTING_OVERHEAD_V2,
	};
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

export class TrustedNetworkV2PolicyReducer {
	private readonly descriptor: NetworkDescriptorV2;
	private readonly resolvePolicyEntry: PolicySnapshotResolverV2;
	private readonly maxPending: number;
	private readonly maxPendingPolicyBytes: number;
	private acceptedHead?: ValidatedPolicySnapshotV2;
	private projectedRoles = new Map<string, number>();
	private readonly pending = new Map<string, PendingPolicySnapshotV2>();
	// Recovery is bound to this exact candidate. Unrelated admissions may enter
	// the bounded pending set but can never restore ACTIVE authorization.
	private unavailable?: UnavailablePolicyComparisonV2;
	private fork?: PolicyForkEvidenceV2;
	private admissionTail: Promise<void> = Promise.resolve();

	constructor(properties: {
		descriptor: NetworkDescriptorV2;
		resolvePolicyEntry: PolicySnapshotResolverV2;
		maxPending?: number;
		maxPendingPolicyBytes?: number;
	}) {
		assertNetworkDescriptorV2(properties.descriptor);
		const maxPending = properties.maxPending ?? DEFAULT_MAX_PENDING_POLICIES_V2;
		if (!Number.isSafeInteger(maxPending) || maxPending < 1) {
			throw new Error("maxPending must be a positive safe integer");
		}
		const maxPendingPolicyBytes =
			properties.maxPendingPolicyBytes ?? DEFAULT_MAX_PENDING_POLICY_BYTES_V2;
		if (
			!Number.isSafeInteger(maxPendingPolicyBytes) ||
			maxPendingPolicyBytes < 1
		) {
			throw new Error("maxPendingPolicyBytes must be a positive safe integer");
		}
		this.descriptor = deserialize(
			serialize(properties.descriptor),
			NetworkDescriptorV2,
		);
		this.resolvePolicyEntry = properties.resolvePolicyEntry;
		this.maxPending = maxPending;
		this.maxPendingPolicyBytes = maxPendingPolicyBytes;
	}

	get state(): "EMPTY" | "ACTIVE" | "UNAVAILABLE" | "FORKED" {
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
	): PolicyAdmissionResultV2 {
		return {
			status,
			reason,
			head: this.head,
			fetchHints: this.fetchHints(),
			pendingCount: this.pending.size,
			pendingBytes: this.pendingBytes,
			evictedPolicyDigests:
				evictedPolicyDigests === undefined
					? undefined
					: evictedPolicyDigests.map(copyBytes),
		};
	}

	private forkedResult(): PolicyAdmissionResultV2 {
		return this.result("forked", "Policy authority signed competing children");
	}

	private unavailableResult(retention?: {
		retained: boolean;
		evictedPolicyDigests: Uint8Array[];
	}): PolicyAdmissionResultV2 {
		const blockedCandidateRetained =
			this.unavailable !== undefined &&
			this.pending.has(this.unavailable.candidateDigestKey);
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
		if (outcome?.status === "forked") return this.forkedResult();
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

	private async resolveSnapshot(
		digest: Uint8Array,
		cache?: SnapshotResolutionCacheV2,
	): Promise<ValidatedPolicySnapshotV2 | undefined> {
		const digestKey = bytesKey(digest);
		if (this.acceptedHead?.digestKey === digestKey) {
			return copySnapshot(this.acceptedHead);
		}
		const pending = this.pending.get(digestKey);
		if (pending !== undefined) return copySnapshot(pending.snapshot);
		let resolution = cache?.get(digestKey);
		if (resolution === undefined) {
			resolution = (async () => {
				const entry = await this.resolvePolicyEntry(copyBytes(digest));
				if (entry === undefined) return undefined;
				const snapshot = await authenticatePolicySnapshotEntryV2(
					entry,
					this.descriptor,
				);
				if (!equals(snapshot.digest, digest)) {
					throw new Error("Policy resolver returned the wrong body digest");
				}
				return snapshot;
			})();
			cache?.set(digestKey, resolution);
		}
		const snapshot = await resolution;
		return snapshot === undefined ? undefined : copySnapshot(snapshot);
	}

	private async parentOf(
		child: ValidatedPolicySnapshotV2,
		cache?: SnapshotResolutionCacheV2,
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
			);
		} catch (error) {
			return {
				status: "reject",
				digest: copyBytes(child.body.previousPolicyDigest),
				reason: `Policy parent validation failed: ${validationMessage(error)}`,
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

	private acceptedAncestryUnavailable(
		resolution: Exclude<ParentResolutionV2, { status: "found" }>,
	): Extract<EvaluationV2, { status: "unavailable" }> {
		return {
			status: "unavailable",
			digest: copyBytes(resolution.digest),
			reason:
				resolution.status === "missing"
					? "Accepted policy ancestry is unavailable from the resolver"
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
				if (parent.status !== "found") return parent;
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
			if (parent.status !== "found") return parent;
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
			if (candidateParent.status !== "found") return candidateParent;
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

	private setFork(evaluation: Extract<EvaluationV2, { status: "fork" }>): void {
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
		this.unavailable = undefined;
		this.pending.clear();
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

	private observeAfterFork(snapshot: ValidatedPolicySnapshotV2): void {
		if (this.fork === undefined || this.acceptedHead === undefined) return;
		const commonParent = this.acceptedHead;
		if (
			snapshot.body.sequence !== commonParent.body.sequence + 1n ||
			!equals(snapshot.body.previousPolicyDigest, commonParent.digest)
		) {
			return;
		}

		// This bounded kernel deliberately retains only the canonical two direct
		// child proofs. Durable storage of every authenticated fork observation is
		// an outer-layer responsibility for a later integration slice.
		this.retainCanonicalForkChild(snapshot);
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
			candidateDigestKey: snapshot.digestKey,
			reason: boundedUnavailableReason(evaluation.reason),
		};
		return retention;
	}

	private async drainPending(): Promise<PendingDrainOutcomeV2 | undefined> {
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
					this.setFork(evaluation);
					return { status: "forked" };
				}
			}
		}
		return accepted ? { status: "accepted" } : undefined;
	}

	private enqueueAdmission(
		operation: () => Promise<PolicyAdmissionResultV2>,
	): Promise<PolicyAdmissionResultV2> {
		const result = this.admissionTail.then(operation);
		this.admissionTail = result.then(
			(): void => {},
			(_reason: unknown): void => {},
		);
		return result;
	}

	ingest(entry: unknown): Promise<PolicyAdmissionResultV2> {
		return this.enqueueAdmission(() => this.ingestOne(entry));
	}

	retryUnavailable(): Promise<PolicyAdmissionResultV2> {
		return this.enqueueAdmission(() => this.retryUnavailableOne());
	}

	private async ingestOne(entry: unknown): Promise<PolicyAdmissionResultV2> {
		let snapshot: ValidatedPolicySnapshotV2;
		try {
			snapshot = await authenticatePolicySnapshotEntryV2(
				entry,
				this.descriptor,
			);
		} catch (error) {
			return this.result("rejected", validationMessage(error));
		}

		if (this.fork !== undefined) {
			this.observeAfterFork(snapshot);
			return this.result(
				"halted",
				"Policy reducer is halted by authority equivocation",
			);
		}
		this.retainCanonicalHeadEntry(snapshot);

		if (this.unavailable !== undefined) {
			if (this.acceptedHead?.digestKey === snapshot.digestKey) {
				return this.result("unavailable", this.unavailable.reason);
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
		if (evaluation.status === "reject") {
			return this.result("rejected", evaluation.reason);
		}
		if (evaluation.status === "duplicate") {
			return this.result("duplicate");
		}
		if (evaluation.status === "fork") {
			this.setFork(evaluation);
			return this.forkedResult();
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
					? "Policy parent is missing"
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
		const pending = this.pending.get(unavailable.candidateDigestKey);
		if (pending === undefined) {
			return this.result(
				"capacity",
				"Unavailable comparison candidate is not retained; re-ingest it before retrying",
			);
		}

		const evaluation = await this.evaluate(pending.snapshot);
		if (evaluation.status === "unavailable") {
			pending.missingParentDigest = copyBytes(evaluation.digest);
			this.unavailable = {
				acceptedAncestorDigest: copyBytes(evaluation.digest),
				candidateDigestKey: pending.snapshot.digestKey,
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
			reason = "Policy parent is missing";
		} else {
			this.pending.delete(pending.snapshot.digestKey);
			if (evaluation.status === "fork") {
				this.setFork(evaluation);
				return this.forkedResult();
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
