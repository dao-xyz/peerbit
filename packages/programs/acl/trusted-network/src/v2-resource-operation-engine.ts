import { deserialize, serialize } from "@dao-xyz/borsh";
import type { BoundedEntryV0CausalReachabilityLimits } from "@peerbit/log";
import {
	type BoundedEntryV0CausalReachabilityResolver,
	checkBoundedEntryV0CausalReachability,
} from "@peerbit/log";
import { compare, equals } from "uint8arrays";
import {
	type AcceptedResourceFenceLeaseV2,
	type ResourceFenceLeaseResultV2,
	TrustedNetworkV2DurableResourceFenceReducer,
} from "./v2-resource-fence-anchor.js";
import type { ResourceFenceFetchHintV2 } from "./v2-resource-fence-engine.js";
import {
	type AuthenticatedResourceOperationEntryV2,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_DIRECT_PARENTS,
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES,
	authenticateResourceOperationEntryV2,
	readAuthenticatedResourceOperationEntryV2,
} from "./v2-resource-operation-entry.js";
import {
	NetworkDescriptorV2,
	TrustedNetworkRole,
	assertNetworkDescriptorV2,
	copyUint8ArrayWithLengthV2,
	exactUint8ArrayByteLengthV2,
} from "./v2.js";

export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_HINTS = 64;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_ENTRIES = 1_024;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_BYTES =
	16 * 1024 * 1024;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_LINKS = 64 * 1024;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_FENCE_STEPS = 64;
export const TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_POLICY_STEPS = 4_096;
export const TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_OPERATION_AUTHORIZATIONS = 64;
export const TRUSTED_NETWORK_V2_MAX_QUEUED_RESOURCE_OPERATION_INPUT_BYTES =
	TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES * 4;

const DEFAULT_OPERATION_TIMEOUT_MS = 10_000;
const MAX_OPERATION_TIMEOUT_MS = 60_000;
const MAX_TIMER_DELAY_MS = 0x7fffffff;
const MAX_REASON_CHARACTERS = 512;

const CAUSAL_LIMITS: Readonly<BoundedEntryV0CausalReachabilityLimits> =
	Object.freeze({
		maxEntryBytes: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES,
		maxDirectParents: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_DIRECT_PARENTS,
		maxVisitedEntries: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_ENTRIES,
		maxTotalBytes: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_BYTES,
		maxParentLinks: TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_LINKS,
		maxResolveBatchSize:
			TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_DIRECT_PARENTS,
	});

const boundedReason = (reason: string): string =>
	reason.slice(0, MAX_REASON_CHARACTERS);

const validationMessage = (error: unknown): string =>
	error instanceof Error ? error.message : String(error);

const copyBytes = (bytes: Uint8Array): Uint8Array =>
	copyUint8ArrayWithLengthV2(bytes, exactUint8ArrayByteLengthV2(bytes));

const bytesKey = (bytes: Uint8Array): string => {
	let key = "";
	for (const byte of bytes) key += byte.toString(16).padStart(2, "0");
	return key;
};

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

export type ResourceOperationAuthorizationStatusV2 =
	| "policy-final"
	| "provisional"
	| "unavailable"
	| "rejected"
	| "halted";

export type ResourceOperationAuthorizationResultV2 = Readonly<{
	status: ResourceOperationAuthorizationStatusV2;
	reason?: string;
	entryCid?: string;
	applicationPayload?: Uint8Array;
	fetchHints: readonly ResourceFenceFetchHintV2[];
}>;

export type ResourceOperationAuthorizationOptionsV2 = Readonly<{
	/** Absolute wall-clock deadline, in milliseconds since the Unix epoch. */
	deadline?: number;
	/** Relative deadline including authentication and both anchor queues. */
	timeoutMs?: number;
	/** Cancels this evaluation without halting either anchor. */
	signal?: AbortSignal;
}>;

export type TrustedNetworkV2ResourceOperationEngineProperties = Readonly<{
	descriptor: NetworkDescriptorV2;
	expectedResourceId: Uint8Array;
	expectedGid: string;
	fenceAnchor: TrustedNetworkV2DurableResourceFenceReducer;
	resolveEntryV0: BoundedEntryV0CausalReachabilityResolver;
	operationTimeoutMs?: number;
	maxFenceSteps?: number;
	maxPolicySteps?: number;
	signal?: AbortSignal;
}>;

type OperationBudgetV2 = {
	signal: AbortSignal;
	deadline: number;
	dispose: () => void;
};

type CausalResultV2 = Awaited<
	ReturnType<typeof checkBoundedEntryV0CausalReachability>
>;

/**
 * Internal deterministic operation classifier. It has no projection or
 * replay state and therefore cannot activate TrustedNetwork v2 by itself.
 */
export class TrustedNetworkV2ResourceOperationEngine {
	private readonly descriptor: NetworkDescriptorV2;
	private readonly expectedResourceId: Uint8Array;
	private readonly expectedGid: string;
	private readonly fenceAnchor: TrustedNetworkV2DurableResourceFenceReducer;
	private readonly resolveEntryV0: BoundedEntryV0CausalReachabilityResolver;
	private readonly operationTimeoutMs: number;
	private readonly maxFenceSteps: number;
	private readonly maxPolicySteps: number;
	private readonly lifecycleController = new AbortController();
	private externalSignal?: AbortSignal;
	private externalAbortListener?: () => void;
	private bufferedOperations = 0;
	private bufferedInputBytes = 0;
	private outstandingResolvers = 0;

	constructor(properties: TrustedNetworkV2ResourceOperationEngineProperties) {
		const suppliedResourceId = properties.expectedResourceId;
		assertNetworkDescriptorV2(properties.descriptor);
		if (typeof properties.expectedGid !== "string") {
			throw new TypeError("Expected resource gid must be a string");
		}
		if (typeof properties.resolveEntryV0 !== "function") {
			throw new TypeError("resolveEntryV0 must be a function");
		}
		if (
			properties.fenceAnchor === null ||
			typeof properties.fenceAnchor !== "object" ||
			typeof properties.fenceAnchor.withAcceptedFenceLease !== "function"
		) {
			throw new TypeError(
				"A durable resource-fence lease provider is required",
			);
		}
		if (exactUint8ArrayByteLengthV2(suppliedResourceId) !== 32) {
			throw new Error("Expected resource id must contain exactly 32 bytes");
		}
		this.descriptor = deserialize(
			serialize(properties.descriptor),
			NetworkDescriptorV2,
		);
		this.expectedResourceId = copyUint8ArrayWithLengthV2(
			suppliedResourceId,
			32,
		);
		this.expectedGid = properties.expectedGid;
		this.fenceAnchor = properties.fenceAnchor;
		this.resolveEntryV0 = properties.resolveEntryV0;
		this.operationTimeoutMs =
			properties.operationTimeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
		if (
			!Number.isSafeInteger(this.operationTimeoutMs) ||
			this.operationTimeoutMs < 1 ||
			this.operationTimeoutMs > MAX_OPERATION_TIMEOUT_MS
		) {
			throw new RangeError(
				`operationTimeoutMs must be between 1 and ${MAX_OPERATION_TIMEOUT_MS}`,
			);
		}
		this.maxFenceSteps =
			properties.maxFenceSteps ??
			TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_FENCE_STEPS;
		if (
			!Number.isSafeInteger(this.maxFenceSteps) ||
			this.maxFenceSteps < 0 ||
			this.maxFenceSteps > TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_FENCE_STEPS
		) {
			throw new RangeError(
				`maxFenceSteps must be between 0 and ${TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_FENCE_STEPS}`,
			);
		}
		this.maxPolicySteps =
			properties.maxPolicySteps ??
			TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_POLICY_STEPS;
		if (
			!Number.isSafeInteger(this.maxPolicySteps) ||
			this.maxPolicySteps < 0 ||
			this.maxPolicySteps >
				TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_POLICY_STEPS
		) {
			throw new RangeError(
				`maxPolicySteps must be between 0 and ${TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_POLICY_STEPS}`,
			);
		}
		if (properties.signal?.aborted) {
			this.lifecycleController.abort();
		} else if (properties.signal !== undefined) {
			if (!isAbortSignalV2(properties.signal)) {
				throw new TypeError("Engine signal must be an AbortSignal");
			}
			this.externalSignal = properties.signal;
			this.externalAbortListener = (): void => this.lifecycleController.abort();
			properties.signal.addEventListener("abort", this.externalAbortListener, {
				once: true,
			});
		}
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

	get bufferedAuthorizationCount(): number {
		return this.bufferedOperations;
	}

	get bufferedAuthorizationBytes(): number {
		return this.bufferedInputBytes;
	}

	async authorize(
		entryBytes: Uint8Array,
		options?: ResourceOperationAuthorizationOptionsV2,
	): Promise<ResourceOperationAuthorizationResultV2> {
		let inputBytes: number;
		try {
			inputBytes = exactUint8ArrayByteLengthV2(entryBytes);
		} catch {
			return this.result(
				"rejected",
				"Resource operation must use canonical EntryV0 bytes",
			);
		}
		if (
			inputBytes < 1 ||
			inputBytes > TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES
		) {
			return this.result(
				"rejected",
				`Resource operation EntryV0 must contain 1-${TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_ENTRY_BYTES} bytes`,
			);
		}
		if (!this.reserveOperation(inputBytes)) {
			return this.result(
				"unavailable",
				"Resource operation authorization queue is at its fixed capacity",
			);
		}
		let captured: Uint8Array;
		try {
			captured = copyUint8ArrayWithLengthV2(entryBytes, inputBytes);
		} catch (error) {
			this.releaseOperation(inputBytes);
			return this.result("rejected", validationMessage(error));
		}
		let budget: OperationBudgetV2;
		try {
			budget = this.beginBudget(options);
		} catch (error) {
			this.releaseOperation(inputBytes);
			return this.result("rejected", validationMessage(error));
		}
		try {
			if (this.lifecycleController.signal.aborted) {
				return this.result("halted", "Resource operation engine is aborted");
			}
			if (budget.signal.aborted) {
				return this.result(
					"unavailable",
					"Resource operation authorization deadline elapsed or was cancelled",
				);
			}
			let token: AuthenticatedResourceOperationEntryV2;
			try {
				token = await authenticateResourceOperationEntryV2({
					entryBytes: captured,
					descriptor: this.descriptor,
					expectedResourceId: this.expectedResourceId,
					expectedGid: this.expectedGid,
				});
			} catch (error) {
				return this.result("rejected", validationMessage(error));
			}
			const snapshot = readAuthenticatedResourceOperationEntryV2(token);
			if (snapshot === undefined) {
				return this.result(
					"halted",
					"Authenticated resource operation token provenance was lost",
				);
			}
			if (this.lifecycleController.signal.aborted) {
				return this.result("halted", "Resource operation engine is aborted");
			}
			if (budget.signal.aborted || Date.now() >= budget.deadline) {
				return this.result(
					"unavailable",
					"Resource operation authorization deadline elapsed or was cancelled",
				);
			}
			const proof = snapshot.envelope.policy;
			const leased = await this.fenceAnchor.withAcceptedFenceLease(
				{
					fenceDigest: proof.fenceDigest,
					policy: {
						sequence: proof.policySequence,
						digest: proof.policyDigest,
					},
					maxFenceSteps: this.maxFenceSteps,
					maxPolicySteps: this.maxPolicySteps,
					deadline: budget.deadline,
					signal: budget.signal,
				},
				(lease) => this.authorizeUnderLease(snapshot, lease, budget),
			);
			return this.interruptionResult(budget) ?? this.mapLeaseResult(leased);
		} finally {
			budget.dispose();
			this.releaseOperation(inputBytes);
		}
	}

	private async authorizeUnderLease(
		snapshot: NonNullable<
			ReturnType<typeof readAuthenticatedResourceOperationEntryV2>
		>,
		lease: AcceptedResourceFenceLeaseV2,
		budget: OperationBudgetV2,
	): Promise<ResourceOperationAuthorizationResultV2> {
		const proof = snapshot.envelope.policy;
		if (
			!equals(proof.fenceDigest, lease.fence.head.digest) ||
			proof.policySequence !== lease.fence.head.policy.sequence ||
			!equals(proof.policyDigest, lease.fence.head.policy.digest) ||
			proof.contentEpoch !== lease.fence.head.contentEpoch
		) {
			return this.result(
				"rejected",
				"Resource operation proof does not exactly match its resource fence",
			);
		}
		if (
			!equals(
				snapshot.envelope.epochManifestDigest,
				lease.fence.head.epochManifestDigest,
			)
		) {
			return this.result(
				"rejected",
				"Resource operation is bound to another epoch manifest",
			);
		}
		// Canonical policy bindings are already sorted. Resolve at most eight
		// signers without a per-operation membership scan or role cache.
		const hasWriter = snapshot.verifiedSignerKeyBytes.some((key) => {
			let lower = 0;
			let upper = lease.policy.bindings.length;
			while (lower < upper) {
				const middle = (lower + upper) >>> 1;
				const binding = lease.policy.bindings[middle]!;
				const order = compare(serialize(binding.signingKey), key);
				if (order === 0)
					return (binding.roles & TrustedNetworkRole.WRITER) !== 0;
				if (order < 0) lower = middle + 1;
				else upper = middle;
			}
			return false;
		});
		if (!hasWriter) {
			return this.result(
				"rejected",
				"Resource operation has no verified signer with WRITER in its policy",
			);
		}

		const descendsFence = await this.causalRelation(
			lease.fence.head.entryCid,
			{ cid: snapshot.entryCid, bytes: snapshot.entryBytes },
			budget,
		);
		const descentInterruption = this.interruptionResult(budget);
		if (descentInterruption) return descentInterruption;
		if (descendsFence.status !== "ancestor") {
			if (
				descendsFence.status === "incomplete" ||
				descendsFence.status === "capacity"
			) {
				return this.causalUnavailable(
					descendsFence,
					"Operation-to-fence ancestry is unavailable",
				);
			}
			return this.result(
				"rejected",
				"Resource operation does not causally descend from its named fence",
			);
		}

		const closing = lease.closingFence;
		if (closing === undefined) {
			return this.acceptedResult("provisional", snapshot);
		}
		const [operationBeforeClosing, operationAfterClosing] = await Promise.all([
			this.causalRelation(
				snapshot.entryCid,
				{ cid: closing.head.entryCid, bytes: closing.entryBytes },
				budget,
			),
			this.causalRelation(
				closing.head.entryCid,
				{ cid: snapshot.entryCid, bytes: snapshot.entryBytes },
				budget,
			),
		]);
		// One positive ancestry proof must not hide cancellation of the other
		// walk. The caller's budget covers completion of the whole classification.
		const closingInterruption = this.interruptionResult(budget);
		if (closingInterruption) return closingInterruption;
		if (
			operationBeforeClosing.status === "ancestor" &&
			operationAfterClosing.status === "ancestor"
		) {
			return this.result(
				"rejected",
				"Resource operation causal relation is cyclic",
			);
		}
		if (operationBeforeClosing.status === "ancestor") {
			return this.acceptedResult("policy-final", snapshot);
		}
		if (operationAfterClosing.status === "ancestor") {
			return this.result(
				"rejected",
				"Old-fence resource operation causally follows its closing fence",
			);
		}
		const unresolved = [operationBeforeClosing, operationAfterClosing].filter(
			(result) =>
				result.status === "incomplete" || result.status === "capacity",
		);
		if (unresolved.length > 0) {
			return this.causalUnavailableMany(
				unresolved,
				"Operation-to-closing-fence relation is unavailable",
			);
		}
		return this.result(
			"rejected",
			"Old-fence resource operation is concurrent with its closing fence",
		);
	}

	private interruptionResult(
		budget: OperationBudgetV2,
	): ResourceOperationAuthorizationResultV2 | undefined {
		if (this.lifecycleController.signal.aborted) {
			return this.result("halted", "Resource operation engine is aborted");
		}
		if (budget.signal.aborted || Date.now() >= budget.deadline) {
			return this.result(
				"unavailable",
				"Resource operation authorization deadline elapsed or was cancelled",
			);
		}
		return undefined;
	}

	private async causalRelation(
		ancestorCid: string,
		descendant: { cid: string; bytes: Uint8Array },
		budget: OperationBudgetV2,
	): Promise<CausalResultV2> {
		if (budget.signal.aborted || Date.now() >= budget.deadline) {
			return {
				status: "capacity",
				visited: { entries: 0, bytes: 0, parentLinks: 0, resolverCalls: 0 },
			};
		}
		try {
			return await checkBoundedEntryV0CausalReachability({
				ancestorCid,
				descendant,
				resolve: async (cids, options) => {
					// Ignoring AbortSignal must not permit unlimited abandoned calls
					// across repeated timeouts. Keep the slot until actual settlement.
					if (
						this.outstandingResolvers >=
						TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_OPERATION_AUTHORIZATIONS
					)
						return new Map();
					this.outstandingResolvers += 1;
					try {
						return await this.resolveEntryV0(cids, options);
					} finally {
						this.outstandingResolvers -= 1;
					}
				},
				limits: CAUSAL_LIMITS,
				signal: budget.signal,
			});
		} catch {
			return {
				status: "capacity",
				visited: { entries: 0, bytes: 0, parentLinks: 0, resolverCalls: 0 },
			};
		}
	}

	private causalUnavailable(
		causal: CausalResultV2,
		reason: string,
	): ResourceOperationAuthorizationResultV2 {
		return this.causalUnavailableMany([causal], reason);
	}

	private causalUnavailableMany(
		causal: readonly CausalResultV2[],
		reason: string,
	): ResourceOperationAuthorizationResultV2 {
		const missing = new Set<string>();
		for (const relation of causal) {
			if (relation.status !== "incomplete") continue;
			for (const cid of relation.missingCids) missing.add(cid);
		}
		const hints = [...missing]
			.sort()
			.slice(0, TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_HINTS)
			.map((cid) => ({ kind: "causal-entry" as const, cid }));
		return this.result("unavailable", reason, hints);
	}

	private acceptedResult(
		status: "policy-final" | "provisional",
		snapshot: NonNullable<
			ReturnType<typeof readAuthenticatedResourceOperationEntryV2>
		>,
	): ResourceOperationAuthorizationResultV2 {
		return {
			status,
			entryCid: snapshot.entryCid,
			applicationPayload: copyBytes(snapshot.envelope.applicationPayload),
			fetchHints: [],
		};
	}

	private mapLeaseResult(
		leased: ResourceFenceLeaseResultV2<ResourceOperationAuthorizationResultV2>,
	): ResourceOperationAuthorizationResultV2 {
		if (leased.status === "completed") return leased.value;
		return this.result(
			leased.status === "capacity" ? "unavailable" : leased.status,
			leased.reason,
			leased.fetchHints,
		);
	}

	private result(
		status: ResourceOperationAuthorizationStatusV2,
		reason?: string,
		fetchHints: readonly ResourceFenceFetchHintV2[] = [],
	): ResourceOperationAuthorizationResultV2 {
		const canonicalHints = new Map<string, ResourceFenceFetchHintV2>();
		for (const hint of fetchHints) {
			const key =
				hint.kind === "causal-entry"
					? `1:${hint.cid}`
					: `0:${bytesKey(hint.digest)}`;
			if (canonicalHints.has(key)) continue;
			canonicalHints.set(
				key,
				hint.kind === "causal-entry"
					? { kind: "causal-entry", cid: hint.cid }
					: {
							kind: "resource-fence-predecessor",
							digest: copyBytes(hint.digest),
						},
			);
		}
		return {
			status,
			reason: reason === undefined ? undefined : boundedReason(reason),
			fetchHints: [...canonicalHints.entries()]
				.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
				.slice(0, TRUSTED_NETWORK_V2_MAX_RESOURCE_OPERATION_CAUSAL_HINTS)
				.map(([, hint]) => hint),
		};
	}

	private beginBudget(
		options: ResourceOperationAuthorizationOptionsV2 | undefined,
	): OperationBudgetV2 {
		const timeoutMs = options?.timeoutMs ?? this.operationTimeoutMs;
		if (
			!Number.isSafeInteger(timeoutMs) ||
			timeoutMs < 0 ||
			timeoutMs > MAX_OPERATION_TIMEOUT_MS
		) {
			throw new RangeError(
				`Resource operation timeoutMs must be between 0 and ${MAX_OPERATION_TIMEOUT_MS}`,
			);
		}
		const suppliedDeadline = options?.deadline;
		if (
			suppliedDeadline !== undefined &&
			(!Number.isSafeInteger(suppliedDeadline) || suppliedDeadline < 0)
		) {
			throw new RangeError(
				"Resource operation deadline must be a non-negative safe integer",
			);
		}
		const callerSignal = options?.signal;
		if (callerSignal !== undefined && !isAbortSignalV2(callerSignal)) {
			throw new TypeError("Resource operation signal must be an AbortSignal");
		}
		const relativeDeadline = Math.min(
			Number.MAX_SAFE_INTEGER,
			Date.now() + timeoutMs,
		);
		const deadline =
			suppliedDeadline === undefined
				? relativeDeadline
				: Math.min(suppliedDeadline, relativeDeadline);
		const controller = new AbortController();
		const abort = (): void => controller.abort();
		this.lifecycleController.signal.addEventListener("abort", abort, {
			once: true,
		});
		callerSignal?.addEventListener("abort", abort, { once: true });
		if (
			this.lifecycleController.signal.aborted ||
			callerSignal?.aborted ||
			Date.now() >= deadline
		) {
			controller.abort();
		}
		const remaining = Math.max(0, deadline - Date.now());
		const timeout = setTimeout(abort, Math.min(remaining, MAX_TIMER_DELAY_MS));
		let disposed = false;
		return {
			signal: controller.signal,
			deadline,
			dispose: (): void => {
				if (disposed) return;
				disposed = true;
				clearTimeout(timeout);
				this.lifecycleController.signal.removeEventListener("abort", abort);
				callerSignal?.removeEventListener("abort", abort);
			},
		};
	}

	private reserveOperation(inputBytes: number): boolean {
		if (
			this.bufferedOperations >=
				TRUSTED_NETWORK_V2_MAX_PENDING_RESOURCE_OPERATION_AUTHORIZATIONS ||
			inputBytes >
				TRUSTED_NETWORK_V2_MAX_QUEUED_RESOURCE_OPERATION_INPUT_BYTES -
					this.bufferedInputBytes
		) {
			return false;
		}
		this.bufferedOperations += 1;
		this.bufferedInputBytes += inputBytes;
		return true;
	}

	private releaseOperation(inputBytes: number): void {
		this.bufferedOperations -= 1;
		this.bufferedInputBytes -= inputBytes;
	}
}
