import { type PublicSignKey, randomBytes } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import type { RPC } from "@peerbit/rpc";
import {
	AcknowledgeDelivery,
	CONVERGENCE_MESSAGE_PRIORITY,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError } from "@peerbit/time";
import type { TransportMessage } from "./message.js";
import type { ReplicationRangeIndexable } from "./ranges.js";
import type { ReplicationInfoMutation } from "./replication-info-mutation.js";
import { deriveReplicationInfoV2ReceiverBinding } from "./replication-info-v2-binding.js";
import {
	AddedReplicationInfoV2Message,
	FullReplicationInfoV2Message,
	ReplicationInfoV2AppliedMessage,
	RequestReplicationInfoV2AppliedMessage,
	RequestReplicationInfoV2Message,
	StoppedReplicationInfoV2Message,
} from "./replication.js";

const logger = loggerFn("peerbit:shared-log:replication-info-v2-send");

const MAX_U64 = (1n << 64n) - 1n;
const DEFAULT_SEND_RETRY_MS = 1_000;
const DEFAULT_MAX_SEND_RETRY_MS = 30_000;
const DEFAULT_CONFIRM_RETRY_MS = 1_000;
const DEFAULT_CONFIRM_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_CONFIRMATION_WAITERS = 1_024;
const MAX_TIMER_MS = 2_147_483_647;
const MAX_BACKOFF_EXPONENT = 20;

export { deriveReplicationInfoV2ReceiverBinding } from "./replication-info-v2-binding.js";

type SendRequest =
	| { kind: "snapshot"; revision: bigint }
	| {
			kind: "mutation";
			mutation: ReplicationInfoMutation;
			revision: bigint;
	  };

type ApplicationConfirmationRequest = {
	sequence: bigint;
	revision: bigint;
	controller: AbortController;
};

type ApplicationConfirmationTarget = {
	peerHash: string;
	peerSession: object;
	receiverTransportSession: bigint;
};

type ApplicationConfirmationWaiter = {
	revision: bigint;
	minPeers: number;
	startedAt: number;
	target?: ApplicationConfirmationTarget;
	resolve: () => void;
	reject: (error: unknown) => void;
	timer?: ReturnType<typeof setTimeout>;
	signal?: AbortSignal;
	onAbort?: () => void;
};

export type ReplicationInfoV2SendDiagnostic = Readonly<{
	state: "absent" | "current" | "stale";
	/** Current-state classification; no historical packet/drop log is retained. */
	reason:
		| "state-absent"
		| "state-retiring"
		| "peer-session-mismatch"
		| "transport-session-mismatch"
		| "destination-not-current"
		| "destination-not-open"
		| "ownership-inactive"
		| "retry-backoff"
		| "send-in-flight"
		| "send-pending"
		| "confirmation-in-flight"
		| "latest-applied"
		| "latest-unconfirmed";
	established?: boolean;
	suspended?: boolean;
	currentRevision: string;
	pendingRevision?: string;
	inFlightRevision?: string;
	inFlightSequence?: string;
	appliedRevision?: string;
	confirmationRevision?: string;
	confirmationSequence?: string;
	retryAttempts?: number;
	confirmationWaiters: number;
	oldestConfirmationAgeMs?: number;
}>;

export type ReplicationInfoV2ConfirmationOptions = {
	/** Distinct current peers that must report durable application. Defaults to 1. */
	minPeers?: number;
	/** Maximum wait in milliseconds. Defaults to 30 seconds. */
	timeout?: number;
	signal?: AbortSignal;
};

export type ReplicationInfoV2SendState = {
	peerHash: string;
	target: PublicSignKey;
	peerSession: object;
	receiverTransportSession: bigint;
	senderTransportSession: bigint;
	capabilityTimestamp: bigint;
	lastRequestTimestamp: bigint;
	receiverRequestChallenge: Uint8Array;
	receiverChallenge: Uint8Array;
	senderEpoch: Uint8Array;
	ownershipLifecycleController: AbortController;
	ownershipAbortListener: () => void;
	nextSequence: bigint;
	established: boolean;
	suspended: boolean;
	inFlightSequence?: bigint;
	inFlightRevision?: bigint;
	retryTimer?: ReturnType<typeof setTimeout>;
	retryAttempts: number;
	controller: AbortController;
	pending?: SendRequest;
	worker?: Promise<void>;
	applicationConfirmationRequest?: ApplicationConfirmationRequest;
	appliedRevision?: bigint;
};

export type ReplicationInfoV2SendDeps<R extends "u32" | "u64"> = {
	getRpc: () => RPC<TransportMessage, TransportMessage>;
	getSelfKey: () => PublicSignKey;
	getSenderTransportSession: () => bigint;
	getMyReplicationSegments: () => Promise<ReplicationRangeIndexable<R>[]>;
	validatePersistedReplicationRangeSnapshot: (
		ranges: readonly { mode: unknown }[],
	) => void;
	isClosed: () => boolean;
	isPeerSessionCurrent: (peerHash: string, peerSession: object) => boolean;
	isPeerSessionOpen: (peerHash: string, peerSession: object) => boolean;
	captureReplicationOwnershipLifecycle: () => AbortController;
	isReplicationOwnershipLifecycleActive: (
		controller: AbortController,
	) => boolean;
	supportsApplicationConfirmation?: (
		peerHash: string,
		receiverTransportSession: bigint,
	) => boolean;
	sendRetryMs?: number;
	maxSendRetryMs?: number;
	confirmationRetryMs?: number;
	maxConfirmationWaiters?: number;
};

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
	if (left.byteLength !== right.byteLength) {
		return false;
	}
	for (let index = 0; index < left.byteLength; index++) {
		if (left[index] !== right[index]) {
			return false;
		}
	}
	return true;
};

/**
 * Per-destination V2 sender streams. Each stream retains at most one in-flight
 * operation and one pending operation. A second pending mutation coalesces to
 * a freshly collected authoritative snapshot, bounding memory at O(peers).
 */
export class ReplicationInfoV2SendCoordinator<R extends "u32" | "u64"> {
	_senderEpoch!: Uint8Array;
	_sendStates!: Map<string, ReplicationInfoV2SendState>;
	_spentPeerSessions!: WeakSet<object>;
	_retiringWorkersByPeer!: Map<string, Promise<void>>;
	_confirmations!: Set<ApplicationConfirmationWaiter>;
	_confirmationRetryTimer?: ReturnType<typeof setTimeout>;
	// design-note: This is application-state identity, not an async-lifecycle
	// fence. One transport session can carry several committed replication
	// assignments, so session identity cannot distinguish which assignment a
	// remote applied; the monotonic value also lets superseded waiters coalesce.
	_revision!: bigint;

	private readonly sendRetryMs: number;
	private readonly maxSendRetryMs: number;
	private readonly confirmationRetryMs: number;
	private readonly maxConfirmationWaiters: number;

	constructor(private readonly deps: ReplicationInfoV2SendDeps<R>) {
		this.sendRetryMs = Math.max(1, deps.sendRetryMs ?? DEFAULT_SEND_RETRY_MS);
		this.maxSendRetryMs = Math.max(
			this.sendRetryMs,
			deps.maxSendRetryMs ?? DEFAULT_MAX_SEND_RETRY_MS,
		);
		this.confirmationRetryMs = Math.max(
			1,
			deps.confirmationRetryMs ?? DEFAULT_CONFIRM_RETRY_MS,
		);
		const configuredMaxConfirmationWaiters =
			deps.maxConfirmationWaiters ?? DEFAULT_MAX_CONFIRMATION_WAITERS;
		this.maxConfirmationWaiters =
			Number.isSafeInteger(configuredMaxConfirmationWaiters) &&
			configuredMaxConfirmationWaiters > 0
				? configuredMaxConfirmationWaiters
				: DEFAULT_MAX_CONFIRMATION_WAITERS;
		this._senderEpoch = randomBytes(32);
		this._sendStates = new Map();
		this._spentPeerSessions = new WeakSet();
		this._retiringWorkersByPeer = new Map();
		this._confirmations = new Set();
		this._revision = 0n;
	}

	resetForOpen(): void {
		this.clearForClose();
		this._senderEpoch = randomBytes(32);
		this._sendStates = new Map();
		this._spentPeerSessions = new WeakSet();
		this._revision = 0n;
	}

	clearForClose(): void {
		this.rejectConfirmations(new AbortError("Replication-info sender closed"));
		for (const state of [...(this._sendStates?.values() ?? [])]) {
			this.clearState(state);
		}
		this._sendStates?.clear();
	}

	clearPeer(peerHash: string, expectedSession?: object): void {
		this.rejectTargetConfirmations(
			{ peerHash, peerSession: expectedSession },
			new AbortError("Replication confirmation destination changed"),
		);
		const state = this._sendStates.get(peerHash);
		if (!state || (expectedSession && state.peerSession !== expectedSession)) {
			return;
		}
		this.clearState(state);
	}

	advancePeerCapability(peerHash: string): void {
		this.rejectTargetConfirmations(
			{ peerHash },
			new AbortError("Replication confirmation destination changed"),
		);
		const state = this._sendStates.get(peerHash);
		if (state) {
			this.clearState(state);
		}
	}

	private clearState(state: ReplicationInfoV2SendState): void {
		this.trackRetiringWorker(state);
		this.rejectTargetConfirmations(
			{
				peerHash: state.peerHash,
				peerSession: state.peerSession,
				receiverTransportSession: state.receiverTransportSession,
			},
			new AbortError("Replication confirmation destination changed"),
		);
		if (state.retryTimer) {
			clearTimeout(state.retryTimer);
			state.retryTimer = undefined;
		}
		state.ownershipLifecycleController.signal.removeEventListener(
			"abort",
			state.ownershipAbortListener,
		);
		state.controller.abort();
		state.applicationConfirmationRequest?.controller.abort();
		state.applicationConfirmationRequest = undefined;
		state.appliedRevision = undefined;
		if (this._sendStates.get(state.peerHash) === state) {
			this._sendStates.delete(state.peerHash);
		}
	}

	private supportsApplicationConfirmation(
		state: ReplicationInfoV2SendState,
	): boolean {
		return (
			this.deps.supportsApplicationConfirmation?.(
				state.peerHash,
				state.receiverTransportSession,
			) === true
		);
	}

	private clearConfirmationWaiter(waiter: ApplicationConfirmationWaiter): void {
		if (!this._confirmations.delete(waiter)) {
			return;
		}
		if (waiter.timer) {
			clearTimeout(waiter.timer);
			waiter.timer = undefined;
		}
		if (waiter.signal && waiter.onAbort) {
			waiter.signal.removeEventListener("abort", waiter.onAbort);
		}
		waiter.onAbort = undefined;
		for (const state of this._sendStates.values()) {
			const requested = state.applicationConfirmationRequest;
			if (requested && !this.hasConfirmationFor(state, requested.revision)) {
				state.applicationConfirmationRequest = undefined;
				requested.controller.abort(
					new AbortError("Replication confirmation no longer pending"),
				);
			}
		}
		if (this._confirmations.size === 0 && this._confirmationRetryTimer) {
			clearTimeout(this._confirmationRetryTimer);
			this._confirmationRetryTimer = undefined;
		}
	}

	private rejectConfirmations(error: unknown): void {
		if (this._confirmationRetryTimer) {
			clearTimeout(this._confirmationRetryTimer);
			this._confirmationRetryTimer = undefined;
		}
		for (const waiter of [...(this._confirmations ?? [])]) {
			this.clearConfirmationWaiter(waiter);
			waiter.reject(error);
		}
	}

	private rejectTargetConfirmations(
		target: {
			peerHash: string;
			peerSession?: object;
			receiverTransportSession?: bigint;
		},
		error: unknown,
	): void {
		for (const waiter of [...this._confirmations]) {
			if (
				waiter.target === undefined ||
				waiter.target.peerHash !== target.peerHash ||
				(target.peerSession !== undefined &&
					waiter.target.peerSession !== target.peerSession) ||
				(target.receiverTransportSession !== undefined &&
					waiter.target.receiverTransportSession !==
						target.receiverTransportSession)
			) {
				continue;
			}
			this.clearConfirmationWaiter(waiter);
			waiter.reject(error);
		}
	}

	private matchesConfirmationTarget(
		state: ReplicationInfoV2SendState,
		target: ApplicationConfirmationTarget | undefined,
	): boolean {
		return (
			target === undefined ||
			(state.peerHash === target.peerHash &&
				state.peerSession === target.peerSession &&
				state.receiverTransportSession === target.receiverTransportSession)
		);
	}

	private countAppliedPeers(waiter: ApplicationConfirmationWaiter): number {
		let applied = 0;
		for (const state of this._sendStates.values()) {
			if (
				state.appliedRevision !== undefined &&
				state.appliedRevision >= waiter.revision &&
				this.matchesConfirmationTarget(state, waiter.target) &&
				this.isDestinationReady(state) &&
				this.supportsApplicationConfirmation(state)
			) {
				applied++;
			}
		}
		return applied;
	}

	private settleConfirmations(): void {
		for (const waiter of [...this._confirmations]) {
			if (this.countAppliedPeers(waiter) >= waiter.minPeers) {
				this.clearConfirmationWaiter(waiter);
				waiter.resolve();
			}
		}
	}

	private hasConfirmationFor(
		state: ReplicationInfoV2SendState,
		revision: bigint,
	): boolean {
		for (const waiter of this._confirmations) {
			if (
				waiter.revision <= revision &&
				this.matchesConfirmationTarget(state, waiter.target)
			) {
				return true;
			}
		}
		return false;
	}

	private scheduleConfirmationRetry(): void {
		if (this._confirmations.size === 0 || this._confirmationRetryTimer) {
			return;
		}
		this._confirmationRetryTimer = setTimeout(() => {
			this._confirmationRetryTimer = undefined;
			this.nudgeConfirmations(true);
		}, this.confirmationRetryMs);
		this._confirmationRetryTimer.unref?.();
	}

	private nudgeConfirmations(forceReassert = false): void {
		this.settleConfirmations();
		if (this._confirmations.size === 0 || this.deps.isClosed()) {
			return;
		}
		for (const state of [...this._sendStates.values()]) {
			if (
				this.isCurrent(state) &&
				this.supportsApplicationConfirmation(state) &&
				this.hasConfirmationFor(state, this._revision)
			) {
				const revision = this._revision;
				if (
					(state.appliedRevision !== undefined &&
						state.appliedRevision >= revision) ||
					(state.inFlightRevision !== undefined &&
						state.inFlightRevision >= revision) ||
					(state.pending !== undefined && state.pending.revision >= revision) ||
					(!forceReassert &&
						state.applicationConfirmationRequest !== undefined &&
						state.applicationConfirmationRequest.revision >= revision)
				) {
					continue;
				}
				this.enqueueState(state, {
					kind: "snapshot",
					revision,
				});
			}
		}
		this.scheduleConfirmationRetry();
	}

	private waitForConfirmation(properties: {
		minPeers: number;
		target?: ApplicationConfirmationTarget;
		timeout?: number;
		signal?: AbortSignal;
		timeoutMessage: (revision: bigint) => string;
	}): Promise<void> {
		const timeout = properties.timeout ?? DEFAULT_CONFIRM_TIMEOUT_MS;
		if (
			!Number.isSafeInteger(timeout) ||
			timeout <= 0 ||
			timeout > MAX_TIMER_MS
		) {
			return Promise.reject(
				new RangeError(
					`Replication confirmation timeout must be an integer from 1 to ${MAX_TIMER_MS} milliseconds`,
				),
			);
		}
		if (this.deps.isClosed()) {
			return Promise.reject(new AbortError("Replication-info sender closed"));
		}
		if (properties.signal?.aborted) {
			return Promise.reject(
				properties.signal.reason instanceof Error
					? properties.signal.reason
					: new AbortError("Replication confirmation aborted"),
			);
		}
		if (this._confirmations.size >= this.maxConfirmationWaiters) {
			return Promise.reject(
				new RangeError(
					`Too many pending replication confirmations (maximum ${this.maxConfirmationWaiters})`,
				),
			);
		}

		return new Promise<void>((resolve, reject) => {
			const waiter: ApplicationConfirmationWaiter = {
				revision: this._revision,
				minPeers: properties.minPeers,
				startedAt: Date.now(),
				target: properties.target ? { ...properties.target } : undefined,
				resolve,
				reject,
				signal: properties.signal,
			};
			waiter.timer = setTimeout(() => {
				this.clearConfirmationWaiter(waiter);
				reject(new TimeoutError(properties.timeoutMessage(waiter.revision)));
			}, timeout);
			waiter.timer.unref?.();
			if (properties.signal) {
				waiter.onAbort = () => {
					this.clearConfirmationWaiter(waiter);
					reject(
						properties.signal!.reason instanceof Error
							? properties.signal!.reason
							: new AbortError("Replication confirmation aborted"),
					);
				};
				properties.signal.addEventListener("abort", waiter.onAbort, {
					once: true,
				});
			}
			this._confirmations.add(waiter);
			this.nudgeConfirmations();
		});
	}

	/**
	 * Bounded, read-only state for persisted-readiness troubleshooting. Opaque
	 * session/challenge values and peer maps are intentionally excluded.
	 */
	diagnosePeer(target: {
		peerHash: string;
		peerSession?: object;
		receiverTransportSession?: bigint;
	}): ReplicationInfoV2SendDiagnostic {
		const state = this._sendStates.get(target.peerHash);
		let matchingConfirmationCount = 0;
		let oldestStartedAt: number | undefined;
		for (const waiter of this._confirmations) {
			if (
				waiter.target?.peerHash !== target.peerHash ||
				(target.peerSession !== undefined &&
					waiter.target.peerSession !== target.peerSession) ||
				(target.receiverTransportSession !== undefined &&
					waiter.target.receiverTransportSession !==
						target.receiverTransportSession)
			) {
				continue;
			}
			matchingConfirmationCount++;
			oldestStartedAt =
				oldestStartedAt === undefined
					? waiter.startedAt
					: Math.min(oldestStartedAt, waiter.startedAt);
		}
		const common = {
			currentRevision: this._revision.toString(),
			confirmationWaiters: Math.min(
				this.maxConfirmationWaiters,
				matchingConfirmationCount,
			),
			...(oldestStartedAt === undefined
				? {}
				: {
						oldestConfirmationAgeMs: Math.min(
							MAX_TIMER_MS,
							Math.max(0, Date.now() - oldestStartedAt),
						),
					}),
		};
		if (!state) {
			return Object.freeze({
				state: this._retiringWorkersByPeer.has(target.peerHash)
					? ("stale" as const)
					: ("absent" as const),
				reason: this._retiringWorkersByPeer.has(target.peerHash)
					? ("state-retiring" as const)
					: ("state-absent" as const),
				...common,
			});
		}

		let status: "current" | "stale" = "current";
		let reason: ReplicationInfoV2SendDiagnostic["reason"];
		if (
			target.peerSession !== undefined &&
			state.peerSession !== target.peerSession
		) {
			status = "stale";
			reason = "peer-session-mismatch";
		} else if (
			target.receiverTransportSession !== undefined &&
			state.receiverTransportSession !== target.receiverTransportSession
		) {
			status = "stale";
			reason = "transport-session-mismatch";
		} else if (!this.isDestinationCurrent(state)) {
			status = "stale";
			reason = "destination-not-current";
		} else if (!this.isDestinationReady(state)) {
			reason = "destination-not-open";
		} else if (
			!this.deps.isReplicationOwnershipLifecycleActive(
				state.ownershipLifecycleController,
			)
		) {
			reason = "ownership-inactive";
		} else if (state.suspended || state.retryTimer !== undefined) {
			reason = "retry-backoff";
		} else if (state.applicationConfirmationRequest !== undefined) {
			reason = "confirmation-in-flight";
		} else if (state.inFlightRevision !== undefined) {
			reason = "send-in-flight";
		} else if (state.pending !== undefined) {
			reason = "send-pending";
		} else if (
			state.appliedRevision !== undefined &&
			state.appliedRevision >= this._revision
		) {
			reason = "latest-applied";
		} else {
			reason = "latest-unconfirmed";
		}

		return Object.freeze({
			state: status,
			reason,
			established: state.established,
			suspended: state.suspended,
			...common,
			...(state.pending === undefined
				? {}
				: { pendingRevision: state.pending.revision.toString() }),
			...(state.inFlightRevision === undefined
				? {}
				: { inFlightRevision: state.inFlightRevision.toString() }),
			...(state.inFlightSequence === undefined
				? {}
				: { inFlightSequence: state.inFlightSequence.toString() }),
			...(state.appliedRevision === undefined
				? {}
				: { appliedRevision: state.appliedRevision.toString() }),
			...(state.applicationConfirmationRequest === undefined
				? {}
				: {
						confirmationRevision:
							state.applicationConfirmationRequest.revision.toString(),
						confirmationSequence:
							state.applicationConfirmationRequest.sequence.toString(),
					}),
			retryAttempts: Math.min(
				MAX_BACKOFF_EXPONENT + 1,
				Math.max(0, state.retryAttempts),
			),
		});
	}

	/**
	 * Wait until the latest committed local replication revision is reported as
	 * durably applied by the configured number of current peers. Revisions newer
	 * than this call satisfy it, so rapid updates coalesce to one authoritative
	 * snapshot without weakening older callers' guarantees.
	 */
	confirmLatest(
		options: ReplicationInfoV2ConfirmationOptions = {},
	): Promise<void> {
		const minPeers = options.minPeers ?? 1;
		if (!Number.isSafeInteger(minPeers) || minPeers <= 0) {
			return Promise.reject(
				new RangeError("Replication confirmation minPeers must be positive"),
			);
		}
		return this.waitForConfirmation({
			minPeers,
			timeout: options.timeout,
			signal: options.signal,
			timeoutMessage: (revision) =>
				`Timed out waiting for ${minPeers} peer${minPeers === 1 ? "" : "s"} to apply replication revision ${revision}`,
		});
	}

	/**
	 * Confirm the latest revision on one exact destination generation. Multiple
	 * callers for that peer share the state's single snapshot/query worker, so
	 * persisted batches remain bounded by peers rather than entries.
	 */
	confirmLatestForPeer(
		target: ApplicationConfirmationTarget,
		options: Omit<ReplicationInfoV2ConfirmationOptions, "minPeers"> = {},
	): Promise<void> {
		if (
			!this.deps.isPeerSessionCurrent(target.peerHash, target.peerSession) ||
			!this.deps.isPeerSessionOpen(target.peerHash, target.peerSession) ||
			this.deps.supportsApplicationConfirmation?.(
				target.peerHash,
				target.receiverTransportSession,
			) !== true
		) {
			return Promise.reject(
				new AbortError("Replication confirmation destination is not current"),
			);
		}
		return this.waitForConfirmation({
			minPeers: 1,
			target,
			timeout: options.timeout,
			signal: options.signal,
			timeoutMessage: (revision) =>
				`Timed out waiting for peer ${target.peerHash} to apply replication revision ${revision}`,
		});
	}

	/**
	 * Read-only counterpart to {@link confirmLatestForPeer}. The target must be
	 * the exact current peer/transport generation and its latest local role
	 * revision must already have crossed the receiver's durable V2 apply lane.
	 */
	isLatestConfirmedForPeer(target: ApplicationConfirmationTarget): boolean {
		const state = this._sendStates.get(target.peerHash);
		return (
			state !== undefined &&
			state.peerSession === target.peerSession &&
			state.receiverTransportSession === target.receiverTransportSession &&
			state.appliedRevision !== undefined &&
			state.appliedRevision >= this._revision &&
			this.isCurrent(state) &&
			this.supportsApplicationConfirmation(state)
		);
	}

	/**
	 * Whether the exact current destination generation already has an outbound
	 * V2 stream. A missing/stale stream cannot be repaired by confirmation
	 * retries alone: the remote receiver must issue another Full request first.
	 */
	hasCurrentStateForPeer(target: ApplicationConfirmationTarget): boolean {
		const state = this._sendStates.get(target.peerHash);
		return (
			state !== undefined &&
			state.peerSession === target.peerSession &&
			state.receiverTransportSession === target.receiverTransportSession &&
			this.isCurrent(state) &&
			this.supportsApplicationConfirmation(state)
		);
	}

	private trackRetiringWorker(state: ReplicationInfoV2SendState): void {
		const worker = state.worker;
		if (!worker) {
			return;
		}
		const previous = this._retiringWorkersByPeer.get(state.peerHash);
		if (previous === worker) {
			return;
		}
		const retirement = previous
			? Promise.allSettled([previous, worker]).then(() => undefined)
			: worker;
		this._retiringWorkersByPeer.set(state.peerHash, retirement);
		const forget = () => {
			if (this._retiringWorkersByPeer.get(state.peerHash) === retirement) {
				this._retiringWorkersByPeer.delete(state.peerHash);
			}
		};
		void retirement.then(forget, forget);
	}

	private isDestinationCurrent(state: ReplicationInfoV2SendState): boolean {
		return (
			!this.deps.isClosed() &&
			!state.controller.signal.aborted &&
			this._sendStates.get(state.peerHash) === state &&
			this.deps.isPeerSessionCurrent(state.peerHash, state.peerSession) &&
			this.deps.getSenderTransportSession() === state.senderTransportSession
		);
	}

	private isDestinationReady(state: ReplicationInfoV2SendState): boolean {
		return (
			this.isDestinationCurrent(state) &&
			this.deps.isPeerSessionOpen(state.peerHash, state.peerSession)
		);
	}

	private isCurrent(state: ReplicationInfoV2SendState): boolean {
		return (
			this.isDestinationReady(state) &&
			this.deps.isReplicationOwnershipLifecycleActive(
				state.ownershipLifecycleController,
			)
		);
	}

	private retireSpentState(state: ReplicationInfoV2SendState): void {
		this._spentPeerSessions.add(state.peerSession);
		this.clearState(state);
	}

	/**
	 * Collapse every interrupted normal-send path to one authoritative snapshot.
	 * A closed readiness gate is temporary while the exact PeerSession remains
	 * current, so it parks instead of destroying the receiver binding. Sequence
	 * exhaustion is terminal even when readiness or ownership changed at the same
	 * time as an in-flight transport attempt.
	 */
	private parkSnapshotForRetry(state: ReplicationInfoV2SendState): void {
		state.inFlightSequence = undefined;
		state.inFlightRevision = undefined;
		state.applicationConfirmationRequest?.controller.abort();
		state.applicationConfirmationRequest = undefined;
		if (state.nextSequence > MAX_U64) {
			this.retireSpentState(state);
			return;
		}
		if (!this.isDestinationCurrent(state)) {
			this.clearState(state);
			return;
		}
		if (
			!this.deps.isReplicationOwnershipLifecycleActive(
				state.ownershipLifecycleController,
			)
		) {
			if (state.retryTimer) {
				clearTimeout(state.retryTimer);
				state.retryTimer = undefined;
			}
			state.pending = undefined;
			return;
		}

		state.suspended = true;
		state.pending = { kind: "snapshot", revision: this._revision };
		this.scheduleRetry(state);
	}

	/**
	 * Accept a signed receiver request. An exact newer retry asks for another
	 * full snapshot without resetting the epoch/sequence. A different challenge
	 * can replace the binding only after a strictly newer signed capability;
	 * retiring work is drained before the replacement can start.
	 */
	acceptRequest(
		request: RequestReplicationInfoV2Message,
		properties: {
			from: PublicSignKey;
			peerSession: object;
			receiverTransportSession: bigint;
			capabilityTimestamp: bigint;
			requestTimestamp: bigint;
		},
	): boolean {
		const self = this.deps.getSelfKey();
		const senderTransportSession = this.deps.getSenderTransportSession();
		if (
			properties.from.equals(self) ||
			!request.intendedSender.equals(self) ||
			request.senderSession !== senderTransportSession
		) {
			return false;
		}

		const peerHash = properties.from.hashcode();
		if (
			this._retiringWorkersByPeer.has(peerHash) ||
			this._spentPeerSessions.has(properties.peerSession) ||
			!this.deps.isPeerSessionCurrent(peerHash, properties.peerSession) ||
			!this.deps.isPeerSessionOpen(peerHash, properties.peerSession)
		) {
			return false;
		}

		let previous = this._sendStates.get(peerHash);
		if (
			previous &&
			(previous.peerSession !== properties.peerSession ||
				previous.senderTransportSession !== senderTransportSession)
		) {
			this.clearState(previous);
			previous = undefined;
			if (this._retiringWorkersByPeer.has(peerHash)) {
				return false;
			}
		}
		if (
			previous &&
			!this.deps.isReplicationOwnershipLifecycleActive(
				previous.ownershipLifecycleController,
			)
		) {
			// Ownership teardown retains the binding only so close/drop can send its
			// terminal empty Full. A later request must not revive normal delivery.
			return false;
		}
		if (previous) {
			const sameBinding =
				previous.peerSession === properties.peerSession &&
				previous.receiverTransportSession ===
					properties.receiverTransportSession &&
				bytesEqual(
					previous.receiverRequestChallenge,
					request.receiverChallenge,
				);
			if (sameBinding) {
				if (properties.requestTimestamp <= previous.lastRequestTimestamp) {
					return false;
				}
				previous.lastRequestTimestamp = properties.requestTimestamp;
				previous.capabilityTimestamp = properties.capabilityTimestamp;
				if (previous.retryTimer) {
					clearTimeout(previous.retryTimer);
					previous.retryTimer = undefined;
				}
				previous.suspended = false;
				previous.pending = {
					kind: "snapshot",
					revision: this._revision,
				};
				this.enqueueState(previous, {
					kind: "snapshot",
					revision: this._revision,
				});
				return true;
			}

			if (properties.capabilityTimestamp <= previous.capabilityTimestamp) {
				return false;
			}
			this.clearState(previous);
			previous = undefined;
			if (this._retiringWorkersByPeer.has(peerHash)) {
				return false;
			}
		}

		const ownershipLifecycleController =
			this.deps.captureReplicationOwnershipLifecycle();
		if (
			!this.deps.isReplicationOwnershipLifecycleActive(
				ownershipLifecycleController,
			)
		) {
			return false;
		}
		const state: ReplicationInfoV2SendState = {
			peerHash,
			target: properties.from,
			peerSession: properties.peerSession,
			receiverTransportSession: properties.receiverTransportSession,
			senderTransportSession,
			capabilityTimestamp: properties.capabilityTimestamp,
			lastRequestTimestamp: properties.requestTimestamp,
			receiverRequestChallenge: request.receiverChallenge.slice(),
			receiverChallenge: deriveReplicationInfoV2ReceiverBinding({
				receiverChallenge: request.receiverChallenge,
				receiver: properties.from,
				receiverTransportSession: properties.receiverTransportSession,
				sender: self,
				senderTransportSession,
			}),
			senderEpoch: this._senderEpoch.slice(),
			ownershipAbortListener: () => {},
			nextSequence: 1n,
			established: false,
			suspended: false,
			retryAttempts: 0,
			controller: new AbortController(),
			ownershipLifecycleController,
		};
		state.ownershipAbortListener = () => {
			if (state.retryTimer) {
				clearTimeout(state.retryTimer);
				state.retryTimer = undefined;
			}
			state.pending = undefined;
		};
		ownershipLifecycleController.signal.addEventListener(
			"abort",
			state.ownershipAbortListener,
			{ once: true },
		);
		this._sendStates.set(peerHash, state);
		this.enqueueState(state, {
			kind: "snapshot",
			revision: this._revision,
		});
		return true;
	}

	enqueue(mutation: ReplicationInfoMutation): void {
		if (this._revision === MAX_U64) {
			throw new Error("Replication-info confirmation revision exhausted");
		}
		this._revision += 1n;
		for (const state of [...this._sendStates.values()]) {
			this.enqueueState(state, {
				kind: "mutation",
				mutation,
				revision: this._revision,
			});
		}
	}

	private enqueueState(
		state: ReplicationInfoV2SendState,
		request: SendRequest,
	): void {
		if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
			this.parkSnapshotForRetry(state);
			return;
		}
		if (state.suspended) {
			// Delivery is ambiguous while the backoff is armed. Never retain a
			// potentially stale delta: one authoritative Full represents every
			// mutation that arrives before the retry fires.
			this.parkSnapshotForRetry(state);
			return;
		}

		if (!state.worker) {
			state.pending = request;
			let worker: Promise<void>;
			worker = Promise.resolve()
				.then(() => this.runWorker(state))
				.catch((error) => {
					// Sequence cleanup and exhaustion fencing must happen before any
					// readiness/ownership classification in the recovery path.
					this.parkSnapshotForRetry(state);
					if (
						this._sendStates.get(state.peerHash) === state &&
						this.deps.isReplicationOwnershipLifecycleActive(
							state.ownershipLifecycleController,
						) &&
						!state.controller.signal.aborted &&
						!this.deps.isClosed()
					) {
						logger.trace(
							"Replication-info V2 destination stream failed for %s: %s",
							state.peerHash,
							(error as Error)?.message ?? String(error),
						);
					}
				})
				.finally(() => {
					if (state.worker === worker) {
						state.worker = undefined;
						// An enqueue can land after runWorker observes an empty slot but
						// before this promise reaction clears `worker`. Re-arm that item
						// here so the one-slot bound cannot become a stranded queue.
						const pending = state.pending;
						if (pending) {
							state.pending = undefined;
							this.enqueueState(state, pending);
						}
					}
				});
			state.worker = worker;
			return;
		}

		if (!state.pending) {
			state.pending = request;
			return;
		}

		// One pending item is the hard bound. Once another mutation arrives,
		// replace the pending delta with a current authoritative snapshot.
		state.pending = { kind: "snapshot", revision: this._revision };
	}

	private retryDelay(state: ReplicationInfoV2SendState): number {
		const exponent = Math.max(0, state.retryAttempts - 1);
		return Math.min(
			this.maxSendRetryMs,
			this.sendRetryMs * 2 ** Math.min(exponent, MAX_BACKOFF_EXPONENT),
		);
	}

	private scheduleRetry(state: ReplicationInfoV2SendState): void {
		if (state.retryTimer) {
			return;
		}
		if (state.nextSequence > MAX_U64) {
			this.retireSpentState(state);
			return;
		}
		if (!this.isDestinationCurrent(state)) {
			this.clearState(state);
			return;
		}
		if (
			!this.deps.isReplicationOwnershipLifecycleActive(
				state.ownershipLifecycleController,
			)
		) {
			state.pending = undefined;
			return;
		}
		state.retryAttempts = Math.min(
			state.retryAttempts + 1,
			MAX_BACKOFF_EXPONENT + 1,
		);
		state.retryTimer = setTimeout(() => {
			state.retryTimer = undefined;
			if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
				this.parkSnapshotForRetry(state);
				return;
			}
			state.suspended = false;
			state.pending = { kind: "snapshot", revision: this._revision };
			this.enqueueState(state, {
				kind: "snapshot",
				revision: this._revision,
			});
		}, this.retryDelay(state));
		state.retryTimer.unref?.();
	}

	private async createMessage(
		state: ReplicationInfoV2SendState,
		request: SendRequest,
	): Promise<
		| FullReplicationInfoV2Message
		| AddedReplicationInfoV2Message
		| StoppedReplicationInfoV2Message
	> {
		const common = {
			receiverChallenge: state.receiverChallenge.slice(),
			senderEpoch: state.senderEpoch.slice(),
			sequence: state.nextSequence,
		};
		if (request.kind === "snapshot") {
			const segments = (await this.deps.getMyReplicationSegments()).map(
				(range) => range.toReplicationRange(),
			);
			this.deps.validatePersistedReplicationRangeSnapshot(segments);
			return new FullReplicationInfoV2Message({ ...common, segments });
		}
		const { mutation } = request;
		if ("full" in mutation) {
			const segments = mutation.full.segments;
			this.deps.validatePersistedReplicationRangeSnapshot(segments);
			return new FullReplicationInfoV2Message({ ...common, segments });
		}
		if ("added" in mutation) {
			return new AddedReplicationInfoV2Message({
				...common,
				segments: mutation.added.segments,
			});
		}
		return new StoppedReplicationInfoV2Message({
			...common,
			segmentIds: mutation.stopped.segmentIds,
		});
	}

	/**
	 * Detach coordinator progress from a transport that ignores abort. The
	 * transport promise still gets a rejection handler, while this logical send
	 * drops its listener as soon as either side settles.
	 */
	private sendConfirmationRequest(
		request: RequestReplicationInfoV2AppliedMessage,
		state: ReplicationInfoV2SendState,
		signal: AbortSignal,
	): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			let settled = false;
			const cleanup = () => signal.removeEventListener("abort", onAbort);
			const succeed = () => {
				if (settled) return;
				settled = true;
				cleanup();
				resolve();
			};
			const fail = (error: unknown) => {
				if (settled) return;
				settled = true;
				cleanup();
				reject(error);
			};
			const onAbort = () =>
				fail(
					signal.reason instanceof Error
						? signal.reason
						: new AbortError("Replication confirmation send aborted"),
				);
			signal.addEventListener("abort", onAbort, { once: true });
			if (signal.aborted) {
				onAbort();
				return;
			}
			void Promise.resolve()
				.then(() => {
					if (signal.aborted) {
						throw signal.reason instanceof Error
							? signal.reason
							: new AbortError("Replication confirmation send aborted");
					}
					return this.deps.getRpc().send(request, {
						mode: new AcknowledgeDelivery({
							to: [state.target],
							redundancy: 1,
						}),
						priority: CONVERGENCE_MESSAGE_PRIORITY,
						signal,
					});
				})
				.then(succeed, fail);
		});
	}

	/** Accept an exact, signed response to this destination's latest query. */
	acceptApplied(
		message: ReplicationInfoV2AppliedMessage,
		properties: {
			from: PublicSignKey;
			receiverTransportSession: bigint;
		},
	): boolean {
		const peerHash = properties.from.hashcode();
		const state = this._sendStates.get(peerHash);
		const requested = state?.applicationConfirmationRequest;
		if (
			!state ||
			!requested ||
			!state.target.equals(properties.from) ||
			state.receiverTransportSession !== properties.receiverTransportSession ||
			!this.isDestinationReady(state) ||
			!this.supportsApplicationConfirmation(state) ||
			!bytesEqual(message.receiverChallenge, state.receiverChallenge) ||
			!bytesEqual(message.senderEpoch, state.senderEpoch) ||
			message.sequence !== requested.sequence ||
			message.revision !== requested.revision ||
			message.revision > this._revision
		) {
			return false;
		}
		state.applicationConfirmationRequest = undefined;
		requested.controller.abort(
			new AbortError("Replication confirmation response received"),
		);
		if (
			state.appliedRevision === undefined ||
			message.revision > state.appliedRevision
		) {
			state.appliedRevision = message.revision;
		}
		this.settleConfirmations();
		return true;
	}

	private async runWorker(state: ReplicationInfoV2SendState): Promise<void> {
		while (true) {
			if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
				this.parkSnapshotForRetry(state);
				return;
			}
			const request = state.pending;
			if (!request) {
				return;
			}
			state.pending = undefined;
			state.inFlightRevision = request.revision;
			const message = await this.createMessage(state, request);
			if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
				this.parkSnapshotForRetry(state);
				return;
			}
			// Consume the sequence before the transport attempt. From this point on
			// delivery is ambiguous even if ownership aborts before the await
			// continuation runs, so `nextSequence` always remains the next value that
			// has never been attempted with different content.
			state.inFlightSequence = state.nextSequence;
			state.nextSequence += 1n;
			await this.deps.getRpc().send(message, {
				mode: new AcknowledgeDelivery({
					to: [state.target],
					redundancy: 1,
				}),
				priority: CONVERGENCE_MESSAGE_PRIORITY,
				signal: AbortSignal.any([
					state.controller.signal,
					state.ownershipLifecycleController.signal,
				]),
			});
			state.inFlightSequence = undefined;
			if (
				this.hasConfirmationFor(state, request.revision) &&
				this.supportsApplicationConfirmation(state) &&
				this.isCurrent(state)
			) {
				const confirmationRequest: ApplicationConfirmationRequest = {
					sequence: message.sequence,
					revision: request.revision,
					controller: new AbortController(),
				};
				// Install before invoking transport: an in-process or very fast remote
				// response may arrive before this send promise resolves.
				state.applicationConfirmationRequest?.controller.abort(
					new AbortError("Replication confirmation superseded"),
				);
				state.applicationConfirmationRequest = confirmationRequest;
				const confirmationSignal = AbortSignal.any([
					state.controller.signal,
					state.ownershipLifecycleController.signal,
					confirmationRequest.controller.signal,
				]);
				try {
					await this.sendConfirmationRequest(
						new RequestReplicationInfoV2AppliedMessage({
							receiverChallenge: state.receiverChallenge.slice(),
							senderEpoch: state.senderEpoch.slice(),
							sequence: message.sequence,
							revision: request.revision,
						}),
						state,
						confirmationSignal,
					);
				} catch (error) {
					if (state.applicationConfirmationRequest === confirmationRequest) {
						state.applicationConfirmationRequest = undefined;
					}
					if (confirmationRequest.controller.signal.aborted) {
						if (!this.isCurrent(state)) return;
						state.inFlightRevision = undefined;
						continue;
					}
					throw error;
				}
			}
			state.inFlightRevision = undefined;
			if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
				this.parkSnapshotForRetry(state);
				return;
			}
			state.retryAttempts = 0;
			state.established = true;
		}
	}

	/**
	 * Terminal close/drop runs after the normal ownership generation is aborted.
	 * Send an authoritative empty Full directly from the retained destination
	 * bindings. `nextSequence` is consumed before every transport attempt, so it
	 * is always safe for different terminal content even if a just-aborted frame
	 * was delivered. Receivers admit Full gaps as authoritative resynchronization.
	 */
	async sendTerminalReset(signal: AbortSignal): Promise<void> {
		const sends: Promise<unknown>[] = [];
		for (const state of [...this._sendStates.values()]) {
			if (
				signal.aborted ||
				!this.isDestinationCurrent(state) ||
				state.nextSequence > MAX_U64
			) {
				continue;
			}
			state.pending = undefined;
			const sequence = state.nextSequence;
			state.nextSequence += 1n;
			const message = new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverChallenge.slice(),
				senderEpoch: state.senderEpoch.slice(),
				sequence,
				segments: [],
			});
			if (sequence === MAX_U64) {
				// Retire the exhausted normal stream before transport invocation. The
				// terminal attempt remains valid because it carries its caller-owned
				// signal rather than the state controller that clearState aborts.
				this.retireSpentState(state);
			}
			try {
				sends.push(
					Promise.resolve(
						this.deps.getRpc().send(message, {
							mode: new AcknowledgeDelivery({
								to: [state.target],
								redundancy: 1,
							}),
							priority: CONVERGENCE_MESSAGE_PRIORITY,
							signal,
						}),
					),
				);
			} catch (error) {
				// Keep one synchronous transport failure isolated to its destination;
				// the sequence was already consumed and later peers must still reset.
				sends.push(Promise.reject(error));
			}
		}
		if (sends.length === 0 || signal.aborted) {
			return;
		}

		// Terminal reset is best-effort and the caller owns its close/drop bound.
		// A transport is expected to observe the signal, but a disconnected
		// acknowledgement path can leave its promise pending indefinitely. Do not
		// let such a transport retain the whole terminal operation past the bound.
		await new Promise<void>((resolve) => {
			let finished = false;
			const finish = () => {
				if (finished) return;
				finished = true;
				signal.removeEventListener("abort", finish);
				resolve();
			};
			signal.addEventListener("abort", finish, { once: true });
			void Promise.allSettled(sends).then(finish);
			// Close the check/listener race if the caller aborted synchronously.
			if (signal.aborted) finish();
		});
	}

	async drain(signal?: AbortSignal): Promise<void> {
		while (!signal?.aborted) {
			const workers = [
				...[...this._sendStates.values()]
					.map((state) => state.worker)
					.filter((worker): worker is Promise<void> => worker != null),
				...this._retiringWorkersByPeer.values(),
			];
			const uniqueWorkers = [...new Set(workers)];
			if (uniqueWorkers.length === 0) {
				return;
			}
			const settled = Promise.allSettled(uniqueWorkers).then(() => undefined);
			if (!signal) {
				await settled;
			} else {
				await new Promise<void>((resolve) => {
					const onAbort = () => resolve();
					signal.addEventListener("abort", onAbort, { once: true });
					void settled.then(() => {
						signal.removeEventListener("abort", onAbort);
						resolve();
					});
				});
			}
		}
	}
}
