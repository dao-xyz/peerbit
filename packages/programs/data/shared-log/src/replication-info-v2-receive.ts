import { serialize } from "@dao-xyz/borsh";
import { type PublicSignKey, randomBytes, sha256Sync } from "@peerbit/crypto";
import {
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
} from "./exchange-heads.js";
import { deriveReplicationInfoV2ReceiverBinding } from "./replication-info-v2-binding.js";
import {
	AddedReplicationInfoV2Message,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	FullReplicationInfoV2Message,
	ReplicationInfoV2AppliedMessage,
	type ReplicationInfoV2Message,
	RequestReplicationInfoV2AppliedMessage,
	RequestReplicationInfoV2Message,
	StoppedReplicating,
	StoppedReplicationInfoV2Message,
} from "./replication.js";

const REQUIRED_SENDER_CAPABILITIES =
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
	SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND;

const DEFAULT_REQUEST_RETRY_MS = 1_000;
const DEFAULT_MAX_REQUEST_RETRY_MS = 30_000;
const DEFAULT_REQUEST_MAX_ATTEMPTS = 7;
const DEFAULT_REMOTE_FULL_REARM_ATTEMPT_TIMEOUT_MS = 2_000;
const DEFAULT_REMOTE_FULL_REARM_COOLDOWN_MS = 5_000;
const MAX_REMOTE_FULL_REARM_COOLDOWN_MS = 30_000;
const MAX_REMOTE_FULL_REARM_OUTSTANDING_PER_SESSION = 2;
const MAX_REMOTE_FULL_REARM_OUTSTANDING_GLOBAL = 64;
const MAX_TIMER_MS = 2_147_483_647;
const MAX_U64 = (1n << 64n) - 1n;
const MAX_BACKOFF_EXPONENT = 20;

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

// Q4 (B12): fingerprints keep constructing the legacy-class canonical forms.
// They are byte-stable local-only hash inputs, never sent; these are the only
// sanctioned legacy-frame construction sites in src (see the no-legacy-
// machinery source ratchet's narrowed whitelist).
const replicationInfoPayloadFingerprint = (
	message: ReplicationInfoV2Message,
): Uint8Array => {
	const canonical =
		message instanceof FullReplicationInfoV2Message
			? new AllReplicatingSegmentsMessage({ segments: message.segments })
			: message instanceof AddedReplicationInfoV2Message
				? new AddedReplicationSegmentMessage({ segments: message.segments })
				: message instanceof StoppedReplicationInfoV2Message
					? new StoppedReplicating({ segmentIds: message.segmentIds })
					: message;
	return sha256Sync(serialize(canonical));
};

export type ReplicationInfoV2ReceivePhase =
	| "awaiting-full"
	| "active"
	| "resync";

type LocalCapabilityReady = {
	peerHash: string;
	receiveEpoch: object | null;
	receiverTransportSession: bigint;
	/**
	 * The local capability envelope and the later RequestV2 envelope use the
	 * same millisecond clock. The sender deliberately requires the request to
	 * be strictly newer, so never construct it in this captured millisecond.
	 */
	requestNotBeforeMs: number;
	advertisement?: ReplicationInfoV2LocalCapabilityAdvertisement;
};

type LocalCapabilityReadyProperties = {
	peerHash: string;
	peerSession: object;
	receiveEpoch: object | null;
	receiverTransportSession: bigint;
	requestNotBeforeMs: number;
};

export type ReplicationInfoV2LocalCapabilityAdvertisementHandle = {
	firstAttempt: Promise<void>;
};

export type ReplicationInfoV2LocalCapabilityContext = {
	peerHash: string;
	target: PublicSignKey;
	lifecycleSignal: AbortSignal;
};

export type ReplicationInfoV2LocalCapabilityAdvertisement = {
	peerHash: string;
	target: PublicSignKey;
	peerSession: object;
	receiveEpoch: object | null;
	lifecycleSignal: AbortSignal;
	onLifecycleAbort: () => void;
	controller: AbortController;
	context: ReplicationInfoV2LocalCapabilityContext;
	attempts: number;
	ready: boolean;
	acknowledgedReady?: LocalCapabilityReady;
	receiverTransportSession?: bigint;
	timer?: ReturnType<typeof setTimeout>;
	inFlight?: Promise<void>;
	firstAttempt?: Promise<void>;
};

type ReplicationInfoV2RemoteFullRearm = {
	peerHash: string;
	receiveEpoch: object | null;
	receiverTransportSession: bigint;
	nextAttemptAt: number;
	attemptsStarted: number;
	outstandingAttempts: number;
	inFlight?: Promise<void>;
	controller?: AbortController;
};

export type ReplicationInfoV2ReceiveDiagnostic = Readonly<{
	state: "absent" | "current" | "stale";
	/** Current-state classification; no historical packet/drop log is retained. */
	reason:
		| "state-absent"
		| "peer-session-mismatch"
		| "receive-epoch-mismatch"
		| "transport-session-mismatch"
		| "state-not-current"
		| "admission-pending"
		| "active"
		| "request-parked"
		| "request-in-flight"
		| "request-scheduled"
		| "awaiting-full"
		| "resync";
	phase?: ReplicationInfoV2ReceivePhase;
	requestState?:
		| "idle"
		| "scheduled"
		| "in-flight"
		| "parked"
		| "admission-pending";
	lastAppliedSequence?: string;
	requestAttempts?: number;
	capabilityAdvertisementAttempts: number;
	capabilityAdvertisement: "absent" | "advertising" | "ready" | "stale";
	remoteFullRearm: "idle" | "in-flight" | "cooldown" | "limited";
	remoteFullRearmAttempts: number;
	remoteFullRearmOutstanding: number;
	remoteFullRearmGlobalOutstanding: number;
	remoteFullRearmCooldownRemainingMs?: number;
}>;

export type ReplicationInfoV2LocalCapabilityRefresh = {
	receiverTransportSession: bigint;
	requestNotBeforeMs: number;
};

export type ReplicationInfoV2ReceiveState = {
	peerHash: string;
	target: PublicSignKey;
	peerSession: object;
	receiveEpoch: object | null;
	capabilities: number;
	capabilityTimestamp: bigint;
	senderTransportSession: bigint;
	receiverTransportSession?: bigint;
	receiverRequestChallenge: Uint8Array;
	receiverBinding?: Uint8Array;
	senderEpoch?: Uint8Array;
	lastSequence?: bigint;
	phase: ReplicationInfoV2ReceivePhase;
	version: number;
	controller: AbortController;
	requestTimer?: ReturnType<typeof setTimeout>;
	requestInFlight?: Promise<void>;
	requestAttempts: number;
	requestsSinceCapabilityRefresh: number;
	requestParked: boolean;
	capabilityRefreshRequired: boolean;
	reservedAdmission?: ReplicationInfoV2ReceiveAdmission;
};

export type ReplicationInfoV2ReceiveAdmission = {
	state: ReplicationInfoV2ReceiveState;
	version: number;
	receiveEpoch: object | null;
	message: ReplicationInfoV2Message;
	kind: "full" | "added" | "stopped";
	payloadFingerprint: Uint8Array;
	transportTimestamp: bigint;
	committed: boolean;
	resyncAfterRelease?: boolean;
};

export type ReplicationInfoV2ReceiveDeps = {
	getSelfKey: () => PublicSignKey;
	getReceiverTransportSession: () => bigint;
	isClosed: () => boolean;
	isPeerSessionCurrent: (peerHash: string, peerSession: object) => boolean;
	isReceiveEpochCurrent: (
		peerHash: string,
		receiveEpoch: object | null,
	) => boolean;
	isPeerStateCurrent: (
		peerHash: string,
		peerSession: object,
		receiveEpoch: object | null,
	) => boolean;
	isSenderTransportSessionCurrent: (
		peerHash: string,
		senderTransportSession: bigint,
	) => boolean;
	sendRequest: (
		request: RequestReplicationInfoV2Message,
		target: PublicSignKey,
		signal: AbortSignal,
	) => Promise<void>;
	refreshLocalCapability: (properties: {
		peerHash: string;
		target: PublicSignKey;
		peerSession: object;
		receiveEpoch: object | null;
		signal: AbortSignal;
		requestRemoteFullRearm?: boolean;
	}) => Promise<ReplicationInfoV2LocalCapabilityRefresh | undefined>;
	onRequestError?: (error: unknown) => void;
	onLocalCapabilityError?: (error: unknown) => void;
	now?: () => number;
	requestRetryMs?: number;
	maxRequestRetryMs?: number;
	requestMaxAttempts?: number;
	remoteFullRearmAttemptTimeoutMs?: number;
	remoteFullRearmCooldownMs?: number;
	maxRemoteFullRearmOutstandingGlobal?: number;
};

/**
 * Authenticated receive grants and sender-authoritative ordering for
 * replication-info V2. State is bounded to one entry per subscribed peer and
 * is always scoped to the exact PeerSession object.
 */
export class ReplicationInfoV2ReceiveCoordinator {
	_receiveStates!: Map<string, ReplicationInfoV2ReceiveState>;
	/**
	 * Post-B12 V2 resync memory: sessions that have already taken one full
	 * replication-info snapshot. Two load-bearing reads, both in
	 * observeCapability: phase selection (a remembered session opens in
	 * "resync" instead of "awaiting-full"), and capability refresh (the
	 * preserveCutover decision, which keeps the memory only when a re-advert
	 * arrives under the same session with a ready sender).
	 */
	_cutoverPeerSessions!: WeakSet<object>;
	_localCapabilityReadyBySession!: WeakMap<object, LocalCapabilityReady>;
	_localCapabilityContextBySession!: WeakMap<
		object,
		ReplicationInfoV2LocalCapabilityContext
	>;
	_localCapabilityAdvertisementsByPeer!: Map<
		string,
		ReplicationInfoV2LocalCapabilityAdvertisement
	>;
	_remoteFullRearmBySession!: WeakMap<object, ReplicationInfoV2RemoteFullRearm>;
	_remoteFullRearmsInFlight!: Set<ReplicationInfoV2RemoteFullRearm>;
	_remoteFullRearmOutstanding!: Set<object>;
	_reservedAdmissionsByPeer!: Map<string, ReplicationInfoV2ReceiveAdmission>;

	private readonly now: () => number;
	private readonly requestRetryMs: number;
	private readonly maxRequestRetryMs: number;
	private readonly requestMaxAttempts: number;
	private readonly remoteFullRearmAttemptTimeoutMs: number;
	private readonly remoteFullRearmCooldownMs: number;
	private readonly maxRemoteFullRearmOutstandingGlobal: number;

	constructor(private readonly deps: ReplicationInfoV2ReceiveDeps) {
		this.now = deps.now ?? Date.now;
		this.requestRetryMs = Math.max(
			1,
			deps.requestRetryMs ?? DEFAULT_REQUEST_RETRY_MS,
		);
		this.maxRequestRetryMs = Math.max(
			this.requestRetryMs,
			deps.maxRequestRetryMs ?? DEFAULT_MAX_REQUEST_RETRY_MS,
		);
		this.requestMaxAttempts = Math.max(
			1,
			Math.floor(deps.requestMaxAttempts ?? DEFAULT_REQUEST_MAX_ATTEMPTS),
		);
		this.remoteFullRearmAttemptTimeoutMs = Math.max(
			1,
			Math.min(
				MAX_TIMER_MS,
				Math.floor(
					deps.remoteFullRearmAttemptTimeoutMs ??
						DEFAULT_REMOTE_FULL_REARM_ATTEMPT_TIMEOUT_MS,
				),
			),
		);
		this.remoteFullRearmCooldownMs = Math.max(
			this.requestRetryMs,
			Math.min(
				MAX_TIMER_MS,
				Math.floor(
					deps.remoteFullRearmCooldownMs ??
						DEFAULT_REMOTE_FULL_REARM_COOLDOWN_MS,
				),
			),
		);
		// The injected value is a test seam that may only tighten this lifetime
		// resource bound, never widen it.
		this.maxRemoteFullRearmOutstandingGlobal = Math.min(
			MAX_REMOTE_FULL_REARM_OUTSTANDING_GLOBAL,
			Math.max(
				1,
				Math.floor(
					deps.maxRemoteFullRearmOutstandingGlobal ??
						MAX_REMOTE_FULL_REARM_OUTSTANDING_GLOBAL,
				),
			),
		);
		this._receiveStates = new Map();
		this._cutoverPeerSessions = new WeakSet();
		this._localCapabilityReadyBySession = new WeakMap();
		this._localCapabilityContextBySession = new WeakMap();
		this._localCapabilityAdvertisementsByPeer = new Map();
		this._remoteFullRearmBySession = new WeakMap();
		this._remoteFullRearmsInFlight = new Set();
		this._remoteFullRearmOutstanding = new Set();
		this._reservedAdmissionsByPeer = new Map();
	}

	resetForOpen(): void {
		this.clearForClose();
		this._receiveStates = new Map();
		this._cutoverPeerSessions = new WeakSet();
		this._localCapabilityReadyBySession = new WeakMap();
		this._localCapabilityContextBySession = new WeakMap();
		this._localCapabilityAdvertisementsByPeer = new Map();
		this._remoteFullRearmBySession = new WeakMap();
		this._remoteFullRearmsInFlight = new Set();
		this._reservedAdmissionsByPeer = new Map();
	}

	clearForClose(): void {
		for (const rearm of this._remoteFullRearmsInFlight ?? []) {
			rearm.controller?.abort(
				new Error(
					"Replication-info V2 receiver closed during remote Full rearm",
				),
			);
		}
		this._remoteFullRearmsInFlight?.clear();
		for (const advertisement of [
			...(this._localCapabilityAdvertisementsByPeer?.values() ?? []),
		]) {
			this.clearLocalCapabilityAdvertisement(advertisement);
		}
		this._localCapabilityAdvertisementsByPeer?.clear();
		for (const state of this._receiveStates?.values() ?? []) {
			this.clearState(state);
		}
		this._receiveStates?.clear();
		this._cutoverPeerSessions = new WeakSet();
		this._localCapabilityReadyBySession = new WeakMap();
		this._localCapabilityContextBySession = new WeakMap();
		this._remoteFullRearmBySession = new WeakMap();
	}

	clearPeer(peerHash: string, expectedSession?: object): void {
		const advertisement =
			this._localCapabilityAdvertisementsByPeer.get(peerHash);
		if (
			advertisement &&
			(!expectedSession || advertisement.peerSession === expectedSession)
		) {
			this.clearLocalCapabilityAdvertisement(advertisement);
		}
		const state = this._receiveStates.get(peerHash);
		if (state && (!expectedSession || state.peerSession === expectedSession)) {
			this.clearState(state);
			this._localCapabilityReadyBySession.delete(state.peerSession);
			this._localCapabilityContextBySession.delete(state.peerSession);
			this._cutoverPeerSessions.delete(state.peerSession);
		}
		if (!expectedSession) {
			for (const rearm of this._remoteFullRearmsInFlight) {
				if (rearm.peerHash === peerHash) {
					rearm.controller?.abort(
						new Error("Replication-info V2 peer cleared during rearm"),
					);
				}
			}
		}
		const rearmSession =
			expectedSession ?? state?.peerSession ?? advertisement?.peerSession;
		if (rearmSession) {
			this._remoteFullRearmBySession
				.get(rearmSession)
				?.controller?.abort(
					new Error("Replication-info V2 peer session cleared during rearm"),
				);
			this._remoteFullRearmBySession.delete(rearmSession);
		}
		if (expectedSession) {
			this._localCapabilityReadyBySession.delete(expectedSession);
			this._localCapabilityContextBySession.delete(expectedSession);
			this._cutoverPeerSessions.delete(expectedSession);
		}
	}

	/** Revoke an unauthenticated or downgraded capability generation. */
	revokePeerCapability(peerHash: string): void {
		const state = this._receiveStates.get(peerHash);
		if (!state) {
			return;
		}
		this.clearState(state);
		// Unconditional: the revoked session must lose its resync memory so a
		// re-advert starts from the first phase again. (This used to sit behind
		// an opt-out parameter that defaulted to true, had no else-arm, and that
		// no caller ever overrode.)
		this._cutoverPeerSessions.delete(state.peerSession);
	}

	private clearState(state: ReplicationInfoV2ReceiveState): void {
		if (state.requestTimer) {
			clearTimeout(state.requestTimer);
			state.requestTimer = undefined;
		}
		state.controller.abort();
		state.version++;
		if (this._receiveStates.get(state.peerHash) === state) {
			this._receiveStates.delete(state.peerHash);
		}
	}

	private clearLocalCapabilityAdvertisement(
		state: ReplicationInfoV2LocalCapabilityAdvertisement,
		options?: { preserveContext?: boolean },
	): void {
		if (state.timer) {
			clearTimeout(state.timer);
			state.timer = undefined;
		}
		state.lifecycleSignal.removeEventListener("abort", state.onLifecycleAbort);
		state.controller.abort();
		const wasMapped =
			this._localCapabilityAdvertisementsByPeer.get(state.peerHash) === state;
		if (wasMapped) {
			this._localCapabilityAdvertisementsByPeer.delete(state.peerHash);
		}
		const ready = this._localCapabilityReadyBySession.get(state.peerSession);
		if (ready?.advertisement === state) {
			this._localCapabilityReadyBySession.delete(state.peerSession);
		}
		if (
			wasMapped &&
			!options?.preserveContext &&
			this._localCapabilityContextBySession.get(state.peerSession) ===
				state.context
		) {
			this._localCapabilityContextBySession.delete(state.peerSession);
		}
	}

	private isLocalCapabilityAdvertisementOwnerCurrent(
		state: ReplicationInfoV2LocalCapabilityAdvertisement,
	): boolean {
		return (
			this._localCapabilityAdvertisementsByPeer.get(state.peerHash) === state &&
			this._localCapabilityContextBySession.get(state.peerSession) ===
				state.context &&
			state.context.peerHash === state.peerHash &&
			state.context.lifecycleSignal === state.lifecycleSignal &&
			state.context.target.equals(state.target) &&
			!state.controller.signal.aborted &&
			!state.lifecycleSignal.aborted &&
			!this.deps.isClosed() &&
			this.deps.isPeerSessionCurrent(state.peerHash, state.peerSession) &&
			(state.receiverTransportSession === undefined ||
				this.deps.getReceiverTransportSession() ===
					state.receiverTransportSession)
		);
	}

	private isLocalCapabilityAdvertisementGenerationCurrent(
		state: ReplicationInfoV2LocalCapabilityAdvertisement,
	): boolean {
		return (
			this.isLocalCapabilityAdvertisementOwnerCurrent(state) &&
			this.deps.isReceiveEpochCurrent(state.peerHash, state.receiveEpoch)
		);
	}

	private isLocalCapabilityAdvertisementReadyOpen(
		state: ReplicationInfoV2LocalCapabilityAdvertisement,
	): boolean {
		return (
			this.isLocalCapabilityAdvertisementGenerationCurrent(state) &&
			this.deps.isPeerStateCurrent(
				state.peerHash,
				state.peerSession,
				state.receiveEpoch,
			)
		);
	}

	private localCapabilityRetryDelay(
		state: ReplicationInfoV2LocalCapabilityAdvertisement,
	): number {
		const exponent = Math.max(0, state.attempts - 1);
		return Math.min(
			this.maxRequestRetryMs,
			this.requestRetryMs * 2 ** Math.min(exponent, MAX_BACKOFF_EXPONENT),
		);
	}

	private armLocalCapabilityAdvertisement(
		state: ReplicationInfoV2LocalCapabilityAdvertisement,
	): void {
		if (state.timer || state.inFlight || state.ready) {
			return;
		}
		if (!this.isLocalCapabilityAdvertisementOwnerCurrent(state)) {
			this.clearLocalCapabilityAdvertisement(state);
			return;
		}
		if (!this.isLocalCapabilityAdvertisementGenerationCurrent(state)) {
			return;
		}
		state.timer = setTimeout(() => {
			state.timer = undefined;
			if (!this.isLocalCapabilityAdvertisementOwnerCurrent(state)) {
				this.clearLocalCapabilityAdvertisement(state);
				return;
			}
			if (!this.isLocalCapabilityAdvertisementGenerationCurrent(state)) {
				return;
			}
			if (!this.isLocalCapabilityAdvertisementReadyOpen(state)) {
				this.armLocalCapabilityAdvertisement(state);
				return;
			}
			void this.runLocalCapabilityAdvertisement(state);
		}, this.localCapabilityRetryDelay(state));
		state.timer.unref?.();
	}

	private async runLocalCapabilityAdvertisement(
		state: ReplicationInfoV2LocalCapabilityAdvertisement,
	): Promise<void> {
		if (state.inFlight) {
			await state.inFlight;
			return;
		}
		if (state.ready) {
			return;
		}
		if (!this.isLocalCapabilityAdvertisementOwnerCurrent(state)) {
			this.clearLocalCapabilityAdvertisement(state);
			return;
		}
		if (!this.isLocalCapabilityAdvertisementGenerationCurrent(state)) {
			return;
		}
		if (state.acknowledgedReady) {
			this.promoteLocalCapabilityAdvertisement(state);
			return;
		}
		if (!this.isLocalCapabilityAdvertisementReadyOpen(state)) {
			this.armLocalCapabilityAdvertisement(state);
			return;
		}
		state.attempts = Math.min(state.attempts + 1, MAX_BACKOFF_EXPONENT + 1);
		let operation: Promise<void>;
		operation = (async () => {
			const refreshed = await this.deps.refreshLocalCapability({
				peerHash: state.peerHash,
				target: state.target,
				peerSession: state.peerSession,
				receiveEpoch: state.receiveEpoch,
				signal: AbortSignal.any([
					state.controller.signal,
					state.lifecycleSignal,
				]),
			});
			if (
				!refreshed ||
				!this.isLocalCapabilityAdvertisementGenerationCurrent(state) ||
				this.deps.getReceiverTransportSession() !==
					refreshed.receiverTransportSession
			) {
				return;
			}
			state.receiverTransportSession = refreshed.receiverTransportSession;
			state.acknowledgedReady = {
				peerHash: state.peerHash,
				receiveEpoch: state.receiveEpoch,
				receiverTransportSession: refreshed.receiverTransportSession,
				requestNotBeforeMs: refreshed.requestNotBeforeMs,
				advertisement: state,
			};
			state.attempts = 0;
			this.promoteLocalCapabilityAdvertisement(state);
		})()
			.catch((error) => {
				if (
					!state.controller.signal.aborted &&
					!state.lifecycleSignal.aborted &&
					!this.deps.isClosed()
				) {
					this.deps.onLocalCapabilityError?.(error);
				}
			})
			.finally(() => {
				if (state.inFlight === operation) {
					state.inFlight = undefined;
				}
				if (!this.isLocalCapabilityAdvertisementOwnerCurrent(state)) {
					this.clearLocalCapabilityAdvertisement(state);
				} else if (
					this.isLocalCapabilityAdvertisementGenerationCurrent(state) &&
					!state.ready
				) {
					this.armLocalCapabilityAdvertisement(state);
				}
			});
		state.inFlight = operation;
		await operation;
	}

	/**
	 * Start local authenticated-apply advertisement. Readiness is promoted as
	 * soon as the ACK lands (B12: the legacy publication this once ordered
	 * behind a two-phase barrier is retired). Failed attempts leave one
	 * exact-session worker retrying with capped exponential backoff.
	 */
	advertiseLocalCapability(properties: {
		target: PublicSignKey;
		peerSession: object;
		receiveEpoch: object | null;
		signal: AbortSignal;
	}): ReplicationInfoV2LocalCapabilityAdvertisementHandle {
		const peerHash = properties.target.hashcode();
		if (
			properties.signal.aborted ||
			this.deps.isClosed() ||
			!this.deps.isPeerSessionCurrent(peerHash, properties.peerSession) ||
			!this.deps.isReceiveEpochCurrent(peerHash, properties.receiveEpoch)
		) {
			return {
				firstAttempt: Promise.resolve(),
			};
		}
		let context = this._localCapabilityContextBySession.get(
			properties.peerSession,
		);
		if (
			context &&
			(context.peerHash !== peerHash ||
				!context.target.equals(properties.target) ||
				context.lifecycleSignal !== properties.signal)
		) {
			return {
				firstAttempt: Promise.resolve(),
			};
		}
		if (!context) {
			context = {
				peerHash,
				target: properties.target,
				lifecycleSignal: properties.signal,
			};
			this._localCapabilityContextBySession.set(
				properties.peerSession,
				context,
			);
		}
		let state = this._localCapabilityAdvertisementsByPeer.get(peerHash);
		if (
			state &&
			(state.peerSession !== properties.peerSession ||
				state.context !== context ||
				state.lifecycleSignal !== properties.signal ||
				!state.target.equals(properties.target))
		) {
			this.clearLocalCapabilityAdvertisement(state);
			state = undefined;
		}
		if (state && state.receiveEpoch !== properties.receiveEpoch) {
			this.clearLocalCapabilityAdvertisement(state, { preserveContext: true });
			state = undefined;
		}
		if (state && !this.isLocalCapabilityAdvertisementOwnerCurrent(state)) {
			this.clearLocalCapabilityAdvertisement(state);
			state = undefined;
		}
		if (
			this._localCapabilityContextBySession.get(properties.peerSession) !==
			context
		) {
			return {
				firstAttempt: Promise.resolve(),
			};
		}
		if (!state) {
			const controller = new AbortController();
			const advertisement: ReplicationInfoV2LocalCapabilityAdvertisement = {
				peerHash,
				target: properties.target,
				peerSession: properties.peerSession,
				receiveEpoch: properties.receiveEpoch,
				lifecycleSignal: properties.signal,
				onLifecycleAbort: () => {},
				controller,
				context,
				attempts: 0,
				ready: false,
				receiverTransportSession: this.deps.getReceiverTransportSession(),
			};
			advertisement.onLifecycleAbort = () =>
				this.clearLocalCapabilityAdvertisement(advertisement);
			properties.signal.addEventListener(
				"abort",
				advertisement.onLifecycleAbort,
				{
					once: true,
				},
			);
			this._localCapabilityAdvertisementsByPeer.set(peerHash, advertisement);
			state = advertisement;
		}
		const firstAttempt =
			state.firstAttempt ??
			(state.firstAttempt = this.runLocalCapabilityAdvertisement(state));
		return {
			firstAttempt,
		};
	}

	/**
	 * Send one transient, authenticated rearm hint for the exact current local
	 * receive grant. The hint asks a patched remote receiver to rotate its
	 * challenge and issue another Full request, rebuilding an outbound sender
	 * stream that was lost while both directions otherwise remained current.
	 *
	 * This deliberately does not mutate the steady advertisement worker. One
	 * attempt is coalesced per exact PeerSession/receive generation and bound to
	 * its own deadline as well as the caller's signal. A cooldown avoids rotating
	 * a healthy in-flight challenge on every recovery tick. The remote rotates
	 * only when its receive state is active; later hints during resync only nudge
	 * that same bounded request generation. At most two transports that disregard
	 * abort remain outstanding for one exact session (64 across this coordinator's
	 * lifetime); reaching either cap stops new hints while the persisted-readiness
	 * gate remains fail closed.
	 */
	reAdvertiseLocalCapabilityForRemoteFull(properties: {
		peerHash: string;
		peerSession: object;
		receiveEpoch: object | null;
		signal: AbortSignal;
	}): boolean {
		const context = this._localCapabilityContextBySession.get(
			properties.peerSession,
		);
		const ready = this._localCapabilityReadyBySession.get(
			properties.peerSession,
		);
		if (
			properties.signal.aborted ||
			!context ||
			context.peerHash !== properties.peerHash ||
			context.lifecycleSignal.aborted ||
			this.deps.isClosed() ||
			!this.deps.isPeerStateCurrent(
				properties.peerHash,
				properties.peerSession,
				properties.receiveEpoch,
			) ||
			!ready ||
			ready.peerHash !== properties.peerHash ||
			ready.receiveEpoch !== properties.receiveEpoch ||
			ready.receiverTransportSession !== this.deps.getReceiverTransportSession()
		) {
			return false;
		}

		const now = this.now();
		const receiverTransportSession = ready.receiverTransportSession;
		let rearm = this._remoteFullRearmBySession.get(properties.peerSession);
		if (
			rearm &&
			(rearm.peerHash !== properties.peerHash ||
				rearm.receiveEpoch !== properties.receiveEpoch ||
				rearm.receiverTransportSession !== receiverTransportSession)
		) {
			rearm.controller?.abort(
				new Error("Replication-info V2 remote Full rearm generation changed"),
			);
			rearm = undefined;
		}
		if (rearm?.inFlight) {
			return true;
		}
		if (rearm !== undefined && now < rearm.nextAttemptAt) {
			return true;
		}
		if (
			(rearm?.outstandingAttempts ?? 0) >=
				MAX_REMOTE_FULL_REARM_OUTSTANDING_PER_SESSION ||
			this._remoteFullRearmOutstanding.size >=
				this.maxRemoteFullRearmOutstandingGlobal
		) {
			return true;
		}
		// Repeated hints must leave time for a slow Full and its Applied response
		// to settle. Reuse this exact generation's bounded attempt counter so a
		// watchdog cannot continually replace the binding it is trying to confirm.
		const nextAttemptAt =
			now +
			Math.min(
				MAX_REMOTE_FULL_REARM_COOLDOWN_MS,
				this.remoteFullRearmCooldownMs *
					2 ** Math.min(rearm?.attemptsStarted ?? 0, MAX_BACKOFF_EXPONENT),
			);
		if (!rearm) {
			rearm = {
				peerHash: properties.peerHash,
				receiveEpoch: properties.receiveEpoch,
				receiverTransportSession,
				nextAttemptAt,
				attemptsStarted: 0,
				outstandingAttempts: 0,
			};
			this._remoteFullRearmBySession.set(properties.peerSession, rearm);
		} else {
			rearm.nextAttemptAt = nextAttemptAt;
		}
		const attemptState = rearm;
		const outstandingReservation = {};
		attemptState.attemptsStarted = Math.min(
			MAX_REMOTE_FULL_REARM_OUTSTANDING_GLOBAL,
			attemptState.attemptsStarted + 1,
		);
		attemptState.outstandingAttempts++;
		this._remoteFullRearmOutstanding.add(outstandingReservation);

		const attemptController = new AbortController();
		const operationSignal = attemptController.signal;
		attemptState.controller = attemptController;
		this._remoteFullRearmsInFlight.add(attemptState);
		const sourceSignals = Array.from(
			new Set([context.lifecycleSignal, properties.signal]),
		);
		const onSourceAbort = (event: Event) => {
			const source = event.currentTarget as AbortSignal;
			attemptController.abort(source.reason);
		};
		for (const source of sourceSignals) {
			source.addEventListener("abort", onSourceAbort, { once: true });
		}
		const attemptTimer = setTimeout(
			() =>
				attemptController.abort(
					new Error("Replication-info V2 remote Full rearm attempt timed out"),
				),
			this.remoteFullRearmAttemptTimeoutMs,
		);
		attemptTimer.unref?.();
		let releasedOutstanding = false;
		const releaseOutstanding = () => {
			if (releasedOutstanding) return;
			releasedOutstanding = true;
			this._remoteFullRearmOutstanding.delete(outstandingReservation);
			attemptState.outstandingAttempts--;
		};
		const refresh = Promise.resolve().then(() => {
			if (operationSignal.aborted) {
				throw operationSignal.reason;
			}
			return this.deps.refreshLocalCapability({
				peerHash: properties.peerHash,
				target: context.target,
				peerSession: properties.peerSession,
				receiveEpoch: properties.receiveEpoch,
				signal: operationSignal,
				requestRemoteFullRearm: true,
			});
		});
		void refresh.then(releaseOutstanding, releaseOutstanding);

		let operation: Promise<void>;
		const detach = () => {
			clearTimeout(attemptTimer);
			operationSignal.removeEventListener("abort", detach);
			for (const source of sourceSignals) {
				source.removeEventListener("abort", onSourceAbort);
			}
			if (attemptState.inFlight === operation) {
				this._remoteFullRearmsInFlight.delete(attemptState);
				attemptState.inFlight = undefined;
				attemptState.controller = undefined;
			}
		};
		const raceWithAttemptSignal = <T>(promise: Promise<T>): Promise<T> =>
			new Promise<T>((resolve, reject) => {
				const onAbort = () => {
					operationSignal.removeEventListener("abort", onAbort);
					reject(operationSignal.reason);
				};
				operationSignal.addEventListener("abort", onAbort, { once: true });
				if (operationSignal.aborted) {
					onAbort();
					return;
				}
				promise.then(
					(value) => {
						operationSignal.removeEventListener("abort", onAbort);
						resolve(value);
					},
					(error) => {
						operationSignal.removeEventListener("abort", onAbort);
						reject(error);
					},
				);
			});
		operation = raceWithAttemptSignal(refresh)
			.then(() => undefined)
			.catch((error) => {
				if (
					!operationSignal.aborted &&
					!this.deps.isClosed() &&
					this.deps.isPeerStateCurrent(
						properties.peerHash,
						properties.peerSession,
						properties.receiveEpoch,
					)
				) {
					this.deps.onLocalCapabilityError?.(error);
				}
			})
			.finally(() => {
				detach();
			});
		attemptState.inFlight = operation;
		operationSignal.addEventListener("abort", detach, { once: true });
		for (const source of sourceSignals) {
			if (source.aborted) {
				attemptController.abort(source.reason);
				break;
			}
		}
		void operation;
		return true;
	}

	/**
	 * Bounded, read-only state for persisted-readiness troubleshooting. This
	 * intentionally omits transport sessions, challenges, payloads, and maps.
	 */
	diagnosePeer(properties: {
		peerHash: string;
		peerSession?: object;
		receiveEpoch?: object | null;
		senderTransportSession?: bigint;
	}): ReplicationInfoV2ReceiveDiagnostic {
		const state = this._receiveStates.get(properties.peerHash);
		const advertisement = this._localCapabilityAdvertisementsByPeer.get(
			properties.peerHash,
		);
		const session =
			properties.peerSession ??
			state?.peerSession ??
			advertisement?.peerSession;
		const ready = session
			? this._localCapabilityReadyBySession.get(session)
			: undefined;
		let capabilityAdvertisement: ReplicationInfoV2ReceiveDiagnostic["capabilityAdvertisement"] =
			"absent";
		if (ready && ready.peerHash === properties.peerHash) {
			capabilityAdvertisement =
				session !== undefined &&
				this.deps.isPeerSessionCurrent(properties.peerHash, session) &&
				this.deps.isReceiveEpochCurrent(
					properties.peerHash,
					ready.receiveEpoch,
				) &&
				ready.receiverTransportSession ===
					this.deps.getReceiverTransportSession() &&
				(properties.receiveEpoch === undefined ||
					ready.receiveEpoch === properties.receiveEpoch)
					? "ready"
					: "stale";
		} else if (advertisement) {
			capabilityAdvertisement =
				advertisement.peerHash === properties.peerHash &&
				advertisement.peerSession === session &&
				(properties.receiveEpoch === undefined ||
					advertisement.receiveEpoch === properties.receiveEpoch) &&
				this.isLocalCapabilityAdvertisementGenerationCurrent(advertisement)
					? "advertising"
					: "stale";
		}

		const rearm = session
			? this._remoteFullRearmBySession.get(session)
			: undefined;
		const now = this.now();
		const rearmLimited =
			(rearm?.outstandingAttempts ?? 0) >=
				MAX_REMOTE_FULL_REARM_OUTSTANDING_PER_SESSION ||
			this._remoteFullRearmOutstanding.size >=
				this.maxRemoteFullRearmOutstandingGlobal;
		const rearmStatus: ReplicationInfoV2ReceiveDiagnostic["remoteFullRearm"] =
			rearm?.inFlight
				? "in-flight"
				: rearmLimited
					? "limited"
					: rearm && now < rearm.nextAttemptAt
						? "cooldown"
						: "idle";
		const common = {
			capabilityAdvertisementAttempts: Math.min(
				MAX_BACKOFF_EXPONENT + 1,
				Math.max(0, advertisement?.attempts ?? 0),
			),
			capabilityAdvertisement,
			remoteFullRearm: rearmStatus,
			remoteFullRearmAttempts: Math.min(
				MAX_REMOTE_FULL_REARM_OUTSTANDING_GLOBAL,
				Math.max(0, rearm?.attemptsStarted ?? 0),
			),
			remoteFullRearmOutstanding: Math.min(
				MAX_REMOTE_FULL_REARM_OUTSTANDING_PER_SESSION,
				Math.max(0, rearm?.outstandingAttempts ?? 0),
			),
			remoteFullRearmGlobalOutstanding: Math.min(
				this.maxRemoteFullRearmOutstandingGlobal,
				this._remoteFullRearmOutstanding.size,
			),
			...(rearm && now < rearm.nextAttemptAt
				? {
						remoteFullRearmCooldownRemainingMs: Math.min(
							MAX_TIMER_MS,
							Math.max(0, rearm.nextAttemptAt - now),
						),
					}
				: {}),
		};
		if (!state) {
			return Object.freeze({
				state: "absent" as const,
				reason: "state-absent" as const,
				...common,
			});
		}

		let status: "current" | "stale" = "current";
		let reason: ReplicationInfoV2ReceiveDiagnostic["reason"];
		if (
			properties.peerSession !== undefined &&
			state.peerSession !== properties.peerSession
		) {
			status = "stale";
			reason = "peer-session-mismatch";
		} else if (
			properties.receiveEpoch !== undefined &&
			state.receiveEpoch !== properties.receiveEpoch
		) {
			status = "stale";
			reason = "receive-epoch-mismatch";
		} else if (
			properties.senderTransportSession !== undefined &&
			state.senderTransportSession !== properties.senderTransportSession
		) {
			status = "stale";
			reason = "transport-session-mismatch";
		} else if (!this.isStateCurrent(state)) {
			status = "stale";
			reason = "state-not-current";
		} else if (
			state.reservedAdmission !== undefined ||
			this._reservedAdmissionsByPeer.has(properties.peerHash)
		) {
			reason = "admission-pending";
		} else if (state.phase === "active") {
			reason = "active";
		} else if (state.requestParked) {
			reason = "request-parked";
		} else if (state.requestInFlight !== undefined) {
			reason = "request-in-flight";
		} else if (state.requestTimer !== undefined) {
			reason = "request-scheduled";
		} else {
			reason = state.phase;
		}
		const requestState: NonNullable<
			ReplicationInfoV2ReceiveDiagnostic["requestState"]
		> =
			state.reservedAdmission !== undefined ||
			this._reservedAdmissionsByPeer.has(properties.peerHash)
				? "admission-pending"
				: state.requestParked
					? "parked"
					: state.requestInFlight !== undefined
						? "in-flight"
						: state.requestTimer !== undefined
							? "scheduled"
							: "idle";
		return Object.freeze({
			state: status,
			reason,
			phase: state.phase,
			requestState,
			...(state.lastSequence === undefined
				? {}
				: { lastAppliedSequence: state.lastSequence.toString() }),
			requestAttempts: Math.min(
				this.requestMaxAttempts,
				Math.max(0, state.requestAttempts),
			),
			...common,
		});
	}

	private promoteLocalCapabilityAdvertisement(
		state: ReplicationInfoV2LocalCapabilityAdvertisement,
	): boolean {
		const ready = state.acknowledgedReady;
		if (
			state.ready ||
			!ready ||
			ready.receiveEpoch !== state.receiveEpoch ||
			ready.advertisement !== state ||
			!this.isLocalCapabilityAdvertisementOwnerCurrent(state) ||
			!this.isLocalCapabilityAdvertisementGenerationCurrent(state) ||
			this.deps.getReceiverTransportSession() !== ready.receiverTransportSession
		) {
			return state.ready;
		}
		if (!this.isLocalCapabilityAdvertisementReadyOpen(state)) {
			this.armLocalCapabilityAdvertisement(state);
			return false;
		}
		if (
			!this.recordLocalCapabilityReady(
				{
					peerHash: state.peerHash,
					peerSession: state.peerSession,
					receiveEpoch: state.receiveEpoch,
					receiverTransportSession: ready.receiverTransportSession,
					requestNotBeforeMs: ready.requestNotBeforeMs,
				},
				state,
			)
		) {
			return false;
		}
		state.ready = true;
		return true;
	}

	/**
	 * Re-advertise one exact current recovery epoch from the stable membership
	 * context captured during opening.
	 */
	reAdvertiseLocalCapabilityForRecovery(properties: {
		peerHash: string;
		peerSession: object;
		receiveEpoch: object | null;
	}): boolean {
		const context = this._localCapabilityContextBySession.get(
			properties.peerSession,
		);
		if (
			!context ||
			context.peerHash !== properties.peerHash ||
			context.lifecycleSignal.aborted ||
			this.deps.isClosed() ||
			!this.deps.isPeerSessionCurrent(
				properties.peerHash,
				properties.peerSession,
			) ||
			!this.deps.isReceiveEpochCurrent(
				properties.peerHash,
				properties.receiveEpoch,
			)
		) {
			return false;
		}
		const state = this._receiveStates.get(properties.peerHash);
		if (
			state?.peerSession === properties.peerSession &&
			state.receiveEpoch === properties.receiveEpoch &&
			state.receiverBinding !== undefined
		) {
			return false;
		}
		const ready = this._localCapabilityReadyBySession.get(
			properties.peerSession,
		);
		if (
			ready?.peerHash === properties.peerHash &&
			ready.receiveEpoch === properties.receiveEpoch &&
			ready.receiverTransportSession === this.deps.getReceiverTransportSession()
		) {
			return false;
		}
		this.advertiseLocalCapability({
			target: context.target,
			peerSession: properties.peerSession,
			receiveEpoch: properties.receiveEpoch,
			signal: context.lifecycleSignal,
		});
		return true;
	}

	/** Record success of this session's ACKed local APPLY advertisement. */
	markLocalCapabilityReady(
		properties: LocalCapabilityReadyProperties,
	): boolean {
		return this.recordLocalCapabilityReady(properties);
	}

	private recordLocalCapabilityReady(
		properties: LocalCapabilityReadyProperties,
		advertisement?: ReplicationInfoV2LocalCapabilityAdvertisement,
	): boolean {
		const { peerHash, peerSession, receiverTransportSession } = properties;
		const state = this._receiveStates.get(peerHash);
		if (
			this.deps.isClosed() ||
			!this.deps.isPeerStateCurrent(
				peerHash,
				peerSession,
				properties.receiveEpoch,
			) ||
			this.deps.getReceiverTransportSession() !== receiverTransportSession
		) {
			return false;
		}

		const ready: LocalCapabilityReady = {
			peerHash,
			receiveEpoch: properties.receiveEpoch,
			receiverTransportSession,
			requestNotBeforeMs: properties.requestNotBeforeMs,
			advertisement,
		};
		this._localCapabilityReadyBySession.set(peerSession, ready);
		if (
			state?.peerSession === peerSession &&
			state.receiveEpoch === properties.receiveEpoch
		) {
			if (
				state.receiverTransportSession !== undefined &&
				state.receiverTransportSession !== receiverTransportSession
			) {
				this.clearState(state);
				return false;
			}
			this.bindLocalCapability(state, ready);
			state.requestAttempts = 0;
			state.requestsSinceCapabilityRefresh = 0;
			state.requestParked = false;
			this.armRequest(state, 0);
		}
		return true;
	}

	/**
	 * Promote one signed capability generation after the opening barrier has
	 * committed. Repeated same-session advertisements refresh freshness only;
	 * they never reset sequence state.
	 */
	observeCapability(properties: {
		peerHash: string;
		target: PublicSignKey;
		peerSession: object;
		receiveEpoch: object | null;
		capabilities: number;
		senderTransportSession: bigint;
		capabilityTimestamp: bigint;
	}): boolean {
		const {
			peerHash,
			target,
			peerSession,
			receiveEpoch,
			capabilities,
			senderTransportSession,
			capabilityTimestamp,
		} = properties;
		if (
			target.equals(this.deps.getSelfKey()) ||
			this.deps.isClosed() ||
			!this.deps.isPeerStateCurrent(peerHash, peerSession, receiveEpoch)
		) {
			return false;
		}

		const senderReady =
			(capabilities & REQUIRED_SENDER_CAPABILITIES) ===
			REQUIRED_SENDER_CAPABILITIES;
		let state = this._receiveStates.get(peerHash);
		if (
			state &&
			(state.peerSession !== peerSession ||
				state.senderTransportSession !== senderTransportSession ||
				!state.target.equals(target))
		) {
			const preserveCutover = state.peerSession === peerSession && senderReady;
			this.clearState(state);
			if (!preserveCutover) {
				this._cutoverPeerSessions.delete(state.peerSession);
			}
			state = undefined;
		}
		if (!senderReady) {
			if (state) {
				this.clearState(state);
				this._cutoverPeerSessions.delete(peerSession);
			}
			return false;
		}

		if (state) {
			if (capabilityTimestamp < state.capabilityTimestamp) {
				return false;
			}
			const previousCapabilities = state.capabilities;
			const previousTimestamp = state.capabilityTimestamp;
			const receiveEpochChanged = state.receiveEpoch !== receiveEpoch;
			const addsCapabilities = (capabilities & ~previousCapabilities) !== 0;
			if (
				capabilityTimestamp === previousTimestamp &&
				!addsCapabilities &&
				!receiveEpochChanged
			) {
				return true;
			}
			state.capabilities |= capabilities;
			state.capabilityTimestamp = capabilityTimestamp;
			if (receiveEpochChanged) {
				state.receiveEpoch = receiveEpoch;
				this.transitionToResync(state, {
					force: true,
					refreshCapability: true,
				});
			}
			const ready = this._localCapabilityReadyBySession.get(peerSession);
			if (
				ready?.peerHash === peerHash &&
				ready.receiveEpoch === receiveEpoch &&
				state.receiverBinding === undefined
			) {
				this.bindLocalCapability(state, ready);
			}
			if (state.phase !== "active") {
				state.requestAttempts = 0;
				state.requestParked = false;
				this.armRequest(state, 0);
			}
			return true;
		}

		const retainedCutover = this._cutoverPeerSessions.has(peerSession);
		state = {
			peerHash,
			target,
			peerSession,
			receiveEpoch,
			capabilities,
			capabilityTimestamp,
			senderTransportSession,
			receiverRequestChallenge: randomBytes(32),
			phase: retainedCutover ? "resync" : "awaiting-full",
			version: 0,
			controller: new AbortController(),
			requestAttempts: 0,
			requestsSinceCapabilityRefresh: 0,
			requestParked: false,
			capabilityRefreshRequired: retainedCutover,
		};
		this._receiveStates.set(peerHash, state);
		const ready = this._localCapabilityReadyBySession.get(peerSession);
		if (ready?.peerHash === peerHash && ready.receiveEpoch === receiveEpoch) {
			this.bindLocalCapability(state, ready);
			this.armRequest(state, 0);
		}
		return true;
	}

	private bindLocalCapability(
		state: ReplicationInfoV2ReceiveState,
		ready: LocalCapabilityReady,
	): void {
		state.receiverTransportSession = ready.receiverTransportSession;
		state.receiverBinding = deriveReplicationInfoV2ReceiverBinding({
			receiverChallenge: state.receiverRequestChallenge,
			receiver: this.deps.getSelfKey(),
			receiverTransportSession: ready.receiverTransportSession,
			sender: state.target,
			senderTransportSession: state.senderTransportSession,
		});
	}

	/** Require a fresh capability-bound grant and authoritative Full. */
	advanceRecovery(properties: {
		peerHash: string;
		peerSession: object;
		receiveEpoch: object | null;
	}): boolean {
		const state = this._receiveStates.get(properties.peerHash);
		if (!state || state.peerSession !== properties.peerSession) {
			return false;
		}
		state.receiveEpoch = properties.receiveEpoch;
		this.transitionToResync(state, {
			force: true,
			refreshCapability: true,
		});
		return true;
	}

	/**
	 * Whether the request generation for this exact peer state is parked: the
	 * bounded retry cycle exhausted its attempts and no timer or send is in
	 * flight. Read-only probe for the host's recovery scheduler.
	 */
	isRequestParked(properties: {
		peerHash: string;
		peerSession: object;
		receiveEpoch: object | null;
	}): boolean {
		const state = this._receiveStates.get(properties.peerHash);
		return (
			state !== undefined &&
			state.peerSession === properties.peerSession &&
			state.receiveEpoch === properties.receiveEpoch &&
			state.phase !== "active" &&
			state.requestParked &&
			state.requestTimer === undefined &&
			state.requestInFlight === undefined
		);
	}

	/**
	 * Ensure an exact current request generation has a worker. In addition to
	 * resuming a bounded cycle that parked normally, this repairs a request whose
	 * timer fired while the host's receive gate was temporarily closed: that
	 * generation is neither parked nor active, but has no timer or send in flight.
	 */
	ensureRequestProgress(properties: {
		peerHash: string;
		peerSession: object;
		receiveEpoch: object | null;
	}): boolean {
		return this.nudgeRequest(properties, false);
	}

	/**
	 * Resume only a request generation that exhausted its bounded retries.
	 * Wait/liveness callers may nudge recovery without invalidating an active,
	 * timed or in-flight request and without advancing the receive epoch.
	 */
	resumeParkedRequest(properties: {
		peerHash: string;
		peerSession: object;
		receiveEpoch: object | null;
	}): boolean {
		return this.nudgeRequest(properties, true);
	}

	private nudgeRequest(
		properties: {
			peerHash: string;
			peerSession: object;
			receiveEpoch: object | null;
		},
		parkedOnly: boolean,
	): boolean {
		const state = this._receiveStates.get(properties.peerHash);
		if (
			!state ||
			state.peerSession !== properties.peerSession ||
			state.receiveEpoch !== properties.receiveEpoch ||
			!this.isStateCurrent(state) ||
			state.phase === "active" ||
			(parkedOnly && !state.requestParked) ||
			state.requestTimer !== undefined ||
			state.requestInFlight !== undefined ||
			this._reservedAdmissionsByPeer.has(properties.peerHash) ||
			state.receiverBinding === undefined ||
			state.lastSequence === MAX_U64
		) {
			return false;
		}
		const restartBoundedCycle =
			state.requestParked || state.requestAttempts >= this.requestMaxAttempts;
		if (restartBoundedCycle) {
			state.requestAttempts = 0;
			state.requestsSinceCapabilityRefresh = 0;
		}
		if (restartBoundedCycle && !state.capabilityRefreshRequired) {
			// Only rotate the grant when it is genuinely stale: the peer's
			// capability rotation paths already flagged a refresh, and a missing
			// or transport-outdated local grant cannot authorize a request. A
			// still-current grant must keep its challenge so a Full already in
			// flight for the pre-park request generation still applies.
			const ready = this._localCapabilityReadyBySession.get(state.peerSession);
			const grantCurrent =
				ready !== undefined &&
				ready.peerHash === state.peerHash &&
				ready.receiveEpoch === state.receiveEpoch &&
				ready.receiverTransportSession === state.receiverTransportSession &&
				ready.receiverTransportSession ===
					this.deps.getReceiverTransportSession();
			if (!grantCurrent) {
				state.capabilityRefreshRequired = true;
			}
		}
		state.requestParked = false;
		this.armRequest(state, 0);
		return true;
	}

	private transitionToResync(
		state: ReplicationInfoV2ReceiveState,
		options?: { force?: boolean; refreshCapability?: boolean },
	): void {
		const shouldRestart =
			state.phase !== "resync" ||
			options?.force === true ||
			state.requestParked;
		if (state.phase !== "resync" || options?.force === true) {
			state.phase = "resync";
			state.version++;
		}
		if (options?.refreshCapability) {
			state.capabilityRefreshRequired = true;
		}
		if (shouldRestart) {
			state.requestAttempts = 0;
			state.requestParked = false;
			this.armRequest(state, 0);
		}
	}

	prepare(
		message: ReplicationInfoV2Message,
		properties: {
			from: PublicSignKey;
			peerSession: object;
			receiveEpoch: object | null;
			senderTransportSession: bigint;
			transportTimestamp: bigint;
		},
	): ReplicationInfoV2ReceiveAdmission | undefined {
		const peerHash = properties.from.hashcode();
		const state = this._receiveStates.get(peerHash);
		if (
			!state ||
			state.peerSession !== properties.peerSession ||
			state.receiveEpoch !== properties.receiveEpoch ||
			state.senderTransportSession !== properties.senderTransportSession ||
			!state.target.equals(properties.from) ||
			!state.receiverBinding ||
			!bytesEqual(message.receiverChallenge, state.receiverBinding) ||
			message.sequence <= 0n ||
			!this.isStateCurrent(state)
		) {
			return undefined;
		}

		const kind =
			message instanceof FullReplicationInfoV2Message
				? "full"
				: message instanceof AddedReplicationInfoV2Message
					? "added"
					: message instanceof StoppedReplicationInfoV2Message
						? "stopped"
						: undefined;
		if (!kind) {
			return undefined;
		}

		if (state.senderEpoch === undefined) {
			if (kind !== "full") {
				return undefined;
			}
		} else if (!bytesEqual(message.senderEpoch, state.senderEpoch)) {
			return undefined;
		}

		const lastSequence = state.lastSequence;
		if (kind === "full") {
			if (lastSequence !== undefined && message.sequence <= lastSequence) {
				return undefined;
			}
		} else {
			if (
				state.phase !== "active" ||
				lastSequence === undefined ||
				message.sequence !== lastSequence + 1n
			) {
				if (
					state.phase === "active" &&
					lastSequence !== undefined &&
					message.sequence > lastSequence + 1n
				) {
					this.transitionToResync(state);
				}
				return undefined;
			}
		}

		return {
			state,
			version: state.version,
			receiveEpoch: state.receiveEpoch,
			message,
			kind,
			payloadFingerprint: replicationInfoPayloadFingerprint(message),
			transportTimestamp: properties.transportTimestamp,
			committed: false,
		};
	}

	/** Reserve at most one decoded V2 frame per peer ahead of the apply lane. */
	reserve(
		message: ReplicationInfoV2Message,
		properties: {
			from: PublicSignKey;
			peerSession: object;
			receiveEpoch: object | null;
			senderTransportSession: bigint;
			transportTimestamp: bigint;
		},
	): ReplicationInfoV2ReceiveAdmission | undefined {
		const peerHash = properties.from.hashcode();
		const reserved = this._reservedAdmissionsByPeer.get(peerHash);
		if (reserved) {
			const state = reserved.state;
			const currentState = this._receiveStates.get(peerHash);
			const knownMessage =
				message instanceof FullReplicationInfoV2Message ||
				message instanceof AddedReplicationInfoV2Message ||
				message instanceof StoppedReplicationInfoV2Message;
			if (
				knownMessage &&
				this._receiveStates.get(peerHash) === state &&
				state.peerSession === properties.peerSession &&
				state.receiveEpoch === properties.receiveEpoch &&
				state.senderTransportSession === properties.senderTransportSession &&
				state.target.equals(properties.from) &&
				state.receiverBinding !== undefined &&
				bytesEqual(message.receiverChallenge, state.receiverBinding) &&
				bytesEqual(message.senderEpoch, reserved.message.senderEpoch) &&
				message.sequence > reserved.message.sequence &&
				this.isStateCurrent(state)
			) {
				// Transport ACKs precede application. Do not invalidate the frame
				// already applying, and do not retain an unbounded successor queue.
				// Commit the reservation, then request one authoritative Full.
				reserved.resyncAfterRelease = true;
			} else if (
				knownMessage &&
				currentState !== undefined &&
				currentState !== state &&
				this.prepare(message, properties)?.state === currentState
			) {
				// A previous generation can still be parked in the host apply lane.
				// Transport already ACKed this current-generation frame, so wake the
				// current state once the peer-global reservation is finally released.
				reserved.resyncAfterRelease = true;
			}
			return undefined;
		}
		const admission = this.prepare(message, properties);
		if (!admission) {
			return undefined;
		}
		const { state } = admission;
		this._reservedAdmissionsByPeer.set(peerHash, admission);
		state.reservedAdmission = admission;
		if (state.requestTimer) {
			clearTimeout(state.requestTimer);
			state.requestTimer = undefined;
		}
		return admission;
	}

	release(admission: ReplicationInfoV2ReceiveAdmission): void {
		const { state } = admission;
		if (this._reservedAdmissionsByPeer.get(state.peerHash) !== admission) {
			return;
		}
		this._reservedAdmissionsByPeer.delete(state.peerHash);
		if (state.reservedAdmission === admission) {
			state.reservedAdmission = undefined;
		}
		const currentState = this._receiveStates.get(state.peerHash);
		if (
			currentState &&
			this.isStateCurrent(currentState) &&
			(admission.resyncAfterRelease ||
				(currentState !== state && currentState.phase !== "active"))
		) {
			this.transitionToResync(currentState, { force: true });
		} else if (
			currentState === state &&
			!admission.committed &&
			this.isStateCurrent(state) &&
			state.phase !== "active" &&
			state.requestTimer === undefined &&
			state.requestInFlight === undefined &&
			!state.requestParked
		) {
			// Reserving a Full pauses this generation's retry timer. If its host
			// apply lane releases without committing, restore the bounded request
			// worker so the exact current generation cannot become stranded.
			this.armRequest(state, 0);
		}
	}

	isAdmissionCurrent(admission: ReplicationInfoV2ReceiveAdmission): boolean {
		const { state } = admission;
		return (
			state.version === admission.version &&
			state.receiveEpoch === admission.receiveEpoch &&
			this.isStateCurrent(state)
		);
	}

	/**
	 * Exact active receive generation used by durability-sensitive callers.
	 * An open PeerSession alone is not evidence that its authoritative Full has
	 * crossed the host apply lane, especially during a same-key reconnect.
	 */
	isCurrentActive(properties: {
		peerHash: string;
		peerSession: object;
		receiveEpoch: object | null;
		senderTransportSession: bigint;
	}): boolean {
		const state = this._receiveStates.get(properties.peerHash);
		return (
			state !== undefined &&
			state.peerHash === properties.peerHash &&
			state.peerSession === properties.peerSession &&
			state.receiveEpoch === properties.receiveEpoch &&
			state.senderTransportSession === properties.senderTransportSession &&
			state.phase === "active" &&
			state.reservedAdmission === undefined &&
			!this._reservedAdmissionsByPeer.has(properties.peerHash) &&
			this.isStateCurrent(state)
		);
	}

	commit(admission: ReplicationInfoV2ReceiveAdmission): boolean {
		if (admission.committed) {
			return this.isAdmissionCurrent(admission);
		}
		if (!this.isAdmissionCurrent(admission)) {
			this.release(admission);
			return false;
		}
		const { state, message } = admission;
		if (admission.kind === "full") {
			if (state.senderEpoch === undefined) {
				state.senderEpoch = message.senderEpoch.slice();
				this._cutoverPeerSessions.add(state.peerSession);
			} else if (!bytesEqual(state.senderEpoch, message.senderEpoch)) {
				this.release(admission);
				return false;
			}
		} else if (
			state.senderEpoch === undefined ||
			!bytesEqual(state.senderEpoch, message.senderEpoch)
		) {
			this.release(admission);
			return false;
		}
		state.lastSequence = message.sequence;
		state.phase = "active";
		state.requestAttempts = 0;
		state.requestsSinceCapabilityRefresh = 0;
		state.requestParked = false;
		state.capabilityRefreshRequired = false;
		if (state.requestTimer) {
			clearTimeout(state.requestTimer);
			state.requestTimer = undefined;
		}
		state.version++;
		admission.version = state.version;
		admission.receiveEpoch = state.receiveEpoch;
		admission.committed = true;
		this.release(admission);
		return true;
	}

	/**
	 * Answer only after the exact authenticated receive generation has committed
	 * the queried sequence (or a newer one). No pending query is retained: the
	 * sender's bounded retry loop reissues an authoritative snapshot and query.
	 */
	confirmApplied(
		request: RequestReplicationInfoV2AppliedMessage,
		properties: {
			from: PublicSignKey;
			peerSession: object;
			receiveEpoch: object | null;
			senderTransportSession: bigint;
		},
	): ReplicationInfoV2AppliedMessage | undefined {
		const state = this._receiveStates.get(properties.from.hashcode());
		if (
			!state ||
			!this.isCurrentActive({
				peerHash: properties.from.hashcode(),
				peerSession: properties.peerSession,
				receiveEpoch: properties.receiveEpoch,
				senderTransportSession: properties.senderTransportSession,
			}) ||
			!state.target.equals(properties.from) ||
			!state.receiverBinding ||
			!bytesEqual(request.receiverChallenge, state.receiverBinding) ||
			!state.senderEpoch ||
			!bytesEqual(request.senderEpoch, state.senderEpoch) ||
			request.sequence <= 0n ||
			state.lastSequence === undefined ||
			state.lastSequence < request.sequence
		) {
			return undefined;
		}
		return new ReplicationInfoV2AppliedMessage({
			receiverChallenge: request.receiverChallenge.slice(),
			senderEpoch: request.senderEpoch.slice(),
			sequence: request.sequence,
			revision: request.revision,
		});
	}

	requireFullAfterFailure(
		admission: ReplicationInfoV2ReceiveAdmission,
	): boolean {
		if (!this.isAdmissionCurrent(admission)) {
			this.release(admission);
			return false;
		}
		this.release(admission);
		this.transitionToResync(admission.state, { force: true });
		return true;
	}

	private isStateCurrent(state: ReplicationInfoV2ReceiveState): boolean {
		return (
			this._receiveStates.get(state.peerHash) === state &&
			!state.controller.signal.aborted &&
			!this.deps.isClosed() &&
			state.receiverTransportSession !== undefined &&
			this.deps.getReceiverTransportSession() ===
				state.receiverTransportSession &&
			this.deps.isSenderTransportSessionCurrent(
				state.peerHash,
				state.senderTransportSession,
			) &&
			this.deps.isPeerStateCurrent(
				state.peerHash,
				state.peerSession,
				state.receiveEpoch,
			)
		);
	}

	private armRequest(
		state: ReplicationInfoV2ReceiveState,
		delayMs: number,
	): void {
		if (state.requestTimer) {
			clearTimeout(state.requestTimer);
		}
		if (
			this._receiveStates.get(state.peerHash) !== state ||
			state.controller.signal.aborted ||
			this.deps.isClosed() ||
			this._reservedAdmissionsByPeer.has(state.peerHash) ||
			state.receiverBinding === undefined ||
			state.phase === "active" ||
			state.requestParked ||
			state.lastSequence === MAX_U64
		) {
			state.requestTimer = undefined;
			return;
		}
		state.requestTimer = setTimeout(
			() => {
				state.requestTimer = undefined;
				void this.runRequest(state);
			},
			Math.max(0, delayMs),
		);
		state.requestTimer.unref?.();
	}

	private requestRetryDelay(state: ReplicationInfoV2ReceiveState): number {
		const exponent = Math.max(0, state.requestAttempts - 1);
		return Math.min(
			this.maxRequestRetryMs,
			this.requestRetryMs * 2 ** Math.min(exponent, 20),
		);
	}

	private async refreshGrant(
		state: ReplicationInfoV2ReceiveState,
	): Promise<boolean> {
		const version = state.version;
		const refreshed = await this.deps.refreshLocalCapability({
			peerHash: state.peerHash,
			target: state.target,
			peerSession: state.peerSession,
			receiveEpoch: state.receiveEpoch,
			signal: state.controller.signal,
		});
		if (
			!refreshed ||
			state.version !== version ||
			this._reservedAdmissionsByPeer.has(state.peerHash) ||
			!this.isStateCurrent(state) ||
			this.deps.getReceiverTransportSession() !==
				refreshed.receiverTransportSession
		) {
			return false;
		}

		const ready: LocalCapabilityReady = {
			peerHash: state.peerHash,
			receiveEpoch: state.receiveEpoch,
			receiverTransportSession: refreshed.receiverTransportSession,
			requestNotBeforeMs: refreshed.requestNotBeforeMs,
		};
		this._localCapabilityReadyBySession.set(state.peerSession, ready);
		state.receiverRequestChallenge = randomBytes(32);
		state.senderEpoch = undefined;
		state.lastSequence = undefined;
		state.phase = "resync";
		state.capabilityRefreshRequired = false;
		state.requestsSinceCapabilityRefresh = 0;
		state.version++;
		this.bindLocalCapability(state, ready);
		return true;
	}

	private async runRequest(
		state: ReplicationInfoV2ReceiveState,
	): Promise<void> {
		if (state.requestInFlight) {
			return;
		}
		if (
			!this.isStateCurrent(state) ||
			this._reservedAdmissionsByPeer.has(state.peerHash)
		) {
			return;
		}
		if (
			state.phase === "active" ||
			state.requestParked ||
			state.lastSequence === MAX_U64
		) {
			return;
		}
		if (state.requestAttempts >= this.requestMaxAttempts) {
			state.requestParked = true;
			return;
		}

		let operation: Promise<void>;
		operation = (async () => {
			if (state.capabilityRefreshRequired) {
				state.requestAttempts++;
				if (!(await this.refreshGrant(state))) {
					return;
				}
			}
			if (
				!this.isStateCurrent(state) ||
				this._reservedAdmissionsByPeer.has(state.peerHash)
			) {
				return;
			}
			const ready = this._localCapabilityReadyBySession.get(state.peerSession);
			if (
				!ready ||
				ready.peerHash !== state.peerHash ||
				ready.receiveEpoch !== state.receiveEpoch ||
				ready.receiverTransportSession !== state.receiverTransportSession
			) {
				return;
			}
			const now = this.now();
			if (now <= ready.requestNotBeforeMs) {
				this.armRequest(state, ready.requestNotBeforeMs - now + 1);
				return;
			}
			if (state.requestAttempts >= this.requestMaxAttempts) {
				state.requestParked = true;
				return;
			}
			state.requestAttempts++;
			state.requestsSinceCapabilityRefresh++;
			const request = new RequestReplicationInfoV2Message({
				receiverChallenge: state.receiverRequestChallenge.slice(),
				intendedSender: state.target,
				senderSession: state.senderTransportSession,
			});
			await this.deps.sendRequest(
				request,
				state.target,
				state.controller.signal,
			);
		})()
			.catch((error) => {
				if (!state.controller.signal.aborted && !this.deps.isClosed()) {
					this.deps.onRequestError?.(error);
				}
			})
			.finally(() => {
				if (state.requestInFlight === operation) {
					state.requestInFlight = undefined;
				}
				if (
					this._receiveStates.get(state.peerHash) === state &&
					!state.controller.signal.aborted &&
					!this._reservedAdmissionsByPeer.has(state.peerHash) &&
					!state.requestTimer &&
					state.phase !== "active"
				) {
					if (state.requestsSinceCapabilityRefresh >= 3) {
						state.capabilityRefreshRequired = true;
					}
					if (state.requestAttempts >= this.requestMaxAttempts) {
						state.requestParked = true;
					} else {
						this.armRequest(state, this.requestRetryDelay(state));
					}
				}
			});
		state.requestInFlight = operation;
		await operation;
	}
}
