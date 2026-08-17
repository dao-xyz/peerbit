import { serialize } from "@dao-xyz/borsh";
import {
	type PublicSignKey,
	randomBytes,
	sha256Base64Sync,
	sha256Sync,
	toHexString,
} from "@peerbit/crypto";
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
	type ReplicationInfoV2Message,
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
const MAX_U64 = (1n << 64n) - 1n;
const MAX_BACKOFF_EXPONENT = 20;

export const MAX_REPLICATION_INFO_V2_CURRENT_OWNER_BATCH = 4096;
export const MAX_REPLICATION_INFO_V2_CURRENT_OWNER_KEY_BYTES = 512;
export const MAX_REPLICATION_INFO_V2_CURRENT_OWNER_HASH_BYTES = 512;
export const DEFAULT_REPLICATION_INFO_V2_CURRENT_OWNER_TTL_MS = 60_000;
export const MAX_REPLICATION_INFO_V2_CURRENT_OWNER_TTL_MS = 5 * 60_000;

const utf8 = new TextEncoder();

/**
 * Volatile evidence that one authenticated V2 update was applied under the
 * current receiver grant. It is not consensus, custody, transfer, or prune
 * authority. It deliberately does not freeze the host's custom replicator
 * policy; any later placement capture must re-evaluate current authorization.
 */
export type ReplicationInfoV2CurrentRemoteOwnerToken = Readonly<{
	peerHash: string;
	/** Canonical bytes of the authenticated sender key, copied and hex encoded. */
	publicKeyHex: string;
	senderTransportSession: bigint;
	receiverTransportSession: bigint;
	/** The capability-bound receiver binding carried by admitted V2 frames. */
	receiverBindingHex: string;
	senderEpochHex: string;
	sequence: bigint;
}>;

export type ReplicationInfoV2CurrentRemoteOwner = Readonly<{
	revision: bigint;
	/** Conservative wall-clock projection of a monotonic local deadline. */
	freshUntilMs: bigint;
	token: ReplicationInfoV2CurrentRemoteOwnerToken;
}>;

export type ReplicationInfoV2CurrentRemoteOwnerCapture = Readonly<{
	revision: bigint;
	/** Minimum conservative wall deadline across every returned token. */
	freshUntilMs?: bigint;
	tokens: ReadonlyArray<ReplicationInfoV2CurrentRemoteOwnerToken>;
}>;

type ReplicationInfoV2RemoteOwnerGrant = {
	generation: object;
	openGeneration: object;
	receiveEpoch: object | null;
	senderTransportSession: bigint;
	receiverTransportSession: bigint;
	receiverRequestChallenge: Uint8Array;
	receiverBinding: Uint8Array;
	deadlineMonotonicMs: number;
	fullFinalized: boolean;
};

type ReplicationInfoV2CurrentRemoteOwnerEntry = {
	openGeneration: object;
	state: ReplicationInfoV2ReceiveState;
	peerSession: object;
	receiveEpoch: object | null;
	grantGeneration: object;
	senderTransportSession: bigint;
	receiverTransportSession: bigint;
	receiverRequestChallenge: Uint8Array;
	receiverBinding: Uint8Array;
	senderEpoch: Uint8Array;
	sequence: bigint;
	deadlineMonotonicMs: number;
	token: ReplicationInfoV2CurrentRemoteOwnerToken;
};

type ReplicationInfoV2CurrentRemoteOwnerCaptureEntry = {
	openGeneration: object;
	revision: bigint;
	deadlineMonotonicMs?: number;
};

type ReplicationInfoV2RemoteOwnerMutationFence = {
	openGeneration: object;
	state: ReplicationInfoV2ReceiveState;
	grantGeneration: object;
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
	/** Local-only freshness for the exact currently bound receiver grant. */
	remoteOwnerGrant?: ReplicationInfoV2RemoteOwnerGrant;
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
	/** Exact current SharedLog ownership lifecycle, including poison/rotation. */
	isOwnershipActive: () => boolean;
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
	}) => Promise<ReplicationInfoV2LocalCapabilityRefresh | undefined>;
	onRequestError?: (error: unknown) => void;
	onLocalCapabilityError?: (error: unknown) => void;
	now?: () => number;
	/** Receiver-local monotonic clock; remote/wall timestamps never grant freshness. */
	monotonicNow?: () => number;
	/** Wall clock used only to project a diagnostic/capture deadline. */
	wallNow?: () => number;
	currentRemoteOwnerTtlMs?: number;
	requestRetryMs?: number;
	maxRequestRetryMs?: number;
	requestMaxAttempts?: number;
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
	_reservedAdmissionsByPeer!: Map<string, ReplicationInfoV2ReceiveAdmission>;
	_currentRemoteOwnerByPeer!: Map<
		string,
		ReplicationInfoV2CurrentRemoteOwnerEntry
	>;
	_currentRemoteOwnerOpenGeneration!: object;
	_currentRemoteOwnerRevision!: bigint;
	_currentRemoteOwnerTokens!: WeakSet<object>;
	_currentRemoteOwnerCaptures!: WeakMap<
		object,
		ReplicationInfoV2CurrentRemoteOwnerCaptureEntry
	>;
	_currentRemoteOwnerMutationFences!: WeakMap<
		object,
		ReplicationInfoV2RemoteOwnerMutationFence
	>;

	private readonly now: () => number;
	private readonly monotonicNow: () => number;
	private readonly wallNow: () => number;
	private readonly currentRemoteOwnerTtlMs: number;
	private lastMonotonicNow = 0;
	private monotonicNowInitialized = false;
	private readonly requestRetryMs: number;
	private readonly maxRequestRetryMs: number;
	private readonly requestMaxAttempts: number;

	constructor(private readonly deps: ReplicationInfoV2ReceiveDeps) {
		this.now = deps.now ?? Date.now;
		this.monotonicNow =
			deps.monotonicNow ??
			(() => globalThis.performance?.now?.() ?? Date.now());
		this.wallNow = deps.wallNow ?? Date.now;
		const currentRemoteOwnerTtlMs =
			deps.currentRemoteOwnerTtlMs ??
			DEFAULT_REPLICATION_INFO_V2_CURRENT_OWNER_TTL_MS;
		if (
			!Number.isSafeInteger(currentRemoteOwnerTtlMs) ||
			currentRemoteOwnerTtlMs <= 0 ||
			currentRemoteOwnerTtlMs > MAX_REPLICATION_INFO_V2_CURRENT_OWNER_TTL_MS
		) {
			throw new Error("Invalid replication-info V2 current-owner TTL");
		}
		this.currentRemoteOwnerTtlMs = currentRemoteOwnerTtlMs;
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
		this._receiveStates = new Map();
		this._cutoverPeerSessions = new WeakSet();
		this._localCapabilityReadyBySession = new WeakMap();
		this._localCapabilityContextBySession = new WeakMap();
		this._localCapabilityAdvertisementsByPeer = new Map();
		this._reservedAdmissionsByPeer = new Map();
		this.resetCurrentRemoteOwnerProvenance();
	}

	resetForOpen(): void {
		this.clearForClose();
		this._receiveStates = new Map();
		this._cutoverPeerSessions = new WeakSet();
		this._localCapabilityReadyBySession = new WeakMap();
		this._localCapabilityContextBySession = new WeakMap();
		this._localCapabilityAdvertisementsByPeer = new Map();
		this._reservedAdmissionsByPeer = new Map();
		this.resetCurrentRemoteOwnerProvenance();
	}

	clearForClose(): void {
		this.clearCurrentRemoteOwnerProvenance();
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
		if (expectedSession) {
			this._localCapabilityReadyBySession.delete(expectedSession);
			this._localCapabilityContextBySession.delete(expectedSession);
			this._cutoverPeerSessions.delete(expectedSession);
		}
		const current = this._currentRemoteOwnerByPeer.get(peerHash);
		if (!expectedSession || current?.peerSession === expectedSession) {
			this.invalidateCurrentRemoteOwner(peerHash);
		}
	}

	/** Revoke an unauthenticated or downgraded capability generation. */
	revokePeerCapability(peerHash: string): void {
		const state = this._receiveStates.get(peerHash);
		if (!state) {
			this.invalidateCurrentRemoteOwner(peerHash);
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
		this.invalidateCurrentRemoteOwner(state.peerHash, state);
		state.remoteOwnerGrant = undefined;
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

	private resetCurrentRemoteOwnerProvenance(): void {
		this._currentRemoteOwnerByPeer = new Map();
		this._currentRemoteOwnerOpenGeneration = {};
		this._currentRemoteOwnerRevision = 0n;
		this._currentRemoteOwnerTokens = new WeakSet();
		this._currentRemoteOwnerCaptures = new WeakMap();
		this._currentRemoteOwnerMutationFences = new WeakMap();
		this.lastMonotonicNow = 0;
		this.monotonicNowInitialized = false;
	}

	private clearCurrentRemoteOwnerProvenance(): void {
		if (this._currentRemoteOwnerByPeer?.size > 0) {
			this._currentRemoteOwnerRevision++;
		}
		this._currentRemoteOwnerByPeer = new Map();
		// Tokens from a closed open-generation must fail before consulting any
		// caller-visible diagnostic field.
		this._currentRemoteOwnerTokens = new WeakSet();
		this._currentRemoteOwnerCaptures = new WeakMap();
		this._currentRemoteOwnerMutationFences = new WeakMap();
		this._currentRemoteOwnerOpenGeneration = {};
	}

	private currentMonotonicNow(): number | undefined {
		let sampled: number;
		try {
			sampled = this.monotonicNow();
		} catch {
			this.clearCurrentRemoteOwnerProvenance();
			return undefined;
		}
		if (
			!Number.isFinite(sampled) ||
			sampled < 0 ||
			(this.monotonicNowInitialized && sampled < this.lastMonotonicNow)
		) {
			// An unmeasured interval can never be repaired by a later clock sample.
			// Rotate the authority generation so every already-bound grant fails
			// closed until the capability worker binds a fresh challenge.
			this.clearCurrentRemoteOwnerProvenance();
			return undefined;
		}
		this.monotonicNowInitialized = true;
		this.lastMonotonicNow = sampled;
		return this.lastMonotonicNow;
	}

	/** O(1) host lifecycle fence; protocol ordering state is intentionally kept. */
	invalidateCurrentRemoteOwnerProvenance(): void {
		this.clearCurrentRemoteOwnerProvenance();
	}

	private hasCurrentOwnershipAuthority(): boolean {
		let active = false;
		try {
			active = !this.deps.isClosed() && this.deps.isOwnershipActive();
		} catch {
			active = false;
		}
		if (!active && this._currentRemoteOwnerByPeer) {
			// Observing a poisoned/rotated host generation is itself a permanent
			// fence: old empty captures must not become current if the predicate
			// later flips back without a coordinator open reset.
			this.clearCurrentRemoteOwnerProvenance();
		}
		return active;
	}

	private isRemoteOwnerGrantCurrent(
		state: ReplicationInfoV2ReceiveState,
		grant: ReplicationInfoV2RemoteOwnerGrant,
	): boolean {
		return (
			grant.openGeneration === this._currentRemoteOwnerOpenGeneration &&
			grant.receiveEpoch === state.receiveEpoch &&
			grant.senderTransportSession === state.senderTransportSession &&
			state.receiverTransportSession !== undefined &&
			grant.receiverTransportSession === state.receiverTransportSession &&
			state.receiverBinding !== undefined &&
			bytesEqual(
				grant.receiverRequestChallenge,
				state.receiverRequestChallenge,
			) &&
			bytesEqual(grant.receiverBinding, state.receiverBinding)
		);
	}

	private boundedCurrentRemoteOwnerPeerHash(
		value: unknown,
	): string | undefined {
		if (
			typeof value !== "string" ||
			value.length === 0 ||
			value.length > MAX_REPLICATION_INFO_V2_CURRENT_OWNER_HASH_BYTES
		) {
			return undefined;
		}
		return utf8.encode(value).byteLength <=
			MAX_REPLICATION_INFO_V2_CURRENT_OWNER_HASH_BYTES
			? value
			: undefined;
	}

	private currentRemoteOwnerKeyFacts(
		state: ReplicationInfoV2ReceiveState,
	): { peerHash: string; publicKeyHex: string } | undefined {
		const bytes = state.target.bytes;
		if (
			!(bytes instanceof Uint8Array) ||
			bytes.byteLength === 0 ||
			bytes.byteLength > MAX_REPLICATION_INFO_V2_CURRENT_OWNER_KEY_BYTES
		) {
			return undefined;
		}
		const copied = bytes.slice();
		const peerHash = sha256Base64Sync(copied);
		if (peerHash !== state.peerHash) {
			return undefined;
		}
		return { peerHash, publicKeyHex: toHexString(copied) };
	}

	private invalidateCurrentRemoteOwner(
		peerHash: string,
		expectedState?: ReplicationInfoV2ReceiveState,
	): boolean {
		const current = this._currentRemoteOwnerByPeer?.get(peerHash);
		if (!current || (expectedState && current.state !== expectedState)) {
			return false;
		}
		this._currentRemoteOwnerByPeer.delete(peerHash);
		this._currentRemoteOwnerRevision++;
		return true;
	}

	private expireCurrentRemoteOwner(
		entry: ReplicationInfoV2CurrentRemoteOwnerEntry,
	): void {
		this.invalidateCurrentRemoteOwner(entry.token.peerHash, entry.state);
		const { state } = entry;
		if (
			this._receiveStates.get(state.peerHash) === state &&
			state.remoteOwnerGrant?.generation === entry.grantGeneration &&
			this.isStateCurrent(state)
		) {
			this.transitionToResync(state, {
				force: true,
				refreshCapability: true,
			});
		}
	}

	private expireCurrentRemoteOwnerGrantIfNeeded(
		peerHash: string,
		now: number,
	): void {
		const state = this._receiveStates.get(peerHash);
		const grant = state?.remoteOwnerGrant;
		if (
			state &&
			grant &&
			now >= grant.deadlineMonotonicMs &&
			this.isStateCurrent(state)
		) {
			this.transitionToResync(state, {
				force: true,
				refreshCapability: true,
			});
		}
	}

	private isCurrentRemoteOwnerEntry(
		entry: ReplicationInfoV2CurrentRemoteOwnerEntry,
		now: number,
	): boolean {
		const { state, token } = entry;
		const grant = state.remoteOwnerGrant;
		if (
			!this.hasCurrentOwnershipAuthority() ||
			this._currentRemoteOwnerByPeer.get(token.peerHash) !== entry ||
			entry.openGeneration !== this._currentRemoteOwnerOpenGeneration ||
			entry.peerSession !== state.peerSession ||
			entry.receiveEpoch !== state.receiveEpoch ||
			entry.senderTransportSession !== state.senderTransportSession ||
			entry.receiverTransportSession !== state.receiverTransportSession ||
			state.phase !== "active" ||
			state.senderEpoch === undefined ||
			state.lastSequence !== entry.sequence ||
			!bytesEqual(state.senderEpoch, entry.senderEpoch) ||
			state.receiverBinding === undefined ||
			!bytesEqual(state.receiverBinding, entry.receiverBinding) ||
			grant === undefined ||
			!this.isRemoteOwnerGrantCurrent(state, grant) ||
			grant.generation !== entry.grantGeneration ||
			grant.receiveEpoch !== entry.receiveEpoch ||
			grant.senderTransportSession !== entry.senderTransportSession ||
			grant.receiverTransportSession !== entry.receiverTransportSession ||
			!bytesEqual(
				grant.receiverRequestChallenge,
				entry.receiverRequestChallenge,
			) ||
			!bytesEqual(
				state.receiverRequestChallenge,
				entry.receiverRequestChallenge,
			) ||
			grant.deadlineMonotonicMs !== entry.deadlineMonotonicMs ||
			!grant.fullFinalized ||
			!bytesEqual(grant.receiverBinding, entry.receiverBinding) ||
			!this.isStateCurrent(state)
		) {
			this.invalidateCurrentRemoteOwner(token.peerHash, state);
			return false;
		}
		if (now >= entry.deadlineMonotonicMs) {
			this.expireCurrentRemoteOwner(entry);
			return false;
		}
		const key = this.currentRemoteOwnerKeyFacts(state);
		if (
			!key ||
			key.peerHash !== token.peerHash ||
			key.publicKeyHex !== token.publicKeyHex ||
			toHexString(state.senderEpoch) !== token.senderEpochHex ||
			toHexString(state.receiverBinding) !== token.receiverBindingHex
		) {
			this.invalidateCurrentRemoteOwner(token.peerHash, state);
			return false;
		}
		return true;
	}

	private currentRemoteOwnerFreshUntil(
		deadlineMonotonicMs: number,
		nowMonotonicMs: number,
	): bigint | undefined {
		let wallNow: number;
		try {
			wallNow = this.wallNow();
		} catch {
			return undefined;
		}
		if (!Number.isSafeInteger(wallNow) || wallNow < 0) {
			return undefined;
		}
		const projected = Math.floor(
			wallNow + Math.max(0, deadlineMonotonicMs - nowMonotonicMs),
		);
		return Number.isSafeInteger(projected) && projected > wallNow
			? BigInt(projected)
			: undefined;
	}

	/** Read one exact current token. It conveys no transfer or prune authority. */
	currentRemoteOwnerProvenance(
		peerHash: string,
	): ReplicationInfoV2CurrentRemoteOwner | undefined {
		const bounded = this.boundedCurrentRemoteOwnerPeerHash(peerHash);
		const now = this.currentMonotonicNow();
		if (!bounded || now === undefined || !this.hasCurrentOwnershipAuthority()) {
			return undefined;
		}
		const entry = this._currentRemoteOwnerByPeer.get(bounded);
		if (!entry || !this.isCurrentRemoteOwnerEntry(entry, now)) {
			if (!entry) {
				this.expireCurrentRemoteOwnerGrantIfNeeded(bounded, now);
			}
			return undefined;
		}
		const freshUntilMs = this.currentRemoteOwnerFreshUntil(
			entry.deadlineMonotonicMs,
			now,
		);
		return freshUntilMs === undefined
			? undefined
			: Object.freeze({
					revision: this._currentRemoteOwnerRevision,
					freshUntilMs,
					token: entry.token,
				});
	}

	/** Validate exact token identity against the complete current V2 state. */
	isCurrentRemoteOwnerProvenance(
		token: ReplicationInfoV2CurrentRemoteOwnerToken,
	): boolean {
		if (
			!token ||
			typeof token !== "object" ||
			!this._currentRemoteOwnerTokens.has(token) ||
			!this.hasCurrentOwnershipAuthority()
		) {
			return false;
		}
		const now = this.currentMonotonicNow();
		if (now === undefined) {
			return false;
		}
		const entry = this._currentRemoteOwnerByPeer.get(token.peerHash);
		if (!entry) {
			this.expireCurrentRemoteOwnerGrantIfNeeded(token.peerHash, now);
		}
		return (
			entry !== undefined &&
			entry.token === token &&
			this.isCurrentRemoteOwnerEntry(entry, now)
		);
	}

	/**
	 * Capture a dense unique peer set at one revision and monotonic instant.
	 * Missing or expired peers fail the whole capture; no prefix is returned.
	 */
	captureCurrentRemoteOwnerProvenance(
		peerHashes: readonly string[],
	): ReplicationInfoV2CurrentRemoteOwnerCapture | undefined {
		if (!Array.isArray(peerHashes)) {
			throw new Error("Invalid replication-info V2 current-owner batch");
		}
		const length = peerHashes.length;
		if (
			!Number.isSafeInteger(length) ||
			length < 0 ||
			length > MAX_REPLICATION_INFO_V2_CURRENT_OWNER_BATCH
		) {
			throw new Error("Invalid replication-info V2 current-owner batch");
		}
		const bounded: string[] = [];
		const unique = new Set<string>();
		for (let index = 0; index < length; index++) {
			if (!Object.prototype.hasOwnProperty.call(peerHashes, index)) {
				throw new Error("Invalid replication-info V2 current-owner batch");
			}
			const peerHash = this.boundedCurrentRemoteOwnerPeerHash(
				peerHashes[index],
			);
			if (!peerHash || unique.has(peerHash)) {
				throw new Error("Invalid replication-info V2 current-owner batch");
			}
			unique.add(peerHash);
			bounded.push(peerHash);
		}

		const now = this.currentMonotonicNow();
		if (now === undefined || !this.hasCurrentOwnershipAuthority()) {
			return undefined;
		}
		const entries: ReplicationInfoV2CurrentRemoteOwnerEntry[] = [];
		let deadline = Number.POSITIVE_INFINITY;
		for (let index = 0; index < bounded.length; index++) {
			const entry = this._currentRemoteOwnerByPeer.get(bounded[index]!);
			if (!entry || !this.isCurrentRemoteOwnerEntry(entry, now)) {
				if (!entry) {
					this.expireCurrentRemoteOwnerGrantIfNeeded(bounded[index]!, now);
				}
				return undefined;
			}
			entries.push(entry);
			deadline = Math.min(deadline, entry.deadlineMonotonicMs);
		}
		const freshUntilMs =
			entries.length === 0
				? undefined
				: this.currentRemoteOwnerFreshUntil(deadline, now);
		if (entries.length > 0 && freshUntilMs === undefined) {
			return undefined;
		}
		const tokens = Object.freeze(entries.map((entry) => entry.token));
		const capture = Object.freeze({
			revision: this._currentRemoteOwnerRevision,
			freshUntilMs,
			tokens,
		});
		this._currentRemoteOwnerCaptures.set(capture, {
			openGeneration: this._currentRemoteOwnerOpenGeneration,
			revision: this._currentRemoteOwnerRevision,
			deadlineMonotonicMs: entries.length === 0 ? undefined : deadline,
		});
		return capture;
	}

	/**
	 * Revalidate a complete batch, including an empty self-only capture, against
	 * the exact open generation and every intervening registry mutation.
	 */
	isCurrentRemoteOwnerProvenanceCapture(
		capture: ReplicationInfoV2CurrentRemoteOwnerCapture,
	): boolean {
		if (
			!capture ||
			typeof capture !== "object" ||
			!this.hasCurrentOwnershipAuthority()
		) {
			return false;
		}
		const retained = this._currentRemoteOwnerCaptures.get(capture);
		if (
			!retained ||
			retained.openGeneration !== this._currentRemoteOwnerOpenGeneration ||
			retained.revision !== this._currentRemoteOwnerRevision
		) {
			return false;
		}
		const now = this.currentMonotonicNow();
		if (now === undefined) {
			return false;
		}
		for (let index = 0; index < capture.tokens.length; index++) {
			const token = capture.tokens[index]!;
			if (!this._currentRemoteOwnerTokens.has(token)) {
				return false;
			}
			const entry = this._currentRemoteOwnerByPeer.get(token.peerHash);
			if (
				!entry ||
				entry.token !== token ||
				!this.isCurrentRemoteOwnerEntry(entry, now)
			) {
				return false;
			}
		}
		return (
			retained.openGeneration === this._currentRemoteOwnerOpenGeneration &&
			retained.revision === this._currentRemoteOwnerRevision
		);
	}

	/** Invalidate at the exact-gated ownership-lane mutation start. */
	beginRemoteOwnerProvenanceMutation(
		admission: ReplicationInfoV2ReceiveAdmission,
	): boolean {
		const existing = this._currentRemoteOwnerMutationFences.get(admission);
		if (existing) {
			return (
				!admission.committed &&
				existing.openGeneration === this._currentRemoteOwnerOpenGeneration &&
				existing.state === admission.state &&
				existing.grantGeneration ===
					admission.state.remoteOwnerGrant?.generation &&
				this.hasCurrentOwnershipAuthority() &&
				this.isAdmissionCurrent(admission)
			);
		}
		const grant = admission.state.remoteOwnerGrant;
		const now = this.currentMonotonicNow();
		if (
			admission.committed ||
			!this.hasCurrentOwnershipAuthority() ||
			!this.isAdmissionCurrent(admission) ||
			!grant ||
			!this.isRemoteOwnerGrantCurrent(admission.state, grant) ||
			now === undefined ||
			now >= grant.deadlineMonotonicMs
		) {
			if (
				grant &&
				now !== undefined &&
				now >= grant.deadlineMonotonicMs &&
				this.isAdmissionCurrent(admission)
			) {
				this.transitionToResync(admission.state, {
					force: true,
					refreshCapability: true,
				});
			}
			return false;
		}
		const invalidated = this.invalidateCurrentRemoteOwner(
			admission.state.peerHash,
			admission.state,
		);
		if (!invalidated) {
			// Even a first Full with no previous token invalidates a previously
			// captured empty/self-only view while its range mutation is in flight.
			this._currentRemoteOwnerRevision++;
		}
		this._currentRemoteOwnerMutationFences.set(admission, {
			openGeneration: this._currentRemoteOwnerOpenGeneration,
			state: admission.state,
			grantGeneration: grant.generation,
		});
		return true;
	}

	/**
	 * Publish only after protocol commit and all fallible ownership bookkeeping.
	 * Full enables the bound grant; deltas can only replace an enabled token.
	 */
	publishRemoteOwnerProvenance(
		admission: ReplicationInfoV2ReceiveAdmission,
	): boolean {
		const mutationFence = this._currentRemoteOwnerMutationFences.get(admission);
		if (
			!admission.committed ||
			!this.hasCurrentOwnershipAuthority() ||
			!this.isAdmissionCurrent(admission) ||
			!mutationFence ||
			mutationFence.openGeneration !== this._currentRemoteOwnerOpenGeneration ||
			mutationFence.state !== admission.state
		) {
			return false;
		}
		const { state, message } = admission;
		const grant = state.remoteOwnerGrant;
		const now = this.currentMonotonicNow();
		if (
			now === undefined ||
			state.phase !== "active" ||
			state.lastSequence !== message.sequence ||
			state.senderEpoch === undefined ||
			!bytesEqual(state.senderEpoch, message.senderEpoch) ||
			state.receiverBinding === undefined ||
			grant === undefined ||
			mutationFence.grantGeneration !== grant.generation ||
			!this.isRemoteOwnerGrantCurrent(state, grant) ||
			grant.receiveEpoch !== state.receiveEpoch ||
			grant.senderTransportSession !== state.senderTransportSession ||
			grant.receiverTransportSession !== state.receiverTransportSession ||
			!bytesEqual(grant.receiverBinding, state.receiverBinding)
		) {
			return false;
		}
		if (now >= grant.deadlineMonotonicMs) {
			const existing = this._currentRemoteOwnerByPeer.get(state.peerHash);
			if (existing) {
				this.expireCurrentRemoteOwner(existing);
			} else {
				this.transitionToResync(state, {
					force: true,
					refreshCapability: true,
				});
			}
			return false;
		}
		if (admission.kind !== "full" && !grant.fullFinalized) {
			return false;
		}
		const key = this.currentRemoteOwnerKeyFacts(state);
		if (!key) {
			return false;
		}
		const existing = this._currentRemoteOwnerByPeer.get(state.peerHash);
		if (
			existing?.state === state &&
			existing.grantGeneration === grant.generation &&
			existing.sequence === message.sequence &&
			this.isCurrentRemoteOwnerEntry(existing, now)
		) {
			return true;
		}

		const token = Object.freeze({
			peerHash: key.peerHash,
			publicKeyHex: key.publicKeyHex,
			senderTransportSession: state.senderTransportSession,
			receiverTransportSession: state.receiverTransportSession!,
			receiverBindingHex: toHexString(state.receiverBinding),
			senderEpochHex: toHexString(state.senderEpoch),
			sequence: message.sequence,
		});
		const entry: ReplicationInfoV2CurrentRemoteOwnerEntry = {
			openGeneration: this._currentRemoteOwnerOpenGeneration,
			state,
			peerSession: state.peerSession,
			receiveEpoch: state.receiveEpoch,
			grantGeneration: grant.generation,
			senderTransportSession: state.senderTransportSession,
			receiverTransportSession: state.receiverTransportSession!,
			receiverRequestChallenge: state.receiverRequestChallenge.slice(),
			receiverBinding: state.receiverBinding.slice(),
			senderEpoch: state.senderEpoch.slice(),
			sequence: message.sequence,
			deadlineMonotonicMs: grant.deadlineMonotonicMs,
			token,
		};
		if (admission.kind === "full") {
			grant.fullFinalized = true;
		}
		this._currentRemoteOwnerByPeer.set(state.peerHash, entry);
		this._currentRemoteOwnerTokens.add(token);
		this._currentRemoteOwnerRevision++;
		this._currentRemoteOwnerMutationFences.delete(admission);
		return true;
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
		const receiverBinding = deriveReplicationInfoV2ReceiverBinding({
			receiverChallenge: state.receiverRequestChallenge,
			receiver: this.deps.getSelfKey(),
			receiverTransportSession: ready.receiverTransportSession,
			sender: state.target,
			senderTransportSession: state.senderTransportSession,
		});
		state.receiverBinding = receiverBinding;
		const current = state.remoteOwnerGrant;
		if (
			current &&
			current.receiveEpoch === state.receiveEpoch &&
			current.senderTransportSession === state.senderTransportSession &&
			current.receiverTransportSession === ready.receiverTransportSession &&
			bytesEqual(
				current.receiverRequestChallenge,
				state.receiverRequestChallenge,
			) &&
			bytesEqual(current.receiverBinding, receiverBinding)
		) {
			// Replayed readiness for the same exact binding must not manufacture a
			// later freshness deadline.
			return;
		}
		this.invalidateCurrentRemoteOwner(state.peerHash, state);
		const boundAt = this.currentMonotonicNow();
		const deadline =
			boundAt === undefined
				? undefined
				: boundAt + this.currentRemoteOwnerTtlMs;
		state.remoteOwnerGrant =
			boundAt === undefined ||
			deadline === undefined ||
			!Number.isFinite(deadline) ||
			deadline <= boundAt
				? undefined
				: {
						generation: {},
						openGeneration: this._currentRemoteOwnerOpenGeneration,
						receiveEpoch: state.receiveEpoch,
						senderTransportSession: state.senderTransportSession,
						receiverTransportSession: ready.receiverTransportSession,
						receiverRequestChallenge: state.receiverRequestChallenge.slice(),
						receiverBinding: receiverBinding.slice(),
						deadlineMonotonicMs: deadline,
						fullFinalized: false,
					};
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
	 * Resume only a request generation that exhausted its bounded retries.
	 * Wait/liveness callers may nudge recovery without invalidating an active,
	 * timed or in-flight request and without advancing the receive epoch.
	 */
	resumeParkedRequest(properties: {
		peerHash: string;
		peerSession: object;
		receiveEpoch: object | null;
	}): boolean {
		const state = this._receiveStates.get(properties.peerHash);
		if (
			!state ||
			state.peerSession !== properties.peerSession ||
			state.receiveEpoch !== properties.receiveEpoch ||
			!this.deps.isPeerStateCurrent(
				properties.peerHash,
				properties.peerSession,
				properties.receiveEpoch,
			) ||
			state.phase === "active" ||
			!state.requestParked ||
			state.requestTimer !== undefined ||
			state.requestInFlight !== undefined ||
			this._reservedAdmissionsByPeer.has(properties.peerHash) ||
			state.receiverBinding === undefined ||
			state.lastSequence === MAX_U64
		) {
			return false;
		}
		state.requestAttempts = 0;
		state.requestsSinceCapabilityRefresh = 0;
		if (!state.capabilityRefreshRequired) {
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
		this.invalidateCurrentRemoteOwner(state.peerHash, state);
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
			state.remoteOwnerGrant = undefined;
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
		if (!this.hasCurrentOwnershipAuthority()) {
			return undefined;
		}
		const grantNow = this.currentMonotonicNow();
		const grant = state.remoteOwnerGrant;
		if (
			grantNow === undefined ||
			grant === undefined ||
			!this.isRemoteOwnerGrantCurrent(state, grant) ||
			grantNow >= grant.deadlineMonotonicMs
		) {
			this.transitionToResync(state, {
				force: true,
				refreshCapability: true,
			});
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
		const mutationFence = this._currentRemoteOwnerMutationFences.get(admission);
		if (
			!mutationFence ||
			mutationFence.openGeneration !== this._currentRemoteOwnerOpenGeneration ||
			mutationFence.state !== state ||
			mutationFence.grantGeneration !== state.remoteOwnerGrant?.generation
		) {
			const invalidated = this.invalidateCurrentRemoteOwner(
				state.peerHash,
				state,
			);
			if (!invalidated) {
				// Direct protocol users do not participate in the host ownership lane,
				// but their successful state advance must still stale empty captures.
				this._currentRemoteOwnerRevision++;
			}
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

	requireFullAfterFailure(
		admission: ReplicationInfoV2ReceiveAdmission,
	): boolean {
		this._currentRemoteOwnerMutationFences.delete(admission);
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
		this.invalidateCurrentRemoteOwner(state.peerHash, state);
		state.remoteOwnerGrant = undefined;

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
