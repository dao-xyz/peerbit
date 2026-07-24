import type { Entry, ShallowEntry } from "@peerbit/log";
import type { DeferredPromise } from "p-defer";
import type { EntryReplicated } from "./ranges.js";

export type CheckedPruneLeaderMap = Map<string, { intersecting: boolean }>;

export type CheckedPruneEntry<T, R extends "u32" | "u64"> =
	| Entry<T>
	| ShallowEntry
	| EntryReplicated<R>;

export type CheckedPrunePendingDelete = {
	requestId: Uint8Array;
	retryOnInvalidation?: boolean;
	promise: DeferredPromise<void>;
	clear: () => void;
	resolve: (
		publicKeyHash: string,
		requestId: Uint8Array,
	) => Promise<void> | void;
	reject(
		reason: any,
		options?: { preserveRetry?: boolean },
	): Promise<void> | void;
};

export type CheckedPruneRetryState<T, R extends "u32" | "u64"> = {
	attempts: number;
	timer?: ReturnType<typeof setTimeout>;
	entry: CheckedPruneEntry<T, R>;
	leaders: CheckedPruneLeaderMap | Set<string>;
};

export type CheckedPruneInvalidatedGeneration<T, R extends "u32" | "u64"> = {
	hash: string;
	pending: CheckedPrunePendingDelete;
	entry: CheckedPruneEntry<T, R>;
	leaders: CheckedPruneLeaderMap | Set<string>;
};

export type CheckedPruneRestartCandidate<T, R extends "u32" | "u64"> = Omit<
	CheckedPruneInvalidatedGeneration<T, R>,
	"pending"
>;

type CheckedPruneSessionPhase =
	| "candidate"
	| "requested"
	| "confirmed"
	| "removing"
	| "retrying"
	| "cancelled"
	| "done";

type CheckedPruneSession<T, R extends "u32" | "u64"> = {
	phase: CheckedPruneSessionPhase;
	entry?: CheckedPruneEntry<T, R>;
	leaders?: CheckedPruneLeaderMap | Set<string>;
	pending?: CheckedPrunePendingDelete;
	retry?: CheckedPruneRetryState<T, R>;
	contacted: Set<string>;
	confirmed: Set<string>;
	revoked: Set<string>;
};

export class CheckedPruneCoordinator<T, R extends "u32" | "u64"> {
	readonly pendingDeletes = new Map<string, CheckedPrunePendingDelete>();
	readonly requestIPruneSent = new Map<string, Set<string>>();
	readonly responseReplicatorSet = new Map<string, Set<string>>();
	readonly retries = new Map<string, CheckedPruneRetryState<T, R>>();
	private readonly sessions = new Map<string, CheckedPruneSession<T, R>>();
	private readonly grantSends = new Map<string, Set<Promise<void>>>();
	private readonly peerRemovalFences = new Map<string, number>();
	private readonly restartReservations = new Map<string, object>();
	private readonly candidateTokens = new Map<string, object>();

	private getOrCreateSession(hash: string): CheckedPruneSession<T, R> {
		let session = this.sessions.get(hash);
		if (!session) {
			session = {
				phase: "candidate",
				contacted: new Set(),
				confirmed: new Set(),
				revoked: new Set(),
			};
			this.sessions.set(hash, session);
		}
		return session;
	}

	private remember(
		hash: string,
		entry?: CheckedPruneEntry<T, R>,
		leaders?: CheckedPruneLeaderMap | Set<string>,
	) {
		const session = this.getOrCreateSession(hash);
		if (entry) {
			session.entry = entry;
		}
		if (leaders) {
			session.leaders = leaders;
		}
		return session;
	}

	private deleteSessionIfIdle(hash: string) {
		if (
			this.pendingDeletes.has(hash) ||
			this.requestIPruneSent.has(hash) ||
			this.responseReplicatorSet.has(hash) ||
			this.retries.has(hash) ||
			this.candidateTokens.has(hash)
		) {
			return;
		}
		this.sessions.delete(hash);
	}

	trackCandidate(
		hash: string,
		entry: CheckedPruneEntry<T, R>,
		leaders: CheckedPruneLeaderMap | Set<string>,
	) {
		this.restartReservations.delete(hash);
		const session = this.remember(hash, entry, leaders);
		if (!this.hasActiveWork(hash)) {
			session.phase = "candidate";
		}
		const token = {};
		this.candidateTokens.set(hash, token);
		return token;
	}

	isCandidateTokenCurrent(hash: string, token: object) {
		return this.candidateTokens.get(hash) === token;
	}

	hasCandidate(hash: string) {
		return this.candidateTokens.has(hash);
	}

	invalidateCandidateToken(hash: string) {
		this.candidateTokens.delete(hash);
		this.deleteSessionIfIdle(hash);
	}

	hasActiveWork(hash: string) {
		return (
			this.pendingDeletes.has(hash) ||
			this.requestIPruneSent.has(hash) ||
			this.responseReplicatorSet.has(hash) ||
			this.retries.has(hash) ||
			this.candidateTokens.has(hash)
		);
	}

	getPendingDelete(hash: string) {
		return this.pendingDeletes.get(hash);
	}

	hasPendingDelete(hash: string) {
		return this.pendingDeletes.has(hash);
	}

	getRestartCandidate(
		hash: string,
	): CheckedPruneRestartCandidate<T, R> | undefined {
		const session = this.sessions.get(hash);
		if (!session?.entry || !session.leaders) {
			return undefined;
		}
		return {
			hash,
			entry: session.entry,
			leaders:
				session.leaders instanceof Map
					? new Map(session.leaders)
					: new Set(session.leaders),
		};
	}

	setPendingDelete(
		hash: string,
		pending: CheckedPrunePendingDelete,
		entry: CheckedPruneEntry<T, R>,
		leaders: CheckedPruneLeaderMap | Set<string>,
	) {
		this.restartReservations.delete(hash);
		this.candidateTokens.delete(hash);
		this.requestIPruneSent.delete(hash);
		this.responseReplicatorSet.delete(hash);
		this.pendingDeletes.set(hash, pending);
		const session = this.remember(hash, entry, leaders);
		session.pending = pending;
		session.contacted.clear();
		session.confirmed.clear();
		session.revoked.clear();
		session.phase = "requested";
	}

	deletePendingDelete(hash: string, pending?: CheckedPrunePendingDelete) {
		const current = this.pendingDeletes.get(hash);
		if (!pending || current === pending) {
			this.pendingDeletes.delete(hash);
			this.requestIPruneSent.delete(hash);
			this.responseReplicatorSet.delete(hash);
			const session = this.sessions.get(hash);
			if (session && session.pending === current) {
				session.pending = undefined;
				session.contacted.clear();
				session.confirmed.clear();
				session.revoked.clear();
			}
			this.deleteSessionIfIdle(hash);
		}
	}

	getRetry(hash: string) {
		return this.retries.get(hash);
	}

	hasRetry(hash: string) {
		return this.retries.has(hash);
	}

	setRetry(hash: string, state: CheckedPruneRetryState<T, R>) {
		this.restartReservations.delete(hash);
		this.candidateTokens.delete(hash);
		this.retries.set(hash, state);
		const session = this.remember(hash, state.entry, state.leaders);
		session.retry = state;
		session.phase = "retrying";
	}

	clearRetry(hash: string) {
		this.restartReservations.delete(hash);
		this.candidateTokens.delete(hash);
		const state = this.retries.get(hash);
		if (state?.timer) {
			clearTimeout(state.timer);
		}
		this.retries.delete(hash);
		const session = this.sessions.get(hash);
		if (session && session.retry === state) {
			session.retry = undefined;
		}
		this.deleteSessionIfIdle(hash);
	}

	clearRetryTimer(hash: string) {
		const state = this.retries.get(hash);
		if (state?.timer) {
			clearTimeout(state.timer);
			state.timer = undefined;
		}
		return state;
	}

	isCurrentRequest(hash: string, pending: CheckedPrunePendingDelete) {
		return this.pendingDeletes.get(hash) === pending;
	}

	isCurrentRequestId(
		hash: string,
		pending: CheckedPrunePendingDelete,
		requestId: Uint8Array,
	) {
		if (!this.isCurrentRequest(hash, pending)) {
			return false;
		}
		if (pending.requestId.byteLength !== requestId.byteLength) {
			return false;
		}
		return pending.requestId.every(
			(value, index) => value === requestId[index],
		);
	}

	addRequestSent(
		hash: string,
		peer: string,
		pending: CheckedPrunePendingDelete,
	) {
		if (!this.isCurrentRequest(hash, pending)) {
			return false;
		}
		const session = this.getOrCreateSession(hash);
		if (session.revoked.has(peer)) {
			return false;
		}
		let set = this.requestIPruneSent.get(hash);
		if (!set) {
			set = new Set();
			this.requestIPruneSent.set(hash, set);
		}
		set.add(peer);
		session.contacted.add(peer);
		if (session.phase === "candidate") {
			session.phase = "requested";
		}
		return true;
	}

	removeRequestSent(hash: string, peer?: string) {
		if (!peer) {
			const session = this.sessions.get(hash);
			if (session?.pending) {
				for (const contactedPeer of session.contacted) {
					session.revoked.add(contactedPeer);
				}
			}
			this.requestIPruneSent.delete(hash);
			session?.contacted.clear();
			this.clearConfirmedReplicators(hash);
			this.deleteSessionIfIdle(hash);
			return;
		}
		const session = this.sessions.get(hash);
		if (session?.pending) {
			// A contact removed during this pending generation must never become
			// authorized again by a delayed receipt carrying the old generation ID.
			session.revoked.add(peer);
		}
		const set = this.requestIPruneSent.get(hash);
		if (!set) {
			this.removeConfirmedReplicator(hash, peer);
			return;
		}
		set.delete(peer);
		this.removeConfirmedReplicator(hash, peer);
		session?.contacted.delete(peer);
		if (set.size === 0) {
			this.requestIPruneSent.delete(hash);
			this.deleteSessionIfIdle(hash);
		}
	}

	removeRequestsSent(hashes: Iterable<string>, peer?: string) {
		for (const hash of hashes) {
			this.removeRequestSent(hash, peer);
		}
	}

	addConfirmedReplicator(
		hash: string,
		peer: string,
		pending: CheckedPrunePendingDelete,
		requestId: Uint8Array,
	) {
		if (
			!this.isCurrentRequestId(hash, pending, requestId) ||
			!this.requestIPruneSent.get(hash)?.has(peer)
		) {
			return undefined;
		}
		let set = this.responseReplicatorSet.get(hash);
		if (!set) {
			set = new Set();
			this.responseReplicatorSet.set(hash, set);
		}
		set.add(peer);
		const session = this.getOrCreateSession(hash);
		session.confirmed.add(peer);
		session.phase = "confirmed";
		return set;
	}

	removeConfirmedReplicator(hash: string, peer: string) {
		const set = this.responseReplicatorSet.get(hash);
		if (!set) {
			return;
		}
		set.delete(peer);
		const session = this.sessions.get(hash);
		session?.confirmed.delete(peer);
		if (set.size === 0) {
			this.responseReplicatorSet.delete(hash);
			this.deleteSessionIfIdle(hash);
		}
	}

	removeConfirmedReplicators(hashes: Iterable<string>, peer: string) {
		if (this.responseReplicatorSet.size === 0) {
			return;
		}
		for (const hash of hashes) {
			this.removeConfirmedReplicator(hash, peer);
		}
	}

	clearConfirmedReplicators(hash: string) {
		this.responseReplicatorSet.delete(hash);
		const session = this.sessions.get(hash);
		session?.confirmed.clear();
		this.deleteSessionIfIdle(hash);
	}

	clearConfirmedReplicatorsBatch(hashes: Iterable<string>) {
		if (this.responseReplicatorSet.size === 0) {
			return;
		}
		for (const hash of hashes) {
			this.clearConfirmedReplicators(hash);
		}
	}

	getConfirmedReplicators(hash: string) {
		return this.responseReplicatorSet.get(hash);
	}

	getContactedReplicators(hash: string) {
		return this.requestIPruneSent.get(hash);
	}

	hasRevokedLeader(
		hash: string,
		pending: CheckedPrunePendingDelete,
		leaders: CheckedPruneLeaderMap | Set<string>,
	) {
		if (!this.isCurrentRequest(hash, pending)) {
			return false;
		}
		const revoked = this.sessions.get(hash)?.revoked;
		if (!revoked || revoked.size === 0) {
			return false;
		}
		for (const leader of leaders.keys()) {
			if (revoked.has(leader)) {
				return true;
			}
		}
		return false;
	}

	trackGrantSend(hashes: Iterable<string>, send: Promise<void>) {
		const uniqueHashes = [...new Set(hashes)];
		const observed = send.then(
			() => undefined,
			() => undefined,
		);
		for (const hash of uniqueHashes) {
			let sends = this.grantSends.get(hash);
			if (!sends) {
				sends = new Set();
				this.grantSends.set(hash, sends);
			}
			sends.add(observed);
		}
		void observed.finally(() => {
			for (const hash of uniqueHashes) {
				const sends = this.grantSends.get(hash);
				sends?.delete(observed);
				if (sends?.size === 0) {
					this.grantSends.delete(hash);
				}
			}
		});
		return observed;
	}

	waitForGrantSends(hash: string): Promise<void> | undefined {
		const sends = this.grantSends.get(hash);
		if (!sends || sends.size === 0) {
			return undefined;
		}
		return Promise.all(sends).then(() => undefined);
	}

	reserveRestart(hash: string) {
		const reservation = {};
		this.restartReservations.set(hash, reservation);
		return reservation;
	}

	consumeRestartReservation(hash: string, reservation: object) {
		if (this.restartReservations.get(hash) !== reservation) {
			return false;
		}
		this.restartReservations.delete(hash);
		return true;
	}

	cancelRestartReservation(hash: string, reservation: object) {
		if (this.restartReservations.get(hash) === reservation) {
			this.restartReservations.delete(hash);
		}
	}

	hasRestartReservation(hash: string) {
		return this.restartReservations.has(hash);
	}

	getExactConfirmedReplicators(
		hash: string,
		pending: CheckedPrunePendingDelete,
	) {
		const exact = new Set<string>();
		if (!this.isCurrentRequest(hash, pending)) {
			return exact;
		}
		const contacted = this.requestIPruneSent.get(hash);
		const confirmed = this.responseReplicatorSet.get(hash);
		const revoked = this.sessions.get(hash)?.revoked;
		if (!contacted || !confirmed) {
			return exact;
		}
		for (const peer of confirmed) {
			if (
				contacted.has(peer) &&
				!revoked?.has(peer) &&
				!this.isPeerRemovalFenced(peer)
			) {
				exact.add(peer);
			}
		}
		return exact;
	}

	fencePeerRemoval(peer: string) {
		this.peerRemovalFences.set(
			peer,
			(this.peerRemovalFences.get(peer) ?? 0) + 1,
		);
		let released = false;
		return () => {
			if (released) {
				return;
			}
			released = true;
			const remaining = (this.peerRemovalFences.get(peer) ?? 1) - 1;
			if (remaining > 0) {
				this.peerRemovalFences.set(peer, remaining);
			} else {
				this.peerRemovalFences.delete(peer);
			}
		};
	}

	isPeerRemovalFenced(peer: string) {
		return (this.peerRemovalFences.get(peer) ?? 0) > 0;
	}

	markRemoving(hash: string, pending?: CheckedPrunePendingDelete) {
		if (pending && !this.isCurrentRequest(hash, pending)) {
			return false;
		}
		const session = this.getOrCreateSession(hash);
		session.phase = "removing";
		return true;
	}

	markDone(hash: string, pending?: CheckedPrunePendingDelete) {
		if (pending && !this.isCurrentRequest(hash, pending)) {
			return false;
		}
		const session = this.sessions.get(hash);
		if (session) {
			session.phase = "done";
		}
		this.restartReservations.delete(hash);
		this.candidateTokens.delete(hash);
		this.pendingDeletes.delete(hash);
		this.requestIPruneSent.delete(hash);
		this.responseReplicatorSet.delete(hash);
		this.clearRetry(hash);
		this.sessions.delete(hash);
		return true;
	}

	markCancelled(
		hash: string,
		pendingOrOptions?: CheckedPrunePendingDelete | { preserveRetry?: boolean },
		options?: { preserveRetry?: boolean },
	) {
		const pending =
			pendingOrOptions && "promise" in pendingOrOptions
				? pendingOrOptions
				: undefined;
		const resolvedOptions: { preserveRetry?: boolean } | undefined = pending
			? options
			: (pendingOrOptions as { preserveRetry?: boolean } | undefined);
		if (pending && !this.isCurrentRequest(hash, pending)) {
			return false;
		}
		this.restartReservations.delete(hash);
		this.candidateTokens.delete(hash);
		const retry = this.retries.get(hash);
		const session = this.sessions.get(hash);
		if (session) {
			session.phase = "cancelled";
			session.pending = undefined;
			session.contacted.clear();
			session.confirmed.clear();
			session.revoked.clear();
			if (!resolvedOptions?.preserveRetry || session.retry !== retry) {
				session.retry = undefined;
			}
		}
		if (!pending || this.pendingDeletes.get(hash) === pending) {
			this.pendingDeletes.delete(hash);
		}
		this.requestIPruneSent.delete(hash);
		this.responseReplicatorSet.delete(hash);
		if (!resolvedOptions?.preserveRetry) {
			this.clearRetry(hash);
		}
		this.deleteSessionIfIdle(hash);
		return true;
	}

	cleanupPeer(peer: string) {
		const invalidated: CheckedPruneInvalidatedGeneration<T, R>[] = [];
		// Fence the peer from every active generation, including generations that
		// have not emitted their first request yet. A removal can begin between
		// generation creation and message emission.
		for (const [hash, session] of this.sessions) {
			const pending = session.pending;
			if (pending && this.pendingDeletes.get(hash) === pending) {
				session.revoked.add(peer);
				const candidate = this.getRestartCandidate(hash);
				if (
					candidate &&
					(session.contacted.has(peer) || candidate.leaders.has(peer))
				) {
					invalidated.push({
						...candidate,
						pending,
					});
				}
			}
		}

		for (const [hash, peers] of [...this.requestIPruneSent]) {
			if (peers.has(peer)) {
				this.removeRequestSent(hash, peer);
			}
		}

		for (const [hash, peers] of [...this.responseReplicatorSet]) {
			if (peers.has(peer)) {
				this.removeConfirmedReplicator(hash, peer);
			}
		}
		return invalidated;
	}

	close() {
		for (const [_hash, pending] of this.pendingDeletes) {
			pending.clear();
			pending.promise.resolve();
		}
		for (const [_hash, retry] of this.retries) {
			if (retry.timer) {
				clearTimeout(retry.timer);
			}
		}
		this.pendingDeletes.clear();
		this.requestIPruneSent.clear();
		this.responseReplicatorSet.clear();
		this.retries.clear();
		this.sessions.clear();
		this.grantSends.clear();
		this.peerRemovalFences.clear();
		this.restartReservations.clear();
		this.candidateTokens.clear();
	}
}
