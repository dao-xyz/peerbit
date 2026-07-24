import type { Entry, ShallowEntry } from "@peerbit/log";
import type { DeferredPromise } from "p-defer";
import type { EntryReplicated } from "./ranges.js";

export type CheckedPruneLeaderMap = Map<string, { intersecting: boolean }>;

export type CheckedPruneEntry<T, R extends "u32" | "u64"> =
	| Entry<T>
	| ShallowEntry
	| EntryReplicated<R>;

export type CheckedPrunePendingDelete = {
	promise: DeferredPromise<void>;
	background: boolean;
	promoteToBackground: () => void;
	clear: () => void;
	resolve: (publicKeyHash: string, requestId: string) => Promise<void> | void;
	reject(reason: any): Promise<void> | void;
};

export type CheckedPruneRetryState<T, R extends "u32" | "u64"> = {
	attempts: number;
	timer?: ReturnType<typeof setTimeout>;
	entry: CheckedPruneEntry<T, R>;
	leaders: CheckedPruneLeaderMap | Set<string>;
};

type CheckedPruneSessionPhase =
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
};

export class CheckedPruneCoordinator<T, R extends "u32" | "u64"> {
	readonly pendingDeletes = new Map<string, CheckedPrunePendingDelete>();
	readonly requestIPruneSent = new Map<string, Set<string>>();
	readonly requestIds = new Map<string, Map<string, string>>();
	readonly responseReplicatorSet = new Map<string, Set<string>>();
	readonly confirmationIds = new Map<string, Map<string, string>>();
	readonly retries = new Map<string, CheckedPruneRetryState<T, R>>();
	private readonly autoResendClaims = new Map<
		string,
		Map<string, { pending: CheckedPrunePendingDelete; requestId: string }>
	>();
	private readonly grantSends = new Map<string, Set<Promise<void>>>();
	private readonly sessions = new Map<string, CheckedPruneSession<T, R>>();

	private getOrCreateSession(hash: string): CheckedPruneSession<T, R> {
		let session = this.sessions.get(hash);
		if (!session) {
			session = {
				phase: "requested",
				contacted: new Set(),
				confirmed: new Set(),
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
			this.retries.has(hash)
		) {
			return;
		}
		this.sessions.delete(hash);
	}

	hasActiveWork(hash: string) {
		return (
			this.pendingDeletes.has(hash) ||
			this.requestIPruneSent.has(hash) ||
			this.responseReplicatorSet.has(hash) ||
			this.retries.has(hash)
		);
	}

	getPendingDelete(hash: string) {
		return this.pendingDeletes.get(hash);
	}

	hasPendingDelete(hash: string) {
		return this.pendingDeletes.has(hash);
	}

	setPendingDelete(
		hash: string,
		pending: CheckedPrunePendingDelete,
		entry: CheckedPruneEntry<T, R>,
		leaders: CheckedPruneLeaderMap | Set<string>,
	) {
		// A new request id starts a new authorization generation. No peer or
		// confirmation from an older generation may survive into it.
		this.requestIPruneSent.delete(hash);
		this.requestIds.delete(hash);
		this.responseReplicatorSet.delete(hash);
		this.confirmationIds.delete(hash);
		this.autoResendClaims.delete(hash);
		this.pendingDeletes.set(hash, pending);
		const session = this.remember(hash, entry, leaders);
		session.pending = pending;
		session.contacted.clear();
		session.confirmed.clear();
		session.phase = "requested";
	}

	deletePendingDelete(hash: string, pending?: CheckedPrunePendingDelete) {
		const current = this.pendingDeletes.get(hash);
		if (!pending || current === pending) {
			this.pendingDeletes.delete(hash);
			const session = this.sessions.get(hash);
			if (session && session.pending === current) {
				session.pending = undefined;
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

	isCurrentRetry(hash: string, state: CheckedPruneRetryState<T, R>) {
		return (
			this.retries.get(hash) === state &&
			this.sessions.get(hash)?.retry === state
		);
	}

	setRetry(hash: string, state: CheckedPruneRetryState<T, R>) {
		this.retries.set(hash, state);
		const session = this.remember(hash, state.entry, state.leaders);
		session.retry = state;
		session.phase = "retrying";
	}

	clearRetry(hash: string, expected?: CheckedPruneRetryState<T, R>) {
		const state = this.retries.get(hash);
		if (expected && state !== expected) {
			return false;
		}
		if (state?.timer) {
			clearTimeout(state.timer);
		}
		this.retries.delete(hash);
		const session = this.sessions.get(hash);
		if (session && session.retry === state) {
			session.retry = undefined;
		}
		this.deleteSessionIfIdle(hash);
		return true;
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
		const session = this.sessions.get(hash);
		return (
			this.pendingDeletes.get(hash) === pending && session?.pending === pending
		);
	}

	promoteToBackground(hash: string, pending: CheckedPrunePendingDelete) {
		if (!this.isCurrentRequest(hash, pending)) {
			return false;
		}
		pending.promoteToBackground();
		return true;
	}

	isCurrentRequestForPeer(
		hash: string,
		pending: CheckedPrunePendingDelete,
		peer: string,
		requestId: string,
	) {
		return (
			this.isCurrentRequest(hash, pending) &&
			this.requestIPruneSent.get(hash)?.has(peer) === true &&
			this.requestIds.get(hash)?.get(peer) === requestId
		);
	}

	addRequestSent(hash: string, peer: string, requestId: string) {
		const pending = this.pendingDeletes.get(hash);
		if (!pending || !this.isCurrentRequest(hash, pending)) {
			return false;
		}
		const previousRequestId = this.requestIds.get(hash)?.get(peer);
		if (previousRequestId != null && previousRequestId !== requestId) {
			// Changing the contact generation intrinsically revokes any grant
			// attached to the previous id. Callers must not be able to preserve an
			// authorization accidentally by overwriting only the request id.
			this.removeConfirmedReplicator(hash, peer);
			this.removeAutoResendClaim(hash, peer);
		}
		let set = this.requestIPruneSent.get(hash);
		if (!set) {
			set = new Set();
			this.requestIPruneSent.set(hash, set);
		}
		set.add(peer);
		let ids = this.requestIds.get(hash);
		if (!ids) {
			ids = new Map();
			this.requestIds.set(hash, ids);
		}
		ids.set(peer, requestId);
		const session = this.getOrCreateSession(hash);
		session.contacted.add(peer);
		session.phase = "requested";
		return true;
	}

	claimRequestSent(
		hash: string,
		peer: string,
		pending: CheckedPrunePendingDelete,
		requestId: string,
	) {
		if (
			!this.isCurrentRequest(hash, pending) ||
			this.requestIPruneSent.get(hash)?.has(peer) === true ||
			this.requestIds.get(hash)?.has(peer) === true
		) {
			return false;
		}
		return this.addRequestSent(hash, peer, requestId);
	}

	claimAutoResend(
		hash: string,
		peer: string,
		pending: CheckedPrunePendingDelete,
		requestId: string,
	) {
		if (
			!this.isCurrentRequestForPeer(hash, pending, peer, requestId) ||
			!pending.background
		) {
			return false;
		}
		let claims = this.autoResendClaims.get(hash);
		if (!claims) {
			claims = new Map();
			this.autoResendClaims.set(hash, claims);
		}
		const existing = claims.get(peer);
		if (existing?.pending === pending && existing.requestId === requestId) {
			return false;
		}
		claims.set(peer, { pending, requestId });
		return true;
	}

	private removeAutoResendClaim(hash: string, peer: string) {
		const claims = this.autoResendClaims.get(hash);
		claims?.delete(peer);
		if (claims?.size === 0) {
			this.autoResendClaims.delete(hash);
		}
	}

	removeRequestSent(hash: string, peer?: string) {
		if (!peer) {
			this.requestIPruneSent.delete(hash);
			this.requestIds.delete(hash);
			this.autoResendClaims.delete(hash);
			this.clearConfirmedReplicators(hash);
			const session = this.sessions.get(hash);
			session?.contacted.clear();
			this.deleteSessionIfIdle(hash);
			return;
		}
		const set = this.requestIPruneSent.get(hash);
		set?.delete(peer);
		this.removeConfirmedReplicator(hash, peer);
		this.removeAutoResendClaim(hash, peer);
		const ids = this.requestIds.get(hash);
		ids?.delete(peer);
		if (ids?.size === 0) {
			this.requestIds.delete(hash);
		}
		const session = this.sessions.get(hash);
		session?.contacted.delete(peer);
		if (set?.size === 0) {
			this.requestIPruneSent.delete(hash);
			this.deleteSessionIfIdle(hash);
		}
	}

	removeRequestsSent(hashes: Iterable<string>, peer?: string) {
		if (this.requestIPruneSent.size === 0) {
			return;
		}
		for (const hash of hashes) {
			this.removeRequestSent(hash, peer);
		}
	}

	addConfirmedReplicator(
		hash: string,
		peer: string,
		pending: CheckedPrunePendingDelete,
		requestId: string,
	) {
		if (!this.isCurrentRequestForPeer(hash, pending, peer, requestId)) {
			return undefined;
		}
		let set = this.responseReplicatorSet.get(hash);
		if (!set) {
			set = new Set();
			this.responseReplicatorSet.set(hash, set);
		}
		set.add(peer);
		let ids = this.confirmationIds.get(hash);
		if (!ids) {
			ids = new Map();
			this.confirmationIds.set(hash, ids);
		}
		ids.set(peer, requestId);
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
		const ids = this.confirmationIds.get(hash);
		ids?.delete(peer);
		if (ids?.size === 0) {
			this.confirmationIds.delete(hash);
		}
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
		this.confirmationIds.delete(hash);
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

	getConfirmedRequestId(hash: string, peer: string) {
		return this.confirmationIds.get(hash)?.get(peer);
	}

	getContactedReplicators(hash: string) {
		return this.requestIPruneSent.get(hash);
	}

	getRequestId(hash: string, peer: string) {
		return this.requestIds.get(hash)?.get(peer);
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

	async waitForGrantSends(hash?: string) {
		if (hash) {
			await Promise.all(this.grantSends.get(hash) ?? []);
			return;
		}
		await Promise.all(
			[...this.grantSends.values()].flatMap((sends) => [...sends]),
		);
	}

	markRemoving(hash: string) {
		const session = this.getOrCreateSession(hash);
		session.phase = "removing";
	}

	markDone(hash: string, pending: CheckedPrunePendingDelete) {
		if (!this.isCurrentRequest(hash, pending)) {
			return false;
		}
		const session = this.sessions.get(hash);
		if (session) {
			session.phase = "done";
		}
		this.pendingDeletes.delete(hash);
		this.requestIPruneSent.delete(hash);
		this.requestIds.delete(hash);
		this.responseReplicatorSet.delete(hash);
		this.confirmationIds.delete(hash);
		this.autoResendClaims.delete(hash);
		this.clearRetry(hash);
		this.sessions.delete(hash);
		return true;
	}

	markCancelled(hash: string, options?: { preserveRetry?: boolean }) {
		const retry = this.retries.get(hash);
		const session = this.sessions.get(hash);
		if (session) {
			session.phase = "cancelled";
			session.pending = undefined;
			session.contacted.clear();
			session.confirmed.clear();
			if (!options?.preserveRetry || session.retry !== retry) {
				session.retry = undefined;
			}
		}
		this.requestIPruneSent.delete(hash);
		this.requestIds.delete(hash);
		this.responseReplicatorSet.delete(hash);
		this.confirmationIds.delete(hash);
		this.autoResendClaims.delete(hash);
		if (!options?.preserveRetry) {
			this.clearRetry(hash);
		}
		this.deleteSessionIfIdle(hash);
	}

	cleanupPeer(peer: string) {
		const affected = new Map<
			string,
			{
				hash: string;
				pending: CheckedPrunePendingDelete;
				entry: CheckedPruneEntry<T, R>;
				leaders: CheckedPruneLeaderMap | Set<string>;
			}
		>();
		for (const [hash, peers] of this.requestIPruneSent) {
			if (!peers.has(peer)) {
				continue;
			}
			const session = this.sessions.get(hash);
			const pending = this.pendingDeletes.get(hash);
			if (
				pending &&
				session?.pending === pending &&
				session.entry &&
				session.leaders
			) {
				affected.set(hash, {
					hash,
					pending,
					entry: session.entry,
					leaders: session.leaders,
				});
			}
		}
		for (const [hash, peers] of this.requestIPruneSent) {
			peers.delete(peer);
			const ids = this.requestIds.get(hash);
			ids?.delete(peer);
			if (ids?.size === 0) {
				this.requestIds.delete(hash);
			}
			this.sessions.get(hash)?.contacted.delete(peer);
			if (peers.size === 0) {
				this.requestIPruneSent.delete(hash);
				this.deleteSessionIfIdle(hash);
			}
		}

		for (const [hash, peers] of this.responseReplicatorSet) {
			peers.delete(peer);
			const ids = this.confirmationIds.get(hash);
			ids?.delete(peer);
			if (ids?.size === 0) {
				this.confirmationIds.delete(hash);
			}
			this.sessions.get(hash)?.confirmed.delete(peer);
			if (peers.size === 0) {
				this.responseReplicatorSet.delete(hash);
				this.deleteSessionIfIdle(hash);
			}
		}
		for (const [hash, claims] of this.autoResendClaims) {
			claims.delete(peer);
			if (claims.size === 0) {
				this.autoResendClaims.delete(hash);
			}
		}
		return [...affected.values()];
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
		this.requestIds.clear();
		this.responseReplicatorSet.clear();
		this.confirmationIds.clear();
		this.autoResendClaims.clear();
		this.retries.clear();
		this.sessions.clear();
	}
}
