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
	clear: () => void;
	resolve: (publicKeyHash: string) => Promise<void> | void;
	reject(reason: any): Promise<void> | void;
};

export type CheckedPruneRetryState<T, R extends "u32" | "u64"> = {
	attempts: number;
	timer?: ReturnType<typeof setTimeout>;
	entry: CheckedPruneEntry<T, R>;
	leaders: CheckedPruneLeaderMap | Set<string>;
};

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
};

export class CheckedPruneCoordinator<T, R extends "u32" | "u64"> {
	readonly pendingDeletes = new Map<string, CheckedPrunePendingDelete>();
	readonly requestIPruneSent = new Map<string, Set<string>>();
	readonly responseReplicatorSet = new Map<string, Set<string>>();
	readonly retries = new Map<string, CheckedPruneRetryState<T, R>>();
	private readonly sessions = new Map<string, CheckedPruneSession<T, R>>();

	private getOrCreateSession(hash: string): CheckedPruneSession<T, R> {
		let session = this.sessions.get(hash);
		if (!session) {
			session = {
				phase: "candidate",
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

	trackCandidate(
		hash: string,
		entry: CheckedPruneEntry<T, R>,
		leaders: CheckedPruneLeaderMap | Set<string>,
	) {
		const session = this.remember(hash, entry, leaders);
		if (!this.hasActiveWork(hash)) {
			session.phase = "candidate";
		}
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
		this.pendingDeletes.set(hash, pending);
		const session = this.remember(hash, entry, leaders);
		session.pending = pending;
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

	setRetry(hash: string, state: CheckedPruneRetryState<T, R>) {
		this.retries.set(hash, state);
		const session = this.remember(hash, state.entry, state.leaders);
		session.retry = state;
		session.phase = "retrying";
	}

	clearRetry(hash: string) {
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

	addRequestSent(hash: string, peer: string) {
		let set = this.requestIPruneSent.get(hash);
		if (!set) {
			set = new Set();
			this.requestIPruneSent.set(hash, set);
		}
		set.add(peer);
		const session = this.getOrCreateSession(hash);
		session.contacted.add(peer);
		if (session.phase === "candidate") {
			session.phase = "requested";
		}
	}

	removeRequestSent(hash: string, peer?: string) {
		if (!peer) {
			this.requestIPruneSent.delete(hash);
			const session = this.sessions.get(hash);
			session?.contacted.clear();
			this.deleteSessionIfIdle(hash);
			return;
		}
		const set = this.requestIPruneSent.get(hash);
		if (!set) {
			return;
		}
		set.delete(peer);
		const session = this.sessions.get(hash);
		session?.contacted.delete(peer);
		if (set.size === 0) {
			this.requestIPruneSent.delete(hash);
			this.deleteSessionIfIdle(hash);
		}
	}

	addConfirmedReplicator(hash: string, peer: string) {
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

	clearConfirmedReplicators(hash: string) {
		this.responseReplicatorSet.delete(hash);
		const session = this.sessions.get(hash);
		session?.confirmed.clear();
		this.deleteSessionIfIdle(hash);
	}

	getConfirmedReplicators(hash: string) {
		return this.responseReplicatorSet.get(hash);
	}

	getContactedReplicators(hash: string) {
		return this.requestIPruneSent.get(hash);
	}

	markRemoving(hash: string) {
		const session = this.getOrCreateSession(hash);
		session.phase = "removing";
	}

	markDone(hash: string) {
		const session = this.sessions.get(hash);
		if (session) {
			session.phase = "done";
		}
		this.pendingDeletes.delete(hash);
		this.requestIPruneSent.delete(hash);
		this.responseReplicatorSet.delete(hash);
		this.clearRetry(hash);
		this.sessions.delete(hash);
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
		this.responseReplicatorSet.delete(hash);
		if (!options?.preserveRetry) {
			this.clearRetry(hash);
		}
		this.deleteSessionIfIdle(hash);
	}

	cleanupPeer(peer: string) {
		for (const [hash, peers] of this.requestIPruneSent) {
			peers.delete(peer);
			this.sessions.get(hash)?.contacted.delete(peer);
			if (peers.size === 0) {
				this.requestIPruneSent.delete(hash);
				this.deleteSessionIfIdle(hash);
			}
		}

		for (const [hash, peers] of this.responseReplicatorSet) {
			peers.delete(peer);
			this.sessions.get(hash)?.confirmed.delete(peer);
			if (peers.size === 0) {
				this.responseReplicatorSet.delete(hash);
				this.deleteSessionIfIdle(hash);
			}
		}
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
	}
}
