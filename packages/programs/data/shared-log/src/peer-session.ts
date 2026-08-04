// One PeerSession per peer connection-epoch. The session instance IS the
// opaque subscription-epoch token introduced at index.ts advanceSubscriptionEpoch:
// every existing `===` epoch comparison keeps working because tokens were
// always compared by identity and never inspected. Stage 2 wraps; stage 3
// migrates the compound predicates onto isCurrent()/isActive()/
// isReceiveAdmissionOpen() and deletes the raw fences one at a time.

export type PeerSessionKind = "opening" | "departing";

export type PeerSessionPhase =
	| "opening" // subscribe transition; reconnect barrier not yet completed
	| "open" // reconnect barrier completed (replication-info unblocked)
	| "departing" // created by an unsubscribe epoch-advance, fencing the old connection
	| "superseded"; // a newer session replaced this one (terminal)

export type PeerSessionDeps = {
	// Late-bound readers into state that stays physically on SharedLog in
	// stage 2. Delegating closures — NOT bound method refs — so sinon spies
	// installed on the SharedLog instance keep intercepting (same constraint
	// as the stage-1 coordinators).
	isReplicationLifecycleActive: (
		controller: AbortController | undefined,
	) => boolean;
	getReplicationLifecycleController: () => AbortController | undefined;
	isReplicationInfoBlocked: (peerHash: string) => boolean;
	getReceiveCleanupGate: (peerHash: string) => number;
};

export type PeerReceiveAdmissionOptions = {
	allowReplicationInfoBlocked?: boolean;
	allowCleanupGate?: boolean;
};

export class PeerSession {
	readonly peerHash: string;
	readonly kind: PeerSessionKind;
	// The lifecycle controller live at rotation. All current seams pair the
	// epoch check with a lifecycle check against a controller captured in the
	// same synchronous window as the epoch advance; capturing it here
	// reproduces that pairing exactly.
	readonly replicationLifecycleController: AbortController | undefined;
	phase: PeerSessionPhase;
	// Diagnostics only in stage 2: the destructive removal funnel observed a
	// committed replicator removal under this connection-epoch.
	replicatorRemoved = false;
	readonly createdAt = Date.now(); // diagnostics only

	constructor(
		private readonly registry: PeerSessionRegistry,
		peerHash: string,
		kind: PeerSessionKind,
		replicationLifecycleController: AbortController | undefined,
	) {
		this.peerHash = peerHash;
		this.kind = kind;
		this.replicationLifecycleController = replicationLifecycleController;
		this.phase = kind;
	}

	/** ≡ SharedLog.isCurrentSubscriptionEpoch(this.peerHash, this). */
	isCurrent(): boolean {
		return this.registry.current(this.peerHash) === this;
	}

	/** ≡ ownsReplicationLifecycle() && ownsSubscriptionEpoch() with both
	 *  captured at the epoch-advance. */
	isActive(): boolean {
		return (
			this.isCurrent() &&
			this.registry.deps.isReplicationLifecycleActive(
				this.replicationLifecycleController,
			)
		);
	}

	/** ≡ SharedLog.isPeerReceiveAdmissionOpen(peerHash,
	 *  this.replicationLifecycleController, this, options). */
	isReceiveAdmissionOpen(options?: PeerReceiveAdmissionOptions): boolean {
		return this.registry.isReceiveAdmissionOpen(
			this.peerHash,
			this,
			this.replicationLifecycleController,
			options,
		);
	}
}

export class PeerSessionRegistry {
	// Historic field name kept deliberately: the fence-ratchet baseline entry
	// moves file-to-file exactly like the stage-1 extractions did for
	// _joinWarmupGenerationByTarget (scripts/ci/check-fence-ratchet.mjs).
	// The values ARE the epoch tokens. Stage 3 renames this to `sessions`
	// once the last raw-token comparison in index.ts is gone.
	_subscriptionEpochByPeer!: Map<string, PeerSession>;

	constructor(readonly deps: PeerSessionDeps) {
		this.resetForOpen();
	}

	/** open()-time re-init: REPLACE the map instance, matching today's
	 *  `this._subscriptionEpochByPeer = new Map()` (index.ts ctor / open).
	 *  The map is intentionally NOT cleared at _close — tokens must stay
	 *  current across close so a late continuation's epoch check resolves
	 *  exactly as it does today (close-safety comes from the paired
	 *  lifecycle-controller check, not the epoch). There is NO clearForClose(). */
	resetForOpen(): void {
		this._subscriptionEpochByPeer = new Map();
	}

	current(peerHash: string): PeerSession | null {
		return this._subscriptionEpochByPeer.get(peerHash) ?? null;
	}

	/** null is a VALID current value: a peer that never subscribed has no
	 *  session, and today's isCurrentSubscriptionEpoch(peer, null) === true
	 *  admits it (the `?? null` in getSubscriptionEpoch). Preserved exactly. */
	isCurrent(peerHash: string, session: object | null): boolean {
		return this.current(peerHash) === session;
	}

	/** The only creation point. Supersedes (never deletes) the previous
	 *  session — per-peer entries are never removed, matching today's map. */
	rotate(peerHash: string, kind: PeerSessionKind): PeerSession {
		const previous = this._subscriptionEpochByPeer.get(peerHash);
		if (previous) {
			previous.phase = "superseded";
		}
		const next = new PeerSession(
			this,
			peerHash,
			kind,
			this.deps.getReplicationLifecycleController(),
		);
		this._subscriptionEpochByPeer.set(peerHash, next);
		return next;
	}

	/** Reconnect barrier completed. Identity-guarded phase mark; no-op when
	 *  the expected token was superseded meanwhile. */
	markOpen(peerHash: string, expectedSession: object): void {
		const current = this._subscriptionEpochByPeer.get(peerHash);
		if (current === expectedSession && current.kind === "opening") {
			current.phase = "open";
		}
	}

	/** Destructive removal committed for this peer's CURRENT epoch. */
	noteReplicatorRemoved(peerHash: string): void {
		const current = this._subscriptionEpochByPeer.get(peerHash);
		if (current) {
			current.replicatorRemoved = true;
		}
	}

	/** ≡ SharedLog.isPeerReceiveAdmissionOpen, term for term. `session` may
	 *  be null (pre-session peer). Stage 3 migrates the host method's body
	 *  onto this; stage 2 only proves parity in tests. */
	isReceiveAdmissionOpen(
		peerHash: string,
		session: object | null,
		replicationLifecycleController: AbortController | undefined,
		options?: PeerReceiveAdmissionOptions,
	): boolean {
		return (
			this.deps.isReplicationLifecycleActive(replicationLifecycleController) &&
			this.isCurrent(peerHash, session) &&
			(options?.allowReplicationInfoBlocked === true ||
				!this.deps.isReplicationInfoBlocked(peerHash)) &&
			(options?.allowCleanupGate === true ||
				this.deps.getReceiveCleanupGate(peerHash) === 0)
		);
	}
}
