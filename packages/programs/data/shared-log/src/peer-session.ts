// One PeerSession per peer connection-epoch. The session instance IS the
// opaque subscription-epoch token formerly stored by SharedLog: every `===`
// epoch comparison keeps working because tokens were always compared by
// identity and never inspected. Compound predicates now live on
// isCurrent()/isActive()/isReceiveAdmissionOpen().

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
};

export type PeerReceiveAdmissionOptions = {
	allowReplicationInfoBlocked?: boolean;
	allowCleanupGate?: boolean;
};

export class PeerSession {
	readonly peerHash: string;
	readonly kind: PeerSessionKind;
	// True when rotate() superseded an earlier subscription generation. This is
	// deliberately independent of the predecessor's phase/kind: a newer
	// transport can announce a subscription without first delivering an
	// unsubscribe, and must not inherit the old transport's signed capability.
	readonly hasPredecessor: boolean;
	// The lifecycle controller live at rotation. All current seams pair the
	// epoch check with a lifecycle check against a controller captured in the
	// same synchronous window as the epoch advance; capturing it here
	// reproduces that pairing exactly.
	readonly replicationLifecycleController: AbortController | undefined;
	phase: PeerSessionPhase;
	// Diagnostics only in stage 2: the destructive removal funnel observed a
	// committed replicator removal under this connection-epoch.
	replicatorRemoved = false;
	// Reconnect-barrier WINDOW sub-state (stage-3 home of the legacy
	// _subscriptionOpeningEpochByPeer map entry, whose value was always this
	// session). True exactly while the barrier window is open: set when
	// handleSubscriptionChange starts the opening barrier (same synchronous
	// window as the rotation that made this session current), cleared in the
	// barrier's `finally` — including the barrier-throw path — where the map
	// entry used to be deleted. Deliberately NOT derived from
	// `phase === "opening"`: phase is "opening" from rotate() — BEFORE the
	// barrier starts — and stays "opening" forever if the barrier throws
	// (markOpen never runs), which would wrongly keep granting the opening
	// lease bypasses that today's window drops.
	openingBarrierActive = false;
	readonly createdAt = Date.now(); // diagnostics only

	constructor(
		private readonly registry: PeerSessionRegistry,
		peerHash: string,
		kind: PeerSessionKind,
		replicationLifecycleController: AbortController | undefined,
		hasPredecessor: boolean,
	) {
		this.peerHash = peerHash;
		this.kind = kind;
		this.replicationLifecycleController = replicationLifecycleController;
		this.hasPredecessor = hasPredecessor;
		this.phase = kind;
	}

	/** ≡ former SharedLog.isCurrentSubscriptionEpoch(this.peerHash, this). */
	isCurrent(): boolean {
		return this.registry.current(this.peerHash) === this;
	}

	/** Barrier window opens — ≡ the legacy
	 *  `_subscriptionOpeningEpochByPeer.set(peerHash, thisSession)`. */
	beginOpeningBarrier(): void {
		this.openingBarrierActive = true;
	}

	/** Barrier window closes — ≡ the legacy identity-guarded
	 *  `_subscriptionOpeningEpochByPeer.delete(peerHash)`. Unconditional:
	 *  the map guard only avoided clobbering a NEWER peer-keyed entry, and
	 *  per-session flags cannot collide (a newer barrier flags its own,
	 *  already-rotated session). */
	finishOpeningBarrier(): void {
		this.openingBarrierActive = false;
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

	/** ≡ former SharedLog.isPeerReceiveAdmissionOpen(peerHash,
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
	// The per-peer session map (formerly _subscriptionEpochByPeer; renamed
	// once the last raw-token comparison in index.ts was gone — the values
	// ARE the legacy epoch tokens, compared by identity and never inspected).
	// The rename retires the map's fence-ratchet baseline entry: session
	// identity is the mechanism the ratchet drains fences INTO, not a fence.
	sessions!: Map<string, PeerSession>;
	// Moved from SharedLog (fence B2, same name — the sanctioned file-to-file
	// ratchet move). Local receive generations fence replication-info handlers
	// that were admitted before a liveness eviction but reach the per-peer
	// apply lane after it. Per-PEER, not per-session, on purpose: the token
	// advances at removeReplicator, where the session does NOT rotate (the
	// peer stays subscribed), and it must also fence a peer that never had a
	// session. Unlike sessions this map IS cleared at _close (see
	// clearReceiveEpochsForClose) and replaced at open.
	_replicationInfoReceiveEpochByPeer!: Map<string, object>;
	// Moved from SharedLog (fence B6, same name — the sanctioned file-to-file
	// ratchet move). Refcount of in-flight destructive peer cleanups: while
	// non-zero, receive admission for the peer is closed and prune final
	// confirmations ignore the peer. Per-PEER, not per-session, on purpose:
	// the gate is held across removeReplicator's awaited lanes while a
	// reconnect may rotate the session; a fresh session with a zero gate
	// would reopen receive admission mid-drain. The map instance is replaced
	// only at open (resetForOpen) and cleared in place at _close.
	_receiveCleanupGateByPeer!: Map<string, number>;
	// Moved from SharedLog (fence B5, same name). Peers whose replication-info
	// is fenced: added when a departure/unsubscribe rotation or a reconnect
	// barrier starts, removed only when an opening barrier commits. Per-PEER,
	// not per-session, on purpose (same reasoning as B2/B6 above): the block
	// is set under one session (the departing rotation) and cleared under a
	// LATER one (the opening barrier), so its lifetime deliberately spans
	// session identities; readers (prune final-confirmation filter,
	// announcement repair targeting, pruneOfflineReplicators) ask about peers
	// that may have no session at all; and the `??`-fallback rotation in
	// handleSubscriptionChange rotates WITHOUT touching blocked state — a
	// per-session flag would silently reset it there. Replaced only at open
	// (resetForOpen); deliberately NOT cleared at _close, matching the legacy
	// host field site-for-site. Kept as a public field so existing tests can
	// keep instrumenting the raw Set instance (events.spec.ts spies on its
	// `delete`).
	_replicationInfoBlockedPeers!: Set<string>;

	constructor(readonly deps: PeerSessionDeps) {
		this.resetForOpen();
	}

	/** open()-time re-init: REPLACE the map instances, matching today's
	 *  `= new Map()` re-inits (index.ts ctor / open).
	 *  The session map is intentionally NOT cleared at _close — tokens must
	 *  stay current across close so a late continuation's epoch check resolves
	 *  exactly as it does today (close-safety comes from the paired
	 *  lifecycle-controller check, not the epoch). There is NO clearForClose()
	 *  for sessions; the receive-epoch map, by contrast, IS cleared at close. */
	resetForOpen(): void {
		this.sessions = new Map();
		this._replicationInfoReceiveEpochByPeer = new Map();
		this._receiveCleanupGateByPeer = new Map();
		this._replicationInfoBlockedPeers = new Set();
	}

	/** ≡ the legacy `this._replicationInfoBlockedPeers.add(peerHash)` (host
	 *  Set, fence B5). */
	blockReplicationInfo(peerHash: string): void {
		this._replicationInfoBlockedPeers.add(peerHash);
	}

	/** ≡ the legacy `this._replicationInfoBlockedPeers.delete(peerHash)`. */
	unblockReplicationInfo(peerHash: string): void {
		this._replicationInfoBlockedPeers.delete(peerHash);
	}

	/** ≡ the legacy `this._replicationInfoBlockedPeers.has(peerHash)`. */
	isReplicationInfoBlocked(peerHash: string): boolean {
		return this._replicationInfoBlockedPeers.has(peerHash);
	}

	/** _close counterpart of the legacy
	 *  `this._replicationInfoReceiveEpochByPeer?.clear()`: a receive-epoch
	 *  capture held across close must compare against null, never against a
	 *  surviving token. Sessions deliberately survive close (see
	 *  resetForOpen); the paired membership-lifecycle term at every
	 *  receive-epoch check site makes the ordering unobservable either way. */
	clearReceiveEpochsForClose(): void {
		this._replicationInfoReceiveEpochByPeer.clear();
	}

	/** _close counterpart of the legacy
	 *  `this._receiveCleanupGateByPeer?.clear()`: an in-place clear (NOT a
	 *  map replacement) so an in-flight removal's captured release still
	 *  drains the instance it incremented — see acquireReceiveCleanupGate. */
	clearCleanupGatesForClose(): void {
		this._receiveCleanupGateByPeer.clear();
	}

	/** ≡ removeReplicator's inline `blockPeerReceiveAdmission` +
	 *  release-in-`finally` pair (fence B6). Acquire captures the CURRENT
	 *  gate-map instance and increments once; the returned release is
	 *  idempotent and decrements the captured map, deleting the entry at 0.
	 *  The map-instance capture is load-bearing: close/reopen replaces the
	 *  map (resetForOpen), and a late release must drain the exact map it
	 *  incremented — a release against a fresh open's map would corrupt that
	 *  open's refcounts. The `?? 1` mirrors the legacy release: an entry
	 *  cleared at _close decrements to 0 and stays deleted. */
	acquireReceiveCleanupGate(peerHash: string): () => void {
		const gates = this._receiveCleanupGateByPeer;
		gates.set(peerHash, (gates.get(peerHash) ?? 0) + 1);
		let released = false;
		return () => {
			if (released) {
				return;
			}
			released = true;
			const remaining = (gates.get(peerHash) ?? 1) - 1;
			if (remaining > 0) {
				gates.set(peerHash, remaining);
			} else {
				gates.delete(peerHash);
			}
		};
	}

	/** ≡ the legacy `(this._receiveCleanupGateByPeer.get(peer) ?? 0) === 0`
	 *  read (receive-admission term and prune final-confirmation filter). */
	isReceiveCleanupGateOpen(peerHash: string): boolean {
		return (this._receiveCleanupGateByPeer.get(peerHash) ?? 0) === 0;
	}

	/** ≡ legacy SharedLog.advanceReplicationInfoReceiveEpoch. Advances
	 *  WITHOUT rotating the session — the recovery fence must survive
	 *  removeReplicator, where the peer stays subscribed. */
	advanceReceiveEpoch(peerHash: string): object {
		const next = {};
		this._replicationInfoReceiveEpochByPeer.set(peerHash, next);
		return next;
	}

	/** ≡ legacy SharedLog.getReplicationInfoReceiveEpoch (`?? null`). */
	receiveEpoch(peerHash: string): object | null {
		return this._replicationInfoReceiveEpochByPeer.get(peerHash) ?? null;
	}

	/** ≡ legacy SharedLog.isCurrentReplicationInfoReceiveEpoch. */
	isReceiveEpochCurrent(peerHash: string, epoch: object | null): boolean {
		return this.receiveEpoch(peerHash) === epoch;
	}

	current(peerHash: string): PeerSession | null {
		return this.sessions.get(peerHash) ?? null;
	}

	/** null is a VALID current value: a peer that never subscribed has no
	 *  session, and the former host predicate admitted
	 *  isCurrent(peer, null) === true. Preserved exactly. */
	isCurrent(peerHash: string, session: object | null): boolean {
		return this.current(peerHash) === session;
	}

	/** The only creation point. Supersedes (never deletes) the previous
	 *  session — per-peer entries are never removed, matching today's map. */
	rotate(peerHash: string, kind: PeerSessionKind): PeerSession {
		const previous = this.sessions.get(peerHash);
		if (previous) {
			previous.phase = "superseded";
		}
		const next = new PeerSession(
			this,
			peerHash,
			kind,
			this.deps.getReplicationLifecycleController(),
			previous !== undefined,
		);
		this.sessions.set(peerHash, next);
		return next;
	}

	/** Reconnect barrier completed. Identity-guarded phase mark; no-op when
	 *  the expected token was superseded meanwhile. */
	markOpen(peerHash: string, expectedSession: object): void {
		const current = this.sessions.get(peerHash);
		if (
			current !== undefined &&
			current === expectedSession &&
			current.kind === "opening"
		) {
			current.phase = "open";
		}
	}

	/** Destructive removal committed for this peer. Mirrors the removal
	 *  funnel's epoch scoping: an epoch-scoped removal only stamps the
	 *  session it was scoped to — a reconnect during the removal's awaited
	 *  lanes must not inherit the stamp. Undefined = unscoped removal,
	 *  which stamps the current session as before; null scopes to "peer had
	 *  no session at capture", so a session created meanwhile is never
	 *  stamped. */
	noteReplicatorRemoved(
		peerHash: string,
		expectedSession?: object | null,
	): void {
		const current = this.sessions.get(peerHash);
		if (
			current &&
			(expectedSession === undefined || current === expectedSession)
		) {
			current.replicatorRemoved = true;
		}
	}

	/** Preserves the former SharedLog.isPeerReceiveAdmissionOpen predicate term
	 *  for term. `session` may be null (pre-session peer). */
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
				!this.isReplicationInfoBlocked(peerHash)) &&
			(options?.allowCleanupGate === true ||
				this.isReceiveCleanupGateOpen(peerHash))
		);
	}
}
