// One InstanceLifecycle per SharedLog.open(). Stage 2 of the
// session/lifecycle refactor introduced the object as a WRAPPER over the
// existing fences via late-bound readers; stage 3 migrated the documented
// multi-fence seams onto this API; stage 4 makes it the physical owner of
// the per-open state: the role sub-generation, the receive-ownership
// counters, and the ownership/membership AbortControllers themselves (the
// index.ts ratchet entries moved file-to-file into this file's baseline in
// scripts/ci/check-fence-ratchet.mjs). The remaining deps are late-bound
// readers for host state that outlives any single lifecycle (poison latch,
// close controller, coordinator/debouncer identities).
import { TerminalOperationNotStartedError } from "@peerbit/program";

export type InstanceLifecyclePhase =
	| "opening" // installed by open(); markOpenComplete() not yet reached
	| "active" // open() returned; current, unfenced, unpoisoned
	| "poisoned" // poisonReplicationOwnership latched (survives until reopen)
	| "terminating" // ownership controller aborted by close()/drop()/_close()
	| "closed"; // host Program reports closed

export type InstanceLifecycleTerminalReason =
	| "close"
	| "drop"
	| "internal-close";

export type InstanceLifecycleDeps = {
	/** host._instanceLifecycle — identity anchor for isCurrent() */
	getCurrentLifecycle: () => InstanceLifecycle | undefined;
	/** host._closeController */
	getCloseController: () => AbortController | undefined;
	/** host._replicationRangeMutationFailure (poison latch, fence A2) */
	getPoisonFailure: () => unknown;
	/** host.closed */
	isHostClosed: () => boolean;
	/** host.isTerminating() — closed || parent-detach || closeController aborted */
	isHostTerminating: () => boolean;
	/** host._checkedPrune — identity only, never dereferenced (fence A3) */
	getCheckedPruneCoordinator: () => object | undefined;
	/** host._replicationRangeMutationsClosing (terminal lane fence, A4) */
	areRangeMutationsClosing: () => boolean;
	/** host._pruneRemovesClosing (terminal lane fence, A4) */
	arePruneRemovesClosing: () => boolean;
	/** host.pruneDebouncedFn — identity only */
	getPruneDebouncer: () => object | undefined;
	/** host.replicationChangeDebounceFn — identity only */
	getReplicationChangeDebouncer: () => object | undefined;
	/** host.rebalanceParticipationDebounced — identity only */
	getRebalanceDebouncer: () => object | undefined;
};

export class InstanceLifecycle {
	// The ONLY state that physically moved in stage 2 (absorbed the former
	// SharedLog._localReplicationRoleGeneration; the accessor shim was deleted
	// in stage 3). Bumped by replicate()'s
	// fixed-role replacement and by full unreplication WITHOUT rotating the
	// lifecycle: unreplicate() deliberately keeps the store open, so admitted
	// adaptive planners are invalidated by this sub-generation, not by
	// retiring the instance identity.
	public roleGeneration = 0;

	// Moved from SharedLog (fences C1/C2, same names — the sanctioned
	// file-to-file ratchet move). Physically owned per-open counters read and
	// written directly through the lifecycle owner; the fresh lifecycle object
	// at open() IS the legacy reset-to-0 (same pattern as roleGeneration above).
	//
	// Receive-side ownership plans may span lower-log joins that invoke user
	// code. Incremented synchronously with leader-cache invalidation so the
	// handler can detect whether its pre-join plan needs one fresh
	// post-persist audit.
	//
	// PERMANENT (fence census closed NO-GO 2026-08-12): a sub-generation
	// WITHIN one lifecycle — it advances while the lifecycle identity is
	// deliberately unchanged, so an identity/session token cannot tell a
	// pre-invalidation plan from a post-invalidation one.
	public _receiveOwnershipRevision = 0;
	// Count of ownership-changing range mutations from queue admission
	// through settlement, including mutations already pending when a receive
	// starts.
	//
	// PERMANENT (fence census closed NO-GO 2026-08-12): a concurrency-DEPTH
	// refcount, not a staleness token — identity answers "which generation
	// started this?", never "how many mutation lanes are open right now?".
	public _receiveOwnershipMutationAdmissions = 0;

	// ---- stage 4: physically owned controllers (moved from SharedLog) ----
	// Fences A1/A5; the index.ts ratchet entries _repairLifecycleController /
	// _replicationLifecycleController moved file-to-file under these names.
	// Rotate-with-the-lifecycle by construction: a fresh InstanceLifecycle IS
	// the rotation of both controllers; within one lifecycle the ownership
	// controller identity never changes (readonly) — poison and terminal
	// paths abort it IN PLACE, exactly like the legacy un-reassigned host
	// field. Background repair work can outlive the await that admitted it;
	// the per-open replacement of this whole object keeps an older runner
	// from dispatching into or mutating a freshly opened lifecycle.
	//
	// INVARIANT every rotation site must keep: whatever installs a new
	// host._instanceLifecycle aborts the outgoing lifecycle's ownership
	// controller in the same synchronous block (startRepairLifecycle does).
	// The stale-capture predicates in isActiveFor lean on
	// non-current ⇒ aborted now that the identity term compares against the
	// owned field rather than the host-current controller.
	public readonly ownershipLifecycleController = new AbortController();
	// undefined until open() reaches resetSubscriptionChangeCallbackTracking
	// (beginMembership) — preserves the legacy `?: AbortController` decl-site
	// semantics for constructed-but-never-opened hosts.
	public membershipLifecycleController?: AbortController;

	// Stage-2 bookkeeping. WRITE-ONLY in production paths: no query method
	// consults these (phase() is derived from the wrapped fences), so a
	// missed or doubled transition cannot change behavior. Stage 3 flips
	// phase() onto declaredPhase once the transitions have soaked in CI.
	public declaredPhase: InstanceLifecyclePhase = "opening";
	public terminalReason?: InstanceLifecycleTerminalReason;
	public poisonCause?: unknown;

	constructor(private readonly deps: InstanceLifecycleDeps) {}

	// ---- rotation-point transitions (stage 2: diagnostics only) ----

	markOpenComplete(): void {
		if (this.declaredPhase === "opening") this.declaredPhase = "active";
	}

	beginTerminal(reason: InstanceLifecycleTerminalReason): void {
		this.terminalReason ??= reason;
		if (this.declaredPhase !== "poisoned") this.declaredPhase = "terminating";
	}

	markPoisoned(cause: unknown): void {
		this.poisonCause ??= cause;
		this.declaredPhase = "poisoned";
	}

	// ---- owned-controller transitions (stage 4) ----

	/** ≡ legacy SharedLog.stopRepairLifecycle body (`controller?.abort()`),
	 * and the abort half of legacy startRepairLifecycle. Abort-in-place: the
	 * lifecycle identity is untouched (the poison contract — poison never
	 * rotates the lifecycle, so captures keep passing the identity term and
	 * fail on `signal.aborted`/poison exactly as before). */
	abortOwnership(): void {
		this.ownershipLifecycleController.abort();
	}

	/** ≡ the legacy guarded abort in stopSubscriptionChangeCallbackAdmission:
	 * `if (!c?.signal.aborted) c?.abort(reason)`. */
	abortMembership(reason?: unknown): void {
		if (!this.membershipLifecycleController?.signal.aborted) {
			this.membershipLifecycleController?.abort(reason);
		}
	}

	/** ≡ legacy `this._replicationLifecycleController = new AbortController()`
	 * in resetSubscriptionChangeCallbackTracking. Called exactly once per
	 * lifecycle, from open(); plain assignment kept (KEEP-OLD overwrite: the
	 * predecessor either is undefined or was aborted at close/drop). A
	 * direct out-of-open rotation (test-only) leaves the NEW lifecycle's
	 * membership undefined until the next open — a deliberate divergence
	 * from legacy, where the host field survived direct rotations; the two
	 * observables that path uses are pinned in the spec. */
	beginMembership(): AbortController {
		return (this.membershipLifecycleController = new AbortController());
	}

	// ---- role sub-generation (absorbed fence A6) ----

	bumpRoleGeneration(): number {
		return ++this.roleGeneration;
	}

	isRoleCurrent(capturedRoleGeneration: number): boolean {
		return this.roleGeneration === capturedRoleGeneration;
	}

	// ---- identity ----

	isCurrent(): boolean {
		return this.deps.getCurrentLifecycle() === this;
	}

	// ---- derived phase (single source of truth = the wrapped fences) ----

	phase(): InstanceLifecyclePhase {
		if (this.deps.getPoisonFailure() !== undefined) return "poisoned";
		if (this.deps.isHostClosed()) return "closed";
		// The legacy `ownership == null` arm is gone: a lifecycle object owns
		// its controller, so the absent-controller state is unrepresentable
		// (clones have no lifecycle and cannot reach phase()).
		if (!this.isCurrent() || this.ownershipLifecycleController.signal.aborted) {
			return "terminating";
		}
		return this.declaredPhase === "opening" ? "opening" : "active";
	}

	// ---- ownership half: exact folds of the legacy predicates ----

	/** ≡ SharedLog.isRepairLifecycleActive(controller). Stage 4 note: the
	 * identity term compares against the OWNED controller. For a stale
	 * captured lifecycle with its co-captured controller the term now
	 * passes where the legacy host-current compare failed; the predicate
	 * still returns false via `signal.aborted`, guaranteed by the rotation
	 * invariant (non-current ⇒ abortOwnership already ran — see the field
	 * comment above). */
	isActiveFor(controller: AbortController | undefined): boolean {
		return (
			controller != null &&
			controller === this.ownershipLifecycleController &&
			!controller.signal.aborted &&
			this.deps.getPoisonFailure() === undefined &&
			!this.deps.isHostClosed()
		);
	}

	/**
	 * Controller-free form for stage-3 seams. Because this object OWNS its
	 * ownership controller (stage 4), lifecycle and controller rotate
	 * together by construction, so `capturedLifecycle.isActive()` ⇔
	 * `isRepairLifecycleActive(capturedController)` for captures taken
	 * together — the equivalence the unit spec asserts.
	 */
	isActive(): boolean {
		return (
			this.isCurrent() && this.isActiveFor(this.ownershipLifecycleController)
		);
	}

	/** ≡ SharedLog.throwIfReplicationOwnershipPoisoned — message and `cause`
	 * must stay byte-identical for stage-3 drop-in. */
	throwIfPoisoned(): void {
		const failure = this.deps.getPoisonFailure();
		if (failure !== undefined) {
			throw new Error(
				"Replication ownership recovery is required before further planning",
				{ cause: failure },
			);
		}
	}

	/** ≡ SharedLog.throwIfReplicationOwnershipLifecycleInactive. */
	throwIfInactive(
		controller: AbortController | undefined = this.ownershipLifecycleController,
	): void {
		this.throwIfPoisoned();
		if (!(this.isCurrent() && this.isActiveFor(controller))) {
			throw new TerminalOperationNotStartedError(
				"Replication ownership lifecycle is no longer active",
			);
		}
	}

	/** ≡ SharedLog.captureReplicationOwnershipLifecycle, but returns the
	 * lifecycle; stage-3 sites capture this instead of the raw controller
	 * (which stays reachable via ownershipSignal()). */
	capture(): InstanceLifecycle {
		this.throwIfInactive();
		return this;
	}

	/** AbortControllers stay the signal carriers. */
	ownershipSignal(): AbortSignal | undefined {
		return this.ownershipLifecycleController.signal;
	}

	// ---- membership half (fence A5): exact fold ----

	/** ≡ SharedLog.isReplicationLifecycleActive(controller).
	 * NOTE the asymmetry vs isActiveFor: this side uses isTerminating()
	 * (closeController + parent-detach), not `closed` — preserve exactly. */
	isMembershipActiveFor(controller: AbortController | undefined): boolean {
		return (
			controller != null &&
			controller === this.membershipLifecycleController &&
			!controller.signal.aborted &&
			!this.deps.isHostTerminating()
		);
	}

	membershipSignal(): AbortSignal | undefined {
		return this.membershipLifecycleController?.signal;
	}

	closeSignal(): AbortSignal | undefined {
		return this.deps.getCloseController()?.signal;
	}

	// ---- checked-prune identity (seams 4 and 7) ----

	/** ≡ prune()'s isCheckedPruneLifecycleCurrent triple for captures taken
	 * at the same admission point. Stage 3 extension: an omitted
	 * `closeController` SKIPS that term (the non-prune() checked-prune seats
	 * never compared it — adding the compare would strengthen their
	 * predicates), and `controller` lets a seat honor an explicitly threaded
	 * (possibly stale) ownership capture instead of the current one. */
	isCheckedPruneCurrent(
		coordinator: object | undefined,
		closeController?: AbortController,
		controller: AbortController | undefined = this.ownershipLifecycleController,
	): boolean {
		return (
			this.isCurrent() &&
			this.isActiveFor(controller) &&
			coordinator === this.deps.getCheckedPruneCoordinator() &&
			(closeController === undefined ||
				closeController === this.deps.getCloseController())
		);
	}

	/** Exact fold of prune()'s throwIfCheckedPruneLifecycleInactive and of
	 * revalidateCheckedPruneOwnership's controller branch (there with the
	 * closeController term omitted). Error order MUST stay: poison Error →
	 * ownership TerminalOperationNotStartedError ("Replication ownership
	 * lifecycle is no longer active") → checked-prune
	 * TerminalOperationNotStartedError ("Checked prune lifecycle is no
	 * longer active"). */
	throwIfCheckedPruneInactive(
		coordinator: object | undefined,
		closeController?: AbortController,
		controller: AbortController | undefined = this.ownershipLifecycleController,
	): void {
		this.throwIfInactive(controller);
		if (
			coordinator !== this.deps.getCheckedPruneCoordinator() ||
			(closeController !== undefined &&
				closeController !== this.deps.getCloseController())
		) {
			throw new TerminalOperationNotStartedError(
				"Checked prune lifecycle is no longer active",
			);
		}
	}

	// ---- terminal lane fences (A4): exposed, NOT folded into isActive ----
	// Folding them would change which TerminalOperationNotStartedError message
	// the admission sites throw ("Replication range mutations are closing",
	// "Prune removals are closing") — a behavior change.

	areRangeMutationsClosing(): boolean {
		return this.deps.areRangeMutationsClosing();
	}

	arePruneRemovesClosing(): boolean {
		return this.deps.arePruneRemovesClosing();
	}

	// ---- debouncer identities (seam 6 et al.) ----
	// Bare === on purpose: at rebalanceParticipation the captured default
	// parameter may be undefined and `undefined === undefined` legitimately
	// passes today. A null-guard would break that.

	isPruneDebouncerCurrent(fn: object | undefined): boolean {
		return fn === this.deps.getPruneDebouncer();
	}

	isReplicationChangeDebouncerCurrent(fn: object | undefined): boolean {
		return fn === this.deps.getReplicationChangeDebouncer();
	}

	isRebalanceDebouncerCurrent(fn: object | undefined): boolean {
		return fn === this.deps.getRebalanceDebouncer();
	}
}
