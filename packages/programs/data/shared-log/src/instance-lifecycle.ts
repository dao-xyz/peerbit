// Stage 2 of the session/lifecycle refactor: one InstanceLifecycle per
// SharedLog.open(). In stage 2 the object WRAPS the existing fences via
// late-bound readers — it physically owns nothing except the role
// sub-generation — so every existing check keeps working unchanged. Stage 3
// migrates the eight documented multi-fence seams onto this API and deletes
// the wrapped fences one at a time (each deletion shrinks the index.ts
// baseline in scripts/ci/check-fence-ratchet.mjs).
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
	/** host._repairLifecycleController (ownership half, fence A1) */
	getOwnershipController: () => AbortController | undefined;
	/** host._replicationLifecycleController (membership half, fence A5) */
	getMembershipController: () => AbortController | undefined;
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
	// written by the host through delegating accessors that keep every legacy
	// site verbatim; the fresh lifecycle object at open() IS the legacy
	// reset-to-0 (same pattern as roleGeneration above).
	//
	// Receive-side ownership plans may span lower-log joins that invoke user
	// code. Incremented synchronously with leader-cache invalidation so the
	// handler can detect whether its pre-join plan needs one fresh
	// post-persist audit.
	public _receiveOwnershipRevision = 0;
	// Count of ownership-changing range mutations from queue admission
	// through settlement, including mutations already pending when a receive
	// starts.
	public _receiveOwnershipMutationAdmissions = 0;

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
		const ownership = this.deps.getOwnershipController();
		if (!this.isCurrent() || ownership == null || ownership.signal.aborted) {
			return "terminating";
		}
		return this.declaredPhase === "opening" ? "opening" : "active";
	}

	// ---- ownership half: exact folds of the legacy predicates ----

	/** ≡ SharedLog.isRepairLifecycleActive(controller). */
	isActiveFor(controller: AbortController | undefined): boolean {
		return (
			controller != null &&
			controller === this.deps.getOwnershipController() &&
			!controller.signal.aborted &&
			this.deps.getPoisonFailure() === undefined &&
			!this.deps.isHostClosed()
		);
	}

	/**
	 * Controller-free form for stage-3 seams. Because this object and
	 * _repairLifecycleController rotate at the same single point (open()),
	 * `capturedLifecycle.isActive()` ⇔
	 * `isRepairLifecycleActive(capturedController)` for captures taken
	 * together — the equivalence the unit spec asserts.
	 */
	isActive(): boolean {
		return (
			this.isCurrent() && this.isActiveFor(this.deps.getOwnershipController())
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
		controller: AbortController | undefined = this.deps.getOwnershipController(),
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

	/** AbortControllers stay the signal carriers in stages 2 and 3. */
	ownershipSignal(): AbortSignal | undefined {
		return this.deps.getOwnershipController()?.signal;
	}

	// ---- membership half (fence A5): exact fold ----

	/** ≡ SharedLog.isReplicationLifecycleActive(controller).
	 * NOTE the asymmetry vs isActiveFor: this side uses isTerminating()
	 * (closeController + parent-detach), not `closed` — preserve exactly. */
	isMembershipActiveFor(controller: AbortController | undefined): boolean {
		return (
			controller != null &&
			controller === this.deps.getMembershipController() &&
			!controller.signal.aborted &&
			!this.deps.isHostTerminating()
		);
	}

	membershipSignal(): AbortSignal | undefined {
		return this.deps.getMembershipController()?.signal;
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
		controller: AbortController | undefined = this.deps.getOwnershipController(),
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
		controller: AbortController | undefined = this.deps.getOwnershipController(),
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
