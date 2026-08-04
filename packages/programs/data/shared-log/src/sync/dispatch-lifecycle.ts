// Shared dispatch-lifecycle registry for the sync module (stage-4 sync
// unification). The simple and rateless synchronizers used to carry
// near-verbatim copies of the capture/abort/finish/dispose/isActive
// lifecycle machinery; this registry hosts the shared mechanics and keeps
// the two deliberate behavioral deltas as injected strategies:
//
// - target activity: the simple synchronizer compares the target's dispatch
//   EPOCH identity, the rateless synchronizer checks SET MEMBERSHIP of the
//   target lifecycle in the active-target registry. The strategy is always
//   injected (`isTargetCurrent`), never defaulted.
// - retention: disposal is deferred while `hasRetainedWork` is true — a
//   retained-work counter for the simple synchronizer, a per-target
//   retainedByProcess/responseLeases scan for the rateless one.
//
// Lifecycle objects are constructed by the callers (they carry
// caller-specific fields such as epochs, batches and retained-work
// counters); the registry owns the per-target active sets, the listener
// add/remove pairing and the dispose gating.

export interface DispatchTargetLifecycleBase<LC> {
	lifecycle: LC;
	target: string;
	controller: AbortController;
}

export interface DispatchLifecycleBase<TL> {
	ownershipLifecycleController: AbortController;
	callerSignal?: AbortSignal;
	controller: AbortController;
	targets: Map<string, TL>;
	onOwnerOrCallerAbort: () => void;
	dispatchFinished: boolean;
	disposed: boolean;
}

export type DispatchLifecycleRegistryDeps<
	LC extends DispatchLifecycleBase<TL>,
	TL extends DispatchTargetLifecycleBase<LC>,
> = {
	isClosed: () => boolean;
	currentOwnershipLifecycleController: () => AbortController;
	// Retention hook: disposal is deferred while this is true.
	hasRetainedWork: (lifecycle: LC) => boolean;
	// Target-activity strategy (epoch identity vs set membership).
	isTargetCurrent: (targetLifecycle: TL) => boolean;
	// Side effects on target abort (the simple synchronizer removes the
	// target's pending response batches here).
	onTargetAborted?: (targetLifecycle: TL) => void;
};

export class DispatchLifecycleRegistry<
	LC extends DispatchLifecycleBase<TL>,
	TL extends DispatchTargetLifecycleBase<LC>,
> {
	readonly activeTargets: Map<string, Set<TL>> = new Map();

	constructor(private readonly deps: DispatchLifecycleRegistryDeps<LC, TL>) {}

	// Registers the caller-constructed lifecycle: creates the per-target
	// lifecycles (the factory may skip a target or abort it right after
	// registration via `onTargetRegistered`), pairs the owner/caller abort
	// listeners and performs the initial staleness abort exactly like both
	// former capture implementations.
	register(
		lifecycle: LC,
		targets: string[],
		createTargetLifecycle: (lifecycle: LC, target: string) => TL | undefined,
		onTargetRegistered?: (targetLifecycle: TL) => void,
	): void {
		for (const target of [...new Set(targets)]) {
			const targetLifecycle = createTargetLifecycle(lifecycle, target);
			if (!targetLifecycle) {
				continue;
			}
			lifecycle.targets.set(target, targetLifecycle);
			let activeForTarget = this.activeTargets.get(target);
			if (!activeForTarget) {
				activeForTarget = new Set();
				this.activeTargets.set(target, activeForTarget);
			}
			activeForTarget.add(targetLifecycle);
			onTargetRegistered?.(targetLifecycle);
		}

		lifecycle.ownershipLifecycleController.signal.addEventListener(
			"abort",
			lifecycle.onOwnerOrCallerAbort,
			{ once: true },
		);
		if (
			lifecycle.callerSignal &&
			lifecycle.callerSignal !== lifecycle.ownershipLifecycleController.signal
		) {
			lifecycle.callerSignal.addEventListener(
				"abort",
				lifecycle.onOwnerOrCallerAbort,
				{ once: true },
			);
		}
		if (
			this.deps.isClosed() ||
			lifecycle.ownershipLifecycleController !==
				this.deps.currentOwnershipLifecycleController() ||
			lifecycle.ownershipLifecycleController.signal.aborted ||
			lifecycle.callerSignal?.aborted
		) {
			lifecycle.onOwnerOrCallerAbort();
		}
	}

	abortTarget(targetLifecycle: TL, reason?: unknown): void {
		if (!targetLifecycle.controller.signal.aborted) {
			targetLifecycle.controller.abort(reason);
		}
		this.deps.onTargetAborted?.(targetLifecycle);
		this.maybeDispose(targetLifecycle.lifecycle);
	}

	abortLifecycle(lifecycle: LC, reason?: unknown): void {
		if (!lifecycle.controller.signal.aborted) {
			lifecycle.controller.abort(reason);
		}
		for (const targetLifecycle of lifecycle.targets.values()) {
			this.abortTarget(targetLifecycle, reason);
		}
		this.maybeDispose(lifecycle);
	}

	finish(lifecycle: LC): void {
		lifecycle.dispatchFinished = true;
		this.maybeDispose(lifecycle);
	}

	maybeDispose(lifecycle: LC): void {
		if (
			lifecycle.disposed ||
			!lifecycle.dispatchFinished ||
			this.deps.hasRetainedWork(lifecycle)
		) {
			return;
		}
		lifecycle.disposed = true;
		lifecycle.ownershipLifecycleController.signal.removeEventListener(
			"abort",
			lifecycle.onOwnerOrCallerAbort,
		);
		if (
			lifecycle.callerSignal &&
			lifecycle.callerSignal !== lifecycle.ownershipLifecycleController.signal
		) {
			lifecycle.callerSignal.removeEventListener(
				"abort",
				lifecycle.onOwnerOrCallerAbort,
			);
		}
		for (const targetLifecycle of lifecycle.targets.values()) {
			const activeForTarget = this.activeTargets.get(targetLifecycle.target);
			activeForTarget?.delete(targetLifecycle);
			if (activeForTarget?.size === 0) {
				this.activeTargets.delete(targetLifecycle.target);
			}
		}
	}

	isLifecycleActive(lifecycle: LC, target?: string): boolean {
		if (
			this.deps.isClosed() ||
			lifecycle.disposed ||
			lifecycle.ownershipLifecycleController !==
				this.deps.currentOwnershipLifecycleController() ||
			lifecycle.ownershipLifecycleController.signal.aborted ||
			lifecycle.callerSignal?.aborted ||
			lifecycle.controller.signal.aborted
		) {
			return false;
		}
		if (target === undefined) {
			return true;
		}
		const targetLifecycle = lifecycle.targets.get(target);
		return (
			targetLifecycle !== undefined &&
			!targetLifecycle.controller.signal.aborted &&
			this.deps.isTargetCurrent(targetLifecycle)
		);
	}

	getSignal(lifecycle: LC, target: string): AbortSignal {
		return (
			lifecycle.targets.get(target)?.controller.signal ??
			lifecycle.controller.signal
		);
	}
}

// The shared ownership-generation identity check: a captured ownership
// controller is current while the synchronizer is open, the controller is
// still THE controller (identity, not just non-aborted) and it has not been
// aborted.
export const isOwnershipGenerationActive = (properties: {
	closed: boolean;
	ownershipLifecycleController: AbortController;
	currentOwnershipLifecycleController: AbortController;
}): boolean =>
	!properties.closed &&
	properties.ownershipLifecycleController ===
		properties.currentOwnershipLifecycleController &&
	!properties.ownershipLifecycleController.signal.aborted;

// The shared tracked-session identity check (the pattern the host-side
// instance lifecycle drained the index.ts fences into): a session is active
// while nothing cancelled or aborted it, its ownership generation is still
// current, and the session registry still maps its id to THIS session
// object.
export const isTrackedSessionActive = <TSession>(properties: {
	closed: boolean;
	cancelled: boolean;
	sessionController: AbortController;
	ownershipLifecycleController: AbortController;
	currentOwnershipLifecycleController: AbortController;
	session: TSession;
	registered: TSession | undefined;
}): boolean =>
	!properties.closed &&
	!properties.cancelled &&
	!properties.sessionController.signal.aborted &&
	!properties.ownershipLifecycleController.signal.aborted &&
	properties.ownershipLifecycleController ===
		properties.currentOwnershipLifecycleController &&
	properties.registered === properties.session;
