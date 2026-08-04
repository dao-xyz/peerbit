// Lifecycle refactor guard: the pure-unit half asserts truth-table
// equivalence with the legacy predicates InstanceLifecycle folds (transcribed
// inline against the owned controllers — stage 4 made the lifecycle the
// physical owner of the ownership/membership AbortControllers), and the
// integration half asserts the per-open rotation/identity semantics against
// a real SharedLog. The legacy `ownership == null` truth-table arm is gone:
// a lifecycle object owns its (readonly) ownership controller, so the
// absent-controller state is unrepresentable by construction; the reachable
// undefined semantics stay pinned via isActiveFor(undefined) and the
// never-opened-clone integration cases.
import { deserialize, serialize } from "@dao-xyz/borsh";
import { TerminalOperationNotStartedError } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import {
	InstanceLifecycle,
	type InstanceLifecycleDeps,
} from "../src/instance-lifecycle.js";
import { EventStore } from "./utils/stores/index.js";

type StubHost = {
	lifecycle?: InstanceLifecycle;
	closeController?: AbortController;
	poison?: unknown;
	closed: boolean;
	terminating: boolean;
	checkedPrune?: object;
	rangeMutationsClosing: boolean;
	pruneRemovesClosing: boolean;
	pruneDebouncer?: object;
	replicationChangeDebouncer?: object;
	rebalanceDebouncer?: object;
};

const createHost = (): StubHost => ({
	closeController: new AbortController(),
	closed: false,
	terminating: false,
	checkedPrune: {},
	rangeMutationsClosing: false,
	pruneRemovesClosing: false,
});

const createDeps = (host: StubHost): InstanceLifecycleDeps => ({
	getCurrentLifecycle: () => host.lifecycle,
	getCloseController: () => host.closeController,
	getPoisonFailure: () => host.poison,
	isHostClosed: () => host.closed,
	isHostTerminating: () => host.terminating,
	getCheckedPruneCoordinator: () => host.checkedPrune,
	areRangeMutationsClosing: () => host.rangeMutationsClosing,
	arePruneRemovesClosing: () => host.pruneRemovesClosing,
	getPruneDebouncer: () => host.pruneDebouncer,
	getReplicationChangeDebouncer: () => host.replicationChangeDebouncer,
	getRebalanceDebouncer: () => host.rebalanceDebouncer,
});

const createCurrentLifecycle = (host: StubHost): InstanceLifecycle => {
	const lifecycle = new InstanceLifecycle(createDeps(host));
	host.lifecycle = lifecycle;
	return lifecycle;
};

describe("lifecycle instance identity", () => {
	describe("unit", () => {
		it("isActiveFor matches the legacy repair-lifecycle predicate on the full fence truth table", () => {
			for (const stale of [false, true]) {
				for (const aborted of [false, true]) {
					for (const poisoned of [false, true]) {
						for (const closed of [false, true]) {
							const host = createHost();
							const lifecycle = createCurrentLifecycle(host);
							const controller = stale
								? new AbortController()
								: lifecycle.ownershipLifecycleController;
							if (aborted) {
								controller.abort();
							}
							if (poisoned) {
								host.poison = new Error("poisoned");
							}
							host.closed = closed;
							// Legacy isRepairLifecycleActive, condition for condition
							// (the current-controller term reads the owned field — its
							// only home since stage 4).
							const legacy =
								controller === lifecycle.ownershipLifecycleController &&
								!controller.signal.aborted &&
								host.poison === undefined &&
								!host.closed;
							const label = `stale=${stale} aborted=${aborted} poisoned=${poisoned} closed=${closed}`;
							expect(lifecycle.isActiveFor(controller)).to.equal(
								legacy,
								label,
							);
							// A capture taken under this lifecycle: controller-free form
							// agrees with the legacy predicate on the current controller.
							if (!stale) {
								expect(lifecycle.isActive()).to.equal(legacy, label);
							}
						}
					}
				}
			}
		});

		it("isActiveFor rejects an absent controller", () => {
			// The legacy `host.ownership = undefined` sub-case is gone: the
			// lifecycle owns a readonly controller, so that state is
			// unrepresentable by construction (clone hosts have no lifecycle
			// at all — pinned by the integration cases below).
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			expect(lifecycle.isActiveFor(undefined)).to.be.false;
		});

		it("isMembershipActiveFor matches the legacy membership predicate including the terminating term", () => {
			for (const stale of [false, true]) {
				for (const aborted of [false, true]) {
					for (const terminating of [false, true]) {
						const host = createHost();
						const lifecycle = createCurrentLifecycle(host);
						const membership = lifecycle.beginMembership();
						const controller = stale ? new AbortController() : membership;
						if (aborted) {
							controller.abort();
						}
						host.terminating = terminating;
						// Legacy isReplicationLifecycleActive, condition for condition
						// (the current-controller term reads the owned field — its
						// only home since stage 4).
						const legacy =
							controller != null &&
							controller === lifecycle.membershipLifecycleController &&
							!controller.signal.aborted &&
							!host.terminating;
						expect(lifecycle.isMembershipActiveFor(controller)).to.equal(
							legacy,
							`stale=${stale} aborted=${aborted} terminating=${terminating}`,
						);
					}
				}
			}
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			// Pre-open lifecycles have no membership controller yet.
			expect(lifecycle.isMembershipActiveFor(undefined)).to.be.false;
			const membership = lifecycle.beginMembership();
			expect(lifecycle.isMembershipActiveFor(undefined)).to.be.false;
			// The asymmetry vs the ownership half: `closed` alone does not gate
			// membership (isTerminating() is the fence on this side).
			host.closed = true;
			expect(lifecycle.isMembershipActiveFor(membership)).to.be.true;
		});

		it("throwIfInactive throws the legacy error types and messages", () => {
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			expect(() => lifecycle.throwIfInactive()).to.not.throw();
			expect(lifecycle.capture()).to.equal(lifecycle);

			// Poison wins and carries the failure as `cause`.
			const failure = new Error("boom");
			host.poison = failure;
			try {
				lifecycle.throwIfInactive();
				expect.fail("expected throw");
			} catch (error: any) {
				expect(error).to.not.be.instanceOf(TerminalOperationNotStartedError);
				expect(error.message).to.equal(
					"Replication ownership recovery is required before further planning",
				);
				expect(error.cause).to.equal(failure);
			}

			// Aborted (unpoisoned) controller: TerminalOperationNotStartedError.
			host.poison = undefined;
			lifecycle.abortOwnership();
			try {
				lifecycle.throwIfInactive();
				expect.fail("expected throw");
			} catch (error: any) {
				expect(error).to.be.instanceOf(TerminalOperationNotStartedError);
				expect(error.message).to.equal(
					"Replication ownership lifecycle is no longer active",
				);
			}
		});

		it("capture throws once the lifecycle identity is superseded", () => {
			const host = createHost();
			const stale = createCurrentLifecycle(host);
			// Reopen installs a fresh lifecycle on the host.
			createCurrentLifecycle(host);
			expect(() => stale.capture()).to.throw(
				TerminalOperationNotStartedError,
				"Replication ownership lifecycle is no longer active",
			);
			expect(host.lifecycle!.capture()).to.equal(host.lifecycle);
		});

		it("debouncer identity uses bare equality so undefined === undefined passes", () => {
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			// The captured default parameter may legitimately be undefined while
			// the host debouncer is also still undefined.
			expect(lifecycle.isRebalanceDebouncerCurrent(undefined)).to.be.true;
			expect(lifecycle.isPruneDebouncerCurrent(undefined)).to.be.true;
			expect(lifecycle.isReplicationChangeDebouncerCurrent(undefined)).to.be
				.true;
			const fn = {};
			host.rebalanceDebouncer = fn;
			host.pruneDebouncer = fn;
			host.replicationChangeDebouncer = fn;
			expect(lifecycle.isRebalanceDebouncerCurrent(undefined)).to.be.false;
			expect(lifecycle.isRebalanceDebouncerCurrent({})).to.be.false;
			expect(lifecycle.isRebalanceDebouncerCurrent(fn)).to.be.true;
			expect(lifecycle.isPruneDebouncerCurrent(fn)).to.be.true;
			expect(lifecycle.isReplicationChangeDebouncerCurrent(fn)).to.be.true;
		});

		it("checked-prune currency requires identity, activity, and both captured identities", () => {
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			const coordinator = host.checkedPrune;
			const closeController = host.closeController;
			expect(lifecycle.isCheckedPruneCurrent(coordinator, closeController)).to
				.be.true;
			expect(lifecycle.isCheckedPruneCurrent({}, closeController)).to.be.false;
			expect(lifecycle.isCheckedPruneCurrent(coordinator, new AbortController()))
				.to.be.false;
			lifecycle.abortOwnership();
			expect(lifecycle.isCheckedPruneCurrent(coordinator, closeController)).to
				.be.false;

			// Stage-3 signature extension: an omitted closeController SKIPS that
			// term (the non-prune seats never compared it), and an explicitly
			// threaded stale controller vetoes even while the current one is live.
			const host2 = createHost();
			const lifecycle2 = createCurrentLifecycle(host2);
			expect(lifecycle2.isCheckedPruneCurrent(host2.checkedPrune)).to.be.true;
			host2.closeController = new AbortController();
			expect(lifecycle2.isCheckedPruneCurrent(host2.checkedPrune)).to.be.true;
			expect(
				lifecycle2.isCheckedPruneCurrent(
					host2.checkedPrune,
					undefined,
					new AbortController(),
				),
			).to.be.false;
			expect(
				lifecycle2.isCheckedPruneCurrent(
					host2.checkedPrune,
					undefined,
					lifecycle2.ownershipLifecycleController,
				),
			).to.be.true;
		});

		it("throwIfCheckedPruneInactive throws poison, then ownership-inactive, then checked-prune-inactive, with legacy messages", () => {
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			const coordinator = host.checkedPrune;
			const closeController = host.closeController;
			expect(() =>
				lifecycle.throwIfCheckedPruneInactive(coordinator, closeController),
			).to.not.throw();
			// closeController omitted => term skipped even when it would mismatch.
			host.closeController = new AbortController();
			expect(() =>
				lifecycle.throwIfCheckedPruneInactive(coordinator),
			).to.not.throw();
			expect(() =>
				lifecycle.throwIfCheckedPruneInactive(coordinator, closeController),
			).to.throw(
				TerminalOperationNotStartedError,
				"Checked prune lifecycle is no longer active",
			);
			expect(() => lifecycle.throwIfCheckedPruneInactive({})).to.throw(
				TerminalOperationNotStartedError,
				"Checked prune lifecycle is no longer active",
			);
			// Ownership inactivity wins over the checked-prune mismatch.
			lifecycle.abortOwnership();
			expect(() => lifecycle.throwIfCheckedPruneInactive({})).to.throw(
				TerminalOperationNotStartedError,
				"Replication ownership lifecycle is no longer active",
			);
			// Poison wins over everything and carries the failure as `cause`.
			const failure = new Error("boom");
			host.poison = failure;
			try {
				lifecycle.throwIfCheckedPruneInactive(coordinator, closeController);
				expect.fail("expected throw");
			} catch (error: any) {
				expect(error).to.not.be.instanceOf(TerminalOperationNotStartedError);
				expect(error.message).to.equal(
					"Replication ownership recovery is required before further planning",
				);
				expect(error.cause).to.equal(failure);
			}
		});

		it("terminal lane fences are exposed without being folded into isActive", () => {
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			expect(lifecycle.areRangeMutationsClosing()).to.be.false;
			expect(lifecycle.arePruneRemovesClosing()).to.be.false;
			host.rangeMutationsClosing = true;
			host.pruneRemovesClosing = true;
			expect(lifecycle.areRangeMutationsClosing()).to.be.true;
			expect(lifecycle.arePruneRemovesClosing()).to.be.true;
			// Distinct admission errors depend on this NOT gating isActive.
			expect(lifecycle.isActive()).to.be.true;
		});

		it("role sub-generation bumps and compares without touching identity", () => {
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			expect(lifecycle.roleGeneration).to.equal(0);
			expect(lifecycle.isRoleCurrent(0)).to.be.true;
			expect(lifecycle.bumpRoleGeneration()).to.equal(1);
			expect(lifecycle.isRoleCurrent(0)).to.be.false;
			expect(lifecycle.isRoleCurrent(1)).to.be.true;
			expect(lifecycle.isCurrent()).to.be.true;
			expect(lifecycle.isActive()).to.be.true;
		});

		it("phase derives from the wrapped fences with poisoned > closed > terminating precedence", () => {
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			expect(lifecycle.phase()).to.equal("opening");
			lifecycle.markOpenComplete();
			expect(lifecycle.phase()).to.equal("active");
			// markOpenComplete is idempotent and does not resurrect later phases.
			lifecycle.beginTerminal("close");
			lifecycle.markOpenComplete();
			expect(lifecycle.declaredPhase).to.equal("terminating");

			// terminating: superseded identity or aborted controller.
			const staleHost = createHost();
			const stale = createCurrentLifecycle(staleHost);
			createCurrentLifecycle(staleHost);
			expect(stale.phase()).to.equal("terminating");
			const abortedHost = createHost();
			const aborted = createCurrentLifecycle(abortedHost);
			aborted.abortOwnership();
			expect(aborted.phase()).to.equal("terminating");

			// closed beats terminating; poisoned beats both.
			abortedHost.closed = true;
			expect(aborted.phase()).to.equal("closed");
			abortedHost.poison = new Error("poisoned");
			expect(aborted.phase()).to.equal("poisoned");
		});

		it("declared transitions latch once", () => {
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			lifecycle.beginTerminal("close");
			lifecycle.beginTerminal("drop");
			expect(lifecycle.terminalReason).to.equal("close");
			expect(lifecycle.declaredPhase).to.equal("terminating");

			const cause = new Error("first");
			lifecycle.markPoisoned(cause);
			lifecycle.markPoisoned(new Error("second"));
			expect(lifecycle.poisonCause).to.equal(cause);
			expect(lifecycle.declaredPhase).to.equal("poisoned");
			// Terminal transitions after poison keep the poisoned declaration.
			lifecycle.beginTerminal("internal-close");
			expect(lifecycle.declaredPhase).to.equal("poisoned");
		});
	});

	describe("integration", () => {
		let session: TestSession;

		afterEach(async () => {
			await session?.stop();
		});

		it("reopen rotates the per-open lifecycle identity", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const log = db.log as any;
			const first = log._instanceLifecycle as InstanceLifecycle;
			expect(first).to.exist;
			expect(first.isCurrent()).to.be.true;
			expect(first.phase()).to.equal("active");

			await db.close();
			expect(first.isCurrent()).to.be.true;
			expect(first.phase()).to.equal("closed");

			const reopened = await session.peers[0].open(db);
			const second = (reopened.log as any)
				._instanceLifecycle as InstanceLifecycle;
			expect(second).to.exist;
			expect(second).to.not.equal(first);
			expect(second.isCurrent()).to.be.true;
			expect(second.phase()).to.equal("active");
			expect(first.isCurrent()).to.be.false;
			expect(first.phase()).to.equal("terminating");
		});

		it("stays predicate-equivalent with the live host before and after close", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const log = db.log as any;
			const lifecycle = log._instanceLifecycle as InstanceLifecycle;
			const controller = log._instanceLifecycle?.ownershipLifecycleController;
			expect(log.isRepairLifecycleActive(controller)).to.be.true;
			expect(lifecycle.isActiveFor(controller)).to.equal(
				log.isRepairLifecycleActive(controller),
			);
			expect(lifecycle.isActive()).to.be.true;

			await db.close();
			expect(log.isRepairLifecycleActive(controller)).to.be.false;
			expect(lifecycle.isActiveFor(controller)).to.equal(
				log.isRepairLifecycleActive(controller),
			);
			expect(lifecycle.isActive()).to.be.false;
		});

		it("fixed replacement and full unreplication bump the role sub-generation; reopen resets it", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore(), {
				args: { replicate: { factor: 1 } },
			});
			const log = db.log as any;
			const lifecycle = log._instanceLifecycle as InstanceLifecycle;
			const initial = lifecycle.roleGeneration;
			// Stage 3 deleted the legacy _localReplicationRoleGeneration accessor
			// shim: the lifecycle's counter is the only home, and isRoleCurrent
			// pins the captured-generation comparison.
			expect(lifecycle.isRoleCurrent(initial)).to.be.true;

			await db.log.replicate({ factor: 0.5 });
			expect(lifecycle.roleGeneration).to.equal(initial + 1);
			expect(lifecycle.isRoleCurrent(initial)).to.be.false;

			await db.log.unreplicate();
			expect(lifecycle.roleGeneration).to.equal(initial + 2);

			await db.close();
			// An empty fixed role takes the no-bump path through open()'s
			// role re-application (empty replacement, no unreplication, and no
			// adaptive planner that could rebalance afterwards), so the fresh
			// lifecycle's sub-generation is observable at its reset value.
			const reopened = await session.peers[0].open(db, {
				args: { replicate: [] },
			});
			const second = (reopened.log as any)
				._instanceLifecycle as InstanceLifecycle;
			expect(second).to.not.equal(lifecycle);
			expect(second.roleGeneration).to.equal(0);
			// The stale handle keeps its own sub-generation; it is not current.
			expect(lifecycle.roleGeneration).to.equal(initial + 2);
			expect(lifecycle.isCurrent()).to.be.false;
		});

		it("close on a deserialized never-opened clone resolves without a lifecycle", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);
			const clone = deserialize(serialize(store), EventStore);
			// borsh skips constructors: no lifecycle exists before open().
			expect((clone.log as any)._instanceLifecycle).to.equal(undefined);
			await clone.close();
			expect((clone.log as any)._instanceLifecycle).to.equal(undefined);
			await db.close();
		});

		it("poison aborts the ownership controller in place without rotating identity", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const log = db.log as any;
			const c0 = log._instanceLifecycle
				?.ownershipLifecycleController as AbortController;
			const l0 = log._instanceLifecycle as InstanceLifecycle;
			const m0 = log._instanceLifecycle
				?.membershipLifecycleController as AbortController;
			expect(c0.signal.aborted).to.be.false;
			log.poisonReplicationOwnership(new Error("boom"));
			// The critical abort-in-place transition: the ownership controller
			// is aborted on the SAME object, the lifecycle identity is not
			// rotated, and the membership half is untouched.
			expect(log._instanceLifecycle?.ownershipLifecycleController).to.equal(c0);
			expect(c0.signal.aborted).to.be.true;
			expect(log._instanceLifecycle).to.equal(l0);
			expect(log._instanceLifecycle?.membershipLifecycleController).to.equal(
				m0,
			);
			expect(m0.signal.aborted).to.be.false;
		});

		it("reopen rotates both controllers together with the instance identity", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const log = db.log as any;
			const c0 = log._instanceLifecycle
				?.ownershipLifecycleController as AbortController;
			const l0 = log._instanceLifecycle as InstanceLifecycle;
			const m0 = log._instanceLifecycle
				?.membershipLifecycleController as AbortController;
			// Repeated accessor reads within one open are identity-stable.
			expect(log._instanceLifecycle?.ownershipLifecycleController).to.equal(c0);
			expect(log._instanceLifecycle?.membershipLifecycleController).to.equal(
				m0,
			);

			await db.close();
			const reopened = await session.peers[0].open(db);
			const log2 = reopened.log as any;
			expect(log2._instanceLifecycle).to.not.equal(l0);
			expect(
				log2._instanceLifecycle?.ownershipLifecycleController,
			).to.not.equal(c0);
			expect(
				log2._instanceLifecycle?.membershipLifecycleController,
			).to.not.equal(m0);
			expect(log2._instanceLifecycle?.ownershipLifecycleController).to.equal(
				log2._instanceLifecycle?.ownershipLifecycleController,
			);
			expect(log2._instanceLifecycle?.membershipLifecycleController).to.equal(
				log2._instanceLifecycle?.membershipLifecycleController,
			);
		});

		it("a membership capture from a previous open stays inactive after reopen", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const log = db.log as any;
			const captured = log._instanceLifecycle
				?.membershipLifecycleController as AbortController;
			expect(log.isReplicationLifecycleActive(captured)).to.be.true;

			await db.close();
			const reopened = await session.peers[0].open(db);
			const log2 = reopened.log as any;
			expect(log2.isReplicationLifecycleActive(captured)).to.be.false;
			expect(
				(log2._instanceLifecycle as InstanceLifecycle).isMembershipActiveFor(
					captured,
				),
			).to.be.false;
		});

		it("startRepairLifecycle invalidates prior ownership captures and leaves a fresh active controller", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const log = db.log as any;
			const c0 = log._instanceLifecycle
				?.ownershipLifecycleController as AbortController;
			const l0 = log._instanceLifecycle;
			expect(log.isRepairLifecycleActive(c0)).to.be.true;
			log.startRepairLifecycle();
			expect(log._instanceLifecycle?.ownershipLifecycleController).to.not.equal(
				c0,
			);
			expect(log.isRepairLifecycleActive(c0)).to.be.false;
			// The load-bearing predecessor abort: stale-captured-lifecycle
			// predicates rely on non-current => aborted, so the rotation must
			// abort the outgoing controller, not merely supersede it.
			expect(c0.signal.aborted).to.be.true;
			expect(l0.isActiveFor(c0)).to.be.false;
			expect(
				log.isRepairLifecycleActive(
					log._instanceLifecycle?.ownershipLifecycleController,
				),
			).to.be.true;
		});

		it("a never-opened deserialized clone exposes no controllers and closes cleanly", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);
			const clone = deserialize(serialize(store), EventStore);
			const cloneLog = clone.log as any;
			expect(
				cloneLog._instanceLifecycle?.ownershipLifecycleController,
			).to.equal(undefined);
			expect(
				cloneLog._instanceLifecycle?.membershipLifecycleController,
			).to.equal(undefined);
			// The legacy TypeError a capture attempt raises on such a clone
			// (reading `.signal` of the undefined controller).
			expect(() => cloneLog.captureReplicationOwnershipLifecycle()).to.throw(
				TypeError,
			);
			await clone.close();
			expect(
				cloneLog._instanceLifecycle?.ownershipLifecycleController,
			).to.equal(undefined);
			expect(
				cloneLog._instanceLifecycle?.membershipLifecycleController,
			).to.equal(undefined);
			await db.close();
		});
	});
});
