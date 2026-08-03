// Stage-2 lifecycle refactor guard: InstanceLifecycle wraps the existing
// fences via late-bound readers, so the pure-unit half asserts truth-table
// equivalence with the legacy predicates it folds, and the integration half
// asserts the per-open rotation/identity semantics against a real SharedLog.
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
	ownership?: AbortController;
	membership?: AbortController;
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
	ownership: new AbortController(),
	membership: new AbortController(),
	closeController: new AbortController(),
	closed: false,
	terminating: false,
	checkedPrune: {},
	rangeMutationsClosing: false,
	pruneRemovesClosing: false,
});

const createDeps = (host: StubHost): InstanceLifecycleDeps => ({
	getCurrentLifecycle: () => host.lifecycle,
	getOwnershipController: () => host.ownership,
	getMembershipController: () => host.membership,
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
								: host.ownership!;
							if (aborted) {
								controller.abort();
							}
							if (poisoned) {
								host.poison = new Error("poisoned");
							}
							host.closed = closed;
							// Legacy isRepairLifecycleActive, condition for condition.
							const legacy =
								controller === host.ownership &&
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
			const host = createHost();
			const lifecycle = createCurrentLifecycle(host);
			expect(lifecycle.isActiveFor(undefined)).to.be.false;
			host.ownership = undefined;
			expect(lifecycle.isActive()).to.be.false;
		});

		it("isMembershipActiveFor matches the legacy membership predicate including the terminating term", () => {
			for (const stale of [false, true]) {
				for (const aborted of [false, true]) {
					for (const terminating of [false, true]) {
						const host = createHost();
						const lifecycle = createCurrentLifecycle(host);
						const controller = stale
							? new AbortController()
							: host.membership!;
						if (aborted) {
							controller.abort();
						}
						host.terminating = terminating;
						// Legacy isReplicationLifecycleActive, condition for condition.
						const legacy =
							controller != null &&
							controller === host.membership &&
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
			expect(lifecycle.isMembershipActiveFor(undefined)).to.be.false;
			// The asymmetry vs the ownership half: `closed` alone does not gate
			// membership (isTerminating() is the fence on this side).
			host.closed = true;
			expect(lifecycle.isMembershipActiveFor(host.membership)).to.be.true;
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
			host.ownership!.abort();
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
			host.ownership!.abort();
			expect(lifecycle.isCheckedPruneCurrent(coordinator, closeController)).to
				.be.false;
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

			// terminating: superseded identity or aborted/absent controller.
			const staleHost = createHost();
			const stale = createCurrentLifecycle(staleHost);
			createCurrentLifecycle(staleHost);
			expect(stale.phase()).to.equal("terminating");
			const abortedHost = createHost();
			const aborted = createCurrentLifecycle(abortedHost);
			abortedHost.ownership!.abort();
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
			const controller = log._repairLifecycleController;
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
			// Legacy accessor and the physically-moved field stay in lockstep.
			expect(log._localReplicationRoleGeneration).to.equal(initial);

			await db.log.replicate({ factor: 0.5 });
			expect(lifecycle.roleGeneration).to.equal(initial + 1);
			expect(log._localReplicationRoleGeneration).to.equal(initial + 1);

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
	});
});
