import { Cache } from "@peerbit/cache";
import { expect } from "chai";
import sinon from "sinon";
import { SharedLog } from "../src/index.js";
import {
	RequestMaybeSyncCoordinate,
	SimpleSyncronizer,
} from "../src/sync/simple.js";

const emptyLog = {
	has: async () => false,
	hasMany: async () => new Set<string>(),
};

describe("sync-repair-session", () => {
	it("coalesces join warmup retries behind one active simple send", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let releaseFirstSimple: (() => void) | undefined;
		try {
			const log = new SharedLog<unknown>();
			log.closed = false;
			const internals = log as any;
			internals._assumeSyncedRepairSuppressedUntil = 0;
			internals._repairMetrics = {
				"join-warmup": {
					dispatches: 0,
					entries: 0,
					ratelessFirstPasses: 0,
					simpleFallbackPasses: 0,
				},
			};
			let activeSimpleSends = 0;
			let maxActiveSimpleSends = 0;
			const simpleEntryBatches: string[][] = [];
			const send = sinon
				.stub(internals, "sendRepairEntriesWithTransport")
				.callsFake(async (...args: unknown[]) => {
					const transport = args[2] as string;
					if (transport !== "simple") {
						return;
					}
					const entries = args[1] as Map<string, unknown>;
					activeSimpleSends += 1;
					maxActiveSimpleSends = Math.max(
						maxActiveSimpleSends,
						activeSimpleSends,
					);
					simpleEntryBatches.push([...entries.keys()]);
					try {
						if (simpleEntryBatches.length === 1) {
							await new Promise<void>((resolve) => {
								releaseFirstSimple = resolve;
							});
						}
					} finally {
						activeSimpleSends -= 1;
					}
				});

			for (const hash of ["first", "second"]) {
				internals.dispatchMaybeMissingEntries(
					"target",
					new Map([[hash, { hash }]]),
					{
						bypassRecentDedupe: true,
						mode: "join-warmup",
						retryScheduleMs: [0, 10, 20, 30],
					},
				);
			}
			const scheduled =
				internals._joinWarmupScheduledRetriesByTarget.get("target");
			expect(scheduled.slotsByDelay.size).to.equal(3);
			for (const slot of scheduled.slotsByDelay.values()) {
				expect(slot.cohorts).to.have.length(1);
				expect(slot.cohorts[0].batches).to.have.length(2);
				expect(
					slot.cohorts[0].batches.reduce(
						(total: number, batch: any) => total + batch.entries.size,
						0,
					),
				).to.equal(2);
			}
			expect(
				internals._joinWarmupRetryTimersByTarget.get("target").size,
			).to.equal(3);
			await clock.tickAsync(30);

			expect(
				send.getCalls().filter((call) => call.args[2] === "rateless"),
			).to.have.length(2);
			expect(simpleEntryBatches).to.have.length(1);
			expect(maxActiveSimpleSends).to.equal(1);
			const blockedState = internals._joinWarmupSendStateByTarget.get("target");
			expect(blockedState.pending).to.be.true;
			expect([...blockedState.entries.keys()]).to.have.members([
				"first",
				"second",
			]);
			expect(internals._joinWarmupRetryTimersByTarget.size).to.equal(0);

			expect(releaseFirstSimple).to.be.a("function");
			releaseFirstSimple!();
			await clock.tickAsync(0);
			await clock.tickAsync(249);
			expect(simpleEntryBatches).to.have.length(1);
			await clock.tickAsync(1);

			expect(simpleEntryBatches).to.have.length(2);
			expect(simpleEntryBatches[1]).to.have.members(["first", "second"]);
			expect(maxActiveSimpleSends).to.equal(1);
			const idleState = internals._joinWarmupSendStateByTarget.get("target");
			expect(idleState.running).to.be.false;
			expect(idleState.pending).to.be.false;
			expect(idleState.entries.size).to.equal(0);
			expect(internals._repairRetryTimers.size).to.equal(0);
			expect(internals._repairMetrics["join-warmup"]).to.deep.equal({
				dispatches: 2,
				entries: 2,
				ratelessFirstPasses: 2,
				simpleFallbackPasses: 2,
			});
		} finally {
			releaseFirstSimple?.();
			await clock.tickAsync(1_000);
			clock.restore();
		}
	});

	it("serializes join warmup sends per target without globalizing the lane", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		const releaseFirstSimpleByTarget = new Map<string, () => void>();
		try {
			const log = new SharedLog<unknown>();
			log.closed = false;
			const internals = log as any;
			internals._assumeSyncedRepairSuppressedUntil = 0;
			internals._repairMetrics = {
				"join-warmup": {
					dispatches: 0,
					entries: 0,
					ratelessFirstPasses: 0,
					simpleFallbackPasses: 0,
				},
			};
			let activeSimpleSends = 0;
			let maxActiveSimpleSends = 0;
			const activeSimpleSendsByTarget = new Map<string, number>();
			const maxActiveSimpleSendsByTarget = new Map<string, number>();
			const simpleCallsByTarget = new Map<string, number>();
			sinon
				.stub(internals, "sendRepairEntriesWithTransport")
				.callsFake(async (...args: unknown[]) => {
					if (args[2] !== "simple") {
						return;
					}
					const target = args[0] as string;
					const callNumber = (simpleCallsByTarget.get(target) ?? 0) + 1;
					simpleCallsByTarget.set(target, callNumber);
					activeSimpleSends += 1;
					maxActiveSimpleSends = Math.max(
						maxActiveSimpleSends,
						activeSimpleSends,
					);
					const activeForTarget =
						(activeSimpleSendsByTarget.get(target) ?? 0) + 1;
					activeSimpleSendsByTarget.set(target, activeForTarget);
					maxActiveSimpleSendsByTarget.set(
						target,
						Math.max(
							maxActiveSimpleSendsByTarget.get(target) ?? 0,
							activeForTarget,
						),
					);
					try {
						if (callNumber === 1) {
							await new Promise<void>((resolve) => {
								releaseFirstSimpleByTarget.set(target, resolve);
							});
						}
					} finally {
						activeSimpleSends -= 1;
						activeSimpleSendsByTarget.set(target, activeForTarget - 1);
					}
				});

			for (const target of ["target-a", "target-b"]) {
				internals.dispatchMaybeMissingEntries(
					target,
					new Map([[`${target}-entry`, { hash: `${target}-entry` }]]),
					{
						bypassRecentDedupe: true,
						mode: "join-warmup",
						retryScheduleMs: [0, 10, 20],
					},
				);
			}
			await clock.tickAsync(20);

			expect([...simpleCallsByTarget.entries()]).to.have.deep.members([
				["target-a", 1],
				["target-b", 1],
			]);
			expect(maxActiveSimpleSends).to.equal(2);
			expect([...maxActiveSimpleSendsByTarget.entries()]).to.have.deep.members([
				["target-a", 1],
				["target-b", 1],
			]);
			expect(internals._joinWarmupSendStateByTarget.get("target-a").pending).to
				.be.true;
			expect(internals._joinWarmupSendStateByTarget.get("target-b").pending).to
				.be.true;

			for (const release of releaseFirstSimpleByTarget.values()) {
				release();
			}
			await clock.tickAsync(0);
			await clock.tickAsync(249);
			expect([...simpleCallsByTarget.values()]).to.deep.equal([1, 1]);
			await clock.tickAsync(1);

			expect([...simpleCallsByTarget.entries()]).to.have.deep.members([
				["target-a", 2],
				["target-b", 2],
			]);
			expect(maxActiveSimpleSends).to.equal(2);
			expect([...maxActiveSimpleSendsByTarget.entries()]).to.have.deep.members([
				["target-a", 1],
				["target-b", 1],
			]);
			expect(internals._repairRetryTimers.size).to.equal(0);
		} finally {
			for (const release of releaseFirstSimpleByTarget.values()) {
				release();
			}
			await clock.tickAsync(1_000);
			clock.restore();
		}
	});

	it("rechecks peer knowledge before a coalesced warmup reaches transport", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let releaseFirstSimple: (() => void) | undefined;
		try {
			const log = new SharedLog<unknown>();
			log.closed = false;
			const internals = log as any;
			internals._assumeSyncedRepairSuppressedUntil = 0;
			internals._repairMetrics = {
				"join-warmup": {
					dispatches: 0,
					entries: 0,
					ratelessFirstPasses: 0,
					simpleFallbackPasses: 0,
				},
			};
			internals._entryKnownPeerObservedAt = new Map();
			const sendRepairEntriesWithTransport =
				internals.sendRepairEntriesWithTransport.bind(internals);
			sinon
				.stub(internals, "sendRepairEntriesWithTransport")
				.callsFake(async (...args: unknown[]) => {
					if (args[2] !== "simple") {
						return;
					}
					await sendRepairEntriesWithTransport(
						args[0],
						args[1],
						args[2],
						args[3],
					);
				});
			const pushedEntryBatches: string[][] = [];
			sinon
				.stub(internals, "pushRepairEntries")
				.callsFake(async (...args: unknown[]) => {
					pushedEntryBatches.push([
						...(args[1] as Map<string, unknown>).keys(),
					]);
					if (pushedEntryBatches.length === 1) {
						await new Promise<void>((resolve) => {
							releaseFirstSimple = resolve;
						});
					}
				});

			internals.dispatchMaybeMissingEntries(
				"target",
				new Map([
					["still-missing", { hash: "still-missing" }],
					["newly-known", { hash: "newly-known" }],
				]),
				{
					bypassRecentDedupe: true,
					mode: "join-warmup",
					retryScheduleMs: [0, 10, 20],
				},
			);
			await clock.tickAsync(20);

			expect(pushedEntryBatches).to.deep.equal([
				["still-missing", "newly-known"],
			]);
			expect(internals._joinWarmupSendStateByTarget.get("target").pending).to.be
				.true;
			internals.markEntriesKnownByPeer(["newly-known"], "target");

			expect(releaseFirstSimple).to.be.a("function");
			releaseFirstSimple!();
			await clock.tickAsync(0);
			await clock.tickAsync(249);
			expect(pushedEntryBatches).to.have.length(1);
			await clock.tickAsync(1);

			expect(pushedEntryBatches).to.deep.equal([
				["still-missing", "newly-known"],
				["still-missing"],
			]);
			expect(internals._repairRetryTimers.size).to.equal(0);
		} finally {
			releaseFirstSimple?.();
			await clock.tickAsync(1_000);
			clock.restore();
		}
	});

	it("releases each warmup delay slot without retaining earlier scan batches", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		try {
			const log = new SharedLog<unknown>();
			log.closed = false;
			const internals = log as any;
			internals._assumeSyncedRepairSuppressedUntil = 0;
			internals._repairMetrics = {
				"join-warmup": {
					dispatches: 0,
					entries: 0,
					ratelessFirstPasses: 0,
					simpleFallbackPasses: 0,
				},
			};
			sinon.stub(internals, "sendRepairEntriesWithTransport").resolves();
			const queued: string[][] = [];
			sinon
				.stub(internals, "queueJoinWarmupSend")
				.callsFake((...args: unknown[]) => {
					queued.push([...(args[2] as Map<string, unknown>).keys()]);
				});

			internals.dispatchMaybeMissingEntries(
				"target",
				new Map([["early", { hash: "early" }]]),
				{
					bypassRecentDedupe: true,
					mode: "join-warmup",
					retryScheduleMs: [0, 10],
				},
			);
			await clock.tickAsync(9);
			internals.dispatchMaybeMissingEntries(
				"target",
				new Map([["late", { hash: "late" }]]),
				{
					bypassRecentDedupe: true,
					mode: "join-warmup",
					retryScheduleMs: [0, 10],
				},
			);
			expect(
				internals._joinWarmupRetryTimersByTarget.get("target").size,
			).to.equal(1);

			await clock.tickAsync(1);
			expect(queued).to.deep.equal([["early"]]);
			const slot = internals._joinWarmupScheduledRetriesByTarget
				.get("target")
				.slotsByDelay.get(10);
			expect(
				slot.cohorts[slot.head].batches.flatMap((batch: any) => [
					...batch.entries.keys(),
				]),
			).to.deep.equal(["late"]);
			expect(
				internals._joinWarmupRetryTimersByTarget.get("target").size,
			).to.equal(1);

			await clock.tickAsync(9);
			expect(queued).to.deep.equal([["early"], ["late"]]);
			expect(internals._joinWarmupScheduledRetriesByTarget.size).to.equal(0);
			expect(internals._joinWarmupRetryTimersByTarget.size).to.equal(0);
			expect(internals._repairRetryTimers.size).to.equal(0);
		} finally {
			await clock.tickAsync(1_000);
			clock.restore();
		}
	});

	it("keeps a reconnected warmup generation behind its active predecessor", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let releaseOldSimple: (() => void) | undefined;
		try {
			const log = new SharedLog<unknown>();
			log.closed = false;
			const internals = log as any;
			internals._assumeSyncedRepairSuppressedUntil = 0;
			internals._repairMetrics = {
				"join-warmup": {
					dispatches: 0,
					entries: 0,
					ratelessFirstPasses: 0,
					simpleFallbackPasses: 0,
				},
			};
			let activeSimpleSends = 0;
			let maxActiveSimpleSends = 0;
			const simpleEntryBatches: string[][] = [];
			sinon
				.stub(internals, "sendRepairEntriesWithTransport")
				.callsFake(async (...args: unknown[]) => {
					if (args[2] !== "simple") {
						return;
					}
					const entries = args[1] as Map<string, unknown>;
					activeSimpleSends += 1;
					maxActiveSimpleSends = Math.max(
						maxActiveSimpleSends,
						activeSimpleSends,
					);
					simpleEntryBatches.push([...entries.keys()]);
					try {
						if (simpleEntryBatches.length === 1) {
							await new Promise<void>((resolve) => {
								releaseOldSimple = resolve;
							});
						}
					} finally {
						activeSimpleSends -= 1;
					}
				});

			internals.dispatchMaybeMissingEntries(
				"target",
				new Map([["old", { hash: "old" }]]),
				{
					bypassRecentDedupe: true,
					mode: "join-warmup",
					retryScheduleMs: [0, 10, 20, 30],
				},
			);
			await clock.tickAsync(10);
			expect(simpleEntryBatches).to.deep.equal([["old"]]);

			const disconnectedGeneration =
				internals._joinWarmupGenerationByTarget.get("target");
			internals.removeRepairFrontierTarget("target");
			expect(internals._joinWarmupRetryTimersByTarget.size).to.equal(0);
			internals.dispatchMaybeMissingEntries(
				"target",
				new Map([["new", { hash: "new" }]]),
				{
					bypassRecentDedupe: true,
					mode: "join-warmup",
					retryScheduleMs: [0, 10],
				},
			);
			await clock.tickAsync(10);
			const reconnectedGeneration =
				internals._joinWarmupGenerationByTarget.get("target");
			internals.removeRepairFrontierTarget("target", {
				expectedJoinWarmupGeneration: disconnectedGeneration,
			});

			expect(simpleEntryBatches).to.deep.equal([["old"]]);
			expect(maxActiveSimpleSends).to.equal(1);
			expect(internals._joinWarmupGenerationByTarget.get("target")).to.equal(
				reconnectedGeneration,
			);
			expect([
				...internals._joinWarmupSendStateByTarget.get("target").entries.keys(),
			]).to.deep.equal(["new"]);

			expect(releaseOldSimple).to.be.a("function");
			releaseOldSimple!();
			await clock.tickAsync(0);
			await clock.tickAsync(249);
			expect(simpleEntryBatches).to.have.length(1);
			await clock.tickAsync(1);

			expect(simpleEntryBatches).to.deep.equal([["old"], ["new"]]);
			expect(maxActiveSimpleSends).to.equal(1);
			expect(internals._repairRetryTimers.size).to.equal(0);
			expect(internals._repairMetrics["join-warmup"]).to.deep.equal({
				dispatches: 2,
				entries: 2,
				ratelessFirstPasses: 2,
				simpleFallbackPasses: 2,
			});
		} finally {
			releaseOldSimple?.();
			await clock.tickAsync(1_000);
			clock.restore();
		}
	});

	it("continues a coalesced warmup after a simple send rejects", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let rejectFirstSimple: ((error: Error) => void) | undefined;
		try {
			const log = new SharedLog<unknown>();
			log.closed = false;
			const internals = log as any;
			internals._assumeSyncedRepairSuppressedUntil = 0;
			internals._repairMetrics = {
				"join-warmup": {
					dispatches: 0,
					entries: 0,
					ratelessFirstPasses: 0,
					simpleFallbackPasses: 0,
				},
			};
			const simpleEntryBatches: string[][] = [];
			sinon
				.stub(internals, "sendRepairEntriesWithTransport")
				.callsFake(async (...args: unknown[]) => {
					if (args[2] !== "simple") {
						return;
					}
					simpleEntryBatches.push([
						...(args[1] as Map<string, unknown>).keys(),
					]);
					if (simpleEntryBatches.length === 1) {
						await new Promise<void>((_resolve, reject) => {
							rejectFirstSimple = reject;
						});
					}
				});

			internals.dispatchMaybeMissingEntries(
				"target",
				new Map([["entry", { hash: "entry" }]]),
				{
					bypassRecentDedupe: true,
					mode: "join-warmup",
					retryScheduleMs: [0, 10, 20],
				},
			);
			await clock.tickAsync(20);
			expect(simpleEntryBatches).to.have.length(1);
			expect(internals._joinWarmupSendStateByTarget.get("target").pending).to.be
				.true;
			expect(rejectFirstSimple).to.be.a("function");
			rejectFirstSimple!(new Error("injected warmup send failure"));
			await clock.tickAsync(0);
			await clock.tickAsync(249);
			expect(simpleEntryBatches).to.have.length(1);
			await clock.tickAsync(1);

			expect(simpleEntryBatches).to.deep.equal([["entry"], ["entry"]]);
			expect(
				internals._repairMetrics["join-warmup"].simpleFallbackPasses,
			).to.equal(2);
			expect(internals._joinWarmupSendStateByTarget.get("target").running).to.be
				.false;
			expect(internals._repairRetryTimers.size).to.equal(0);
		} finally {
			await clock.tickAsync(1_000);
			clock.restore();
		}
	});

	it("retains the bounded default join warmup retry window", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		try {
			const log = new SharedLog<unknown>();
			log.closed = false;
			const internals = log as any;
			internals._assumeSyncedRepairSuppressedUntil = 0;
			internals._repairMetrics = {
				"join-warmup": {
					dispatches: 0,
					entries: 0,
					ratelessFirstPasses: 0,
					simpleFallbackPasses: 0,
				},
			};
			const send = sinon
				.stub(internals, "sendRepairEntriesWithTransport")
				.resolves();

			internals.dispatchMaybeMissingEntries(
				"target",
				new Map([["entry", { hash: "entry" }]]),
				{
					bypassRecentDedupe: true,
					mode: "join-warmup",
				},
			);
			expect(
				internals._joinWarmupScheduledRetriesByTarget.get("target").slotsByDelay
					.size,
			).to.equal(6);
			await clock.tickAsync(60_000);

			expect(
				send.getCalls().filter((call) => call.args[2] === "rateless"),
			).to.have.length(1);
			expect(
				send.getCalls().filter((call) => call.args[2] === "simple"),
			).to.have.length(6);
			expect(internals._joinWarmupRetryTimersByTarget.size).to.equal(0);
			expect(internals._repairRetryTimers.size).to.equal(0);

			await clock.tickAsync(60_001);
			expect(send.callCount).to.equal(7);
		} finally {
			await clock.tickAsync(1_000);
			clock.restore();
		}
	});

	it("does not let a stale warmup sweep adopt a reconnected target", () => {
		const log = new SharedLog<unknown>();
		log.closed = false;
		const internals = log as any;
		internals._repairSweepRunning = true;
		const oldGeneration = internals.getJoinWarmupGeneration("target");
		internals.cancelJoinWarmupTarget("target");
		const currentGeneration = internals.getJoinWarmupGeneration("target");

		internals.scheduleRepairSweep({
			joinWarmupGenerations: new Map([["target", oldGeneration]]),
			mode: "join-warmup",
			peers: ["target"],
		});
		expect(
			internals._repairSweepPendingPeersByMode.get("join-warmup").has("target"),
		).to.be.false;
		expect(internals._repairSweepPendingModes.has("join-warmup")).to.be.false;

		internals.scheduleRepairSweep({
			joinWarmupGenerations: new Map([["target", currentGeneration]]),
			mode: "join-warmup",
			peers: ["target"],
		});
		expect(
			internals._repairSweepPendingPeersByMode.get("join-warmup").has("target"),
		).to.be.true;
		expect(
			internals._repairSweepJoinWarmupGenerationByTarget.get("target"),
		).to.equal(currentGeneration);

		internals.cancelJoinWarmupTarget("target");
		expect(internals._repairSweepPendingModes.has("join-warmup")).to.be.false;
	});

	it("stops an in-flight warmup sweep when its target reconnects", async () => {
		const log = new SharedLog<unknown>();
		log.closed = false;
		const internals = log as any;
		internals.node = {
			identity: { publicKey: { hashcode: () => "self" } },
		};
		internals._nativeSharedLogState = {};
		internals._residentEntryCoordinatesByHash = new Map([
			["entry", { hash: "entry" }],
		]);
		internals.repairSweepTargetBufferSize = 100;
		sinon.stub(internals, "hasCustomFindLeaders").returns(false);
		let releasePlan!: () => void;
		let markPlanEntered!: () => void;
		const planEntered = new Promise<void>((resolve) => {
			markPlanEntered = resolve;
		});
		sinon
			.stub(internals, "planResidentRepairDispatchBatch")
			.callsFake(async () => {
				markPlanEntered();
				await new Promise<void>((resolve) => {
					releasePlan = resolve;
				});
				return new Map([
					["join-warmup", new Map([["target", new Set(["entry"])]])],
				]);
			});
		sinon
			.stub(internals, "getFullReplicaRepairCandidates")
			.resolves(new Set(["self", "target"]));
		const dispatch = sinon.stub(internals, "dispatchMaybeMissingEntries");
		const oldGeneration = internals.getJoinWarmupGeneration("target");
		internals.markRepairSweepOptimisticPeer("gid", "target", oldGeneration);
		internals._repairSweepPendingModes.add("join-warmup");
		internals._repairSweepPendingPeersByMode.get("join-warmup").add("target");
		internals._repairSweepJoinWarmupGenerationByTarget.set(
			"target",
			oldGeneration,
		);
		internals._repairSweepRunning = true;

		const running = internals.runRepairSweep();
		await planEntered;
		internals.cancelJoinWarmupTarget("target");
		const newGeneration = internals.getJoinWarmupGeneration("target");
		internals.markRepairSweepOptimisticPeer("gid", "target", newGeneration);
		releasePlan();
		await running;

		expect(dispatch.called).to.be.false;
		expect(internals._joinWarmupGenerationByTarget.get("target")).to.equal(
			newGeneration,
		);
		expect(
			internals._repairSweepOptimisticGidPeersPending.get("gid").get("target"),
		).to.deep.equal({ count: 1, generation: newGeneration });
		expect(
			internals._repairSweepOptimisticGidsByPeer.get("target"),
		).to.deep.equal(new Set(["gid"]));
		expect(internals._repairSweepRunning).to.be.false;
	});

	it("captures warmup generation before an async rebalance starts", async () => {
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let releaseTrim: (() => void) | undefined;
		try {
			const log = new SharedLog<unknown>();
			log.closed = false;
			const internals = log as any;
			internals.node = {
				identity: { publicKey: { hashcode: () => "self" } },
			};
			internals._entryCoordinatesIndex = {
				iterate: () => ({
					close: async () => {},
					done: () => true,
					next: async () => [],
				}),
			};
			internals.repairSweepTargetBufferSize = 100;
			let markTrimEntered!: () => void;
			const trimEntered = new Promise<void>((resolve) => {
				markTrimEntered = resolve;
			});
			sinon.stub(log.log, "trim").callsFake(async () => {
				markTrimEntered();
				await new Promise<void>((resolve) => {
					releaseTrim = resolve;
				});
				return undefined;
			});
			sinon.stub(internals, "scheduleJoinAuthoritativeRepair");
			const changing = internals.onReplicationChange([
				{
					range: {
						end1: 1n,
						end2: 0n,
						hash: "target",
						idString: "id",
						mode: 1,
						rangeHash: "range",
						start1: 0n,
						start2: 0n,
						timestamp: 0n,
					},
					timestamp: 0n,
					type: "added",
				},
			]);

			await trimEntered;
			const oldGeneration =
				internals._joinWarmupGenerationByTarget.get("target");
			expect(oldGeneration).to.be.an("object");
			internals.cancelJoinWarmupTarget("target");
			const newGeneration = internals.getJoinWarmupGeneration("target");
			releaseTrim!();
			await changing;
			await clock.tickAsync(250);

			expect(internals._joinWarmupGenerationByTarget.get("target")).to.equal(
				newGeneration,
			);
			expect(
				internals._repairSweepPendingPeersByMode
					.get("join-warmup")
					.has("target"),
			).to.be.false;
		} finally {
			releaseTrim?.();
			await clock.tickAsync(1_000);
			clock.restore();
		}
	});

	it("deduplicates known hash aliases without changing coordinate requests", async () => {
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 10 });
		coordinateToHash.add(42n, "entry-hash");
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: emptyLog as any,
			coordinateToHash,
		});
		const from = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;

		await sync.queueSync([42n, "entry-hash"], from);

		expect(sync.syncInFlightQueue.has(42n)).to.equal(true);
		expect(sync.syncInFlightQueue.has("entry-hash")).to.equal(false);
		expect(sync.syncInFlightQueueInverted.get("p1")).to.deep.equal(
			new Set([42n]),
		);
		expect(send.calledOnce).to.equal(true);
	});

	it("deduplicates hash sync requests with already queued coordinate aliases", async () => {
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 10 });
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: emptyLog as any,
			coordinateToHash,
		});
		const p1 = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;
		const p2 = {
			hashcode: () => "p2",
			equals: () => false,
		} as any;

		await sync.queueSync([42n], p1);
		coordinateToHash.add(42n, "entry-hash");
		await sync.queueSync(["entry-hash"], p2);

		expect(sync.syncInFlightQueue.has(42n)).to.equal(true);
		expect(sync.syncInFlightQueue.has("entry-hash")).to.equal(false);
		expect(
			sync.syncInFlightQueue.get(42n)?.map((x) => x.hashcode()),
		).to.deep.equal(["p1", "p2"]);
		expect(sync.syncInFlightQueueInverted.get("p2")).to.deep.equal(
			new Set([42n]),
		);
		expect(send.calledTwice).to.equal(true);
		expect(send.secondCall.args[0]).to.be.instanceOf(
			RequestMaybeSyncCoordinate,
		);
		expect(send.secondCall.args[0].hashNumbers).to.deep.equal([42n]);
		expect(send.secondCall.args[1].mode.to).to.deep.equal(["p2"]);
	});

	it("clears in-flight coordinate aliases when an entry is received by hash", () => {
		const coordinateToHash = new Cache<string>({ max: 10 });
		coordinateToHash.add(42n, "entry-hash");
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: emptyLog as any,
			coordinateToHash,
		});
		const from = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;

		sync.syncInFlightQueue.set(42n, [from]);
		sync.syncInFlightQueueInverted.set("p1", new Set([42n]));
		sync.syncInFlight.set("p1", new Map([[42n, { timestamp: Date.now() }]]));

		sync.onReceivedEntries({
			entries: [{ entry: { hash: "entry-hash" } }] as any,
			from,
		});

		expect(sync.syncInFlightQueue.has(42n)).to.equal(true);
		expect(sync.syncInFlightQueueInverted.has("p1")).to.equal(true);
		expect(sync.syncInFlight.has("p1")).to.equal(false);
	});

	it("clears pending coordinate aliases when an entry is added by hash", () => {
		const coordinateToHash = new Cache<string>({ max: 10 });
		coordinateToHash.add(42n, "entry-hash");
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: emptyLog as any,
			coordinateToHash,
		});
		const from = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;

		sync.syncInFlightQueue.set(42n, [from]);
		sync.syncInFlightQueueInverted.set("p1", new Set([42n]));
		sync.syncInFlight.set("p1", new Map([[42n, { timestamp: Date.now() }]]));

		sync.onEntryAdded({ hash: "entry-hash" } as any);

		expect(sync.syncInFlightQueue.has(42n)).to.equal(false);
		expect(sync.syncInFlightQueueInverted.has("p1")).to.equal(false);
		expect(sync.syncInFlight.has("p1")).to.equal(false);
	});

	it("resolves convergent session when missing entries are received", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const known = new Set<string>();

		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {
				count: async ({ query }: any) => {
					const value = query?.hashNumber;
					return known.has(String(value)) ? 1 : 0;
				},
			} as any,
			log: {
				has: async (hash: string) => known.has(hash),
				hasMany: async (hashes: Iterable<string>) =>
					new Set([...hashes].filter((hash) => known.has(hash))),
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		const entries = new Map<string, any>();
		entries.set("a", { hash: "a" });
		entries.set("b", { hash: "b" });

		const session = sync.startRepairSession({
			entries: entries as any,
			targets: ["p1"],
			mode: "convergent",
			timeoutMs: 2_000,
			retryIntervalsMs: [0],
		});

		const from = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;
		known.add("a");
		known.add("b");
		sync.onReceivedEntryHashes({
			hashes: ["a", "b"],
			from,
		});

		const result = await session.done;
		expect(result).to.have.length(1);
		expect(result[0]!.completed).to.equal(true);
		expect(result[0]!.unresolved).to.deep.equal([]);
		expect(result[0]!.resolved).to.equal(2);
	});

	it("returns unresolved entries when convergent session times out", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;

		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: emptyLog as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		const entries = new Map<string, any>();
		entries.set("a", { hash: "a" });

		const session = sync.startRepairSession({
			entries: entries as any,
			targets: ["p1"],
			mode: "convergent",
			timeoutMs: 120,
			retryIntervalsMs: [0],
		});

		const result = await session.done;
		expect(send.called).to.equal(true);
		expect(result).to.have.length(1);
		expect(result[0]!.completed).to.equal(false);
		expect(result[0]!.unresolved).to.deep.equal(["a"]);
		expect(result[0]!.attempts).to.be.greaterThan(0);
	});

	it("caps tracked hashes for large convergent sessions", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;

		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: emptyLog as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxConvergentTrackedHashes: 0.5,
			},
		});

		const entries = new Map<string, any>();
		entries.set("a", { hash: "a" });
		entries.set("b", { hash: "b" });
		entries.set("c", { hash: "c" });

		const session = sync.startRepairSession({
			entries: entries as any,
			targets: ["p1"],
			mode: "convergent",
			timeoutMs: 120,
			retryIntervalsMs: [0],
		});

		const result = await session.done;
		expect(send.called).to.equal(true);
		expect(result).to.have.length(1);
		expect(result[0]!.requested).to.equal(1);
		expect(result[0]!.requestedTotal).to.equal(3);
		expect(result[0]!.truncated).to.equal(true);
		expect(result[0]!.unresolved).to.have.length(1);
	});
});
