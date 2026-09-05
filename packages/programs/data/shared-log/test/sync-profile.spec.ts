import { expect } from "chai";
import sinon from "sinon";
import {
	ExchangeHeadsMessage,
	RawExchangeHeadsMessage,
	SharedLog,
	type SyncProfileEvent,
	type SyncProfileFn,
} from "../src/index.js";

describe("advisory sync profiling", () => {
	const methods = SharedLog.prototype as any;
	afterEach(() => sinon.restore());

	const adaptiveHost = (profile?: SyncProfileFn) => {
		const controller = new AbortController();
		const debouncer = { call: sinon.spy() };
		const host = {
			_logProperties: { sync: { profile } },
			_isAdaptiveReplicating: true,
			_isReplicating: true,
			closed: false,
			_instanceLifecycle: {
				roleGeneration: 1,
				isActiveFor: () => true,
				isRebalanceDebouncerCurrent: () => true,
				isRoleCurrent: () => true,
			},
			_lastLocalAppendAt: 0,
			adaptiveRebalanceIdleMs: 10_000,
			shouldDelayAdaptiveRebalance: methods.shouldDelayAdaptiveRebalance,
			getMemoryUsage: sinon.stub().resolves(100),
			scheduleReplicationStatusRefreshForStorage: sinon.spy(),
			getDynamicRange: sinon.stub().resolves({
				widthNormalized: 0.2,
				start1: 0,
				hash: "private-peer",
				id: new Uint8Array(32),
				mode: 0,
				timestamp: 0,
			}),
			replicationIndex: { getSize: sinon.stub().resolves(2) },
			calculateTotalParticipation: sinon.stub().resolves(0.4),
			replicationController: {
				maxMemoryLimit: 200,
				step: sinon.stub().returns(0.3),
			},
			cpuUsage: { value: sinon.stub().returns(0.1) },
			indexableDomain: {
				constructorRange: class {
					constructor(properties: object) {
						Object.assign(this, properties);
					}
				},
				numbers: { denormalize: (value: number) => value },
			},
			startAnnounceReplicating: sinon.stub().resolves(),
		};
		return {
			host,
			run: () =>
				methods.rebalanceParticipation.call(host, controller, debouncer),
			debouncer,
		};
	};

	const repairHost = (profile?: SyncProfileFn) => ({
		_logProperties: { sync: { profile } },
		isEntryRecentlyKnownByPeer: sinon.spy(
			(hash: string) => hash === "private-known",
		),
		isEntryKnownByPeer: sinon.stub().returns(false),
		clearRepairFrontierHashes: sinon.spy(),
		pushRepairEntries: sinon.stub().resolves(),
		syncronizer: { onMaybeMissingEntries: sinon.stub().resolves() },
	});
	const repairEntries = () =>
		new Map([
			["private-known", { hash: "private-known" }],
			["private-missing", { hash: "private-missing" }],
		]);
	const runRepair = (
		host: object,
		transport = "simple",
		signal?: AbortSignal,
	) =>
		methods.sendRepairEntriesWithTransport.call(
			host,
			"private-peer",
			repairEntries(),
			transport,
			{ signal },
			"churn",
		);

	const placementHost = (profile?: SyncProfileFn) => ({
		_logProperties: { sync: { profile } },
		captureReplicationOwnershipLifecycle: () => new AbortController(),
		isRepairLifecycleActive: () => true,
		closed: false,
		node: { identity: { publicKey: { hashcode: () => "private-self" } } },
		log: { trim: sinon.stub().resolves() },
		_recentRepairDispatch: new Map(),
		_isAdaptiveReplicating: false,
		joinWarmup: { _warmupSessionsByTarget: new Map() },
	});

	const sweepHost = (profile?: SyncProfileFn) => {
		const controller = new AbortController();
		const host = {
			_logProperties: { sync: { profile } },
			_instanceLifecycle: { ownershipLifecycleController: controller },
			isRepairLifecycleActive: () => true,
			_repairSweepPendingModes: new Set(["churn"]),
			_repairSweepPendingPeersByMode: new Map([
				["churn", new Set(["private-peer"])],
			]),
			joinWarmup: {
				_repairSweepWarmupSessionByTarget: new Map(),
				_warmupSessionsByTarget: new Map(),
			},
			_repairSweepOptimisticGidPeersPending: new Map(),
			_repairSweepOptimisticGidsByPeer: new Map(),
			_coordinates: { _residentEntryCoordinatesByHash: repairEntries() },
			_nativeBackbone: {},
			hasCustomFindLeaders: () => false,
			getFullReplicaRepairCandidates: sinon
				.stub()
				.resolves(new Set(["private-peer"])),
			planResidentRepairDispatchBatch: sinon
				.stub()
				.resolves(
					new Map([
						["churn", new Map([["private-peer", [...repairEntries().keys()]]])],
					]),
				),
			node: { identity: { publicKey: { hashcode: () => "private-self" } } },
			repairSweepTargetBufferSize: 100,
			dispatchMaybeMissingEntries: sinon.spy(),
			_repairFrontierByMode: new Map([["churn", new Map()]]),
			_repairSweepRunning: true,
		};
		return { host, run: () => methods.runRepairSweep.call(host, controller) };
	};

	for (const throws of [false, true]) {
		describe(throws ? "throwing sink" : "recording sink", () => {
			let events: SyncProfileEvent[];
			let profile: SyncProfileFn;
			beforeEach(() => {
				events = [];
				profile = (event) => {
					events.push(event);
					if (throws) throw new Error("diagnostic sink failure");
				};
			});
			afterEach(() => {
				for (const event of events) {
					expect(event.durationMs).to.be.at.least(0);
					expect(JSON.stringify(event)).not.to.contain("private-");
					expect(JSON.stringify(event).length).to.be.lessThan(2_048);
					expect(event.bytes).to.equal(undefined);
				}
			});

			it("summarizes adaptive inputs and update settlement without additional reads", async () => {
				const { host, run, debouncer } = adaptiveHost(profile);
				expect(await run()).to.equal(true);
				expect(host.getMemoryUsage.callCount).to.equal(1);
				expect(host.getDynamicRange.callCount).to.equal(1);
				expect(host.replicationIndex.getSize.callCount).to.equal(1);
				expect(host.calculateTotalParticipation.callCount).to.equal(1);
				expect(host.cpuUsage.value.callCount).to.equal(1);
				expect(host.replicationController.step.callCount).to.equal(1);
				expect(host.startAnnounceReplicating.callCount).to.equal(1);
				expect(debouncer.call.callCount).to.equal(1);
				expect(events).to.have.length(1);
				expect(events[0].name).to.equal("sharedLog.adaptive.rebalance");
				expect(events[0].details).to.include({
					outcome: "apply-settled",
					idleRemainingMs: 0,
					storageUsedBytes: 100,
					storageObjectiveBytes: 200,
					currentFactor: 0.2,
					proposedFactor: 0.3,
					totalFactor: 0.4,
					controllerPeerCount: 2,
					cpuUsage: 0.1,
				});
				for (const name of ["preStepMs", "stepMs", "applyMs"])
					expect(events[0].details?.[name]).to.be.at.least(0);
			});

			it("reports the idle gate without entering the controller", async () => {
				sinon.stub(Date, "now").returns(10_000);
				const { host, run, debouncer } = adaptiveHost(profile);
				host._lastLocalAppendAt = 5_000;
				expect(await run()).to.equal(false);
				expect(host.getMemoryUsage.callCount).to.equal(0);
				expect(host.replicationController.step.callCount).to.equal(0);
				expect(host.startAnnounceReplicating.callCount).to.equal(0);
				expect(debouncer.call.callCount).to.equal(1);
				expect(events[0].details).to.deep.equal({
					outcome: "idle-deferred",
					idleRemainingMs: 5_000,
				});
			});

			it("reports unchanged and stale adaptive ticks", async () => {
				const { host, run } = adaptiveHost(profile);
				host.replicationController.step.returns(0.2);
				expect(await run()).to.equal(false);
				expect(events[0].details?.outcome).to.equal("unchanged");
				host._instanceLifecycle.isActiveFor = () => false;
				expect(await run()).to.equal(false);
				expect(events[1].details?.outcome).to.equal("stale");
				expect(host.getMemoryUsage.callCount).to.equal(1);
				expect(host.startAnnounceReplicating.callCount).to.equal(0);
			});

			it("preserves adaptive failures", async () => {
				const { host, run } = adaptiveHost(profile);
				const error = new Error("controller input failure");
				host.getMemoryUsage.rejects(error);
				let caught: unknown;
				try {
					await run();
				} catch (value) {
					caught = value;
				}
				expect(caught).to.equal(error);
				expect(events[0].details?.outcome).to.equal("error");
			});

			for (const transport of ["simple", "rateless"]) {
				it(`summarizes ${transport} repair selection without changing dispatch`, async () => {
					const host = repairHost(profile);
					await runRepair(host, transport);
					expect(host.isEntryRecentlyKnownByPeer.callCount).to.equal(2);
					expect(host.isEntryKnownByPeer.callCount).to.equal(1);
					expect(host.clearRepairFrontierHashes.firstCall.args).to.deep.equal([
						"private-peer",
						["private-known"],
					]);
					expect(host.pushRepairEntries.callCount).to.equal(
						transport === "simple" ? 1 : 0,
					);
					expect(host.syncronizer.onMaybeMissingEntries.callCount).to.equal(
						transport === "simple" ? 0 : 1,
					);
					expect(events).to.have.length(1);
					expect(events[0]).to.include({
						name: "sharedLog.repair.dispatch",
						entries: 2,
						count: 1,
						targets: 1,
					});
					expect(events[0].details).to.deep.equal({
						mode: "churn",
						transport,
						outcome: "dispatched",
						knownSuppressedEntries: 1,
					});
				});
			}

			it("reports suppressed and cancelled repair without promising delivery", async () => {
				const host = repairHost(profile);
				host.isEntryKnownByPeer.returns(true);
				await runRepair(host);
				expect(host.pushRepairEntries.callCount).to.equal(0);
				expect(events[0]).to.include({ count: 0 });
				expect(events[0].details?.outcome).to.equal("known-suppressed");
				host.isEntryKnownByPeer.returns(false);
				const controller = new AbortController();
				host.pushRepairEntries.callsFake(async () => controller.abort());
				await runRepair(host, "simple", controller.signal);
				expect(events[1].details?.outcome).to.equal("cancelled");
			});

			it("preserves repair failures", async () => {
				const host = repairHost(profile);
				const error = new Error("repair dispatch failure");
				host.pushRepairEntries.rejects(error);
				let caught: unknown;
				try {
					await runRepair(host);
				} catch (value) {
					caught = value;
				}
				expect(caught).to.equal(error);
				expect(events[0].details?.outcome).to.equal("error");
			});

			it("summarizes an empty range-change pass without scanning", async () => {
				const host = placementHost(profile);
				expect(await methods.onReplicationChange.call(host, [])).to.equal(
					false,
				);
				expect(host.log.trim.callCount).to.equal(1);
				expect(events).to.have.length(1);
				expect(events[0]).to.include({
					name: "sharedLog.placement.pass",
					entries: 0,
					count: 0,
				});
				expect(events[0].details).to.include({
					phase: "range-change",
					outcome: "completed",
					changes: 0,
					repairBatches: 0,
					pruneScan: false,
				});
			});

			it("summarizes a resident repair sweep using existing plan counts", async () => {
				const { host, run } = sweepHost(profile);
				await run();
				expect(host.getFullReplicaRepairCandidates.callCount).to.equal(1);
				expect(host.planResidentRepairDispatchBatch.callCount).to.equal(1);
				expect(host.dispatchMaybeMissingEntries.callCount).to.equal(1);
				expect(host._repairSweepRunning).to.equal(false);
				expect(events).to.have.length(1);
				expect(events[0]).to.include({
					name: "sharedLog.placement.pass",
					entries: 2,
					count: 2,
				});
				expect(events[0].details).to.deep.equal({
					phase: "repair-sweep",
					outcome: "completed",
					passes: 1,
					nativePasses: 1,
					repairBatches: 1,
				});
			});

			it("counts distinct existing raw heads without another lookup", async () => {
				const host = {
					node: { identity: { publicKey: {} } },
					log: { hasMany: sinon.stub().resolves(new Set(["private-known"])) },
				};
				const result = await methods.materializeRawReceiveMessage.call(
					host,
					{ heads: [{ hash: "private-known" }, { hash: "private-known" }] },
					{
						from: { equals: () => true },
						stashBackedRawMessage: {},
						syncProfile: profile,
					},
				);
				expect(result).to.equal(undefined);
				expect(host.log.hasMany.callCount).to.equal(1);
				expect(events).to.have.length(1);
				expect(events[0]).to.include({
					name: "sharedLog.rawReceive.existingHeads",
					entries: 2,
					count: 1,
				});
			});

			for (const rawContinuation of [false, true]) {
				it(`counts plain existing heads${rawContinuation ? " without repeating a raw lookup" : " and releases its receive lease"}`, async () => {
					const release = sinon.spy();
					const from = { equals: () => true, hashcode: () => "private-self" };
					const plain = new ExchangeHeadsMessage({
						heads: rawContinuation ? [] : ([{}, {}] as any),
						preparedHashes: rawContinuation
							? []
							: ["private-known", "private-known"],
					});
					const host = {
						_logProperties: {
							sync: {
								profile: (event: SyncProfileEvent) => {
									if (event.name === "sharedLog.receive.existingHeads")
										profile(event);
								},
							},
						},
						throwIfNativeDurableCommitFailed: sinon.spy(),
						_peerSessions: { current: () => null, receiveEpoch: () => 0 },
						acquirePeerReceiveLease: () => release,
						captureReplicationOwnershipLifecycle: () => new AbortController(),
						node: { identity: { publicKey: from } },
						log: { hasMany: sinon.stub().resolves(new Set(["private-known"])) },
						materializeRawReceiveMessage: sinon
							.stub()
							.resolves({ message: plain }),
					};
					await methods.onMessage.call(
						host,
						rawContinuation
							? new RawExchangeHeadsMessage({ heads: [] })
							: plain,
						{ from },
					);
					expect(host.log.hasMany.callCount).to.equal(rawContinuation ? 0 : 1);
					expect(release.callCount).to.equal(1);
					expect(events).to.have.length(1);
					expect(events[0]).to.include({
						name: "sharedLog.receive.existingHeads",
						entries: rawContinuation ? 0 : 2,
						count: rawContinuation ? undefined : 1,
					});
					expect(events[0].details?.rawMaterializedKnownMissing).to.equal(
						rawContinuation,
					);
				});
			}
		});
	}

	for (const sinkMode of ["disabled", "recording", "throwing"]) {
		const recordingSink = (
			events: SyncProfileEvent[],
		): SyncProfileFn | undefined =>
			sinkMode === "disabled"
				? undefined
				: (event) => {
						events.push(event);
						if (sinkMode === "throwing")
							throw new Error("diagnostic sink failure");
					};

		it(`preserves selected repair count across a custom synchronizer's mutation (${sinkMode})`, async () => {
			const events: SyncProfileEvent[] = [];
			const host = repairHost(recordingSink(events));
			const isStillCurrent = sinon.stub().returns(true);
			let selectedBeforeMutation = 0;
			host.syncronizer.onMaybeMissingEntries.callsFake(async ({ entries }) => {
				selectedBeforeMutation = entries.size;
				entries.clear();
			});
			await methods.sendRepairEntriesWithTransport.call(
				host,
				"private-peer",
				repairEntries(),
				"rateless",
				{ isStillCurrent },
				"churn",
			);
			expect(selectedBeforeMutation).to.equal(1);
			expect(host.syncronizer.onMaybeMissingEntries.callCount).to.equal(1);
			expect(isStillCurrent.callCount).to.equal(3);
			expect(host.isEntryRecentlyKnownByPeer.callCount).to.equal(2);
			expect(host.isEntryKnownByPeer.callCount).to.equal(1);
			expect(events).to.have.length(sinkMode === "disabled" ? 0 : 1);
			if (events.length) {
				expect(events[0]).to.include({ entries: 2, count: 1 });
				expect(events[0].details?.outcome).to.equal("dispatched");
			}
		});

		it(`observes stale repair checks without additional predicate calls (${sinkMode})`, async () => {
			const events: SyncProfileEvent[] = [];
			const host = repairHost(recordingSink(events));
			const isStillCurrent = sinon.stub().returns(true);
			isStillCurrent.onCall(2).returns(false);
			const shipped = sinon.spy();
			host.pushRepairEntries.callsFake(async (_target, _entries, current) => {
				await Promise.resolve();
				if (!current()) return;
				shipped();
			});
			await methods.sendRepairEntriesWithTransport.call(
				host,
				"private-peer",
				repairEntries(),
				"simple",
				{ isStillCurrent },
				"churn",
			);
			expect(host.pushRepairEntries.callCount).to.equal(1);
			expect(isStillCurrent.callCount).to.equal(3);
			expect(shipped.callCount).to.equal(0);
			expect(host.isEntryRecentlyKnownByPeer.callCount).to.equal(2);
			expect(host.isEntryKnownByPeer.callCount).to.equal(1);
			expect(events).to.have.length(sinkMode === "disabled" ? 0 : 1);
			if (events.length) {
				expect(events[0]).to.include({ entries: 2, count: 1 });
				expect(events[0].details?.outcome).to.equal("stale");
			} else {
				expect(host.pushRepairEntries.firstCall.args[2]).to.equal(
					isStillCurrent,
				);
			}
		});

		it(`observes staleness through the real simple repair stack (${sinkMode})`, async () => {
			const events: SyncProfileEvent[] = [];
			const profile = recordingSink(events);
			let current = true;
			const isStillCurrent = sinon.spy(() => current);
			const host = {
				...repairHost(profile),
				_logProperties: { sync: { profile, rawExchangeHeads: true } },
				pushRepairEntries: methods.pushRepairEntries,
				pushEntryHashes: methods.pushEntryHashes,
				pushEntryHashChunk: methods.pushEntryHashChunk,
				peerSupportsRawExchangeHeads: () => true,
				trySendFusedRawExchangeHeads: sinon.stub().callsFake(async () => {
					await Promise.resolve();
					current = false;
					return 0;
				}),
			};
			await methods.sendRepairEntriesWithTransport.call(
				host,
				"private-peer",
				repairEntries(),
				"simple",
				{ isStillCurrent },
				"churn",
			);
			expect(host.trySendFusedRawExchangeHeads.callCount).to.equal(1);
			expect(isStillCurrent.callCount).to.equal(5);
			expect(current).to.equal(false);
			expect(events).to.have.length(sinkMode === "disabled" ? 0 : 1);
			if (events.length) {
				expect(events[0]).to.include({ entries: 2, count: 1 });
				expect(events[0].details?.outcome).to.equal("stale");
			}
		});

		it(`reports a declined adaptive update as settled without another authorization check (${sinkMode})`, async () => {
			const events: SyncProfileEvent[] = [];
			const { host, run, debouncer } = adaptiveHost(recordingSink(events));
			const authorize = sinon.stub().resolves(false);
			authorize.onFirstCall().resolves(true);
			const enterMutationLane = sinon.spy();
			Object.assign(host, {
				_isTrustedReplicator: authorize,
				node: { identity: { publicKey: {} } },
				startAnnounceReplicating: methods.startAnnounceReplicating,
				addReplicationRange: methods.addReplicationRange,
				throwIfReplicationOwnershipLifecycleInactive: () => {},
				ensureCurrentHeadCoordinatesIndexed: sinon.stub().resolves(),
				validateReplicationRangeAnnouncement: () => {},
				withReceiveOwnershipMutationQueue: enterMutationLane,
			});
			expect(await run()).to.equal(true);
			expect(authorize.callCount).to.equal(2);
			expect(enterMutationLane.callCount).to.equal(0);
			expect(debouncer.call.callCount).to.equal(1);
			expect(events).to.have.length(sinkMode === "disabled" ? 0 : 1);
			if (events.length) {
				expect(events[0].details).to.include({
					outcome: "apply-settled",
					proposedFactor: 0.3,
				});
				expect(events[0].details).not.to.have.property("appliedFactor");
				expect(events[0].details?.applyMs).to.be.at.least(0);
			}
		});
	}

	it("separates elapsed adaptive phases without another controller invocation", async () => {
		let time = 100;
		sinon.stub(globalThis.performance, "now").callsFake(() => time);
		const events: SyncProfileEvent[] = [];
		const { host, run } = adaptiveHost((event) => events.push(event));
		host.getMemoryUsage.callsFake(async () => {
			time += 10;
			return 100;
		});
		host.replicationController.step.callsFake(() => {
			time += 2;
			return 0.3;
		});
		host.startAnnounceReplicating.callsFake(async () => {
			time += 20;
		});
		expect(await run()).to.equal(true);
		expect(events[0].durationMs).to.equal(32);
		expect(events[0].details).to.include({
			preStepMs: 10,
			stepMs: 2,
			applyMs: 20,
		});
		expect(host.replicationController.step.callCount).to.equal(1);
	});

	it("does not make an observer enter the adaptive controller", async () => {
		const profile = sinon.spy();
		const { host, run } = adaptiveHost(profile);
		host._isAdaptiveReplicating = false;
		host._isReplicating = false;
		expect(await run()).to.equal(false);
		expect(host.getMemoryUsage.callCount).to.equal(0);
		expect(host.replicationController.step.callCount).to.equal(0);
		expect(profile.callCount).to.equal(0);
	});

	it("does not read the diagnostic clock or add work when disabled", async () => {
		const now = sinon.spy(globalThis.performance, "now");
		const { host, run } = adaptiveHost();
		expect(await run()).to.equal(true);
		expect(host.getMemoryUsage.callCount).to.equal(1);
		expect(host.getDynamicRange.callCount).to.equal(1);
		expect(host.replicationIndex.getSize.callCount).to.equal(1);
		expect(host.calculateTotalParticipation.callCount).to.equal(1);
		expect(host.cpuUsage.value.callCount).to.equal(1);
		const repair = repairHost();
		await runRepair(repair);
		expect(repair.isEntryRecentlyKnownByPeer.callCount).to.equal(2);
		expect(repair.isEntryKnownByPeer.callCount).to.equal(1);
		expect(repair.pushRepairEntries.callCount).to.equal(1);
		await methods.onReplicationChange.call(placementHost(), []);
		const sweep = sweepHost();
		await sweep.run();
		expect(sweep.host.planResidentRepairDispatchBatch.callCount).to.equal(1);
		expect(sweep.host.dispatchMaybeMissingEntries.callCount).to.equal(1);
		expect(now.callCount).to.equal(0);
	});
});
