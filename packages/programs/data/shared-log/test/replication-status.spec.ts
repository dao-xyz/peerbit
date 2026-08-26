import { ClosedError } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	AbsoluteReplicas,
	MAX_U32,
	MAX_U64,
	type ReplicationStatus,
	classifyReplicationStatus,
	createReplicationDomainHash,
} from "../src/index.js";
import { createNumbers } from "../src/integers.js";
import { calculateCoverage } from "../src/ranges.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("replication status", function () {
	this.timeout(30_000);

	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	it("classifies combined failures in deterministic order", () => {
		const status = classifyReplicationStatus({
			storageUsedBytes: 101,
			storageObjectiveBytes: 100,
			rangeCoverage: 2,
			defaultReplicaTarget: 3,
			activeReplicators: 2,
		});

		expect(status.healthy).to.be.false;
		expect(status.reasons).to.deep.equal([
			"storage-objective-exceeded",
			"range-coverage-underfilled",
			"default-replica-target-unattainable",
		]);
		expect(status.storage).to.deep.equal({
			usedBytes: 101,
			objectiveBytes: 100,
			semantics: "soft-objective",
		});
		expect(Object.isFrozen(status)).to.be.true;
		expect(Object.isFrozen(status.reasons)).to.be.true;
	});

	it("detects a u64 coverage gap whose endpoints differ above 2^53", async () => {
		const numbers = createNumbers("u64");
		const firstEnd = 2n ** 63n;
		const secondStart = firstEnd + 2n;
		const row = (start: bigint, end: bigint) => ({
			start1: start,
			end1: end,
			start2: start,
			end2: end,
		});
		// Preserve the adversarial index order: converting these endpoints to
		// Number makes them equal and used to hide the real gap.
		const values = [row(secondStart, numbers.maxValue), row(0n, firstEnd)];
		const peers = {
			iterate: () => ({
				all: async () => values.map((value) => ({ value })),
			}),
		};

		expect(await calculateCoverage({ peers: peers as any, numbers })).to.equal(
			0,
		);
	});

	for (const resolution of ["u32", "u64"] as const) {
		it(`reports two active ${resolution} replicas as unable to satisfy min 3`, async () => {
			session = await TestSession.connected(2);
			const replicate =
				resolution === "u32"
					? { factor: MAX_U32, offset: 0, normalized: false as const }
					: { factor: MAX_U64, offset: 0n, normalized: false as const };
			const setup = {
				domain: createReplicationDomainHash(resolution),
				type: resolution,
				syncronizer: SimpleSyncronizer,
				name: `${resolution}-simple-status`,
			};
			const db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate,
					replicas: { min: 3 },
					timeUntilRoleMaturity: 0,
					setup,
				},
			});
			await EventStore.open(db1.address!, session.peers[1], {
				args: {
					replicate,
					replicas: { min: 3 },
					timeUntilRoleMaturity: 0,
					setup,
				},
			});

			await waitForResolved(
				async () => {
					const status = await db1.log.getReplicationStatus();
					expect(status.coverage).to.deep.equal({ actual: 2, target: 3 });
					expect(status.replicas).to.deep.equal({ active: 2, target: 3 });
					expect(status.reasons).to.deep.equal([
						"range-coverage-underfilled",
						"default-replica-target-unattainable",
					]);
				},
				{ timeout: 20_000, delayInterval: 50 },
			);
		});
	}

	it("emits only reason transitions, including recovery", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: MAX_U64,
					offset: 0n,
					normalized: false,
				},
				replicas: { min: 1 },
			},
		});
		const log = db.log;
		await log.getReplicationStatus();
		const segmentsBefore = (await log.getMyReplicationSegments()).map(
			(segment) => segment.rangeHash,
		);
		const internal = log as any;
		const wasAdaptive = internal._isAdaptiveReplicating;
		const previousController = internal.replicationController;
		let usedBytes = 9;
		const memoryUsage = sinon
			.stub(log, "getMemoryUsage")
			.callsFake(async () => usedBytes);

		try {
			internal._isAdaptiveReplicating = true;
			internal.replicationController = { maxMemoryLimit: 10 };
			await log.getReplicationStatus();

			const events: ReplicationStatus[] = [];
			log.events.addEventListener("replication:status", (event) => {
				events.push(event.detail);
			});

			usedBytes = 11;
			await log.getReplicationStatus();
			usedBytes = 12;
			await log.getReplicationStatus();
			expect(events.map((event) => event.reasons)).to.deep.equal([
				["storage-objective-exceeded"],
			]);

			usedBytes = 9;
			await log.getReplicationStatus();
			expect(events.map((event) => event.reasons)).to.deep.equal([
				["storage-objective-exceeded"],
				[],
			]);
			expect(
				(await log.getMyReplicationSegments()).map(
					(segment) => segment.rangeHash,
				),
			).to.deep.equal(segmentsBefore);
		} finally {
			internal._isAdaptiveReplicating = wasAdaptive;
			internal.replicationController = previousController;
			memoryUsage.restore();
		}
	});

	it("runs automatic scans only while status is observed", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: MAX_U64,
					offset: 0n,
					normalized: false,
				},
				replicas: { min: 2 },
			},
		});
		const log = db.log;
		const internal = log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();

		await delay(20);
		expect(internal._replicationStatus).to.equal(undefined);
		const coverage = sinon.spy(log, "calculateCoverage");
		const events: ReplicationStatus[] = [];
		const onStatus = (event: CustomEvent<ReplicationStatus>) => {
			events.push(event.detail);
		};

		try {
			internal.uniqueReplicators.add(remoteHash);
			log.events.dispatchEvent(
				new CustomEvent("replicator:join", {
					detail: { publicKey: remote },
				}),
			);
			await delay(20);
			expect(coverage.callCount).to.equal(0);

			log.events.addEventListener("replication:status", onStatus);
			await log.getReplicationStatus();
			expect(coverage.callCount).to.equal(1);
			expect(events.at(-1)?.reasons).to.deep.equal([
				"range-coverage-underfilled",
			]);

			internal.uniqueReplicators.delete(remoteHash);
			log.events.dispatchEvent(
				new CustomEvent("replicator:leave", {
					detail: { publicKey: remote },
				}),
			);
			await waitForResolved(
				async () => {
					expect(coverage.callCount).to.equal(2);
					expect(events.at(-1)?.reasons).to.include(
						"default-replica-target-unattainable",
					);
				},
				{ timeout: 5_000, delayInterval: 10 },
			);

			log.events.removeEventListener("replication:status", onStatus);
			internal.uniqueReplicators.add(remoteHash);
			log.events.dispatchEvent(
				new CustomEvent("replicator:join", {
					detail: { publicKey: remote },
				}),
			);
			await delay(20);
			expect(coverage.callCount).to.equal(2);
		} finally {
			log.events.removeEventListener("replication:status", onStatus);
			coverage.restore();
		}
	});

	it("refreshes active-replica status on membership-only join and leave", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: MAX_U64,
					offset: 0n,
					normalized: false,
				},
				replicas: { min: 2 },
			},
		});
		const log = db.log;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const internal = log as any;
		const initial = await log.getReplicationStatus();
		expect(initial.reasons).to.include("default-replica-target-unattainable");

		const events: ReplicationStatus[] = [];
		log.events.addEventListener("replication:status", (event) => {
			events.push(event.detail);
		});

		internal.uniqueReplicators.add(remoteHash);
		log.events.dispatchEvent(
			new CustomEvent("replicator:join", {
				detail: { publicKey: remote },
			}),
		);
		await waitForResolved(
			async () => {
				expect(
					events
						.at(-1)
						?.reasons.includes("default-replica-target-unattainable"),
				).to.equal(false);
			},
			{ timeout: 5_000, delayInterval: 10 },
		);

		internal.uniqueReplicators.delete(remoteHash);
		log.events.dispatchEvent(
			new CustomEvent("replicator:leave", {
				detail: { publicKey: remote },
			}),
		);
		await waitForResolved(
			async () => {
				expect(events.at(-1)?.reasons).to.include(
					"default-replica-target-unattainable",
				);
			},
			{ timeout: 5_000, delayInterval: 10 },
		);
	});

	it("refreshes after a no-event removal retires membership without ranges", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: MAX_U64,
					offset: 0n,
					normalized: false,
				},
				replicas: { min: 2 },
			},
		});
		const log = db.log;
		const remote = session.peers[1].identity.publicKey;
		const internal = log as any;
		internal.uniqueReplicators.add(remote.hashcode());
		expect((await log.getReplicationStatus()).reasons).to.not.include(
			"default-replica-target-unattainable",
		);

		const events: ReplicationStatus[] = [];
		log.events.addEventListener("replication:status", (event) => {
			events.push(event.detail);
		});

		expect(await internal.removeReplicator(remote, { noEvent: true })).to.equal(
			true,
		);
		await waitForResolved(
			async () => {
				expect(events.at(-1)?.reasons).to.include(
					"default-replica-target-unattainable",
				);
			},
			{ timeout: 5_000, delayInterval: 10 },
		);
	});

	it("refreshes same-geometry adaptive and fixed role transitions", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: MAX_U64,
					offset: 0n,
					normalized: false,
				},
				replicas: { min: 1 },
			},
		});
		const log = db.log;
		const [range] = await log.getMyReplicationSegments();
		const internal = log as any;
		const dynamicRange = sinon
			.stub(internal, "getDynamicRange")
			.resolves(range);
		const memoryUsage = sinon.stub(log, "getMemoryUsage").resolves(20);
		await log.getReplicationStatus();

		const events: ReplicationStatus[] = [];
		log.events.addEventListener("replication:status", (event) => {
			events.push(event.detail);
		});

		try {
			await log.replicate({ limits: { storage: 10 } });
			await waitForResolved(
				async () => {
					expect(events.at(-1)?.reasons).to.include(
						"storage-objective-exceeded",
					);
				},
				{ timeout: 5_000, delayInterval: 10 },
			);

			await log.replicate(
				{
					id: range.id,
					factor: MAX_U64,
					offset: 0n,
					normalized: false,
				},
				{ reset: true },
			);
			await waitForResolved(
				async () => {
					expect(events.at(-1)?.reasons).to.not.include(
						"storage-objective-exceeded",
					);
				},
				{ timeout: 5_000, delayInterval: 10 },
			);
		} finally {
			dynamicRange.restore();
			memoryUsage.restore();
		}
	});

	it("bounds refresh churn to one active scan and one dirty rerun", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: MAX_U64,
					offset: 0n,
					normalized: false,
				},
				replicas: { min: 1 },
			},
		});
		const log = db.log;
		await log.getReplicationStatus();

		let releaseFirst!: () => void;
		const firstGate = new Promise<void>((resolve) => {
			releaseFirst = resolve;
		});
		let markFirstStarted!: () => void;
		const firstStarted = new Promise<void>((resolve) => {
			markFirstStarted = resolve;
		});
		let scans = 0;
		const coverage = sinon
			.stub(log, "calculateCoverage")
			.callsFake(async () => {
				scans += 1;
				if (scans === 1) {
					markFirstStarted();
					await firstGate;
				}
				return 1;
			});
		const internal = log as any;
		const onStatus = () => {};
		log.events.addEventListener("replication:status", onStatus);

		try {
			internal.scheduleReplicationStatusRefresh();
			await firstStarted;
			for (let i = 0; i < 25; i++) {
				internal.scheduleReplicationStatusRefresh();
				await Promise.resolve();
			}
			expect(scans).to.equal(1);

			releaseFirst();
			await waitForResolved(
				async () => {
					expect(scans).to.equal(2);
				},
				{ timeout: 5_000, delayInterval: 10 },
			);
			await delay(20);
			expect(scans).to.equal(2);
		} finally {
			releaseFirst();
			log.events.removeEventListener("replication:status", onStatus);
			coverage.restore();
		}
	});

	it("drops a dirty refresh rerun across close and reopen", async () => {
		session = await TestSession.disconnected(1);
		const args = {
			replicate: {
				factor: MAX_U64,
				offset: 0n,
				normalized: false as const,
			},
			replicas: { min: 1 },
		};
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args,
		});
		const log = db.log;
		await log.getReplicationStatus();

		let releaseOld!: () => void;
		const oldGate = new Promise<void>((resolve) => {
			releaseOld = resolve;
		});
		let markOldStarted!: () => void;
		const oldStarted = new Promise<void>((resolve) => {
			markOldStarted = resolve;
		});
		let scans = 0;
		const coverage = sinon
			.stub(log, "calculateCoverage")
			.callsFake(async () => {
				scans += 1;
				if (scans === 1) {
					markOldStarted();
					await oldGate;
				}
				return 1;
			});
		const internal = log as any;
		const onStatus = () => {};
		log.events.addEventListener("replication:status", onStatus);

		try {
			internal.scheduleReplicationStatusRefresh();
			await oldStarted;
			internal.scheduleReplicationStatusRefresh();
			await db.close();
			await session.peers[0].open(db, { args });
			await waitForResolved(
				async () => {
					expect(scans).to.equal(2);
				},
				{ timeout: 5_000, delayInterval: 10 },
			);

			releaseOld();
			await delay(20);
			expect(scans).to.equal(2);
		} finally {
			releaseOld();
			log.events.removeEventListener("replication:status", onStatus);
			coverage.restore();
		}
	});

	it("does not publish a mixed snapshot across a same-open role change", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: MAX_U64,
					offset: 0n,
					normalized: false,
				},
				replicas: { min: 1 },
			},
		});
		const log = db.log;
		await log.getReplicationStatus();
		const internal = log as any;
		const previousController = internal.replicationController;
		const wasAdaptive = internal._isAdaptiveReplicating;

		let releaseOld!: () => void;
		const oldGate = new Promise<void>((resolve) => {
			releaseOld = resolve;
		});
		let markOldStarted!: () => void;
		const oldStarted = new Promise<void>((resolve) => {
			markOldStarted = resolve;
		});
		let first = true;
		const memoryUsage = sinon
			.stub(log, "getMemoryUsage")
			.callsFake(async () => {
				if (first) {
					first = false;
					markOldStarted();
					await oldGate;
				}
				return 20;
			});
		const events: ReplicationStatus[] = [];
		log.events.addEventListener("replication:status", (event) => {
			events.push(event.detail);
		});

		try {
			internal._isAdaptiveReplicating = true;
			internal.replicationController = { maxMemoryLimit: 10 };
			const measurement = log.getReplicationStatus();
			await oldStarted;

			internal._instanceLifecycle.bumpRoleGeneration();
			internal._isAdaptiveReplicating = false;
			releaseOld();

			const status = await measurement;
			expect(status.storage.objectiveBytes).to.equal(undefined);
			expect(status.reasons).to.not.include("storage-objective-exceeded");
			expect(events).to.have.length(0);
		} finally {
			releaseOld();
			internal._isAdaptiveReplicating = wasAdaptive;
			internal.replicationController = previousController;
			memoryUsage.restore();
		}
	});

	it("refreshes restored self membership after a resume reopen", async () => {
		session = await TestSession.disconnected(1);
		const fixed = {
			factor: MAX_U64,
			offset: 0n,
			normalized: false as const,
		};
		let db = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: fixed, replicas: { min: 1 } },
		});
		const log = db.log;
		await log.getReplicationStatus();
		const events: ReplicationStatus[] = [];
		const onStatus = (event: CustomEvent<ReplicationStatus>) => {
			events.push(event.detail);
		};
		log.events.addEventListener("replication:status", onStatus);

		try {
			await db.close();
			events.length = 0;
			db = await session.peers[0].open(db, {
				args: {
					replicate: { type: "resume", default: fixed },
					replicas: { min: 1 },
				},
			});

			await waitForResolved(
				async () => {
					expect(db.log.uniqueReplicators.size).to.equal(1);
					const cached = (db.log as any)
						._replicationStatus as ReplicationStatus;
					const latest = events.at(-1);
					for (const status of [cached, latest!]) {
						expect(status.replicas.active).to.equal(1);
						expect(status.reasons).to.not.include(
							"default-replica-target-unattainable",
						);
					}
				},
				{ timeout: 5_000, delayInterval: 10 },
			);
		} finally {
			log.events.removeEventListener("replication:status", onStatus);
		}
	});

	it("rejects a stale measurement after close and does not publish it after reopen", async () => {
		session = await TestSession.disconnected(1);
		const args = {
			replicate: {
				factor: MAX_U64,
				offset: 0n,
				normalized: false as const,
			},
			replicas: { min: 1 },
		};
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args,
		});
		const log = db.log;
		await log.getReplicationStatus();

		let releaseOld!: () => void;
		const oldGate = new Promise<void>((resolve) => {
			releaseOld = resolve;
		});
		let markOldStarted!: () => void;
		const oldStarted = new Promise<void>((resolve) => {
			markOldStarted = resolve;
		});
		let first = true;
		const memoryUsage = sinon
			.stub(log, "getMemoryUsage")
			.callsFake(async () => {
				if (first) {
					first = false;
					markOldStarted();
					await oldGate;
				}
				return 0;
			});
		const events: ReplicationStatus[] = [];
		log.events.addEventListener("replication:status", (event) => {
			events.push(event.detail);
		});

		try {
			log.replicas.min = new AbsoluteReplicas(2);
			const stale = log.getReplicationStatus();
			await oldStarted;
			await db.close();
			await session.peers[0].open(db, { args });
			await log.getReplicationStatus();
			events.length = 0;

			releaseOld();
			await expect(stale).to.be.rejectedWith(ClosedError);
			await delay(20);
			expect(events).to.have.length(0);
			expect((await log.getReplicationStatus()).healthy).to.be.true;
		} finally {
			releaseOld();
			memoryUsage.restore();
		}
	});
});
