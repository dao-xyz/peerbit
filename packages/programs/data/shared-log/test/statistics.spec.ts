import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { waitForConverged } from "./utils.js";
import { EventStore } from "./utils/stores/event-store.js";

const STATS_DIAG =
	process.env.PEERBIT_TRACE_STATISTICS_TESTS === "1" ||
	process.env.PEERBIT_TRACE_ALL_TEST_FAILURES === "1";

const emitStatisticsDiag = async (
	label: string,
	dbs: Array<EventStore<string, any> | undefined>,
) => {
	if (!STATS_DIAG) {
		return;
	}
	const rows = await Promise.all(
		dbs
			.filter((db): db is EventStore<string, any> => !!db)
			.map(async (db) => {
				let totalParticipation: number | string = "n/a";
				let myParticipation: number | string = "n/a";
				let assignedHeads: number | string = "n/a";
				let approxHeads: number | string = "n/a";
				try {
					totalParticipation = await db.log.calculateTotalParticipation();
					myParticipation = await db.log.calculateMyTotalParticipation();
					assignedHeads = await db.log.countAssignedHeads();
					approxHeads = await db.log.countHeads({ approximate: true });
				} catch {
					// Keep diagnostics best-effort.
				}
				return {
					id: db.log.node.identity.publicKey.hashcode(),
					length: db.log.log.length,
					totalParticipation,
					myParticipation,
					assignedHeads,
					approxHeads,
				};
			}),
	);
	console.error(`[statistics-diag] ${label}: ${JSON.stringify(rows)}`);
};

describe(`countAssignedHeads`, function () {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("all", async () => {
		let db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		await db1.add("hello");
		expect(await db1.log.countAssignedHeads()).to.eq(1);
	});

	it("partial", async () => {
		const diagLabel = "countAssignedHeads::partial";
		let db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					offset: 0,
					factor: 0.5,
				},
				replicas: {
					min: 1,
				},
			},
		});

		let db2 = await session.peers[1].open(db1.clone(), {
			args: {
				replicate: {
					offset: 0.5,
					factor: 0.5,
				},
				replicas: {
					min: 1,
				},
			},
		});

		let count = 1000;

		for (let i = 0; i < count; i++) {
			await db1.add(i.toString(), { meta: { next: [] } });
		}

		try {
			await waitForResolved(() =>
				expect(db2.log.log.length).to.be.greaterThan(0),
			);
			await waitForConverged(() => db1.log.log.length, {
				timeout: 120_000,
				tests: 3,
				interval: 1_000,
				delta: 2,
				jitter: 2,
			});
			await waitForConverged(() => db2.log.log.length, {
				timeout: 120_000,
				tests: 3,
				interval: 1_000,
				delta: 2,
				jitter: 2,
			});

			const owned1 = await db1.log.countAssignedHeads();
			const owned2 = await db2.log.countAssignedHeads();

			expect(owned1).to.be.within(count * 0.4, count * 0.6);
			expect(owned2).to.be.within(count * 0.4, count * 0.6);
		} catch (error) {
			await emitStatisticsDiag(diagLabel, [db1, db2]);
			throw error;
		}
	});
});

describe(`countHeads`, function () {
	describe("approximative", () => {
		let session: TestSession;

		beforeEach(async () => {
			session = await TestSession.connected(1);
		});

		afterEach(async () => {
			await session.stop();
		});

		it("throws when not replicating", async () => {
			let db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: false,
				},
			});

			await db1.add("hello");
			await expect(
				db1.log.countHeads({ approximate: true }),
			).eventually.rejectedWith("Not implemented for non-replicators");
		});

		it("counts when replicating all", async () => {
			let db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});

			await db1.add("hello", { meta: { next: [] } });
			await db1.add("hello again", { meta: { next: [] } });

			expect(await db1.log.countHeads({ approximate: true })).to.eq(2);
		});

		it("partial 0.5", async () => {
			const diagLabel = "countHeads::partial 0.5";
			let db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.5,
					},
				},
			});
			let count = 1000;
			for (let i = 0; i < count; i++) {
				await db1.add(i.toString(), { meta: { next: [] } });
			}

			try {
				await waitForConverged(() => db1.log.log.length, {
					timeout: 120_000,
					tests: 3,
					interval: 1_000,
					delta: 2,
					jitter: 2,
				});
				const total1 = await db1.log.countHeads({ approximate: true });
				expect(total1).to.be.within(count * 0.8, count * 1.2);
			} catch (error) {
				await emitStatisticsDiag(diagLabel, [db1]);
				throw error;
			}
		});

		it("partial 0.25", async () => {
			const diagLabel = "countHeads::partial 0.25";
			let db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.25,
					},
				},
			});
			let count = 1000;
			for (let i = 0; i < count; i++) {
				await db1.add(i.toString(), { meta: { next: [] } });
			}

			try {
				await waitForConverged(() => db1.log.log.length, {
					timeout: 120_000,
					tests: 3,
					interval: 1_000,
					delta: 2,
					jitter: 2,
				});
				const total1 = await db1.log.countHeads({ approximate: true });
				expect(total1).to.be.within(count * 0.8, count * 1.2);
			} catch (error) {
				await emitStatisticsDiag(diagLabel, [db1]);
				throw error;
			}
		});
	});
});
