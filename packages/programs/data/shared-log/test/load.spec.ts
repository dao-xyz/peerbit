// Include test utilities
import { keys } from "@libp2p/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import mapSeries from "p-each-series";
import { v4 as uuid } from "uuid";
import { waitForConverged } from "./utils.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("load", function () {
	let db1: EventStore<string, any>, db2: EventStore<string, any>;

	let session: TestSession;
	const TRACE_LOAD_DIAG =
		process.env.PEERBIT_TRACE_LOAD_TESTS === "1" ||
		process.env.PEERBIT_TRACE_ALL_TEST_FAILURES === "1";

	const emitLoadDiag = async (label: string) => {
		if (!TRACE_LOAD_DIAG) {
			return;
		}
		const rows = await Promise.all(
			[db1, db2]
				.filter((db): db is EventStore<string, any> => !!db)
				.map(async (db) => {
					try {
						return {
							id: db.node.identity.publicKey.hashcode(),
							length: db.log.log.length,
							isReplicating: await db.log.isReplicating(),
							replicationIndexSize:
								(await db.log.replicationIndex?.getSize()) ?? "n/a",
							totalParticipation: await db.log.calculateTotalParticipation(),
							myParticipation: await db.log.calculateMyTotalParticipation(),
						};
					} catch (error: any) {
						return {
							id: db.node.identity.publicKey.hashcode(),
							error: error?.message ?? String(error),
						};
					}
				}),
		);
		console.error(`[load-diag] ${label}: ${JSON.stringify(rows)}`);
	};

	afterEach(async () => {
		await session.stop();

		if (db1 && db1.closed === false) {
			await db1.drop();
		}

		if (db2 && db2.closed === false) {
			await db2.drop();
		}
	});

	it("load after replicate", async () => {
		session = await TestSession.connected(2);

		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicas: { min: 2 },
				replicate: { offset: 0, factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: { min: 2 },
					replicate: { offset: 0, factor: 1 },
					timeUntilRoleMaturity: 0,
				},
			},
		);

		// Ensure the remote peer is known as a replicator before we append; otherwise the
		// writer may compute a leader set that only includes itself and skip directed delivery.
		await db1.log.waitForReplicator(session.peers[1].identity.publicKey, {
			timeout: 15e3,
			roleAge: 0,
		});

		const entryCount = 100;
		const entryArr: number[] = [];

		for (let i = 0; i < entryCount; i++) entryArr.push(i);

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		await waitForResolved(async () => {
			const items = (await db2.iterator({ limit: -1 })).collect();
			expect(items.length).equal(entryCount);
			expect(items[0].payload.getValue().value).equal("hello0");
			expect(items[items.length - 1].payload.getValue().value).equal(
				"hello" + (items.length - 1),
			);
		});
	});

	it("load after prune", async function () {
		const pruneTimeout = 180_000;
		this.timeout(pruneTimeout + 30_000);
		// TODO fix test flakiness

		session = await TestSession.connected(2, [
			{
				directory: "./tmp/shared-log/load-after-prune/" + uuid(),
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
							168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
							159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53,
							142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72,
							148, 82, 66, 138, 199, 185,
						]),
					),
				},
			},
			{
				directory: "./tmp/shared-log/load-after-prune/" + uuid(),
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
							157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120,
							122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251,
							100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6,
							174, 212, 159, 187, 2, 137, 47, 192,
						]),
					),
				},
			},
		]);

		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { offset: 0, factor: 0.5 },
				replicas: {
					min: 1,
				} /* 
				timeUntilRoleMaturity: 0 */,
			},
		});

		let count = 100;

		for (let i = 0; i < count; i++) {
			await db1.add("hello" + i, { meta: { next: [] } });
		}

		db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: { offset: 0.3, factor: 0.5 },
					replicas: {
						min: 1,
					} /* 
					timeUntilRoleMaturity: 0 */,
				},
			},
		);

		try {
			await waitForResolved(
				() => expect(db1.log.log.length).lessThan(count),
				{ timeout: 60_000, delayInterval: 500 },
			); // pruning started

			await waitForConverged(() => db1.log.log.length, {
				timeout: pruneTimeout,
				interval: 1_000,
				tests: 3,
				delta: 2,
				jitter: 2,
			}); // pruning done

			await waitForConverged(() => db2.log.log.length, {
				timeout: pruneTimeout,
				interval: 1_000,
				tests: 3,
				delta: 2,
				jitter: 2,
			});
			await session.peers[1].stop();
			await waitForConverged(() => db1.log.log.length, {
				timeout: pruneTimeout,
				interval: 1_000,
				tests: 3,
				delta: 2,
				jitter: 2,
			});
			const lengthBeforeClose = db1.log.log.length;
			await db1.close();
			db1 = await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[0],
				{
					args: {
						replicate: { factor: 0.5 },
						replicas: {
							min: 1,
						},
					},
				},
			);
			let lengthAfterClose = db1.log.log.length;
			expect(lengthBeforeClose).equal(lengthAfterClose);
			expect(lengthAfterClose).greaterThan(0);
		} catch (error) {
			await emitLoadDiag("load after prune");
			throw error;
		}
	});

	it("reload emit change events for loaded entries", async () => {
		session = await TestSession.connected(1, [
			{ directory: "./tmp/shared-log/load-events/" + uuid() },
		]);

		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				replicas: {
					min: 1,
				},
			},
		});
		await db1.add("hello", { meta: { next: [] } });

		await db1.close();

		let added = 0;
		let removed = 0;

		db1 = await session.peers[0].open(db1.clone(), {
			args: {
				replicate: { factor: 1 },
				replicas: {
					min: 1,
				},
				onChange: (change) => {
					added += change.added.length;
					removed += change.removed.length;
				},
			},
		});

		expect(db1.log.log.length).to.equal(1);
		await db1.log.reset();
		await waitForResolved(() => {
			expect(added).equal(1);
			expect(removed).equal(0); // no entries were removed
		});
	});

	it("will set replicaiton info on load", async () => {
		session = await TestSession.connected(2, [
			{
				directory:
					"./tmp/shared-log/waitForReplicators/will-set-replicaiton-info-on-load/" +
					uuid(),
			},
			{
				directory: undefined,
			},
		]);

		const store = new EventStore();
		let store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
			},
		});

		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
			},
		});

		await waitForResolved(async () =>
			expect(store.log.uniqueReplicators.size).equal(2),
		);
		expect(await store1.log.replicationIndex.count()).to.equal(2);
		await store1.close();
		store1 = await session.peers[0].open(store1.clone(), {
			args: {
				replicate: {
					type: "resume",
					default: {
						factor: 1,
					},
				},
			},
		});

		await waitForResolved(async () =>
			expect(store1.log.uniqueReplicators.size).equal(2),
		);
	});
});
