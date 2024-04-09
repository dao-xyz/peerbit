import { EventStore } from "./utils/stores/event-store.js";
import mapSeries from "p-each-series";

// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { waitForConverged } from "./utils.js";
import { v4 as uuid } from "uuid";
import { expect } from "chai";

describe("load", function () {
	let db1: EventStore<string>, db2: EventStore<string>;

	let session: TestSession;


	afterEach(async () => {
		await session.stop();

		if (db1) {
			await db1.drop();
		}

		if (db2) {
			await db2.drop();
		}
	});

	it("load after replicate", async () => {
		session = await TestSession.connected(2);

		db1 = await session.peers[0].open(new EventStore<string>());
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		);

		const entryCount = 100;
		const entryArr: number[] = [];

		for (let i = 0; i < entryCount; i++) entryArr.push(i);

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		await waitForResolved(async () => {
			const items = (await db2.iterator({ limit: -1 })).collect();
			expect(items.length).equal(entryCount);
			expect(items[0].payload.getValue().value).equal("hello0");
			expect(items[items.length - 1].payload.getValue().value).equal(
				"hello" + (items.length - 1)
			);
		});
	});

	it("load after prune", async () => {
		session = await TestSession.connected(2, [
			{ directory: "./tmp/shared-log/load-after-prune/" + uuid() },
			{ directory: "./tmp/shared-log/load-after-prune/" + uuid() }
		]);

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				role: { type: "replicator", factor: 0.5 },
				replicas: {
					min: 1
				}
			}
		});

		for (let i = 0; i < 100; i++) {
			await db1.add("hello" + i, { meta: { next: [] } });
		}

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					role: { type: "replicator", factor: 0.5 },
					replicas: {
						min: 1
					}
				}
			}
		);

		await waitForResolved(() => expect(db1.log.log.length).lessThan(100)); // pruning started
		await waitForConverged(() => db1.log.log.length); // pruning done
		const lengthBeforeClose = db1.log.log.length;
		await waitForConverged(() => db2.log.log.length);
		await session.peers[1].stop();
		await db1.close();
		db1 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[0],
			{
				args: {
					role: { type: "replicator", factor: 0.5 },
					replicas: {
						min: 1
					}
				}
			}
		);
		let lengthAfterClose = db1.log.log.length;
		expect(lengthBeforeClose).equal(lengthAfterClose);
		expect(lengthAfterClose).greaterThan(0);
	});
});
