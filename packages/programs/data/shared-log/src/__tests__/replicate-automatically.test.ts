import { TestSession } from "@peerbit/test-utils";
import { waitForAsync } from "@peerbit/time";
import { EventStore } from "./utils/stores/event-store";
import assert from "assert";
import mapSeries from "p-each-series";

describe(`Automatic Replication`, function () {
	let session: TestSession;
	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("starts replicating the database when peers connect", async () => {
		const entryCount = 33;
		const entryArr: number[] = [];

		const db1 = await session.peers[0].open(new EventStore<string>());
		const db3 = await session.peers[0].open(new EventStore<string>());

		// Create the entries in the first database
		for (let i = 0; i < entryCount; i++) {
			entryArr.push(i);
		}

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		// Open the second database
		const db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		))!;

		const db4 = (await EventStore.open<EventStore<string>>(
			db3.address!,
			session.peers[1]
		))!;

		await waitForAsync(
			async () =>
				(await db2.iterator({ limit: -1 })).collect().length === entryCount
		);
		const result1 = (await db1.iterator({ limit: -1 })).collect();
		const result2 = (await db2.iterator({ limit: -1 })).collect();
		expect(result1.length).toEqual(result2.length);
		for (let i = 0; i < result1.length; i++) {
			assert(result1[i].equals(result2[i]));
		}

		expect(db3.log.log.length).toEqual(0);
		expect(db4.log.log.length).toEqual(0);
	});

	it("starts replicating the database when peers connect in write mode", async () => {
		const entryCount = 1;
		const entryArr: number[] = [];
		const db1 = await session.peers[0].open(new EventStore<string>());

		// Create the entries in the first database
		for (let i = 0; i < entryCount; i++) {
			entryArr.push(i);
		}

		await mapSeries(entryArr, (i) => db1.add("hello" + i));

		// Open the second database
		const db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		))!;

		await waitForAsync(
			async () =>
				(await db2.iterator({ limit: -1 })).collect().length === entryCount
		);
		const result1 = (await db1.iterator({ limit: -1 })).collect();
		const result2 = (await db2.iterator({ limit: -1 })).collect();
		expect(result1.length).toEqual(result2.length);
		for (let i = 0; i < result1.length; i++) {
			expect(result1[i].equals(result2[i])).toBeTrue();
		}
	});
});
