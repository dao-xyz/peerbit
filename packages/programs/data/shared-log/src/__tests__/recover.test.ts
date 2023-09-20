import { TestSession } from "@peerbit/test-utils";
import { EventStore } from "./utils/stores";

describe("recover", () => {
	let session: TestSession;
	let db1: EventStore<string>;

	beforeEach(async () => {
		session = await TestSession.connected(1);

		db1 = await session.peers[0].open(new EventStore<string>());
	});

	afterEach(async () => {
		if (db1) await db1.drop();
		await session.stop();
	});

	it("can recover from too strict access log", async () => {
		await db1.add("a");
		await db1.add("b");
		await db1.add("c");
		expect(db1.log.log.length).toEqual(3);
		await db1.close();
		db1 = await session.peers[0].open(new EventStore<string>({ id: db1.id }));
		await db1.add("d");
		expect(db1.log.log.length).toEqual(1);
		await db1.log.log.recover();
		expect(db1.log.log.length).toEqual(4);
	});
});
