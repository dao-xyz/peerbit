import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { SimpleStoreContract } from "./utils/access";


describe(`shared`, () => {
	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		db1: SimpleStoreContract,
		db2: SimpleStoreContract;

	beforeAll(async () => {
		session = await LSession.connected(2);
	});

	afterAll(async () => {
		await session.stop();
	});

	beforeEach(async () => {
		client1 = await Peerbit.create(session.peers[0], {});
		client2 = await Peerbit.create(session.peers[1], {});
	});

	afterEach(async () => {
		if (db1) await db1.store.drop();

		if (db2) await db2.store.drop();

		if (client1) await client1.stop();

		if (client2) await client2.stop();
	});

	it("open same store twice will share instance", async () => {
		db1 = await client1.open(
			new SimpleStoreContract({
				store: new EventStore({ id: "some db" }),
			})
		);
		const sameDb = await client1.open(
			new SimpleStoreContract({
				store: new EventStore({ id: "some db" }),
			})
		);
		expect(db1 === sameDb);
	});

	it("can share nested stores", async () => {
		const topic = "topic";
		db1 = await client1.open(
			new SimpleStoreContract({
				store: new EventStore<string>({
					id: "event store",
				}),
			})
		);
		db2 = await client1.open(
			new SimpleStoreContract({
				store: new EventStore<string>({
					id: "event store",
				}),
			})
		);
		expect(db1 !== db2);
		expect(db1.store === db2.store);
	});

	// TODO add tests and define behaviour for cross topic programs
	// TODO add tests for shared subprogams
	// TODO add tests for subprograms that is also open as root program
});
