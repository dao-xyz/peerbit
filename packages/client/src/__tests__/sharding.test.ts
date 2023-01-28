import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { delay, waitFor, waitForAsync } from "@dao-xyz/peerbit-time";
import { PermissionedEventStore } from "./utils/stores/test-store";

import { v4 as uuid } from "uuid";

describe(`sharding`, () => {
	jest.retryTimes(1); // TODO this tests are FLAKY

	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		client3: Peerbit,
		db1: PermissionedEventStore,
		db2: PermissionedEventStore,
		db3: PermissionedEventStore;

	beforeAll(async () => {
		session = await LSession.connected(3);
	});

	beforeEach(async () => {
		const topic = uuid();
		client1 = await Peerbit.create({ libp2p: session.peers[0] });
		client2 = await Peerbit.create({ libp2p: session.peers[1] });
		client3 = await Peerbit.create({ libp2p: session.peers[2] });

		db1 = await client1.open<PermissionedEventStore>(
			new PermissionedEventStore({
				trusted: [client1.id, client2.id, client3.id],
			})
		);
	});

	afterEach(async () => {
		await db1?.drop();
		await db2?.drop();
		await db3?.drop();

		if (client1) {
			await client1.stop();
		}
		if (client2) {
			await client2.stop();
		}
		if (client3) {
			await client3.stop();
		}
	});

	afterAll(async () => {
		await session.stop();
	});

	it("can distribute evenly among peers", async () => {
		// TODO this test is flaky, because it sometimes timeouts because distribution of data among peers is random for small entry counts
		db2 = await client2.open<PermissionedEventStore>(db1.address!);
		db3 = await client3.open<PermissionedEventStore>(db1.address!);
		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 2
		);
		await waitFor(
			() => client3.getReplicators(db1.address!.toString())?.length === 2
		);

		const entryCount = 100;
		//await delay(5000);

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.store.add(i.toString(), { nexts: [] });
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.store.store.oplog.values.length === entryCount);

		await delay(10000);
		// this could failed, if we are unlucky probability wise
		await waitFor(
			() =>
				db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.store.oplog.values.length;
			await delay(2500); // arb delay
			return a === db.store.oplog.values.length;
		};

		await waitForAsync(() => checkConverged(db2.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		expect(
			db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		).toBeTrue();
		expect(
			db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		).toBeTrue();
	});

	// TODO add tests for late joining and leaving peers

	it("will distribute to joining peers", async () => {
		db2 = await client2.open<PermissionedEventStore>(db1.address!);
		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 1
		);

		const entryCount = 100;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.store.store.oplog.values.length === entryCount);
		await waitFor(() => db2.store.store.oplog.values.length === entryCount);

		db3 = await client3.open<PermissionedEventStore>(db1.address!);
		// client 3 will subscribe and start to recive heads before recieving subscription info about other peers

		await waitFor(
			() => client1.getReplicators(db1.address!.toString())?.length === 2
		);
		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 2
		);
		await waitFor(
			() => client3.getReplicators(db1.address!.toString())?.length === 2
		);

		await waitFor(
			() =>
				db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		);

		const checkConverged = async (db: EventStore<any>) => {
			const a = db.store.oplog.values.length;
			await delay(2500); // arb delay
			return a === db.store.oplog.values.length;
		};

		await waitForAsync(() => checkConverged(db2.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		await waitForAsync(() => checkConverged(db3.store), {
			timeout: 20000,
			delayInterval: 500,
		});
		expect(
			db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		).toBeTrue();
		expect(
			db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		).toBeTrue();
	});

	it("will distribute when peers leave", async () => {
		db2 = await client2.open<PermissionedEventStore>(db1.address!);
		db3 = await client3.open<PermissionedEventStore>(db1.address!);
		await waitFor(
			() => client2.getReplicators(db1.address!.toString())?.length === 2
		);
		await waitFor(
			() => client3.getReplicators(db1.address!.toString())?.length === 2
		);

		const entryCount = 100;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(db1.store.add(i.toString(), { nexts: [] }));
		}

		await Promise.all(promises);
		await waitFor(() => db1.store.store.oplog.values.length === entryCount);

		await waitFor(
			() =>
				db2.store.store.oplog.values.length > entryCount * 0.5 &&
				db2.store.store.oplog.values.length < entryCount * 0.85
		);
		await waitFor(
			() =>
				db3.store.store.oplog.values.length > entryCount * 0.5 &&
				db3.store.store.oplog.values.length < entryCount * 0.85
		);

		await db3.close();

		await waitFor(() => db2.store.store.oplog.values.length === entryCount);
	});
});
