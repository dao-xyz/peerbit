import { waitFor } from "@dao-xyz/peerbit-time";
import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { v4 as uuid } from "uuid";

// Include test utilities
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { waitForPeers as waitForPeersBlock } from '@dao-xyz/libp2p-direct-stream';

/**
 * Tests that are relavent for browser environments
 */

describe(`browser`, function () {
	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		db1: EventStore<string>,
		db2: EventStore<string>;

	afterAll(async () => { });

	beforeEach(async () => { });

	afterEach(async () => {
		if (db1) await db1.store.drop();

		if (db2) await db2.store.drop();

		if (client1) await client1.stop();

		if (client2) await client2.stop();

		await session.stop();
	});

	it("can replicate entries", async () => {
		session = await LSession.connected(2);

		client1 = await Peerbit.create(session.peers[0], {
			browser: true,
		});
		client2 = await Peerbit.create(session.peers[1], {
			browser: true,
		});

		let topic = uuid();
		db1 = await client1.open(
			new EventStore<string>({
				id: uuid(),
			}),
			{ replicate: true }
		);

		db2 = await client2.open<EventStore<string>>(
			await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			),
			{ replicate: true }
		);

		await waitForPeers(session.peers[1], [client1.id], topic);
		await waitForPeers(session.peers[0], [client2.id], topic);
		await waitForPeers(session.peers[1], [client1.id], topic);
		await waitForPeers(session.peers[0], [client2.id], topic);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			db1.store.oplog.values.map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		expect(db2.store.oplog.values.length).toEqual(2);
	});
	it("can replicate entries through relay", async () => {
		session = await LSession.disconnected(3);
		let topic = uuid();

		// peer 3 is relay, and dont connect 1 with 2 directly
		session.peers[2].pubsub.subscribe(topic);
		session.peers[0].dial(session.peers[2].getMultiaddrs()[0]);
		session.peers[1].dial(session.peers[2].getMultiaddrs()[0]);

		await waitForPeersBlock(
			session.peers[0].directblock,
			session.peers[2].directblock
		);
		await waitForPeers(session.peers[0], session.peers[2], topic);
		await waitForPeersBlock(
			session.peers[1].directblock,
			session.peers[2].directblock
		);
		await waitForPeers(session.peers[1], session.peers[2], topic);

		client1 = await Peerbit.create(session.peers[0], {
			browser: true,
		});
		client2 = await Peerbit.create(session.peers[1], {
			browser: true,
		});

		db1 = await client1.open(
			new EventStore<string>({
				id: uuid(),
			}),
			{ replicate: true }
		);

		db2 = await client2.open<EventStore<string>>(
			await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			),
			{ replicate: true }
		);

		await waitForPeers(session.peers[2], [client1.id], topic);
		await waitForPeers(session.peers[2], [client2.id], topic);
		/* 
			expect(client1._directConnections.size).toEqual(0); // since browser
				expect(client2._directConnections.size).toEqual(0); // since browser
		 */
		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			db1.store.oplog.values.map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		expect(db2.store.oplog.values.length).toEqual(2);
	});

	it("will share entries as replicator on peer connect", async () => {
		session = await LSession.connected(2);

		client1 = await Peerbit.create(session.peers[0], {
			browser: true,
		});
		client2 = await Peerbit.create(session.peers[1], {
			browser: true,
		});

		let topic = uuid();
		db1 = await client1.open(
			new EventStore<string>({
				id: uuid(),
			}),
			{ replicate: true }
		);

		await db1.add("hello");
		await db1.add("world");

		db2 = await client2.open<EventStore<string>>(
			await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			),
			{ replicate: true }
		);

		await waitForPeers(session.peers[1], [client1.id], topic);
		await waitForPeers(session.peers[0], [client2.id], topic);
		await waitForPeers(session.peers[1], [client1.id], topic);
		await waitForPeers(session.peers[0], [client2.id], topic);
		/*   expect(client1._directConnections.size).toEqual(0); // since browser
		  expect(client2._directConnections.size).toEqual(0); // since browser
   */
		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			db1.store.oplog.values.map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		await waitFor(() => db2.store.oplog.values.length === 2);
	});

	it("will share entries as observer on peer connect", async () => {
		let topic = uuid();
		session = await LSession.connected(2);

		client1 = await Peerbit.create(session.peers[0], {
			browser: true,
		});
		client2 = await Peerbit.create(session.peers[1], {
			browser: true,
		});

		db1 = await client1.open(
			new EventStore<string>({
				id: uuid(),
			}),
			{ replicate: false }
		);

		await db1.add("hello");
		await db1.add("world");

		db2 = await client2.open<EventStore<string>>(
			await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			),
			{ replicate: true }
		);

		await waitForPeers(session.peers[1], [client1.id], topic);
		await waitForPeers(session.peers[0], [client2.id], topic);
		await waitForPeers(session.peers[0], [client2.id], topic);
		expect(client1.libp2p.directsub.topics.has(topic)).toEqual(false);
		/*  expect(client1._directConnections.size).toEqual(0); // since browser
		 expect(client2._directConnections.size).toEqual(0); // since browser
  */
		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			db1.store.oplog.values.map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		await waitFor(() => db2.store.oplog.values.length === 2);
	});
});
