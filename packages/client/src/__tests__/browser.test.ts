import { waitFor } from "@dao-xyz/peerbit-time";
import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { v4 as uuid } from "uuid";

// Include test utilities
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { waitForPeers as waitForPeersBlock } from "@dao-xyz/libp2p-direct-stream";
import { ObserverType, ReplicatorType } from "@dao-xyz/peerbit-program";

/**
 * Tests that are relavent for browser environments
 */

describe(`browser`, function () {
	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		db1: EventStore<string>,
		db2: EventStore<string>;

	afterAll(async () => {});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
		if (client1) await client1.stop();
		if (client2) await client2.stop();
		await session.stop();
	});

	it("can replicate entries", async () => {
		session = await LSession.connected(2);

		client1 = await Peerbit.create({
			libp2p: session.peers[0],
		});
		client2 = await Peerbit.create({
			libp2p: session.peers[1],
		});

		db1 = await client1.open(
			new EventStore<string>({
				id: uuid(),
			}),
			{ role: new ReplicatorType() }
		);
		await waitForPeersBlock(
			session.peers[0].directblock,
			session.peers[1].directblock
		);
		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			))!,
			{ role: new ReplicatorType() }
		);

		await waitForPeers(session.peers[1], [client1.id], db1.address!.toString());
		await waitForPeers(session.peers[0], [client2.id], db1.address!.toString());
		await waitForPeers(session.peers[1], [client1.id], db1.address!.toString());
		await waitForPeers(session.peers[0], [client2.id], db1.address!.toString());

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			(await db1.store.oplog.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		expect(db2.store.oplog.values.length).toEqual(2);
	});

	it("can replicate entries through relay", async () => {
		session = await LSession.disconnected(3);

		// peer 3 is relay, and dont connect 1 with 2 directly
		session.peers[0].dial(session.peers[2].getMultiaddrs()[0]);
		session.peers[1].dial(session.peers[2].getMultiaddrs()[0]);

		client1 = await Peerbit.create({
			libp2p: session.peers[0],
		});

		client2 = await Peerbit.create({
			libp2p: session.peers[1],
		});

		await waitForPeersBlock(
			session.peers[0].directblock,
			session.peers[2].directblock
		);
		await waitForPeersBlock(
			session.peers[1].directblock,
			session.peers[2].directblock
		);

		db1 = await client1.open(
			new EventStore<string>({
				id: uuid(),
			}),
			{ role: new ReplicatorType() }
		);

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			))!,
			{ role: new ReplicatorType() }
		);

		await waitForPeers(session.peers[2], [client1.id], db1.address!.toString()); // TODO is this needed?
		await waitForPeers(session.peers[2], [client2.id], db1.address!.toString()); // TODO is this needed?

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			(await db1.store.oplog.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		expect(db2.store.oplog.values.length).toEqual(2);
	});

	it("will share entries as replicator on peer connect", async () => {
		session = await LSession.connected(2);

		client1 = await Peerbit.create({
			libp2p: session.peers[0],
		});
		client2 = await Peerbit.create({
			libp2p: session.peers[1],
		});
		await waitForPeersBlock(
			session.peers[0].directblock,
			session.peers[1].directblock
		);

		db1 = await client1.open(
			new EventStore<string>({
				id: uuid(),
			}),
			{ role: new ReplicatorType() }
		);

		await db1.add("hello");
		await db1.add("world");

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			))!,
			{ role: new ReplicatorType() }
		);

		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
		await waitForPeers(session.peers[0], [client2.id], db1.address.toString());
		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
		await waitForPeers(session.peers[0], [client2.id], db1.address.toString());

		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			(await db1.store.oplog.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		await waitFor(() => db2.store.oplog.values.length === 2);
	});

	it("will share entries as observer on peer connect", async () => {
		session = await LSession.connected(2);

		client1 = await Peerbit.create({
			libp2p: session.peers[0],
		});
		client2 = await Peerbit.create({
			libp2p: session.peers[1],
		});

		await waitForPeersBlock(
			session.peers[0].directblock,
			session.peers[1].directblock
		);

		db1 = await client1.open(
			new EventStore<string>({
				id: uuid(),
			}),
			{ role: new ObserverType() }
		);

		await db1.add("hello");
		await db1.add("world");

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			))!,
			{ role: new ReplicatorType() }
		);

		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
		await waitForPeers(session.peers[0], [client2.id], db1.address.toString());
		await waitForPeers(session.peers[0], [client2.id], db1.address.toString());
		expect(client1.libp2p.directsub.topics.has(db1.address.toString())).toEqual(
			true
		);

		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			(await db1.store.oplog.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		await waitFor(() => db2.store.oplog.values.length === 2);
	});
});
