import { waitFor } from "@dao-xyz/peerbit-time";
import { Peerbit } from "../peer.js";
import { EventStore } from "./utils/stores/event-store";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
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

		db1 = await client1.open(new EventStore<string>(), {
			role: new ReplicatorType(),
		});
		await waitForPeersBlock(
			session.peers[0].services.blocks,
			session.peers[1].services.blocks
		);
		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!,
			{ role: new ReplicatorType() }
		);

		await db1.waitFor(client2.libp2p);
		await db2.waitFor(client1.libp2p);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.values.length === 2);
		expect(
			(await db1.log.values.toArray()).map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.values.length).toEqual(2);
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
			session.peers[0].services.blocks,
			session.peers[2].services.blocks
		);
		await waitForPeersBlock(
			session.peers[1].services.blocks,
			session.peers[2].services.blocks
		);

		db1 = await client1.open(new EventStore<string>(), {
			role: new ReplicatorType(),
		});

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!,
			{ role: new ReplicatorType() }
		);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.values.length === 2);
		expect(
			(await db1.log.values.toArray()).map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.values.length).toEqual(2);
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
			session.peers[0].services.blocks,
			session.peers[1].services.blocks
		);

		db1 = await client1.open(new EventStore<string>(), {
			role: new ReplicatorType(),
		});

		await db1.add("hello");
		await db1.add("world");

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!,
			{ role: new ReplicatorType() }
		);

		await db1.waitFor(client2.libp2p);
		await db2.waitFor(client1.libp2p);

		await waitFor(() => db1.log.values.length === 2);
		expect(
			(await db1.log.values.toArray()).map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		await waitFor(() => db2.log.values.length === 2);
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
			session.peers[0].services.blocks,
			session.peers[1].services.blocks
		);

		db1 = await client1.open(new EventStore<string>(), {
			role: new ObserverType(),
		});

		await db1.add("hello");
		await db1.add("world");

		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!,
			{ role: new ReplicatorType() }
		);

		await db1.waitFor(client2.libp2p);
		await db2.waitFor(client1.libp2p);

		await waitFor(() => db1.log.values.length === 2);
		expect(
			(await db1.log.values.toArray()).map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		await waitFor(() => db2.log.values.length === 2);
	});
});
