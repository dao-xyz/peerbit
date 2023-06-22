import { waitFor } from "@peerbit/time";
import { EventStore } from "./utils/stores/event-store";

// Include test utilities
import { LSession } from "@peerbit/test-utils";
import { waitForPeers as waitForPeersBlock } from "@peerbit/stream";
import { WaitForPeer } from "@peerbit/stream-interface";
import { PeerId } from "@libp2p/interface-peer-id";

/**
 * Tests that are relavent for browser environments
 */

describe(`browser`, function () {
	let session: LSession;
	let db1: EventStore<string>, db2: EventStore<string>;

	afterAll(async () => {});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
		await session.stop();
	});

	it("can replicate entries", async () => {
		session = await LSession.connected(2);

		db1 = await new EventStore<string>().open(session.peers[0]);

		await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);

		db2 = await (await EventStore.load<EventStore<string>>(
			db1.address!,
			session.peers[1].services.blocks
		))!.open(session.peers[1]);

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.values.length === 2);
		expect(
			(await db1.log.log.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.log.values.length).toEqual(2);
	});

	it("can replicate entries through relay", async () => {
		session = await LSession.disconnected(3);

		// peer 3 is relay, and dont connect 1 with 2 directly
		session.peers[0].dial(session.peers[2].getMultiaddrs()[0]);
		session.peers[1].dial(session.peers[2].getMultiaddrs()[0]);

		await session.peers[0].services.blocks.waitFor(session.peers[2].peerId);
		await session.peers[1].services.blocks.waitFor(session.peers[2].peerId);

		db1 = await new EventStore<string>().open(session.peers[0]);

		db2 = await (await EventStore.load<EventStore<string>>(
			db1.address!,
			session.peers[1].services.blocks
		))!.open(session.peers[1]);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.values.length === 2);
		expect(
			(await db1.log.log.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.log.values.length).toEqual(2);
	});

	it("will share entries as replicator on peer connect", async () => {
		session = await LSession.connected(2);

		await session.peers[0].services.blocks.waitFor(session.peers[2].peerId);

		db1 = await new EventStore<string>().open(session.peers[0]);

		await db1.add("hello");
		await db1.add("world");

		db2 = await (await EventStore.load<EventStore<string>>(
			db1.address!,
			session.peers[1].services.blocks
		))!.open(session.peers[1]);

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);

		await waitFor(() => db1.log.log.values.length === 2);
		expect(
			(await db1.log.log.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		await waitFor(() => db2.log.log.values.length === 2);
	});

	it("will share entries as observer on peer connect", async () => {
		session = await LSession.connected(2);

		await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);

		db1 = await new EventStore<string>().open(session.peers[0]);

		await db1.add("hello");
		await db1.add("world");

		db2 = await (await EventStore.load<EventStore<string>>(
			db1.address!,
			session.peers[1].services.blocks
		))!.open(session.peers[1]);

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);

		await waitFor(() => db1.log.log.values.length === 2);
		expect(
			(await db1.log.log.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		await waitFor(() => db2.log.log.values.length === 2);
	});
});
