import { waitFor } from "@peerbit/time";
import { EventStore } from "./utils/stores/event-store";
import { TestSession } from "@peerbit/test-utils";
import { Observer, Replicator } from "../role";

describe(`role`, () => {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>;

	beforeAll(async () => {
		session = await TestSession.disconnected(3);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]]
		]);
		await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);
		await session.peers[2].services.blocks.waitFor(session.peers[1].peerId);
	});

	afterAll(async () => {
		await session.stop();
	});

	beforeEach(async () => {
		db1 = await session.peers[0].open(new EventStore<string>());
	});

	afterEach(async () => {
		await db1?.drop();
		await db2?.drop();
	});

	it("can update", async () => {
		expect(
			db1.log.node.services.pubsub["subscriptions"].get(db1.log.rpc.rpcTopic)
				.counter
		).toEqual(1);
		expect(db1.log.getReplicatorsSorted()?.map((x) => x.hash)).toEqual([
			db1.node.identity.publicKey.hashcode()
		]);
		expect(db1.log.role).toBeInstanceOf(Replicator);
		await db1.log.updateRole(new Observer());
		expect(db1.log.role).toBeInstanceOf(Observer);
		expect(
			db1.log.node.services.pubsub["subscriptions"].get(db1.log.rpc.rpcTopic)
				.counter
		).toEqual(1);
	});

	it("observer", async () => {
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: { role: new Observer() }
			}
		))!;

		await db1.waitFor(session.peers[1].peerId);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.values.length === 2); // db2 can write ...
		expect(
			(await db1.log.log.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.log.values.length).toEqual(1); // ... but will not receive entries
	});

	it("none", async () => {
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: { role: new Observer() }
			}
		))!;

		await db1.waitFor(session.peers[1].peerId);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.values.length === 2); // db2 can write ...
		expect(
			(await db1.log.log.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.log.values.length).toEqual(1); // ... but will not receive entries
	});

	it("sync", async () => {
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: { role: new Observer(), sync: () => true }
			}
		))!;

		await db1.waitFor(session.peers[1].peerId);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.values.length === 2); // db2 can write ...
		expect(
			(await db1.log.log.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);

		await waitFor(() => db2.log.log.values.length === 2); // ... since syncAll: true

		await db2.log.replicationReorganization();
		expect(db2.log.log.values.length).toEqual(2);
	});
});

/* it("encrypted clock sync write 1 entry replicate false", async () => {
	await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
	const encryptionKey = await client1.keystore.createEd25519Key({
		id: "encryption key",
		group: topic,
	});
	db2 = await client2.open<EventStore<string>>(
		await EventStore.load<EventStore<string>>(
			client2.libp2p.services.blocks,
			db1.address!
		),
		{ replicate: false }
	);

	await db1.add("hello", {
		receiver: {
			next: encryptionKey.keypair.publicKey,
			meta: encryptionKey.keypair.publicKey,
			payload: encryptionKey.keypair.publicKey,
			signatures: encryptionKey.keypair.publicKey,
		},
	});


	// Now the db2 will request sync clocks even though it does not replicate any content
	await db2.add("world");

	await waitFor(() => db1.store.oplog.values.length === 2);
	expect(
		db1.store.oplog.values.toArray().map((x) => x.payload.getValue().value)
	).toContainAllValues(["hello", "world"]);
	expect(db2.store.oplog.values.length).toEqual(1);
}); */
