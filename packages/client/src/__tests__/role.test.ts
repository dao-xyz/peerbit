import { waitFor } from "@dao-xyz/peerbit-time";
import { Peerbit } from "../peer.js";
import { EventStore } from "./utils/stores/event-store";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { Observer } from "@dao-xyz/peerbit-program";
import { waitForPeers } from "@dao-xyz/libp2p-direct-stream";

describe(`Write-only`, () => {
	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		db1: EventStore<string>,
		db2: EventStore<string>;

	beforeAll(async () => {
		session = await LSession.disconnected(3);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);
		await waitForPeers(
			...session.peers.slice(0, 2).map((x) => x.services.blocks)
		);
		await waitForPeers(
			...session.peers.slice(1, 3).map((x) => x.services.blocks)
		);
	});

	afterAll(async () => {
		await session.stop();
	});

	beforeEach(async () => {
		client1 = await Peerbit.create({ libp2p: session.peers[0] });
		client2 = await Peerbit.create({
			libp2p: session.peers[2],
			limitSigning: true,
		}); // limitSigning = dont sign exchange heads request
		db1 = await client1.open(new EventStore<string>());
	});

	afterEach(async () => {
		if (client1) await client1.stop();
		if (client2) await client2.stop();
	});

	it("observer", async () => {
		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!,
			{ role: new Observer() }
		);

		await db1.waitFor(client2.libp2p);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.values.length === 2); // db2 can write ...
		expect(
			(await db1.log.values.toArray()).map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.values.length).toEqual(1); // ... but will not recieve entries
	});

	it("none", async () => {
		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!,
			{ role: new Observer() }
		);
		await db1.waitFor(client2.libp2p);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.values.length === 2); // db2 can write ...
		expect(
			(await db1.log.values.toArray()).map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.values.length).toEqual(1); // ... but will not recieve entries
	});

	it("sync", async () => {
		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.services.blocks,
				db1.address!
			))!,
			{ role: new Observer(), sync: () => true }
		);

		await db1.waitFor(client2.libp2p);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.values.length === 2); // db2 can write ...
		expect(
			(await db1.log.values.toArray()).map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);

		await waitFor(() => db2.log.values.length === 2); // ... since syncAll: true

		await client2.replicationReorganization([...client2.programs.keys()]);
		expect(db2.log.values.length).toEqual(2);
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
		reciever: {
			next: encryptionKey.keypair.publicKey,
			metadata: encryptionKey.keypair.publicKey,
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
