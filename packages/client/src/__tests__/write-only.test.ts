import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";

describe(`Write-only`, () => {
	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		db1: EventStore<string>,
		db2: EventStore<string>;

	beforeAll(async () => {
		session = await LSession.connected(2);
	});

	afterAll(async () => {
		await session.stop();
	});

	beforeEach(async () => {
		client1 = await Peerbit.create({ libp2p: session.peers[0] });
		client2 = await Peerbit.create({
			libp2p: session.peers[1],
			limitSigning: true,
		}); // limitSigning = dont sign exchange heads request
		db1 = await client1.open(
			new EventStore<string>({
				id: "abc",
			})
		);
	});

	afterEach(async () => {
		if (client1) await client1.stop();
		if (client2) await client2.stop();
	});

	it("write 1 entry replicate false", async () => {
		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
		db2 = await client2.open<EventStore<string>>(
			(await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
				db1.address!
			))!,
			{ replicate: false }
		);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.store.oplog.values.length === 2);
		expect(
			db1.store.oplog.values.map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		expect(db2.store.oplog.values.length).toEqual(1);
	});

	/* it("encrypted clock sync write 1 entry replicate false", async () => {
		await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
		const encryptionKey = await client1.keystore.createEd25519Key({
			id: "encryption key",
			group: topic,
		});
		db2 = await client2.open<EventStore<string>>(
			await EventStore.load<EventStore<string>>(
				client2.libp2p.directblock,
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
			db1.store.oplog.values.map((x) => x.payload.getValue().value)
		).toContainAllValues(["hello", "world"]);
		expect(db2.store.oplog.values.length).toEqual(1);
	}); */
});
