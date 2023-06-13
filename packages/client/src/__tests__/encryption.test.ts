import { Entry } from "@dao-xyz/peerbit-log";
import { Peerbit } from "../peer.js";
import { EventStore, Operation } from "./utils/stores/event-store";
import { Ed25519Keypair, X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { waitFor, waitForAsync } from "@dao-xyz/peerbit-time";
import { base58btc } from "multiformats/bases/base58";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";

const addHello = async (db: EventStore<string>, receiver: X25519PublicKey) => {
	await db.add("hello", {
		reciever: {
			metadata: receiver,
			next: receiver,
			payload: receiver,
			signatures: receiver,
		},
	});
};
const checkHello = async (db: EventStore<string>) => {
	await waitForAsync(
		async () => (await db.iterator({ limit: -1 })).collect().length === 1
	);

	const entries: Entry<Operation<string>>[] = (
		await db.iterator({ limit: -1 })
	).collect();

	expect(entries.length).toEqual(1);
	await entries[0].getPayload();
	expect(entries[0].payload.getValue().value).toEqual("hello");
};

describe(`encryption`, function () {
	let session: LSession;
	let client1: Peerbit,
		client2: Peerbit,
		db1: EventStore<string>,
		db2: EventStore<string>;
	let recieverKey: Ed25519Keypair;

	beforeAll(async () => {});
	beforeEach(async () => {
		session = await LSession.connected(2);

		client1 = await Peerbit.create({ libp2p: session.peers[0] });

		// Trusted client 2
		client2 = await Peerbit.create({ libp2p: session.peers[1] });

		await client2.libp2p.keychain.createKey("receiver", "Ed25519");
		recieverKey = await Ed25519Keypair.fromPeerId(
			await client2.libp2p.keychain.exportPeerId("receiver")
		);
		db1 = await client1.open(new EventStore());
	});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();

		if (client1) {
			await client1.stop();
		}
		if (client2) {
			await client2.stop();
		}

		await session.stop();
	});

	afterAll(async () => {});

	it("replicates database of 1 entry known keys", async () => {
		db2 = await client2.open<EventStore<string>>(db1.address);
		await db1.waitFor(client2.libp2p);

		await client2.importKeypair(recieverKey);
		expect(
			await client2.libp2p.keychain.exportPeerId(
				base58btc.encode(recieverKey.publicKey.bytes)
			)
		).toBeDefined();

		expect(
			await client2.libp2p.keychain.exportPeerId(
				base58btc.encode(
					(
						await X25519PublicKey.from(recieverKey.publicKey)
					).bytes
				)
			)
		).toBeDefined();

		await addHello(db1, recieverKey.publicKey);
		await waitFor(() => db2.log.length === 1);
		await checkHello(db2);
	});
});
