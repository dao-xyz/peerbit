/* 
import { Peerbit } from "../peer.js";
import { EventStore, Operation } from "./utils/stores/event-store";
import { Ed25519Keypair, X25519PublicKey } from "@peerbit/crypto";
import { waitFor, waitForAsync } from "@peerbit/time";
import { base58btc } from "multiformats/bases/base58";

// Include test utilities
import { LSession } from "@peerbit/test-utils";

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
	let db1: EventStore<string>, db2: EventStore<string>;
	let recieverKey: Ed25519Keypair;

	beforeAll(async () => {});
	beforeEach(async () => {
		session = await LSession.connected(2);

		await session.peers[0].keychain.createKey("receiver", "Ed25519");
		recieverKey = await Ed25519Keypair.fromPeerId(
			await session.peers[1].keychain.exportPeerId("receiver")
		);
		db1 = await session.peers[0].open(new EventStore());
	});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
		await session.stop();
	});

	afterAll(async () => {});

	it("can encrypt by peerId", async () => {
		db2 = await session.peers[1].open<EventStore<string>>(db1.address!);
		await db1.waitFor(session.peers[1].peerId);
		await addHello(db1, session.peers[1].identity.publicKey);
		await waitFor(() => db2.log.length === 1);
		await checkHello(db2);
	});

	it("can encrypt with custom key", async () => {
		db2 = await session.peers[1].open<EventStore<string>>(db1.address!);
		await db1.waitFor(session.peers[1].peerId);

		await session.peers[1].importKeypair(recieverKey);
		expect(
			await session.peers[1].exportKeypair(recieverKey.publicKey)
		).toBeDefined();

		expect(
			await session.peers[1].exportKeypair(
				await X25519PublicKey.from(recieverKey.publicKey)
			)
		).toBeDefined();

		await addHello(db1, recieverKey.publicKey);
		await waitFor(() => db2.log.length === 1);
		await checkHello(db2);
	});
});

*/

it("_", () => {
	// TODO
});
