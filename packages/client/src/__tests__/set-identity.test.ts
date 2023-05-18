import { Peerbit } from "../peer.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { EventStore } from "./utils/stores";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { MemoryLevel } from "memory-level";

export const createStore = (): MemoryLevel<string, Uint8Array> => {
	return new MemoryLevel<string, Uint8Array>({ valueEncoding: "view" });
};

describe(`Set identities`, function () {
	let session: LSession, client: Peerbit, keystore: Keystore, options: any;
	let signKey1: KeyWithMeta<Ed25519Keypair>,
		signKey2: KeyWithMeta<Ed25519Keypair>;

	beforeAll(async () => {
		session = await LSession.connected(1);

		const identityStore = await createStore();

		keystore = new Keystore(identityStore);
		signKey1 =
			(await keystore.createEd25519Key()) as KeyWithMeta<Ed25519Keypair>;
		signKey2 =
			(await keystore.createEd25519Key()) as KeyWithMeta<Ed25519Keypair>;

		client = await Peerbit.create({ libp2p: session.peers[0] });
	});

	afterAll(async () => {
		await keystore.close();
		if (client) await client.stop();

		await session.stop();
	});

	beforeEach(async () => {
		options = Object.assign({}, options, {});
	});

	it("sets identity", async () => {
		const db = await client.open(new EventStore<string>(), options);
		expect(db.log.identity.publicKey.equals(client.identity.publicKey));
		db.log.setIdentity({
			publicKey: signKey1.keypair.publicKey,
			privateKey: signKey1.keypair.privateKey,
			sign: (data) => signKey1.keypair.sign(data),
		});
		expect(db.log.identity.publicKey.equals(signKey1.keypair.publicKey));
		await db.close();
	});
});
