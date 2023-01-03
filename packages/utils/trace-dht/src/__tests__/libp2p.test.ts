import { LSession } from "@dao-xyz/peerbit-test-utils";
import { LibP2PBlockStore } from "../libp2p";
import { MemoryLevelBlockStore } from "../level";
import { stringifyCid } from "../block.js";
import { Blocks } from "..";
import { waitFor, delay } from "@dao-xyz/peerbit-time";
import crypto from "crypto";

const waitForPeers = async (...stores: Blocks[]) => {
	for (let i = 0; i < stores.length; i++) {
		await waitFor(() => (stores[i]._store as LibP2PBlockStore)._blockSub.peers.size === stores.length - 1);
		const peers = (stores[i]._store as LibP2PBlockStore)._blockSub.peers;
		for (const peer of peers.values()) {
			await waitFor(() => peer.isWritable)
		}
	}
}

describe('transport', function () {
	let session: LSession, store: Blocks, store2: Blocks;


	beforeEach(async () => {
		session = await LSession.connected(2);
	})

	afterEach(async () => {
		await store?.close();
		await store2?.close();
		await session.stop();

	});

	it('can restart', async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
		);
		await store.open();
		store2 = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);

		await store2.open();

		await waitForPeers(store, store2);
		await store.close();
		await store2.close();

		await delay(1000); // Some delay seems to be necessary TODO fix
		await store.open()
		await store2.open();
		await waitForPeers(store, store2)

	})


	it("rw", async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
		);

		store2 = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);

		expect((store._store as LibP2PBlockStore)._gossipCache).toBeUndefined();
		expect((store._store as LibP2PBlockStore)._gossip).toBeFalse();

		await store.open();
		await store2.open();
		// await session.connect();

		//await waitForPeers(store, store2);


		const data = new Uint8Array([5, 4, 3]);
		const cid = await store.put(data, "raw", { pin: true });

		expect(stringifyCid(cid)).toEqual(
			"zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J"
		);
		const readData = await store2.get<Uint8Array>(stringifyCid(cid));
		expect(readData).toEqual(data);
	});

	it("large", async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
		);
		await store.open();

		store2 = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);
		await store2.open();

		const cids: string[] = [];

		const rnds: Uint8Array[] = [];
		let len = 3900000;
		for (let i = 0; i < 100; i++) {
			rnds.push(crypto.randomBytes(len));
		}

		const t1 = +new Date();
		for (let i = 0; i < 100; i++) {
			cids.push(await store.put(rnds[i], "raw", { pin: true }));
		}

		for (const [i, cid] of cids.entries()) {
			const readData = await store2.get<Uint8Array>(stringifyCid(cid));
			expect(readData).toHaveLength(len);
		}
		const t2 = +new Date();
		console.log("Large", t2 - t1);
	});


	it("small", async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
		);
		await store.open();

		store2 = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);

		await store2.open();
		// await session.connect();

		//  await waitForPeers(store, store2);

		const cids: string[] = [];

		const rnds: Uint8Array[] = [];
		let len = 100;
		const t1 = +new Date();
		for (let i = 0; i < 5000; i++) {
			rnds.push(crypto.randomBytes(len));
		}

		for (let i = 0; i < 5000; i++) {
			cids.push(await store.put(rnds[i], "raw", { pin: true }));
		}

		for (const [i, cid] of cids.entries()) {
			const readData = await store2.get<Uint8Array>(stringifyCid(cid));
			expect(readData).toHaveLength(len);
		}
		const t2 = +new Date();
		console.log("Small", t2 - t1);
	});
	it("timeout", async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
		);
		await store.open();
		store2 = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);
		await store2.open();

		// await session.connect();
		await waitForPeers(store, store2);


		const t1 = +new Date();
		const readData = await store.get<Uint8Array>(
			"zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr",
			{ timeout: 3000 }
		);
		const t2 = +new Date();
		expect(readData).toBeUndefined();
		expect(t2 - t1 < 3100);
	});

	it("gossip", async () => {
		store = new Blocks(
			new LibP2PBlockStore(
				session.peers[0],
				new MemoryLevelBlockStore(),
				{ gossip: { cache: {} } }
			)
		);
		await store.open();

		store2 = new Blocks(
			new LibP2PBlockStore(session.peers[1], undefined, {
				gossip: { cache: {} },
			})
		);
		await store2.open();
		// await session.connect();
		await waitForPeers(store, store2);

		const data = new Uint8Array([1, 2, 3]);


		const cid = await store.put(data, "raw", { pin: true });
		expect(stringifyCid(cid)).toEqual(
			"zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU"
		);

		await delay(5000);

		await waitFor(
			() => (store2._store as LibP2PBlockStore)._gossipCache!.size === 1
		);

		(store2._store as LibP2PBlockStore)._readFromPubSub = () =>
			Promise.resolve(undefined); // make sure we only read from gossipCache

		const readData = await store2.get<Uint8Array>(stringifyCid(cid));
		expect(readData).toEqual(data);
	});


});
