import { LSession } from "@dao-xyz/libp2p-test-utils";
import { MemoryLevelBlockStore } from "../level";
import { createBlock, getBlockValue, stringifyCid } from "../block.js";
import { DirectBlock } from "..";
import { delay } from "@dao-xyz/peerbit-time";
import { waitForPeers } from "./utils";

describe("transport", function () {
	let session: LSession, store: DirectBlock, store2: DirectBlock;

	afterEach(async () => {
		await store?.close();
		await store2?.close();
		await session.stop();
	});

	it("can restart", async () => {
		session = await LSession.connected(2);

		store = new DirectBlock(session.peers[0], new MemoryLevelBlockStore());
		await store.open();
		store2 = new DirectBlock(session.peers[1], new MemoryLevelBlockStore());

		await store2.open();

		await waitForPeers(store, store2);
		await store.close();
		await store2.close();

		await delay(1000); // Some delay seems to be necessary TODO fix
		await store.open();
		await store2.open();
		await waitForPeers(store, store2);
	});

	it("rw", async () => {
		session = await LSession.connected(2);

		store = new DirectBlock(session.peers[0], new MemoryLevelBlockStore());
		store2 = new DirectBlock(session.peers[1], new MemoryLevelBlockStore());

		expect((store as DirectBlock)._gossip).toBeFalse();

		await store.open();
		await store2.open();

		await waitForPeers(store, store2);

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store.put(await createBlock(data, "raw"));

		expect(stringifyCid(cid)).toEqual(
			"zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J"
		);
		const readData = await store2.get<Uint8Array>(stringifyCid(cid));
		expect(await getBlockValue(readData!)).toEqual(data);
	});

	it("read concurrently", async () => {
		session = await LSession.connected(2);

		store = new DirectBlock(session.peers[0], new MemoryLevelBlockStore());
		store2 = new DirectBlock(session.peers[1], new MemoryLevelBlockStore());

		expect((store as DirectBlock)._gossip).toBeFalse();

		await store.open();
		await store2.open();

		await waitForPeers(store, store2);

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store.put(await createBlock(data, "raw"));

		expect(stringifyCid(cid)).toEqual(
			"zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J"
		);
		let publishCounter = 0;
		const publish = store2.publish.bind(store2);
		store2.publish = (d, o) => {
			publishCounter += 1;
			return publish(d, o);
		};
		const readDataPromise1 = store2.get<Uint8Array>(stringifyCid(cid));
		const readDataPromise2 = store2.get<Uint8Array>(stringifyCid(cid));
		const readData1 = await readDataPromise1;
		const readData2 = await readDataPromise2;
		expect(publishCounter).toEqual(1);
		expect(await getBlockValue(readData1!)).toEqual(data);
		//	expect(await getBlockValue(readData2!)).toEqual(data);
	});

	it("reads from joining peer", async () => {
		session = await LSession.disconnected(2);

		store = new DirectBlock(session.peers[0], new MemoryLevelBlockStore());
		store2 = new DirectBlock(session.peers[1], new MemoryLevelBlockStore());

		expect((store as DirectBlock)._gossip).toBeFalse();

		await store.open();
		await store2.open();

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store.put(await createBlock(data, "raw"));

		expect(stringifyCid(cid)).toEqual(
			"zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J"
		);
		const readDataPromise = store2.get<Uint8Array>(stringifyCid(cid));

		await session.connect(); // we connect after get request is sent
		await waitForPeers(store, store2);

		expect(await getBlockValue((await readDataPromise)!)).toEqual(data);
	});

	it("timeout", async () => {
		session = await LSession.connected(2);

		store = new DirectBlock(session.peers[0], new MemoryLevelBlockStore());
		await store.open();
		store2 = new DirectBlock(session.peers[1], new MemoryLevelBlockStore());
		await store2.open();

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

	/* it('can handle conurrent read/write', async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
		);
		await store.open();

		store2 = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);
		await store2.open();
		await session.connect();
		await waitForPeers(store, store2);
		const rnds: Uint8Array[] = [];
		let len = 390;//0000;
		let count = 1000;
		for (let i = 0; i < count; i++) {
			rnds.push(new Uint8Array(crypto.randomBytes(len)));
		}

		const t1 = +new Date();
		let promises: Promise<any>[] = [];
		for (let i = 0; i < count; i++) {
			const p = async () => {
				const cid = await store.put(rnds[i], "raw", { pin: true });
				const readData = await store2.get<Uint8Array>(stringifyCid(cid));
				expect(readData).toEqual(rnds[i]);
			}
			promises.push(p());
		}

		await Promise.all(promises);
		const t3 = +new Date();
		console.log("xxx", t3 - t1);
	})
	it("large", async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
		);
		await store.open();

		store2 = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);
		await store2.open();
		await session.connect();
		await waitForPeers(store, store2);
		await delay(1000)
		let cids: string[] = [];

		const rnds: Uint8Array[] = [];
		let len = 390;//0000;
		let count = 50000;
		for (let i = 0; i < count; i++) {
			rnds.push(crypto.randomBytes(len));
		}

		const t1 = +new Date();
		let promises: Promise<string>[] = [];
		for (let i = 0; i < count; i++) {
			promises.push(store.put(rnds[i], "raw", { pin: true }));
		}
		cids = await Promise.all(promises);

		const t2 = +new Date();

		let readPromises: Promise<void>[] = [];
		for (const [i, cid] of cids.entries()) {
			const p = async () => {
				const readData = await store2.get<Uint8Array>(stringifyCid(cid));
				expect(readData).toHaveLength(len);
			}
			readPromises.push(p())

		}
		await Promise.all(readPromises);
		const t3 = +new Date();
		console.log("Large", t3 - t1, t2 - t1, t3 - t2);

	}); */
	/* 
	
		it("small", async () => {
			store = new Blocks(
				new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
			);
			await store.open();
	
			store2 = new Blocks(
				new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
			);
	
			await store2.open();
			await session.connect();
			await waitForPeers(store, store2);
	
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
		}); */
});
