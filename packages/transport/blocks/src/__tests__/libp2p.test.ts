import { LSession } from "@peerbit/libp2p-test-utils";
import { getBlockValue } from "../block.js";
import { DirectBlock } from "..";
import { waitForPeers } from "@peerbit/stream";

const store = (s: LSession<{ blocks: DirectBlock }>, i: number) =>
	s.peers[i].services.blocks;

describe("transport", function () {
	let session: LSession<{ blocks: DirectBlock }>;

	afterEach(async () => {
		await session.stop();
	});

	it("can restart", async () => {
		session = await LSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForPeers(store(session, 0), store(session, 1));
		await store(session, 0).stop();
		await store(session, 1).stop();

		await store(session, 0).start();
		await store(session, 1).start();
		await waitForPeers(store(session, 0), store(session, 1));
	});

	it("rw", async () => {
		session = await LSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		await store(session, 0).start();
		await store(session, 1).start();

		await waitForPeers(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);

		expect(cid).toEqual("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");
		const readData = await store(session, 1).get(cid);
		expect(new Uint8Array(readData!)).toEqual(data);
	});

	it("read concurrently", async () => {
		session = await LSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		await store(session, 0).start();
		await store(session, 1).start();

		await waitForPeers(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);

		expect(cid).toEqual("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");
		let publishCounter = 0;
		const publish = store(session, 1).publish.bind(store(session, 1));
		store(session, 1).publish = (d, o) => {
			publishCounter += 1;
			return publish(d, o);
		};
		const promises: Promise<any>[] = [];
		for (let i = 0; i < 100; i++) {
			promises.push(store(session, 1).get(cid));
		}
		const resolved = await Promise.all(promises);
		expect(publishCounter).toEqual(1);
		for (const b of resolved) {
			expect(new Uint8Array(b!)).toEqual(data);
		}
	});

	it("reads from joining peer", async () => {
		session = await LSession.disconnected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);

		expect(cid).toEqual("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");
		const readDataPromise = store(session, 1).get(cid);

		await session.connect(); // we connect after get request is sent
		await waitForPeers(store(session, 0), store(session, 1));

		expect(new Uint8Array((await readDataPromise)!)).toEqual(data);
	});

	it("timeout", async () => {
		session = await LSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForPeers(store(session, 0), store(session, 1));

		const t1 = +new Date();
		const readData = await store(session, 0).get(
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
		await sx(session,0).start();
	
		sx(session,1) = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);
		await sx(session,1).start();
		await session.connect();
		await waitForPeers(sx(session,0), sx(session,1));
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
				const cid = await sx(session,0).put(rnds[i], "raw", { pin: true });
				const readData = await sx(session,1).get<Uint8Array>(cid);
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
		await sx(session,0).start();
	
		sx(session,1) = new Blocks(
			new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
		);
		await sx(session,1).start();
		await session.connect();
		await waitForPeers(sx(session,0), sx(session,1));
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
			promises.push(sx(session,0).put(rnds[i], "raw", { pin: true }));
		}
		cids = await Promise.all(promises);
	
		const t2 = +new Date();
	
		let readPromises: Promise<void>[] = [];
		for (const [i, cid] of cids.entries()) {
			const p = async () => {
				const readData = await sx(session,1).get<Uint8Array>(cid);
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
			await sx(session,0).start();
	
			sx(session,1) = new Blocks(
				new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
			);
	
			await sx(session,1).start();
			await session.connect();
			await waitForPeers(sx(session,0), sx(session,1));
	
			const cids: string[] = [];
	
			const rnds: Uint8Array[] = [];
			let len = 100;
			const t1 = +new Date();
			for (let i = 0; i < 5000; i++) {
				rnds.push(crypto.randomBytes(len));
			}
	
			for (let i = 0; i < 5000; i++) {
				cids.push(await sx(session,0).put(rnds[i], "raw", { pin: true }));
			}
	
			for (const [i, cid] of cids.entries()) {
				const readData = await sx(session,1).get<Uint8Array>(cid);
				expect(readData).toHaveLength(len);
			}
			const t2 = +new Date();
			console.log("Small", t2 - t1);
		}); */
});
