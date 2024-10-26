import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { DirectBlock } from "../src/libp2p.js";

const store = (s: TestSession<{ blocks: DirectBlock }>, i: number) =>
	s.peers[i].services.blocks;

describe("transport", function () {
	let session: TestSession<{ blocks: DirectBlock }>;

	afterEach(async () => {
		await session.stop();
	});

	// TODO feat(!) before libpip 0.46 this was possible, now connections are terminated for some reason
	// could be mplex that is aborting
	// autiodialer could perhaps help?

	/* it("can restart", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForPeers(store(session, 0), store(session, 1));
		await store(session, 0).stop();
		await store(session, 1).stop();
		await store(session, 0).start();
		await store(session, 1).start();
		await waitForPeers(store(session, 0), store(session, 1));
	}); */

	it("write then read over relay", async () => {
		session = await TestSession.disconnected(3, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		await store(session, 0).start();
		await store(session, 1).start();
		await store(session, 2).start();

		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);

		await waitForNeighbour(store(session, 0), store(session, 1));
		await waitForNeighbour(store(session, 1), store(session, 2));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);

		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");
		const readData = await store(session, 2).get(cid, { remote: true });
		expect(new Uint8Array(readData!)).to.deep.equal(data);
	});

	it("read while join over relay", async () => {
		session = await TestSession.disconnected(3, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		await store(session, 0).start();
		await store(session, 1).start();
		await store(session, 2).start();

		await session.connect([[session.peers[0], session.peers[1]]]);

		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const readDataPromise = store(session, 2).get(cid, {
			remote: { timeout: 5000 },
		});
		await delay(1000);
		await session.connect([[session.peers[1], session.peers[2]]]);

		const readData = await readDataPromise;
		expect(readData).to.exist;
		expect(new Uint8Array(readData!)).to.deep.equal(data);
	});

	it("read concurrently", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		await store(session, 0).start();
		await store(session, 1).start();

		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);

		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");
		const publish = sinon.spy(
			store(session, 1)["remoteBlocks"].options.publish,
		);
		store(session, 1)["remoteBlocks"].options.publish = publish;

		const promises: Promise<any>[] = [];
		for (let i = 0; i < 100; i++) {
			promises.push(store(session, 1).get(cid, { remote: true }));
		}
		const resolved = await Promise.all(promises);
		expect(publish.calledOnce).to.be.true;
		for (const b of resolved) {
			expect(new Uint8Array(b!)).to.deep.equal(data);
		}
	});

	it("get from specific peer", async () => {
		session = await TestSession.disconnected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);

		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");
		const readDataPromise = store(session, 1).get(cid, {
			remote: { from: [session.peers[0].services.blocks.publicKey.hashcode()] },
		});

		await session.connect(); // we connect after get request is sent
		await waitForNeighbour(store(session, 0), store(session, 1));

		expect(new Uint8Array((await readDataPromise)!)).to.deep.equal(data);
	});

	it("reads from joining peer", async () => {
		session = await TestSession.disconnected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);

		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");
		const readDataPromise = store(session, 1).get(cid, { remote: true });

		await session.connect(); // we connect after get request is sent
		await waitForNeighbour(store(session, 0), store(session, 1));

		expect(new Uint8Array((await readDataPromise)!)).to.deep.equal(data);
	});

	it("timeout", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const t1 = +new Date();
		const readData = await store(session, 0).get(
			"zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr",
			{ remote: { timeout: 3000 } },
		);
		const t2 = +new Date();
		expect(readData).equal(undefined);
		expect(t2 - t1 < 3100);
	});

	it("iterate", async () => {
		session = await TestSession.disconnected(1, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		await store(session, 0).stop();
		await store(session, 0).start();
		let once = false;
		for await (const resolved of store(session, 0).iterator()) {
			once = true;
			expect(resolved[0]).equal(cid);
		}
		expect(once).to.be.true;
	});

	/* it('can handle conurrent read/write', async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new AnyBlockStore())
		);
		await sx(session,0).start();
	
		sx(session,1) = new Blocks(
			new LibP2PBlockStore(session.peers[1], new AnyBlockStore())
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
				expect(readData).equal(rnds[i]);
			}
			promises.push(p());
		}
	
		await Promise.all(promises);
		const t3 = +new Date();
		console.log("xxx", t3 - t1);
	})
	it("large", async () => {
		store = new Blocks(
			new LibP2PBlockStore(session.peers[0], new AnyBlockStore())
		);
		await sx(session,0).start();
	
		sx(session,1) = new Blocks(
			new LibP2PBlockStore(session.peers[1], new AnyBlockStore())
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
				expect(readData).to.have.length(len);
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
				new LibP2PBlockStore(session.peers[0], new AnyBlockStore())
			);
			await sx(session,0).start();
	
			sx(session,1) = new Blocks(
				new LibP2PBlockStore(session.peers[1], new AnyBlockStore())
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
				expect(readData).to.have.length(len);
			}
			const t2 = +new Date();
			console.log("Small", t2 - t1);
		}); */
});
