import { serialize } from "@dao-xyz/borsh";
import { createBlock, stringifyCid } from "@peerbit/blocks-interface";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import {
	BACKGROUND_MESSAGE_PRIORITY,
	CONVERGENCE_MESSAGE_PRIORITY,
	FOREGROUND_READ_MESSAGE_PRIORITY,
} from "@peerbit/stream-interface";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { DirectBlock } from "../src/libp2p.js";
import {
	BlockRequest,
	BlockResponse,
	type RemoteBlocks,
} from "../src/remote.js";

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
		const readData = await store(session, 2).get(cid, {
			remote: { from: [store(session, 0).publicKeyHash] },
		});
		expect(new Uint8Array(readData!)).to.deep.equal(data);
	});

	it("drops an undecodable data frame instead of throwing out of the listener", async () => {
		session = await TestSession.disconnected(1, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await store(session, 0).start();

		const db = store(session, 0) as DirectBlock;
		const onMessageSpy = sinon.spy(
			(db as any).remoteBlocks as RemoteBlocks,
			"onMessage",
		);
		// A borsh BlockMessage with an unknown variant byte: the TS decoder
		// throws (the native decoder likewise throws, e.g. on a non-UTF-8
		// cid). The listener must swallow the throw and drop the frame rather
		// than let it escape (an uncaughtException in Node).
		const malformed = new Uint8Array([7, 7, 7]);
		const event = new CustomEvent("data", {
			detail: {
				data: malformed,
				header: { signatures: { publicKeys: [] } },
			},
		});
		expect(() => (db as any).onDataFn(event)).to.not.throw();
		expect(onMessageSpy.called).to.equal(false);

		// A well-formed frame still dispatches to onMessage.
		const ok = serialize(new BlockRequest("zb2rhbnwihVzMMEGAPf9EwTZ"));
		const okEvent = new CustomEvent("data", {
			detail: {
				data: ok,
				header: { signatures: { publicKeys: [] } },
			},
		});
		(db as any).onDataFn(okEvent);
		expect(onMessageSpy.called).to.equal(true);
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
			remote: { timeout: 5000, from: [store(session, 0).publicKeyHash] },
		});
		await delay(1000);
		await session.connect([[session.peers[1], session.peers[2]]]);

		const readData = await readDataPromise;
		expect(readData).to.exist;
		expect(new Uint8Array(readData!)).to.deep.equal(data);
	});

	it("reads from neighbour without explicit providers", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await store(session, 0).start();
		await store(session, 1).start();
		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const readData = await store(session, 1).get(cid, {
			remote: { timeout: 5000 },
		});
		expect(new Uint8Array(readData!)).to.deep.equal(data);
	});

	it("persists verified remote reads through the known-CID path", async () => {
		const onPut = sinon.spy();
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c, { onPut }) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		onPut.resetHistory();

		const requesterRemoteBlocks = (store(session, 1) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const put = sinon.spy(requesterRemoteBlocks.localStore, "put");
		const putKnown = sinon.spy(requesterRemoteBlocks.localStore, "putKnown");

		try {
			const readData = await store(session, 1).get(cid, {
				remote: {
					timeout: 5_000,
					from: [store(session, 0).publicKeyHash],
					replicate: true,
				},
			});

			expect(new Uint8Array(readData!)).to.deep.equal(data);
			expect(put.callCount).to.equal(0);
			expect(putKnown.callCount).to.equal(1);
			expect(putKnown.getCall(0).args[0]).to.equal(cid);
			expect(putKnown.getCall(0).args[1]).to.deep.equal(data);
			expect(onPut.callCount).to.equal(1);
			expect(onPut.getCall(0).args[0]).to.equal(cid);
			expect(await store(session, 1).has(cid)).to.equal(true);
		} finally {
			put.restore();
			putKnown.restore();
		}
	});

	it("preserves a verified DAG-CBOR CID when replicating remotely", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const block = await createBlock({ hello: "world" }, "dag-cbor");
		const cid = stringifyCid(block.cid);
		await store(session, 0).put({ block, cid });

		const requesterRemoteBlocks = (store(session, 1) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const put = sinon.spy(requesterRemoteBlocks.localStore, "put");
		const putKnown = sinon.spy(requesterRemoteBlocks.localStore, "putKnown");

		try {
			const readData = await store(session, 1).get(cid, {
				remote: {
					timeout: 5_000,
					from: [store(session, 0).publicKeyHash],
					replicate: true,
				},
			});

			expect(new Uint8Array(readData!)).to.deep.equal(block.bytes);
			expect(put.callCount).to.equal(0);
			expect(putKnown.callCount).to.equal(1);
			expect(putKnown.getCall(0).args[0]).to.equal(cid);
			expect(putKnown.getCall(0).args[1]).to.deep.equal(block.bytes);
			expect(await store(session, 1).has(cid)).to.equal(true);
		} finally {
			put.restore();
			putKnown.restore();
		}
	});

	it("does not persist a corrupt remote response", async () => {
		const onPut = sinon.spy();
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c, { onPut }) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		onPut.resetHistory();

		const requesterRemoteBlocks = (store(session, 1) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const providerRemoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const put = sinon.spy(requesterRemoteBlocks.localStore, "put");
		const putKnown = sinon.spy(requesterRemoteBlocks.localStore, "putKnown");
		const originalPublish = providerRemoteBlocks.options.publish;
		let corruptResponseCount = 0;

		providerRemoteBlocks.options.publish = async (message, options) => {
			if (message instanceof BlockResponse) {
				corruptResponseCount++;
				await requesterRemoteBlocks.onMessage(
					new BlockResponse(message.cid, new Uint8Array([9, 9, 9])),
					{ from: store(session, 0).publicKeyHash },
				);
				return;
			}
			return originalPublish(message, options);
		};

		try {
			const readData = await store(session, 1).get(cid, {
				remote: {
					timeout: 250,
					from: [store(session, 0).publicKeyHash],
					replicate: true,
				},
			});

			expect(readData).to.equal(undefined);
			expect(corruptResponseCount).to.be.greaterThan(0);
			expect(put.callCount).to.equal(0);
			expect(putKnown.callCount).to.equal(0);
			expect(onPut.callCount).to.equal(0);
			expect(await store(session, 1).has(cid)).to.equal(false);
		} finally {
			providerRemoteBlocks.options.publish = originalPublish;
			put.restore();
			putKnown.restore();
		}
	});

	it("puts known cid blocks through direct block storage", async () => {
		const onPut = sinon.spy();
		session = await TestSession.connected(1, {
			services: { blocks: (c) => new DirectBlock(c, { onPut }) },
		});
		await store(session, 0).start();

		const data = new Uint8Array([5, 4, 3]);
		const cid = "zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J";
		const cids = await store(session, 0).putKnownMany([[cid, data]]);

		expect(cids).to.deep.equal([cid]);
		expect(onPut.callCount).equal(1);
		expect(onPut.getCall(0).args[0]).equal(cid);
		expect(await store(session, 0).get(cid)).to.deep.equal(data);
	});

	it("puts a single known cid block through direct block storage", async () => {
		const onPut = sinon.spy();
		session = await TestSession.connected(1, {
			services: { blocks: (c) => new DirectBlock(c, { onPut }) },
		});
		await store(session, 0).start();

		const data = new Uint8Array([5, 4, 3]);
		const cid = "zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J";
		const storedCid = await store(session, 0).putKnown(cid, data);

		expect(storedCid).equal(cid);
		expect(onPut.callCount).equal(1);
		expect(onPut.getCall(0).args[0]).equal(cid);
		expect(await store(session, 0).get(cid)).to.deep.equal(data);
	});

	it("defers and de-duplicates stored block notifications", async () => {
		const onPut = sinon.spy();
		session = await TestSession.connected(1, {
			services: { blocks: (c) => new DirectBlock(c, { onPut }) },
		});
		await store(session, 0).start();

		const remoteBlocks = (store(session, 0) as any)
			.remoteBlocks as RemoteBlocks;
		const cid = "zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J";
		const secondCid = "zb2rhf3riC3q7Fxv2JtBMJw77QzZnDgNEQMk9GVK31qH4gcLx";

		remoteBlocks.notifyStoredDeferred(cid);
		remoteBlocks.notifyStoredManyDeferred([cid, secondCid]);

		expect(onPut.callCount).equal(0);
		await delay(10);
		expect(onPut.callCount).equal(2);
		expect(onPut.getCalls().map((call) => call.args[0])).to.have.members([
			cid,
			secondCid,
		]);
	});

	it("learns provider from response and reuses it", async () => {
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

		// First read uses an explicit provider (0) so the request can reach the true holder.
		const read1 = await store(session, 2).get(cid, {
			remote: { timeout: 5000, from: [store(session, 0).publicKeyHash] },
		});
		expect(new Uint8Array(read1!)).to.deep.equal(data);

		// Second read uses the learned provider cache (no explicit `from`).
		const read2 = await store(session, 2).get(cid, {
			remote: { timeout: 5000 },
		});
		expect(new Uint8Array(read2!)).to.deep.equal(data);
	});

	it("can seed providers for an in-flight get via hintProviders", async () => {
		session = await TestSession.disconnected(2, {
			services: {
				blocks: (c) =>
					new DirectBlock(c, {
						// Simulate program-level provider discovery: start empty, then add hints later.
						resolveProviders: () => [],
					}),
			},
		});

		await store(session, 0).start();
		await store(session, 1).start();

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const readPromise = store(session, 1).get(cid, {
			remote: { timeout: 10_000 },
		});

		// Connect after the get is already waiting (no explicit `remote.from`).
		await session.connect([[session.peers[0], session.peers[1]]]);
		await waitForNeighbour(store(session, 0), store(session, 1));

		// Provide a hint once the peer is reachable to avoid a "seek then deliver" roundtrip.
		store(session, 1).hintProviders?.(cid, [store(session, 0).publicKeyHash]);

		const read = await readPromise;
		expect(new Uint8Array(read!)).to.deep.equal(data);
	});

	it("can wake an in-flight get from watchProviders before retry polling", async () => {
		session = await TestSession.connected(2, {
			services: {
				blocks: (c) =>
					new DirectBlock(c, {
						resolveProviders: () => [],
						watchProviders: (_cid, { onProviders }) => {
							const timer = setTimeout(() => {
								onProviders([store(session, 0).publicKeyHash]);
							}, 50);
							return () => clearTimeout(timer);
						},
					}),
			},
		});

		await store(session, 0).start();
		await store(session, 1).start();
		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const read = await store(session, 1).get(cid, {
			remote: { timeout: 200 },
		});
		expect(new Uint8Array(read!)).to.deep.equal(data);
	});

	it("rechecks provider discovery quickly while a get is already waiting", async () => {
		let providersReady = false;
		session = await TestSession.connected(2, {
			services: {
				blocks: (c) =>
					new DirectBlock(c, {
						resolveProviders: () =>
							providersReady ? [store(session, 0).publicKeyHash] : [],
					}),
			},
		});

		await store(session, 0).start();
		await store(session, 1).start();
		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const startedAt = Date.now();
		const readPromise = store(session, 1).get(cid, {
			remote: { timeout: 10_000 },
		});

		setTimeout(() => {
			providersReady = true;
		}, 250);

		const read = await readPromise;
		expect(new Uint8Array(read!)).to.deep.equal(data);
		expect(Date.now() - startedAt).to.be.lessThan(3_000);
	});

	it("can recover when explicit providers are stale but resolver knows a better peer", async () => {
		session = await TestSession.disconnected(3, {
			services: {
				blocks: (c) =>
					new DirectBlock(c, {
						resolveProviders: () => [store(session, 0).publicKeyHash],
					}),
			},
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

		const read = await store(session, 1).get(cid, {
			remote: {
				timeout: 5_000,
				from: [store(session, 2).publicKeyHash],
			},
		});
		expect(new Uint8Array(read!)).to.deep.equal(data);
	});

	it("can bypass stale cached explicit providers on forced retry", async () => {
		session = await TestSession.disconnected(3, {
			services: {
				blocks: (c) =>
					new DirectBlock(c, {
						resolveProviders: () => [store(session, 0).publicKeyHash],
					}),
			},
		});

		await store(session, 0).start();
		await store(session, 1).start();
		await store(session, 2).start();

		await session.connect([[session.peers[0], session.peers[1]]]);

		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const read = await store(session, 1).get(cid, {
			remote: {
				timeout: 5_000,
				from: [store(session, 2).publicKeyHash],
			},
		});
		expect(new Uint8Array(read!)).to.deep.equal(data);
	});

	it("widens an in-flight read when a later caller supplies a better explicit provider", async () => {
		session = await TestSession.disconnected(3, {
			services: {
				blocks: (c) =>
					new DirectBlock(c, {
						resolveProviders: () => [],
						requeryOnReachable: 1,
					}),
			},
		});

		await store(session, 0).start();
		await store(session, 1).start();
		await store(session, 2).start();

		await session.connect([[session.peers[0], session.peers[1]]]);
		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const requesterRemoteBlocks = (store(session, 1) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const providerRemoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const originalRequesterPublish = requesterRemoteBlocks.options.publish;
		const originalProviderPublish = providerRemoteBlocks.options.publish;
		let requestsToProvider = 0;
		const staleRequestSeen = pDefer<void>();

		requesterRemoteBlocks.options.publish = async (
			message: any,
			options: any,
		) => {
			if (message instanceof BlockRequest) {
				const to = (options?.mode as any)?.to ?? [];
				if (to.includes(store(session, 2).publicKeyHash)) {
					staleRequestSeen.resolve();
				}
				if (to.includes(store(session, 0).publicKeyHash)) {
					requestsToProvider++;
					await providerRemoteBlocks.onMessage(message, {
						from: store(session, 1).publicKeyHash,
					});
				}
				return;
			}
			return originalRequesterPublish(message, options);
		};

		providerRemoteBlocks.options.publish = async (
			message: any,
			options: any,
		) => {
			if (message instanceof BlockResponse) {
				const to = (options?.to ??
					(options?.mode as any)?.to ??
					[]) as string[];
				if (to.includes(store(session, 1).publicKeyHash)) {
					await requesterRemoteBlocks.onMessage(message, {
						from: store(session, 0).publicKeyHash,
					});
				}
				return;
			}
			return originalProviderPublish(message, options);
		};

		try {
			const staleRead = store(session, 1).get(cid, {
				remote: {
					timeout: 5_000,
					from: [store(session, 2).publicKeyHash],
				},
			});

			await staleRequestSeen.promise;

			const widenedRead = store(session, 1).get(cid, {
				remote: {
					timeout: 5_000,
					from: [store(session, 0).publicKeyHash],
				},
			});

			const read = await widenedRead;
			expect(new Uint8Array(read!)).to.deep.equal(data);
			expect(new Uint8Array((await staleRead)!)).to.deep.equal(data);
			expect(requestsToProvider).to.equal(1);
		} finally {
			requesterRemoteBlocks.options.publish = originalRequesterPublish;
			providerRemoteBlocks.options.publish = originalProviderPublish;
		}
	});

	it("probes additional explicit providers when the first candidate does not answer", async () => {
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

		const requesterRemoteBlocks = (store(session, 1) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const originalPublish = requesterRemoteBlocks.options.publish;
		let forwardedToSource = 0;

		requesterRemoteBlocks.options.publish = (message: any, options: any) => {
			if (message instanceof BlockRequest) {
				const to = (options?.mode as any)?.to ?? [];
				const redundancy = Math.max(1, (options?.mode as any)?.redundancy ?? 1);
				const selected = to.slice(0, redundancy);
				return (async (): Promise<void> => {
					const sourceRemoteBlocks = (store(session, 0) as any)[
						"remoteBlocks"
					] as RemoteBlocks;
					await Promise.all(
						selected.map(async (target: string): Promise<void> => {
							if (target === store(session, 0).publicKeyHash) {
								forwardedToSource++;
								await sourceRemoteBlocks.onMessage(message, {
									from: store(session, 1).publicKeyHash,
								});
							}
						}),
					);
				})();
			}
			return originalPublish(message, options);
		};

		try {
			const read = await store(session, 1).get(cid, {
				remote: {
					timeout: 5_000,
					from: [
						store(session, 2).publicKeyHash,
						store(session, 0).publicKeyHash,
					],
				},
			});
			expect(new Uint8Array(read!)).to.deep.equal(data);
			expect(forwardedToSource).greaterThan(0);
		} finally {
			requesterRemoteBlocks.options.publish = originalPublish;
		}
	});

	it("only responds to peer that needs block", async () => {
		session = await TestSession.disconnected(3, {
			services: { blocks: (c) => new DirectBlock(c) },
		});

		await store(session, 0).start();
		await store(session, 1).start();
		await store(session, 2).start();

		await session.connect([[session.peers[0], session.peers[1]]]);
		await session.connect([[session.peers[1], session.peers[2]]]);

		await waitForNeighbour(store(session, 0), store(session, 1));
		await waitForNeighbour(store(session, 1), store(session, 2));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		let receivedblockInfo = false;
		const db1 = store(session, 1) as DirectBlock;
		const rmb1 = db1["remoteBlocks"] as RemoteBlocks;
		const onMessage1 = rmb1.onMessage.bind(rmb1);
		rmb1.onMessage = (data: any, context: any) => {
			if (data instanceof BlockResponse) {
				receivedblockInfo = true;
			}
			return onMessage1(data, context);
		};

		expect(
			new Uint8Array(
				(await store(session, 2).get(cid, {
					remote: { timeout: 5000, from: [store(session, 0).publicKeyHash] },
				}))!,
			),
		).to.deep.equal(data);
		await delay(3000);
		expect(receivedblockInfo).to.be.false;
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
			promises.push(
				store(session, 1).get(cid, {
					remote: { from: [store(session, 0).publicKeyHash] },
				}),
			);
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
			remote: { from: [store(session, 0).publicKeyHash] },
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
		const readDataPromise = store(session, 1).get(cid, {
			remote: { from: [store(session, 0).publicKeyHash] },
		});

		await session.connect(); // we connect after get request is sent
		await waitForNeighbour(store(session, 0), store(session, 1));

		expect(new Uint8Array((await readDataPromise)!)).to.deep.equal(data);
	});

	it("inherits response transport options from block request envelope", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const requesterRemoteBlocks = (store(session, 1) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const providerRemoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const requestPublish = sinon.spy(requesterRemoteBlocks.options, "publish");
		const responsePublish = sinon.spy(providerRemoteBlocks.options, "publish");

		expect(
			new Uint8Array(
				(await store(session, 1).get(cid, {
					remote: {
						timeout: 5_000,
						from: [store(session, 0).publicKeyHash],
						priority: CONVERGENCE_MESSAGE_PRIORITY,
					},
				}))!,
			),
		).to.deep.equal(data);

		const requestCall = requestPublish
			.getCalls()
			.find((call) => call.args[0] instanceof BlockRequest);
		expect(requestCall, "expected block request publish").to.exist;
		expect(requestCall!.args[1]?.priority).to.equal(
			CONVERGENCE_MESSAGE_PRIORITY,
		);
		expect(requestCall!.args[1]?.responsePriority).to.equal(
			CONVERGENCE_MESSAGE_PRIORITY,
		);
		expect(requestCall!.args[1]?.expiresAt).to.be.a("number");

		const responseCall = responsePublish
			.getCalls()
			.find((call) => call.args[0] instanceof BlockResponse);
		expect(responseCall, "expected block response publish").to.exist;
		expect(responseCall!.args[1]?.priority).to.equal(
			CONVERGENCE_MESSAGE_PRIORITY,
		);
		expect(responseCall!.args[1]?.expiresAt).to.equal(
			requestCall!.args[1]?.expiresAt,
		);
	});

	it("serves queued foreground block requests before background requests", async () => {
		session = await TestSession.disconnected(1, {
			services: {
				blocks: (components) =>
					new DirectBlock(components, { messageProcessingConcurrency: 1 }),
			},
		});
		await store(session, 0).start();

		const remoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const activeStarted = pDefer<void>();
		const releaseActive = pDefer<void>();
		const order: string[] = [];
		const handleFetchRequest = sinon
			.stub(remoteBlocks as any, "handleFetchRequest")
			.callsFake(async (...args: unknown[]) => {
				const request = args[0] as BlockRequest;
				order.push(request.cid);
				if (request.cid === "active") {
					activeStarted.resolve();
					await releaseActive.promise;
				}
			});
		const context = (requestPriority: number) => ({
			from: "requester",
			transport: {
				expiresAt: Date.now() + 5_000,
				requestPriority,
				responsePriority: requestPriority,
				remainingTime: () => 5_000,
				withResponseOptions: <T extends object>(options: T) => ({
					...options,
					priority: requestPriority,
					expiresAt: Date.now() + 5_000,
				}),
			},
		});

		await remoteBlocks.onMessage(
			new BlockRequest("active"),
			context(BACKGROUND_MESSAGE_PRIORITY),
		);
		await activeStarted.promise;
		await remoteBlocks.onMessage(
			new BlockRequest("background"),
			context(BACKGROUND_MESSAGE_PRIORITY),
		);
		await remoteBlocks.onMessage(
			new BlockRequest("foreground"),
			context(FOREGROUND_READ_MESSAGE_PRIORITY),
		);

		releaseActive.resolve();
		await Promise.all([
			(remoteBlocks as any)._backgroundLoadFetchQueue.onIdle(),
			(remoteBlocks as any)._loadFetchQueue.onIdle(),
		]);

		expect(order).to.deep.equal(["active", "foreground", "background"]);
		handleFetchRequest.restore();
	});

	it("reserves provider capacity for foreground block requests", async () => {
		session = await TestSession.disconnected(1, {
			services: {
				blocks: (components) =>
					new DirectBlock(components, { messageProcessingConcurrency: 2 }),
			},
		});
		await store(session, 0).start();

		const remoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const activeStarted = pDefer<void>();
		const foregroundStarted = pDefer<void>();
		const releaseBackground = pDefer<void>();
		const order: string[] = [];
		const handleFetchRequest = sinon
			.stub(remoteBlocks as any, "handleFetchRequest")
			.callsFake(async (...args: unknown[]) => {
				const request = args[0] as BlockRequest;
				order.push(request.cid);
				if (request.cid === "active") {
					activeStarted.resolve();
				}
				if (request.cid === "active" || request.cid === "background") {
					await releaseBackground.promise;
				}
				if (request.cid === "foreground") {
					foregroundStarted.resolve();
				}
			});
		const context = (requestPriority: number) => ({
			from: "requester",
			transport: {
				expiresAt: Date.now() + 5_000,
				requestPriority,
				responsePriority: requestPriority,
				remainingTime: () => 5_000,
				withResponseOptions: <T extends object>(options: T) => ({
					...options,
					priority: requestPriority,
					expiresAt: Date.now() + 5_000,
				}),
			},
		});

		try {
			await remoteBlocks.onMessage(
				new BlockRequest("active"),
				context(BACKGROUND_MESSAGE_PRIORITY),
			);
			await activeStarted.promise;
			await remoteBlocks.onMessage(
				new BlockRequest("background"),
				context(BACKGROUND_MESSAGE_PRIORITY),
			);
			await remoteBlocks.onMessage(
				new BlockRequest("foreground"),
				context(FOREGROUND_READ_MESSAGE_PRIORITY),
			);

			const startedBeforeBackgroundReleased = await Promise.race([
				foregroundStarted.promise.then(() => true),
				delay(100).then(() => false),
			]);
			expect(startedBeforeBackgroundReleased).to.equal(true);
			expect(order).to.deep.equal(["active", "foreground"]);
		} finally {
			releaseBackground.resolve();
			await Promise.all([
				(remoteBlocks as any)._backgroundLoadFetchQueue.onIdle(),
				(remoteBlocks as any)._loadFetchQueue.onIdle(),
			]);
			handleFetchRequest.restore();
		}

		expect(order).to.deep.equal(["active", "foreground", "background"]);
	});

	it("drains nested provider queues without leaking work across restart", async () => {
		session = await TestSession.disconnected(1, {
			services: {
				blocks: (components) =>
					new DirectBlock(components, { messageProcessingConcurrency: 1 }),
			},
		});
		await store(session, 0).start();

		const remoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const activeStarted = pDefer<void>();
		const releaseActive = pDefer<void>();
		const handled: string[] = [];
		const handleFetchRequest = sinon
			.stub(remoteBlocks as any, "handleFetchRequest")
			.callsFake(async (...args: unknown[]) => {
				const request = args[0] as BlockRequest;
				handled.push(request.cid);
				if (request.cid === "active") {
					activeStarted.resolve();
					await releaseActive.promise;
				}
			});
		const context = (requestPriority: number) => ({
			from: "requester",
			transport: {
				expiresAt: Date.now() + 5_000,
				requestPriority,
				responsePriority: requestPriority,
				remainingTime: () => 5_000,
				withResponseOptions: <T extends object>(options: T) => ({
					...options,
					priority: requestPriority,
					expiresAt: Date.now() + 5_000,
				}),
			},
		});

		let stopSettled = false;
		let stopping: Promise<void> | undefined;
		try {
			await remoteBlocks.onMessage(
				new BlockRequest("active"),
				context(BACKGROUND_MESSAGE_PRIORITY),
			);
			await activeStarted.promise;
			await remoteBlocks.onMessage(
				new BlockRequest("queued-background"),
				context(BACKGROUND_MESSAGE_PRIORITY),
			);
			await remoteBlocks.onMessage(
				new BlockRequest("queued-foreground"),
				context(FOREGROUND_READ_MESSAGE_PRIORITY),
			);

			stopping = remoteBlocks.stop().then(() => {
				stopSettled = true;
			});
			await delay(25);
			expect(stopSettled).to.equal(false);

			releaseActive.resolve();
			await stopping;
			expect(handled).to.deep.equal(["active"]);

			await remoteBlocks.start();
			await remoteBlocks.onMessage(
				new BlockRequest("after-restart"),
				context(FOREGROUND_READ_MESSAGE_PRIORITY),
			);
			await (remoteBlocks as any)._loadFetchQueue.onIdle();
			expect(handled).to.deep.equal(["active", "after-restart"]);
		} finally {
			releaseActive.resolve();
			await stopping;
			handleFetchRequest.restore();
		}
	});

	it("relay proxy honors request timeout when upstream response is slow", async () => {
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

		const providerRemoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const proxyRemoteBlocks = (store(session, 1) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const originalPublish = providerRemoteBlocks.options.publish.bind(
			providerRemoteBlocks.options,
		);
		const originalProxyPublish = proxyRemoteBlocks.options.publish.bind(
			proxyRemoteBlocks.options,
		);
		const proxiedRequestOptions: Array<{
			priority: number | undefined;
			responsePriority: number | undefined;
		}> = [];
		providerRemoteBlocks.options.publish = async (message, options) => {
			if (message instanceof BlockResponse) {
				await delay(1_500);
			}
			return originalPublish(message, options);
		};
		proxyRemoteBlocks.options.publish = async (message, options) => {
			if (message instanceof BlockRequest) {
				proxiedRequestOptions.push({
					priority: options.priority,
					responsePriority: options.responsePriority,
				});
			}
			return originalProxyPublish(message, options);
		};

		const readData = await store(session, 2).get(cid, {
			remote: {
				timeout: 5_000,
				from: [store(session, 1).publicKeyHash],
				priority: FOREGROUND_READ_MESSAGE_PRIORITY,
			},
		});
		expect(new Uint8Array(readData!)).to.deep.equal(data);
		expect(proxiedRequestOptions[0]).to.deep.equal({
			priority: FOREGROUND_READ_MESSAGE_PRIORITY,
			responsePriority: FOREGROUND_READ_MESSAGE_PRIORITY,
		});
	});

	it("cancels a relay proxy read when its provider lookup outlives stop", async () => {
		const proxyStarted = pDefer<AbortSignal | undefined>();
		const releaseDownstreamRead = pDefer<void>();
		session = await TestSession.disconnected(1, {
			services: {
				blocks: (components) =>
					new DirectBlock(components, {
						resolveProviders: async (_cid, options) => {
							const signal = options?.signal;
							proxyStarted.resolve(signal);
							if (!signal?.aborted) {
								await new Promise<void>((resolve) =>
									signal?.addEventListener("abort", () => resolve(), {
										once: true,
									}),
								);
							}
							// A resolver is allowed to finish normally despite cancellation. Returning
							// a candidate here exercises the downstream read's pre-aborted path.
							return ["upstream-provider"];
						},
					}),
			},
		});
		await store(session, 0).start();

		const remoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		let downstreamReadCalls = 0;
		remoteBlocks.options.publish = async (message) => {
			if (message instanceof BlockRequest) {
				downstreamReadCalls += 1;
				// Without the pre-aborted check, the proxy read reaches this point and
				// stop() remains queued behind this deliberately stalled request.
				await releaseDownstreamRead.promise;
			}
		};
		remoteBlocks.onMessage(
			new BlockRequest("zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr"),
			{ from: "requester" },
		);
		const proxySignal = await proxyStarted.promise;

		const stopPromises = [remoteBlocks.stop(), remoteBlocks.stop()];
		let deadline: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				Promise.all(stopPromises),
				new Promise<never>((_resolve, reject) => {
					deadline = setTimeout(
						() => reject(new Error("relay proxy stop did not settle promptly")),
						2_000,
					);
				}),
			]);
		} finally {
			if (deadline) clearTimeout(deadline);
			releaseDownstreamRead.resolve();
			await Promise.all(stopPromises);
		}

		expect(proxySignal?.aborted).to.equal(true);
		expect(downstreamReadCalls).to.equal(0);
		expect((remoteBlocks as any)._backgroundLoadFetchQueue.pending).to.equal(0);
		expect((remoteBlocks as any)._backgroundLoadFetchQueue.size).to.equal(0);
		expect((remoteBlocks as any)._loadFetchQueue.pending).to.equal(0);
		expect((remoteBlocks as any)._loadFetchQueue.size).to.equal(0);
		expect((remoteBlocks as any)._readFromPeersPromises.size).to.equal(0);
		expect((remoteBlocks as any)._resolvers.size).to.equal(0);
	});

	it("retries after a dropped block response", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);
		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const providerRemoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const originalPublish = providerRemoteBlocks.options.publish.bind(
			providerRemoteBlocks.options,
		);
		let droppedResponses = 0;
		providerRemoteBlocks.options.publish = async (message, options) => {
			if (message instanceof BlockResponse && droppedResponses === 0) {
				droppedResponses++;
				return;
			}
			return originalPublish(message, options);
		};

		const readData = await store(session, 1).get(cid, {
			remote: {
				timeout: 5_000,
				from: [store(session, 0).publicKeyHash],
				priority: CONVERGENCE_MESSAGE_PRIORITY,
			},
		});
		expect(droppedResponses).to.equal(1);
		expect(new Uint8Array(readData!)).to.deep.equal(data);
	});

	it("timeout", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const t1 = +new Date();
		const readData = await store(session, 0).get(
			"zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr",
			{ remote: { timeout: 3000, from: [store(session, 1).publicKeyHash] } },
		);
		const t2 = +new Date();
		expect(readData).equal(undefined);
		expect(t2 - t1 < 3100);
	});

	it("honors shorter caller timeout when reusing an in-flight get", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const missingCid = "zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr";
		const firstRead = store(session, 0)
			.get(missingCid, {
				remote: { timeout: 1000, from: [store(session, 1).publicKeyHash] },
			})
			.catch((): undefined => undefined);

		await delay(25);

		const t1 = +new Date();
		const secondRead = await store(session, 0).get(missingCid, {
			remote: { timeout: 100, from: [store(session, 1).publicKeyHash] },
		});
		const t2 = +new Date();

		expect(secondRead).equal(undefined);
		expect(t2 - t1).to.be.lessThan(500);
		await firstRead;
	});

	it("promotes a shared in-flight block request for a foreground reader", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c) },
		});
		await waitForNeighbour(store(session, 0), store(session, 1));

		const requesterRemoteBlocks = (store(session, 0) as any)[
			"remoteBlocks"
		] as RemoteBlocks;
		const originalPublish = requesterRemoteBlocks.options.publish.bind(
			requesterRemoteBlocks.options,
		);
		const firstRequestPublished = pDefer<void>();
		const requestTransportOptions: Array<{
			priority: number | undefined;
			responsePriority: number | undefined;
		}> = [];
		requesterRemoteBlocks.options.publish = async (message, options) => {
			if (message instanceof BlockRequest) {
				requestTransportOptions.push({
					priority: options.priority,
					responsePriority: options.responsePriority,
				});
				if (requestTransportOptions.length === 1) {
					firstRequestPublished.resolve();
				}
			}
			return originalPublish(message, options);
		};

		const missingCid = "zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr";
		const backgroundRead = store(session, 0).get(missingCid, {
			remote: {
				timeout: 1_000,
				from: [store(session, 1).publicKeyHash],
				priority: BACKGROUND_MESSAGE_PRIORITY,
			},
		});
		await firstRequestPublished.promise;

		const foregroundRead = store(session, 0).get(missingCid, {
			remote: {
				timeout: 100,
				from: [store(session, 1).publicKeyHash],
				priority: FOREGROUND_READ_MESSAGE_PRIORITY,
			},
		});

		expect(await foregroundRead).to.equal(undefined);
		expect(requestTransportOptions.slice(0, 2)).to.deep.equal([
			{
				priority: BACKGROUND_MESSAGE_PRIORITY,
				responsePriority: BACKGROUND_MESSAGE_PRIORITY,
			},
			{
				priority: FOREGROUND_READ_MESSAGE_PRIORITY,
				responsePriority: FOREGROUND_READ_MESSAGE_PRIORITY,
			},
		]);
		expect(
			await store(session, 0).get(missingCid, {
				remote: {
					timeout: 50,
					from: [store(session, 1).publicKeyHash],
					priority: CONVERGENCE_MESSAGE_PRIORITY,
				},
			}),
		).to.equal(undefined);
		expect(requestTransportOptions).to.have.length(2);
		await backgroundRead;
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

	it("waitForRequest option", async () => {
		session = await TestSession.connected(2, {
			services: { blocks: (c) => new DirectBlock(c, { eagerBlocks: true }) },
		});

		await waitForNeighbour(store(session, 0), store(session, 1));

		const db1 = store(session, 0) as DirectBlock;
		const db2 = store(session, 1) as DirectBlock;

		const data = new Uint8Array([5, 4, 3]);
		const cid = await store(session, 0).put(data);

		expect(cid).equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

		const db2ReceivedBlockResponse = pDefer();
		let timeout = setTimeout(() => {
			db2ReceivedBlockResponse.reject(new Error("Timed out"));
		}, 5000);
		db2ReceivedBlockResponse.promise.finally(() => {
			clearTimeout(timeout);
		});
		const rmb2 = db2["remoteBlocks"] as RemoteBlocks;
		const onMessage2 = rmb2.onMessage.bind(rmb2);
		rmb2.onMessage = (data, context) => {
			if (data instanceof BlockResponse) {
				db2ReceivedBlockResponse.resolve();
			}
			return onMessage2(data, context);
		};
		await db1.publish(
			serialize(
				new BlockResponse(
					"zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
					data,
				),
			),
			{ to: [session.peers[1].peerId] },
		);
		await db2ReceivedBlockResponse.promise;

		// now try to fetch the block and make sure no requests are sent
		let sent = false;
		rmb2.options.publish = async () => {
			sent = true;
		};

		const bytes = await db2.get(
			"zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
			{ remote: true },
		);
		expect(bytes).to.deep.equal(data);
		expect(sent).to.be.false;
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
