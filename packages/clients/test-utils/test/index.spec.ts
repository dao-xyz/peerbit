import { waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import type { Peerbit } from "peerbit";
import { TestSession } from "../src/session.js";

describe("session", () => {
	let session: TestSession;
	before(async () => {});

	after(async () => {
		await session.stop();
	});
	it("pubsub", async () => {
		session = await TestSession.connected(3);

		let result: any = undefined;

		await session.peers[0].services.pubsub.subscribe("x");
		await session.peers[1].services.pubsub.subscribe("x");
		await session.peers[2].services.pubsub.subscribe("x");

		await session.peers[0].services.pubsub.requestSubscribers("x");
		await session.peers[1].services.pubsub.requestSubscribers("x");
		await session.peers[2].services.pubsub.requestSubscribers("x");

		session.peers[2].services.pubsub.addEventListener("data", (evt) => {
			result = evt.detail;
		});
		await waitForResolved(async () =>
			expect(
				(await session.peers[0].services.pubsub.getSubscribers("x"))?.length,
			).equal(3),
		);
		await waitForResolved(async () =>
			expect(
				(await session.peers[1].services.pubsub.getSubscribers("x"))?.length,
			).equal(3),
		);
		session.peers[0].services.pubsub.publish(new Uint8Array([1, 2, 3]), {
			topics: ["x"],
		});
		await waitFor(() => !!result);
	});

	it("indexer", async () => {
		session = await TestSession.connected(2);
		expect(session.peers[0].indexer).to.exist;
		expect(session.peers[0].indexer !== session.peers[1].indexer).to.be.true;
	});

	it("can stop individually", async () => {
		session = await TestSession.connected(2);
		const peers: Peerbit[] = session.peers as Peerbit[];
		await peers[0].stop();
		expect(peers.map((x) => x.libp2p.status)).to.deep.eq([
			"stopped",
			"started",
		]);
	});

	it("can create with directory", async () => {
		let directory = `tmp/test-utils/tests/can-create-with-directory-${Math.random()}`;
		session = await TestSession.connected(1, {
			directory,
		});

		let client = session.peers[0] as Peerbit;
		expect(client.directory).to.equal(directory);

		// put block
		const cid = await client.services.blocks.put(new Uint8Array([1, 2, 3]));
		expect(cid).to.exist;

		await session.stop();
		session = await TestSession.connected(1, {
			directory,
		});

		client = session.peers[0] as Peerbit;

		expect(client.directory).to.equal(directory);
		expect(client.libp2p.status).to.equal("started");
		const bytes = await client.services.blocks.get(cid);
		expect(bytes).to.deep.equal(new Uint8Array([1, 2, 3]));
	});

	it("pubsub (in-memory transport)", async () => {
		const s = await TestSession.connectedInMemory(3, {
			degree: 2,
			seed: 1,
			mockCrypto: true,
		});

		try {
			let result: any = undefined;
			await s.peers[0].services.pubsub.subscribe("x");
			await s.peers[1].services.pubsub.subscribe("x");
			await s.peers[2].services.pubsub.subscribe("x");

			await s.peers[0].services.pubsub.requestSubscribers("x");
			await s.peers[1].services.pubsub.requestSubscribers("x");
			await s.peers[2].services.pubsub.requestSubscribers("x");

			s.peers[2].services.pubsub.addEventListener("data", (evt) => {
				result = (evt as any).detail;
			});

			await waitForResolved(async () =>
				expect((await s.peers[0].services.pubsub.getSubscribers("x"))?.length).equal(
					3,
				),
			);

			s.peers[0].services.pubsub.publish(new Uint8Array([1, 2, 3]), {
				topics: ["x"],
			});

			await waitFor(() => !!result);
		} finally {
			await s.stop();
		}
	});

	it("fanout (in-memory transport, bootstraps, sparse relays)", async () => {
		const nodes = 40;
		const rootIndex = 0;
		const seed = 1;

		const s = await TestSession.disconnectedInMemory(nodes, {
			seed,
			concurrency: 200,
			mockCrypto: true,
		});

		try {
			const [graph] = await s.connectRandomGraph({
				degree: 4,
				seed,
				concurrency: 200,
			});

			const topic = "concert";
			const rootPeer = s.peers[rootIndex] as any;
			const root = rootPeer.services.fanout as any;
			const rootId = root.publicKeyHash as string;

			const bootstrapIndices = [1, 2, 3].filter((i) => i < nodes && i !== rootIndex);
			const bootstrapAddrs = [
				...new Set(
					bootstrapIndices.flatMap((idx) =>
						(s.peers[idx] as any).libp2p.getMultiaddrs().map((a: any) => a.toString()),
					),
				),
			];
			root.setBootstraps(bootstrapAddrs);

			const rootNeighbors = new Set<number>((graph ?? [])[rootIndex] ?? []);
			const subscriberIndices: number[] = [];
			for (let i = 0; i < nodes; i++) {
				if (i === rootIndex) continue;
				subscriberIndices.push(i);
			}

			const relayCount = Math.max(
				1,
				Math.floor(0.25 * subscriberIndices.length),
			);
			const relaySet = new Set<number>();
			for (const idx of rootNeighbors) {
				if (idx === rootIndex) continue;
				relaySet.add(idx);
				if (relaySet.size >= relayCount) break;
			}
			for (const idx of subscriberIndices) {
				if (relaySet.size >= relayCount) break;
				relaySet.add(idx);
			}

			const rootMaxChildren = Math.max(2, Math.min(6, rootNeighbors.size || 2));
			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 30,
				msgSize: 64,
				uploadLimitBps: 50_000_000,
				maxChildren: rootMaxChildren,
				allowKick: true,
				repair: true,
				neighborRepair: false,
			});

			const relayJoinOpts = {
				bootstrap: bootstrapAddrs,
				bootstrapMaxPeers: 1,
				timeoutMs: 15_000,
				joinReqTimeoutMs: 500,
				announceIntervalMs: 100,
				announceTtlMs: 10_000,
				trackerQueryIntervalMs: 100,
				bootstrapEnsureIntervalMs: 100,
			};

			// Seed the tree by joining relays adjacent to root first.
			const bootstrapRelays = [...rootNeighbors].filter((i) => i !== rootIndex);
			await Promise.all(
				bootstrapRelays.map(async (idx) => {
					const fanout = (s.peers[idx] as any).services.fanout;
					await fanout.joinChannel(
						topic,
						rootId,
						{
							msgRate: 30,
							msgSize: 64,
							uploadLimitBps: 20_000_000,
							maxChildren: 32,
							allowKick: true,
							repair: true,
							neighborRepair: false,
						},
						relayJoinOpts,
					);
				}),
			);

			await Promise.all(
				subscriberIndices.map(async (idx) => {
					const fanout = (s.peers[idx] as any).services.fanout;
					const isRelay = relaySet.has(idx);
					await fanout.joinChannel(
						topic,
						rootId,
						{
							msgRate: 30,
							msgSize: 64,
							uploadLimitBps: isRelay ? 20_000_000 : 0,
							maxChildren: isRelay ? 32 : 0,
							allowKick: true,
							repair: true,
							neighborRepair: false,
						},
						isRelay
							? relayJoinOpts
							: { bootstrap: bootstrapAddrs, bootstrapMaxPeers: 1, timeoutMs: 15_000, joinReqTimeoutMs: 500 },
					);
				}),
			);

			// Verify all subscribers are attached (root itself has no parent).
			for (const idx of subscriberIndices) {
				const fanout = (s.peers[idx] as any).services.fanout;
				const st = fanout.getChannelStats(topic, rootId);
				expect(st, `missing channel stats for node ${idx}`).to.exist;
				expect(st!.parent, `node ${idx} missing parent`).to.be.a("string");
			}

			// Delivery check.
			const messages = 20;
			const received = subscriberIndices.map(() => new Uint8Array(messages));
			let delivered = 0;

			for (let i = 0; i < subscriberIndices.length; i++) {
				const idx = subscriberIndices[i]!;
				const fanout = (s.peers[idx] as any).services.fanout;
				fanout.addEventListener("fanout:data", (ev: any) => {
					const d = ev?.detail;
					if (!d) return;
					if (d.topic !== topic) return;
					if (d.root !== rootId) return;
					const seq = d.seq >>> 0;
					if (seq >= messages) return;
					if (received[i]![seq]) return;
					received[i]![seq] = 1;
					delivered += 1;
				});
			}

			const payload = new Uint8Array(64);
			for (let seq = 0; seq < messages; seq++) {
				await root.publishData(topic, rootId, payload);
			}
			await root.publishEnd(topic, rootId, messages);

			const expected = subscriberIndices.length * messages;
			await waitForResolved(
				() => {
					expect(delivered).to.equal(expected);
				},
				{ timeout: 10_000, delayInterval: 50 },
			);
		} finally {
			await s.stop();
		}
	});
});
