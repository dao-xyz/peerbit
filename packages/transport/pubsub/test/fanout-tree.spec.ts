import { TestSession } from "@peerbit/libp2p-test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutChannel, FanoutTree } from "../src/index.js";

describe("fanout-tree", () => {
	it("forms a small tree and delivers data", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(
			3,
			{
			services: {
				fanout: (c) => new FanoutTree(c, { connectionManager: false }),
			},
			},
		);

		try {
			// Connect 0<->1<->2 (line) so 2 can join via 1 if root is full.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "concert";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			// Relay can accept one child.
			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			// Leaf should end up attaching to relay (root is full).
			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let received: Uint8Array | undefined;
			leaf.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([1, 2, 3, 4]);
			await root.publishData(topic, rootId, payload);

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("exposes channel peers for fanout membership-aware consumers", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(
			3,
			{
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const leafA = session.peers[1].services.fanout;
			const leafB = session.peers[2].services.fanout;

			const topic = "peer-list-demo";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const leafAChannel = new FanoutChannel(leafA, { topic, root: rootId });
			await leafAChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const leafBChannel = new FanoutChannel(leafB, { topic, root: rootId });
			await leafBChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			await waitForResolved(() => {
				const peers = new Set(rootChannel.getPeerHashes());
				expect(peers.has(leafA.publicKeyHash)).to.equal(true);
				expect(peers.has(leafB.publicKeyHash)).to.equal(true);
			});

			await waitForResolved(() => {
				const peers = new Set(leafAChannel.getPeerHashes());
				expect(peers.has(root.publicKeyHash)).to.equal(true);
			});
		} finally {
			await session.stop();
		}
	});

	it("supports economical unicast via route tokens through the root", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(
			3,
			{
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const sender = session.peers[1].services.fanout;
			const target = session.peers[2].services.fanout;

			const topic = "unicast-demo";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });
			await senderChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const targetChannel = new FanoutChannel(target, { topic, root: rootId });
			await targetChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let targetRoute: string[] | undefined;
			await waitForResolved(() => {
				targetRoute = targetChannel.getRouteToken();
				expect(targetRoute).to.exist;
			});
			expect(targetRoute![0]).to.equal(rootId);
			expect(targetRoute![targetRoute!.length - 1]).to.equal(target.publicKeyHash);

			let received: Uint8Array | undefined;
			let origin: string | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
				origin = ev.detail.origin;
			});

			const payload = new Uint8Array([4, 3, 2, 1]);
			await senderChannel.unicast(targetRoute!, payload);

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
			expect(origin).to.equal(sender.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("resolves route tokens through control-plane proxy and unicasts across branches", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(5, {
			services: {
				fanout: (c) => new FanoutTree(c, { connectionManager: false }),
			},
		});

		try {
			// Root <-> relayA and root <-> relayB. sender is only connected to relayA,
			// target is only connected to relayB.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
				[session.peers[1], session.peers[3]],
				[session.peers[2], session.peers[4]],
			]);

			const root = session.peers[0].services.fanout;
			const relayA = session.peers[1].services.fanout;
			const relayB = session.peers[2].services.fanout;
			const sender = session.peers[3].services.fanout;
			const target = session.peers[4].services.fanout;

			const topic = "unicast-proxy-demo";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const relayAChannel = new FanoutChannel(relayA, { topic, root: rootId });
			await relayAChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 2,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const relayBChannel = new FanoutChannel(relayB, { topic, root: rootId });
			await relayBChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 2,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });
			await senderChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const targetChannel = new FanoutChannel(target, { topic, root: rootId });
			await targetChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let resolvedRoute: string[] | undefined;
			await waitForResolved(async () => {
				resolvedRoute = await senderChannel.resolveRouteToken(target.publicKeyHash, {
					timeoutMs: 2_000,
				});
				expect(resolvedRoute).to.exist;
			});
			expect(resolvedRoute![0]).to.equal(rootId);
			expect(resolvedRoute![resolvedRoute!.length - 1]).to.equal(target.publicKeyHash);

			let received: Uint8Array | undefined;
			let origin: string | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
				origin = ev.detail.origin;
			});

			const payload = new Uint8Array([5, 6, 7, 8]);
			await senderChannel.unicastTo(target.publicKeyHash, payload, { timeoutMs: 2_000 });

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
			expect(origin).to.equal(sender.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("bounds route cache size and evicts old entries", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(5, {
			services: {
				fanout: (c) => new FanoutTree(c, { connectionManager: false }),
			},
		});

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[1], session.peers[3]],
				[session.peers[1], session.peers[4]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leafA = session.peers[2].services.fanout;
			const leafB = session.peers[3].services.fanout;
			const leafC = session.peers[4].services.fanout;

			const topic = "route-cache-bound";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
				routeCacheMaxEntries: 2,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 3,
					repair: true,
					routeCacheMaxEntries: 2,
				},
				{ timeoutMs: 10_000 },
			);

			for (const leaf of [leafA, leafB, leafC]) {
				await leaf.joinChannel(
					topic,
					rootId,
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: true,
						routeCacheMaxEntries: 2,
					},
					{ timeoutMs: 10_000 },
				);
			}

			await waitForResolved(() =>
				expect(root.getChannelStats(topic, rootId)?.routeCacheEntries).to.be.at.most(2),
			);
			await waitForResolved(() =>
				expect(root.getChannelMetrics(topic, rootId).routeCacheEvictions).to.be.greaterThan(0),
			);
		} finally {
			await session.stop();
		}
	});

	it("keeps routes resolvable with ttl + periodic re-announces", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(4, {
			services: {
				fanout: (c) => new FanoutTree(c, { connectionManager: false }),
			},
		});

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[1], session.peers[3]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const sender = session.peers[2].services.fanout;
			const target = session.peers[3].services.fanout;

			const topic = "route-cache-refresh";
			const rootId = root.publicKeyHash;

			const routeCacheTtlMs = 40;
			const routeAnnounceIntervalMs = 20;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
				routeCacheMaxEntries: 16,
				routeCacheTtlMs,
				routeAnnounceIntervalMs,
			});

			const relayChannel = new FanoutChannel(relay, { topic, root: rootId });
			await relayChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 2,
					repair: true,
					routeCacheMaxEntries: 16,
					routeCacheTtlMs,
					routeAnnounceIntervalMs,
				},
				{ timeoutMs: 10_000, routeAnnounceIntervalMs },
			);

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });
			await senderChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
					routeCacheMaxEntries: 16,
					routeCacheTtlMs,
					routeAnnounceIntervalMs,
				},
				{ timeoutMs: 10_000, routeAnnounceIntervalMs },
			);

			const targetChannel = new FanoutChannel(target, { topic, root: rootId });
			await targetChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
					routeCacheMaxEntries: 16,
					routeCacheTtlMs,
					routeAnnounceIntervalMs,
				},
				{ timeoutMs: 10_000, routeAnnounceIntervalMs },
			);

			await waitForResolved(async () => {
				const route = await senderChannel.resolveRouteToken(target.publicKeyHash, {
					timeoutMs: 2_000,
				});
				expect(route).to.exist;
			});

			await delay(150);

			await waitForResolved(async () => {
				const route = await senderChannel.resolveRouteToken(target.publicKeyHash, {
					timeoutMs: 2_000,
				});
				expect(route).to.exist;
				expect(route?.[route.length - 1]).to.equal(target.publicKeyHash);
			});
		} finally {
			await session.stop();
		}
	});

	it("resolves route tokens after cache expiry via subtree fallback search", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(6, {
			services: {
				fanout: (c) => new FanoutTree(c, { connectionManager: false }),
			},
		});

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
				[session.peers[1], session.peers[3]],
				[session.peers[2], session.peers[4]],
				[session.peers[4], session.peers[5]],
			]);

			const root = session.peers[0].services.fanout;
			const relayA = session.peers[1].services.fanout;
			const relayB = session.peers[2].services.fanout;
			const sender = session.peers[3].services.fanout;
			const relayB2 = session.peers[4].services.fanout;
			const target = session.peers[5].services.fanout;

			const topic = "route-cache-subtree-fallback";
			const rootId = root.publicKeyHash;
			const routeCacheTtlMs = 40;
			const routeAnnounceIntervalMs = 60_000;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
				routeCacheMaxEntries: 64,
				routeCacheTtlMs,
				routeAnnounceIntervalMs,
			});

			for (const [node, maxChildren] of [
				[relayA, 1],
				[relayB, 2],
				[sender, 0],
				[relayB2, 1],
				[target, 0],
			] as const) {
				const ch = new FanoutChannel(node, { topic, root: rootId });
				await ch.join(
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 1_000_000,
						maxChildren,
						repair: true,
						routeCacheMaxEntries: 64,
						routeCacheTtlMs,
						routeAnnounceIntervalMs,
					},
					{ timeoutMs: 10_000, routeAnnounceIntervalMs },
				);
			}

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });

			// Let initial route announcements age out from caches before resolving.
			await delay(160);
			const missesBefore = root.getChannelMetrics(topic, rootId).routeCacheMisses;

			let resolvedRoute: string[] | undefined;
			await waitForResolved(async () => {
				resolvedRoute = await senderChannel.resolveRouteToken(target.publicKeyHash, {
					timeoutMs: 4_000,
				});
				expect(resolvedRoute).to.exist;
			});
			expect(resolvedRoute![0]).to.equal(rootId);
			expect(resolvedRoute![resolvedRoute!.length - 1]).to.equal(target.publicKeyHash);

			const missesAfter = root.getChannelMetrics(topic, rootId).routeCacheMisses;
			expect(missesAfter).to.be.greaterThan(missesBefore);
		} finally {
			await session.stop();
		}
	});

	it("uses JOIN_REJECT redirects to attach via relay without trackers", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(
			3,
			{
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			// 0 connected to both 1 and 2. Leaf (2) should be able to re-attach to relay (1)
			// when root (0) is full, using JOIN_REJECT redirects (no bootstrap tracker).
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "concert";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			// Relay consumes root's only slot.
			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			// Leaf joins via root first (connected peer), gets rejected, then follows redirects to relay.
			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const stats = leaf.getChannelStats(topic, rootId);
			expect(stats?.parent).to.equal(relay.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("joins via bootstrap tracker (dial + capacity announcements)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(
			4,
			{
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			// Star topology via a bootstrap node so join must happen via dial + tracker redirect.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[2], session.peers[1]],
				[session.peers[3], session.peers[1]],
			]);

			const root = session.peers[0].services.fanout;
			const bootstrap = session.peers[1];
			const relay = session.peers[2].services.fanout;
			const leaf = session.peers[3].services.fanout;

			const bootstrapAddrs = bootstrap.getMultiaddrs();
			root.setBootstraps(bootstrapAddrs);

			const topic = "concert";
			const rootId = root.publicKeyHash;

			// Root can only accept one child (the relay).
			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{
					timeoutMs: 10_000,
					bootstrap: bootstrapAddrs,
					announceIntervalMs: 200,
					announceTtlMs: 5_000,
				},
			);

			// Leaf should end up attaching to relay (root is full).
			let parent: string | undefined;
			leaf.addEventListener("fanout:joined", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				parent = ev.detail.parent;
			});

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000, bootstrap: bootstrapAddrs },
			);

			await waitForResolved(() => expect(parent).to.exist);
			expect(parent).to.equal(relay.publicKeyHash);

			let received: Uint8Array | undefined;
			leaf.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([9, 9, 9]);
			await root.publishData(topic, rootId, payload);

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("re-parents when its parent disconnects", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> = await TestSession.disconnected(
			3,
			{
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			// Root connected to both relay and leaf. Leaf initially joins via relay (root full),
			// then relay disappears and leaf should attach directly to root.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relayNode = session.peers[1];
			const relay = relayNode.services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "concert";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			// Relay consumes root's only slot.
			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			// Leaf attaches via relay using JOIN_REJECT redirects.
			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(relay.publicKeyHash);

			// Kill relay.
			await relayNode.stop();

			// Leaf should eventually attach directly to root.
			await waitForResolved(() =>
				expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
			);

			let received: Uint8Array | undefined;
			leaf.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([7, 7, 7]);
			await root.publishData(topic, rootId, payload);

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});
});
