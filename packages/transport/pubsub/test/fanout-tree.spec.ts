import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree } from "../src/index.js";

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
