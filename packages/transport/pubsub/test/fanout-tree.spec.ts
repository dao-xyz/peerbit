import { TestSession } from "@peerbit/libp2p-test-utils";
import { TimeoutError, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	FanoutChannel,
	FanoutTree,
	type FanoutTreeChannelMetrics,
} from "../src/index.js";

type FanoutServices = { fanout: FanoutTree };

const createFanoutService = (components: any) =>
	new FanoutTree(components, { connectionManager: false });

const createFanoutTestSession = (n: number) =>
	TestSession.disconnected<FanoutServices>(n, {
		services: {
			fanout: createFanoutService,
		},
	});

describe("fanout-tree", () => {
	it("keeps cold-join counters additive at the exported metrics boundary", async () => {
		const session = await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const metrics = fanout.getChannelMetrics(
				"metrics-source-compatibility",
				fanout.publicKeyHash,
			);
			type NewColdJoinMetric =
				| "joinBootstrapDialAttempts"
				| "joinBootstrapDialFailures"
				| "joinCandidateDialAttempts"
				| "joinCandidateDialFailures"
				| "joinConnectedCandidateAttempts"
				| "joinUnconnectedCandidateAttempts"
				| "joinReqTimeouts"
				| "joinDeadlineExpirations";
			const legacyMetrics: Omit<
				FanoutTreeChannelMetrics,
				NewColdJoinMetric
			> = metrics;
			const acceptsPublicMetrics = (value: FanoutTreeChannelMetrics) => value;

			expect(acceptsPublicMetrics(legacyMetrics)).to.equal(legacyMetrics);
		} finally {
			await session.stop();
		}
	});

	it("bounds per-channel route token cache (LRU + TTL)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const topic = "route-cache";
			const root = fanout.publicKeyHash;

			const id = fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 128,
				repair: false,
				routeCacheMaxEntries: 3,
				routeCacheTtlMs: 0,
			});

			const ch = (fanout as any).channelsBySuffixKey.get(id.suffixKey);
			expect(ch).to.exist;

			const cacheRoute = (fanout as any).cacheRoute.bind(fanout) as (
				ch: any,
				route: string[],
			) => void;
			const getCachedRoute = (fanout as any).getCachedRoute.bind(fanout) as (
				ch: any,
				target: string,
			) => string[] | undefined;

			// Root route-cache entries must start with a valid child hop.
			ch.children.set("child1", { bidPerByte: 0 });

			cacheRoute(ch, [root, "child1", "p1"]);
			cacheRoute(ch, [root, "child1", "p2"]);
			cacheRoute(ch, [root, "child1", "p3"]);
			expect(ch.routeByPeer.size).to.equal(3);

			// LRU touch p1, then insert p4: p2 should be evicted.
			expect(getCachedRoute(ch, "p1")).to.deep.equal([root, "child1", "p1"]);
			cacheRoute(ch, [root, "child1", "p4"]);
			expect(ch.routeByPeer.size).to.equal(3);
			expect(ch.routeByPeer.has("p2")).to.equal(false);
			expect(ch.routeByPeer.has("p1")).to.equal(true);
			expect(ch.routeByPeer.has("p3")).to.equal(true);
			expect(ch.routeByPeer.has("p4")).to.equal(true);

			// TTL expiry prunes oldest entries.
			// Mutate timestamps to avoid relying on wall-clock timing (keeps this test deterministic).
			ch.routeCacheTtlMs = 25;
			const expiredAt = Date.now() - 1_000;
			for (const entry of ch.routeByPeer.values()) entry.updatedAt = expiredAt;
			expect(getCachedRoute(ch, "p1")).to.equal(undefined);
			expect(getCachedRoute(ch, "p3")).to.equal(undefined);
			expect(getCachedRoute(ch, "p4")).to.equal(undefined);
			expect(ch.routeByPeer.size).to.equal(0);
		} finally {
			await session.stop();
		}
	});

	it("invalidates cached routes when root child set changes", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const topic = "route-cache-validity";
			const root = fanout.publicKeyHash;

			const id = fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 128,
				repair: false,
				routeCacheMaxEntries: 16,
				routeCacheTtlMs: 0,
			});

			const ch = (fanout as any).channelsBySuffixKey.get(id.suffixKey);
			expect(ch).to.exist;

			const cacheRoute = (fanout as any).cacheRoute.bind(fanout) as (
				ch: any,
				route: string[],
			) => void;
			const getCachedRoute = (fanout as any).getCachedRoute.bind(fanout) as (
				ch: any,
				target: string,
			) => string[] | undefined;

			// Root requires the first hop after root to be a current child.
			ch.children.set("child1", { bidPerByte: 0 });
			cacheRoute(ch, [root, "child1", "target"]);
			expect(getCachedRoute(ch, "target")).to.deep.equal([
				root,
				"child1",
				"target",
			]);

			// Drop child1, cached route must be treated as invalid and removed.
			ch.children.delete("child1");
			expect(getCachedRoute(ch, "target")).to.equal(undefined);
			expect(ch.routeByPeer.has("target")).to.equal(false);
		} finally {
			await session.stop();
		}
	});

	it("does not miss channel attachment when parent appears during join-listener setup", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout as any;
			const waitForChannelAttachment = fanout.waitForChannelAttachment.bind(
				fanout,
			) as (ch: any, timeoutMs: number) => Promise<void>;
			const ch = {
				isRoot: false,
				parent: undefined as string | undefined,
				id: { topic: "attachment-race", root: "root" },
			};

			const originalAddEventListener = fanout.addEventListener.bind(fanout);
			fanout.addEventListener = ((
				type: string,
				listener: EventListenerOrEventListenerObject,
				options?: AddEventListenerOptions | boolean,
			) => {
				const result = originalAddEventListener(type, listener, options);
				if (type === "fanout:joined") {
					ch.parent = "parent";
				}
				return result;
			}) as typeof fanout.addEventListener;

			try {
				await waitForChannelAttachment(ch, 25);
			} finally {
				fanout.addEventListener = originalAddEventListener;
			}

			expect(ch.parent).to.equal("parent");
		} finally {
			await session.stop();
		}
	});

	it("accepts parent attachment that becomes visible before timeout even without a join event", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout as any;
			const waitForChannelAttachment = fanout.waitForChannelAttachment.bind(
				fanout,
			) as (ch: any, timeoutMs: number) => Promise<void>;
			const ch = {
				isRoot: false,
				parent: undefined as string | undefined,
				id: { topic: "attachment-timeout-fallback", root: "root" },
			};

			setTimeout(() => {
				ch.parent = "parent";
			}, 5);

			await waitForChannelAttachment(ch, 25);
			expect(ch.parent).to.equal("parent");
		} finally {
			await session.stop();
		}
	});

	it("reports attachment waits as delivery timeouts", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout as any;
			const waitForChannelAttachment = fanout.waitForChannelAttachment.bind(
				fanout,
			) as (ch: any, timeoutMs: number) => Promise<void>;
			const ch = {
				isRoot: false,
				parent: undefined as string | undefined,
				id: { topic: "attachment-timeout", root: "root" },
			};

			await expect(waitForChannelAttachment(ch, 5)).to.be.rejectedWith(
				TimeoutError,
				"fanout proxy publish timed out waiting for attachment",
			);
		} finally {
			await session.stop();
		}
	});

	it("returns false when maybe-publishing to a channel that is not open", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const ok = await fanout.publishToChannelMaybe(
				"missing-channel",
				fanout.publicKeyHash,
				new Uint8Array([1]),
			);
			expect(ok).to.equal(false);
		} finally {
			await session.stop();
		}
	});

	it("returns false for late channel-close races on maybe-publish but still rethrows unexpected errors", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const topic = "maybe-publish-close-race";
			const root = fanout.publicKeyHash;

			fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			const originalPublishToChannel = fanout.publishToChannel.bind(fanout);

			try {
				fanout.publishToChannel = (async () => {
					throw new Error(`Channel not open: ${topic} (${root})`);
				}) as typeof fanout.publishToChannel;

				const ok = await fanout.publishToChannelMaybe(
					topic,
					root,
					new Uint8Array([1]),
				);
				expect(ok).to.equal(false);

				fanout.publishToChannel = (async () => {
					throw new Error("unexpected publish failure");
				}) as typeof fanout.publishToChannel;

				await expect(
					fanout.publishToChannelMaybe(topic, root, new Uint8Array([1])),
				).to.be.rejectedWith("unexpected publish failure");
			} finally {
				fanout.publishToChannel =
					originalPublishToChannel as typeof fanout.publishToChannel;
			}
		} finally {
			await session.stop();
		}
	});

	it("resets a kicked child connection when control delivery returns false", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const root = session.peers[0].services.fanout;
			const child = session.peers[1].services.fanout;
			const rootInternals = root as any;
			const childHash = child.publicKeyHash;
			await waitForResolved(
				() => expect(rootInternals.peers.get(childHash)).to.exist,
			);

			const id = root.openChannel("kick-delivery-failure", root.publicKeyHash, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});
			const ch = rootInternals.channelsBySuffixKey.get(id.suffixKey);
			expect(ch).to.exist;
			ch.children.set(childHash, { bidPerByte: 0 });

			const connectionManager = rootInternals.components
				.connectionManager as any;
			const originalPublishMessageMaybe = root.publishMessageMaybe;
			const originalCloseConnections = connectionManager.closeConnections;
			const closedPeerIds: string[] = [];

			root.publishMessageMaybe = (async () =>
				false) as typeof root.publishMessageMaybe;
			connectionManager.closeConnections = async (peerId: any) => {
				closedPeerIds.push(peerId.toString());
			};

			try {
				await rootInternals.kickChildHashes(ch, [childHash], {
					resetPeerConnections: true,
				});
				expect(closedPeerIds).to.deep.equal([
					rootInternals.peers.get(childHash).peerId.toString(),
				]);
				expect(ch.children.has(childHash)).to.equal(false);

				ch.children.set(childHash, { bidPerByte: 0 });
				root.publishMessageMaybe = (async () => {
					throw new Error("unexpected control publish failure");
				}) as typeof root.publishMessageMaybe;

				await expect(
					rootInternals.kickChildHashes(ch, [childHash], {
						resetPeerConnections: true,
					}),
				).to.be.rejectedWith("unexpected control publish failure");
				expect(closedPeerIds).to.have.length(2);
			} finally {
				root.publishMessageMaybe = originalPublishMessageMaybe;
				connectionManager.closeConnections = originalCloseConnections;
			}
		} finally {
			await session.stop();
		}
	});

	it("forms a small tree and delivers data", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

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

	it("prioritizes direct root candidate during join when root is outside ranked top-K", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(10);

		try {
			const fanouts = session.peers.map((p) => p.services.fanout);
			const byHash = fanouts
				.map((f) => ({ hash: f.publicKeyHash, fanout: f }))
				.sort((a, b) => a.hash.localeCompare(b.hash));

			// Pick a root that is guaranteed to be outside the first ranked window.
			const rootEntry = byHash[byHash.length - 1]!;
			const root = rootEntry.fanout;
			const rootId = rootEntry.hash;

			const joinerEntry = byHash.find((x) => x.hash !== rootId)!;
			const joiner = joinerEntry.fanout;
			const joinerPeer = session.peers.find(
				(p) => p.services.fanout.publicKeyHash === joinerEntry.hash,
			)!;
			const starGroups = session.peers
				.filter((p) => p !== joinerPeer)
				.map((p) => [joinerPeer, p]);
			await session.connect(starGroups);

			const topic = "join-root-priority";
			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 32,
				repair: true,
			});

			await joiner.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{
					timeoutMs: 10_000,
					joinReqTimeoutMs: 500,
					retryMs: 100,
				},
			);

			await waitForResolved(() =>
				expect(joiner.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
			);
		} finally {
			await session.stop();
		}
	});

	it("allows a child to leave and immediately frees parent capacity", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "leave-demo";
			const rootId = root.publicKeyHash;

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
				{ timeoutMs: 10_000 },
			);

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

			await waitForResolved(() =>
				expect(relay.getChannelStats(topic, rootId)?.children).to.equal(1),
			);

			await leaf.closeChannel(topic, rootId);

			await waitForResolved(() =>
				expect(relay.getChannelStats(topic, rootId)?.children).to.equal(0),
			);
		} finally {
			await session.stop();
		}
	});

	it("proxies publish from non-root via the root", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const publisher = session.peers[1].services.fanout;
			const subscriber = session.peers[2].services.fanout;

			const topic = "proxy-publish";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
			});

			const publisherChannel = new FanoutChannel(publisher, {
				topic,
				root: rootId,
			});
			await publisherChannel.join(
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			const subscriberChannel = new FanoutChannel(subscriber, {
				topic,
				root: rootId,
			});
			await subscriberChannel.join(
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let receivedBySubscriber: Uint8Array | undefined;
			subscriber.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				receivedBySubscriber = ev.detail.payload;
			});

			let receivedByPublisher: Uint8Array | undefined;
			publisher.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if (ev.detail.seq !== 0) return;
				receivedByPublisher = ev.detail.payload;
			});

			const payload = new Uint8Array([9, 8, 7, 6]);
			await publisherChannel.publish(payload);

			await waitForResolved(() => expect(receivedBySubscriber).to.exist);
			expect([...receivedBySubscriber!]).to.deep.equal([...payload]);

			await waitForResolved(() => expect(receivedByPublisher).to.exist);
			expect([...receivedByPublisher!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("exposes channel peers for fanout membership-aware consumers", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

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
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

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
			expect(targetRoute![targetRoute!.length - 1]).to.equal(
				target.publicKeyHash,
			);

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
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(5);

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
				resolvedRoute = await senderChannel.resolveRouteToken(
					target.publicKeyHash,
					{
						timeoutMs: 2_000,
					},
				);
				expect(resolvedRoute).to.exist;
			});
			expect(resolvedRoute![0]).to.equal(rootId);
			expect(resolvedRoute![resolvedRoute!.length - 1]).to.equal(
				target.publicKeyHash,
			);

			let received: Uint8Array | undefined;
			let origin: string | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
				origin = ev.detail.origin;
			});

			const payload = new Uint8Array([5, 6, 7, 8]);
			await senderChannel.unicastTo(target.publicKeyHash, payload, {
				timeoutMs: 2_000,
			});

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
			expect(origin).to.equal(sender.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("supports economical unicast with ACKs (shared intermediate hop)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(4);

		try {
			// Root <-> relay. Two leaves only connect to relay. This creates a shared intermediate
			// hop so the unicast goes: sender -> relay -> root -> relay -> target.
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[1], session.peers[3]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const sender = session.peers[2].services.fanout;
			const target = session.peers[3].services.fanout;

			const topic = "unicast-ack-shared-hop";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
			});

			const relayChannel = new FanoutChannel(relay, { topic, root: rootId });
			await relayChannel.join(
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

			let targetRoute: string[] | undefined;
			await waitForResolved(() => {
				targetRoute = targetChannel.getRouteToken();
				expect(targetRoute).to.exist;
			});

			let received: Uint8Array | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([9, 8, 7, 6]);
			received = undefined;
			await senderChannel.unicast(targetRoute!, payload);
			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);

			received = undefined;
			await senderChannel.unicastAck(targetRoute!, payload, {
				timeoutMs: 2_000,
			});
			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("supports economical unicast with ACKs across branches", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(5);

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

			const topic = "unicast-ack-branches";
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

			let received: Uint8Array | undefined;
			targetChannel.addEventListener("unicast", (ev: any) => {
				received = ev.detail.payload;
			});

			const payload = new Uint8Array([1, 3, 3, 7]);
			await senderChannel.unicastToAck(target.publicKeyHash, payload, {
				timeoutMs: 10_000,
			});

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("bounds route cache size and evicts old entries", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(5);

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

			// Drive route discovery on-demand so the root cache actually fills and evicts.
			for (const leaf of [leafA, leafB, leafC]) {
				await waitForResolved(async () => {
					const route = await relay.resolveRouteToken(
						topic,
						rootId,
						leaf.publicKeyHash,
						{
							timeoutMs: 4_000,
						},
					);
					expect(route).to.exist;
				});
			}

			await waitForResolved(() =>
				expect(
					root.getChannelStats(topic, rootId)?.routeCacheEntries,
				).to.be.at.most(2),
			);
			await waitForResolved(() =>
				expect(
					root.getChannelMetrics(topic, rootId).routeCacheEvictions,
				).to.be.greaterThan(0),
			);
		} finally {
			await session.stop();
		}
	});

	it("clamps requested route cache size to a hard safety cap", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const root = session.peers[0].services.fanout;
			const topic = "route-cache-hard-cap";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
				routeCacheMaxEntries: 2_000_000_000,
			});

			const stats = root.getChannelStats(topic, rootId);
			expect(stats).to.exist;
			expect(stats?.routeCacheMaxEntries).to.equal(100_000);
		} finally {
			await session.stop();
		}
	});

	it("bounds peer hint cache size and prunes old entries", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(8);

		try {
			// Star topology: all peers connect to the root so we can drive many JOIN_REQs.
			await session.connect(
				session.peers.slice(1).map((peer) => [session.peers[0], peer] as const),
			);

			const root = session.peers[0].services.fanout;
			const leaves = session.peers.slice(1).map((p) => p.services.fanout);

			const topic = "peer-hints-bound";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 32,
				repair: true,
				peerHintMaxEntries: 2,
			});

			for (const leaf of leaves) {
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
			}

			const stats = root.getChannelStats(topic, rootId);
			expect(stats?.peerHintMaxEntries).to.equal(2);
			expect(stats?.peerHintEntries).to.equal(2);
		} finally {
			await session.stop();
		}
	});

	it("clamps requested peer hint size to a hard safety cap", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const root = session.peers[0].services.fanout;
			const topic = "peer-hints-hard-cap";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
				peerHintMaxEntries: 2_000_000_000,
			});

			const stats = root.getChannelStats(topic, rootId);
			expect(stats).to.exist;
			expect(stats?.peerHintMaxEntries).to.equal(100_000);
		} finally {
			await session.stop();
		}
	});

	it("root resolves deep route tokens on-demand without route announcements", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "root-route-resolve";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: true,
				routeCacheMaxEntries: 16,
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
					routeCacheMaxEntries: 16,
				},
				{ timeoutMs: 10_000 },
			);

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
					routeCacheMaxEntries: 16,
				},
				{ timeoutMs: 10_000 },
			);

			const route = await root.resolveRouteToken(
				topic,
				rootId,
				leaf.publicKeyHash,
				{
					timeoutMs: 4_000,
				},
			);
			expect(route).to.exist;
			expect(route?.[0]).to.equal(rootId);
			expect(route?.[1]).to.equal(relay.publicKeyHash);
			expect(route?.[route.length - 1]).to.equal(leaf.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("resolves route tokens after cache expiry via subtree fallback search", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(6);

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

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: true,
				routeCacheMaxEntries: 64,
				routeCacheTtlMs,
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
					},
					{ timeoutMs: 10_000 },
				);
			}

			const senderChannel = new FanoutChannel(sender, { topic, root: rootId });

			// Warm caches once, then let route tokens expire before resolving again.
			await waitForResolved(async () => {
				const route = await senderChannel.resolveRouteToken(
					target.publicKeyHash,
					{
						timeoutMs: 4_000,
					},
				);
				expect(route).to.exist;
			});

			await delay(160);
			const missesBefore = root.getChannelMetrics(
				topic,
				rootId,
			).routeCacheMisses;

			let resolvedRoute: string[] | undefined;
			await waitForResolved(async () => {
				resolvedRoute = await senderChannel.resolveRouteToken(
					target.publicKeyHash,
					{
						timeoutMs: 4_000,
					},
				);
				expect(resolvedRoute).to.exist;
			});
			expect(resolvedRoute![0]).to.equal(rootId);
			expect(resolvedRoute![resolvedRoute!.length - 1]).to.equal(
				target.publicKeyHash,
			);

			const missesAfter = root.getChannelMetrics(
				topic,
				rootId,
			).routeCacheMisses;
			expect(missesAfter).to.be.greaterThan(missesBefore);
		} finally {
			await session.stop();
		}
	});

	it("uses JOIN_REJECT redirects to attach via relay without trackers", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

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
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(4);

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

	it("re-parents when no data arrives within staleAfterMs", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const root = session.peers[0].services.fanout;
			const leaf = session.peers[1].services.fanout;

			const topic = "stale";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				maxDataAgeMs: 10_000,
				repair: false,
			});

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					maxDataAgeMs: 10_000,
					repair: false,
				},
				{
					timeoutMs: 10_000,
					staleAfterMs: 200,
					retryMs: 50,
				},
			);

			await waitForResolved(() =>
				expect(
					leaf.getChannelMetrics(topic, rootId).reparentStale,
				).to.be.greaterThan(0),
			);
		} finally {
			await session.stop();
		}
	});

	it("resets an already-connected root after unanswered initial joins", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const rootNode = session.peers[0];
			const root = rootNode.services.fanout;
			const leaf = session.peers[1].services.fanout;
			const topic = "initial-join-zombie-reset";
			const rootId = root.publicKeyHash;
			const bootstrapAddrs = rootNode
				.getMultiaddrs()
				.filter((address) => !address.getComponents().some((c) => c.code === 290));
			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			const leafInternals = leaf as any;
			await waitForResolved(() =>
				expect(leafInternals.peers.get(rootId)).to.exist,
			);
			const rootPeer = leafInternals.peers.get(rootId);
			expect(rootPeer).to.exist;
			const connectionManager = leafInternals.components.connectionManager as any;
			const originalSendControl = leafInternals._sendControl;
			const originalCloseConnections =
				connectionManager.closeConnections.bind(connectionManager);
			let dropJoinControls = true;
			let resetCount = 0;

			leafInternals._sendControl = async (to: string, bytes: Uint8Array) => {
				if (dropJoinControls && to === rootId) return;
				return originalSendControl.call(leaf, to, bytes);
			};
			connectionManager.closeConnections = async (...args: any[]) => {
				if (args[0]?.toString?.() === rootPeer.peerId.toString()) {
					resetCount += 1;
					await originalCloseConnections(...args);
					dropJoinControls = false;
					return;
				}
				return originalCloseConnections(...args);
			};

			try {
				await leaf.joinChannel(
					topic,
					rootId,
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: false,
					},
					{
						timeoutMs: 5_000,
						bootstrap: bootstrapAddrs,
						retryMs: 5,
						joinReqTimeoutMs: 25,
						candidateCooldownMs: 0,
						candidateScoringMode: "ranked-strict",
					},
				);
				expect(resetCount).to.equal(1);
				expect(leaf.getChannelMetrics(topic, rootId).joinPeerResets).to.equal(1);
				expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);
				expect(connectionManager.getConnections(rootPeer.peerId).length).to.be.greaterThan(
					0,
				);
			} finally {
				leafInternals._sendControl = originalSendControl;
				connectionManager.closeConnections = originalCloseConnections;
			}
		} finally {
			await session.stop();
		}
	});

	it("starts a fresh join after an initial join timeout", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const root = session.peers[0].services.fanout;
			const leaf = session.peers[1].services.fanout;
			const topic = "initial-join-timeout-retry";
			const rootId = root.publicKeyHash;
			const channelOptions = {
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 0,
				maxChildren: 0,
				repair: false,
			};
			const joinOptions = {
				timeoutMs: 200,
				retryMs: 5,
				joinReqTimeoutMs: 25,
				candidateCooldownMs: 0,
				candidateScoringMode: "ranked-strict" as const,
			};

			await expect(
				leaf.joinChannel(topic, rootId, channelOptions, joinOptions),
			).to.be.rejectedWith("fanout join timed out after 200ms");

			root.openChannel(topic, rootId, {
				...channelOptions,
				role: "root",
				maxChildren: 1,
				uploadLimitBps: 1_000_000,
			});
			await leaf.joinChannel(topic, rootId, channelOptions, {
				...joinOptions,
				timeoutMs: 5_000,
			});
			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);
		} finally {
			await session.stop();
		}
	});

	it("clamps a stalled bootstrap dial to the initial join deadline", async function () {
		this.timeout(5_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const internals = fanout as any;
			const connectionManager = internals.components.connectionManager as any;
			const originalOpenConnection =
				connectionManager.openConnection.bind(connectionManager);
			const bootstrap = session.peers[0].getMultiaddrs()[0];
			expect(bootstrap).to.exist;
			connectionManager.openConnection = (
				_address: unknown,
				options?: { signal?: AbortSignal },
			) =>
				new Promise<never>((_resolve, reject) => {
					const signal = options?.signal;
					if (!signal) {
						reject(new Error("bootstrap dial was not given a bounded signal"));
						return;
					}
					const rejectOnAbort = () =>
						reject(signal.reason ?? new Error("bootstrap dial aborted"));
					if (signal.aborted) rejectOnAbort();
					else signal.addEventListener("abort", rejectOnAbort, { once: true });
				});

			const topic = "initial-join-bootstrap-deadline";
			const root = "unavailable-root";
			const startedAt = Date.now();
			try {
				const joining = fanout.joinChannel(
					topic,
					root,
					{
						msgRate: 1,
						msgSize: 8,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: false,
					},
					{
						timeoutMs: 40,
						bootstrap: [bootstrap!],
						bootstrapDialTimeoutMs: 5_000,
						retryMs: 1,
					},
				);
				await Promise.race([
					expect(joining).to.be.rejectedWith(
						"fanout join timed out after 40ms",
					),
					delay(500).then(() => {
						throw new Error("bootstrap join exceeded its deadline backstop");
					}),
				]);
			} finally {
				connectionManager.openConnection = originalOpenConnection;
			}

			expect(Date.now() - startedAt).to.be.lessThan(250);
			const metrics = fanout.getChannelMetrics(topic, root);
			expect(metrics.joinBootstrapDialAttempts).to.equal(1);
			expect(metrics.joinBootstrapDialFailures).to.equal(1);
			expect(metrics.joinDeadlineExpirations).to.equal(1);
		} finally {
			await session.stop();
		}
	});

	it("uses the first newly ready bootstrap without dialing a stale follower", async function () {
		this.timeout(10_000);
		const session = await TestSession.disconnected<FanoutServices>(3, {
			services: {
				fanout: (components) =>
					new FanoutTree(components, {
						connectionManager: false,
						random: () => 0.999_999,
					}),
			},
		});

		try {
			const rootNode = session.peers[0];
			const root = rootNode.services.fanout;
			const leaf = session.peers[1].services.fanout;
			const staleNode = session.peers[2];
			const liveAddress = rootNode.getMultiaddrs()[0];
			const staleAddress = staleNode.getMultiaddrs()[0];
			expect(liveAddress).to.exist;
			expect(staleAddress).to.exist;

			const topic = "initial-join-live-bootstrap-before-stale";
			const rootHash = root.publicKeyHash;
			root.openChannel(topic, rootHash, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			const internals = leaf as any;
			const connectionManager = internals.components.connectionManager as any;
			const originalOpenConnection =
				connectionManager.openConnection.bind(connectionManager);
			const dialedAddresses: string[] = [];
			connectionManager.openConnection = (
				address: { toString(): string },
				options?: { signal?: AbortSignal },
			) => {
				const value = address.toString();
				dialedAddresses.push(value);
				if (value !== staleAddress!.toString()) {
					return originalOpenConnection(address, options);
				}
				return new Promise<never>((_resolve, reject) => {
					const signal = options?.signal;
					if (!signal) {
						reject(new Error("stale bootstrap dial was not bounded"));
						return;
					}
					const rejectOnAbort = () =>
						reject(signal.reason ?? new Error("stale bootstrap dial aborted"));
					if (signal.aborted) rejectOnAbort();
					else signal.addEventListener("abort", rejectOnAbort, { once: true });
				});
			};

			try {
				await Promise.race([
					leaf.joinChannel(
						topic,
						rootHash,
						{
							msgRate: 1,
							msgSize: 8,
							uploadLimitBps: 0,
							maxChildren: 0,
							repair: false,
						},
						{
							timeoutMs: 500,
							bootstrap: [liveAddress!, staleAddress!],
							bootstrapDialTimeoutMs: 5_000,
							bootstrapMaxPeers: 0,
							trackerCandidates: 0,
							retryMs: 1,
						},
					),
					delay(1_000).then(() => {
						throw new Error("join did not use the newly ready bootstrap");
					}),
				]);
			} finally {
				connectionManager.openConnection = originalOpenConnection;
			}

			expect(leaf.getChannelStats(topic, rootHash)?.parent).to.equal(rootHash);
			expect(dialedAddresses[0]).to.equal(liveAddress!.toString());
			expect(dialedAddresses).not.to.include(staleAddress!.toString());
			const metrics = leaf.getChannelMetrics(topic, rootHash);
			expect(metrics.joinBootstrapDialAttempts).to.equal(1);
			expect(metrics.joinBootstrapDialFailures).to.equal(0);
		} finally {
			await session.stop();
		}
	});

	it("falls back from a ready unhelpful bootstrap to a later bootstrap", async function () {
		this.timeout(10_000);
		const session = await TestSession.disconnected<FanoutServices>(3, {
			services: {
				fanout: (components) =>
					new FanoutTree(components, {
						connectionManager: false,
						random: () => 0.999_999,
					}),
			},
		});

		try {
			const rootNode = session.peers[0];
			const leafNode = session.peers[1];
			const unhelpfulNode = session.peers[2];
			await session.connect([[leafNode, unhelpfulNode]]);
			const root = rootNode.services.fanout;
			const leaf = leafNode.services.fanout;
			const rootAddress = rootNode.getMultiaddrs()[0];
			const unhelpfulAddress = unhelpfulNode.getMultiaddrs()[0];
			expect(rootAddress).to.exist;
			expect(unhelpfulAddress).to.exist;

			const topic = "initial-join-unhelpful-bootstrap-fallback";
			const rootHash = root.publicKeyHash;
			const unhelpfulHash = unhelpfulNode.services.fanout.publicKeyHash;
			root.openChannel(topic, rootHash, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			const internals = leaf as any;
			const connectionManager = internals.components.connectionManager as any;
			const originalOpenConnection =
				connectionManager.openConnection.bind(connectionManager);
			const originalTryJoinOnce = internals.tryJoinOnce.bind(internals);
			const originalSendControl = internals._sendControl.bind(internals);
			const dialedAddresses: string[] = [];
			let triedReadyBootstrapBeforeRootDial = false;
			connectionManager.openConnection = (
				address: { toString(): string },
				options?: { signal?: AbortSignal },
			) => {
				dialedAddresses.push(address.toString());
				return originalOpenConnection(address, options);
			};
			internals.tryJoinOnce = (...args: any[]) => {
				if (args[1] === unhelpfulHash) {
					triedReadyBootstrapBeforeRootDial = !dialedAddresses.includes(
						rootAddress!.toString(),
					);
				}
				return originalTryJoinOnce(...args);
			};
			internals._sendControl = (to: string, ...args: any[]) => {
				if (to === unhelpfulHash) return Promise.resolve();
				return originalSendControl(to, ...args);
			};

			try {
				await leaf.joinChannel(
					topic,
					rootHash,
					{
						msgRate: 1,
						msgSize: 8,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: false,
					},
					{
						timeoutMs: 1_000,
						bootstrap: [unhelpfulAddress!, rootAddress!],
						bootstrapDialTimeoutMs: 200,
						bootstrapMaxPeers: 1,
						bootstrapEnsureIntervalMs: 10_000,
						trackerCandidates: 0,
						joinReqTimeoutMs: 30,
						retryMs: 1,
					},
				);
			} finally {
				connectionManager.openConnection = originalOpenConnection;
				internals.tryJoinOnce = originalTryJoinOnce;
				internals._sendControl = originalSendControl;
			}

			expect(triedReadyBootstrapBeforeRootDial).to.equal(true);
			expect(dialedAddresses).to.include(rootAddress!.toString());
			expect(leaf.getChannelStats(topic, rootHash)?.parent).to.equal(rootHash);
		} finally {
			await session.stop();
		}
	});

	it("keeps an excluded bootstrap excluded after transient readiness", async function () {
		this.timeout(5_000);
		const session = await TestSession.disconnected<FanoutServices>(3, {
			services: {
				fanout: (components) =>
					new FanoutTree(components, {
						connectionManager: false,
						random: () => 0.999_999,
					}),
			},
		});

		try {
			const targetNode = session.peers[0];
			const leafNode = session.peers[1];
			const excludedNode = session.peers[2];
			await session.connect([[leafNode, excludedNode]]);
			const leaf = leafNode.services.fanout;
			const targetHash = targetNode.services.fanout.publicKeyHash;
			const excludedHash = excludedNode.services.fanout.publicKeyHash;
			const targetAddress = targetNode.getMultiaddrs()[0];
			const excludedAddress = excludedNode.getMultiaddrs()[0];
			expect(targetAddress).to.exist;
			expect(excludedAddress).to.exist;

			const internals = leaf as any;
			const channelId = leaf.openChannel("transient-bootstrap-exclusion", targetHash, {
				role: "node",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 0,
				maxChildren: 0,
				repair: false,
			});
			const channel = internals.channelsBySuffixKey.get(channelId.suffixKey);
			const originalConnectedHash =
				internals.connectedPeerHashForBootstrap.bind(internals);
			internals.connectedPeerHashForBootstrap = (address: {
				toString(): string;
			}) =>
				address.toString() === excludedAddress!.toString()
					? undefined
					: originalConnectedHash(address);

			try {
				const peers = await internals.ensureBootstrapPeers(
					[excludedAddress!, targetAddress!],
					500,
					new AbortController().signal,
					1,
					{
						metrics: channel.metrics,
						preferConnected: true,
						excludeReadyPeerHashes: new Set([excludedHash]),
					},
				);
				expect(peers).to.deep.equal([targetHash]);
				expect(channel.metrics.joinBootstrapDialAttempts).to.equal(2);
				expect(channel.metrics.joinBootstrapDialFailures).to.equal(0);
			} finally {
				internals.connectedPeerHashForBootstrap = originalConnectedHash;
			}
		} finally {
			await session.stop();
		}
	});

	it("rotates from a responsive tracker with stale candidates", async function () {
		this.timeout(5_000);
		const session = await TestSession.disconnected<FanoutServices>(3, {
			services: {
				fanout: (components) =>
					new FanoutTree(components, {
						connectionManager: false,
						random: () => 0.999_999,
					}),
			},
		});

		try {
			const rootNode = session.peers[0];
			const leafNode = session.peers[1];
			const staleTrackerNode = session.peers[2];
			await session.connect([[leafNode, staleTrackerNode]]);
			const root = rootNode.services.fanout;
			const leaf = leafNode.services.fanout;
			const rootHash = root.publicKeyHash;
			const staleTrackerHash = staleTrackerNode.services.fanout.publicKeyHash;
			const rootAddress = rootNode.getMultiaddrs()[0];
			const staleTrackerAddress = staleTrackerNode.getMultiaddrs()[0];
			expect(rootAddress).to.exist;
			expect(staleTrackerAddress).to.exist;

			const topic = "responsive-stale-bootstrap-rotation";
			root.openChannel(topic, rootHash, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			const internals = leaf as any;
			const originalQueryTrackers = internals.queryTrackers.bind(internals);
			const originalTryJoinOnce = internals.tryJoinOnce.bind(internals);
			const queriedCohorts: string[][] = [];
			const attemptedParents: string[] = [];
			internals.queryTrackers = async (
				_channel: unknown,
				peers: string[],
			) => {
				queriedCohorts.push([...peers]);
				if (peers.includes(staleTrackerHash)) {
					return [
						{
							hash: staleTrackerHash,
							addrs: [] as any[],
							level: 0,
							freeSlots: 1,
							bidPerByte: 0,
						},
					];
				}
				return [];
			};
			internals.tryJoinOnce = (...args: any[]) => {
				attemptedParents.push(args[1]);
				return originalTryJoinOnce(...args);
			};

			try {
				await leaf.joinChannel(
					topic,
					rootHash,
					{
						msgRate: 1,
						msgSize: 8,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: false,
					},
					{
						timeoutMs: 1_000,
						bootstrap: [staleTrackerAddress!, rootAddress!],
						bootstrapDialTimeoutMs: 200,
						bootstrapMaxPeers: 1,
						bootstrapEnsureIntervalMs: 10_000,
						trackerCandidates: 1,
						joinReqTimeoutMs: 50,
						retryMs: 1,
					},
				);
			} finally {
				internals.queryTrackers = originalQueryTrackers;
				internals.tryJoinOnce = originalTryJoinOnce;
			}

			expect(queriedCohorts.slice(0, 2)).to.deep.equal([
				[staleTrackerHash],
				[rootHash],
			]);
			expect(attemptedParents.slice(0, 2)).to.deep.equal([
				staleTrackerHash,
				rootHash,
			]);
			expect(leaf.getChannelStats(topic, rootHash)?.parent).to.equal(rootHash);
		} finally {
			await session.stop();
		}
	});

	it("clamps a silent connected parent to the initial join deadline", async function () {
		this.timeout(5_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);
			const root = session.peers[0].services.fanout;
			const leaf = session.peers[1].services.fanout;
			const internals = leaf as any;
			const originalSendControl = internals._sendControl;
			internals._sendControl = async () => {
				// Model a recently stopped channel host on a still-live stream.
			};

			const topic = "initial-join-request-deadline";
			const rootHash = root.publicKeyHash;
			const startedAt = Date.now();
			try {
				const joining = leaf.joinChannel(
					topic,
					rootHash,
					{
						msgRate: 1,
						msgSize: 8,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: false,
					},
					{
						timeoutMs: 40,
						joinReqTimeoutMs: 5_000,
						retryMs: 1,
					},
				);
				await Promise.race([
					expect(joining).to.be.rejectedWith(
						"fanout join timed out after 40ms",
					),
					delay(500).then(() => {
						throw new Error("join request exceeded its deadline backstop");
					}),
				]);
			} finally {
				internals._sendControl = originalSendControl;
			}

			expect(Date.now() - startedAt).to.be.lessThan(250);
			const metrics = leaf.getChannelMetrics(topic, rootHash);
			expect(metrics.joinConnectedCandidateAttempts).to.equal(1);
			expect(metrics.joinReqTimeouts).to.equal(1);
			expect(metrics.joinDeadlineExpirations).to.equal(1);
		} finally {
			await session.stop();
		}
	});

	it("preserves the 1ms sub-timeout floor for an unbounded join", async function () {
		this.timeout(5_000);
		const session = await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);
			const rootNode = session.peers[0];
			const root = rootNode.services.fanout;
			const leaf = session.peers[1].services.fanout;
			const topic = "unbounded-zero-sub-timeouts";
			const rootHash = root.publicKeyHash;
			const channelOptions = {
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 0,
				maxChildren: 0,
				repair: false,
			};
			root.openChannel(topic, rootHash, {
				...channelOptions,
				role: "root",
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
			});

			const internals = leaf as any;
			const originalQueryTrackers = internals.queryTrackers;
			const originalTryJoinOnce = internals.tryJoinOnce;
			const trackerTimeouts: number[] = [];
			const joinTimeouts: number[] = [];
			internals.queryTrackers = async (...args: any[]) => {
				trackerTimeouts.push(args[3]);
				return [
					{
						hash: rootHash,
						addrs: [] as any[],
						level: 0,
						freeSlots: 1,
						bidPerByte: 0,
					},
				];
			};
			internals.tryJoinOnce = async (...args: any[]) => {
				const channel = args[0];
				const parentHash = args[1];
				joinTimeouts.push(args[3]);
				channel.parent = parentHash;
				channel.level = 1;
				channel.routeFromRoot = [rootHash];
				return { ok: true };
			};

			try {
				await leaf.joinChannel(topic, rootHash, channelOptions, {
					timeoutMs: 0,
					bootstrap: rootNode.getMultiaddrs(),
					bootstrapMaxPeers: 1,
					trackerCandidates: 1,
					trackerQueryTimeoutMs: 0,
					joinReqTimeoutMs: 0,
					retryMs: 1,
				});
			} finally {
				internals.queryTrackers = originalQueryTrackers;
				internals.tryJoinOnce = originalTryJoinOnce;
			}

			expect(trackerTimeouts).to.deep.equal([1]);
			expect(joinTimeouts).to.deep.equal([1]);
			expect(leaf.getChannelStats(topic, rootHash)?.parent).to.equal(rootHash);
		} finally {
			await session.stop();
		}
	});

	it("cancels an unsent join request when its attempt settles", async function () {
		this.timeout(5_000);
		const session = await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);
			const root = session.peers[0].services.fanout;
			const leaf = session.peers[1].services.fanout;
			const topic = "cancel-unsent-join-request";
			const rootHash = root.publicKeyHash;
			const channelOptions = {
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 0,
				maxChildren: 0,
				repair: false,
			};
			root.openChannel(topic, rootHash, {
				...channelOptions,
				role: "root",
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
			});
			const channelId = leaf.openChannel(topic, rootHash, {
				...channelOptions,
				role: "node",
			});
			const internals = leaf as any;
			const channel = internals.channelsBySuffixKey.get(channelId.suffixKey);
			await waitForResolved(() =>
				expect(internals.peers.get(rootHash)).to.exist,
			);
			const rootStream = internals.peers.get(rootHash);
			const originalWaitForWrite = rootStream.waitForWrite.bind(rootStream);
			let markWriteStarted!: () => void;
			const writeStarted = new Promise<void>((resolve) => {
				markWriteStarted = resolve;
			});
			let releaseBlockedWrite = () => {};
			let writeCalls = 0;
			rootStream.waitForWrite = async (
				bytes: Uint8Array,
				priority: number,
				signal?: AbortSignal,
			) => {
				writeCalls += 1;
				markWriteStarted();
				await new Promise<void>((resolve, reject) => {
					let finished = false;
					const finish = (callback: () => void) => {
						if (finished) return;
						finished = true;
						signal?.removeEventListener("abort", onAbort);
						callback();
					};
					const onAbort = () =>
						finish(() => reject(signal?.reason ?? new Error("write aborted")));
					releaseBlockedWrite = () => finish(resolve);
					if (signal?.aborted) onAbort();
					else signal?.addEventListener("abort", onAbort, { once: true });
				});
				await originalWaitForWrite(bytes, priority, signal);
			};

			try {
				const resultPromise = internals.tryJoinOnce(
					channel,
					rootHash,
					0x12345678,
					30,
					new AbortController().signal,
				);
				await writeStarted;
				expect(await resultPromise).to.deep.equal({
					ok: false,
					timedOut: true,
				});
				expect(channel.pendingJoin.size).to.equal(0);

				releaseBlockedWrite();
				await delay(100);
				expect(root.getChannelMetrics(topic, rootHash).joinReqReceived).to.equal(
					0,
				);
				expect(root.getChannelStats(topic, rootHash)?.children).to.equal(0);

				const abortController = new AbortController();
				const abortReason = new Error("cancel join attempt");
				abortController.abort(abortReason);
				await expect(
					internals.tryJoinOnce(
						channel,
						rootHash,
						0x12345679,
						1_000,
						abortController.signal,
					),
				).to.be.rejectedWith("cancel join attempt");
				expect(channel.pendingJoin.size).to.equal(0);
				expect(writeCalls).to.equal(1);
			} finally {
				rootStream.waitForWrite = originalWaitForWrite;
			}
		} finally {
			await session.stop();
		}
	});

	it("does not gate a successful join on tracker feedback", async function () {
		this.timeout(5_000);
		const session = await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);
			const rootNode = session.peers[0];
			const root = rootNode.services.fanout;
			const leaf = session.peers[1].services.fanout;
			const topic = "initial-join-non-gating-feedback";
			const rootHash = root.publicKeyHash;
			root.openChannel(topic, rootHash, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			const internals = leaf as any;
			const originalSendTrackerFeedback = internals.sendTrackerFeedback;
			let releaseFeedback!: () => void;
			const feedbackRelease = new Promise<void>((resolve) => {
				releaseFeedback = resolve;
			});
			let feedbackCalls = 0;
			internals.sendTrackerFeedback = async () => {
				feedbackCalls += 1;
				await feedbackRelease;
			};

			try {
				await Promise.race([
					leaf.joinChannel(
						topic,
						rootHash,
						{
							msgRate: 1,
							msgSize: 8,
							uploadLimitBps: 0,
							maxChildren: 0,
							repair: false,
						},
						{
							timeoutMs: 250,
							bootstrap: rootNode.getMultiaddrs(),
							trackerCandidates: 0,
							retryMs: 1,
						},
					),
					delay(500).then(() => {
						throw new Error("tracker feedback gated the successful join");
					}),
				]);
				expect(feedbackCalls).to.equal(1);
				expect(leaf.getChannelStats(topic, rootHash)?.parent).to.equal(rootHash);
			} finally {
				releaseFeedback();
				internals.sendTrackerFeedback = originalSendTrackerFeedback;
			}
		} finally {
			await session.stop();
		}
	});

	it("tries a connected donor before shuffled stale tracker candidates", async function () {
		this.timeout(10_000);
		const session = await TestSession.disconnected<FanoutServices>(3, {
			services: {
				fanout: (components) =>
					new FanoutTree(components, {
						connectionManager: false,
						random: () => 0.999_999,
					}),
			},
		});

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);
			const root = session.peers[0].services.fanout;
			const relayNode = session.peers[1];
			const relay = relayNode.services.fanout;
			const leaf = session.peers[2].services.fanout;
			const topic = "initial-join-connected-donor-first";
			const rootHash = root.publicKeyHash;
			const channelOptions = {
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: false,
			};

			root.openChannel(topic, rootHash, { ...channelOptions, role: "root" });
			await relay.joinChannel(topic, rootHash, channelOptions, {
				timeoutMs: 2_000,
				joinReqTimeoutMs: 100,
				retryMs: 5,
			});

			const internals = leaf as any;
			const originalQueryTrackers = internals.queryTrackers;
			const staleAddresses = relayNode.getMultiaddrs();
			internals.queryTrackers = async () =>
				Array.from({ length: 6 }, (_, index) => ({
					hash: `stale-parent-${index}`,
					addrs: staleAddresses,
					level: 0,
					freeSlots: 8,
					bidPerByte: 0,
				}));
			try {
				await leaf.joinChannel(topic, rootHash, channelOptions, {
					timeoutMs: 500,
					bootstrap: staleAddresses,
					bootstrapMaxPeers: 1,
					bootstrapDialTimeoutMs: 100,
					joinReqTimeoutMs: 100,
					retryMs: 5,
					candidateScoringMode: "ranked-shuffle",
					candidateShuffleTopK: 8,
				});
			} finally {
				internals.queryTrackers = originalQueryTrackers;
			}

			expect(leaf.getChannelStats(topic, rootHash)?.parent).to.equal(
				relay.publicKeyHash,
			);
			const metrics = leaf.getChannelMetrics(topic, rootHash);
			expect(metrics.joinConnectedCandidateAttempts).to.equal(1);
			expect(metrics.joinUnconnectedCandidateAttempts).to.equal(0);
			expect(metrics.joinBootstrapDialAttempts).to.equal(0);
			expect(metrics.joinCandidateDialAttempts).to.equal(0);
		} finally {
			await session.stop();
		}
	});

	it("promotes only one connected candidate ahead of a ranked unconnected root", async function () {
		this.timeout(10_000);
		const session = await createFanoutTestSession(5);

		try {
			const rootNode = session.peers[0];
			const leafNode = session.peers[1];
			const bystanderNodes = session.peers.slice(2);
			await session.connect(
				bystanderNodes.map((bystander) => [leafNode, bystander]),
			);

			const root = rootNode.services.fanout;
			const leaf = leafNode.services.fanout;
			const topic = "initial-join-connected-budget-fairness";
			const rootHash = root.publicKeyHash;
			const rootAddress = rootNode.getMultiaddrs()[0];
			const bootstrapAddress = bystanderNodes[0]!.getMultiaddrs()[0];
			expect(rootAddress).to.exist;
			expect(bootstrapAddress).to.exist;
			root.openChannel(topic, rootHash, {
				role: "root",
				msgRate: 1,
				msgSize: 8,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			const internals = leaf as any;
			expect(internals.peers.has(rootHash)).to.equal(false);
			const bystanderHashes = new Set(
				bystanderNodes.map((node) => node.services.fanout.publicKeyHash),
			);
			const connectionManager = internals.components.connectionManager as any;
			const originalCloseConnections =
				connectionManager.closeConnections.bind(connectionManager);
			const originalQueryTrackers = internals.queryTrackers;
			const originalSendControl = internals._sendControl;
			const originalSendTrackerFeedback = internals.sendTrackerFeedback;
			const originalTryJoinOnce = internals.tryJoinOnce;
			const attemptedParents: string[] = [];
			internals.queryTrackers = async () => [
				{
					hash: rootHash,
					addrs: [rootAddress!],
					level: 0,
					freeSlots: 1,
					bidPerByte: 0,
				},
			];
			internals._sendControl = async (to: string, bytes: Uint8Array) => {
				if (bystanderHashes.has(to)) return;
				return originalSendControl.call(leaf, to, bytes);
			};
			internals.sendTrackerFeedback = async () => {};
			internals.tryJoinOnce = async (...args: any[]) => {
				attemptedParents.push(args[1]);
				return originalTryJoinOnce.apply(leaf, args);
			};
			connectionManager.closeConnections = async (...args: any[]) => {
				const peerId = args[0];
				const peer = [...internals.peers.entries()].find(
					([, stream]: any[]) => stream.peerId.toString() === peerId.toString(),
				);
				if (peer && bystanderHashes.has(peer[0])) return;
				return originalCloseConnections(...args);
			};

			try {
				await leaf.joinChannel(
					topic,
					rootHash,
					{
						msgRate: 1,
						msgSize: 8,
						uploadLimitBps: 0,
						maxChildren: 0,
						repair: false,
					},
					{
						timeoutMs: 500,
						bootstrap: [bootstrapAddress!],
						bootstrapMaxPeers: 1,
						trackerCandidates: 1,
						joinAttemptsPerRound: 2,
						joinReqTimeoutMs: 50,
						candidateCooldownMs: 0,
						candidateScoringMode: "ranked-shuffle",
						candidateShuffleTopK: 0,
						retryMs: 1,
					},
				);
			} finally {
				connectionManager.closeConnections = originalCloseConnections;
				internals.queryTrackers = originalQueryTrackers;
				internals._sendControl = originalSendControl;
				internals.sendTrackerFeedback = originalSendTrackerFeedback;
				internals.tryJoinOnce = originalTryJoinOnce;
			}

			expect(leaf.getChannelStats(topic, rootHash)?.parent).to.equal(rootHash);
			expect(bystanderHashes.has(attemptedParents[0]!)).to.equal(true);
			expect(attemptedParents[1]).to.equal(rootHash);
			const metrics = leaf.getChannelMetrics(topic, rootHash);
			expect(metrics.joinConnectedCandidateAttempts).to.equal(1);
			expect(metrics.joinUnconnectedCandidateAttempts).to.equal(1);
		} finally {
			await session.stop();
		}
	});

	it("keeps rejoining after the initial join timeout has elapsed", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const rootNode = session.peers[0];
			const root = rootNode.services.fanout;
			const leaf = session.peers[1].services.fanout;

			const bootstrapAddrs = rootNode
				.getMultiaddrs()
				.filter((x) => !x.getComponents().some((c) => c.code === 290));

			const topic = "rejoin-timeout";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				maxDataAgeMs: 10_000,
				repair: false,
			});

			const timeoutMs = 2_000;
			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					maxDataAgeMs: 10_000,
					repair: false,
				},
				{
					timeoutMs,
					bootstrap: bootstrapAddrs,
					staleAfterMs: 250,
					retryMs: 50,
				},
			);

			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);

			// Keep data flowing until after the initial `timeoutMs` has elapsed, so any later
			// detach/rejoin would have previously tripped the join-loop timeout bug.
			const keepAliveUntil = Date.now() + timeoutMs + 500;
			while (Date.now() < keepAliveUntil) {
				await root.publishData(topic, rootId, new Uint8Array([0x01]));
				// eslint-disable-next-line no-await-in-loop
				await delay(100);
			}

			// Stop sending for long enough to trigger stale re-parenting.
			await waitForResolved(
				() =>
					expect(
						leaf.getChannelMetrics(topic, rootId).reparentStale,
					).to.be.greaterThan(0),
				{ timeout: 20_000, delayInterval: 50 },
			);

			// Once it has re-joined, it should receive fresh data again.
			let markerReceived = false;
			leaf.addEventListener("fanout:data", (ev: any) => {
				if (ev.detail.topic !== topic) return;
				if (ev.detail.root !== rootId) return;
				if ((ev.detail.payload as Uint8Array)?.[0] !== 0x99) return;
				markerReceived = true;
			});
			for (let i = 0; i < 20 && !markerReceived; i++) {
				await root.publishData(topic, rootId, new Uint8Array([0x99]));
				// eslint-disable-next-line no-await-in-loop
				await delay(100);
			}
			expect(markerReceived).to.equal(true);
		} finally {
			await session.stop();
		}
	});

	it("re-parents when its parent disconnects", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

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

			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(
				relay.publicKeyHash,
			);

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

	it("re-parents after repeated parent data write failures", async function () {
			this.timeout(30_000);
			const session: TestSession<{ fanout: FanoutTree }> =
				await createFanoutTestSession(2);

			try {
				await session.connect([[session.peers[0], session.peers[1]]]);

				const rootNode = session.peers[0];
				const root = rootNode.services.fanout;
				const leaf = session.peers[1].services.fanout;
				const bootstrapAddrs = rootNode
					.getMultiaddrs()
					.filter((x) => !x.getComponents().some((c) => c.code === 290));

				const topic = "write-fail-reparent";
				const rootId = root.publicKeyHash;

				root.openChannel(topic, rootId, {
					role: "root",
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					allowKick: true,
					repair: true,
				});

				await leaf.joinChannel(
					topic,
					rootId,
					{
						msgRate: 10,
						msgSize: 64,
						uploadLimitBps: 0,
						maxChildren: 0,
						allowKick: true,
						repair: true,
					},
					{ timeoutMs: 10_000, bootstrap: bootstrapAddrs, retryMs: 50 },
				);

				expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);

				const rootToLeaf = (root as any).peers.get(leaf.publicKeyHash);
				expect(rootToLeaf).to.exist;
				let failedWrites = 0;
				rootToLeaf.write = () => {
					failedWrites += 1;
					throw new Error("simulated fanout data write failure");
				};

				for (let i = 0; i < 3; i++) {
					// eslint-disable-next-line no-await-in-loop
					await root.publishData(topic, rootId, new Uint8Array([i]));
				}

				await waitForResolved(
					() => {
						expect(failedWrites).to.be.at.least(3);
						expect(root.getChannelStats(topic, rootId)?.children).to.equal(0);
						expect(root.getChannelMetrics(topic, rootId).dataWriteDrops).to.be.at.least(
							3,
						);
					},
					{ timeout: 10_000, delayInterval: 50 },
				);

				await waitForResolved(
					() =>
						expect(
							leaf.getChannelMetrics(topic, rootId).reparentDisconnect,
						).to.be.greaterThan(0),
					{ timeout: 20_000, delayInterval: 50 },
				);

				await waitForResolved(
					() => expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
					{ timeout: 20_000, delayInterval: 50 },
				);

				let markerReceived = false;
				leaf.addEventListener("fanout:data", (ev: any) => {
					if (ev.detail.topic !== topic) return;
					if (ev.detail.root !== rootId) return;
					if ((ev.detail.payload as Uint8Array)?.[0] !== 0x88) return;
					markerReceived = true;
				});
				for (let i = 0; i < 20 && !markerReceived; i++) {
					// eslint-disable-next-line no-await-in-loop
					await root.publishData(topic, rootId, new Uint8Array([0x88]));
					// eslint-disable-next-line no-await-in-loop
					await delay(100);
				}
				expect(markerReceived).to.equal(true);
			} finally {
				await session.stop();
			}
		});

		it("sends the end watermark to children that join after publishEnd", async function () {
			this.timeout(30_000);
			const session: TestSession<{ fanout: FanoutTree }> =
				await createFanoutTestSession(2);

			try {
				await session.connect([[session.peers[0], session.peers[1]]]);

				const root = session.peers[0].services.fanout;
				const leaf = session.peers[1].services.fanout;

				const topic = "late-end";
				const rootId = root.publicKeyHash;
				const channelId = root.getChannelId(topic, rootId);

				root.openChannel(topic, rootId, {
					role: "root",
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
				});

				await root.publishEnd(topic, rootId, 3);
				expect(
					(root as any).channelsBySuffixKey.get(channelId.suffixKey)?.endSeqExclusive,
				).to.equal(3);

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

				await waitForResolved(
					() => {
						const leafState = (leaf as any).channelsBySuffixKey.get(
							channelId.suffixKey,
						);
						expect(leafState?.endSeqExclusive).to.equal(3);
						expect([...leafState.missingSeqs].sort((a, b) => a - b)).to.deep.equal([
							0, 1, 2,
						]);
					},
					{ timeout: 10_000, delayInterval: 50 },
				);
			} finally {
				await session.stop();
			}
		});

		it("retries the end watermark for existing children", async function () {
			this.timeout(30_000);
			const session: TestSession<{ fanout: FanoutTree }> =
				await createFanoutTestSession(2);

			try {
				await session.connect([[session.peers[0], session.peers[1]]]);

				const root = session.peers[0].services.fanout;
				const leaf = session.peers[1].services.fanout;

				const topic = "end-heartbeat";
				const rootId = root.publicKeyHash;
				const channelId = root.getChannelId(topic, rootId);

				root.openChannel(topic, rootId, {
					role: "root",
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: true,
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
					{ timeoutMs: 10_000 },
				);

				await root.publishEnd(topic, rootId, 3);
				const leafState = (leaf as any).channelsBySuffixKey.get(
					channelId.suffixKey,
				);
				await waitForResolved(
					() => expect(leafState?.endSeqExclusive).to.equal(3),
					{ timeout: 10_000, delayInterval: 50 },
				);

				leafState.endSeqExclusive = -1;
				leafState.nextExpectedSeq = 0;
				leafState.missingSeqs.clear();

				const rootState = (root as any).channelsBySuffixKey.get(
					channelId.suffixKey,
				);
				rootState.lastIHaveSentAt = 0;
				await (root as any).maybeSendIHave(rootState, Date.now() + 5_000);

				await waitForResolved(
					() => {
						expect(leafState.endSeqExclusive).to.equal(3);
						expect([...leafState.missingSeqs].sort((a, b) => a - b)).to.deep.equal([
							0, 1, 2,
						]);
					},
					{ timeout: 10_000, delayInterval: 50 },
				);
			} finally {
				await session.stop();
			}
		});

	it("prevents stable disconnected components when an intermediate relay loses the root", async function () {
		this.timeout(30_000);
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const rootNode = session.peers[0];
			const relayNode = session.peers[1];

			const root = rootNode.services.fanout;
			const relay = relayNode.services.fanout;
			const leaf = session.peers[2].services.fanout;

			const bootstrapAddrs = rootNode
				.getMultiaddrs()
				.filter((x) => !x.getComponents().some((c) => c.code === 290));

			const topic = "partition";
			const rootId = root.publicKeyHash;

			root.openChannel(topic, rootId, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: false,
				},
				{ timeoutMs: 10_000, bootstrap: bootstrapAddrs },
			);

			await leaf.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: false,
				},
				{ timeoutMs: 10_000, bootstrap: bootstrapAddrs },
			);

			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(
				relay.publicKeyHash,
			);

			// Break the relay<->root connection but keep relay alive.
			const rootConnMgr = (root as any)?.components?.connectionManager;
			const relayConnMgr = (relay as any)?.components?.connectionManager;
			expect(rootConnMgr).to.exist;
			expect(relayConnMgr).to.exist;
			const relayAsSeenByRoot = (root as any)?.peers?.get?.(
				relay.publicKeyHash,
			);
			const rootAsSeenByRelay = (relay as any)?.peers?.get?.(rootId);
			const relayPeerId = relayAsSeenByRoot?.peerId;
			const rootPeerId = rootAsSeenByRelay?.peerId;
			expect(relayPeerId).to.exist;
			expect(rootPeerId).to.exist;
			await Promise.allSettled([
				rootConnMgr?.closeConnections?.(relayPeerId),
				relayConnMgr?.closeConnections?.(rootPeerId),
			]);

			// Ensure the connection is actually down (otherwise the rest of the test is meaningless).
			await waitForResolved(
				() => {
					const a = rootConnMgr?.getConnections?.(relayPeerId) ?? [];
					const b = relayConnMgr?.getConnections?.(rootPeerId) ?? [];
					expect(a.length).to.equal(0);
					expect(b.length).to.equal(0);
				},
				{ timeout: 20_000, delayInterval: 50 },
			);

			// Relay should detect the disconnect from its parent and trigger a reparent.
			// `stats.parent` can be transiently undefined and then quickly restored if the
			// root reconnects, so assert on the metric rather than the brief state.
			await waitForResolved(
				() =>
					expect(
						relay.getChannelMetrics(topic, rootId).reparentDisconnect,
					).to.be.greaterThan(0),
				{ timeout: 20_000, delayInterval: 50 },
			);

			// Relay should kick its children once it loses the rooted route, and leaf should
			// rejoin directly to the root instead of stabilizing in a disconnected component.
			await waitForResolved(
				() =>
					expect(
						leaf.getChannelMetrics(topic, rootId).reparentKicked,
					).to.be.greaterThan(0),
				{ timeout: 20_000, delayInterval: 50 },
			);
			await waitForResolved(
				() =>
					expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
				{ timeout: 20_000, delayInterval: 50 },
			);
		} finally {
			await session.stop();
		}
	});

	it("rate limits proxy publish ingress (abuse resistance)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const root = session.peers[0].services.fanout;
			const leaf = session.peers[1].services.fanout;

			const topic = "proxy-publish-rate-limit";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 32,
				uploadLimitBps: 1_000_000,
				maxChildren: 1,
				repair: false,
				// Deterministic drop: capacity=1 byte, but payload > 1 byte.
				proxyPublishBudgetBps: 1,
				proxyPublishBurstMs: 1_000,
			});

			const leafChannel = new FanoutChannel(leaf, { topic, root: rootId });
			await leafChannel.join(
				{
					msgRate: 10,
					msgSize: 32,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: false,
				},
				{ timeoutMs: 10_000 },
			);

			let received = 0;
			leafChannel.addEventListener("data", () => {
				received += 1;
			});

			await leafChannel.publish(new Uint8Array(16).fill(7));
			await delay(200);
			expect(received).to.equal(0);

			const id = root.getChannelId(topic, rootId);
			const ch = (root as any).channelsBySuffixKey.get(id.suffixKey);
			expect(ch?.metrics?.proxyPublishDrops ?? 0).to.be.greaterThan(0);
		} finally {
			await session.stop();
		}
	});

	it("rate limits unicast ingress (abuse resistance)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[0], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const leafA = session.peers[1].services.fanout;
			const leafB = session.peers[2].services.fanout;

			const topic = "unicast-rate-limit";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 2,
				repair: false,
				// Deterministic drop for unicast payload frames.
				unicastBudgetBps: 1,
				unicastBurstMs: 1_000,
			});

			const leafAChannel = new FanoutChannel(leafA, { topic, root: rootId });
			await leafAChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: false,
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
					repair: false,
				},
				{ timeoutMs: 10_000 },
			);

			let received: Uint8Array | undefined;
			leafBChannel.addEventListener("unicast", (ev: any) => {
				received = (ev?.detail as any)?.payload;
			});

			await leafAChannel.unicastTo(
				leafB.publicKeyHash,
				new Uint8Array([1, 2, 3]),
				{
					timeoutMs: 5_000,
				},
			);

			await delay(200);
			expect(received).to.equal(undefined);

			const id = root.getChannelId(topic, rootId);
			const ch = (root as any).channelsBySuffixKey.get(id.suffixKey);
			expect(ch?.metrics?.unicastDrops ?? 0).to.be.greaterThan(0);
		} finally {
			await session.stop();
		}
	});
});
