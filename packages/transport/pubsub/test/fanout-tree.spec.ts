import { TestSession } from "@peerbit/libp2p-test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutChannel, FanoutTree } from "../src/index.js";

type FanoutServices = { fanout: FanoutTree };

const createFanoutService = (components: any) =>
	new FanoutTree(components, { connectionManager: false });

const createFanoutTestSession = (n: number) =>
	TestSession.disconnected<FanoutServices>(n, {
		services: {
			fanout: createFanoutService,
		},
	});

type ImproveCandidate = {
	hash: string;
	addrs: [];
	level: number;
	freeSlots: number;
	bidPerByte: number;
};

type ImproveChannel = {
	parent: string;
	closed: boolean;
	isRoot: boolean;
	level: number;
	id: { root: string; key: Uint8Array };
	cachedTrackerCandidates: ImproveCandidate[];
	children: Map<string, { bidPerByte: number }>;
};

type ImproveOptions = {
	signal: AbortSignal;
	candidateShuffleTopK: number;
	candidateScoringMode: "ranked-shuffle" | "ranked-strict" | "weighted";
	candidateScoringWeights: {
		level: number;
		freeSlots: number;
		connected: number;
		bidPerByte: number;
		source: number;
	};
	joinAttemptsPerRound: number;
	joinReqTimeoutMs: number;
};

type ImproveContext = {
	publicKeyHash: string;
	peers: Map<string, { peerId: string; isReadable: boolean; isWritable: boolean }>;
	components: {
		connectionManager: {
			getConnections: (peerId: string) => unknown[];
		};
	};
	random: () => number;
	tryJoinOnce: (
		ch: ImproveChannel,
		parentHash: string,
		reqId: number,
		joinReqTimeoutMs: number,
		signal: AbortSignal,
		options?: { allowReplace?: boolean },
	) => Promise<{ ok: boolean }>;
	_sendControl: (to: string, payload: Uint8Array) => Promise<void>;
};

const maybeImproveParent = Reflect.get(FanoutTree.prototype, "maybeImproveParent") as (
	this: ImproveContext,
	ch: ImproveChannel,
	options: ImproveOptions,
) => Promise<boolean>;

const createImproveChannel = (
	overrides: Partial<ImproveChannel> = {},
): ImproveChannel => ({
	parent: "relay",
	closed: false,
	isRoot: false,
	level: 4,
	id: { root: "root", key: new Uint8Array([1, 2, 3]) },
	cachedTrackerCandidates: [],
	children: new Map(),
	...overrides,
});

const runMaybeImproveParent = async (args: {
	peerHashes: string[];
	cachedTrackerCandidates?: ImproveCandidate[];
	channelOverrides?: Partial<ImproveChannel>;
	getConnections?: (peerId: string) => unknown[];
	random?: () => number;
	options?: Partial<ImproveOptions>;
	tryJoinOnce?: ImproveContext["tryJoinOnce"];
}) => {
	const attempts: string[] = [];
	const ch = createImproveChannel({
		cachedTrackerCandidates: args.cachedTrackerCandidates ?? [],
		...args.channelOverrides,
	});
	const ctx: ImproveContext = {
		publicKeyHash: "self",
		peers: new Map(
			args.peerHashes.map((hash) => [
				hash,
				{
					peerId: hash,
					isReadable: true,
					isWritable: false,
				},
			]),
		),
		components: {
			connectionManager: {
				getConnections:
					args.getConnections ??
					((peerId) => (args.peerHashes.includes(peerId) ? [{}] : [])),
			},
		},
		random: args.random ?? (() => 0),
		tryJoinOnce: async (
			channel,
			parentHash,
			reqId,
			joinReqTimeoutMs,
			signal,
			options,
		) => {
			attempts.push(parentHash);
			return (
				args.tryJoinOnce?.(
					channel,
					parentHash,
					reqId,
					joinReqTimeoutMs,
					signal,
					options,
				) ?? { ok: false }
			);
		},
		_sendControl: async () => {},
	};

	const result = await maybeImproveParent.call(ctx, ch, {
		signal: new AbortController().signal,
		candidateShuffleTopK: 0,
		candidateScoringMode: "ranked-strict",
		candidateScoringWeights: {
			level: 1,
			freeSlots: 1,
			connected: 1,
			bidPerByte: 1,
			source: 1,
		},
		joinAttemptsPerRound: 8,
		joinReqTimeoutMs: 1_000,
		...args.options,
	});

	return { attempts, result, ch };
};

describe("fanout-tree", () => {
	it("falls back to peer readability when connection lookup throws", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["root", "relay"],
			getConnections: () => {
				throw new Error("boom");
			},
			tryJoinOnce: async (channel, parentHash) => {
				channel.parent = parentHash;
				return { ok: true };
			},
		});

		expect(result).to.equal(true);
		expect(attempts).to.deep.equal(["root"]);
		expect(ch.parent).to.equal("root");
	});

	it("orders ranked candidates by free slots, bid, source and hash", async () => {
		const byFreeSlots = await runMaybeImproveParent({
			peerHashes: ["relay", "slot-low", "slot-high"],
			cachedTrackerCandidates: [
				{ hash: "slot-low", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
				{ hash: "slot-high", addrs: [], level: 1, freeSlots: 2, bidPerByte: 0 },
			],
			tryJoinOnce: async () => ({ ok: false }),
		});
		expect(byFreeSlots.attempts[0]).to.equal("slot-high");

		const byBid = await runMaybeImproveParent({
			peerHashes: ["relay", "bid-low", "bid-high"],
			cachedTrackerCandidates: [
				{ hash: "bid-low", addrs: [], level: 1, freeSlots: 2, bidPerByte: 1 },
				{ hash: "bid-high", addrs: [], level: 1, freeSlots: 2, bidPerByte: 2 },
			],
			tryJoinOnce: async () => ({ ok: false }),
		});
		expect(byBid.attempts[0]).to.equal("bid-high");

		const bySource = await runMaybeImproveParent({
			peerHashes: ["root", "relay", "tracker-root"],
			cachedTrackerCandidates: [
				{
					hash: "tracker-root",
					addrs: [],
					level: 0,
					freeSlots: Number.MAX_SAFE_INTEGER,
					bidPerByte: 0,
				},
			],
			tryJoinOnce: async () => ({ ok: false }),
		});
		expect(bySource.attempts[0]).to.equal("root");

		const byHash = await runMaybeImproveParent({
			peerHashes: ["relay", "alpha", "beta"],
			cachedTrackerCandidates: [
				{ hash: "beta", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
				{ hash: "alpha", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
			],
			tryJoinOnce: async () => ({ ok: false }),
		});
		expect(byHash.attempts[0]).to.equal("alpha");
	});

	it("shuffles top ranked candidates when parent upgrades use ranked-shuffle", async () => {
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "alpha", "beta"],
			cachedTrackerCandidates: [
				{ hash: "alpha", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
				{ hash: "beta", addrs: [], level: 1, freeSlots: 1, bidPerByte: 0 },
			],
			random: () => 0,
			options: {
				candidateScoringMode: "ranked-shuffle",
				candidateShuffleTopK: 2,
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts.slice(0, 2)).to.deep.equal(["beta", "alpha"]);
	});

	it("supports weighted parent-upgrade candidate selection", async () => {
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "weighted-a", "weighted-b"],
			cachedTrackerCandidates: [
				{
					hash: "weighted-a",
					addrs: [],
					level: 1,
					freeSlots: 4,
					bidPerByte: 1,
				},
				{
					hash: "weighted-b",
					addrs: [],
					level: 2,
					freeSlots: 1,
					bidPerByte: 0,
				},
			],
			random: () => 0.999,
			options: {
				candidateScoringMode: "weighted",
				candidateShuffleTopK: 2,
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.have.length(2);
		expect(new Set(attempts)).to.deep.equal(new Set(["weighted-a", "weighted-b"]));
	});

	it("falls back to the existing weighted order when all candidate weights collapse", async () => {
		const { attempts, result } = await runMaybeImproveParent({
			peerHashes: ["relay", "nan-a", "nan-b"],
			cachedTrackerCandidates: [
				{ hash: "nan-a", addrs: [], level: 1, freeSlots: Number.NaN, bidPerByte: 0 },
				{ hash: "nan-b", addrs: [], level: 1, freeSlots: Number.NaN, bidPerByte: 0 },
			],
			options: {
				candidateScoringMode: "weighted",
				candidateShuffleTopK: 2,
			},
			tryJoinOnce: async () => ({ ok: false }),
		});

		expect(result).to.equal(false);
		expect(attempts.slice(0, 2)).to.deep.equal(["nan-a", "nan-b"]);
	});

	it("returns false when a join succeeds without actually replacing the parent", async () => {
		const { attempts, result, ch } = await runMaybeImproveParent({
			peerHashes: ["root", "relay"],
			tryJoinOnce: async () => ({ ok: true }),
		});

		expect(result).to.equal(false);
		expect(attempts).to.deep.equal(["root"]);
		expect(ch.parent).to.equal("relay");
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
				expect(getCachedRoute(ch, "target")).to.deep.equal([root, "child1", "target"]);

				// Drop child1, cached route must be treated as invalid and removed.
				ch.children.delete("child1");
				expect(getCachedRoute(ch, "target")).to.equal(undefined);
				expect(ch.routeByPeer.has("target")).to.equal(false);
			} finally {
				await session.stop();
			}
		});

	it("forms a small tree and delivers data", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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

			const publisherChannel = new FanoutChannel(publisher, { topic, root: rootId });
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

			const subscriberChannel = new FanoutChannel(subscriber, { topic, root: rootId });
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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(5);

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

	it("supports economical unicast with ACKs (shared intermediate hop)", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(4);

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
				await senderChannel.unicastAck(targetRoute!, payload, { timeoutMs: 2_000 });
				await waitForResolved(() => expect(received).to.exist);
				expect([...received!]).to.deep.equal([...payload]);
			} finally {
				await session.stop();
		}
	});

	it("supports economical unicast with ACKs across branches", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(5);

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
			await senderChannel.unicastToAck(target.publicKeyHash, payload, { timeoutMs: 10_000 });

			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([...payload]);
		} finally {
			await session.stop();
		}
	});

	it("bounds route cache size and evicts old entries", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(5);

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
					const route = await relay.resolveRouteToken(topic, rootId, leaf.publicKeyHash, {
						timeoutMs: 4_000,
					});
					expect(route).to.exist;
				});
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

	it("clamps requested route cache size to a hard safety cap", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(1);

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(8);

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(1);

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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

			const route = await root.resolveRouteToken(topic, rootId, leaf.publicKeyHash, {
				timeoutMs: 4_000,
			});
			expect(route).to.exist;
			expect(route?.[0]).to.equal(rootId);
			expect(route?.[1]).to.equal(relay.publicKeyHash);
			expect(route?.[route.length - 1]).to.equal(leaf.publicKeyHash);
		} finally {
			await session.stop();
		}
	});

	it("resolves route tokens after cache expiry via subtree fallback search", async () => {
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(6);

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
				const route = await senderChannel.resolveRouteToken(target.publicKeyHash, {
					timeoutMs: 4_000,
				});
				expect(route).to.exist;
			});

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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
		const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(4);

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
			const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(2);

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
					expect(leaf.getChannelMetrics(topic, rootId).reparentStale).to.be.greaterThan(0),
				);
			} finally {
				await session.stop();
			}
		});

		it("keeps rejoining after the initial join timeout has elapsed", async function () {
			this.timeout(30_000);
			const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(2);

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
							expect(leaf.getChannelMetrics(topic, rootId).reparentStale).to.be.greaterThan(0),
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
			const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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

					expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(relay.publicKeyHash);

					// Break the relay<->root connection but keep relay alive.
					const rootConnMgr = (root as any)?.components?.connectionManager;
					const relayConnMgr = (relay as any)?.components?.connectionManager;
					expect(rootConnMgr).to.exist;
					expect(relayConnMgr).to.exist;
					const relayAsSeenByRoot = (root as any)?.peers?.get?.(relay.publicKeyHash);
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
							expect(leaf.getChannelMetrics(topic, rootId).reparentKicked).to.be.greaterThan(0),
					{ timeout: 20_000, delayInterval: 50 },
				);
				await waitForResolved(
					() => expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
					{ timeout: 20_000, delayInterval: 50 },
				);
			} finally {
				await session.stop();
			}
		});

		it("rate limits proxy publish ingress (abuse resistance)", async () => {
			const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(2);

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
			const session: TestSession<{ fanout: FanoutTree }> = await createFanoutTestSession(3);

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

				await leafAChannel.unicastTo(leafB.publicKeyHash, new Uint8Array([1, 2, 3]), {
					timeoutMs: 5_000,
				});

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
