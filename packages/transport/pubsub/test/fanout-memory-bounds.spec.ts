import { InMemorySession } from "@peerbit/libp2p-test-utils/inmemory-libp2p.js";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree } from "../src/fanout-tree.js";

// Mirror the bounds of the same names in fanout-tree.ts.
const DETACHED_METRICS_MAX_KEYS = 4_096;
const PROVIDER_WATCH_MAX_NAMESPACES = 4_096;
const PROVIDER_NAMESPACE_NAME_MAX_ENTRIES = 16_384;
const INGRESS_BUCKETS_MIN_ENTRIES = 64;
const INGRESS_BUCKETS_PER_CHILD = 4;

type Buckets = Map<string, { tokens: number; lastRefillAt: number }>;

const internals = (fanout: FanoutTree) =>
	fanout as unknown as {
		channelsBySuffixKey: Map<
			string,
			{ proxyPublishTokensByPeer: Buckets; unicastTokensByPeer: Buckets }
		>;
		takeIngressBudget: (
			ch: unknown,
			kind: "proxy-publish" | "unicast",
			fromHash: string,
			costBytes: number,
		) => boolean;
		metricsBySuffixKey: Map<string, unknown>;
		providerWatchersBySuffixKey: Map<string, Map<string, unknown>>;
		providerNamespaceBySuffixKey: Map<string, string>;
		getMetricsForSuffixKey: (suffixKey: string) => Record<string, number>;
		getProviderNamespaceId: (namespace: string) => { suffixKey: string };
		touchProviderWatchers: (suffixKey: string) => Map<string, unknown>;
		components: { events: EventTarget };
	};

describe("fanout memory bounds", () => {
	it("does not keep per-namespace metrics on a bootstrap tracker for announced providers", async function () {
		this.timeout(60_000);

		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			2,
			{
				basePort: 47_300,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			const tracker = session.peers[0]!.services.fanout;
			const provider = session.peers[1]!.services.fanout;
			provider.setBootstraps(session.peers[0]!.getMultiaddrs());

			// More unique provider namespaces than the detached-metrics bound, like a
			// client that puts one block per namespace (`cid:<cid>`).
			const count = DETACHED_METRICS_MAX_KEYS + 1_000;
			const namespaces = Array.from(
				{ length: count },
				(_, i) => `cid:bound-${i}`,
			);
			await provider.announceProviders(namespaces, {
				ttlMs: 10_000,
				bootstrapMaxPeers: 1,
			});

			const received = () =>
				tracker.getProviderControlMetrics().controlReceives;
			const deadline = Date.now() + 30_000;
			while (Date.now() < deadline && received() < count) {
				await delay(100);
			}

			expect(received()).to.be.at.least(count);
			// Provider frames must not create per-namespace entries at all (the LRU
			// cap alone would still allow DETACHED_METRICS_MAX_KEYS of them).
			expect(internals(tracker).metricsBySuffixKey.size).to.be.below(100);
		} finally {
			await session.stop();
		}
	});

	it("bounds detached per-key metrics without evicting open channels", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			1,
			{
				basePort: 47_400,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			const fanout = session.peers[0]!.services.fanout;
			const root = fanout.publicKeyHash;
			const topic = "bounds-open-channel";
			fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 4,
			});
			const metrics = fanout.getChannelMetrics(topic, root);
			metrics.controlSends += 7;

			for (let i = 0; i < DETACHED_METRICS_MAX_KEYS + 1_000; i++) {
				internals(fanout).getMetricsForSuffixKey(`detached-${i}`);
			}

			expect(internals(fanout).metricsBySuffixKey.size).to.equal(
				DETACHED_METRICS_MAX_KEYS,
			);
			// The open channel's counters survive the churn.
			expect(fanout.getChannelMetrics(topic, root)).to.equal(metrics);
			expect(fanout.getChannelMetrics(topic, root).controlSends).to.equal(7);

			// After close they stay readable, bounded by the same LRU.
			await fanout.closeChannel(topic, root);
			expect(fanout.getChannelMetrics(topic, root).controlSends).to.be.at.least(
				7,
			);
			expect(internals(fanout).metricsBySuffixKey.size).to.equal(
				DETACHED_METRICS_MAX_KEYS,
			);
		} finally {
			await session.stop();
		}
	});

	it("drops provider watches of a disconnected peer and bounds watched namespaces", async function () {
		this.timeout(20_000);

		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			2,
			{
				basePort: 47_500,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			const tracker = session.peers[0]!.services.fanout;
			const consumer = session.peers[1]!.services.fanout;
			consumer.setBootstraps(session.peers[0]!.getMultiaddrs());

			const handle = consumer.watchProviders("bounds-watch", {
				want: 4,
				ttlMs: 60_000,
				renewIntervalMs: 30_000,
				bootstrapMaxPeers: 1,
				onProviders: () => {},
			});

			const watched = () => internals(tracker).providerWatchersBySuffixKey.size;
			try {
				const deadline = Date.now() + 10_000;
				while (Date.now() < deadline && watched() === 0) {
					await delay(50);
				}
				expect(watched()).to.equal(1);

				// The in-memory network only notifies topologies, so emit the libp2p
				// `peer:disconnect` event the tracker listens to, before the consumer
				// can unsubscribe.
				internals(tracker).components.events.dispatchEvent(
					new CustomEvent("peer:disconnect", {
						detail: session.peers[1]!.peerId,
					}),
				);
				expect(watched()).to.equal(0);
			} finally {
				handle.close();
			}

			for (let i = 0; i < PROVIDER_WATCH_MAX_NAMESPACES + 100; i++) {
				internals(tracker).touchProviderWatchers(`watch-${i}`);
			}
			expect(watched()).to.equal(PROVIDER_WATCH_MAX_NAMESPACES);
		} finally {
			await session.stop();
		}
	});

	it("bounds ingress token buckets on a long-lived root as children churn", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			1,
			{
				basePort: 47_700,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			const fanout = session.peers[0]!.services.fanout;
			const root = fanout.publicKeyHash;
			const maxChildren = 4;
			const id = fanout.openChannel("bounds-ingress", root, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren,
			});
			const ch = internals(fanout).channelsBySuffixKey.get(id.suffixKey)!;
			const cap = Math.max(
				INGRESS_BUCKETS_MIN_ENTRIES,
				maxChildren * INGRESS_BUCKETS_PER_CHILD,
			);

			// Many distinct (ephemeral) children each send once over time.
			for (let i = 0; i < cap + 500; i++) {
				internals(fanout).takeIngressBudget(
					ch,
					"proxy-publish",
					`child-${i}`,
					1,
				);
				internals(fanout).takeIngressBudget(ch, "unicast", `child-${i}`, 1);
			}

			expect(ch.proxyPublishTokensByPeer.size).to.equal(cap);
			expect(ch.unicastTokensByPeer.size).to.equal(cap);
			expect(ch.proxyPublishTokensByPeer.has(`child-${cap + 499}`)).to.equal(
				true,
			);
		} finally {
			await session.stop();
		}
	});

	it("bounds provider namespace names kept for announced namespaces", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			1,
			{
				basePort: 47_600,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);

		try {
			const fanout = session.peers[0]!.services.fanout;
			const total = PROVIDER_NAMESPACE_NAME_MAX_ENTRIES + 100;
			for (let i = 0; i < total; i++) {
				internals(fanout).getProviderNamespaceId(`cid:name-${i}`);
			}
			const names = internals(fanout).providerNamespaceBySuffixKey;
			expect(names.size).to.equal(PROVIDER_NAMESPACE_NAME_MAX_ENTRIES);
			// The oldest names were evicted and the most recent ones kept.
			const kept = new Set(names.values());
			expect(kept.has("cid:name-0")).to.equal(false);
			expect(kept.has(`cid:name-${total - 1}`)).to.equal(true);
		} finally {
			await session.stop();
		}
	});
});
