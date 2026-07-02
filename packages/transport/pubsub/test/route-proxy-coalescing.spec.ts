import { TestSession } from "@peerbit/libp2p-test-utils";
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

describe("fanout-tree (route proxy coalescing)", () => {
	it("coalesces concurrent same-target resolutions into a single subtree search", async () => {
		const session: TestSession<FanoutServices> =
			await createFanoutTestSession(3);

		try {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;
			const leaf = session.peers[2].services.fanout;

			const topic = "route-proxy-coalescing";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: false,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 8,
					repair: false,
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
					repair: false,
				},
				{ timeoutMs: 10_000 },
			);

			// Cold cache at the root: N concurrent lookups for the same target
			// must launch exactly one subtree search and all resolve from it.
			const attempts = 5;
			const routes = await Promise.all(
				Array.from({ length: attempts }, () =>
					root.resolveRouteToken(topic, rootId, leaf.publicKeyHash, {
						timeoutMs: 4_000,
					}),
				),
			);

			for (const route of routes) {
				expect(route).to.exist;
				expect(route?.[0]).to.equal(rootId);
				expect(route?.[route.length - 1]).to.equal(leaf.publicKeyHash);
			}

			const metrics = root.getChannelMetrics(topic, rootId);
			expect(metrics.routeProxyQueries).to.equal(1);
			expect(metrics.routeProxyCoalesced).to.equal(attempts - 1);
		} finally {
			await session.stop();
		}
	});

	it("settles coalesced waiters instead of hanging when the channel closes", async () => {
		const session: TestSession<FanoutServices> =
			await createFanoutTestSession(2);

		try {
			await session.connect([[session.peers[0], session.peers[1]]]);

			const root = session.peers[0].services.fanout;
			const relay = session.peers[1].services.fanout;

			const topic = "route-proxy-close-settles";
			const rootId = root.publicKeyHash;

			const rootChannel = FanoutChannel.fromSelf(root, topic);
			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: false,
			});

			await relay.joinChannel(
				topic,
				rootId,
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 1_000_000,
					maxChildren: 8,
					repair: false,
				},
				{ timeoutMs: 10_000 },
			);

			// Lookups for a target that does not exist keep a search in flight
			// (the relay proxies onward and nothing answers before the timeout).
			const pending = Promise.all(
				Array.from({ length: 3 }, () =>
					root.resolveRouteToken(topic, rootId, "does-not-exist", {
						timeoutMs: 8_000,
					}),
				),
			);
			// Give the queries a beat to register as in-flight state.
			await new Promise((r) => setTimeout(r, 100));

			rootChannel.close();

			// All callers must settle promptly (undefined), well before their
			// 8s timeouts — closing used to strand localResolve callers.
			const routes = await Promise.race([
				pending,
				new Promise<undefined>((_, reject) =>
					setTimeout(() => reject(new Error("waiters left hanging")), 4_000),
				),
			]);
			expect(routes).to.have.length(3);
			for (const route of routes!) {
				expect(route).to.equal(undefined);
			}
		} finally {
			await session.stop();
		}
	});
});
