import { delay } from "@peerbit/time";
import { expect } from "chai";
import { InMemorySession } from "@peerbit/libp2p-test-utils/inmemory-libp2p.js";
import { FanoutTree } from "../src/fanout-tree.js";

describe("fanout provider discovery", () => {
	it("discovers providers via bootstrap trackers", async function () {
		this.timeout(20_000);

		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(3, {
			basePort: 47_000,
			services: {
				fanout: (c) => new FanoutTree(c, { connectionManager: false }),
			},
		});

		try {
			const trackerAddr = session.peers[0]!.getMultiaddrs();
			const provider = session.peers[1]!.services.fanout;
			const consumer = session.peers[2]!.services.fanout;

			provider.setBootstraps(trackerAddr);
			consumer.setBootstraps(trackerAddr);

			const ns = "provider-test";
			const providing = provider.provide(ns, {
				ttlMs: 10_000,
				announceIntervalMs: 200,
				bootstrapMaxPeers: 1,
			});

			const expected = provider.publicKeyHash;

			let got: string[] = [];
			const deadline = Date.now() + 10_000;
			while (Date.now() < deadline) {
				got = await consumer.queryProviders(ns, {
					want: 4,
					timeoutMs: 2_000,
					queryTimeoutMs: 500,
					bootstrapMaxPeers: 1,
				});
				if (got.includes(expected)) break;
				await delay(100);
			}

			expect(got).to.include(expected);
			providing.close();
		} finally {
			await session.stop();
		}
	});

	it("pushes provider updates to active watches via bootstrap trackers", async function () {
		this.timeout(20_000);

		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(3, {
			basePort: 47_100,
			services: {
				fanout: (c) => new FanoutTree(c, { connectionManager: false }),
			},
		});

		try {
			const trackerAddr = session.peers[0]!.getMultiaddrs();
			const provider = session.peers[1]!.services.fanout;
			const consumer = session.peers[2]!.services.fanout;

			provider.setBootstraps(trackerAddr);
			consumer.setBootstraps(trackerAddr);

			const ns = "provider-watch-test";
			const expected = provider.publicKeyHash;
			let seen: string[] = [];

			const handle = consumer.watchProviders(ns, {
				want: 4,
				ttlMs: 4_000,
				renewIntervalMs: 1_000,
				bootstrapMaxPeers: 1,
				onProviders: (providers) => {
					seen = providers.map((provider) => provider.hash);
				},
			});

			try {
				await delay(250);
				await provider.announceProvider(ns, {
					ttlMs: 10_000,
					bootstrapMaxPeers: 1,
				});

				const deadline = Date.now() + 10_000;
				while (Date.now() < deadline) {
					if (seen.includes(expected)) break;
					await delay(100);
				}

				expect(seen).to.include(expected);
			} finally {
				handle.close();
			}
		} finally {
			await session.stop();
		}
	});
});
