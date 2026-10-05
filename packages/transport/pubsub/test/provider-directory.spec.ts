import { InMemorySession } from "@peerbit/libp2p-test-utils/inmemory-libp2p.js";
import { AnyWhere } from "@peerbit/stream-interface";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { FanoutTree } from "../src/fanout-tree.js";

describe("fanout provider discovery", () => {
	it("does not materialize a provider batch without bootstrap trackers", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(1, {
			basePort: 46_900,
			services: {
				fanout: (c) => new FanoutTree(c, { connectionManager: false }),
			},
		});

		try {
			let consumed = false;
			const namespaces = function* () {
				consumed = true;
				yield "provider-unused";
			};
			await session.peers[0]!.services.fanout.announceProviders(namespaces());
			expect(consumed).to.equal(false);
		} finally {
			await session.stop();
		}
	});

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

	it("announces a wire-compatible batch of provider namespaces", async function () {
		this.timeout(20_000);

		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(3, {
			basePort: 47_200,
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

			const namespaces = ["provider-batch-a", "provider-batch-b"];
			await provider.announceProviders(namespaces, {
				ttlMs: 10_000,
				bootstrapMaxPeers: 1,
			});

			const expected = provider.publicKeyHash;
			const discovered = new Set<string>();
			const deadline = Date.now() + 10_000;
			while (Date.now() < deadline && discovered.size < namespaces.length) {
				for (const namespace of namespaces) {
					const providers = await consumer.queryProviders(namespace, {
						want: 4,
						timeoutMs: 2_000,
						queryTimeoutMs: 500,
						bootstrapMaxPeers: 1,
					});
					if (providers.includes(expected)) discovered.add(namespace);
				}
				if (discovered.size < namespaces.length) await delay(100);
			}

			expect([...discovered]).to.have.members(namespaces);
		} finally {
			await session.stop();
		}
	});

	describe("provider cache expiry", () => {
		let session: InMemorySession<{ fanout: FanoutTree }>;
		let consumer: FanoutTree;
		let internals: any;
		let namespaceId: any;
		let now: number;
		let replies: string[];
		let send: sinon.SinonStub;
		const sandbox = sinon.createSandbox();
		const namespace = "provider-cache-expiry";

		beforeEach(async () => {
			session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(2, {
				basePort: 47_300,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			});
			consumer = session.peers[0]!.services.fanout;
			const tracker = session.peers[1]!.services.fanout;
			internals = consumer as any;
			namespaceId = internals.getProviderNamespaceId(namespace);
			now = Date.now();
			replies = [];
			// Advance cache age without sleeps or changing transport timers.
			sandbox.stub(Date, "now").callsFake(() => now);
			sandbox
				.stub(internals, "ensureBootstrapPeers")
				.resolves([tracker.publicKeyHash]);
			// Keep the real reply codec and receive handler; only replace transport delivery.
			send = sandbox
				.stub(internals, "_sendControl")
				.callsFake(async (...parameters: unknown[]) => {
					const bytes = parameters[1] as Uint8Array;
					const query = internals.codec.decodeProviderQuery(bytes);
					const message = await tracker.createMessage(
						internals.codec.encodeProviderReply(
							namespaceId.key,
							query.reqId,
							replies.map((hash) => ({
								hash,
								addrs: [] as Uint8Array[],
								expiresAt: 0, // Expiry is not carried in provider replies.
							})),
						),
						{ mode: new AnyWhere() },
					);
					await consumer.onDataMessage(
						tracker.publicKey,
						{} as any,
						message,
						0,
					);
				});
		});

		afterEach(async () => {
			sandbox.restore();
			await session?.stop();
		});

		const expiresAt = (hash: string): number | undefined =>
			internals.providerBySuffixKey.get(namespaceId.suffixKey)?.get(hash)
				?.expiresAt;
		const remember = (hash: string, expiry: number) =>
			internals.rememberProviderCandidates(
				namespaceId,
				[{ hash, addrs: [] }],
				expiry,
			);
		const enableTracker = () =>
			consumer.setBootstraps(session.peers[1]!.getMultiaddrs());

		for (const { label, want, tracker } of [
			{ label: "warm-cache reads", want: 1, tracker: false },
			{
				label: "partial-cache reads without trackers",
				want: 2,
				tracker: false,
			},
			{ label: "empty tracker replies", want: 2, tracker: true },
		]) {
			it(`does not renew cached candidates on ${label}`, async () => {
				const originalExpiry = now + 1_000;
				remember("old-provider", originalExpiry);
				if (tracker) enableTracker();
				for (const remaining of [900, 500, 1]) {
					now = originalExpiry - remaining;
					expect(
						await consumer.queryProviders(namespace, { want }),
					).to.deep.equal(["old-provider"]);
					expect(expiresAt("old-provider")).to.equal(originalExpiry);
				}
				now = originalExpiry;
				expect(
					await consumer.queryProviders(namespace, { want }),
				).to.deep.equal([]);
				expect(expiresAt("old-provider")).to.equal(undefined);
				expect(send.callCount).to.equal(tracker ? 4 : 0);
			});
		}

		it("does not extend old candidates when a tracker supplies different fresh candidates", async () => {
			const originalExpiry = now + 1_000;
			remember("old-provider", originalExpiry);
			enableTracker();
			replies = ["fresh-provider"];
			now += 100;
			expect(
				await consumer.queryProviders(namespace, {
					want: 3,
					cacheTtlMs: 5_000,
				}),
			).to.have.members(["old-provider", "fresh-provider"]);
			expect(expiresAt("old-provider")).to.equal(originalExpiry);
			expect(expiresAt("fresh-provider")).to.equal(now + 5_000);
			consumer.setBootstraps([]);
			now = originalExpiry;
			expect(
				await consumer.queryProviders(namespace, { want: 3 }),
			).to.deep.equal(["fresh-provider"]);
			expect(expiresAt("old-provider")).to.equal(undefined);
			expect(send.callCount).to.equal(1);
		});

		it("renews a candidate from a fresh reply using the requested cache TTL", async () => {
			enableTracker();
			replies = ["fresh-provider"];
			const query = () =>
				consumer.queryProviders(namespace, { want: 2, cacheTtlMs: 2_500 });
			expect(await query()).to.deep.equal(["fresh-provider"]);
			const originalExpiry = now + 2_500;
			expect(expiresAt("fresh-provider")).to.equal(originalExpiry);
			now += 100;
			expect(await query()).to.deep.equal(["fresh-provider"]);
			const renewedExpiry = now + 2_500;
			expect(expiresAt("fresh-provider")).to.equal(renewedExpiry);
			consumer.setBootstraps([]);
			now = originalExpiry;
			expect(
				await consumer.queryProviders(namespace, { want: 1 }),
			).to.deep.equal(["fresh-provider"]);
			now = renewedExpiry;
			expect(
				await consumer.queryProviders(namespace, { want: 1 }),
			).to.deep.equal([]);
			expect(send.callCount).to.equal(2);
		});

		it("keeps the receive handler cache when the query cache TTL is zero", async () => {
			enableTracker();
			replies = ["fresh-provider"];
			expect(
				await consumer.queryProviders(namespace, { want: 2, cacheTtlMs: 0 }),
			).to.deep.equal(["fresh-provider"]);
			expect(expiresAt("fresh-provider")).to.equal(now + 60_000);
			expect(send.callCount).to.equal(1);
		});
	});
});
