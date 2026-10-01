import { InMemorySession } from "@peerbit/libp2p-test-utils/inmemory-libp2p.js";
import { AnyWhere, NotStartedError } from "@peerbit/stream-interface";
import { AbortError, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	MSG_PROVIDER_ANNOUNCE,
	MSG_PROVIDER_SUBSCRIBE,
	MSG_PROVIDER_UNSUBSCRIBE,
} from "../src/fanout-tree-codec.js";
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

	it("cancels a watch unsubscribe held in message creation when its service stops", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			2,
			{
				basePort: 47_300,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);
		const sandbox = sinon.createSandbox();
		const creationEntered = pDefer<void>();
		const releaseCreation = pDefer<void>();
		let handle: ReturnType<FanoutTree["watchProviders"]> | undefined;
		let stopping: Promise<void> | undefined;
		let unsubscribe: Promise<unknown> | undefined;
		try {
			const tracker = session.peers[0]!.services.fanout;
			const consumer = session.peers[1]!.services.fanout;
			consumer.setBootstraps(session.peers[0]!.getMultiaddrs());
			const originalCreate = consumer.createMessage.bind(consumer);
			sandbox
				.stub(consumer, "createMessage")
				.callsFake(async (data, options) => {
					const message = await originalCreate(data, options);
					if (
						data instanceof Uint8Array &&
						data[0] === MSG_PROVIDER_UNSUBSCRIBE
					) {
						creationEntered.resolve();
						await releaseCreation.promise;
					}
					return message;
				});
			const sends = sandbox.spy(consumer as any, "_sendControl");
			handle = consumer.watchProviders("provider-watch-stop-race", {
				onProviders: () => {},
			});
			await waitForResolved(() => {
				expect(
					sends
						.getCalls()
						.some(
							(call) =>
								call.args[0] === tracker.publicKeyHash &&
								call.args[1][0] === MSG_PROVIDER_SUBSCRIBE,
						),
				).to.equal(true);
			});
			await Promise.all(sends.getCalls().map((call) => call.returnValue));
			const publish = sandbox.spy(consumer, "publishMessageMaybe");

			handle.close();
			await creationEntered.promise;
			unsubscribe = sends
				.getCalls()
				.find(
					(call) => call.args[1][0] === MSG_PROVIDER_UNSUBSCRIBE,
				)!.returnValue;
			// Observe the raw send without replacing the watch's own error handler.
			const sendSettled = Promise.allSettled([unsubscribe]);
			const closeSignal = (consumer as any).closeController
				.signal as AbortSignal;
			stopping = consumer.stop();
			await waitForResolved(() => expect(closeSignal.aborted).to.equal(true));
			releaseCreation.resolve();
			await stopping;
			await sendSettled;
			// Give the detached watch continuation a turn: an unhandled rejection
			// must fail this test, even if the public handle has already closed.
			await delay(0);
			expect(publish.called).to.equal(false);
		} finally {
			releaseCreation.resolve();
			handle?.close();
			try {
				await unsubscribe?.catch(() => {});
				await stopping;
			} finally {
				sandbox.restore();
				await session.stop();
			}
		}
	});

	it("does not rearm a closed watch when pending bootstrap discovery completes", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			2,
			{
				basePort: 47_350,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);
		const sandbox = sinon.createSandbox();
		const discovered = pDefer<void>();
		const releaseDiscovery = pDefer<void>();
		let discoveredPeers: string[] = [];
		let handle: ReturnType<FanoutTree["watchProviders"]> | undefined;
		let watch: { loop: Promise<void>; trackerPeers: string[] } | undefined;
		try {
			const tracker = session.peers[0]!.services.fanout;
			const consumer = session.peers[1]!.services.fanout;
			consumer.setBootstraps(session.peers[0]!.getMultiaddrs());
			const originalDiscovery = (consumer as any).ensureBootstrapPeers.bind(
				consumer,
			);
			sandbox
				.stub(consumer as any, "ensureBootstrapPeers")
				.callsFake(async (...args) => {
					discoveredPeers = await originalDiscovery(...args);
					discovered.resolve();
					await releaseDiscovery.promise;
					return discoveredPeers;
				});
			const sends = sandbox.spy(consumer as any, "_sendControl");
			handle = consumer.watchProviders("provider-watch-pending-bootstrap", {
				onProviders: () => {},
			});
			watch = [...(consumer as any).providerWatchesBySuffixKey.values()][0]
				.values()
				.next().value;
			await discovered.promise;
			expect(discoveredPeers).to.include(tracker.publicKeyHash);
			handle.close();
			releaseDiscovery.resolve();
			await watch!.loop;
			await Promise.allSettled(
				sends.getCalls().map((call) => call.returnValue),
			);
			expect(sends.called).to.equal(false);
			expect(watch!.trackerPeers).to.deep.equal([]);
		} finally {
			releaseDiscovery.resolve();
			handle?.close();
			try {
				await watch?.loop;
			} finally {
				sandbox.restore();
				await session.stop();
			}
		}
	});

	it("rejects an awaited provider announcement from before a service restart", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			2,
			{
				basePort: 47_375,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);
		const sandbox = sinon.createSandbox();
		const creationEntered = pDefer<void>();
		const releaseCreation = pDefer<void>();
		let announcement: Promise<unknown> | undefined;
		try {
			const provider = session.peers[1]!.services.fanout;
			provider.setBootstraps(session.peers[0]!.getMultiaddrs());
			await provider.announceProvider("provider-restart-setup");
			const originalCreate = provider.createMessage.bind(provider);
			let heldMessage: unknown;
			sandbox
				.stub(provider, "createMessage")
				.callsFake(async (data, options) => {
					const message = await originalCreate(data, options);
					if (data instanceof Uint8Array && data[0] === MSG_PROVIDER_ANNOUNCE) {
						heldMessage = message;
						creationEntered.resolve();
						await releaseCreation.promise;
					}
					return message;
				});
			const publish = sandbox.spy(provider, "publishMessageMaybe");
			announcement = provider.announceProvider("provider-before-restart").then(
				(): undefined => undefined,
				(error: unknown) => error,
			);
			await creationEntered.promise;
			const oldSignal = (provider as any).closeController.signal as AbortSignal;
			await provider.stop();
			await provider.start();
			expect(provider.started).to.equal(true);
			expect(oldSignal.aborted).to.equal(true);
			expect((provider as any).closeController.signal).not.to.equal(oldSignal);
			expect((provider as any).closeController.signal.aborted).to.equal(false);
			releaseCreation.resolve();
			expect(await announcement).to.be.instanceOf(AbortError);
			expect(
				publish.getCalls().some((call) => call.args[1] === heldMessage),
			).to.equal(false);
		} finally {
			releaseCreation.resolve();
			try {
				await announcement;
			} finally {
				sandbox.restore();
				await session.stop();
			}
		}
	});

	it("keeps a replacement watch when an old renewal finishes after service restart", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			3,
			{
				basePort: 47_390,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);
		const sandbox = sinon.createSandbox();
		const renewalEntered = pDefer<void>();
		const releaseRenewal = pDefer<void>();
		let handle: ReturnType<FanoutTree["watchProviders"]> | undefined;
		let replacement: ReturnType<FanoutTree["watchProviders"]> | undefined;
		let oldWatch: { loop: Promise<void>; trackerPeers: string[] } | undefined;
		let sends: sinon.SinonSpy | undefined;
		try {
			const tracker = session.peers[0]!.services.fanout;
			const consumer = session.peers[1]!.services.fanout;
			const provider = session.peers[2]!.services.fanout;
			const namespace = "provider-watch-late-renewal";
			const internals = consumer as any;
			const id = internals.getProviderNamespaceId(namespace);
			consumer.setBootstraps(session.peers[0]!.getMultiaddrs());
			provider.setBootstraps(session.peers[0]!.getMultiaddrs());
			const originalDiscovery = internals.ensureBootstrapPeers.bind(consumer);
			let discoveries = 0;
			sandbox
				.stub(internals, "ensureBootstrapPeers")
				.callsFake(async (...args) => {
					const peers = await originalDiscovery(...args);
					if (++discoveries === 2) {
						renewalEntered.resolve();
						await releaseRenewal.promise;
					}
					return peers;
				});
			sends = sandbox.spy(internals, "_sendControl");
			handle = consumer.watchProviders(namespace, {
				renewIntervalMs: 250,
				onProviders: () => {},
			});
			oldWatch = internals.providerWatchesBySuffixKey
				.get(id.suffixKey)
				.values()
				.next().value;
			const registration = () =>
				(tracker as any).providerWatchersBySuffixKey
					.get(id.suffixKey)
					?.get(consumer.publicKeyHash);
			await waitForResolved(() =>
				expect(registration()).not.to.equal(undefined),
			);
			const oldRegistration = registration();
			await renewalEntered.promise;
			expect(oldWatch!.trackerPeers).to.include(tracker.publicKeyHash);
			await consumer.stop();
			await consumer.start();
			const received: string[] = [];
			replacement = consumer.watchProviders(namespace, {
				onProviders: (providers) =>
					received.push(...providers.map((provider) => provider.hash)),
			});
			const replacementWatches = internals.providerWatchesBySuffixKey.get(
				id.suffixKey,
			);
			expect(replacementWatches.has(oldWatch)).to.equal(true);
			expect(replacementWatches.size).to.equal(2);
			await waitForResolved(() => {
				expect(registration()).not.to.equal(undefined);
				expect(registration()).not.to.equal(oldRegistration);
			});
			const creations = sandbox.spy(consumer, "createMessage");
			releaseRenewal.resolve();
			await oldWatch!.loop;
			await Promise.allSettled(
				sends.getCalls().map((call) => call.returnValue),
			);
			expect(internals.providerWatchesBySuffixKey.get(id.suffixKey)).to.equal(
				replacementWatches,
			);
			expect(replacementWatches.size).to.equal(1);
			expect(
				creations
					.getCalls()
					.some(
						(call) =>
							call.args[0] instanceof Uint8Array &&
							call.args[0][0] === MSG_PROVIDER_UNSUBSCRIBE,
					),
			).to.equal(false);
			await provider.announceProvider(namespace);
			await waitForResolved(() =>
				expect(received).to.include(provider.publicKeyHash),
			);
		} finally {
			releaseRenewal.resolve();
			handle?.close();
			replacement?.close();
			try {
				await oldWatch?.loop;
				await Promise.allSettled(
					sends?.getCalls().map((call) => call.returnValue) ?? [],
				);
			} finally {
				sandbox.restore();
				await session.stop();
			}
		}
	});

	it("preserves exact active-service errors from an awaited provider announcement", async () => {
		const session = await InMemorySession.disconnected<{ fanout: FanoutTree }>(
			2,
			{
				basePort: 47_400,
				services: {
					fanout: (c) => new FanoutTree(c, { connectionManager: false }),
				},
			},
		);
		const sandbox = sinon.createSandbox();
		try {
			const provider = session.peers[1]!.services.fanout;
			provider.setBootstraps(session.peers[0]!.getMultiaddrs());
			await provider.announceProvider("provider-error-setup");
			const publish = sandbox.stub(provider, "publishMessage");
			for (const expected of [
				new NotStartedError(),
				new Error("publish failed"),
			]) {
				publish.rejects(expected);
				expect(provider.started).to.equal(true);
				expect(provider.stopping).to.equal(false);
				let failure: unknown;
				try {
					await provider.announceProvider("provider-awaited-error");
				} catch (error) {
					failure = error;
				}
				expect(failure).to.equal(expected);
			}
			expect(publish.callCount).to.equal(2);
		} finally {
			sandbox.restore();
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
