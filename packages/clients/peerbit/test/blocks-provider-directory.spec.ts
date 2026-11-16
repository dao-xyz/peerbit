import { multiaddr } from "@multiformats/multiaddr";
import { expect } from "chai";
import sinon from "sinon";
import { Peerbit } from "../src/index.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe("blocks provider discovery", () => {
	(isNode ? it : it.skip)(
		"finishes a local block put while its provider bootstrap dial never answers",
		async function () {
			this.timeout(30_000);
			const peer = await Peerbit.create();
			const fanout = peer.services.fanout;
			const pendingDials = new Set<(error: Error) => void>();
			const dialSignals: Array<AbortSignal | undefined> = [];
			let markDialStarted!: () => void;
			const dialStarted = new Promise<void>((resolve) => {
				markDialStarted = resolve;
			});
			// The dial never answers; it ends only when its signal aborts.
			const dial = sinon
				.stub((fanout as any).components.connectionManager, "openConnection")
				.callsFake((_address, options: { signal?: AbortSignal } = {}) => {
					dialSignals.push(options.signal);
					markDialStarted();
					return new Promise((_resolve, reject) => {
						const finish = (error: Error) => {
							options.signal?.removeEventListener("abort", onAbort);
							pendingDials.delete(finish);
							reject(error);
						};
						const onAbort = () => finish(options.signal!.reason);
						pendingDials.add(finish);
						if (options.signal?.aborted) onAbort();
						else
							options.signal?.addEventListener("abort", onAbort, {
								once: true,
							});
					});
				});
			const announce = sinon.spy(fanout, "announceProviders");
			let putting: Promise<string> | undefined;
			let stopped = false;
			try {
				fanout.addBootstraps([multiaddr("/ip4/127.0.0.1/tcp/1")]);
				const bytes = new Uint8Array([7, 11, 13]);
				let settled = false;
				putting = peer.services.blocks.put(bytes).then((cid) => {
					settled = true;
					return cid;
				});
				await dialStarted;
				// Everything the put still had to do is microtasks; the dial is
				// pending until its 2-second budget or an abort ends it.
				await new Promise<void>((resolve) => setImmediate(resolve));
				expect(settled, "put resolved while its announcement dials").equal(
					true,
				);
				expect(announce.callCount).equal(1);
				const cid = await putting;
				expect(await peer.services.blocks.get(cid)).deep.equal(bytes);

				// Stopping cancels the announcement instead of waiting for its dial.
				await peer.stop();
				stopped = true;
				expect(dialSignals.length).greaterThan(0);
				expect(dialSignals.every((signal) => signal?.aborted)).equal(true);
				expect(pendingDials.size).equal(0);
			} finally {
				for (const reject of pendingDials) reject(new Error("test cleanup"));
				await putting?.catch((): void => undefined);
				announce.restore();
				dial.restore();
				if (!stopped) await peer.stop();
			}
		},
	);

	(isNode ? it : it.skip)(
		"announces a stored block to the provider directory after its put resolves",
		async function () {
			this.timeout(30_000);

			const tracker = await Peerbit.create();
			const provider = await Peerbit.create();
			const consumer = await Peerbit.create();
			const fanout = provider.services.fanout;
			const announceProviders = fanout.announceProviders;
			let releaseAnnouncement!: () => void;
			const announcementReleased = new Promise<void>((resolve) => {
				releaseAnnouncement = resolve;
			});
			let markAnnouncementStarted!: () => void;
			const announcementStarted = new Promise<void>((resolve) => {
				markAnnouncementStarted = resolve;
			});
			let onProvider: ((event: Event) => void) | undefined;

			try {
				await provider.bootstrap(tracker.getMultiaddrs());
				await consumer.bootstrap(tracker.getMultiaddrs());

				// Hold the provider's announcement until the put has resolved.
				fanout.announceProviders = async function (
					this: typeof fanout,
					...args: Parameters<typeof announceProviders>
				) {
					markAnnouncementStarted();
					await announcementReleased;
					return announceProviders.apply(this, args);
				};
				const providerHash = provider.identity.publicKey.hashcode();
				const recorded = new Promise<void>((resolve) => {
					onProvider = (event: Event) => {
						const { providers } = (
							event as CustomEvent<{ providers: Array<{ hash: string }> }>
						).detail;
						if (providers.some((candidate) => candidate.hash === providerHash))
							resolve();
					};
					tracker.services.fanout.addEventListener(
						"fanout:provider",
						onProvider,
					);
				});

				const data = new Uint8Array([4, 5, 6]);
				let settled = false;
				const putting = provider.services.blocks.put(data).then((cid) => {
					settled = true;
					return cid;
				});
				await announcementStarted;
				await new Promise<void>((resolve) => setImmediate(resolve));
				expect(settled, "put resolved while its announcement is held").equal(
					true,
				);
				const cid = await putting;

				releaseAnnouncement();
				// The tracker records the provider before it emits this event.
				await recorded;
				const bytes = await consumer.services.blocks.get(cid, {
					remote: { timeout: 10_000 },
				});
				expect(bytes && new Uint8Array(bytes)).to.deep.equal(data);
			} finally {
				releaseAnnouncement();
				fanout.announceProviders = announceProviders;
				if (onProvider) {
					tracker.services.fanout.removeEventListener(
						"fanout:provider",
						onProvider,
					);
				}
				await Promise.all([consumer.stop(), provider.stop(), tracker.stop()]);
			}
		},
	);

	(isNode ? it : it.skip)(
		"preserves connected and directory evidence under saturation",
		async function () {
			this.timeout(30_000);

			const provider = await Peerbit.create();
			const consumer = await Peerbit.create();
			let queryStub: sinon.SinonStub | undefined;
			const syntheticConnected = Array.from(
				{ length: 8 },
				(_, index) => `connected-non-provider-${index}`,
			);

			try {
				await consumer.dial(provider.getMultiaddrs()[0]!);
				const directoryProvider = "directory-only-provider";
				queryStub = sinon
					.stub(consumer.services.fanout, "queryProviders")
					.resolves([directoryProvider]);
				const blocks = consumer.services.blocks as any;
				for (const peerHash of syntheticConnected) {
					blocks.peers.set(peerHash, {});
				}
				const remoteBlocks = blocks.remoteBlocks;

				const candidates = await remoteBlocks.options.resolveProviders(
					"test-cid",
				);
				expect(queryStub.calledOnce).to.equal(true);
				expect(candidates[0]).to.equal(
					provider.identity.publicKey.hashcode(),
				);
				expect(candidates).to.include(directoryProvider);
				expect(candidates).to.have.length.lessThanOrEqual(8);
			} finally {
				queryStub?.restore();
				const peers = (consumer.services.blocks as any).peers;
				for (const peerHash of syntheticConnected) peers.delete(peerHash);
				await Promise.all([consumer.stop(), provider.stop()]);
			}
		},
	);

	(isNode ? it : it.skip)(
		"widens a refresh beyond excluded directory candidates",
		async function () {
			this.timeout(30_000);

			const consumer = await Peerbit.create();
			let queryStub: sinon.SinonStub | undefined;
			const staleProviders = Array.from(
				{ length: 8 },
				(_, index) => `stale-provider-${index}`,
			);
			const syntheticConnected = Array.from(
				{ length: 8 },
				(_, index) => `connected-non-provider-${index}`,
			);
			const liveProvider = "live-provider";

			try {
				queryStub = sinon
					.stub(consumer.services.fanout, "queryProviders")
					.callsFake(async (_namespace, options) =>
						[...staleProviders, liveProvider].slice(0, options?.want),
					);
				const blocks = consumer.services.blocks as any;
				for (const peerHash of syntheticConnected) {
					blocks.peers.set(peerHash, {});
				}
				const remoteBlocks = blocks.remoteBlocks;
				const candidates = await remoteBlocks.options.resolveProviders(
					"test-cid",
					{
						refresh: true,
						exclude: staleProviders.slice(0, 2),
					},
				);

				expect(queryStub.calledOnce).to.equal(true);
				expect(queryStub.firstCall.args[1]?.want).to.equal(10);
				expect(candidates).to.include(liveProvider);
				expect(candidates).to.include(syntheticConnected[0]);
				expect(candidates).not.to.include(staleProviders[0]);
				expect(candidates).not.to.include(staleProviders[1]);
				expect(candidates).to.have.length.lessThanOrEqual(8);
			} finally {
				queryStub?.restore();
				const peers = (consumer.services.blocks as any).peers;
				for (const peerHash of syntheticConnected) peers.delete(peerHash);
				await consumer.stop();
			}
		},
	);

	(isNode ? it : it.skip)("fetches via fanout provider directory", async function () {
		this.timeout(30_000);

		const tracker = await Peerbit.create();
		const provider = await Peerbit.create();
		const consumer = await Peerbit.create();

		try {
			await provider.bootstrap(tracker.getMultiaddrs());
			await consumer.bootstrap(tracker.getMultiaddrs());

			const announceSpy = sinon.spy(provider.services.fanout, "announceProvider");
			const querySpy = sinon.spy(consumer.services.fanout, "queryProviders");

			const data = new Uint8Array([1, 2, 3]);
			const cid = await provider.services.blocks.put(data);
			// The put does not wait for its best-effort announcement.
			await provider.services.blocks.waitForStoredNotifications();

			const bytes = await consumer.services.blocks.get(cid, {
				remote: { timeout: 10_000 },
			});

			expect(bytes && new Uint8Array(bytes)).to.deep.equal(data);
			expect(announceSpy.called).to.equal(true);
			expect(querySpy.called).to.equal(true);

			announceSpy.restore();
			querySpy.restore();
		} finally {
			await Promise.all([consumer.stop(), provider.stop(), tracker.stop()]);
		}
	});
});
