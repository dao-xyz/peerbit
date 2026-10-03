import { multiaddr } from "@multiformats/multiaddr";
import { expect } from "chai";
import sinon from "sinon";
import { Peerbit } from "../src/index.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe("blocks provider discovery", () => {
	(isNode ? it : it.skip)(
		"finishes a local block put when a provider bootstrap dial times out",
		async function () {
			this.timeout(12_000);
			const peer = await Peerbit.create();
			const fanout = peer.services.fanout;
			const pendingDials = new Set<(error: Error) => void>();
			const dialSignals: Array<AbortSignal | undefined> = [];
			const dial = sinon
				.stub((fanout as any).components.connectionManager, "openConnection")
				.callsFake((_address, options: { signal?: AbortSignal } = {}) => {
					dialSignals.push(options.signal);
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
			let deadline: ReturnType<typeof setTimeout> | undefined;
			let putting: Promise<string> | undefined;
			try {
				fanout.addBootstraps([multiaddr("/ip4/127.0.0.1/tcp/1")]);
				const bytes = new Uint8Array([7, 11, 13]);
				putting = peer.services.blocks.put(bytes);
				// The production announcement uses a 2-second dial budget. Allow
				// scheduling headroom, but do not release the mocked dial to pass.
				const cid = await Promise.race([
					putting,
					new Promise<never>((_resolve, reject) => {
						deadline = setTimeout(
							() =>
								reject(new Error("local put is stuck on provider discovery")),
							4_000,
						);
					}),
				]);
				expect(dialSignals.length).greaterThan(0);
				expect(dialSignals.every((signal) => signal?.aborted)).equal(true);
				expect(pendingDials.size).equal(0);
				expect(await peer.services.blocks.get(cid)).deep.equal(bytes);
			} finally {
				clearTimeout(deadline);
				for (const reject of pendingDials) reject(new Error("test cleanup"));
				await putting?.catch((): void => undefined);
				dial.restore();
				await peer.stop();
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
