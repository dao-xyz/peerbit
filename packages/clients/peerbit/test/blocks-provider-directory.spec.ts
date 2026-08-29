import { expect } from "chai";
import sinon from "sinon";
import { Peerbit } from "../src/index.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe("blocks provider discovery", () => {
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
