import { expect } from "chai";
import sinon from "sinon";
import { Peerbit } from "../src/index.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe("blocks provider discovery", () => {
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

