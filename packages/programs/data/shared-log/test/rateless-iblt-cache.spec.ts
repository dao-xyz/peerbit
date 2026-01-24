import { Cache } from "@peerbit/cache";
import { expect } from "chai";
import sinon from "sinon";
import {
	RatelessIBLTSynchronizer,
	StartSync,
} from "../src/sync/rateless-iblt.js";

describe("rateless-iblt-syncronizer cache", () => {
	it("reuses cached local range encoder across StartSync", async () => {
		const iterate = sinon.stub().returns({
			all: async () => [
				{
					value: {
						hash: "h0",
						hashNumber: 1n,
					},
				},
			],
		});

		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: { iterate } as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		});

		const context = { from: "p" } as any;
		const createStartSync = () =>
			new StartSync({ from: 0n, to: 10n, symbols: [] });

		expect(await sync.onMessage(createStartSync(), context)).to.equal(true);
		expect(await sync.onMessage(createStartSync(), context)).to.equal(true);

		expect(iterate.callCount).to.equal(1);

		await sync.close();
	});

	it("invalidates cached range encoder on entry removal", async () => {
		const iterate = sinon.stub().returns({
			all: async () => [
				{
					value: {
						hash: "h0",
						hashNumber: 1n,
					},
				},
			],
		});

		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: { iterate } as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		});

		const context = { from: "p" } as any;
		const createStartSync = () =>
			new StartSync({ from: 0n, to: 10n, symbols: [] });

		await sync.onMessage(createStartSync(), context);
		sync.onEntryRemoved("h0");
		await sync.onMessage(createStartSync(), context);

		expect(iterate.callCount).to.equal(2);

		await sync.close();
	});
});
