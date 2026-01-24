import { Cache } from "@peerbit/cache";
import { expect } from "chai";
import sinon from "sinon";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { RequestMaybeSync, SimpleSyncronizer } from "../src/sync/simple.js";

describe("sync-priority", () => {
	it("orders outgoing hashes by priority (simple)", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				priority: (entry: any) => (entry.hash === "b" ? 10 : 0),
			},
		});

		const entries = new Map<string, any>();
		entries.set("a", { hash: "a" });
		entries.set("b", { hash: "b" });
		entries.set("c", { hash: "c" });

		await sync.onMaybeMissingEntries({
			entries: entries as any,
			targets: ["p"],
		});

		expect(send.calledOnce).to.equal(true);
		const msg = send.getCall(0).args[0];
		expect(msg).to.be.instanceOf(RequestMaybeSync);
		expect((msg as RequestMaybeSync).hashes).to.deep.equal(["b", "a", "c"]);
	});

	it("pre-syncs top priority entries when using rateless IBLT", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;

		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			sync: {
				maxSimpleEntries: 2,
				priority: (entry: any) =>
					entry.hash === "h10" ? 100 : entry.hash === "h20" ? 50 : 0,
			},
		});

		const simpleSyncSpy = sinon.stub(sync.simple, "onMaybeMissingEntries");
		simpleSyncSpy.resolves();

		const entries = new Map<string, any>();
		for (let i = 0; i < 334; i++) {
			entries.set(`h${i}`, {
				hash: `h${i}`,
				hashNumber: BigInt(i + 1),
				assignedToRangeBoundary: i === 0,
			});
		}

		await sync.onMaybeMissingEntries({
			entries: entries as any,
			targets: ["p"],
		});

		expect(simpleSyncSpy.calledOnce).to.equal(true);
		const arg = simpleSyncSpy.getCall(0).args[0];
		expect(arg.entries.size).to.equal(3);
		expect([...arg.entries.keys()]).to.have.members(["h0", "h10", "h20"]);
	});
});
