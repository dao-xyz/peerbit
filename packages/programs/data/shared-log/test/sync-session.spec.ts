import { Cache } from "@peerbit/cache";
import { expect } from "chai";
import sinon from "sinon";
import { SimpleSyncronizer } from "../src/sync/simple.js";

describe("sync-repair-session", () => {
	it("resolves convergent session when missing entries are received", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const known = new Set<string>();

		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {
				count: async ({ query }: any) => {
					const value = query?.hashNumber;
					return known.has(String(value)) ? 1 : 0;
				},
			} as any,
			log: {
				has: async (hash: string) => known.has(hash),
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		const entries = new Map<string, any>();
		entries.set("a", { hash: "a" });
		entries.set("b", { hash: "b" });

		const session = sync.startRepairSession({
			entries: entries as any,
			targets: ["p1"],
			mode: "convergent",
			timeoutMs: 2_000,
			retryIntervalsMs: [0],
		});

		const from = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;
		known.add("a");
		known.add("b");
		sync.onReceivedEntries({
			entries: [{ entry: { hash: "a" } }, { entry: { hash: "b" } }] as any,
			from,
		});

		const result = await session.done;
		expect(result).to.have.length(1);
		expect(result[0]!.completed).to.equal(true);
		expect(result[0]!.unresolved).to.deep.equal([]);
		expect(result[0]!.resolved).to.equal(2);
	});

	it("returns unresolved entries when convergent session times out", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;

		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: {
				has: async () => false,
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		const entries = new Map<string, any>();
		entries.set("a", { hash: "a" });

		const session = sync.startRepairSession({
			entries: entries as any,
			targets: ["p1"],
			mode: "convergent",
			timeoutMs: 120,
			retryIntervalsMs: [0],
		});

		const result = await session.done;
		expect(send.called).to.equal(true);
		expect(result).to.have.length(1);
		expect(result[0]!.completed).to.equal(false);
		expect(result[0]!.unresolved).to.deep.equal(["a"]);
		expect(result[0]!.attempts).to.be.greaterThan(0);
	});
});
