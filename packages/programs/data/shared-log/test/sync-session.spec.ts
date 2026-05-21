import { Cache } from "@peerbit/cache";
import { expect } from "chai";
import sinon from "sinon";
import { SimpleSyncronizer } from "../src/sync/simple.js";

describe("sync-repair-session", () => {
	it("deduplicates known hash aliases without changing coordinate requests", async () => {
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 10 });
		coordinateToHash.add(42n, "entry-hash");
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: {
				has: async () => false,
			} as any,
			coordinateToHash,
		});
		const from = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;

		await sync.queueSync([42n, "entry-hash"], from);

		expect(sync.syncInFlightQueue.has(42n)).to.equal(true);
		expect(sync.syncInFlightQueue.has("entry-hash")).to.equal(false);
		expect(sync.syncInFlightQueueInverted.get("p1")).to.deep.equal(
			new Set([42n]),
		);
		expect(send.calledOnce).to.equal(true);
	});

	it("deduplicates hash sync requests with already queued coordinate aliases", async () => {
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 10 });
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: {
				has: async () => false,
			} as any,
			coordinateToHash,
		});
		const p1 = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;
		const p2 = {
			hashcode: () => "p2",
			equals: () => false,
		} as any;

		await sync.queueSync([42n], p1);
		coordinateToHash.add(42n, "entry-hash");
		await sync.queueSync(["entry-hash"], p2);

		expect(sync.syncInFlightQueue.has(42n)).to.equal(true);
		expect(sync.syncInFlightQueue.has("entry-hash")).to.equal(false);
		expect(
			sync.syncInFlightQueue.get(42n)?.map((x) => x.hashcode()),
		).to.deep.equal(["p1", "p2"]);
		expect(sync.syncInFlightQueueInverted.get("p2")).to.deep.equal(
			new Set([42n]),
		);
		expect(send.calledOnce).to.equal(true);
	});

	it("clears in-flight coordinate aliases when an entry is received by hash", () => {
		const coordinateToHash = new Cache<string>({ max: 10 });
		coordinateToHash.add(42n, "entry-hash");
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: {
				has: async () => false,
			} as any,
			coordinateToHash,
		});
		const from = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;

		sync.syncInFlightQueue.set(42n, [from]);
		sync.syncInFlightQueueInverted.set("p1", new Set([42n]));
		sync.syncInFlight.set("p1", new Map([[42n, { timestamp: Date.now() }]]));

		sync.onReceivedEntries({
			entries: [{ entry: { hash: "entry-hash" } }] as any,
			from,
		});

		expect(sync.syncInFlightQueue.has(42n)).to.equal(true);
		expect(sync.syncInFlightQueueInverted.has("p1")).to.equal(true);
		expect(sync.syncInFlight.has("p1")).to.equal(false);
	});

	it("clears pending coordinate aliases when an entry is added by hash", () => {
		const coordinateToHash = new Cache<string>({ max: 10 });
		coordinateToHash.add(42n, "entry-hash");
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {
				count: async () => 0,
			} as any,
			log: {
				has: async () => false,
			} as any,
			coordinateToHash,
		});
		const from = {
			hashcode: () => "p1",
			equals: () => false,
		} as any;

		sync.syncInFlightQueue.set(42n, [from]);
		sync.syncInFlightQueueInverted.set("p1", new Set([42n]));
		sync.syncInFlight.set("p1", new Map([[42n, { timestamp: Date.now() }]]));

		sync.onEntryAdded({ hash: "entry-hash" } as any);

		expect(sync.syncInFlightQueue.has(42n)).to.equal(false);
		expect(sync.syncInFlightQueueInverted.has("p1")).to.equal(false);
		expect(sync.syncInFlight.has("p1")).to.equal(false);
	});

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

	it("caps tracked hashes for large convergent sessions", async () => {
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
			sync: {
				maxConvergentTrackedHashes: 1,
			},
		});

		const entries = new Map<string, any>();
		entries.set("a", { hash: "a" });
		entries.set("b", { hash: "b" });
		entries.set("c", { hash: "c" });

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
		expect(result[0]!.requested).to.equal(1);
		expect(result[0]!.requestedTotal).to.equal(3);
		expect(result[0]!.truncated).to.equal(true);
		expect(result[0]!.unresolved).to.have.length(1);
	});
});
