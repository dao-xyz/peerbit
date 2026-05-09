import { expect } from "chai";
import { createRangePlanner } from "../src/index.js";

const range = (properties: {
	id: string;
	hash: string;
	start1: number;
	end1: number;
	timestamp?: bigint;
	mode?: number;
	start2?: number;
	end2?: number;
}) => {
	const start2 = properties.start2 ?? properties.start1;
	const end2 = properties.end2 ?? properties.end1;
	return {
		...properties,
		start2,
		end2,
		width:
			properties.end1 -
			properties.start1 +
			(end2 < properties.end1 ? end2 - start2 : 0),
		timestamp: properties.timestamp ?? 0n,
		mode: properties.mode ?? 0,
	};
};

describe("native shared-log range planner", () => {
	it("returns intersecting leaders", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 10, end1: 20 }));

		expect(planner.getSamples([15], { now: 1_000 })).to.deep.equal(
			new Map([["peer-a", { intersecting: true }]]),
		);
	});

	it("falls back to closest mature non-strict ranges", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 90, end1: 100 }));

		expect(planner.getSamples([50, 75], { now: 1_000 })).to.deep.equal(
			new Map([
				["peer-a", { intersecting: false }],
				["peer-b", { intersecting: false }],
			]),
		);
	});

	it("returns full replica leaders when ranges fit within the replica count", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 90, end1: 100 }));

		expect(planner.getFullReplicaLeaders(2, { now: 1_000 })).to.deep.equal(
			new Map([
				["peer-a", { intersecting: true }],
				["peer-b", { intersecting: true }],
			]),
		);
	});

	it("skips full replica leaders when ranges exceed the replica count", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 90, end1: 100 }));

		expect(planner.getFullReplicaLeaders(1, { now: 1_000 })).to.equal(
			undefined,
		);
	});

	it("honors full replica filters, maturity, and strict fallback options", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(
			range({
				id: "b",
				hash: "peer-b",
				start1: 20,
				end1: 30,
				mode: 1,
			}),
		);
		planner.put(
			range({
				id: "c",
				hash: "peer-c",
				start1: 40,
				end1: 50,
				timestamp: 950n,
			}),
		);

		expect(
			planner.getFullReplicaLeaders(3, {
				now: 1_000,
				roleAge: 100,
				includeStrict: false,
				peerFilter: ["peer-a", "peer-b", "peer-c"],
			}),
		).to.deep.equal(new Map([["peer-a", { intersecting: true }]]));
	});

	it("expands underfilled peer filters with mature indexed peers", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(
			range({
				id: "b",
				hash: "peer-b",
				start1: 20,
				end1: 30,
				mode: 1,
			}),
		);
		planner.put(
			range({
				id: "c",
				hash: "peer-c",
				start1: 40,
				end1: 50,
				timestamp: 950n,
			}),
		);

		expect(
			planner.includeMaturedPeers(["peer-a"], 1, {
				now: 1_000,
				roleAge: 100,
				selfHash: "peer-self",
				selfReplicating: true,
			}),
		).to.deep.equal(new Set(["peer-a", "peer-b"]));
	});

	it("does not add self to peer filters when self is not replicating", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));

		expect(
			planner.includeMaturedPeers(["peer-a"], 1, {
				now: 1_000,
				selfHash: "peer-b",
				selfReplicating: false,
			}),
		).to.deep.equal(new Set(["peer-a"]));
	});

	it("honors peer filters and maturity", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 10, end1: 20 }));
		planner.put(
			range({
				id: "b",
				hash: "peer-b",
				start1: 10,
				end1: 20,
				timestamp: 950n,
			}),
		);

		expect(
			planner.getSamples([15], {
				now: 1_000,
				roleAge: 100,
				peerFilter: ["peer-b"],
			}),
		).to.deep.equal(new Map([["peer-b", { intersecting: true }]]));
	});

	it("does not fallback to future ranges", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(
			range({
				id: "a",
				hash: "peer-a",
				start1: 10,
				end1: 20,
				timestamp: 2_000n,
			}),
		);

		expect(planner.getSamples([50], { now: 1_000 })).to.deep.equal(new Map());
	});

	it("supports wrapped ranges", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(
			range({
				id: "a",
				hash: "peer-a",
				start1: 90,
				end1: 100,
				start2: 0,
				end2: 10,
			}),
		);

		expect(planner.getSamples([5], { now: 1_000 })).to.deep.equal(
			new Map([["peer-a", { intersecting: true }]]),
		);
	});

	it("supports u64 cursor values", async () => {
		const planner = await createRangePlanner("u64");
		planner.put({
			id: "a",
			hash: "peer-a",
			timestamp: 0n,
			start1: 10n,
			end1: 20n,
			start2: 10n,
			end2: 20n,
			width: 10n,
			mode: 0,
		});

		expect(planner.getSamples([15n], { now: 1_000n })).to.deep.equal(
			new Map([["peer-a", { intersecting: true }]]),
		);
	});
});
