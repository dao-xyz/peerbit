import { expect } from "chai";
import { createRangePlanner, createSharedLogState } from "../src/index.js";

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

	it("finds leaders through the combined native path", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));

		expect(
			planner.findLeaders([5], 1, {
				now: 1_000,
				peerFilter: ["peer-a"],
				fullReplicaFallback: false,
			}),
		).to.deep.equal(new Map([["peer-a", { intersecting: true }]]));
	});

	it("uses full replica fallback through the combined native path", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));

		expect(
			planner.findLeaders([50, 75], 2, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		).to.deep.equal(
			new Map([
				["peer-a", { intersecting: true }],
				["peer-b", { intersecting: true }],
			]),
		);
	});

	it("finds hash gid leaders without returning coordinates to TypeScript", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));

		const gid = "entry-gid";
		expect(
			planner.findLeadersForGid(gid, 2, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		).to.deep.equal(
			planner.findLeaders(planner.getGidCoordinates(gid, 2), 2, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		);
	});

	it("plans hash gid coordinates and leaders together", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));

		const gid = "entry-gid";
		const coordinates = planner.getGidCoordinates(gid, 2);
		expect(
			planner.planLeadersForGid(gid, 2, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		).to.deep.equal({
			coordinates,
			leaders: planner.findLeaders(coordinates, 2, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		});
	});

	it("finds persisted-coordinate leaders in one native batch", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));
		planner.put(range({ id: "c", hash: "peer-c", start1: 80, end1: 90 }));

		const items = [
			{ cursors: [5], replicas: 1 },
			{ cursors: [50, 75], replicas: 2 },
		];

		expect(
			planner.findLeadersBatch(items, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		).to.deep.equal(
			items.map((item) =>
				planner.findLeaders(item.cursors, item.replicas, {
					now: 1_000,
					fullReplicaFallback: true,
				}),
			),
		);
	});

	it("plans hash gid coordinates and leaders in one native batch", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));
		planner.put(range({ id: "c", hash: "peer-c", start1: 80, end1: 90 }));

		const items = [
			{ gid: "entry-gid-a", replicas: 1 },
			{ gid: "entry-gid-b", replicas: 2 },
		];

		expect(
			planner.planLeadersForGidsBatch(items, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		).to.deep.equal(
			items.map((item) =>
				planner.planLeadersForGid(item.gid, item.replicas, {
					now: 1_000,
					fullReplicaFallback: true,
				}),
			),
		);
	});

	it("plans hash gid leaders from resident shared-log state", async () => {
		const planner = await createRangePlanner("u32");
		const state = await createSharedLogState("u32");
		const ranges = [
			range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }),
			range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }),
		];
		for (const item of ranges) {
			planner.put(item);
			state.put(item);
		}

		expect(
			state.planLeadersForGid("entry-gid", 2, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		).to.deep.equal(
			planner.planLeadersForGid("entry-gid", 2, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
		);
	});

	it("plans entry assignment metadata from resident shared-log state", async () => {
		const planner = await createRangePlanner("u32");
		const state = await createSharedLogState("u32");
		const ranges = [
			range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }),
			range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }),
		];
		for (const item of ranges) {
			planner.put(item);
			state.put(item);
		}

		const plan = state.planEntryAssignmentForGid("entry-gid", 2, {
			now: 1_000,
			fullReplicaFallback: true,
		});

		expect(plan).to.deep.equal({
			...planner.planLeadersForGid("entry-gid", 2, {
				now: 1_000,
				fullReplicaFallback: true,
			}),
			assignedToRangeBoundary:
				plan.leaders.size < 2 ||
				[...plan.leaders.values()].some((leader) => !leader.intersecting),
		});
	});

	it("commits entry coordinates to resident shared-log state", async () => {
		const state = await createSharedLogState("u32");
		state.putEntryCoordinates("old-head", [1, 2]);

		state.commitEntryCoordinates("new-head", [3, 4], ["old-head"]);

		expect(state.getEntryCoordinates("new-head")).to.deep.equal([3, 4]);
		expect(state.getEntryCoordinates("old-head")).to.equal(undefined);
	});

	it("plans repair dispatch targets in one native batch", async () => {
		const planner = await createRangePlanner("u32");

		expect(
			planner.planRepairDispatchBatch({
				entries: [
					{
						hash: "entry-a",
						gid: "gid-a",
						requestedReplicas: 2,
						currentLeaders: ["peer-a", "peer-b"],
						knownGidPeers: ["peer-b"],
						knownEntryPeers: ["peer-known"],
					},
					{
						hash: "entry-b",
						gid: "gid-b",
						requestedReplicas: 1,
						currentLeaders: ["peer-self", "peer-c"],
					},
				],
				pendingModes: ["churn", "join-authoritative"],
				pendingPeersByMode: new Map([
					["churn", ["peer-a", "peer-c", "peer-optimistic"]],
					["join-authoritative", ["peer-a", "peer-full", "peer-known"]],
				]),
				optimisticPeersByMode: new Map([
					[
						"churn",
						new Map([
							["gid-a", ["peer-optimistic"]],
							["gid-b", []],
						]),
					],
				]),
				fullReplicaRepairCandidates: ["peer-full"],
				fullReplicaRepairCandidateCount: 2,
				selfHash: "peer-self",
			}),
		).to.deep.equal(
			new Map([
				[
					"churn",
					new Map([
						["peer-a", ["entry-a"]],
						["peer-b", ["entry-a"]],
						["peer-optimistic", ["entry-a"]],
						["peer-c", ["entry-b"]],
					]),
				],
				[
					"join-authoritative",
					new Map([
						["peer-a", ["entry-a"]],
						["peer-full", ["entry-a"]],
					]),
				],
			]),
		);
	});

	it("plans repair dispatch from resident shared-log state", async () => {
		const state = await createSharedLogState("u32");
		state.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		state.addGidPeers("gid-known", ["peer-a"]);
		state.markEntriesKnownByPeer(["entry-known"], "peer-a");

		expect(
			state.planRepairDispatchForEntries(
				{
					entries: [
						{
							hash: "entry-known",
							gid: "gid-fresh",
							requestedReplicas: 1,
							coordinates: [5],
						},
						{
							hash: "entry-gid",
							gid: "gid-known",
							requestedReplicas: 1,
							coordinates: [5],
						},
						{
							hash: "entry-fresh",
							gid: "gid-fresh",
							requestedReplicas: 1,
							coordinates: [5],
						},
					],
					pendingModes: ["join-warmup", "join-authoritative"],
					pendingPeersByMode: new Map([
						["join-warmup", ["peer-a"]],
						["join-authoritative", ["peer-a"]],
					]),
					fullReplicaRepairCandidateCount: 1,
					selfHash: "peer-self",
				},
				{ now: 1_000 },
			),
		).to.deep.equal(
			new Map([
				["join-warmup", new Map([["peer-a", ["entry-fresh"]]])],
				[
					"join-authoritative",
					new Map([["peer-a", ["entry-gid", "entry-fresh"]]]),
				],
			]),
		);
	});

	it("plans repair dispatch from native leader selection", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));

		expect(
			planner.planRepairDispatchForEntries(
				{
					entries: [
						{
							hash: "entry-a",
							gid: "gid-a",
							requestedReplicas: 1,
							coordinates: [5],
						},
					],
					pendingModes: ["churn", "join-authoritative"],
					pendingPeersByMode: new Map([
						["churn", ["peer-a"]],
						["join-authoritative", ["peer-a", "peer-b"]],
					]),
					fullReplicaRepairCandidateCount: 1,
					selfHash: "peer-self",
				},
				{ now: 1_000 },
			),
		).to.deep.equal(
			new Map([
				["churn", new Map([["peer-a", ["entry-a"]]])],
				["join-authoritative", new Map([["peer-a", ["entry-a"]]])],
			]),
		);
	});

	it("expands peer filters through the combined native path", async () => {
		const planner = await createRangePlanner("u32");
		planner.put(range({ id: "a", hash: "peer-a", start1: 0, end1: 10 }));
		planner.put(range({ id: "b", hash: "peer-b", start1: 20, end1: 30 }));

		expect(
			planner.findLeaders([50, 75], 2, {
				now: 1_000,
				peerFilter: ["peer-a"],
				expandPeerFilter: true,
				selfHash: "peer-self",
				selfReplicating: true,
				fullReplicaFallback: true,
			}),
		).to.deep.equal(
			new Map([
				["peer-a", { intersecting: true }],
				["peer-b", { intersecting: true }],
			]),
		);
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

	it("creates u32 coordinate grids", async () => {
		const planner = await createRangePlanner("u32");
		const max = 4_294_967_295;
		const from = 100;

		expect(planner.getGrid(from, 3)).to.deep.equal([
			from,
			Math.round(from + max / 3) % max,
			Math.round(from + (2 * max) / 3) % max,
		]);
	});

	it("creates u64 coordinate grids", async () => {
		const planner = await createRangePlanner("u64");
		const max = 18_446_744_073_709_551_615n;
		const from = 100n;

		expect(planner.getGrid(from, 3)).to.deep.equal([
			from,
			(from + max / 3n) % max,
			(from + (2n * max) / 3n) % max,
		]);
	});
});
