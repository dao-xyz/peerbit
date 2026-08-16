import { serialize } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import type { Index } from "@peerbit/indexer-interface";
import { expect } from "chai";
import sinon from "sinon";
import { MAX_U32, MAX_U64, type NumberFromType } from "../src/integers.js";
import {
	type EntryReplicated,
	type RebalanceScanPlan,
	type RebalanceScanTask,
	type RebalanceTaskCursor,
	type ReplicationChange,
	ReplicationIntent,
	type ReplicationRangeIndexable,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
	createRebalanceScanPlan,
	findOwningRebalanceTask,
	getRebalanceScanTaskPage,
	toRebalance,
} from "../src/ranges.js";

type Resolution = "u32" | "u64";

const makeId = (value: number) => {
	const id = new Uint8Array(32);
	new DataView(id.buffer).setUint32(28, value);
	return id;
};

const asCoordinate = <R extends Resolution>(
	resolution: R,
	value: number | bigint,
): NumberFromType<R> =>
	(resolution === "u32" ? Number(value) : BigInt(value)) as NumberFromType<R>;

const makeRange = <R extends Resolution>(
	resolution: R,
	properties: {
		id: number;
		owner?: string;
		offset: number | bigint;
		width: number | bigint;
		mode?: ReplicationIntent;
		timestamp?: bigint;
	},
): ReplicationRangeIndexable<R> => {
	const common = {
		id: makeId(properties.id),
		publicKeyHash: properties.owner ?? "owner-a",
		mode: properties.mode ?? ReplicationIntent.Strict,
		timestamp: properties.timestamp ?? BigInt(properties.id + 1),
	};
	return (resolution === "u32"
		? new ReplicationRangeIndexableU32({
				...common,
				offset: Number(properties.offset),
				width: Number(properties.width),
			})
		: new ReplicationRangeIndexableU64({
				...common,
				offset: BigInt(properties.offset),
				width: BigInt(properties.width),
			})) as unknown as ReplicationRangeIndexable<R>;
};

const change = <R extends Resolution>(
	range: ReplicationRangeIndexable<R>,
	type: ReplicationChange<ReplicationRangeIndexable<R>>["type"] = "added",
): ReplicationChange<ReplicationRangeIndexable<R>> => ({
	range,
	type,
	timestamp: range.timestamp,
});

const scanShape = <R extends Resolution>(plan: RebalanceScanPlan<R>) => ({
	boundary: plan.boundary,
	geometryRanges: plan.geometryRanges,
	ownedIntervals: plan.ownedIntervals,
	taskCount: plan.taskCount,
});

const collectTaskPages = <R extends Resolution>(plan: RebalanceScanPlan<R>) => {
	const pages: RebalanceScanTask<R>[][] = [];
	let cursor: RebalanceTaskCursor | undefined;
	for (;;) {
		const page = getRebalanceScanTaskPage(plan, cursor);
		pages.push([...page.tasks]);
		if (!page.next) {
			break;
		}
		cursor = page.next;
	}
	return pages;
};

const consume = async <R extends Resolution>(
	iterable: AsyncIterable<EntryReplicated<R>>,
) => {
	const entries: EntryReplicated<R>[] = [];
	for await (const entry of iterable) {
		entries.push(entry);
	}
	return entries;
};

const sparseChanges = <R extends Resolution>(
	resolution: R,
	count: number,
	options?: { boundary?: boolean },
) =>
	Array.from({ length: count }, (_, index) =>
		change(
			makeRange(resolution, {
				id: index + 1,
				offset: 10 + index * 2,
				width: 1,
				mode:
					options?.boundary && index === 0
						? ReplicationIntent.NonStrict
						: ReplicationIntent.Strict,
			}),
		),
	);

const emptySequentialIndex = <R extends Resolution>(properties?: {
	failurePass?: number;
	failure?: "next" | "close";
	incompletePass?: number;
	entryPasses?: number[];
	entry?: EntryReplicated<R>;
}) => {
	let passes = 0;
	let active = 0;
	const closed: number[] = [];
	return {
		index: {
			iterate: () => {
				const pass = passes++;
				expect(active).to.equal(0);
				active++;
				let done = false;
				return {
					next: async () => {
						if (
							properties?.failurePass === pass &&
							properties.failure === "next"
						) {
							throw new Error("planned next failure");
						}
						if (properties?.incompletePass !== pass) {
							done = true;
						}
						return properties?.entryPasses?.includes(pass) && properties.entry
							? ([{ value: properties.entry }] as any)
							: [];
					},
					all: async () => {
						throw new Error("unbounded iterator drain should not be used");
					},
					done: () => done,
					pending: async () => (done ? 0 : 1),
					close: async () => {
						closed.push(pass);
						active--;
						if (
							properties?.failurePass === pass &&
							properties.failure === "close"
						) {
							throw new Error("planned close failure");
						}
					},
				};
			},
		} as unknown as Index<EntryReplicated<R>>,
		closed,
		get passes() {
			return passes;
		},
		get active() {
			return active;
		},
	};
};

for (const resolution of ["u32", "u64"] as const) {
	describe(`rebalance scan planner: ${resolution}`, () => {
		it("is pure and canonical for independent change permutations", () => {
			const first = makeRange(resolution, {
				id: 1,
				owner: "owner-a",
				offset: 100,
				width: 10,
			});
			const second = makeRange(resolution, {
				id: 2,
				owner: "owner-b",
				offset: 300,
				width: 10,
			});
			const changes = [change(first), change(second)];
			const history = new Set(["unrelated"]);
			const firstBytes = serialize(first);
			const secondBytes = serialize(second);

			const plan = createRebalanceScanPlan({ changes, history });
			const repeated = createRebalanceScanPlan({ changes, history });
			const permuted = createRebalanceScanPlan({
				changes: [...changes].reverse(),
				history,
			});

			expect(repeated).to.deep.equal(plan);
			expect(scanShape(permuted)).to.deep.equal(scanShape(plan));
			expect(permuted.historyMutations.map((x) => x.rangeHash)).to.deep.equal(
				plan.historyMutations.map((x) => x.rangeHash).reverse(),
			);
			expect(serialize(first)).to.deep.equal(firstBytes);
			expect(serialize(second)).to.deep.equal(secondBytes);
			expect(changes.map((item) => item.range)).to.deep.equal([first, second]);
			expect([...history]).to.deep.equal(["unrelated"]);
		});

		it("uses canonical DTOs and emits one leading boundary task", () => {
			const max = resolution === "u32" ? BigInt(MAX_U32) : MAX_U64;
			const wrapped = makeRange(resolution, {
				id: 1,
				offset: max - 4n,
				width: 8,
				mode: ReplicationIntent.NonStrict,
			});
			const ordinary = makeRange(resolution, {
				id: 2,
				offset: 100,
				width: 3,
				mode: ReplicationIntent.NonStrict,
			});
			const plan = createRebalanceScanPlan({
				changes: [change(ordinary), change(wrapped)],
				history: new Set(),
			});
			const tasks = collectTaskPages(plan).flat();

			expect(tasks[0]).to.deep.equal({ kind: "boundary", ordinal: 0 });
			expect(tasks.filter((task) => task.kind === "boundary")).to.have.length(
				1,
			);
			expect(plan.geometryRanges).to.have.length(2);
			expect(plan.geometryRanges[0].start1).to.equal(
				asCoordinate(resolution, max - 4n),
			);
			expect(plan.geometryRanges[0].start2).to.equal(
				asCoordinate(resolution, 0),
			);
			expect(Object.keys(plan.geometryRanges[0]).sort()).to.deep.equal([
				"end1",
				"end2",
				"mode",
				"start1",
				"start2",
			]);
			expect((plan.geometryRanges[0] as any).hash).to.equal(undefined);
			expect((plan.geometryRanges[0] as any).id).to.equal(undefined);
			expect((plan.geometryRanges[0] as any).timestamp).to.equal(undefined);
			expect(Object.getPrototypeOf(plan.geometryRanges[0])).to.equal(
				Object.prototype,
			);
		});

		it("does not hide an identical-geometry owner swap", () => {
			const previous = makeRange(resolution, {
				id: 1,
				owner: "owner-a",
				offset: 100,
				width: 20,
			});
			const next = makeRange(resolution, {
				id: 2,
				owner: "owner-b",
				offset: 100,
				width: 20,
			});
			const plan = createRebalanceScanPlan({
				changes: [change(previous, "removed"), change(next)],
				history: new Set([previous.rangeHash]),
			});

			expect(plan.geometryRanges).to.have.length(1);
			expect(plan.taskCount).to.equal(1);
		});

		it("keeps a proven same-owner no-op separate from history commit", () => {
			const previous = makeRange(resolution, {
				id: 1,
				owner: "owner-a",
				offset: 100,
				width: 20,
			});
			const next = makeRange(resolution, {
				id: 2,
				owner: "owner-a",
				offset: 100,
				width: 20,
			});
			const plan = createRebalanceScanPlan({
				changes: [change(previous, "removed"), change(next)],
				history: new Set([previous.rangeHash]),
			});

			expect(plan.taskCount).to.equal(0);
			expect(plan.geometryRanges).to.be.empty;
			expect(plan.historyMutations).to.deep.equal([
				{ rangeHash: previous.rangeHash, present: false },
				{ rangeHash: next.rangeHash, present: true },
			]);
			expect(getRebalanceScanTaskPage(plan).tasks).to.be.empty;
			expect(getRebalanceScanTaskPage(plan).next).to.equal(undefined);
			expect(
				getRebalanceScanTaskPage(plan, { nextTask: plan.taskCount }),
			).to.deep.equal({ tasks: [], next: undefined });
		});

		it("caps ranges per task and counts boundary work in task pages", () => {
			for (const [count, expectedTasks] of [
				[128, 1],
				[129, 2],
			] as const) {
				const plan = createRebalanceScanPlan({
					changes: sparseChanges(resolution, count),
					history: new Set(),
				});
				expect(plan.taskCount).to.equal(expectedTasks);
				expect(
					collectTaskPages(plan)
						.flat()
						.filter((task) => task.kind === "geometry")
						.map((task) => task.ranges.length),
				).to.deep.equal(count === 128 ? [128] : [128, 1]);
			}

			const plan = createRebalanceScanPlan({
				changes: sparseChanges(resolution, 1024, { boundary: true }),
				history: new Set(),
			});
			const pages = collectTaskPages(plan);
			const tasks = pages.flat();

			expect(pages.map((page) => page.length)).to.deep.equal([8, 1]);
			expect(pages.every((page) => page.length <= 8)).to.be.true;
			expect(tasks.map((task) => task.ordinal)).to.deep.equal(
				Array.from({ length: 9 }, (_, index) => index),
			);
			expect(tasks[0].kind).to.equal("boundary");
			expect(
				tasks
					.filter((task) => task.kind === "geometry")
					.every((task) => task.ranges.length <= 128),
			).to.be.true;
			const firstPage = getRebalanceScanTaskPage(plan);
			expect(getRebalanceScanTaskPage(plan)).to.deep.equal(firstPage);
			expect(firstPage.next).to.deep.equal({ nextTask: 8 });
			expect(getRebalanceScanTaskPage(plan, firstPage.next).next).to.equal(
				undefined,
			);

			const owner = findOwningRebalanceTask(
				[
					asCoordinate(resolution, 10 + 895 * 2),
					asCoordinate(resolution, 10 + 896 * 2),
				],
				plan.ownedIntervals,
			);
			expect(owner).to.equal(6);
		});

		it("rejects task offsets outside the current plan", () => {
			const plan = createRebalanceScanPlan({
				changes: sparseChanges(resolution, 129),
				history: new Set(),
			});
			for (const nextTask of [-1, 0.5, Number.NaN, plan.taskCount + 1]) {
				expect(() => getRebalanceScanTaskPage(plan, { nextTask })).to.throw(
					"Invalid rebalance task cursor",
				);
			}
		});

		it("does not commit history after a page-two failure or incomplete page", async () => {
			const changes = sparseChanges(resolution, 1024, { boundary: true });
			for (const failure of ["next", "close"] as const) {
				const cache = new Cache<string>({ max: 2048, ttl: 1e5 });
				const observed = emptySequentialIndex<typeof resolution>({
					failurePass: 8,
					failure,
				});
				let caught: unknown;
				try {
					await consume(toRebalance(changes, observed.index, cache));
				} catch (error) {
					caught = error;
				}
				expect(caught).to.be.instanceOf(Error);
				expect(observed.closed).to.deep.equal(
					Array.from({ length: 9 }, (_, index) => index),
				);
				expect(observed.active).to.equal(0);
				expect(cache.has(changes.at(-1)!.range.rangeHash)).to.be.false;
			}

			const incompleteCache = new Cache<string>({ max: 2048, ttl: 1e5 });
			const incomplete = emptySequentialIndex<typeof resolution>({
				incompletePass: 8,
			});
			expect(
				await consume(toRebalance(changes, incomplete.index, incompleteCache)),
			).to.be.empty;
			expect(incomplete.closed).to.deep.equal(
				Array.from({ length: 9 }, (_, index) => index),
			);
			expect(incompleteCache.has(changes.at(-1)!.range.rangeHash)).to.be.false;
		});

		it("does not commit history after a consumer return on page two", async () => {
			const changes = sparseChanges(resolution, 1024, { boundary: true });
			const entry = {
				hash: "page-two",
				gid: "page-two",
				coordinates: [asCoordinate(resolution, 10 + 896 * 2)],
				assignedToRangeBoundary: false,
			} as EntryReplicated<typeof resolution>;
			const cache = new Cache<string>({ max: 2048, ttl: 1e5 });
			const observed = emptySequentialIndex<typeof resolution>({
				entryPasses: [8],
				entry,
			});

			for await (const value of toRebalance(changes, observed.index, cache)) {
				expect(value.hash).to.equal("page-two");
				break;
			}

			expect(observed.closed).to.deep.equal(
				Array.from({ length: 9 }, (_, index) => index),
			);
			expect(observed.active).to.equal(0);
			expect(cache.has(changes.at(-1)!.range.rangeHash)).to.be.false;
		});

		it("deduplicates a multi-coordinate row across task pages", async () => {
			const changes = sparseChanges(resolution, 1024, { boundary: true });
			const entry = {
				hash: "multi",
				gid: "multi",
				coordinates: [
					asCoordinate(resolution, 10 + 895 * 2),
					asCoordinate(resolution, 10 + 896 * 2),
				],
				assignedToRangeBoundary: false,
			} as EntryReplicated<typeof resolution>;
			const cache = new Cache<string>({ max: 2048, ttl: 1e5 });
			// Pass zero is boundary. Geometry task six is pass seven; returning the
			// row again on page-two pass eight must not duplicate it.
			const observed = emptySequentialIndex<typeof resolution>({
				entryPasses: [7, 8],
				entry,
			});
			const result = await consume(toRebalance(changes, observed.index, cache));

			expect(result.map((value) => value.hash)).to.deep.equal(["multi"]);
			expect(observed.passes).to.equal(9);
			expect(observed.closed).to.deep.equal(
				Array.from({ length: 9 }, (_, index) => index),
			);
			expect(cache.has(changes.at(-1)!.range.rangeHash)).to.be.true;
		});

		it("trims history before planning and never rotates it in force-fresh mode", async () => {
			const clock = sinon.useFakeTimers({ now: 1_000 });
			try {
				const previous = makeRange(resolution, {
					id: 1,
					owner: "owner-a",
					offset: 100,
					width: 20,
				});
				const next = makeRange(resolution, {
					id: 2,
					owner: "owner-a",
					offset: 100,
					width: 20,
				});
				const expired = new Cache<string>({ max: 100, ttl: 10 });
				expired.add(previous.rangeHash);
				clock.tick(11);
				const observed = emptySequentialIndex<typeof resolution>();
				await consume(
					toRebalance(
						[change(previous, "removed"), change(next)],
						observed.index,
						expired,
					),
				);
				expect(observed.passes).to.equal(1);
				expect(expired.has(previous.rangeHash)).to.be.false;
				expect(expired.has(next.rangeHash)).to.be.true;

				const forceFresh = new Cache<string>({ max: 100, ttl: 1e5 });
				forceFresh.add(previous.rangeHash);
				await consume(
					toRebalance(
						[change(previous, "removed")],
						emptySequentialIndex<typeof resolution>().index,
						forceFresh,
						{ forceFresh: true },
					),
				);
				expect(forceFresh.has(previous.rangeHash)).to.be.true;
			} finally {
				clock.restore();
			}
		});
	});
}
