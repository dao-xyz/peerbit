import { expect } from "chai";
import { type RebalanceScanPlan, ReplicationIntent } from "../src/ranges.js";
import {
	type BoundedRebalanceScanSource,
	type RebalanceScanCandidate,
	RebalanceScanExecutor,
	type RebalanceScanSourceRequest,
	type RebalanceScanSourceResult,
	type RebalanceVisitKey,
} from "../src/rebalance-scan-executor.js";
import {
	type RebalanceWorkPersistence,
	RebalanceWorkStore,
} from "../src/rebalance-work-store.js";

class MemoryPersistence implements RebalanceWorkPersistence {
	readonly writes: string[] = [];
	readonly barriers: string[] = [];
	private nextBarrierFailure?: unknown;

	constructor(readonly files = new Map<string, Uint8Array>()) {}

	async read(name: string, maxBytes: number) {
		const value = this.files.get(name);
		if (value && value.byteLength > maxBytes) throw new Error("oversized");
		return value ? new Uint8Array(value) : undefined;
	}

	async write(name: string, bytes: Uint8Array) {
		this.writes.push(name);
		this.files.set(name, new Uint8Array(bytes));
	}

	async durableBarrier(name?: string) {
		this.barriers.push(name!);
		if (this.nextBarrierFailure !== undefined) {
			const error = this.nextBarrierFailure;
			this.nextBarrierFailure = undefined;
			throw error;
		}
	}

	failNextBarrier(error: unknown) {
		this.nextBarrierFailure = error;
	}

	fork() {
		return new MemoryPersistence(this.files);
	}
}

const digest = (character: string) => character.repeat(64);

const plan = (options?: {
	boundary?: boolean;
	geometry?: boolean;
}): RebalanceScanPlan<"u32"> => {
	const boundary = options?.boundary ?? true;
	const geometry = options?.geometry ?? false;
	return {
		boundary,
		geometryRanges: geometry
			? [
					{
						start1: 10,
						end1: 20,
						start2: 10,
						end2: 20,
						mode: ReplicationIntent.Strict,
					},
				]
			: [],
		ownedIntervals: geometry ? [{ start: 10n, end: 20n, geometryTask: 0 }] : [],
		taskCount: (boundary ? 1 : 0) + (geometry ? 1 : 0),
		historyMutations: [{ rangeHash: "range", present: true }],
	};
};

const planU64 = (): RebalanceScanPlan<"u64"> => ({
	boundary: true,
	geometryRanges: [],
	ownedIntervals: [],
	taskCount: 1,
	historyMutations: [{ rangeHash: "range-u64", present: true }],
});

const candidate = (
	hash: string,
	properties?: { coordinates?: number[]; boundary?: boolean },
): RebalanceScanCandidate<"u32"> => ({
	hash,
	coordinates: properties?.coordinates ?? [15],
	assignedToRangeBoundary: properties?.boundary ?? true,
});

const bytesFor = (candidates: readonly RebalanceScanCandidate[]) =>
	candidates.reduce(
		(total, value) =>
			total + value.hash.length + 1 + value.coordinates.length * 4,
		0,
	);

const bucket = (
	hashNumber: number,
	candidates: readonly RebalanceScanCandidate<"u32">[],
): RebalanceScanSourceResult => ({
	resolution: "u32",
	eof: false,
	hashNumber,
	candidates,
	visited: candidates.length,
	results: candidates.length,
	bytes: bytesFor(candidates),
});

const eof = (): RebalanceScanSourceResult => ({
	resolution: "u32",
	eof: true,
	candidates: [],
	visited: 0,
	results: 0,
	bytes: 0,
});

class QueuedSource implements BoundedRebalanceScanSource {
	readonly calls: Array<
		Parameters<BoundedRebalanceScanSource["readNextCollisionBucket"]>[0]
	> = [];

	constructor(
		readonly queue: Array<
			| RebalanceScanSourceResult
			| Error
			| (() => Promise<RebalanceScanSourceResult>)
		>,
	) {}

	async readNextCollisionBucket(properties: RebalanceScanSourceRequest) {
		this.calls.push(properties);
		const next = this.queue.shift();
		if (!next) throw new Error("No scripted source result");
		if (next instanceof Error) throw next;
		return typeof next === "function" ? await next() : next;
	}
}

const openStore = (
	persistence: MemoryPersistence,
	durability: "strict" | "memory" = "strict",
) =>
	RebalanceWorkStore.open({
		persistence,
		durability,
	});

const install = async (
	store: RebalanceWorkStore,
	value = plan(),
	view = digest("a"),
) =>
	store.install(0n, {
		resolution: "u32",
		viewId: view,
		plan: value,
	});

const installU64 = async (store: RebalanceWorkStore) =>
	store.install(0n, {
		resolution: "u64",
		viewId: digest("d"),
		plan: planU64(),
	});

const alwaysCurrent = () => true;

describe("rebalance scan executor", () => {
	it("freezes, visits, advances, and completes one durable phase per tick", async () => {
		const persistence = new MemoryPersistence();
		const store = await openStore(persistence);
		await install(store);
		const source = new QueuedSource([
			bucket(7, [
				candidate("geometry", { boundary: false }),
				candidate("boundary-b"),
				candidate("boundary-a"),
			]),
			eof(),
		]);
		const visits: RebalanceVisitKey[] = [];
		const executor = new RebalanceScanExecutor({
			store,
			source,
			viewGuard: alwaysCurrent,
			visit: ({ key }) => {
				visits.push(key);
				return { bytes: 2 };
			},
		});

		const frozen = await executor.tick();
		expect(frozen.status).to.equal("bucket-frozen");
		if (frozen.status !== "bucket-frozen") throw new Error("unexpected");
		expect(frozen.kept).to.equal(2);
		expect(source.calls[0].task).to.deep.include({
			kind: "boundary",
			ordinal: 0,
		});
		expect(source.calls[0].excludeBoundary).to.equal(false);
		expect(frozen.snapshot.active?.cursor.bucket).to.deep.equal({
			hashNumber: 7,
			hashes: ["boundary-a", "boundary-b"],
			nextIndex: 0,
		});
		expect(frozen.durableCommit).to.not.equal(undefined);

		const processed = await executor.tick();
		expect(processed).to.deep.include({
			status: "bucket-processed",
			processed: 2,
			bytes: 4,
		});
		expect(visits.map((value) => value.hash)).to.deep.equal([
			"boundary-a",
			"boundary-b",
		]);
		expect(visits[0]).to.deep.include({
			viewId: digest("a"),
			taskOrdinal: 0,
			hashNumber: 7,
		});

		expect((await executor.tick()).status).to.equal("bucket-advanced");
		const advanced = await executor.tick();
		expect(advanced.status).to.equal("task-advanced");
		expect(advanced.snapshot.active?.cursor).to.deep.equal({
			taskOrdinal: 1,
			afterHashNumber: undefined,
			bucket: undefined,
		});
		const revision = advanced.snapshot.revision;
		const complete = await executor.tick();
		expect(complete).to.deep.equal({
			status: "complete",
			snapshot: advanced.snapshot,
		});
		expect(store.snapshot().revision).to.equal(revision);
		// Completion deliberately leaves the immutable plan/history available to a
		// future, separately fenced finalizer.
		expect(store.snapshot().active).to.not.equal(undefined);
		await store.close();
	});

	it("freezes an empty task-owned bucket instead of mistaking it for EOF", async () => {
		const store = await openStore(new MemoryPersistence());
		await install(store);
		const source = new QueuedSource([
			bucket(1, [candidate("not-boundary", { boundary: false })]),
		]);
		let visits = 0;
		const executor = new RebalanceScanExecutor({
			store,
			source,
			viewGuard: alwaysCurrent,
			visit: () => {
				visits++;
				return { bytes: 0 };
			},
		});
		const frozen = await executor.tick();
		expect(frozen.status).to.equal("bucket-frozen");
		expect(frozen.snapshot.active?.cursor.bucket?.hashes).to.deep.equal([]);
		expect((await executor.tick()).status).to.equal("bucket-advanced");
		expect(visits).to.equal(0);
		await store.close();
	});

	it("applies boundary and global earliest-geometry ownership in TypeScript", async () => {
		const store = await openStore(new MemoryPersistence());
		await install(store, plan({ boundary: true, geometry: true }));
		const source = new QueuedSource([
			bucket(2, [
				candidate("boundary"),
				candidate("geometry", { boundary: false }),
			]),
			eof(),
			bucket(2, [
				candidate("boundary"),
				candidate("geometry", { boundary: false }),
			]),
		]);
		const executor = new RebalanceScanExecutor({
			store,
			source,
			viewGuard: alwaysCurrent,
			visit: () => ({ bytes: 0 }),
		});

		let result = await executor.tick();
		expect(result.snapshot.active?.cursor.bucket?.hashes).to.deep.equal([
			"boundary",
		]);
		await executor.tick();
		await executor.tick();
		await executor.tick();
		result = await executor.tick();
		expect(result.status).to.equal("bucket-frozen");
		expect(result.snapshot.active?.cursor.bucket?.hashes).to.deep.equal([
			"geometry",
		]);
		await store.close();
	});

	it("rejects source cap, completeness, ordering, and byte-accounting violations without checkpointing", async () => {
		const invalid: RebalanceScanSourceResult[] = [
			{ ...bucket(1, [candidate("a"), candidate("b")]), results: 2 },
			{ ...bucket(1, [candidate("a"), candidate("a")]) },
			{ ...bucket(1, [candidate("a")]), bytes: 0 },
			bucket(1, []),
		];
		for (let index = 0; index < invalid.length; index++) {
			const store = await openStore(new MemoryPersistence());
			const installed = await install(store);
			const executor = new RebalanceScanExecutor({
				store,
				source: new QueuedSource([invalid[index]]),
				viewGuard: alwaysCurrent,
				visit: () => ({ bytes: 0 }),
				limits: index === 0 ? { maxSourceResults: 1 } : undefined,
			});
			await expect(executor.tick()).to.be.rejected;
			expect(store.snapshot()).to.deep.equal(installed.snapshot);
			await store.close();
		}

		const store = await openStore(new MemoryPersistence());
		const installed = await install(store);
		const fence = installed.snapshot.active!;
		await store.checkpoint(
			{
				viewId: fence.viewId,
				planDigest: fence.planDigest,
				installSequence: fence.installSequence,
			},
			installed.snapshot.revision,
			{ taskOrdinal: 0, bucket: { hashNumber: 5, hashes: [], nextIndex: 0 } },
		);
		await store.checkpoint(
			{
				viewId: fence.viewId,
				planDigest: fence.planDigest,
				installSequence: fence.installSequence,
			},
			store.snapshot().revision,
			{ taskOrdinal: 0, afterHashNumber: 5 },
		);
		const before = store.snapshot();
		const executor = new RebalanceScanExecutor({
			store,
			source: new QueuedSource([bucket(5, [candidate("same")])]),
			viewGuard: alwaysCurrent,
			visit: () => ({ bytes: 0 }),
		});
		await expect(executor.tick()).to.be.rejectedWith(
			"did not advance its hash number",
		);
		expect(store.snapshot()).to.deep.equal(before);
		await store.close();
	});

	it("retries source and at-least-once visitor failures from the same cursor", async () => {
		const store = await openStore(new MemoryPersistence());
		await install(store);
		const source = new QueuedSource([
			new Error("source failed"),
			bucket(3, [candidate("hash")]),
		]);
		const keys: RebalanceVisitKey[] = [];
		let failVisit = true;
		const executor = new RebalanceScanExecutor({
			store,
			source,
			viewGuard: alwaysCurrent,
			visit: ({ key }) => {
				keys.push(key);
				if (failVisit) {
					failVisit = false;
					throw new Error("visitor failed after its idempotent effect");
				}
				return { bytes: 1 };
			},
		});
		const before = store.snapshot().revision;
		await expect(executor.tick()).to.be.rejectedWith("source failed");
		expect(store.snapshot().revision).to.equal(before);
		expect((await executor.tick()).status).to.equal("bucket-frozen");
		const frozenRevision = store.snapshot().revision;
		await expect(executor.tick()).to.be.rejectedWith("visitor failed");
		expect(store.snapshot().revision).to.equal(frozenRevision);
		expect((await executor.tick()).status).to.equal("bucket-processed");
		expect(keys).to.have.length(2);
		expect(keys[1]).to.deep.equal(keys[0]);
		await store.close();
	});

	it("enforces visited, byte, identifier, and coordinate caps before checkpointing", async () => {
		const cases: Array<{
			result: RebalanceScanSourceResult;
			limits: ConstructorParameters<typeof RebalanceScanExecutor>[0]["limits"];
		}> = [
			{
				result: { ...bucket(1, [candidate("a")]), visited: 1026 },
				limits: {},
			},
			{
				result: bucket(1, [candidate("ab")]),
				limits: { maxIdentifierBytes: 1 },
			},
			{
				result: bucket(1, [candidate("a", { coordinates: [1, 2] })]),
				limits: { maxCoordinateValues: 1 },
			},
			{
				result: bucket(1, [candidate("a")]),
				limits: { maxSourceBytes: 5 },
			},
		];
		for (const value of cases) {
			const store = await openStore(new MemoryPersistence());
			const installed = await install(store);
			const executor = new RebalanceScanExecutor({
				store,
				source: new QueuedSource([value.result]),
				viewGuard: alwaysCurrent,
				visit: () => ({ bytes: 0 }),
				limits: value.limits,
			});
			await expect(executor.tick()).to.be.rejected;
			expect(store.snapshot()).to.deep.equal(installed.snapshot);
			await store.close();
		}
	});

	it("replays the same visitor key after restart before a checkpoint", async () => {
		const persistence = new MemoryPersistence();
		let store = await openStore(persistence);
		await install(store);
		const source = new QueuedSource([bucket(9, [candidate("restart")])]);
		const keys: RebalanceVisitKey[] = [];
		let fail = true;
		let executor = new RebalanceScanExecutor({
			store,
			source,
			viewGuard: alwaysCurrent,
			visit: ({ key }) => {
				keys.push(key);
				if (fail) {
					fail = false;
					throw new Error("crash boundary");
				}
				return { bytes: 0 };
			},
		});
		await executor.tick();
		await expect(executor.tick()).to.be.rejectedWith("crash boundary");
		await store.close();
		store = await openStore(persistence.fork());
		executor = new RebalanceScanExecutor({
			store,
			source: new QueuedSource([eof()]),
			viewGuard: alwaysCurrent,
			visit: ({ key }) => {
				keys.push(key);
				return { bytes: 0 };
			},
		});
		await executor.tick();
		expect(keys).to.have.length(2);
		expect(keys[1]).to.deep.equal(keys[0]);
		await store.close();
	});

	it("recovers an ambiguous visitor checkpoint through the durable store", async () => {
		const persistence = new MemoryPersistence();
		let store = await openStore(persistence);
		await install(store);
		const keys: string[] = [];
		let executor = new RebalanceScanExecutor({
			store,
			source: new QueuedSource([bucket(9, [candidate("barrier")])]),
			viewGuard: alwaysCurrent,
			visit: ({ idempotenceKey }) => {
				keys.push(idempotenceKey);
				return { bytes: 0 };
			},
		});
		expect((await executor.tick()).status).to.equal("bucket-frozen");
		persistence.failNextBarrier(new Error("ambiguous barrier"));
		await expect(executor.tick()).to.be.rejectedWith(
			"Failed to persist rebalance work frame",
		);
		expect(keys).to.have.length(1);
		await expect(store.close()).to.be.rejectedWith(
			"Rebalance work store is poisoned",
		);

		store = await openStore(persistence.fork());
		executor = new RebalanceScanExecutor({
			store,
			source: new QueuedSource([eof()]),
			viewGuard: alwaysCurrent,
			visit: ({ idempotenceKey }) => {
				keys.push(idempotenceKey);
				return { bytes: 0 };
			},
		});
		expect((await executor.tick()).status).to.equal("bucket-advanced");
		expect((await executor.tick()).status).to.equal("task-advanced");
		expect((await executor.tick()).status).to.equal("complete");
		expect(keys).to.have.length(1);
		await store.close();
	});

	it("checks view and cancellation gates around asynchronous work", async () => {
		const store = await openStore(new MemoryPersistence());
		const installed = await install(store);
		let guardCalls = 0;
		const executor = new RebalanceScanExecutor({
			store,
			source: new QueuedSource([bucket(1, [candidate("a")])]),
			viewGuard: () => ++guardCalls < 3,
			visit: () => ({ bytes: 0 }),
		});
		await expect(executor.tick()).to.be.rejectedWith(
			"view is no longer current",
		);
		expect(store.snapshot()).to.deep.equal(installed.snapshot);

		const controller = new AbortController();
		controller.abort();
		const source = new QueuedSource([bucket(2, [candidate("b")])]);
		const aborted = new RebalanceScanExecutor({
			store,
			source,
			viewGuard: alwaysCurrent,
			visit: () => ({ bytes: 0 }),
		});
		await expect(
			aborted.tick({ signal: controller.signal }),
		).to.be.rejected.then((error: Error) =>
			expect(error.name).to.equal("AbortError"),
		);
		expect(source.calls).to.have.length(0);
		expect(store.snapshot()).to.deep.equal(installed.snapshot);
		await store.close();
	});

	it("rejects stale work when a source await supersedes its plan", async () => {
		const store = await openStore(new MemoryPersistence());
		const installed = await install(store);
		const currentFence = () => {
			const active = store.snapshot().active;
			return active
				? `${active.viewId}:${active.planDigest}:${active.installSequence}`
				: "";
		};
		const executor = new RebalanceScanExecutor({
			store,
			source: {
				readNextCollisionBucket: async () => {
					await store.install(installed.snapshot.revision, {
						resolution: "u32",
						viewId: digest("b"),
						plan: plan(),
					});
					throw new Error("old source failed too");
				},
			},
			viewGuard: (fence) =>
				currentFence() ===
				`${fence.viewId}:${fence.planDigest}:${fence.installSequence}`,
			visit: () => ({ bytes: 0 }),
		});
		let error: unknown;
		try {
			await executor.tick();
		} catch (caught) {
			error = caught;
		}
		expect(error).to.be.instanceOf(AggregateError);
		expect((error as AggregateError).errors[0]).to.have.property(
			"message",
			"old source failed too",
		);
		expect(store.snapshot().active?.viewId).to.equal(digest("b"));
		expect(store.snapshot().active?.cursor.taskOrdinal).to.equal(0);
		expect(store.snapshot().active?.cursor.bucket).to.equal(undefined);
		await store.close();
	});

	it("passes and enforces a cooperative per-tick deadline", async () => {
		const store = await openStore(new MemoryPersistence());
		const installed = await install(store);
		let now = 0;
		let observedDeadline: number | undefined;
		const executor = new RebalanceScanExecutor({
			store,
			source: {
				readNextCollisionBucket: (properties) => {
					observedDeadline = properties.deadline;
					now = 11;
					return bucket(1, [candidate("late")]);
				},
			},
			viewGuard: alwaysCurrent,
			visit: () => ({ bytes: 0 }),
			limits: { maxTickMs: 10 },
			now: () => now,
		});
		await expect(executor.tick()).to.be.rejectedWith("exceeded its deadline");
		expect(observedDeadline).to.equal(10);
		expect(store.snapshot()).to.deep.equal(installed.snapshot);
		await store.close();
	});

	it("captures collaborators at construction instead of trusting later mutation", async () => {
		const store = await openStore(new MemoryPersistence());
		await install(store);
		const source = new QueuedSource([bucket(1, [candidate("captured")])]);
		const properties = {
			store,
			source: source as BoundedRebalanceScanSource,
			viewGuard: alwaysCurrent,
			visit: () => ({ bytes: 0 }),
		};
		const executor = new RebalanceScanExecutor(properties);
		properties.source = {
			readNextCollisionBucket: () => {
				throw new Error("mutated source");
			},
		};
		expect((await executor.tick()).status).to.equal("bucket-frozen");
		expect(source.calls).to.have.length(1);
		await store.close();
	});

	it("does not let a source mutate the task or fence used for validation", async () => {
		const store = await openStore(new MemoryPersistence());
		await install(store);
		const executor = new RebalanceScanExecutor({
			store,
			source: {
				readNextCollisionBucket: (request) => {
					(request.fence as { viewId: string }).viewId = digest("z");
					(request.task as { kind: string }).kind = "geometry";
					return bucket(1, [candidate("still-boundary")]);
				},
			},
			viewGuard: (fence) => fence.viewId === digest("a"),
			visit: () => ({ bytes: 0 }),
		});
		const result = await executor.tick();
		expect(result.status).to.equal("bucket-frozen");
		expect(result.snapshot.active?.cursor.bucket?.hashes).to.deep.equal([
			"still-boundary",
		]);
		await store.close();
	});

	it("single-flights concurrent ticks and keeps visit rows and bytes capped", async () => {
		const store = await openStore(new MemoryPersistence());
		await install(store);
		let release!: () => void;
		let entered!: () => void;
		const waiting = new Promise<void>((resolve) => {
			release = resolve;
		});
		const sourceEntered = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const source = new QueuedSource([
			async () => {
				entered();
				await waiting;
				return bucket(4, [candidate("a"), candidate("b")]);
			},
		]);
		const allowances: number[] = [];
		const executor = new RebalanceScanExecutor({
			store,
			source,
			viewGuard: alwaysCurrent,
			visit: ({ maxBytes }) => {
				allowances.push(maxBytes);
				return { bytes: maxBytes };
			},
			limits: { maxVisitRows: 2, maxVisitBytes: 3 },
		});
		const first = executor.tick();
		const second = executor.tick();
		await sourceEntered;
		expect(source.calls).to.have.length(1);
		release();
		expect((await first).status).to.equal("bucket-frozen");
		const processed = await second;
		expect(processed).to.deep.include({
			status: "bucket-processed",
			processed: 1,
			bytes: 3,
		});
		expect(allowances).to.deep.equal([3]);
		await store.close();
	});

	it("supports bounded ephemeral execution without manufacturing a durable commit", async () => {
		const store = await openStore(new MemoryPersistence(), "memory");
		await install(store);
		const executor = new RebalanceScanExecutor({
			store,
			source: new QueuedSource([bucket(1, [candidate("a")])]),
			viewGuard: alwaysCurrent,
			visit: () => ({ bytes: 0 }),
		});
		const result = await executor.tick();
		expect(result.status).to.equal("bucket-frozen");
		if (result.status !== "bucket-frozen") throw new Error("unexpected");
		expect(result.durableCommit).to.equal(undefined);
		await store.close();
	});

	it("keeps u64 source values correlated and emits a bigint-safe scalar visit key", async () => {
		const store = await openStore(new MemoryPersistence());
		const installed = await installU64(store);
		const invalid = {
			resolution: "u64",
			eof: false,
			hashNumber: 1,
			candidates: [
				{
					hash: "u64",
					coordinates: [1n],
					assignedToRangeBoundary: true,
				},
			],
			visited: 1,
			results: 1,
			bytes: 12,
		} as unknown as RebalanceScanSourceResult;
		let executor = new RebalanceScanExecutor({
			store,
			source: new QueuedSource([invalid]),
			viewGuard: alwaysCurrent,
			visit: () => ({ bytes: 0 }),
		});
		await expect(executor.tick()).to.be.rejectedWith(
			"Invalid rebalance source hash number",
		);
		expect(store.snapshot()).to.deep.equal(installed.snapshot);

		const idempotenceKeys: string[] = [];
		executor = new RebalanceScanExecutor({
			store,
			source: new QueuedSource([
				{
					resolution: "u64",
					eof: false,
					hashNumber: 1n,
					candidates: [
						{
							hash: "u64",
							coordinates: [1n],
							assignedToRangeBoundary: true,
						},
					],
					visited: 1,
					results: 1,
					bytes: 12,
				},
			]),
			viewGuard: alwaysCurrent,
			visit: ({ idempotenceKey }) => {
				idempotenceKeys.push(idempotenceKey);
				return { bytes: 0 };
			},
		});
		await executor.tick();
		await executor.tick();
		expect(JSON.parse(idempotenceKeys[0]).slice(0, 2)).to.deep.equal([
			"peerbit-shared-log-rebalance-visit-v1",
			digest("d"),
		]);
		expect(idempotenceKeys[0]).to.include('"bigint","1","u64"');
		await store.close();
	});

	it("matches a deterministic crash/retry model over small collision domains", async () => {
		const random = (() => {
			let state = 0x5eedc0de;
			return () => {
				state |= 0;
				state = (state + 0x6d2b79f5) | 0;
				let value = Math.imul(state ^ (state >>> 15), 1 | state);
				value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
				return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
			};
		})();
		const model = new Map<number, RebalanceScanCandidate<"u32">[]>();
		for (let number = 1; number <= 6; number++) {
			const rows: RebalanceScanCandidate<"u32">[] = [];
			const count = 1 + Math.floor(random() * 4);
			for (let row = 0; row < count; row++) {
				rows.push(candidate(`h-${number}-${row}`));
			}
			model.set(number, rows);
		}
		const expected = new Set(
			[...model.values()].flat().map((value) => value.hash),
		);
		const persistence = new MemoryPersistence();
		let store = await openStore(persistence);
		await install(store);
		const effects = new Set<string>();
		const attempts = new Map<string, number>();
		const failedOnce = new Set<string>();

		const makeSource = (): BoundedRebalanceScanSource => ({
			readNextCollisionBucket: ({ afterHashNumber }) => {
				const after = (afterHashNumber as number | undefined) ?? -1;
				const next = [...model.keys()].find((number) => number > after);
				return next == null ? eof() : bucket(next, model.get(next)!);
			},
		});
		const makeExecutor = () =>
			new RebalanceScanExecutor({
				store,
				source: makeSource(),
				viewGuard: alwaysCurrent,
				visit: ({ key }) => {
					attempts.set(key.hash, (attempts.get(key.hash) ?? 0) + 1);
					effects.add(key.hash);
					if (random() < 0.25 && !failedOnce.has(key.hash)) {
						failedOnce.add(key.hash);
						throw new Error("modeled crash after effect");
					}
					return { bytes: 1 };
				},
				limits: { maxVisitRows: 2 },
			});

		let executor = makeExecutor();
		let completed = false;
		for (let step = 0; step < 200 && !completed; step++) {
			try {
				completed = (await executor.tick()).status === "complete";
			} catch (error) {
				expect((error as Error).message).to.equal("modeled crash after effect");
			}
			if (!completed && random() < 0.2) {
				await store.close();
				store = await openStore(persistence.fork());
				executor = makeExecutor();
			}
		}
		expect(completed).to.equal(true);
		expect(effects).to.deep.equal(expected);
		for (const count of attempts.values()) expect(count).to.be.within(1, 2);
		expect(store.snapshot().active?.cursor.taskOrdinal).to.equal(1);
		await store.close();
	});
});
