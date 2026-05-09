import { create as createIndex } from "@peerbit/indexer-sqlite3";
import { performance } from "node:perf_hooks";
import { createRangePlanner } from "../src/index.js";

type Resolution = "u32" | "u64";
type BenchResult = {
	scenario: string;
	tsOpsPerSecond: string;
	tsMeanMs: string;
	nativeOpsPerSecond: string;
	nativeMeanMs: string;
	speedup: string;
};
type RangeLike = {
	idString: string;
	hash: string;
	timestamp: bigint;
	start1: number | bigint;
	end1: number | bigint;
	start2: number | bigint;
	end2: number | bigint;
	width: number | bigint;
	mode: number;
};

const loadSharedLog = async () => {
	const integersPath = "../../../dist/src/integers.js";
	const rangesPath = "../../../dist/src/ranges.js";
	const [integers, ranges] = await Promise.all([
		import(integersPath),
		import(rangesPath),
	]);
	return {
		createNumbers: integers.createNumbers,
		denormalizer: integers.denormalizer,
		ReplicationRangeIndexableU32: ranges.ReplicationRangeIndexableU32,
		ReplicationRangeIndexableU64: ranges.ReplicationRangeIndexableU64,
		getSamples: ranges.getSamples,
	};
};

const format = (value: number) =>
	value.toLocaleString("en-US", { maximumFractionDigits: 1 });

const deterministicId = (id: number) => {
	const bytes = new Uint8Array(32);
	new DataView(bytes.buffer).setUint32(28, id, false);
	return bytes;
};

const toNativeRange = (range: RangeLike) => ({
	id: range.idString,
	hash: range.hash,
	timestamp: range.timestamp,
	start1: range.start1,
	end1: range.end1,
	start2: range.start2,
	end2: range.end2,
	width: range.width,
	mode: range.mode,
});

const createRandom = (seed: number) => {
	let state = seed;
	return () => {
		state = (1664525 * state + 1013904223) >>> 0;
		return state / 0x100000000;
	};
};

const benchmark = async (iterations: number, fn: () => Promise<void> | void) => {
	const warmup = Math.min(50, Math.max(5, Math.floor(iterations / 10)));
	for (let i = 0; i < warmup; i++) {
		await fn();
	}

	const start = performance.now();
	for (let i = 0; i < iterations; i++) {
		await fn();
	}
	const elapsedMs = performance.now() - start;
	return {
		meanMs: elapsedMs / iterations,
		opsPerSecond: iterations / (elapsedMs / 1000),
	};
};

const runResolution = async (resolution: Resolution): Promise<BenchResult[]> => {
	const sharedLog = await loadSharedLog();
	const numbers = sharedLog.createNumbers(resolution);
	const denormalize = sharedLog.denormalizer(resolution);
	const RangeClass =
		resolution === "u32"
			? sharedLog.ReplicationRangeIndexableU32
			: sharedLog.ReplicationRangeIndexableU64;
	const peerHashes = ["peer-a", "peer-b", "peer-c"];

	const makeRange = (
		id: number,
		hash: string,
		width: number,
		offset: number,
	): RangeLike =>
		new RangeClass({
			id: deterministicId(id),
			publicKeyHash: hash,
			width: denormalize(width),
			offset: denormalize(offset % 1),
			timestamp: 0n,
		});

	const sparseRanges = () => {
		const out: RangeLike[] = [];
		const random = createRandom(987654321);
		let id = 1;
		for (let i = 0; i < 10_000; i++) {
			out.push(makeRange(id++, peerHashes[0], 0.2 / 10_000, random()));
			out.push(makeRange(id++, peerHashes[1], 0.4 / 10_000, random()));
			out.push(makeRange(id++, peerHashes[2], 0.6 / 10_000, random()));
			out.push(makeRange(id++, peerHashes[2], 0.6 / 10_000, random()));
		}
		return out;
	};

	const overlapRanges = () => {
		const out = [
			makeRange(1, peerHashes[0], 1, 0.1),
			makeRange(2, peerHashes[1], 1, 0.7),
		];
		const random = createRandom(123456789);
		let id = 3;
		for (let i = 0; i < 10_000; i++) {
			out.push(makeRange(id++, peerHashes[0], 0.2 / 10_000, random()));
			out.push(makeRange(id++, peerHashes[1], 0.4 / 10_000, random()));
			out.push(makeRange(id++, peerHashes[2], 0.6 / 10_000, random()));
			out.push(makeRange(id++, peerHashes[2], 0.6 / 10_000, random()));
		}
		return out;
	};

	const scenarios = [
		{
			name: "one full range",
			ranges: () => [makeRange(1, peerHashes[0], 1, 0.1)],
			iterations: 1_000,
			options: undefined,
		},
		{
			name: "one full range with unique filter",
			ranges: () => [makeRange(1, peerHashes[0], 1, 0.1)],
			iterations: 1_000,
			options: { uniqueReplicators: new Set([peerHashes[0]]) },
		},
		{
			name: "overlap onlyIntersecting with 40k sparse noise",
			ranges: overlapRanges,
			iterations: 200,
			options: { onlyIntersecting: true },
		},
		{
			name: "sparse fallback with 40k ranges",
			ranges: sparseRanges,
			iterations: 100,
			options: undefined,
		},
	];

	const rows: BenchResult[] = [];
	for (const scenario of scenarios) {
		const ranges = scenario.ranges();
		const indices = await createIndex();
		const index = await indices.init({ schema: RangeClass });
		await indices.start();
		const planner = await createRangePlanner(resolution);

		for (const range of ranges) {
			await index.put(range);
			planner.put(toNativeRange(range));
		}

		const makeCursors = () => {
			const random = createRandom(246813579);
			return () => numbers.getGrid(numbers.denormalize(random()), 2);
		};

		try {
			let cursors = makeCursors();
			const ts = await benchmark(scenario.iterations, async () => {
				const samples = await sharedLog.getSamples(
					cursors(),
					index,
					0,
					numbers,
					scenario.options,
				);
				if (samples.size === 0) {
					throw new Error("Expected TypeScript samples");
				}
			});

			cursors = makeCursors();
			const native = await benchmark(scenario.iterations, () => {
				const samples = planner.getSamples(cursors(), {
					now: Date.now(),
					roleAge: 0,
					...scenario.options,
				});
				if (samples.size === 0) {
					throw new Error("Expected native samples");
				}
			});

			rows.push({
				scenario: `${scenario.name} (${resolution}, ${ranges.length} ranges)`,
				tsOpsPerSecond: format(ts.opsPerSecond),
				tsMeanMs: format(ts.meanMs),
				nativeOpsPerSecond: format(native.opsPerSecond),
				nativeMeanMs: format(native.meanMs),
				speedup: `${format(native.opsPerSecond / ts.opsPerSecond)}x`,
			});
		} finally {
			await indices.stop();
		}
	}

	return rows;
};

const rows = [
	...(await runResolution("u32")),
	...(await runResolution("u64")),
];

console.table(rows);
