import { Cache } from "@peerbit/cache";
import { ready as ribltReady } from "@peerbit/riblt";
import { Bench } from "tinybench";
import { type Numbers, createNumbers } from "../src/integers.js";
import {
	RatelessIBLTSynchronizer,
	RequestAll,
	StartSync,
} from "../src/sync/rateless-iblt.js";

// Run with:
//   cd packages/programs/data/shared-log
//   RIBLT_SIZES="16384,16385" RIBLT_WARMUP=5 RIBLT_ITERATIONS=20 \
//     node --loader ts-node/esm ./benchmark/rateless-iblt-startsync-cache.ts

const DEFAULT_MAX_RATELESS_RECEIVE_RANGE_ENTRIES = 16_384;
const DEFAULT_RATELESS_OVERFLOW_SENTINEL =
	DEFAULT_MAX_RATELESS_RECEIVE_RANGE_ENTRIES + 1;
const CONCURRENT_OVERFLOW_ATTEMPTS = 33;
const MAX_CONCURRENT_RATELESS_ADMISSIONS = 32;

const parseNumberList = (value: string | undefined, defaults: number[]) => {
	if (!value) return defaults;
	const parsed = value
		.split(",")
		.map((x) => Number.parseInt(x.trim(), 10))
		.filter((x) => Number.isFinite(x) && x > 0);
	return parsed.length > 0 ? parsed : defaults;
};

const sizes = parseNumberList(process.env.RIBLT_SIZES, [
	DEFAULT_MAX_RATELESS_RECEIVE_RANGE_ENTRIES,
	DEFAULT_RATELESS_OVERFLOW_SENTINEL,
]);
const warmupIterations = Number.parseInt(process.env.RIBLT_WARMUP || "5", 10);
const iterations = Number.parseInt(process.env.RIBLT_ITERATIONS || "20", 10);

type IndexedHashNumber = {
	value: { hash: string; hashNumber: bigint };
};

const createEntries = (size: number): IndexedHashNumber[] => {
	const entries = new Array<IndexedHashNumber>(size);
	for (let i = 0; i < size; i++) {
		entries[i] = { value: { hash: `h${i}`, hashNumber: BigInt(i + 1) } };
	}
	return entries;
};

const createSync = (entries: IndexedHashNumber[], numbers: Numbers<"u64">) => {
	const entryIndex = {
		iterate: () => {
			let offset = 0;
			let closed = false;
			return {
				all: async () => {
					throw new Error("bounded range benchmark must not call all()");
				},
				next: async (amount: number) => {
					if (closed || offset >= entries.length) {
						return [];
					}
					const wanted = Number.isFinite(amount)
						? Math.max(0, Math.floor(amount))
						: entries.length - offset;
					const end = Math.min(entries.length, offset + wanted);
					const page = entries.slice(offset, end);
					offset = end;
					return page;
				},
				done: () => closed || offset >= entries.length,
				pending: async () => (closed ? 0 : entries.length - offset),
				close: async () => {
					closed = true;
				},
			};
		},
	} as any;

	const send = async () => {};
	const rpc = { send } as any;

	return new RatelessIBLTSynchronizer<"u64">({
		rpc,
		rangeIndex: {} as any,
		entryIndex,
		log: {} as any,
		coordinateToHash: new Cache<string>({ max: 10 }),
		numbers,
	});
};

await ribltReady;

const numbers = createNumbers("u64") as Numbers<"u64">;

function assertBenchmarkInvariant(
	condition: boolean,
	message: string,
): asserts condition {
	if (!condition) {
		throw new Error(`benchmark invariant failed: ${message}`);
	}
}

const assertExpectedDecoderAndFree = (
	decoder: any,
	size: number,
	phase: string,
) => {
	if (size > DEFAULT_MAX_RATELESS_RECEIVE_RANGE_ENTRIES) {
		assertBenchmarkInvariant(
			decoder === false,
			`${phase} returns false above the receive-range cap (n=${size})`,
		);
		return;
	}
	assertBenchmarkInvariant(
		decoder != null && decoder !== false,
		`${phase} produces a decoder at or below the receive-range cap (n=${size})`,
	);
	decoder.free();
};

type ConcurrentOverflowRun = {
	resolverCalls: number;
	sendCalls: number;
	activeResolverCalls: number;
	peakActiveResolverCalls: number;
	totalEntriesAllocated: number;
	totalLogicalBytesAllocated: number;
	memoryBeforeAdmissions: MemoryObservation;
	memoryAtAdmissionPeak?: MemoryObservation;
	admissionsReady: Promise<void>;
	markAdmissionsReady: () => void;
	releaseResolvers: Promise<void>;
	markResolversReleased: () => void;
};

type MemoryObservation = {
	rssBytes: number;
	heapUsedBytes: number;
	arrayBuffersBytes: number;
};

type ConcurrentOverflowObservation = {
	peakActiveResolverCalls: number;
	totalEntriesAllocated: number;
	totalLogicalBytesAllocated: number;
	memoryBeforeAdmissions: MemoryObservation;
	memoryAtAdmissionPeak: MemoryObservation;
	memoryDeltaAtAdmissionPeak: MemoryObservation;
};

const captureMemoryObservation = (): MemoryObservation => {
	const memory = process.memoryUsage();
	return {
		rssBytes: memory.rss,
		heapUsedBytes: memory.heapUsed,
		arrayBuffersBytes: memory.arrayBuffers,
	};
};

const subtractMemoryObservations = (
	after: MemoryObservation,
	before: MemoryObservation,
): MemoryObservation => ({
	rssBytes: after.rssBytes - before.rssBytes,
	heapUsedBytes: after.heapUsedBytes - before.heapUsedBytes,
	arrayBuffersBytes: after.arrayBuffersBytes - before.arrayBuffersBytes,
});

const createConcurrentOverflowRun = (): ConcurrentOverflowRun => {
	let markAdmissionsReady!: () => void;
	let markResolversReleased!: () => void;
	return {
		resolverCalls: 0,
		sendCalls: 0,
		activeResolverCalls: 0,
		peakActiveResolverCalls: 0,
		totalEntriesAllocated: 0,
		totalLogicalBytesAllocated: 0,
		memoryBeforeAdmissions: captureMemoryObservation(),
		admissionsReady: new Promise<void>((resolve) => {
			markAdmissionsReady = resolve;
		}),
		markAdmissionsReady: () => markAdmissionsReady(),
		releaseResolvers: new Promise<void>((resolve) => {
			markResolversReleased = resolve;
		}),
		markResolversReleased: () => markResolversReleased(),
	};
};

let activeConcurrentOverflowRun: ConcurrentOverflowRun | undefined;
let lastConcurrentOverflowObservation:
	| ConcurrentOverflowObservation
	| undefined;
const concurrentOverflowSync = new RatelessIBLTSynchronizer<"u64">({
	rpc: {
		send: async (message: unknown) => {
			const run = activeConcurrentOverflowRun;
			assertBenchmarkInvariant(run != null, "fallback send has an active run");
			assertBenchmarkInvariant(
				message instanceof RequestAll,
				"overflow sends RequestAll",
			);
			run.sendCalls += 1;
		},
	} as any,
	rangeIndex: {} as any,
	entryIndex: {
		iterate: () => {
			throw new Error("concurrent overflow benchmark must use its resolver");
		},
	} as any,
	log: {} as any,
	coordinateToHash: new Cache<string>({ max: 10 }),
	numbers,
	resolveHashNumbersInRange: async ({ limit }) => {
		const run = activeConcurrentOverflowRun;
		assertBenchmarkInvariant(run != null, "resolver has an active run");
		assertBenchmarkInvariant(
			limit === DEFAULT_RATELESS_OVERFLOW_SENTINEL,
			`resolver limit is ${DEFAULT_RATELESS_OVERFLOW_SENTINEL}`,
		);
		const hashNumbers = new BigUint64Array(limit);
		run.resolverCalls += 1;
		run.activeResolverCalls += 1;
		run.peakActiveResolverCalls = Math.max(
			run.peakActiveResolverCalls,
			run.activeResolverCalls,
		);
		run.totalEntriesAllocated += hashNumbers.length;
		run.totalLogicalBytesAllocated += hashNumbers.byteLength;
		assertBenchmarkInvariant(
			run.activeResolverCalls <= MAX_CONCURRENT_RATELESS_ADMISSIONS,
			"resolver calls stay within the global admission bound",
		);
		if (run.activeResolverCalls === MAX_CONCURRENT_RATELESS_ADMISSIONS) {
			run.memoryAtAdmissionPeak = captureMemoryObservation();
			run.markAdmissionsReady();
		}
		try {
			await run.releaseResolvers;
			return hashNumbers;
		} finally {
			run.activeResolverCalls -= 1;
		}
	},
});

const concurrentPeers = Array.from(
	{ length: CONCURRENT_OVERFLOW_ATTEMPTS },
	(_, i) => ({ hashcode: () => `benchmark-peer-${i}` }),
);

const runConcurrentOverflow = async () => {
	const run = createConcurrentOverflowRun();
	const expectedTotalEntriesAllocated =
		MAX_CONCURRENT_RATELESS_ADMISSIONS * DEFAULT_RATELESS_OVERFLOW_SENTINEL;
	const expectedTotalLogicalBytesAllocated =
		expectedTotalEntriesAllocated * BigUint64Array.BYTES_PER_ELEMENT;
	activeConcurrentOverflowRun = run;
	const admitted = concurrentPeers
		.slice(0, MAX_CONCURRENT_RATELESS_ADMISSIONS)
		.map((from) =>
			concurrentOverflowSync.onMessage(
				new StartSync({ from: 0n, to: 20_000n, symbols: [] }),
				{ from } as any,
			),
		);

	try {
		await run.admissionsReady;
		assertBenchmarkInvariant(
			(concurrentOverflowSync as any).incomingRatelessProcessAdmissions.size ===
				MAX_CONCURRENT_RATELESS_ADMISSIONS,
			"32 initializations hold all logical admissions",
		);

		await concurrentOverflowSync.onMessage(
			new StartSync({ from: 0n, to: 20_000n, symbols: [] }),
			{ from: concurrentPeers[MAX_CONCURRENT_RATELESS_ADMISSIONS] } as any,
		);
		assertBenchmarkInvariant(
			run.resolverCalls === MAX_CONCURRENT_RATELESS_ADMISSIONS,
			"the 33rd attempt does not start another range probe",
		);
		assertBenchmarkInvariant(
			run.peakActiveResolverCalls === MAX_CONCURRENT_RATELESS_ADMISSIONS,
			"peak active resolver calls reaches the 32-admission bound",
		);
		assertBenchmarkInvariant(
			run.totalEntriesAllocated === expectedTotalEntriesAllocated,
			"typed-array entries are bounded to 32 cap-plus-one allocations",
		);
		assertBenchmarkInvariant(
			run.totalLogicalBytesAllocated === expectedTotalLogicalBytesAllocated,
			"logical typed-array bytes match 32 independent allocations",
		);
		assertBenchmarkInvariant(
			run.memoryAtAdmissionPeak != null,
			"memory is observed while all 32 resolver allocations are live",
		);
	} finally {
		run.markResolversReleased();
		await Promise.allSettled(admitted);
		activeConcurrentOverflowRun = undefined;
	}

	assertBenchmarkInvariant(
		run.sendCalls === MAX_CONCURRENT_RATELESS_ADMISSIONS,
		"each admitted overflow falls back exactly once",
	);
	assertBenchmarkInvariant(
		(concurrentOverflowSync as any).incomingRatelessProcessAdmissions.size ===
			0,
		"logical admissions are released after fallback",
	);
	assertBenchmarkInvariant(
		concurrentOverflowSync.ingoingSyncProcesses.size === 0,
		"incoming processes are released after fallback",
	);
	assertBenchmarkInvariant(
		run.activeResolverCalls === 0,
		"all admitted resolver calls are released after fallback",
	);
	assertBenchmarkInvariant(
		(concurrentOverflowSync as any).localRangeEncoderCache.size === 0,
		"overflow probes never populate the encoder cache",
	);

	const memoryAtAdmissionPeak = run.memoryAtAdmissionPeak;
	assertBenchmarkInvariant(
		memoryAtAdmissionPeak != null,
		"the admission-peak memory observation is available",
	);
	lastConcurrentOverflowObservation = {
		peakActiveResolverCalls: run.peakActiveResolverCalls,
		totalEntriesAllocated: run.totalEntriesAllocated,
		totalLogicalBytesAllocated: run.totalLogicalBytesAllocated,
		memoryBeforeAdmissions: run.memoryBeforeAdmissions,
		memoryAtAdmissionPeak,
		memoryDeltaAtAdmissionPeak: subtractMemoryObservations(
			memoryAtAdmissionPeak,
			run.memoryBeforeAdmissions,
		),
	};
};

const suite = new Bench({
	name: "rateless-iblt-startsync-cache",
	warmupIterations: Number.isFinite(warmupIterations) ? warmupIterations : 0,
	iterations: Number.isFinite(iterations) ? iterations : undefined,
});

const syncsToClose: Array<() => Promise<void>> = [];

syncsToClose.push(() => concurrentOverflowSync.close() as Promise<void>);

for (const size of sizes) {
	const entries = createEntries(size);
	const range = {
		start1: 0n,
		end1: numbers.maxValue,
		start2: 0n,
		end2: 0n,
	};

	const coldSync = createSync(entries, numbers);

	syncsToClose.push(() => coldSync.close() as Promise<void>);

	const coldTaskName =
		size > DEFAULT_MAX_RATELESS_RECEIVE_RANGE_ENTRIES
			? `StartSync local decoder overflow probe (n=${size}, cap=${DEFAULT_MAX_RATELESS_RECEIVE_RANGE_ENTRIES})`
			: `StartSync local decoder (cold, n=${size})`;
	suite.add(coldTaskName, async () => {
		(coldSync as any).invalidateLocalRangeEncoderCache();
		const decoder = await (coldSync as any).getLocalDecoderForRange(range);
		assertExpectedDecoderAndFree(decoder, size, "cold decoder lookup");
	});

	if (size > DEFAULT_MAX_RATELESS_RECEIVE_RANGE_ENTRIES) {
		continue;
	}

	const warmSync = createSync(entries, numbers);
	const warmDecoder = await (warmSync as any).getLocalDecoderForRange(range);
	assertExpectedDecoderAndFree(warmDecoder, size, "warm-cache prefill");
	syncsToClose.push(() => warmSync.close() as Promise<void>);

	suite.add(`StartSync local decoder (warm, n=${size})`, async () => {
		const decoder = await (warmSync as any).getLocalDecoderForRange(range);
		assertExpectedDecoderAndFree(decoder, size, "warm decoder lookup");
	});

	suite.add(
		`StartSync local decoder (after invalidation, n=${size})`,
		async () => {
			(warmSync as any).invalidateLocalRangeEncoderCache();
			const decoder = await (warmSync as any).getLocalDecoderForRange(range);
			assertExpectedDecoderAndFree(
				decoder,
				size,
				"post-invalidation decoder lookup",
			);
		},
	);
}

suite.add(
	`StartSync concurrent overflow admission + typed-array allocation (admitted=${MAX_CONCURRENT_RATELESS_ADMISSIONS}/${CONCURRENT_OVERFLOW_ATTEMPTS}, n=${DEFAULT_RATELESS_OVERFLOW_SENTINEL})`,
	runConcurrentOverflow,
);

await suite.run();
if (process.env.BENCH_JSON === "1") {
	const tasks = suite.tasks.map((task) => ({
		name: task.name,
		hz: task.result?.hz ?? null,
		mean_ms: task.result?.mean ?? null,
		rme: task.result?.rme ?? null,
		samples: task.result?.samples?.length ?? null,
	}));
	process.stdout.write(
		JSON.stringify(
			{
				name: suite.name,
				tasks,
				meta: {
					sizes,
					warmupIterations,
					iterations,
					defaultMaxRatelessReceiveRangeEntries:
						DEFAULT_MAX_RATELESS_RECEIVE_RANGE_ENTRIES,
					concurrentOverflow: {
						fixture:
							"synthetic per-resolver BigUint64Array allocation; excludes native index scan",
						attempts: CONCURRENT_OVERFLOW_ATTEMPTS,
						admitted: MAX_CONCURRENT_RATELESS_ADMISSIONS,
						entriesPerAllocation: DEFAULT_RATELESS_OVERFLOW_SENTINEL,
						logicalBytesPerAllocation:
							DEFAULT_RATELESS_OVERFLOW_SENTINEL *
							BigUint64Array.BYTES_PER_ELEMENT,
						maxTotalEntriesAllocated:
							MAX_CONCURRENT_RATELESS_ADMISSIONS *
							DEFAULT_RATELESS_OVERFLOW_SENTINEL,
						maxTotalLogicalBytesAllocated:
							MAX_CONCURRENT_RATELESS_ADMISSIONS *
							DEFAULT_RATELESS_OVERFLOW_SENTINEL *
							BigUint64Array.BYTES_PER_ELEMENT,
						memoryDeltasAreObservational: true,
						lastObservation: lastConcurrentOverflowObservation ?? null,
					},
				},
			},
			null,
			2,
		),
	);
} else {
	console.table(suite.table());
}

for (const closeSync of syncsToClose) {
	await closeSync();
}
