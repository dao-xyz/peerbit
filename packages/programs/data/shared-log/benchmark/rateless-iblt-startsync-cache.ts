import { Cache } from "@peerbit/cache";
import { ready as ribltReady } from "@peerbit/riblt";
import { Bench } from "tinybench";
import { createNumbers, type Numbers } from "../src/integers.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";

// Run with:
//   cd packages/programs/data/shared-log
//   RIBLT_SIZES="1000,10000,50000" RIBLT_WARMUP=5 RIBLT_ITERATIONS=20 \
//     node --loader ts-node/esm ./benchmark/rateless-iblt-startsync-cache.ts

const parseNumberList = (value: string | undefined, defaults: number[]) => {
	if (!value) return defaults;
	const parsed = value
		.split(",")
		.map((x) => Number.parseInt(x.trim(), 10))
		.filter((x) => Number.isFinite(x) && x > 0);
	return parsed.length > 0 ? parsed : defaults;
};

const sizes = parseNumberList(process.env.RIBLT_SIZES, [1_000, 10_000, 50_000]);
const warmupIterations = Number.parseInt(process.env.RIBLT_WARMUP || "5", 10);
const iterations = Number.parseInt(process.env.RIBLT_ITERATIONS || "20", 10);

const createEntries = (size: number) => {
	const entries: Array<{ value: { hash: string; hashNumber: bigint } }> =
		new Array(size);
	for (let i = 0; i < size; i++) {
		entries[i] = { value: { hash: `h${i}`, hashNumber: BigInt(i + 1) } };
	}
	return entries;
};

const createSync = (
	entries: Array<{ value: { hash: string; hashNumber: bigint } }>,
	numbers: Numbers<"u64">,
) => {
	const entryIndex = {
		iterate: () => ({
			all: async () => entries,
		}),
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

const suite = new Bench({
	name: "rateless-iblt-startsync-cache",
	warmupIterations: Number.isFinite(warmupIterations) ? warmupIterations : 0,
	iterations: Number.isFinite(iterations) ? iterations : undefined,
});

const syncsToClose: Array<() => Promise<void>> = [];

for (const size of sizes) {
	const entries = createEntries(size);
	const range = {
		start1: 0n,
		end1: numbers.maxValue,
		start2: 0n,
		end2: 0n,
	};

	const coldSync = createSync(entries, numbers);
	const warmSync = createSync(entries, numbers);

	const warmDecoder = await (warmSync as any).getLocalDecoderForRange(range);
	if (warmDecoder) {
		warmDecoder.free();
	}

	syncsToClose.push(() => coldSync.close() as Promise<void>);
	syncsToClose.push(() => warmSync.close() as Promise<void>);

	suite.add(`StartSync local decoder (cold, n=${size})`, async () => {
		(coldSync as any).invalidateLocalRangeEncoderCache();
		const decoder = await (coldSync as any).getLocalDecoderForRange(range);
		if (decoder) {
			decoder.free();
		}
	});

	suite.add(`StartSync local decoder (warm, n=${size})`, async () => {
		const decoder = await (warmSync as any).getLocalDecoderForRange(range);
		if (decoder) {
			decoder.free();
		}
	});

	suite.add(`StartSync local decoder (after invalidation, n=${size})`, async () => {
		(warmSync as any).invalidateLocalRangeEncoderCache();
		const decoder = await (warmSync as any).getLocalDecoderForRange(range);
		if (decoder) {
			decoder.free();
		}
	});
}

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
				meta: { sizes, warmupIterations, iterations },
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
