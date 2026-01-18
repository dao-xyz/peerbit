import { Cache } from "@peerbit/cache";
import { ready as ribltReady } from "@peerbit/riblt";
import { Bench } from "tinybench";
import { createNumbers, type Numbers } from "../src/integers.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";

// Benchmarks sender-side StartSync setup cost (sort + encoder build + initial symbols),
// by timing `RatelessIBLTSynchronizer.onMaybeMissingEntries()` for large batches.
//
// Run with:
//   cd packages/programs/data/shared-log
//   RIBLT_SIZES="1000,10000,50000" RIBLT_WARMUP=5 RIBLT_ITERATIONS=20 \\
//     node --loader ts-node/esm ./benchmark/rateless-iblt-sender-startsync.ts

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

const createEntries = <R extends "u32" | "u64">(size: number) => {
	const entries = new Map<string, any>();
	for (let i = 0; i < size; i++) {
		const hash = `h${i}`;
		entries.set(hash, {
			hash,
			hashNumber: BigInt(i + 1),
			assignedToRangeBoundary: false,
		});
	}
	return entries as Map<string, any>;
};

const createSync = <R extends "u32" | "u64">(numbers: Numbers<R>) => {
	const send = async () => {};
	const rpc = { send } as any;

	return new RatelessIBLTSynchronizer<R>({
		rpc,
		rangeIndex: {} as any,
		entryIndex: {} as any,
		log: {} as any,
		coordinateToHash: new Cache<string>({ max: 10 }),
		numbers,
	});
};

const clearOutgoing = async (sync: RatelessIBLTSynchronizer<any>) => {
	const ids = Array.from(sync.outgoingSyncProcesses.keys());
	for (const id of ids) {
		sync.outgoingSyncProcesses.get(id)?.free();
	}
};

await ribltReady;

const numbers = createNumbers("u64") as Numbers<"u64">;

const suite = new Bench({
	name: "rateless-iblt-sender-startsync",
	warmupIterations: Number.isFinite(warmupIterations) ? warmupIterations : 0,
	iterations: Number.isFinite(iterations) ? iterations : undefined,
});

const sync = createSync(numbers);

for (const size of sizes) {
	const entries = createEntries(size);

	suite.add(`onMaybeMissingEntries (rateless IBLT, n=${size})`, async () => {
		await clearOutgoing(sync);
		await sync.onMaybeMissingEntries({ entries, targets: ["t"] });
		await clearOutgoing(sync);
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

await sync.close();

