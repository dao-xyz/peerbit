import { randomBytes } from "@peerbit/crypto";
import { And, Compare, IntegerCompare, Or } from "@peerbit/indexer-interface";
import { create as createIndex } from "@peerbit/indexer-sqlite3";
import { LamportClock, Meta } from "@peerbit/log";
import { createSharedLogState } from "@peerbit/shared-log-rust";
import { Bench } from "tinybench";
import { EntryReplicatedU64 } from "../src/ranges.js";

// Run with:
//   cd packages/programs/data/shared-log
//   COORD_LOOKUP_ENTRIES=20000 COORD_LOOKUP_SYMBOLS=5000 COORD_LOOKUP_ITERATIONS=30 \
//     BENCH_JSON=1 node --loader ts-node/esm ./benchmark/native-coordinate-symbol-lookup.ts

const parsePositiveInt = (value: string | undefined, fallback: number) => {
	const parsed = Number.parseInt(value ?? "", 10);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const entryCount = parsePositiveInt(process.env.COORD_LOOKUP_ENTRIES, 20_000);
const symbolCount = parsePositiveInt(process.env.COORD_LOOKUP_SYMBOLS, 5_000);
const preflightSymbolCount = Math.min(
	symbolCount,
	parsePositiveInt(process.env.COORD_LOOKUP_PREFLIGHT_SYMBOLS, 500),
);
const warmupIterations = parsePositiveInt(process.env.COORD_LOOKUP_WARMUP, 5);
const iterations = parsePositiveInt(process.env.COORD_LOOKUP_ITERATIONS, 30);
const queryBatchSize = parsePositiveInt(process.env.COORD_LOOKUP_BATCH, 128);

const indices = await createIndex();
const entryIndex = await indices.init({ schema: EntryReplicatedU64 });
await indices.start();
const nativeState = await createSharedLogState("u64");

const meta = new Meta({
	clock: new LamportClock({ id: randomBytes(32) }),
	gid: "entry-gid",
	next: [],
	type: 0,
	data: undefined,
});

for (let i = 0; i < entryCount; i++) {
	const hashNumber = BigInt(i + 1);
	const entry = new EntryReplicatedU64({
		hash: `entry-${i}`,
		hashNumber,
		coordinates: [hashNumber],
		meta,
		assignedToRangeBoundary: false,
	});
	await entryIndex.put(entry);
	nativeState.putEntryCoordinates(
		entry.hash,
		entry.gid,
		entry.coordinates,
		false,
		1,
		entry.hashNumber,
	);
}

const symbols = Array.from(
	{ length: symbolCount },
	(_, i) => BigInt(((i * 17) % entryCount) + 1),
);
const preflightSymbols = symbols.slice(0, preflightSymbolCount);
const rangeEnd = BigInt(Math.floor(entryCount / 2) + 1);
const rangeHitCount = Math.floor(entryCount / 2);
const symbolRange = {
	start1: 1n,
	end1: rangeEnd,
	start2: 0n,
	end2: 0n,
};

const lookupViaIndex = async () => {
	let found = 0;
	for (let i = 0; i < symbols.length; i += queryBatchSize) {
		const queries = symbols.slice(i, i + queryBatchSize).map(
			(value) =>
				new IntegerCompare({
					key: "hashNumber",
					compare: Compare.Equal,
					value,
				}),
		);
		const entries = await entryIndex
			.iterate(
				{ query: queries.length > 1 ? new Or(queries) : queries },
				{ shape: { hash: true, hashNumber: true } },
			)
			.all();
		found += entries.length;
	}
	if (found !== symbols.length) {
		throw new Error(`Expected ${symbols.length} index hits, got ${found}`);
	}
};

const lookupViaNativeState = () => {
	const result = nativeState.getEntryHashesForHashNumbers(symbols);
	let found = 0;
	for (const hashes of result.values()) {
		found += hashes.length;
	}
	if (found !== symbols.length) {
		throw new Error(`Expected ${symbols.length} native hits, got ${found}`);
	}
};

const lookupRangeViaIndex = async () => {
	const entries = await entryIndex
		.iterate(
			{
				query: new And([
					new IntegerCompare({
						key: "hashNumber",
						compare: Compare.GreaterOrEqual,
						value: symbolRange.start1,
					}),
					new IntegerCompare({
						key: "hashNumber",
						compare: Compare.Less,
						value: symbolRange.end1,
					}),
				]),
			},
			{ shape: { hash: true, hashNumber: true } },
		)
		.all();
	if (entries.length !== rangeHitCount) {
		throw new Error(
			`Expected ${rangeHitCount} index range hits, got ${entries.length}`,
		);
	}
};

const lookupRangeViaNativeState = () => {
	const result = nativeState.getEntryHashNumbersInRange(symbolRange);
	if (result.length !== rangeHitCount) {
		throw new Error(
			`Expected ${rangeHitCount} native range hits, got ${result.length}`,
		);
	}
};

const preflightViaIndexCount = async () => {
	let found = 0;
	for (const symbol of preflightSymbols) {
		if ((await entryIndex.count({ query: { hashNumber: symbol } })) > 0) {
			found += 1;
		}
	}
	if (found !== preflightSymbols.length) {
		throw new Error(
			`Expected ${preflightSymbols.length} index preflight hits, got ${found}`,
		);
	}
};

const preflightViaNativeBatch = () => {
	const result = nativeState.getEntryHashesForHashNumbers(preflightSymbols);
	let found = 0;
	for (const symbol of preflightSymbols) {
		const hashes = result.get(symbol);
		if (hashes && hashes.length > 0) {
			found += 1;
		}
	}
	if (found !== preflightSymbols.length) {
		throw new Error(
			`Expected ${preflightSymbols.length} native preflight hits, got ${found}`,
		);
	}
};

const suite = new Bench({
	name: "native-coordinate-symbol-lookup",
	warmupIterations,
	iterations,
});

suite.add("generic index hashNumber lookup", lookupViaIndex);
suite.add("native resident hashNumber lookup", lookupViaNativeState);
suite.add("generic index hashNumber range lookup", lookupRangeViaIndex);
suite.add("native resident hashNumber range lookup", lookupRangeViaNativeState);
suite.add("generic index hashNumber preflight count", preflightViaIndexCount);
suite.add("native resident hashNumber preflight batch", preflightViaNativeBatch);

try {
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
						entryCount,
						symbolCount,
						preflightSymbolCount,
						rangeHitCount,
						queryBatchSize,
						warmupIterations,
						iterations,
					},
				},
				null,
				2,
			),
		);
	} else {
		console.table(suite.table());
	}
} finally {
	await indices.stop();
}
