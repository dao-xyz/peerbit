import { Cache } from "@peerbit/cache";
import { type Numbers, createNumbers } from "../src/integers.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { DecoderWrapper, EncoderWrapper, ready as ribltReady } from "@peerbit/riblt";
import { Bench } from "tinybench";

// Run with "node --loader ts-node/esm ./benchmark/rateless-iblt-rebuild.ts"
//
// Env:
// - RIBLT_SIZES="1000,10000,50000"
// - RIBLT_WARMUP=5
// - RIBLT_ITERATIONS=20

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

const createSymbols = (size: number): bigint[] => {
	const symbols = new Array<bigint>(size);
	for (let i = 0; i < size; i++) {
		symbols[i] = BigInt(i + 1);
	}
	return symbols;
};

const createEntryMap = (symbols: bigint[]) => {
	const entries = new Map<string, any>();
	for (let i = 0; i < symbols.length; i++) {
		const hash = `h${i}`;
		entries.set(hash, {
			hash,
			hashNumber: symbols[i],
			assignedToRangeBoundary: false,
		});
	}
	return entries;
};

const createRateless = (numbers: Numbers<"u64">) => {
	const send = async () => {};
	const rpc = { send } as any;

	return new RatelessIBLTSynchronizer<"u64">({
		rpc,
		rangeIndex: {} as any,
		entryIndex: {} as any,
		log: {} as any,
		coordinateToHash: new Cache<string>({ max: 10 }),
		numbers,
	});
};

await ribltReady;

const numbers = createNumbers("u64") as Numbers<"u64">;

const prebuilt = sizes.map((size) => {
	const symbols = createSymbols(size);
	const entries = createEntryMap(symbols);
	const baseEncoder = new EncoderWrapper();
	for (const symbol of symbols) {
		baseEncoder.add_symbol(symbol);
	}
	return { size, symbols, entries, baseEncoder };
});

const suite = new Bench({
	name: "rateless-iblt-rebuild",
	warmupIterations: Number.isFinite(warmupIterations) ? warmupIterations : 0,
	iterations: Number.isFinite(iterations) ? iterations : undefined,
});

for (const { size, symbols, entries, baseEncoder } of prebuilt) {
	suite.add(`EncoderWrapper build (n=${size})`, () => {
		const encoder = new EncoderWrapper();
		for (const symbol of symbols) {
			encoder.add_symbol(symbol);
		}
		encoder.free();
	});

	suite.add(`DecoderWrapper build (n=${size})`, () => {
		const decoder = new DecoderWrapper();
		for (const symbol of symbols) {
			decoder.add_symbol(symbol);
		}
		decoder.free();
	});

	suite.add(`EncoderWrapper clone (n=${size})`, () => {
		const clone = baseEncoder.clone();
		clone.free();
	});

	suite.add(`EncoderWrapper clone+to_decoder (n=${size})`, () => {
		const clone = baseEncoder.clone();
		const decoder = clone.to_decoder();
		clone.free();
		decoder.free();
	});

	if (size > 333) {
		const sync = createRateless(numbers);
		suite.add(`RatelessIBLTSynchronizer.onMaybeMissingEntries (n=${size})`, async () => {
			await sync.onMaybeMissingEntries({ entries: entries as any, targets: ["p"] });
			for (const [, proc] of [...sync.outgoingSyncProcesses]) {
				proc.free();
			}
		});
	}
}

await suite.run();
console.table(suite.table());

for (const { baseEncoder } of prebuilt) {
	baseEncoder.free();
}

