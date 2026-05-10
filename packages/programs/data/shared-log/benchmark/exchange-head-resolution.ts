import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Log } from "@peerbit/log";
import { Bench } from "tinybench";

// Run with:
//   cd packages/programs/data/shared-log
//   EXCHANGE_HEADS=1000 EXCHANGE_HEAD_WARMUP=5 EXCHANGE_HEAD_ITERATIONS=30 \
//     BENCH_JSON=1 node --loader ts-node/esm ./benchmark/exchange-head-resolution.ts

const parsePositiveInt = (value: string | undefined, fallback: number) => {
	const parsed = Number.parseInt(value ?? "", 10);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const headCount = parsePositiveInt(process.env.EXCHANGE_HEADS, 1_000);
const warmupIterations = parsePositiveInt(process.env.EXCHANGE_HEAD_WARMUP, 5);
const iterations = parsePositiveInt(process.env.EXCHANGE_HEAD_ITERATIONS, 30);

const store = new AnyBlockStore();
await store.start();
const key = await Ed25519Keypair.create();
const log = new Log<Uint8Array>();
await log.open(store, key, {
	appendDurability: "strict",
	nativeGraph: true,
});

const hashes: string[] = [];
for (let i = 0; i < headCount; i++) {
	const { entry } = await log.append(new Uint8Array([i & 0xff]), {
		meta: { next: [], gidSeed: new Uint8Array([i & 0xff, (i >> 8) & 0xff]) },
	});
	hashes.push(entry.hash);
}

const suite = new Bench({
	name: "exchange-head-resolution",
	warmupIterations,
	iterations,
});

suite.add("sequential log.get heads", async () => {
	(log.entryIndex as any).cache.clear();
	let found = 0;
	for (const hash of hashes) {
		if (await log.get(hash)) {
			found += 1;
		}
	}
	if (found !== hashes.length) {
		throw new Error(`Expected ${hashes.length} heads, got ${found}`);
	}
});

suite.add("batched entryIndex.getMany heads", async () => {
	(log.entryIndex as any).cache.clear();
	const entries = await log.entryIndex.getMany(hashes, {
		type: "full",
		ignoreMissing: true,
	});
	if (entries.filter(Boolean).length !== hashes.length) {
		throw new Error(`Expected ${hashes.length} heads, got ${entries.length}`);
	}
});

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
					meta: { headCount, warmupIterations, iterations },
				},
				null,
				2,
			),
		);
	} else {
		console.table(suite.table());
	}
} finally {
	await log.close();
	await store.stop();
}
