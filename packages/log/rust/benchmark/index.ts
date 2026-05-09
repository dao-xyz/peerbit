import { createLogGraphIndex, type NativeLogEntry } from "../src/index.js";

const makeEntry = (i: number, next: string[]): NativeLogEntry => ({
	hash: `entry-${i}`,
	gid: "default",
	next,
	type: 0,
	head: true,
	payloadSize: 1,
	clock: { timestamp: { wallTime: BigInt(i + 1), logical: 0 } },
});

const count = Number(process.env.PEERBIT_LOG_RUST_BENCH_COUNT ?? 100_000);

const index = await createLogGraphIndex();
const started = performance.now();

for (let i = 0; i < count; i++) {
	index.put(makeEntry(i, i === 0 ? [] : [`entry-${i - 1}`]));
}

const elapsed = performance.now() - started;

console.table([
	{
		entries: count,
		heads: index.heads().length,
		elapsedMs: Math.round(elapsed),
		opsPerSecond: Math.round((count / elapsed) * 1000),
	},
]);
