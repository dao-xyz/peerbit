import { Cache } from "@peerbit/cache";
import { ready } from "@peerbit/crypto";
import { create } from "@peerbit/indexer-sqlite3";
import { LamportClock, Meta } from "@peerbit/log";
import { MAX_U64 } from "../src/integers.js";
import {
	EntryReplicatedU64,
	ReplicationIntent,
	ReplicationRangeIndexableU64,
	toRebalance,
} from "../src/ranges.js";

// A deterministic full-range donor scan, not a network convergence benchmark.
// node packages/programs/data/shared-log/dist/benchmark/rebalance-responsiveness.js [rows] [runs]
const rows = Number(process.argv[2] ?? 7618);
const runs = Number(process.argv[3] ?? 3);
if (
	!Number.isSafeInteger(rows) ||
	rows < 1 ||
	!Number.isSafeInteger(runs) ||
	runs < 1
)
	throw new Error("Expected positive integer row and run counts");

const indices = await create();
await ready;
await indices.start();
try {
	const index = await indices.init({ schema: EntryReplicatedU64 });
	if (!index.putBatch) throw new Error("SQLite putBatch is required");
	const meta = new Meta({
		clock: new LamportClock({ id: new Uint8Array(32) }),
		gid: "rebalance-responsiveness",
		next: [],
		type: 0,
	});
	await index.putBatch(
		Array.from(
			{ length: rows },
			(_, i) =>
				new EntryReplicatedU64({
					hash: String(i).padStart(12, "0"),
					hashNumber: BigInt(i),
					coordinates: [(BigInt(i) * MAX_U64) / BigInt(rows)],
					assignedToRangeBoundary: false,
					meta,
				}),
		),
	);
	const range = new ReplicationRangeIndexableU64({
		id: new Uint8Array(32),
		publicKeyHash: "joining-peer",
		offset: 0n,
		width: MAX_U64,
		timestamp: 1n,
		mode: ReplicationIntent.Strict,
	});
	for (let run = 0; run < runs; run++) {
		let returned = 0;
		let firstTaskAtRows: number | undefined;
		let firstTaskMs: number | undefined;
		let pages = 0;
		let maxPageMs = 0;
		const originalIterate = index.iterate.bind(index);
		index.iterate = ((...args: Parameters<typeof index.iterate>) => {
			const iterator = originalIterate(...args);
			const next = iterator.next.bind(iterator);
			iterator.next = async (...nextArgs: Parameters<typeof iterator.next>) => {
				const start = performance.now();
				const result = await next(...nextArgs);
				maxPageMs = Math.max(maxPageMs, performance.now() - start);
				pages++;
				return result;
			};
			return iterator;
		}) as typeof index.iterate;
		const start = performance.now();
		const pendingTask = new Promise<void>((resolve) => {
			setTimeout(() => {
				firstTaskAtRows = returned;
				firstTaskMs = performance.now() - start;
				resolve();
			}, 0);
		});
		try {
			const hashes = new Set<string>();
			for await (const entry of toRebalance(
				[{ range, type: "added", timestamp: range.timestamp }],
				index,
				new Cache<string>({ max: 100, ttl: 60_000 }),
			)) {
				hashes.add(entry.hash);
				returned++;
			}
			const scanMs = performance.now() - start;
			await pendingTask;
			if (returned !== rows || hashes.size !== rows)
				throw new Error(
					`Expected ${rows} distinct rows, got ${returned}/${hashes.size}`,
				);
			console.log(
				JSON.stringify({
					run,
					rows,
					pages,
					scanMs,
					maxPageMs,
					firstTaskAtRows,
					firstTaskMs,
				}),
			);
		} finally {
			index.iterate = originalIterate;
			await pendingTask;
		}
	}
} finally {
	await indices.stop();
}
