import { Cache } from "@peerbit/cache";
import { ready } from "@peerbit/crypto";
import { SQLiteIndices, createDatabase } from "@peerbit/indexer-sqlite3";
import { LamportClock, Meta } from "@peerbit/log";
import { MAX_U64 } from "../src/integers.js";
import {
	EntryReplicatedU64,
	ReplicationIntent,
	ReplicationRangeIndexableU64,
	toRebalance,
} from "../src/ranges.js";

// A deterministic full-range donor scan, not a network convergence benchmark.
// node packages/programs/data/shared-log/dist/benchmark/rebalance-responsiveness.js [rows] [runs] [coordinates] [static|mutation]
const rows = Number(process.argv[2] ?? 7618);
const runs = Number(process.argv[3] ?? 3);
const coordinates = Number(process.argv[4] ?? 2);
const mode = process.argv[5] ?? "static";
if (
	!Number.isSafeInteger(rows) ||
	rows < 1 ||
	!Number.isSafeInteger(runs) ||
	runs < 1 ||
	!Number.isSafeInteger(coordinates) ||
	coordinates < 1 ||
	!["static", "mutation"].includes(mode)
)
	throw new Error(
		"Expected positive integer counts and static or mutation mode",
	);

const db = await createDatabase();
const indices = new SQLiteIndices({ db });
type SqlSample = {
	sql: string;
	bindings: unknown[];
	ms: number;
	rows: number;
};
let sqlSamples: SqlSample[] | undefined;
const plans = new Map<string, unknown>();
const prepare = db.prepare.bind(db);
const instrumented = new WeakSet<object>();
db.prepare = async (...args: Parameters<typeof db.prepare>) => {
	const statement = await prepare(...args);
	if (!instrumented.has(statement)) {
		instrumented.add(statement);
		const all = statement.all.bind(statement);
		statement.all = (...allArgs: Parameters<typeof all>) => {
			if (!sqlSamples) return all(...allArgs);
			const start = performance.now();
			const result = all(...allArgs);
			// This benchmark intentionally measures the synchronous Node adapter.
			if (!Array.isArray(result)) throw new Error("Expected synchronous rows");
			sqlSamples.push({
				sql: args[0],
				bindings: [...allArgs[0]],
				ms: performance.now() - start,
				rows: result.length,
			});
			return result;
		};
	}
	return statement;
};
await ready;
await indices.start();
try {
	const index = await indices.init({ schema: EntryReplicatedU64 });
	if (!index.putBatch) throw new Error("SQLite putBatch is required");
	const meta = new Meta({
		clock: new LamportClock({ id: new Uint8Array(32) }),
		gid: "rebalance-responsiveness",
		next: Array.from(
			{ length: 2 },
			(_, i) => `parent-${String(i).padStart(52, "0")}`,
		),
		type: 0,
	});
	const entries = Array.from(
		{ length: rows },
		(_, i) =>
			new EntryReplicatedU64({
				hash: String(i).padStart(12, "0"),
				hashNumber: BigInt(i),
				coordinates: Array.from(
					{ length: coordinates },
					(_, n) =>
						((BigInt(i) * MAX_U64) / BigInt(rows) +
							BigInt(n) * (MAX_U64 / BigInt(coordinates))) %
						MAX_U64,
				),
				assignedToRangeBoundary: false,
				meta,
			}),
	);
	await index.putBatch(entries);
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
		let closedIterators = 0;
		let openedIterators = 0;
		let mutationMs: number | undefined;
		const pageSamples: { ms: number; returned: number; statements: number }[] =
			[];
		const eventLoopDelays: number[] = [];
		let previousTick = performance.now();
		const probe = setInterval(() => {
			const now = performance.now();
			eventLoopDelays.push(Math.max(0, now - previousTick - 50));
			previousTick = now;
		}, 50);
		sqlSamples = [];
		const originalIterate = index.iterate.bind(index);
		index.iterate = ((...args: Parameters<typeof index.iterate>) => {
			openedIterators++;
			const iterator = originalIterate(...args);
			const close = iterator.close.bind(iterator);
			iterator.close = () => {
				closedIterators++;
				return close();
			};
			const next = iterator.next.bind(iterator);
			iterator.next = async (...nextArgs: Parameters<typeof iterator.next>) => {
				const start = performance.now();
				const before = sqlSamples!.length;
				const result = await next(...nextArgs);
				const ms = performance.now() - start;
				pageSamples.push({
					ms,
					returned: result.length,
					statements: sqlSamples!.length - before,
				});
				maxPageMs = Math.max(maxPageMs, ms);
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
				const expected = entries[returned];
				if (
					entry.hash !== expected.hash ||
					entry.coordinates.length !== expected.coordinates.length ||
					entry.coordinates.some(
						(value, i) => value !== expected.coordinates[i],
					) ||
					entry.getMetaBytes().length !== expected.getMetaBytes().length ||
					entry
						.getMetaBytes()
						.some((value, i) => value !== expected.getMetaBytes()[i])
				)
					throw new Error("Rebalance changed row order or complete values");
				hashes.add(entry.hash);
				returned++;
				if (mode === "mutation" && returned === 1048) {
					const mutationStart = performance.now();
					// An identical put changes the index generation, not the expected set.
					await index.put(entries[entries.length - 1]);
					mutationMs = performance.now() - mutationStart;
				}
			}
			const scanMs = performance.now() - start;
			const samples = sqlSamples;
			sqlSamples = undefined;
			await pendingTask;
			// Let a delayed final probe run before clearing it (outside scan timing).
			await new Promise<void>((resolve) => setTimeout(resolve, 50));
			clearInterval(probe);
			if (returned !== rows || hashes.size !== rows)
				throw new Error(
					`Expected ${rows} distinct rows, got ${returned}/${hashes.size}`,
				);
			if (openedIterators !== closedIterators)
				throw new Error("Rebalance left an iterator open");
			const selects = samples.filter((sample) =>
				/\blimit\s+\?/i.test(sample.sql),
			);
			for (const sample of selects) {
				if (!plans.has(sample.sql)) {
					const explain = await prepare(`EXPLAIN QUERY PLAN ${sample.sql}`);
					plans.set(
						sample.sql,
						await explain.all(
							sample.bindings as Parameters<typeof explain.all>[0],
						),
					);
				}
			}
			console.log(
				JSON.stringify({
					run,
					rows,
					coordinates,
					mode,
					mutationMs,
					pages,
					scanMs,
					maxPageMs,
					firstTaskAtRows,
					firstTaskMs,
					maxEventLoopDelayMs: Math.max(0, ...eventLoopDelays),
					eventLoopDelays,
					pageSamples,
					selects: selects.map(({ ms, rows, bindings }) => ({
						ms,
						rows,
						limit: bindings.at(-2),
						offset: bindings.at(-1),
					})),
					sqlMs: samples.reduce((total, sample) => total + sample.ms, 0),
					sqlCalls: samples.length,
					closedIterators,
				}),
			);
		} finally {
			sqlSamples = undefined;
			clearInterval(probe);
			index.iterate = originalIterate;
			await pendingTask;
		}
	}
	console.log(
		JSON.stringify({ queryPlans: [...plans] }, (_, value) =>
			typeof value === "bigint" ? value.toString() : value,
		),
	);
} finally {
	await indices.stop();
}
