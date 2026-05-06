import DB from "better-sqlite3";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

const rows = Number(process.env.QUERY_PLANNER_BENCH_ROWS ?? 500_000);
const runs = Number(process.env.QUERY_PLANNER_BENCH_RUNS ?? 50);
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "peerbit-qplanner-"));
const dbPath = path.join(tmpDir, "bench.sqlite");

type BenchResult = {
	name: string;
	avgMs: number;
	p50Ms: number;
	minMs: number;
	maxMs: number;
	plan: string;
};

const percentile = (values: number[], p: number) => {
	const sorted = [...values].sort((a, b) => a - b);
	return sorted[Math.floor((sorted.length - 1) * p)];
};

const bench = (
	db: DB.Database,
	name: string,
	sql: string,
	params: unknown[],
): BenchResult => {
	const stmt = db.prepare(sql);
	for (let i = 0; i < 5; i++) {
		stmt.all(...params);
	}

	const times: number[] = [];
	for (let i = 0; i < runs; i++) {
		const start = performance.now();
		stmt.all(...params);
		times.push(performance.now() - start);
	}

	const plan = db
		.prepare(`EXPLAIN QUERY PLAN ${sql}`)
		.all(...params)
		.map((row) => String((row as { detail: string }).detail))
		.join("\n");

	return {
		name,
		avgMs: times.reduce((sum, value) => sum + value, 0) / times.length,
		p50Ms: percentile(times, 0.5),
		minMs: Math.min(...times),
		maxMs: Math.max(...times),
		plan,
	};
};

try {
	const db = new DB(dbPath);
	db.pragma("journal_mode = OFF");
	db.pragma("synchronous = OFF");
	db.pragma("temp_store = MEMORY");
	db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT)");

	const insert = db.prepare("INSERT INTO t(id, a, b, c) VALUES (?, ?, ?, ?)");
	const insertMany = db.transaction(() => {
		for (let i = 1; i <= rows; i++) {
			insert.run(i, i % 10, i % 1000, Math.floor(Math.random() * rows));
		}
	});

	const loadStart = performance.now();
	insertMany();
	const loadMs = performance.now() - loadStart;

	const indexTimes: Record<string, number> = {};
	for (const [name, sql] of [
		["old_forward", "CREATE INDEX idx_old_a_b_c ON t(a, b, c)"],
		["old_reverse", "CREATE INDEX idx_old_c_b_a ON t(c, b, a)"],
		["new_semantic", "CREATE INDEX idx_new_a_c_b ON t(a, c, b)"],
	] as const) {
		const start = performance.now();
		db.exec(sql);
		indexTimes[name] = performance.now() - start;
	}
	db.exec("ANALYZE");

	const query =
		"SELECT id FROM t WHERE a = ? AND b BETWEEN ? AND ? ORDER BY c LIMIT ?";
	const forcedOldForward =
		"SELECT id FROM t INDEXED BY idx_old_a_b_c WHERE a = ? AND b BETWEEN ? AND ? ORDER BY c LIMIT ?";
	const forcedOldReverse =
		"SELECT id FROM t INDEXED BY idx_old_c_b_a WHERE a = ? AND b BETWEEN ? AND ? ORDER BY c LIMIT ?";
	const forcedNew =
		"SELECT id FROM t INDEXED BY idx_new_a_c_b WHERE a = ? AND b BETWEEN ? AND ? ORDER BY c LIMIT ?";
	const params = [1, 0, 999, 50];

	const results = [
		bench(db, "old forced (a,b,c)", forcedOldForward, params),
		bench(db, "old forced reverse (c,b,a)", forcedOldReverse, params),
		bench(db, "new semantic forced (a,c,b)", forcedNew, params),
		bench(db, "sqlite choice with all indexes", query, params),
	];

	/* eslint-disable no-console */
	console.log(
		JSON.stringify(
			{
				rows,
				runs,
				loadMs,
				indexTimes,
			},
			null,
			2,
		),
	);
	console.table(
		results.map(({ name, avgMs, p50Ms, minMs, maxMs }) => ({
			name,
			avgMs: Number(avgMs.toFixed(3)),
			p50Ms: Number(p50Ms.toFixed(3)),
			minMs: Number(minMs.toFixed(3)),
			maxMs: Number(maxMs.toFixed(3)),
		})),
	);
	for (const result of results) {
		console.log(`\n${result.name}\n${result.plan}`);
	}
} finally {
	fs.rmSync(tmpDir, { recursive: true, force: true });
}
