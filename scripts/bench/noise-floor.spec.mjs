import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	analyze,
	buildBaseline,
	ceilTo,
	median,
	p95IsJustTheMaximum,
	pairwiseAbsDeltasPct,
	percentileNearestRank,
	readMeasurementMetadata,
	renderMarkdown,
	runCli,
	stringifyBaseline,
} from "./noise-floor.mjs";

const withFixture = (body) => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "peerbit-noise-floor-"));
	try {
		return body(root);
	} finally {
		fs.rmSync(root, { recursive: true, force: true });
	}
};

const writeSuite = (root, run, file, body) => {
	const dir = path.join(root, run);
	fs.mkdirSync(dir, { recursive: true });
	fs.writeFileSync(
		path.join(dir, file),
		typeof body === "string" ? body : JSON.stringify(body, null, 2),
	);
};

const task = (name, meanMs) => ({
	name,
	hz: meanMs > 0 ? 1000 / meanMs : 0,
	mean_ms: meanMs,
	rme: 1.5,
	samples: 10,
});

const suite = (...tasks) => ({ name: "fixture", tasks, meta: {} });

// The REAL document-put.json shape, copied field-for-field from
// packages/programs/data/document/document/benchmark/document-put.ts: a BenchRow
// list under `rows`, no `tasks` anywhere, `opsPerSecond` instead of `hz` and
// `totalPutMs` instead of `mean_ms`. The six other suites emit tinybench's
// `tasks`; this one never has.
const documentPutRow = (name, totalPutMs, iterations = 10) => ({
	name,
	iterations,
	payloadBytes: 1024,
	// document-put.ts: Math.round((iterations / profile.totalPutMs) * 1000).
	opsPerSecond: Math.round((iterations / totalPutMs) * 1000),
	cleanupMs: 0,
	serializeMs: 1.5,
	existingHeadLookupMs: 0.25,
	sharedAppendMs: Math.round(totalPutMs * 60) / 100,
	logAppendMs: Math.round(totalPutMs * 40) / 100,
	documentIndexPutMs: Math.round(totalPutMs * 20) / 100,
	documentIndexTransformMs: 0.1,
	documentBackendIndexPutMs: Math.round(totalPutMs * 10) / 100,
	totalPutMs,
});

const documentPutSuite = (...rows) => ({
	name: "document-put",
	rows,
	meta: {
		payloadBytes: 1024,
		warmupIterations: 2,
		iterations: 10,
		profileDeep: false,
		profileNativeBackbone: false,
		coordinateWalFlushBytes: 0,
		coordinateWalFlushIntervalMs: 0,
	},
});

// Lets a fixture hold values JSON.stringify cannot express (Infinity) or that a
// broken benchmark would emit verbatim (null, 0, "12").
const rawSuite = (rows) =>
	`{"name":"fixture","tasks":[${rows
		.map(
			(row) =>
				`{"name":${JSON.stringify(row.name)},"mean_ms":${row.mean_ms},"hz":${row.hz}}`,
		)
		.join(",")}]}`;

const findSuite = (report, file) =>
	report.suites.find((entry) => entry.file === file);

const findTask = (report, file, name) =>
	findSuite(report, file).tasks.find((entry) => entry.name === name);

const codes = (report, severity) =>
	report.problems
		.filter((problem) => problem.severity === severity)
		.map((problem) => problem.code);

const captureCli = (argv) => {
	const stdout = [];
	const stderr = [];
	const code = runCli(argv, {
		log: (line) => stdout.push(String(line)),
		logError: (line) => stderr.push(String(line)),
	});
	return { code, stdout: stdout.join("\n"), stderr: stderr.join("\n") };
};

// Field-for-field what .github/workflows/benchmark-noise-floor.yml's "Record run
// metadata" step writes into the results directory. The baseline emitter reads
// its entire provenance from here, so a fixture that drifts from that step is a
// test that proves nothing.
const METADATA = {
	kind: "benchmark-noise-floor",
	note: "One commit, one build, repeated runs.",
	ref: "master",
	sha: "b776198cd0bb1a2c3d4e5f60718293a4b5c6d7e8",
	repeats: 3,
	selected_suites: ["shared-log", "transport", "document"],
	run_url: "https://github.com/dao-xyz/peerbit/actions/runs/31844749062",
	measured_at: "2026-08-15T09:00:00.000Z",
};

const writeMetadata = (root, overrides = {}) => {
	const metadata = { ...METADATA, ...overrides };
	// An explicit `undefined` in `overrides` means "this run wrote no such
	// field", which is how the artifacts predating `measured_at` look.
	for (const [key, value] of Object.entries(overrides))
		if (value === undefined) delete metadata[key];
	fs.writeFileSync(
		path.join(root, "metadata.json"),
		typeof overrides.raw === "string"
			? overrides.raw
			: `${JSON.stringify(metadata, null, 2)}\n`,
	);
	return metadata;
};

// Three runs of one suite plus the metadata the emitter demands: the smallest
// input a committable baseline is allowed to come from.
const baselineFixture = (root, means = [100, 101, 102.7249], overrides = {}) => {
	means.forEach((meanMs, index) =>
		writeSuite(
			root,
			`run-${index + 1}`,
			"pid-convergence.json",
			suite(task("T", meanMs)),
		),
	);
	return writeMetadata(root, overrides);
};

const readBaseline = (file) => JSON.parse(fs.readFileSync(file, "utf8"));

// Exactly how .github/workflows/benchmarks.yml reads a floor: index by the suite
// FILE name, then by the task name, then take the metric straight off the entry.
// Going through this helper rather than poking at the object keeps the tests
// honest about the contract that matters.
const consumerFloor = (baseline, file, taskName, metricKey) => {
	const floors = baseline.tasks[file];
	if (!floors || !Object.hasOwn(floors, taskName)) return null;
	const entry = floors[taskName];
	if (!entry || !Object.hasOwn(entry, metricKey)) return null;
	const value = entry[metricKey];
	return typeof value === "number" && Number.isFinite(value) && value >= 0
		? value
		: null;
};

const COMMITTED_BASELINE = new URL(
	"./noise-floor-baseline.json",
	import.meta.url,
);

const round6 = (value) => Number(value.toFixed(6));

const forEachNumber = (value, visit) => {
	if (typeof value === "number") visit(value);
	else if (Array.isArray(value))
		for (const item of value) forEachNumber(item, visit);
	else if (value && typeof value === "object")
		for (const item of Object.values(value)) forEachNumber(item, visit);
};

test("pairwise |Δ| uses min(a,b) and matches a hand-computed fixture", () => {
	// 100, 110, 90 -> pairs 10/100, 10/90, 20/90 = 10%, 11.111111%, 22.222222%.
	assert.deepEqual(
		pairwiseAbsDeltasPct([100, 110, 90]).map(round6),
		[10, 11.111111, 22.222222],
	);
	// Nearest rank, never interpolated: p95 of 1..20 is the 19th value, not 19.05.
	const twenty = Array.from({ length: 20 }, (_, index) => index + 1);
	assert.equal(percentileNearestRank(twenty, 0.95), 19);
	assert.equal(
		percentileNearestRank([10, 11.111111, 22.222222], 0.95),
		22.222222,
	);
	assert.equal(median([50, 100, 200]), 100);
	assert.equal(median([1, 2, 3, 4]), 2.5);

	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, true);
		assert.equal(report.pairsPerTask, 3);

		const stats = findTask(report, "pid-convergence.json", "T").metrics.mean_ms;
		assert.equal(stats.status, "ok");
		assert.equal(stats.n, 3);
		assert.equal(stats.pairs, 3);
		assert.equal(stats.mean, 100);
		assert.equal(stats.min, 90);
		assert.equal(stats.max, 110);
		assert.equal(stats.median_abs_delta_pct, 11.111111);
		assert.equal(stats.p95_abs_delta_pct, 22.222222);
		assert.equal(stats.max_abs_delta_pct, 22.222222);

		// min(a,b) makes the floor invariant under hz = 1000/mean_ms...
		const hz = findTask(report, "pid-convergence.json", "T").metrics.hz;
		assert.deepEqual(
			[hz.median_abs_delta_pct, hz.p95_abs_delta_pct, hz.max_abs_delta_pct],
			[
				stats.median_abs_delta_pct,
				stats.p95_abs_delta_pct,
				stats.max_abs_delta_pct,
			],
		);
		// ...while the dispersion statistics are not, which is why both metrics
		// are reported.
		assert.notEqual(hz.cv_pct, stats.cv_pct);
	});
});

test("stddev is the sample (n-1) form and CV is stddev/mean as a percent", () => {
	withFixture((root) => {
		// 2, 4, 6 -> mean 4, sample stddev 2, CV 50%.
		for (const [run, meanMs] of [
			["run-1", 2],
			["run-2", 4],
			["run-3", 6],
		])
			writeSuite(root, run, "document-put.json", suite(task("put", meanMs)));

		const stats = analyze({ resultsDir: root, minRuns: 3 });
		const put = findTask(stats, "document-put.json", "put").metrics.mean_ms;
		assert.equal(put.mean, 4);
		assert.equal(put.stddev, 2);
		assert.equal(put.cv_pct, 50);
		// Pairs: 2/2, 4/2, 2/4 -> 100%, 200%, 50%.
		assert.equal(put.median_abs_delta_pct, 100);
		assert.equal(put.max_abs_delta_pct, 200);
		assert.equal(stats.summary.mean_ms.worstP95, 200);
		assert.equal(stats.summary.mean_ms.tasksAbove10Pct, 1);
	});
});

test("refuses to publish a floor built from fewer than --min-runs usable runs", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
		])
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));

		const { code, stdout, stderr } = captureCli([root]);
		assert.equal(code, 1);

		const report = JSON.parse(
			fs.readFileSync(path.join(root, "noise-floor.json"), "utf8"),
		);
		assert.equal(report.minRuns, 3);
		assert.equal(report.runCount, 2);
		assert.equal(report.trustworthy, false);
		const problem = report.problems.find(
			(entry) => entry.code === "insufficient-runs",
		);
		assert.ok(problem, "expected an insufficient-runs problem");
		assert.equal(problem.severity, "blocking");
		assert.match(problem.message, /Only 2 usable run\(s\), need at least 3/);
		assert.match(stdout, /NOT TRUSTWORTHY/);
		assert.match(stdout, /insufficient-runs/);
		assert.match(stderr, /blocking problem/);

		// A third run clears it, and only then does the same data pass.
		writeSuite(root, "run-3", "pid-convergence.json", suite(task("T", 90)));
		assert.equal(captureCli([root]).code, 0);
		// ...and an explicitly higher bar still refuses it.
		const stricter = captureCli([root, "--min-runs", "5"]);
		assert.equal(stricter.code, 1);
		assert.match(stricter.stdout, /Only 3 usable run\(s\), need at least 5/);
	});
});

test("refuses --min-runs below 2 instead of clamping it", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));

		for (const argv of [
			[root, "--min-runs", "1"],
			[root, "--min-runs=0"],
		]) {
			const { code, stdout, stderr } = captureCli(argv);
			assert.equal(code, 2, `expected a usage error for ${argv.join(" ")}`);
			assert.match(stderr, /--min-runs must be at least 2/);
			assert.match(stderr, /no spread to measure/);
			assert.equal(stdout, "", "a refused invocation must not print a report");
			assert.equal(
				fs.existsSync(path.join(root, "noise-floor.json")),
				false,
				"a refused invocation must not write a report",
			);
		}

		assert.equal(captureCli([root, "--min-runs", "two"]).code, 2);
		assert.equal(captureCli([root, "--min-runs"]).code, 2);
		assert.equal(captureCli([root, "--nonsense"]).code, 2);
		assert.equal(captureCli([]).code, 2);
	});
});

test("a task missing from some runs is blocking and its n is visible", () => {
	withFixture((root) => {
		writeSuite(
			root,
			"run-1",
			"pid-convergence.json",
			suite(task("A", 100), task("B", 50), task("C", 20)),
		);
		writeSuite(
			root,
			"run-2",
			"pid-convergence.json",
			suite(task("A", 104), task("B", 55)),
		);
		writeSuite(root, "run-3", "pid-convergence.json", suite(task("A", 96)));

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, false);
		assert.equal(report.runCount, 3);

		const a = findTask(report, "pid-convergence.json", "A");
		assert.equal(a.n, 3);
		assert.equal(a.complete, true);
		assert.equal(a.metrics.mean_ms.status, "ok");

		const b = findTask(report, "pid-convergence.json", "B");
		assert.equal(b.n, 2);
		assert.equal(b.complete, false);
		assert.deepEqual(b.absentIn, ["run-3"]);
		assert.equal(b.metrics.mean_ms.pairs, 1);

		const c = findTask(report, "pid-convergence.json", "C");
		assert.equal(c.n, 1);
		assert.deepEqual(c.absentIn, ["run-2", "run-3"]);
		assert.equal(c.metrics.mean_ms.status, "insufficient-samples");
		// A single sample has unknown spread; reporting 0 would read as "stable".
		assert.equal(c.metrics.mean_ms.stddev, null);
		assert.equal(c.metrics.mean_ms.cv_pct, null);
		assert.equal(c.metrics.mean_ms.max_abs_delta_pct, null);

		const missing = report.problems.filter(
			(problem) => problem.code === "task-missing-in-run",
		);
		assert.deepEqual(
			missing.map((problem) => `${problem.task}@${problem.run}`).sort(),
			["B@run-3", "C@run-2", "C@run-3"],
		);
		assert.ok(missing.every((problem) => problem.severity === "blocking"));

		const markdown = renderMarkdown(report);
		assert.match(markdown, /\| B \| 2\/3 \|/);
		assert.match(markdown, /\*\*n=2\/3\*\* \(absent: run-3\)/);
		assert.match(markdown, /\*\*n=1\/3\*\* \(absent: run-2, run-3\)/);
	});
});

test("a missing, empty, unparseable or task-less suite file is blocking, not skipped", () => {
	const brokenRun3 = [
		{ label: "missing", body: undefined, reason: "missing" },
		{ label: "empty", body: "", reason: "empty" },
		{ label: "unparseable", body: "{not json", reason: "unparseable" },
		{
			label: "no tasks",
			body: { name: "fixture", tasks: [] },
			reason: "no-tasks",
		},
		{
			label: "tasks is not an array",
			body: { name: "fixture", tasks: {} },
			reason: "unparseable",
		},
	];

	for (const broken of brokenRun3)
		withFixture((root) => {
			for (const [run, meanMs] of [
				["run-1", 100],
				["run-2", 110],
				["run-3", 90],
			]) {
				// pid-convergence is healthy everywhere, so run-3 stays usable and the
				// sweep's absence cannot be written off as a dead run.
				writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));
				if (run !== "run-3")
					writeSuite(
						root,
						run,
						"sync-batch-sweep.json",
						suite(task("batch 256", meanMs)),
					);
			}
			if (broken.body !== undefined)
				writeSuite(root, "run-3", "sync-batch-sweep.json", broken.body);

			const report = analyze({ resultsDir: root, minRuns: 3 });
			const sweep = findSuite(report, "sync-batch-sweep.json");
			assert.equal(report.trustworthy, false, broken.label);
			assert.equal(sweep.status, "partial", broken.label);
			assert.deepEqual(sweep.presentIn, ["run-1", "run-2"], broken.label);
			assert.deepEqual(sweep.missingIn, ["run-3"], broken.label);

			const problem = report.problems.find(
				(entry) =>
					entry.code === "suite-missing-in-run" &&
					entry.suite === "sync-batch-sweep.json",
			);
			assert.ok(problem, broken.label);
			assert.equal(problem.severity, "blocking", broken.label);
			assert.equal(problem.run, "run-3", broken.label);
			assert.equal(problem.reason, broken.reason, broken.label);

			// The surviving runs must not be quietly promoted to a full sample.
			const batch = findTask(report, "sync-batch-sweep.json", "batch 256");
			assert.equal(batch.n, 2, broken.label);
			assert.equal(batch.complete, false, broken.label);
			assert.match(
				renderMarkdown(report),
				/missing or unreadable in run-3/,
				broken.label,
			);
			// The healthy suite in the same runs is still reported.
			assert.equal(
				findTask(report, "pid-convergence.json", "T").metrics.mean_ms.status,
				"ok",
				broken.label,
			);
		});
});

test("a run directory with no readable suite is excluded and blocking", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));
		fs.mkdirSync(path.join(root, "run-4"));

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.runCount, 3);
		assert.deepEqual(report.excludedRuns, ["run-4"]);
		assert.equal(report.trustworthy, false);
		assert.ok(codes(report, "blocking").includes("unusable-run"));
		// The exclusion must not turn into "T is missing from run-4" noise.
		assert.equal(findTask(report, "pid-convergence.json", "T").complete, true);
	});
});

test("zero, null, non-numeric and infinite values invalidate a metric instead of becoming 0%", () => {
	withFixture((root) => {
		// Every value is written as raw JSON text: Infinity has no JSON.stringify
		// form, and `null` / `0` / `"12"` are exactly what a half-failed
		// benchmark writes (`mean_ms: task.result?.mean ?? null`,
		// `hz: mean_ms > 0 ? 1000 / mean_ms : 0`).
		const rows = (ok, zero, nul, inf, str) => [
			{ name: "ok-task", mean_ms: ok, hz: ok },
			{ name: "zero-task", mean_ms: zero, hz: zero },
			{ name: "null-task", mean_ms: nul, hz: nul },
			{ name: "inf-task", mean_ms: inf, hz: inf },
			{ name: "string-task", mean_ms: str, hz: str },
		];
		writeSuite(
			root,
			"run-1",
			"chunk-transfer.json",
			rawSuite(rows("10", "10", "10", "10", "10")),
		);
		writeSuite(
			root,
			"run-2",
			"chunk-transfer.json",
			rawSuite(rows("10.5", "0", "10.5", "10.5", "10.5")),
		);
		writeSuite(
			root,
			"run-3",
			"chunk-transfer.json",
			rawSuite(rows("9.8", "9.8", "null", "1e999", '"12"')),
		);

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, false);

		const expected = [
			["zero-task", "run-2", "not positive (0)"],
			["null-task", "run-3", "missing (null)"],
			["inf-task", "run-3", "Infinity"],
			["string-task", "run-3", "non-numeric (string)"],
		];
		for (const [name, run, reason] of expected)
			for (const metric of ["mean_ms", "hz"]) {
				const stats = findTask(report, "chunk-transfer.json", name).metrics[
					metric
				];
				assert.equal(stats.status, "invalid", `${name}.${metric}`);
				assert.deepEqual(
					stats.invalidSamples,
					[{ run, reason }],
					`${name}.${metric}`,
				);
				// No 0%, no Infinity, no NaN leaking into the floor.
				assert.equal(stats.max_abs_delta_pct, null, `${name}.${metric}`);
				assert.equal(stats.p95_abs_delta_pct, null, `${name}.${metric}`);
				assert.equal(stats.cv_pct, null, `${name}.${metric}`);
				assert.equal(stats.noVariationResolved, false, `${name}.${metric}`);
			}

		const blocking = report.problems.filter(
			(problem) => problem.code === "invalid-metric-value",
		);
		assert.equal(blocking.length, expected.length * 2);
		assert.ok(blocking.every((problem) => problem.severity === "blocking"));

		// The healthy task in the same suite is still measured.
		assert.equal(
			findTask(report, "chunk-transfer.json", "ok-task").metrics.mean_ms.status,
			"ok",
		);

		// Nothing anywhere in the serialised report is NaN or Infinity.
		const serialised = JSON.parse(JSON.stringify(report));
		forEachNumber(serialised, (value) =>
			assert.ok(
				Number.isFinite(value),
				`non-finite number in the report: ${value}`,
			),
		);
		// "Infinity" may appear as a diagnostic reason, but never as a rendered
		// statistic: every invalid cell must be "-".
		const markdown = renderMarkdown(report);
		assert.doesNotMatch(markdown, /\|\s*-?(NaN|Infinity)\s*\|/);
		assert.match(
			markdown,
			/\| inf-task \| 3\/3 \| - \| - \| - \| - \| - \| - \| - \| - \|/,
		);
		assert.match(markdown, /\*\*invalid\*\*: run-2 not positive \(0\)/);
		assert.match(markdown, /\*\*invalid\*\*: run-3 Infinity/);
	});
});

test("a task with no resolved variation is labelled without claiming determinism", () => {
	withFixture((root) => {
		for (const run of ["run-1", "run-2", "run-3"])
			writeSuite(
				root,
				run,
				"pid-convergence.json",
				suite(task("PID convergence (model)", 12.5)),
			);

		const { code, stdout } = captureCli([root]);
		assert.equal(code, 0, "a deterministic benchmark is not a failure");

		const report = analyze({ resultsDir: root, minRuns: 3 });
		const stats = findTask(
			report,
			"pid-convergence.json",
			"PID convergence (model)",
		).metrics.mean_ms;
		assert.equal(stats.status, "ok");
		assert.equal(stats.n, 3);
		assert.equal(stats.max_abs_delta_pct, 0);
		assert.equal(stats.p95_abs_delta_pct, 0);
		assert.equal(stats.cv_pct, 0);
		assert.equal(stats.noVariationResolved, true);

		const notes = report.problems.filter(
			(problem) => problem.code === "no-variation-resolved",
		);
		assert.equal(notes.length, 2, "one note per metric");
		assert.ok(notes.every((note) => note.severity === "note"));
		assert.match(notes[0].message, /identical .* in all 3 runs/);
		// The note must describe the OBSERVATION and refuse to name a cause. A
		// zero floor is indistinguishable from variation below the source's
		// rounding step (document-put emits round(opsPerSecond)), so claiming
		// "deterministic" would bless every later A/B delta on this task.
		assert.match(notes[0].message, /below the resolution/i);
		assert.doesNotMatch(
			notes[0].message,
			/deterministic|reproducible|real observation/i,
			"must not assert a cause the data cannot distinguish",
		);
		assert.equal(report.summary.mean_ms.noVariationTasks, 1);
		assert.equal(report.summary.mean_ms.worstP95, 0);

		assert.match(stdout, /`no variation resolved`/);
		assert.match(stdout, /TRUSTWORTHY/);
	});
});

test("output ordering is deterministic and independent of filesystem order", () => {
	withFixture((root) => {
		// Created out of order, with task names written in reverse.
		for (const [run, meanMs] of [
			["run-10", 90],
			["run-2", 110],
			["run-1", 100],
		])
			writeSuite(
				root,
				run,
				"pid-convergence.json",
				suite(task("zeta", meanMs), task("mid", meanMs), task("alpha", meanMs)),
			);

		const first = analyze({ resultsDir: root, minRuns: 3 });
		const second = analyze({ resultsDir: root, minRuns: 3 });

		// run-10 sorts after run-2, not between run-1 and run-2.
		assert.deepEqual(
			first.runs.map((run) => run.name),
			["run-1", "run-2", "run-10"],
		);
		assert.deepEqual(
			findSuite(first, "pid-convergence.json").tasks.map((entry) => entry.name),
			["alpha", "mid", "zeta"],
		);
		assert.deepEqual(
			first.suites.map((entry) => entry.file),
			second.suites.map((entry) => entry.file),
		);
		assert.equal(JSON.stringify(first), JSON.stringify(second));
		assert.equal(renderMarkdown(first), renderMarkdown(second));
		// No wall clock anywhere in the computed output.
		assert.doesNotMatch(
			JSON.stringify(first),
			/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/,
			"the report must not embed a timestamp",
		);
	});
});

test("suites nobody collected warn; unknown files in a run warn; neither blocks", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));
		fs.writeFileSync(
			path.join(root, "run-2", "rateless-iblt-startsync-cach.json"),
			"{}",
		);

		const { code, stdout } = captureCli([
			root,
			"--json-out",
			path.join(root, "nested", "report.json"),
		]);
		assert.equal(code, 0);

		const report = JSON.parse(
			fs.readFileSync(path.join(root, "nested", "report.json"), "utf8"),
		);
		assert.equal(report.trustworthy, true);
		assert.equal(report.suitesCollected, 1);
		assert.equal(report.suitesKnown, 7);
		assert.equal(
			report.problems.filter(
				(problem) => problem.code === "suite-not-collected",
			).length,
			6,
		);
		const unknown = report.problems.find(
			(problem) => problem.code === "unrecognized-file",
		);
		assert.equal(unknown.severity, "warning");
		assert.match(unknown.message, /rateless-iblt-startsync-cach\.json/);
		assert.match(stdout, /Suites collected: 1\/7/);
		assert.match(stdout, /_Not collected in any run/);
		assert.equal(fs.existsSync(path.join(root, "noise-floor.json")), false);
	});
});

test("duplicate task rows within one run are blocking", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(
				root,
				run,
				"file-ingest.json",
				suite(
					task("ingest", meanMs),
					...(run === "run-2" ? [task("ingest", 4000)] : []),
				),
			);

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, false);
		const problem = report.problems.find(
			(entry) => entry.code === "duplicate-task",
		);
		assert.equal(problem.severity, "blocking");
		assert.equal(problem.run, "run-2");
		// The first row wins, so the duplicate cannot silently inflate the floor.
		assert.equal(
			findTask(report, "file-ingest.json", "ingest").metrics.mean_ms.max,
			110,
		);
	});
});

test("an empty results directory is reported, not treated as a clean run", () => {
	withFixture((root) => {
		const { code, stdout } = captureCli([root]);
		assert.equal(code, 1);
		assert.match(stdout, /NOT TRUSTWORTHY/);
		const report = JSON.parse(
			fs.readFileSync(path.join(root, "noise-floor.json"), "utf8"),
		);
		assert.equal(report.runCount, 0);
		assert.ok(codes(report, "blocking").includes("no-runs"));
		assert.ok(codes(report, "blocking").includes("insufficient-runs"));
		assert.equal(report.summary.mean_ms.tasksMeasured, 0);
	});

	const missing = captureCli([
		path.join(os.tmpdir(), "peerbit-noise-floor-does-not-exist"),
	]);
	assert.equal(missing.code, 2);
	assert.match(missing.stderr, /Results directory does not exist/);
});

// --- D1: document-put.json is a `rows` file, not a `tasks` file -------------

test("document-put's real `rows` shape is read, with totalPutMs/opsPerSecond as the two metrics", () => {
	withFixture((root) => {
		// 10 iterations, so opsPerSecond is exactly 10000/totalPutMs and the
		// reciprocal invariance of |Δ%| is exact rather than rounded.
		for (const [run, totalPutMs] of [
			["run-1", 100],
			["run-2", 125],
			["run-3", 200],
		])
			writeSuite(
				root,
				run,
				"document-put.json",
				documentPutSuite(
					documentPutRow("put 1024b", totalPutMs),
					documentPutRow("native-ceiling", totalPutMs * 2),
				),
			);

		const { code } = captureCli([root]);
		assert.equal(code, 0, "the real document-put shape must not be rejected");

		const report = analyze({ resultsDir: root, minRuns: 3 });
		const put = findSuite(report, "document-put.json");
		assert.equal(put.status, "ok");
		// Task names come from the row's own `name`, and every row is a task.
		assert.deepEqual(
			put.tasks.map((entry) => entry.name),
			["native-ceiling", "put 1024b"],
		);

		const mean = findTask(report, "document-put.json", "put 1024b").metrics
			.mean_ms;
		assert.equal(mean.status, "ok");
		assert.equal(mean.n, 3);
		// mean_ms is read from totalPutMs: 100, 125, 200.
		assert.equal(mean.min, 100);
		assert.equal(mean.max, 200);
		// Pairs: 25/100, 100/100, 75/125 -> 25%, 100%, 60%.
		assert.equal(mean.median_abs_delta_pct, 60);
		assert.equal(mean.max_abs_delta_pct, 100);

		const hz = findTask(report, "document-put.json", "put 1024b").metrics.hz;
		assert.equal(hz.status, "ok");
		// hz is read from opsPerSecond: 100, 80, 50 ops/s.
		assert.equal(hz.min, 50);
		assert.equal(hz.max, 100);
		// Exactly proportional to 1/totalPutMs, so the floor is identical...
		assert.equal(hz.median_abs_delta_pct, mean.median_abs_delta_pct);
		assert.equal(hz.max_abs_delta_pct, mean.max_abs_delta_pct);
		// ...while CV is not reciprocal-invariant, which is why both are kept.
		assert.notEqual(hz.cv_pct, mean.cv_pct);

		// The suite reaches the headline instead of silently leaving it.
		assert.equal(report.summary.mean_ms.tasksMeasured, 2);
		assert.equal(report.summary.mean_ms.worstMax, 100);
		assert.equal(report.suitesCollected, 1);
	});
});

test("a `rows` row without totalPutMs or opsPerSecond is blocking and names the field", () => {
	for (const field of ["totalPutMs", "opsPerSecond"])
		withFixture((root) => {
			for (const [run, totalPutMs] of [
				["run-1", 100],
				["run-2", 125],
				["run-3", 200],
			]) {
				const row = documentPutRow("put 1024b", totalPutMs);
				if (run === "run-2") delete row[field];
				writeSuite(root, run, "document-put.json", documentPutSuite(row));
			}

			const { code } = captureCli([root]);
			assert.equal(code, 1, field);

			const report = analyze({ resultsDir: root, minRuns: 3 });
			assert.equal(report.trustworthy, false, field);
			const metric = field === "totalPutMs" ? "mean_ms" : "hz";
			const stats = findTask(report, "document-put.json", "put 1024b").metrics[
				metric
			];
			// Not skipped, not coerced to 0, not averaged over the two survivors.
			assert.equal(stats.status, "invalid", field);
			assert.deepEqual(
				stats.invalidSamples,
				[{ run: "run-2", reason: "missing (absent field)" }],
				field,
			);
			assert.equal(stats.max_abs_delta_pct, null, field);

			const problem = report.problems.find(
				(entry) => entry.code === "invalid-metric-value",
			);
			assert.equal(problem.severity, "blocking", field);
			assert.equal(problem.sourceField, field);
			assert.match(problem.message, new RegExp(`row field "${field}"`), field);
			// The other metric of the same row is untouched.
			const other = field === "totalPutMs" ? "hz" : "mean_ms";
			assert.equal(
				findTask(report, "document-put.json", "put 1024b").metrics[other]
					.status,
				"ok",
				field,
			);
		});
});

test("a suite file with neither `tasks` nor `rows` is unparseable, not empty of tasks", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		]) {
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));
			writeSuite(
				root,
				run,
				"file-ingest.json",
				run === "run-3"
					? { name: "file-ingest", meta: {} }
					: suite(task("ingest", meanMs)),
			);
		}

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, false);
		const problem = report.problems.find(
			(entry) =>
				entry.code === "suite-missing-in-run" &&
				entry.suite === "file-ingest.json",
		);
		assert.equal(problem.reason, "unparseable");
		assert.match(problem.message, /no `tasks` array and no `rows` array/);
	});

	// `tasks` wins when a file somehow carries both.
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(root, run, "document-put.json", {
				name: "document-put",
				tasks: [task("from-tasks", meanMs)],
				rows: [documentPutRow("from-rows", meanMs)],
			});

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.deepEqual(
			findSuite(report, "document-put.json").tasks.map((entry) => entry.name),
			["from-tasks"],
		);
	});
});

// --- D2: present-but-unreadable everywhere is a collection failure ----------

test("a suite unreadable in EVERY run is blocking and exits non-zero, with the reason in the JSON", () => {
	const broken = [
		{ label: "empty", body: "", reason: "empty" },
		{ label: "unparseable", body: "{not json", reason: "unparseable" },
		{
			label: "wrong shape",
			body: { name: "chunk-transfer", meta: {} },
			reason: "unparseable",
		},
		{
			label: "zero tasks",
			body: { name: "chunk-transfer", tasks: [] },
			reason: "no-tasks",
		},
	];

	for (const kind of broken)
		withFixture((root) => {
			// A quiet suite and a noisy one. The noisy one is the whole point: it is
			// the task that sets the headline, so losing it is what understates.
			for (const [run, quiet, noisy] of [
				["run-1", 1000, 1000],
				["run-2", 1001, 1600],
				["run-3", 1000.5, 1200],
			]) {
				writeSuite(
					root,
					run,
					"pid-convergence.json",
					suite(task("quiet", quiet)),
				);
				writeSuite(
					root,
					run,
					"chunk-transfer.json",
					suite(task("noisy", noisy)),
				);
			}

			// Healthy baseline: the noisy task sets a 60% headline.
			const healthy = analyze({ resultsDir: root, minRuns: 3 });
			assert.equal(healthy.trustworthy, true, kind.label);
			assert.equal(healthy.summary.mean_ms.tasksMeasured, 2, kind.label);
			assert.equal(healthy.summary.mean_ms.worstMax, 60, kind.label);

			// Now break the noisy suite in EVERY run.
			for (const run of ["run-1", "run-2", "run-3"])
				writeSuite(root, run, "chunk-transfer.json", kind.body);

			const { code, stdout, stderr } = captureCli([root]);
			assert.equal(
				code,
				1,
				`${kind.label}: a systematically broken suite must exit non-zero`,
			);
			assert.match(stderr, /blocking problem/, kind.label);

			const report = JSON.parse(
				fs.readFileSync(path.join(root, "noise-floor.json"), "utf8"),
			);
			assert.equal(report.trustworthy, false, kind.label);
			// The headline really did collapse; the point is that it is now loud.
			assert.equal(report.summary.mean_ms.tasksMeasured, 1, kind.label);
			assert.equal(report.summary.mean_ms.worstMax, 0.1, kind.label);

			const suiteReport = findSuite(report, "chunk-transfer.json");
			assert.equal(suiteReport.status, "unreadable-everywhere", kind.label);
			assert.match(suiteReport.reason, /present but unreadable/, kind.label);
			assert.deepEqual(
				suiteReport.missingReasons.map((entry) => entry.reason),
				[kind.reason, kind.reason, kind.reason],
				kind.label,
			);
			assert.ok(
				suiteReport.missingReasons.every((entry) => entry.detail),
				`${kind.label}: every missing run must carry a detail`,
			);
			// It must NOT be counted as collected.
			assert.equal(report.suitesCollected, 1, kind.label);
			assert.deepEqual(
				report.suitesUnreadableEverywhere,
				["chunk-transfer.json"],
				kind.label,
			);

			const problems = report.problems.filter(
				(entry) => entry.code === "suite-unreadable-everywhere",
			);
			assert.equal(problems.length, 3, kind.label);
			assert.ok(
				problems.every((entry) => entry.severity === "blocking"),
				kind.label,
			);
			assert.ok(
				problems.every((entry) => entry.suite === "chunk-transfer.json"),
				kind.label,
			);
			assert.equal(problems[0].reason, kind.reason, kind.label);
			// And it must not be filed as a benign deselection.
			assert.equal(
				report.problems.filter(
					(entry) =>
						entry.code === "suite-not-collected" &&
						entry.suite === "chunk-transfer.json",
				).length,
				0,
				kind.label,
			);

			assert.match(stdout, /NOT TRUSTWORTHY/, kind.label);
			assert.match(
				stdout,
				/Suites present but unreadable in every run: chunk-transfer\.json/,
				kind.label,
			);
			assert.match(stdout, /Present but unreadable in every run/, kind.label);
		});
});

test("a suite that is unreadable in some runs and absent in the rest is still blocking", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));
		// Present-but-broken in run-1 only; simply absent from run-2 and run-3.
		writeSuite(root, "run-1", "file-ingest.json", "{not json");

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, false);
		const suiteReport = findSuite(report, "file-ingest.json");
		assert.equal(suiteReport.status, "unreadable-everywhere");
		assert.deepEqual(
			suiteReport.missingReasons.map((entry) => `${entry.run}:${entry.reason}`),
			["run-1:unparseable", "run-2:missing", "run-3:missing"],
		);
		const problems = report.problems.filter(
			(entry) => entry.code === "suite-unreadable-everywhere",
		);
		// Only the run where the file actually existed is accused.
		assert.equal(problems.length, 1);
		assert.equal(problems[0].run, "run-1");
	});
});

test("a suite genuinely absent (ENOENT) everywhere stays a warning, and says why", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));

		const { code, stdout } = captureCli([root]);
		assert.equal(code, 0, "a deselected suite is not a failure");

		const report = analyze({ resultsDir: root, minRuns: 3 });
		const suiteReport = findSuite(report, "document-put.json");
		assert.equal(suiteReport.status, "not-collected");
		assert.match(suiteReport.reason, /absent \(file not found\) in every/);
		assert.deepEqual(
			suiteReport.missingReasons.map((entry) => entry.reason),
			["missing", "missing", "missing"],
		);
		const warning = report.problems.find(
			(entry) =>
				entry.code === "suite-not-collected" &&
				entry.suite === "document-put.json",
		);
		assert.equal(warning.severity, "warning");
		assert.equal(warning.reason, "missing");
		assert.equal(report.suitesUnreadableEverywhere.length, 0);
		assert.match(
			stdout,
			/Not collected in any run \(absent \(file not found\)/,
		);
	});
});

// --- D3: the headline summary itself ---------------------------------------

test("the headline aggregates per-task maxima and names the worst task", () => {
	withFixture((root) => {
		// Three tasks with deliberately different spreads, across two suites, so
		// both the aggregate and the identity of the worst task are observable.
		//   steady   1000, 1002, 1001 -> max 0.2%
		//   middling 1000, 1070, 1030 -> max 7%
		//   wild     1000, 1750, 1200 -> max 75%
		for (const [run, steady, middling, wild] of [
			["run-1", 1000, 1000, 1000],
			["run-2", 1002, 1070, 1750],
			["run-3", 1001, 1030, 1200],
		]) {
			writeSuite(
				root,
				run,
				"pid-convergence.json",
				suite(task("steady", steady), task("middling", middling)),
			);
			writeSuite(root, run, "chunk-transfer.json", suite(task("wild", wild)));
		}

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, true);

		const perTask = (file, name) =>
			findTask(report, file, name).metrics.mean_ms.max_abs_delta_pct;
		assert.equal(perTask("pid-convergence.json", "steady"), 0.2);
		assert.equal(perTask("pid-convergence.json", "middling"), 7);
		assert.equal(perTask("chunk-transfer.json", "wild"), 75);

		const summary = report.summary.mean_ms;
		assert.equal(summary.tasksMeasured, 3);
		// Median of {0.2, 7, 75}. NOT the min (0.2) and NOT the max (75).
		assert.equal(summary.medianOfMax, 7);
		assert.equal(summary.worstMax, 75);
		assert.deepEqual(summary.worstTask, {
			suite: "chunk-transfer.json",
			task: "wild",
		});
		assert.equal(summary.tasksAbove5Pct, 2);
		assert.equal(summary.tasksAbove10Pct, 1);
		assert.equal(summary.noVariationTasks, 0);
		// The secondary p95 aggregate is pinned too, at its own values.
		assert.equal(summary.medianOfP95, 7);
		assert.equal(summary.worstP95, 75);
		// hz is reciprocal-invariant, so the same aggregate must fall out of it.
		assert.equal(report.summary.hz.medianOfMax, 7);
		assert.deepEqual(report.summary.hz.worstTask, summary.worstTask);

		// ...and the rendered headline carries all three numbers.
		const markdown = renderMarkdown(report);
		assert.match(
			markdown,
			/\| mean_ms \| 3 \| 7\.00% \| 75\.00% \| chunk-transfer\.json › wild \| 2 \| 1 \| 0 \|/,
		);
		assert.match(markdown, /median max \\\|Δ\\\|/);
		assert.match(markdown, /Secondary: p95 over 3 pair\(s\) per task/);
	});

	// A tie is broken by (suite, task) so the named worst task is stable, and the
	// tie-break must not be able to pick the quiet task instead.
	withFixture((root) => {
		for (const [run, wild] of [
			["run-1", 1000],
			["run-2", 1750],
			["run-3", 1200],
		]) {
			writeSuite(root, run, "chunk-transfer.json", suite(task("wild", wild)));
			writeSuite(root, run, "file-ingest.json", suite(task("wild", wild)));
			writeSuite(root, run, "pid-convergence.json", suite(task("calm", 1000)));
		}

		const summary = analyze({ resultsDir: root, minRuns: 3 }).summary.mean_ms;
		assert.equal(summary.worstMax, 75);
		assert.deepEqual(summary.worstTask, {
			suite: "chunk-transfer.json",
			task: "wild",
		});
		// {0, 75, 75} -> median 75, so a "median" that quietly became a min or a
		// mean would not survive here either.
		assert.equal(summary.medianOfMax, 75);
	});
});

test("tasks without a name are blocking, and a file of only unnamed rows has no tasks", () => {
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(
				root,
				run,
				"pid-convergence.json",
				// The unnamed entry cannot be matched across runs, so it is refused
				// rather than positionally guessed at.
				`{"name":"fixture","tasks":[{"name":"T","mean_ms":${meanMs},"hz":${1000 / meanMs}},{"mean_ms":1,"hz":1000}]}`,
			);

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, false);
		const problems = report.problems.filter(
			(entry) => entry.code === "unnamed-task",
		);
		assert.equal(problems.length, 3, "one per run");
		assert.ok(problems.every((entry) => entry.severity === "blocking"));
		assert.match(problems[0].message, /1 task\(s\) without a name/);
		// The named task in the same file is still measured.
		assert.equal(
			findTask(report, "pid-convergence.json", "T").metrics.mean_ms.status,
			"ok",
		);
	});

	// Unnamed rows in the `rows` shape count the same way, and a file made only
	// of them is "no-tasks" rather than silently empty.
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		]) {
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));
			const anonymous = documentPutRow("put", meanMs);
			delete anonymous.name;
			writeSuite(root, run, "document-put.json", documentPutSuite(anonymous));
		}

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(report.trustworthy, false);
		assert.equal(
			findSuite(report, "document-put.json").status,
			"unreadable-everywhere",
		);
		const problem = report.problems.find(
			(entry) => entry.code === "suite-unreadable-everywhere",
		);
		assert.equal(problem.reason, "no-tasks");
		assert.match(
			problem.detail,
			/zero named rows \(1 entr\(y\/ies\) had no name\)/,
		);
	});
});

test("the coarse-p95 note appears exactly when nearest-rank p95 is the maximum", () => {
	// Derived, not a hard-coded pair count: ceil(0.95 * pairs) === pairs.
	assert.equal(p95IsJustTheMaximum(0), false);
	assert.equal(p95IsJustTheMaximum(10), true);
	assert.equal(p95IsJustTheMaximum(19), true);
	assert.equal(p95IsJustTheMaximum(20), false);
	assert.equal(p95IsJustTheMaximum(21), false);

	const noteFor = (runCount) =>
		withFixture((root) => {
			for (let index = 1; index <= runCount; index++)
				writeSuite(
					root,
					`run-${index}`,
					"pid-convergence.json",
					suite(task("T", 100 + index)),
				);
			return analyze({ resultsDir: root, minRuns: 3 });
		});

	// 3 runs -> 3 pairs: the "p95" is the maximum and says so.
	const coarse = noteFor(3);
	const note = coarse.problems.find((entry) => entry.code === "coarse-p95");
	assert.equal(note.severity, "note");
	assert.equal(note.pairs, 3);
	assert.match(note.message, /ceil\(0\.95 \* 3\) = 3/);
	assert.match(note.message, /the headline is the max either way/);
	assert.equal(coarse.summary.mean_ms.p95IsJustTheMaximum, true);
	assert.equal(coarse.summary.mean_ms.pairsPerTask, 3);
	assert.match(renderMarkdown(coarse), /so this p95 \*\*is\*\* the maximum/);

	// 7 runs -> 21 pairs: p95 finally separates from the maximum, so the note
	// disappears and the report stops claiming the two are the same.
	const fine = noteFor(7);
	assert.equal(
		fine.problems.filter((entry) => entry.code === "coarse-p95").length,
		0,
	);
	assert.equal(fine.summary.mean_ms.p95IsJustTheMaximum, false);
	assert.equal(fine.summary.mean_ms.pairsPerTask, 21);
	assert.match(
		renderMarkdown(fine),
		/this p95 sits below the observed maximum/,
	);
});

// --- D4: the headline must not shrink when runs are added ------------------

test("adding runs never lowers the headline, though it does lower the p95", () => {
	withFixture((root) => {
		const write = (run, meanMs) =>
			writeSuite(root, run, "pid-convergence.json", suite(task("T", meanMs)));
		// One outlier pair (90 vs 110) sets the true worst A/A gap at 22.22%.
		for (const [run, meanMs] of [
			["run-1", 90],
			["run-2", 100],
			["run-3", 100],
			["run-4", 100],
			["run-5", 110],
		])
			write(run, meanMs);

		const five = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(five.pairsPerTask, 10);
		assert.equal(five.summary.mean_ms.worstMax, 22.222222);
		assert.equal(five.summary.mean_ms.worstP95, 22.222222);
		assert.equal(five.summary.mean_ms.p95IsJustTheMaximum, true);

		// Two more runs of the SAME code, contributing no new extreme.
		write("run-6", 100);
		write("run-7", 100);

		const seven = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(seven.pairsPerTask, 21);
		// The defect this pins: p95 HALVES on unchanged data purely because the
		// nearest rank moved off the last element.
		assert.equal(seven.summary.mean_ms.worstP95, 11.111111);
		assert.equal(seven.summary.mean_ms.p95IsJustTheMaximum, false);
		// The headline does not move, because pairs only accumulate.
		assert.equal(seven.summary.mean_ms.worstMax, 22.222222);
		assert.ok(
			seven.summary.mean_ms.worstMax >= five.summary.mean_ms.worstMax,
			"more runs must never lower the headline for unchanged data",
		);
		assert.ok(
			seven.summary.mean_ms.medianOfMax >= five.summary.mean_ms.medianOfMax,
			"more runs must never lower the median headline either",
		);
		assert.ok(
			seven.summary.mean_ms.worstP95 < five.summary.mean_ms.worstP95,
			"the p95 is expected to fall here; that is why it is not the headline",
		);

		// The decision rule the report states must point at the max, and the
		// headline table must carry it.
		const markdown = renderMarkdown(seven);
		assert.match(markdown, /\| mean_ms \| 1 \| 22\.22% \| 22\.22% \|/);
		assert.match(markdown, /exceeds T's A\/A max \\?\|Δ%\\?\|/);
		assert.match(markdown, /observed maximum/);
		assert.match(seven.headlineStatistic, /^max \|Δ%\|/);
		assert.match(seven.decisionRule, /max \|Δ%\|/);
	});
});

test("byte-identical run directories are called out instead of yielding a silent 0% floor", () => {
	withFixture((root) => {
		for (const run of ["run-1", "run-2", "run-3"])
			writeSuite(root, run, "file-ingest.json", suite(task("ingest", 12.5)));

		const { code, stdout } = captureCli([root]);
		// Still a warning, not a refusal: a genuinely deterministic model suite
		// can legitimately emit the same bytes twice.
		assert.equal(code, 0);
		const report = analyze({ resultsDir: root, minRuns: 3 });
		const warning = report.problems.find(
			(entry) => entry.code === "duplicate-run-input",
		);
		assert.equal(warning.severity, "warning");
		assert.deepEqual(warning.runs, ["run-1", "run-2", "run-3"]);
		assert.match(warning.message, /fabricated 0%/);
		assert.match(stdout, /duplicate-run-input/);
	});

	// Control: real runs differ, so no warning.
	withFixture((root) => {
		for (const [run, meanMs] of [
			["run-1", 100],
			["run-2", 110],
			["run-3", 90],
		])
			writeSuite(root, run, "file-ingest.json", suite(task("ingest", meanMs)));

		const report = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(
			report.problems.filter((entry) => entry.code === "duplicate-run-input")
				.length,
			0,
		);
	});
});

// Real data, run 31827888077: chunk-transfer's completion-lag tasks are
// structurally 0 whenever the ordering runs the other way, in every run. A
// relative delta against 0 is undefined however healthy the benchmark is, so
// blocking on them condemned an otherwise clean transport run. Blocking is for
// what can BIAS the floor; a metric that never yields a value in any run never
// reaches the summary, so it cannot.
test("a metric invalid in EVERY run warns; invalid in only SOME runs still blocks", () => {
	withFixture((root) => {
		const rows = (lagAlwaysZero, sometimes) => [
			{ name: "silent: sender-complete", mean_ms: "19.1", hz: "52.3" },
			// The real shape: mean_ms 0 and hz null, in every run.
			{
				name: "silent: sender-after-receiver",
				mean_ms: lagAlwaysZero,
				hz: "null",
			},
			{ name: "flaky-task", mean_ms: sometimes, hz: sometimes },
		];
		writeSuite(root, "run-1", "chunk-transfer.json", rawSuite(rows("0", "10")));
		writeSuite(root, "run-2", "chunk-transfer.json", rawSuite(rows("0", "11")));
		writeSuite(root, "run-3", "chunk-transfer.json", rawSuite(rows("0", "12")));

		const withFlakeOk = analyze({ resultsDir: root, minRuns: 3 });
		// The always-zero lag alone must NOT condemn the report.
		assert.equal(
			withFlakeOk.trustworthy,
			true,
			"an always-invalid metric must not be blocking",
		);
		const lag = findTask(
			withFlakeOk,
			"chunk-transfer.json",
			"silent: sender-after-receiver",
		);
		assert.equal(lag.metrics.mean_ms.status, "never-measurable");
		assert.equal(lag.metrics.mean_ms.max_abs_delta_pct, null);
		assert.ok(
			codes(withFlakeOk, "warning").includes("metric-never-measurable"),
			"it must still be listed loudly as a warning",
		);
		assert.ok(
			!codes(withFlakeOk, "blocking").includes("metric-never-measurable"),
		);
		// And it must say the task tells you nothing, not merely that it is absent.
		const note = withFlakeOk.problems.find(
			(problem) => problem.code === "metric-never-measurable",
		);
		assert.match(note.message, /tells you NOTHING/);
		assert.equal(note.neverMeasurable, true);

		// Now make the OTHER task invalid in one run only: that biases the floor
		// toward the runs that worked, so it must block.
		writeSuite(root, "run-3", "chunk-transfer.json", rawSuite(rows("0", "0")));
		const withFlakeBroken = analyze({ resultsDir: root, minRuns: 3 });
		assert.equal(withFlakeBroken.trustworthy, false);
		const blocking = codes(withFlakeBroken, "blocking");
		assert.ok(blocking.includes("invalid-metric-value"));
		assert.equal(
			findTask(withFlakeBroken, "chunk-transfer.json", "flaky-task").metrics
				.mean_ms.status,
			"invalid",
		);
		// The always-zero lag is still only a warning even in this run.
		assert.ok(!blocking.includes("metric-never-measurable"));
	});
});

// --------------------------------------------------------------------------
// --emit-baseline: the committable file.

test("the emitted baseline carries the per-task floor, rounded away from zero", () => {
	// Rounding a floor DOWN would let the rounding step itself turn noise into a
	// signal: a task whose identical code moved 2.7249% must not be published as
	// 2.72%, because a later A/B delta of 2.73% would then read as "above the
	// floor". 1.1 is the float trap -- Math.ceil(1.1 * 100) / 100 is 1.11.
	assert.equal(ceilTo(2.7249, 2), 2.73);
	assert.equal(ceilTo(2.72, 2), 2.72);
	assert.equal(ceilTo(1.1, 2), 1.1);
	assert.equal(ceilTo(0, 2), 0);
	assert.equal(ceilTo(null, 2), null);

	withFixture((root) => {
		baselineFixture(root);
		const out = path.join(root, "baseline.json");
		const { code, stderr } = captureCli([root, "--emit-baseline", out]);
		assert.equal(code, 0);

		const baseline = readBaseline(out);
		assert.equal(baseline.schema, "peerbit-bench-noise-floor-baseline/1");
		assert.equal(baseline.status, "measured");
		assert.equal(baseline.provenance.tasksPublished, 1);

		// 100 vs 102.7249 is the widest pair, so the floor is 2.7249% -> 2.73%,
		// and |Δ%| is reciprocal-invariant so hz reports the same number. Read the
		// way the consumer reads it: tasks[<suite file>][<task name>][<metric>].
		assert.equal(consumerFloor(baseline, "pid-convergence.json", "T", "mean_ms"), 2.73);
		assert.equal(consumerFloor(baseline, "pid-convergence.json", "T", "hz"), 2.73);
		const entry = baseline.tasks["pid-convergence.json"].T;
		assert.equal(entry.n, 3);
		// `unusable` is present only when something is unusable.
		assert.equal("unusable" in entry, false);
		// A task the baseline has never heard of stays unknown -- no inheriting
		// the floor of a neighbour, and no suite-level default.
		assert.equal(consumerFloor(baseline, "pid-convergence.json", "T2", "mean_ms"), null);
		assert.equal(consumerFloor(baseline, "file-ingest.json", "T", "mean_ms"), null);

		// The A/A report's own numbers travel with it, so a reader can see how
		// wide the whole distribution was without the artifact.
		assert.equal(baseline.headline.mean_ms.tasksMeasured, 1);
		assert.equal(baseline.headline.mean_ms.worstMax, 2.73);
		assert.deepEqual(baseline.headline.mean_ms.worstTask, {
			suite: "pid-convergence.json",
			task: "T",
		});
		assert.equal(baseline.provenance.runs, 3);
		assert.equal(baseline.provenance.pairsPerTask, 3);
		assert.match(stderr, /baseline written to/);
	});
});

test("the baseline is byte-deterministic with sorted keys and stable task order", () => {
	withFixture((root) => {
		// Two suites whose order in the SUITES list is the REVERSE of their
		// alphabetical order (sync-batch-sweep is declared before pid-convergence),
		// and tasks written in reverse order within a file. Anything that leans on
		// discovery order instead of sorting therefore shows up as a diff.
		for (const [run, offset] of [
			["run-1", 0],
			["run-2", 1],
			["run-3", 3],
		]) {
			writeSuite(
				root,
				run,
				"sync-batch-sweep.json",
				suite(task("zeta", 10 + offset), task("alpha", 20 + offset)),
			);
			writeSuite(
				root,
				run,
				"pid-convergence.json",
				suite(task("beta", 30 + offset)),
			);
		}
		writeMetadata(root);

		const first = path.join(root, "one.json");
		const second = path.join(root, "two.json");
		assert.equal(captureCli([root, "--emit-baseline", first]).code, 0);
		assert.equal(captureCli([root, "--emit-baseline", second]).code, 0);
		const bytes = fs.readFileSync(first, "utf8");
		assert.equal(bytes, fs.readFileSync(second, "utf8"));

		// Keys sorted at every depth -- including the suite and task names, which
		// are data -- independent of the SUITES declaration order, so reordering
		// that list can never produce a committed diff with no content in it.
		const baseline = JSON.parse(bytes);
		const sorted = (value) => [...Object.keys(value)].sort();
		assert.deepEqual(Object.keys(baseline.tasks), [
			"pid-convergence.json",
			"sync-batch-sweep.json",
		]);
		assert.deepEqual(Object.keys(baseline.tasks["sync-batch-sweep.json"]), [
			"alpha",
			"zeta",
		]);
		assert.deepEqual(Object.keys(baseline), sorted(baseline));
		assert.deepEqual(
			Object.keys(baseline.provenance),
			sorted(baseline.provenance),
		);

		// Two-space indent, trailing newline, and the serialiser rather than the
		// construction order is what guarantees the ordering, so the committed
		// file re-serialises to itself byte for byte.
		assert.equal(stringifyBaseline(baseline), bytes);
		assert.ok(bytes.endsWith("}\n"));
	});
});

test("provenance is read from metadata.json, never invented", () => {
	withFixture((root) => {
		baselineFixture(root);
		const out = path.join(root, "baseline.json");
		assert.equal(captureCli([root, "--emit-baseline", out]).code, 0);

		const { provenance } = readBaseline(out);
		// Every one of these is quoted straight from the file the workflow wrote.
		// A baseline whose ref/sha/run URL came from the machine that happened to
		// run the emitter would point at the wrong commit entirely. The camelCase
		// spelling is the consumer's: benchmarks.yml reads p.runUrl / p.measuredAt
		// / p.pairsPerTask, and a snake_case producer renders "unknown date" and
		// "_not recorded_" while looking perfectly well-formed.
		assert.equal(provenance.ref, METADATA.ref);
		assert.equal(provenance.sha, METADATA.sha);
		assert.equal(provenance.runUrl, METADATA.run_url);
		assert.equal(provenance.measuredAt, METADATA.measured_at);
		assert.match(provenance.measuredAtSource, /field measured_at/);
		assert.deepEqual(provenance.selectedSuites, METADATA.selected_suites);
		assert.equal(provenance.repeatsRequested, METADATA.repeats);
		assert.equal(provenance.runs, 3);
		assert.equal(provenance.pairsPerTask, 3);
		assert.equal(provenance.suitesKnown, 7);
		assert.equal(provenance.suitesCollected, 1);
		assert.equal(
			provenance.generator,
			"scripts/bench/noise-floor.mjs --emit-baseline",
		);
	});

	// The artifacts measured before the workflow recorded a timestamp still have
	// to yield a dated baseline, so the file's own mtime stands in -- and says
	// so, because a weaker source a reader can see beats a fabricated one.
	withFixture((root) => {
		baselineFixture(root, [100, 101, 102], { measured_at: undefined });
		// Pinned to a distinctly past mtime on purpose: a fixture written moments
		// ago has an mtime of ~now, so a fallback that quietly used `new Date()`
		// would pass by coincidence.
		const metadataFile = path.join(root, "metadata.json");
		const measured = new Date("2026-02-03T04:05:06.789Z");
		fs.utimesSync(metadataFile, measured, measured);

		const out = path.join(root, "baseline.json");
		assert.equal(captureCli([root, "--emit-baseline", out]).code, 0);

		const { provenance } = readBaseline(out);
		assert.equal(provenance.measuredAt, "2026-02-03T04:05:06.789Z");
		assert.equal(
			provenance.measuredAt,
			fs.statSync(metadataFile).mtime.toISOString(),
		);
		assert.match(provenance.measuredAtSource, /mtime/);
	});
});

test("a floor of zero is published as unknown, never as a usable 0%", () => {
	withFixture((root) => {
		// Three shapes in one suite: a task that never moved, a task whose metric
		// is structurally 0 in every run (chunk-transfer's completion lags), and a
		// task that actually varied.
		const rows = (moving) => [
			{ name: "identical", mean_ms: "12.5", hz: "80" },
			{ name: "lag", mean_ms: "0", hz: "null" },
			{ name: "moving", mean_ms: moving, hz: `${1000 / Number(moving)}` },
		];
		writeSuite(root, "run-1", "chunk-transfer.json", rawSuite(rows("10")));
		writeSuite(root, "run-2", "chunk-transfer.json", rawSuite(rows("11")));
		writeSuite(root, "run-3", "chunk-transfer.json", rawSuite(rows("12")));
		writeMetadata(root);

		const out = path.join(root, "baseline.json");
		assert.equal(captureCli([root, "--emit-baseline", out]).code, 0);
		const baseline = readBaseline(out);

		// A committed 0 would be permanent permission: benchmarks.yml's taskFloor()
		// accepts any finite value >= 0, so every later A/B delta on that task
		// would clear it. Identical runs mean no variation was RESOLVED, which is
		// not the same as a task that cannot move -- so the floor must read as
		// unknown, and the reason has to survive somewhere a human can see it.
		const floors = baseline.tasks["chunk-transfer.json"];
		assert.equal(
			consumerFloor(baseline, "chunk-transfer.json", "identical", "mean_ms"),
			null,
		);
		assert.equal(floors.identical.unusable.mean_ms, "no-variation-resolved");
		assert.equal(floors.identical.unusable.hz, "no-variation-resolved");

		assert.equal(
			consumerFloor(baseline, "chunk-transfer.json", "lag", "mean_ms"),
			null,
		);
		assert.equal(floors.lag.unusable.mean_ms, "never-measurable");

		// The real one still publishes a floor: 10 -> 12 is 20%.
		assert.equal(
			consumerFloor(baseline, "chunk-transfer.json", "moving", "mean_ms"),
			20,
		);

		// Nothing anywhere in the file is a zero floor, and the key is present
		// holding null rather than dropped -- both readings have to be unknown.
		for (const [name, entry] of Object.entries(floors))
			for (const metric of ["mean_ms", "hz"]) {
				assert.ok(Object.hasOwn(entry, metric), `${name} dropped ${metric}`);
				assert.notEqual(entry[metric], 0, `${name} published a 0% floor`);
			}
	});
});

test("refuses to emit a baseline from a NOT TRUSTWORTHY report", () => {
	withFixture((root) => {
		// A task that appears in only two of three runs: blocking, because the
		// runs that produced it are the runs where nothing went wrong.
		writeSuite(root, "run-1", "pid-convergence.json", suite(task("T", 100)));
		writeSuite(root, "run-2", "pid-convergence.json", suite(task("T", 101)));
		writeSuite(
			root,
			"run-3",
			"pid-convergence.json",
			suite(task("T", 102), task("only-here", 5)),
		);
		writeMetadata(root);

		const out = path.join(root, "baseline.json");
		const { code, stdout, stderr } = captureCli([root, "--emit-baseline", out]);
		assert.equal(code, 1);
		// Refusing must leave NOTHING behind: a half-written baseline would be
		// committed as readily as a good one.
		assert.equal(fs.existsSync(out), false);
		assert.match(stderr, /refusing to emit a committable baseline/);
		assert.match(stderr, /NOT TRUSTWORTHY/);
		// The diagnosis is still printed, which is the whole point of publishing
		// the report even when it is unusable.
		assert.match(stdout, /task-missing-in-run/);

		// Control: with the stray task removed the same directory emits.
		writeSuite(root, "run-3", "pid-convergence.json", suite(task("T", 102)));
		assert.equal(captureCli([root, "--emit-baseline", out]).code, 0);
		assert.equal(fs.existsSync(out), true);
	});
});

test("refuses a baseline from too few runs, or from a partial run", () => {
	// Two runs give ONE pair, so the "max" is a single observation with no tail.
	// The REPORT may be published from that; a committed file may not, and
	// --min-runs 2 must not be able to buy its way past that.
	withFixture((root) => {
		baselineFixture(root, [100, 110], { repeats: 2 });
		const out = path.join(root, "baseline.json");

		const plain = captureCli([root, "--min-runs", "2"]);
		assert.equal(plain.code, 0, "the report itself is publishable");

		const { code, stderr } = captureCli([
			root,
			"--min-runs",
			"2",
			"--emit-baseline",
			out,
		]);
		assert.equal(code, 1);
		assert.equal(fs.existsSync(out), false);
		assert.match(stderr, /only 2 usable run\(s\); a committed baseline needs/);
	});

	// A run that asked for 4 repeats and produced 3 is a NON-RANDOM subset --
	// the repeats that finished -- and its floor reads low. The report cannot see
	// this on its own: --min-runs 3 makes it trustworthy. metadata.json can.
	withFixture((root) => {
		baselineFixture(root, [100, 101, 102], { repeats: 4 });
		const out = path.join(root, "baseline.json");

		assert.equal(captureCli([root]).code, 0, "the report itself is publishable");
		const { code, stderr } = captureCli([root, "--emit-baseline", out]);
		assert.equal(code, 1);
		assert.equal(fs.existsSync(out), false);
		assert.match(stderr, /asked for 4 repeat\(s\) but only 3/);
	});

	// Every task unmeasurable: the file would answer "unknown" for every row it
	// was ever consulted about, which is the placeholder's job, not a
	// measurement's.
	withFixture((root) => {
		for (const run of ["run-1", "run-2", "run-3"])
			writeSuite(
				root,
				run,
				"chunk-transfer.json",
				rawSuite([{ name: "lag", mean_ms: "0", hz: "null" }]),
			);
		writeMetadata(root);
		const out = path.join(root, "baseline.json");
		const { code, stderr } = captureCli([root, "--emit-baseline", out]);
		assert.equal(code, 1);
		assert.equal(fs.existsSync(out), false);
		assert.match(stderr, /not one \(suite, task, metric\) produced a usable floor/);
	});

	// The same refusal is what stops copied run directories from being committed
	// as a floor of zeros. analyze() only WARNS about byte-identical runs (a
	// deterministic model suite can legitimately produce them), so the report
	// stays publishable -- but every metric comes out no-variation-resolved, and
	// a file that blesses every future A/B delta on every task must not be
	// committed on the strength of a warning nobody read.
	withFixture((root) => {
		for (const run of ["run-1", "run-2", "run-3"])
			writeSuite(root, run, "file-ingest.json", suite(task("ingest", 12.5)));
		writeMetadata(root);
		const out = path.join(root, "baseline.json");

		assert.equal(captureCli([root]).code, 0, "the report itself is publishable");
		const { code, stderr } = captureCli([root, "--emit-baseline", out]);
		assert.equal(code, 1);
		assert.equal(fs.existsSync(out), false);
		assert.match(stderr, /not one \(suite, task, metric\) produced a usable floor/);
	});
});

test("refuses a baseline when metadata.json is absent, malformed or not ours", () => {
	// Exit 2, not 1: the directory is not a noise-floor results directory at all,
	// which is a different thing from a measurement that came out badly.
	withFixture((root) => {
		baselineFixture(root);
		fs.rmSync(path.join(root, "metadata.json"));
		const out = path.join(root, "baseline.json");
		const { code, stderr } = captureCli([root, "--emit-baseline", out]);
		assert.equal(code, 2);
		assert.equal(fs.existsSync(out), false);
		assert.match(stderr, /metadata\.json: not found/);
		assert.match(stderr, /cannot be traced to a commit/);
	});

	const cases = [
		["unparseable", { raw: "{ not json" }, /JSON parse error/],
		["not an object", { raw: "[]\n" }, /is not a JSON object/],
		[
			"another job's metadata",
			{ kind: "benchmark-ab" },
			/does not look like a noise-floor results directory/,
		],
		["no ref", { ref: "" }, /ref must be a non-empty string/],
		["no sha", { sha: undefined }, /sha must be a hex commit id/],
		["repeats as text", { repeats: "3" }, /repeats must be a whole number/],
		[
			"no suites",
			{ selected_suites: [] },
			/selected_suites must be a non-empty array/,
		],
		// This is what "Record run metadata" literally writes when it is run
		// outside Actions: every GITHUB_* variable interpolates as "undefined".
		[
			"a run URL from outside Actions",
			{ run_url: "undefined/undefined/actions/runs/undefined" },
			/run_url must look like/,
		],
		[
			"a non-ISO measured_at",
			{ measured_at: "last tuesday" },
			/measured_at, when present, must be an ISO-8601 timestamp/,
		],
	];
	for (const [label, overrides, expected] of cases)
		withFixture((root) => {
			baselineFixture(root, [100, 101, 102], overrides);
			const out = path.join(root, "baseline.json");
			const { code, stderr } = captureCli([root, "--emit-baseline", out]);
			assert.equal(code, 2, `${label} must be refused with exit 2`);
			assert.equal(fs.existsSync(out), false, `${label} wrote a baseline`);
			assert.match(stderr, expected, label);
		});

	// The reader is a plain function, so the refusal is available to callers that
	// never touch the CLI.
	withFixture((root) => {
		baselineFixture(root);
		assert.equal(readMeasurementMetadata(root).sha, METADATA.sha);
		assert.throws(
			() => readMeasurementMetadata(path.join(root, "run-1")),
			(error) => error.exitCode === 2,
		);
	});
});

test("the committed placeholder has the emitted shape and answers unknown everywhere", () => {
	const bytes = fs.readFileSync(COMMITTED_BASELINE, "utf8");
	const placeholder = JSON.parse(bytes);

	// It is a placeholder, loudly: nothing in it may be read as a measurement.
	assert.equal(placeholder.status, "not-yet-measured");
	assert.deepEqual(placeholder.tasks, {});
	assert.equal(placeholder.provenance.ref, null);
	assert.equal(placeholder.provenance.sha, null);
	assert.equal(placeholder.provenance.runUrl, null);
	assert.equal(placeholder.provenance.measuredAt, null);
	assert.equal(placeholder.provenance.runs, 0);
	assert.equal(placeholder.provenance.tasksPublished, 0);
	for (const metric of ["mean_ms", "hz"]) {
		assert.equal(placeholder.headline[metric].tasksMeasured, 0);
		assert.equal(placeholder.headline[metric].worstMax, null);
	}

	// The consumer's "unknown" branch, exercised against the real committed file:
	// `tasks` must be a plain OBJECT (an array reads to benchmarks.yml as a
	// broken baseline, which is a louder and less accurate story than "nothing
	// measured yet"), and every lookup through it must come back null.
	assert.equal(Array.isArray(placeholder.tasks), false);
	assert.equal(typeof placeholder.tasks, "object");
	assert.notEqual(placeholder.tasks, null);
	assert.equal(
		consumerFloor(placeholder, "pid-convergence.json", "anything", "mean_ms"),
		null,
	);
	// Committed in the emitter's own serialisation, so a hand-edit that breaks
	// key order or indentation is caught here rather than in a review diff.
	assert.equal(stringifyBaseline(placeholder), bytes);

	// The consuming workflow reads one shape. If the emitter grows a field the
	// placeholder does not have, the "not yet measured" path stops exercising the
	// real thing and starts exercising a fossil.
	const emitted = withFixture((root) => {
		baselineFixture(root);
		const out = path.join(root, "baseline.json");
		assert.equal(captureCli([root, "--emit-baseline", out]).code, 0);
		return readBaseline(out);
	});
	assert.deepEqual(Object.keys(placeholder), Object.keys(emitted));
	assert.deepEqual(
		Object.keys(placeholder.provenance),
		Object.keys(emitted.provenance),
	);
	assert.deepEqual(Object.keys(placeholder.headline), Object.keys(emitted.headline));
	for (const metric of ["mean_ms", "hz"])
		assert.deepEqual(
			Object.keys(placeholder.headline[metric]),
			Object.keys(emitted.headline[metric]),
		);
	assert.equal(placeholder.schema, emitted.schema);
	assert.equal(placeholder.decisionRule, emitted.decisionRule);
	assert.equal(placeholder.deltaDefinition, emitted.deltaDefinition);
	assert.equal(placeholder.headlineStatistic, emitted.headlineStatistic);
	assert.equal(placeholder.rounding, emitted.rounding);
	assert.equal(placeholder.provenance.generator, emitted.provenance.generator);
	assert.equal(placeholder.provenance.suitesKnown, emitted.provenance.suitesKnown);
	assert.notEqual(placeholder.status, emitted.status);
});

test("buildBaseline is callable without the CLI and refuses the same things", () => {
	withFixture((root) => {
		baselineFixture(root);
		const report = analyze({ resultsDir: root, minRuns: 3 });
		const metadata = readMeasurementMetadata(root);
		assert.equal(
			buildBaseline({ report, metadata }).provenance.tasksPublished,
			1,
		);
		assert.throws(
			() => buildBaseline({ report, metadata: { ...metadata, repeats: 9 } }),
			(error) => error.exitCode === 1 && /only 3 produced/.test(error.message),
		);
	});
});
