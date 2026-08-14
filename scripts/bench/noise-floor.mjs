// Turns "how big does a benchmark delta have to be before it means anything?"
// into a measured number.
//
// .github/workflows/benchmarks.yml checks out a PR's base and head onto one
// runner, runs the same suites twice, and prints a per-task "Δ mean %". Nothing
// in that report says how large a Δ has to be to be a signal. Folklore from
// earlier sessions: base-vs-base of *identical* code moved by ±10%, and
// requestMaybeSyncTotal swung 30-70% between identical runs. This script
// consumes N repeats of the SAME commit (an A/A experiment) and reports, per
// (suite, task, metric), the distribution of |Δ| that identical code produces.
//
// Usage:
//   node scripts/bench/noise-floor.mjs <results-dir> [--min-runs <n>] [--json-out <path>]
//
// <results-dir> holds run-1/ ... run-N/, each containing the suite JSON files
// benchmarks.yml already writes (same names).
//
// TWO SUITE SHAPES, BOTH REAL. Six of the seven suites emit tinybench's
// { tasks: [{ name, mean_ms, hz, ... }] }. document-put.json does NOT: it emits
// { name, rows: [{ name, iterations, payloadBytes, opsPerSecond, cleanupMs,
// ...profile fields..., totalPutMs }], meta } -- see
// packages/programs/data/document/document/benchmark/document-put.ts, which
// builds a BenchRow per scenario and writes `rows`, never `tasks`. Assuming the
// tasks shape made this script drop the whole document suite; see
// readSuiteFile for the normalisation and for why the mapping is what it is.
//
// (NOTE for a later change, deliberately NOT fixed here: .github/workflows/
// benchmarks.yml makes the same wrong assumption in its A/B report, which is
// why its "document: put" table has been rendering empty. Out of scope for this
// file.)
//
// THE HEADLINE STATISTIC. For each (suite, task, metric) we take all C(N,2)
// unordered pairs of runs and report median / p95 / max of
//
//   |Δ%| = |a - b| / min(a, b) * 100
//
// and the HEADLINE -- the number a reader is meant to quote, and the one the
// decision rule points at -- is the observed MAXIMUM, not the p95. Three
// reasons, all the same reason:
//
//   * Understating the floor is the catastrophic direction. It makes every
//     perf claim look significant. The max is the only one of the three that
//     can never understate the spread that was actually observed.
//   * The max is monotone in the number of runs. Pairs from N+1 runs are a
//     superset of the pairs from N, so adding runs can only reveal more tail.
//     p95 is NOT monotone here: nearest-rank p95 over C(N,2) pairs equals the
//     maximum for N <= 6 and then steps down to the second-largest at N = 7
//     (21 pairs), so raising `repeats` from 5 to 7 could HALVE the published
//     number on unchanged data and delete the caveat with it.
//   * "More measurement made the floor smaller" is precisely the incentive a
//     tool like this must not create.
//
// p95 is still reported, as a clearly secondary column, always labelled with
// the actual pair count. It is never presented as authoritative.
//
// Two deliberate choices there:
//
//   * Unordered pairs, not "run-1 as the base". benchmarks.yml happens to
//     divide by the base run, but in an A/A experiment either run could have
//     been the base, so the pair has two possible readings. Dividing by
//     min(a, b) reports the larger of the two. A noise floor that reads low is
//     the dangerous direction -- it makes every perf claim look significant --
//     so every rounding decision in this file leans high.
//
//   * min(a, b) also makes the floor invariant under the reciprocal, so a task
//     yields the identical |Δ%| whether you look at mean_ms or at
//     hz = 1000/mean_ms. (Dividing by the base does not: that is why Δ mean and
//     Δ ops/s in benchmarks.yml never quite mirror each other.) Both metrics
//     are still reported, because the *dispersion* statistics -- stddev and
//     coefficient of variation -- are not reciprocal-invariant, and a task can
//     look stable in one metric and not the other.
//
// p95 uses nearest-rank, no interpolation: interpolation would pull the value
// below an observed sample. Whether nearest-rank p95 is even distinguishable
// from the maximum is derived, not guessed: it equals the maximum exactly when
// ceil(0.95 * pairs) === pairs, which the report states alongside the pair
// count instead of hard-coding a threshold. stddev is the sample (n-1) form for
// the same reason -- it is the larger of the two conventions.
//
// WHY THIS FILE IS SO SUSPICIOUS OF ITS OWN INPUT. A floor computed from too
// few samples, or from the runs that happened to survive, is a *small* number,
// and a small floor silently blesses every subsequent perf claim. Degrading
// quietly is worse than not running at all. So the script never averages over
// whatever it found:
//
//   * fewer than --min-runs usable runs   -> blocking, and --min-runs < 2 is
//                                            refused outright (no spread from
//                                            one sample)
//   * a suite present in some runs but    -> blocking. The suites that vanish
//     missing/corrupt/task-less in others    are the runs where something went
//                                            wrong, so the survivors are biased
//                                            toward stability.
//   * a task present in some runs only    -> blocking, and its n is printed
//                                            next to its floor either way
//   * null / NaN / Infinity / <= 0        -> the metric is marked invalid, not
//                                            averaged over the valid subset. A
//                                            relative delta against a zero
//                                            baseline is undefined; emitting
//                                            Infinity or a 0% "floor" would be
//                                            a lie in the dangerous direction.
//   * a floor of exactly 0                -> labelled `no variation resolved`,
//                                            never `deterministic`. Identical
//                                            values across runs mean no
//                                            variation was RESOLVED, which is
//                                            either a genuinely reproducible
//                                            benchmark OR real variation below
//                                            the source's rounding step --
//                                            document-put emits
//                                            round(opsPerSecond), 0.05% at 2000
//                                            ops/s but 4% at 24. The two are
//                                            indistinguishable from the data, so
//                                            this file names the observation and
//                                            refuses to name the cause. A 0%
//                                            floor must never be used to bless a
//                                            small A/B delta.
//
// A suite that is ABSENT (ENOENT) from every run is only a warning: the
// noise-floor job deliberately selects a subset of suites, and a file that was
// never written cannot be a collection failure. It is still printed in the
// header ("Suites collected: k/7").
//
// A suite that is PRESENT in every run but unreadable in every run is a
// different animal and is BLOCKING. This is the fail-open this script exists to
// prevent: the summary aggregates only the tasks it managed to measure, so a
// systematically broken suite quietly leaves the denominator and takes its
// noisiest task with it. Measured on a real fixture, one suite broken in every
// run moved the headline from 55.56% to 2.02% -- a 27x understatement, printed
// with trustworthy:true. `readSuiteFile` already distinguishes the two cases
// ("missing" is ENOENT and nothing else); the verdict now uses that instead of
// discarding it, and the reason travels into the JSON report so a downstream
// consumer can tell "we did not ask for this suite" from "this suite broke".
//
// The report is always written and always printed, including when it is
// untrustworthy -- the diagnosis is the useful part. The exit code carries the
// verdict: 0 trustworthy, 1 report produced but not trustworthy, 2 usage error.
//
// Output is a pure function of the input files: no timestamps, no randomness,
// every list explicitly sorted.
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const USAGE =
	"Usage: node scripts/bench/noise-floor.mjs <results-dir> [--min-runs <n>] [--json-out <path>]";

const DEFAULT_MIN_RUNS = 3;
const ABSOLUTE_MIN_RUNS = 2;
const DEFAULT_REPORT_NAME = "noise-floor.json";
const P95_FRACTION = 0.95;
const MAX_LISTED_PROBLEMS = 50;
const SCHEMA = "peerbit-bench-noise-floor/2";
const HEADLINE_STATISTIC =
	"max |Δ%| observed across all unordered run pairs (never the p95: see the header comment)";
const DECISION_RULE =
	"An A/B delta on task T is evidence only if it exceeds T's A/A max |Δ%|. Compare against the max column, not the p95 column.";

// Nearest-rank p95 over `pairs` samples picks index ceil(0.95 * pairs); that is
// the last index exactly when ceil(0.95 * pairs) === pairs, i.e. the "p95" is
// literally the maximum and carries no extra information. Derived rather than
// hard-coded so the report never asserts a resolution the sample size cannot
// support (with 0.95 this is true up to 19 pairs, so up to 6 runs).
export const p95IsJustTheMaximum = (pairs) =>
	pairs > 0 && Math.ceil(P95_FRACTION * pairs) === pairs;

// Same files, same order, as the suites in .github/workflows/benchmarks.yml.
const SUITES = [
	{
		file: "rateless-iblt-startsync-cache.json",
		title: "shared-log: StartSync local decoder cache",
	},
	{
		file: "rateless-iblt-sender-startsync.json",
		title: "shared-log: sender StartSync setup (onMaybeMissingEntries)",
	},
	{
		file: "sync-batch-sweep.json",
		title: "shared-log: sync catch-up batch sweep",
	},
	{
		file: "pid-convergence.json",
		title: "shared-log: PID convergence (model)",
	},
	{ file: "document-put.json", title: "document: put" },
	{ file: "chunk-transfer.json", title: "transport: multi-hop chunk transfer" },
	{ file: "file-ingest.json", title: "document: chunked file-ingest" },
];
const SUITE_FILES = new Set(SUITES.map((suite) => suite.file));

const METRICS = [
	{ key: "mean_ms", label: "mean_ms", digits: 6 },
	{ key: "hz", label: "hz (ops/s)", digits: 3 },
];

export class UsageError extends Error {}

const ensure = (condition, message) => {
	if (!condition) throw new Error(message);
};

const byName = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

const round = (value, digits) =>
	value === null || value === undefined ? null : Number(value.toFixed(digits));

export const parseArgs = (argv) => {
	let resultsDir;
	let minRuns = DEFAULT_MIN_RUNS;
	let jsonOut;
	const pending = [...argv];

	const takeValue = (flag, inline) => {
		const value = inline ?? pending.shift();
		if (value === undefined) throw new UsageError(`${flag} requires a value`);
		return value;
	};

	while (pending.length > 0) {
		const argument = pending.shift();
		const separator = argument.startsWith("--") ? argument.indexOf("=") : -1;
		const flag = separator > 0 ? argument.slice(0, separator) : argument;
		const inline = separator > 0 ? argument.slice(separator + 1) : undefined;

		if (flag === "--min-runs") {
			const raw = takeValue(flag, inline);
			if (!/^\d+$/.test(raw))
				throw new UsageError(`--min-runs must be a whole number, got "${raw}"`);
			const parsed = Number(raw);
			// Not clamped, refused. Silently raising a caller's 1 to 2 would let a
			// workflow believe it asked for something this script never honoured.
			if (parsed < ABSOLUTE_MIN_RUNS)
				throw new UsageError(
					`--min-runs must be at least ${ABSOLUTE_MIN_RUNS}: a noise floor is a spread between runs, and ${parsed} run(s) have no spread to measure`,
				);
			minRuns = parsed;
		} else if (flag === "--json-out") {
			jsonOut = takeValue(flag, inline);
		} else if (argument.startsWith("-")) {
			throw new UsageError(`Unknown option: ${argument}`);
		} else if (resultsDir === undefined) {
			resultsDir = argument;
		} else {
			throw new UsageError(`Unexpected extra argument: ${argument}`);
		}
	}

	if (resultsDir === undefined) throw new UsageError("Missing <results-dir>");
	return {
		resultsDir,
		minRuns,
		jsonOut: jsonOut ?? path.join(resultsDir, DEFAULT_REPORT_NAME),
	};
};

// A benchmark number is usable only if a relative delta against it is defined.
// null/NaN/Infinity/0 all arrive here from real failure modes: tinybench writes
// `mean_ms: task.result?.mean ?? null` when a task never ran, and
// sync-batch-sweep writes `hz: mean_ms > 0 ? 1000 / mean_ms : 0`.
export const classifyValue = (value) => {
	if (value === null) return { ok: false, reason: "missing (null)" };
	// Distinguished from null on purpose: `undefined` is what a suite row that
	// simply does not carry the field looks like after normalisation, and
	// "missing (absent field)" points at a shape problem rather than at a
	// benchmark that ran and recorded nothing.
	if (value === undefined)
		return { ok: false, reason: "missing (absent field)" };
	if (typeof value !== "number")
		return { ok: false, reason: `non-numeric (${typeof value})` };
	if (Number.isNaN(value)) return { ok: false, reason: "NaN" };
	if (!Number.isFinite(value))
		return { ok: false, reason: value > 0 ? "Infinity" : "-Infinity" };
	if (value <= 0) return { ok: false, reason: `not positive (${value})` };
	return { ok: true, value };
};

export const describeSamples = (values) => {
	const n = values.length;
	if (n === 0)
		return { n, mean: null, stddev: null, cv_pct: null, min: null, max: null };
	const mean = values.reduce((total, value) => total + value, 0) / n;
	// Sample (n-1) stddev, and null rather than 0 for a single sample: a lone
	// observation has unknown spread, and printing 0.00% would read as "rock
	// solid" -- exactly the fail-open this script exists to prevent.
	const stddev =
		n > 1
			? Math.sqrt(
					values.reduce((total, value) => total + (value - mean) ** 2, 0) /
						(n - 1),
				)
			: null;
	return {
		n,
		mean,
		stddev,
		cv_pct: stddev === null || mean <= 0 ? null : (stddev / mean) * 100,
		min: Math.min(...values),
		max: Math.max(...values),
	};
};

// All C(n,2) unordered pairs, as |a - b| / min(a, b) * 100, ascending.
export const pairwiseAbsDeltasPct = (values) => {
	for (const value of values)
		ensure(
			typeof value === "number" && Number.isFinite(value) && value > 0,
			`pairwiseAbsDeltasPct requires positive finite samples, got ${value}`,
		);
	const deltas = [];
	for (let i = 0; i < values.length; i++)
		for (let j = i + 1; j < values.length; j++) {
			const a = values[i];
			const b = values[j];
			deltas.push((Math.abs(a - b) / Math.min(a, b)) * 100);
		}
	deltas.sort((x, y) => x - y);
	return deltas;
};

export const percentileNearestRank = (sorted, fraction) => {
	if (sorted.length === 0) return null;
	const rank = Math.ceil(fraction * sorted.length);
	return sorted[Math.min(Math.max(rank, 1), sorted.length) - 1];
};

export const median = (sorted) => {
	if (sorted.length === 0) return null;
	const middle = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 1
		? sorted[middle]
		: (sorted[middle - 1] + sorted[middle]) / 2;
};

// Which field of a suite row carries each METRICS key, per shape. The tinybench
// shape names them identically; document-put's `rows` shape does not, and the
// mapping below is the whole reason this script can see that suite at all.
//
//   mean_ms <- totalPutMs    the row's own put duration. document-put.ts times
//                            `iterations` puts into profile.totalPutMs, so this
//                            is a total rather than a per-op mean. That is fine
//                            and deliberate: |Δ%| divides by min(a, b), so it is
//                            invariant under multiplication by a constant, and
//                            `iterations` is fixed by DOC_ITERATIONS across the
//                            repeats of one A/A experiment. Dividing by
//                            `iterations` here would be the only mapping that
//                            could *hide* a run whose iteration count drifted,
//                            and hiding is the direction this file refuses.
//   hz      <- opsPerSecond  document-put.ts computes it as
//                            round(iterations / totalPutMs * 1000), i.e. the
//                            same reciprocal relationship tinybench's hz has to
//                            mean_ms, up to that same constant factor. So the
//                            reciprocal-invariance of |Δ%| holds for this shape
//                            too, and the two metrics still differ in CV, which
//                            is why both are kept.
//
// Neither field is defaulted. A row that lacks one arrives at classifyValue as
// `undefined` and becomes a blocking `invalid-metric-value` naming the field --
// a loud problem, never a silent skip.
const TASKS_SHAPE = { shape: "tasks", mean_ms: "mean_ms", hz: "hz" };
const ROWS_SHAPE = { shape: "rows", mean_ms: "totalPutMs", hz: "opsPerSecond" };

const normalizeRow = (row, fields) => ({
	name: row.name,
	mean_ms: row[fields.mean_ms],
	hz: row[fields.hz],
});

const unreadable = (status, detail, digest = null) => ({
	status,
	detail,
	digest,
	shape: null,
	metricFields: TASKS_SHAPE,
	tasks: new Map(),
	duplicates: [],
	unnamed: 0,
});

const readSuiteFile = (file) => {
	let raw;
	try {
		raw = fs.readFileSync(file, "utf8");
	} catch (error) {
		// ENOENT is the ONLY status that means "this suite was never collected".
		// Everything else means the file exists and we failed to read it, which
		// analyze() treats as a collection failure rather than a deselection.
		return error.code === "ENOENT"
			? unreadable("missing", "file not found")
			: unreadable("unreadable", error.message);
	}
	// Byte digest of the exact input, used only to spot run directories that are
	// copies of each other rather than independent measurements.
	const digest = crypto.createHash("sha256").update(raw).digest("hex");
	if (raw.trim() === "") return unreadable("empty", "file is empty", digest);
	let parsed;
	try {
		parsed = JSON.parse(raw);
	} catch (error) {
		return unreadable(
			"unparseable",
			`JSON parse error: ${error.message}`,
			digest,
		);
	}
	// `tasks` wins when both are present: it is the canonical tinybench shape,
	// and a file carrying both would be a new emitter worth noticing rather than
	// guessing at.
	const fields = Array.isArray(parsed?.tasks)
		? TASKS_SHAPE
		: Array.isArray(parsed?.rows)
			? ROWS_SHAPE
			: null;
	if (fields === null)
		return unreadable(
			"unparseable",
			"no `tasks` array and no `rows` array (a suite must emit one of the two shapes this script knows)",
			digest,
		);
	const rows = fields === TASKS_SHAPE ? parsed.tasks : parsed.rows;

	const tasks = new Map();
	const duplicates = [];
	let unnamed = 0;
	for (const row of rows) {
		if (typeof row?.name !== "string") {
			unnamed += 1;
			continue;
		}
		// Two rows with one name make the per-run sample ambiguous; the last one
		// would silently win. Keep the first and say so.
		if (tasks.has(row.name)) {
			duplicates.push(row.name);
			continue;
		}
		tasks.set(row.name, normalizeRow(row, fields));
	}
	if (tasks.size === 0)
		return {
			...unreadable(
				"no-tasks",
				unnamed > 0
					? `zero named ${fields.shape} (${unnamed} entr(y/ies) had no name)`
					: `zero ${fields.shape}`,
				digest,
			),
			shape: fields.shape,
			metricFields: fields,
			duplicates,
			unnamed,
		};
	return {
		status: "ok",
		detail: null,
		digest,
		shape: fields.shape,
		metricFields: fields,
		tasks,
		duplicates,
		unnamed,
	};
};

const discoverRuns = (root) => {
	const runs = [];
	for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
		if (!entry.isDirectory()) continue;
		const match = /^run-(\d+)$/.exec(entry.name);
		if (!match) continue;
		runs.push({
			name: entry.name,
			index: Number(match[1]),
			dir: path.join(root, entry.name),
		});
	}
	// Numeric, so run-10 sorts after run-9; name breaks ties like run-1/run-01.
	runs.sort((a, b) => a.index - b.index || byName(a.name, b.name));
	return runs;
};

const unrecognizedJsonFiles = (dir) => {
	let entries;
	try {
		entries = fs.readdirSync(dir, { withFileTypes: true });
	} catch {
		return [];
	}
	return entries
		.filter(
			(entry) =>
				entry.isFile() &&
				entry.name.endsWith(".json") &&
				!SUITE_FILES.has(entry.name),
		)
		.map((entry) => entry.name)
		.sort(byName);
};

export const analyze = ({ resultsDir, minRuns = DEFAULT_MIN_RUNS }) => {
	ensure(
		Number.isInteger(minRuns) && minRuns >= ABSOLUTE_MIN_RUNS,
		`analyze requires an integer minRuns >= ${ABSOLUTE_MIN_RUNS}`,
	);
	const root = path.resolve(resultsDir);
	const problems = [];
	const add = (severity, code, message, extra = {}) =>
		problems.push({ severity, code, message, ...extra });

	const discovered = discoverRuns(root);
	const reads = discovered.map((run) => ({
		run,
		suites: new Map(
			SUITES.map((suite) => [
				suite.file,
				readSuiteFile(path.join(run.dir, suite.file)),
			]),
		),
		extras: unrecognizedJsonFiles(run.dir),
	}));

	// A run directory with no readable suite at all (the job died before it ran,
	// or the copy step missed it) is dropped from the sample rather than counted
	// as "every suite is missing here" -- but dropping it is itself blocking, so
	// the smaller denominator can never pass unnoticed.
	const usable = reads.filter((read) =>
		[...read.suites.values()].some((suite) => suite.status === "ok"),
	);
	const excluded = reads.filter((read) => !usable.includes(read));
	for (const read of excluded)
		add(
			"blocking",
			"unusable-run",
			`${read.run.name} contains no readable suite JSON with at least one task; it is excluded from the sample`,
			{ run: read.run.name },
		);
	for (const read of reads)
		for (const extra of read.extras)
			add(
				"warning",
				"unrecognized-file",
				`${read.run.name}/${extra} is not a known suite file; it was ignored (a renamed benchmark would land here)`,
				{ run: read.run.name, file: extra },
			);

	// Cheap insurance against a hand-assembled results directory: N byte-identical
	// run directories would otherwise produce a 0.00% floor and exit 0. The
	// workflow's own loop cannot generate this (real timings never repeat to the
	// byte), so it means the inputs were copied. A warning rather than a refusal,
	// because a *single* deliberately deterministic model suite really can emit
	// identical bytes twice -- see the `deterministic` handling below.
	const signatures = new Map();
	for (const read of usable) {
		const parts = [...read.suites.entries()]
			.filter(([, suite]) => suite.digest !== null)
			.map(([file, suite]) => `${file}\u0000${suite.digest}`)
			.sort(byName);
		if (parts.length === 0) continue;
		const signature = crypto
			.createHash("sha256")
			.update(parts.join("\u0001"))
			.digest("hex");
		if (!signatures.has(signature)) signatures.set(signature, []);
		signatures.get(signature).push(read.run.name);
	}
	for (const group of [...signatures.values()]
		.filter((names) => names.length > 1)
		.sort((a, b) => byName(a[0], b[0])))
		add(
			"warning",
			"duplicate-run-input",
			`${group.join(", ")} contain byte-identical suite JSON; independent runs do not repeat to the byte, so these are almost certainly copies of one measurement and every |Δ%| they contribute is a fabricated 0%`,
			{ runs: group },
		);

	const runCount = usable.length;
	if (discovered.length === 0)
		add(
			"blocking",
			"no-runs",
			`No run-<n> directories found under ${root}; there is nothing to compare`,
		);
	if (runCount < minRuns)
		add(
			"blocking",
			"insufficient-runs",
			`Only ${runCount} usable run(s), need at least ${minRuns}. A noise floor from too few runs reads low, which makes every perf claim look significant; refusing to publish one`,
			{ usableRuns: runCount, minRuns },
		);

	const suiteReports = [];
	for (const suite of SUITES) {
		const present = usable.filter(
			(read) => read.suites.get(suite.file).status === "ok",
		);
		const absent = usable.filter(
			(read) => read.suites.get(suite.file).status !== "ok",
		);

		// Every absent run carries WHY, and the reason travels into the JSON
		// report. "not-collected" with no reason tells a downstream consumer
		// nothing, and the difference between the two reasons is the difference
		// between a deselected suite and a silent 27x understatement.
		const missingReasons = absent.map((read) => ({
			run: read.run.name,
			reason: read.suites.get(suite.file).status,
			detail: read.suites.get(suite.file).detail,
		}));

		if (present.length === 0) {
			// The discriminator that used to be thrown away. `missing` is ENOENT and
			// only ENOENT (see readSuiteFile), so anything else means the file was
			// there and we could not read it.
			const broken = absent.filter(
				(read) => read.suites.get(suite.file).status !== "missing",
			);
			suiteReports.push({
				file: suite.file,
				title: suite.title,
				status: broken.length > 0 ? "unreadable-everywhere" : "not-collected",
				reason:
					broken.length > 0
						? `present but unreadable in ${broken.length}/${runCount} run(s); no task from this suite reached the summary`
						: // Says what was observed, not why. This script never reads
							// the job's metadata, so it cannot tell "deselected" from
							// "selected but the step never wrote the file". Naming the
							// benign cause would hide the other one.
							"absent (file not found) in every usable run; either it was not selected, or its step never wrote a file -- this report cannot tell which, so confirm the suite was deselected before treating its absence as expected",
				presentIn: [],
				missingIn: absent.map((read) => read.run.name),
				missingReasons,
				tasks: [],
			});
			if (broken.length > 0)
				for (const read of broken)
					add(
						"blocking",
						"suite-unreadable-everywhere",
						`${suite.file} exists in ${read.run.name} but is ${read.suites.get(suite.file).status} (${read.suites.get(suite.file).detail}), and it produced no usable output in ANY of the ${runCount} run(s). Its tasks are therefore absent from the summary entirely, which understates the headline floor by exactly the amount they would have contributed. A suite that is present but broken is a collection failure, not a deselection`,
						{
							suite: suite.file,
							run: read.run.name,
							reason: read.suites.get(suite.file).status,
							detail: read.suites.get(suite.file).detail,
						},
					);
			else if (runCount > 0)
				add(
					"warning",
					"suite-not-collected",
					`${suite.file} is absent (file not found) from all ${runCount} run(s); it was not collected, so no floor is reported for it`,
					{ suite: suite.file, reason: "missing" },
				);
			continue;
		}

		for (const read of absent)
			add(
				"blocking",
				"suite-missing-in-run",
				`${suite.file} is usable in ${present.length}/${runCount} run(s) but ${read.run.name} has it ${read.suites.get(suite.file).status} (${read.suites.get(suite.file).detail}); the surviving runs are not a fair sample`,
				{
					suite: suite.file,
					run: read.run.name,
					reason: read.suites.get(suite.file).status,
				},
			);
		for (const read of present) {
			const parsed = read.suites.get(suite.file);
			for (const name of [...parsed.duplicates].sort(byName))
				add(
					"blocking",
					"duplicate-task",
					`${read.run.name}/${suite.file} lists task "${name}" more than once; which row is the sample is ambiguous`,
					{ suite: suite.file, run: read.run.name, task: name },
				);
			if (parsed.unnamed > 0)
				add(
					"blocking",
					"unnamed-task",
					`${read.run.name}/${suite.file} has ${parsed.unnamed} task(s) without a name; they cannot be matched across runs`,
					{ suite: suite.file, run: read.run.name },
				);
		}

		const taskNames = [
			...new Set(
				present.flatMap((read) => [
					...read.suites.get(suite.file).tasks.keys(),
				]),
			),
		].sort(byName);

		const taskReports = [];
		for (const name of taskNames) {
			const seenIn = [];
			const absentIn = [];
			for (const read of usable) {
				const parsed = read.suites.get(suite.file);
				if (parsed.status === "ok" && parsed.tasks.has(name)) seenIn.push(read);
				else absentIn.push(read.run.name);
			}
			// Only raise a task-level problem when the suite itself was fine in that
			// run; otherwise the suite-level entry above already explains it and we
			// would emit one duplicate per task.
			for (const read of present)
				if (!read.suites.get(suite.file).tasks.has(name))
					add(
						"blocking",
						"task-missing-in-run",
						`${suite.file} task "${name}" is absent from ${read.run.name} although that run's suite parsed; a task that only appears in some runs of identical code is a red flag, and its floor would rest on ${seenIn.length} of ${runCount} runs`,
						{ suite: suite.file, run: read.run.name, task: name },
					);

			const metrics = {};
			for (const metric of METRICS) {
				const values = [];
				const invalid = [];
				const sourceFields = new Map();
				for (const read of seenIn) {
					const parsedSuite = read.suites.get(suite.file);
					const task = parsedSuite.tasks.get(name);
					const classified = classifyValue(task[metric.key]);
					if (classified.ok) values.push(classified.value);
					else {
						invalid.push({ run: read.run.name, reason: classified.reason });
						sourceFields.set(
							read.run.name,
							parsedSuite.metricFields[metric.key],
						);
					}
				}

				if (invalid.length > 0) {
					for (const entry of invalid) {
						// Naming the underlying field matters for the `rows` shape: a
						// document-put row that lost `totalPutMs` must not read as "the
						// mean_ms was null", which points at the wrong thing.
						const field = sourceFields.get(entry.run) ?? metric.key;
						add(
							"blocking",
							"invalid-metric-value",
							`${suite.file} task "${name}" has ${metric.key} = ${entry.reason} in ${entry.run}${
								field === metric.key
									? ""
									: ` (read from the row field "${field}")`
							}; a relative delta against it is undefined, so no floor is computed for this metric`,
							{
								suite: suite.file,
								run: entry.run,
								task: name,
								metric: metric.key,
								sourceField: field,
							},
						);
					}
					metrics[metric.key] = {
						status: "invalid",
						n: values.length,
						invalidSamples: invalid,
						mean: null,
						stddev: null,
						cv_pct: null,
						min: null,
						max: null,
						pairs: 0,
						median_abs_delta_pct: null,
						p95_abs_delta_pct: null,
						max_abs_delta_pct: null,
						noVariationResolved: false,
					};
					continue;
				}

				const stats = describeSamples(values);
				if (values.length < 2) {
					metrics[metric.key] = {
						status: "insufficient-samples",
						n: values.length,
						invalidSamples: [],
						mean: round(stats.mean, 9),
						stddev: null,
						cv_pct: null,
						min: round(stats.min, 9),
						max: round(stats.max, 9),
						pairs: 0,
						median_abs_delta_pct: null,
						p95_abs_delta_pct: null,
						max_abs_delta_pct: null,
						noVariationResolved: false,
					};
					continue;
				}

				const deltas = pairwiseAbsDeltasPct(values);
				const worst = deltas[deltas.length - 1];
				// A zero floor means no variation was RESOLVED. It does not mean
				// the benchmark is deterministic, and this file must not claim a
				// cause it cannot observe. Two different things produce it:
				//
				//   1. a genuinely reproducible benchmark (some model suites are), or
				//   2. real variation smaller than the source's own rounding step.
				//
				// (2) is not hypothetical. document-put.ts emits
				// `opsPerSecond: Math.round(...)`, so its resolution is one whole
				// op/s -- 0.05% at 2000 ops/s, but 4% at 24 -- and totalPutMs is
				// rounded to 2 decimals. A "0% floor" taken as determinism would
				// let every later A/B delta on that task clear the bar, which is
				// the understating direction this file exists to refuse.
				const noVariationResolved = worst === 0;
				if (noVariationResolved)
					add(
						"note",
						"no-variation-resolved",
						`${suite.file} task "${name}" reported an identical ${metric.key} in all ${values.length} runs. Treat this as "below the resolution of this measurement", NOT as a measured 0% floor: it is indistinguishable from variation smaller than the source's rounding step. Do not use it to justify accepting a small A/B delta on this task.`,
						{ suite: suite.file, task: name, metric: metric.key },
					);
				metrics[metric.key] = {
					status: "ok",
					n: values.length,
					invalidSamples: [],
					mean: round(stats.mean, 9),
					stddev: round(stats.stddev, 9),
					cv_pct: round(stats.cv_pct, 6),
					min: round(stats.min, 9),
					max: round(stats.max, 9),
					pairs: deltas.length,
					median_abs_delta_pct: round(median(deltas), 6),
					p95_abs_delta_pct: round(
						percentileNearestRank(deltas, P95_FRACTION),
						6,
					),
					max_abs_delta_pct: round(worst, 6),
					noVariationResolved,
				};
			}

			taskReports.push({
				name,
				n: seenIn.length,
				complete: seenIn.length === runCount,
				seenIn: seenIn.map((read) => read.run.name),
				absentIn,
				metrics,
			});
		}

		suiteReports.push({
			file: suite.file,
			title: suite.title,
			status: absent.length === 0 ? "ok" : "partial",
			reason: null,
			presentIn: present.map((read) => read.run.name),
			missingIn: absent.map((read) => read.run.name),
			missingReasons,
			tasks: taskReports,
		});
	}

	const pairsPerTask = runCount >= 2 ? (runCount * (runCount - 1)) / 2 : 0;
	const p95Degenerate = p95IsJustTheMaximum(pairsPerTask);
	if (p95Degenerate)
		add(
			"note",
			"coarse-p95",
			`${runCount} runs give only ${pairsPerTask} pairs per task, and ceil(${P95_FRACTION} * ${pairsPerTask}) = ${pairsPerTask}, so the nearest-rank p95 IS the maximum. It carries no information the headline max does not already carry; the headline is the max either way`,
			{ pairs: pairsPerTask },
		);

	const summary = {};
	for (const metric of METRICS) {
		const measured = [];
		for (const suite of suiteReports)
			for (const task of suite.tasks) {
				const stats = task.metrics[metric.key];
				if (stats?.status === "ok")
					measured.push({
						suite: suite.file,
						task: task.name,
						max: stats.max_abs_delta_pct,
						p95: stats.p95_abs_delta_pct,
					});
			}
		// The headline aggregates the per-task MAXIMA. Sorting a copy leaves
		// `measured` in its deterministic (suite, task) order for the worst-task
		// tie-break below.
		const maxes = measured.map((entry) => entry.max).sort((a, b) => a - b);
		const p95s = measured.map((entry) => entry.p95).sort((a, b) => a - b);
		// Ties resolve by (suite, task) so the named worst task is stable.
		const worst = measured
			.slice()
			.sort(
				(a, b) =>
					b.max - a.max || byName(a.suite, b.suite) || byName(a.task, b.task),
			)[0];
		summary[metric.key] = {
			tasksMeasured: measured.length,
			// Headline. Monotone in the run count: more runs can only add pairs.
			medianOfMax: round(median(maxes), 6),
			worstMax: worst ? worst.max : null,
			worstTask: worst ? { suite: worst.suite, task: worst.task } : null,
			tasksAbove5Pct: maxes.filter((value) => value > 5).length,
			tasksAbove10Pct: maxes.filter((value) => value > 10).length,
			// Secondary, and never quoted without its pair count: at these sample
			// sizes p95 is either the maximum outright or one rank below it, and it
			// can DROP when runs are added. See the header comment.
			pairsPerTask,
			p95IsJustTheMaximum: p95Degenerate,
			medianOfP95: round(median(p95s), 6),
			worstP95: measured.length > 0 ? p95s[p95s.length - 1] : null,
			noVariationTasks: suiteReports.reduce(
				(total, suite) =>
					total +
					suite.tasks.filter(
						(task) => task.metrics[metric.key]?.noVariationResolved,
					).length,
				0,
			),
		};
	}

	const blocking = problems.filter((entry) => entry.severity === "blocking");
	return {
		schema: SCHEMA,
		resultsDir: root,
		minRuns,
		deltaDefinition: "|a - b| / min(a, b) * 100 over all unordered run pairs",
		headlineStatistic: HEADLINE_STATISTIC,
		decisionRule: DECISION_RULE,
		p95Method:
			"nearest-rank (no interpolation), reported as a secondary column",
		stddevMethod: "sample (n-1)",
		runs: usable.map((read) => ({
			name: read.run.name,
			index: read.run.index,
		})),
		excludedRuns: excluded.map((read) => read.run.name),
		runCount,
		pairsPerTask,
		// "Collected" means at least one run produced usable tasks. A suite that is
		// present-but-broken everywhere is NOT collected and is counted separately,
		// so `k/7` can never quietly include a suite that contributed nothing.
		suitesCollected: suiteReports.filter(
			(suite) => suite.status === "ok" || suite.status === "partial",
		).length,
		suitesUnreadableEverywhere: suiteReports
			.filter((suite) => suite.status === "unreadable-everywhere")
			.map((suite) => suite.file),
		suitesKnown: SUITES.length,
		// Single source of truth: every refusal above goes through `blocking`, so
		// the verdict has exactly one place to be wrong and one place to pin.
		trustworthy: blocking.length === 0,
		blockingCount: blocking.length,
		problems,
		suites: suiteReports,
		summary,
	};
};

const cell = (value) => String(value).replace(/\|/g, "\\|");

const fmt = (value, digits) =>
	value === null || value === undefined ? "-" : value.toFixed(digits);

const flagsFor = (task, stats, runCount) => {
	const flags = [];
	if (!task.complete)
		flags.push(
			`**n=${task.n}/${runCount}** (absent: ${task.absentIn.join(", ")})`,
		);
	if (stats.status === "invalid")
		flags.push(
			`**invalid**: ${stats.invalidSamples
				.map((entry) => `${entry.run} ${entry.reason}`)
				.join("; ")}`,
		);
	if (stats.status === "insufficient-samples")
		flags.push("**no spread** (fewer than 2 samples)");
	if (stats.noVariationResolved)
		flags.push(
			"`no variation resolved` (identical in every run -- may be below measurement resolution)",
		);
	return flags.join("<br>") || "";
};

export const renderMarkdown = (report) => {
	const lines = [];
	const push = (line = "") => lines.push(line);

	push("# Benchmark A/A noise floor");
	push();
	if (report.trustworthy)
		push(
			`**Verdict: TRUSTWORTHY** — ${report.runCount} usable run(s) of identical code (minimum ${report.minRuns}), ${report.suitesCollected}/${report.suitesKnown} suite(s) collected.`,
		);
	else
		push(
			`**Verdict: NOT TRUSTWORTHY** — ${report.blockingCount} blocking problem(s). Any floor below is likely to be **understated**, which would make real regressions look like noise and noise look like wins. Fix the problems and re-run before quoting a number.`,
		);
	push();
	push(`- Results directory: \`${report.resultsDir}\``);
	push(
		`- Usable runs: ${report.runCount} (\`--min-runs ${report.minRuns}\`)${
			report.runs.length > 0
				? ` — ${report.runs.map((run) => run.name).join(", ")}`
				: ""
		}`,
	);
	if (report.excludedRuns.length > 0)
		push(`- Excluded runs: ${report.excludedRuns.join(", ")}`);
	push(`- Suites collected: ${report.suitesCollected}/${report.suitesKnown}`);
	if (report.suitesUnreadableEverywhere.length > 0)
		push(
			`- **Suites present but unreadable in every run: ${report.suitesUnreadableEverywhere.join(", ")}** — their tasks are missing from the headline below, which therefore reads LOW.`,
		);
	push(`- Pairs compared per task: ${report.pairsPerTask}`);
	push();

	push("## How to read this");
	push();
	push(
		"Every run here is the **same commit**. Any spread is measurement noise, not code.",
	);
	push();
	push(
		`\`|Δ%|\` is \`${report.deltaDefinition}\`. \`benchmarks.yml\` divides by the base run, but either run of an A/A pair could have been the base, so this reports the larger of the two readings — a floor that reads low is the dangerous direction. This also makes \`mean_ms\` and \`hz\` yield the same \`|Δ%|\`; their \`CV\` still differs, which is why both are listed.`,
	);
	push();
	push(
		`The headline is the **observed maximum** \`|Δ%|\`, not the p95. The max never understates what was actually seen, and it is monotone in the run count — pairs from N+1 runs contain the pairs from N — so adding runs can only reveal more tail. Nearest-rank p95 over \`C(N,2)\` pairs is **not** monotone: it equals the maximum up to 6 runs and steps down at 7, so quoting it would let extra measurement shrink the published floor.`,
	);
	push();
	push(
		`p95 is ${report.p95Method}; stddev is ${report.stddevMethod}; \`CV\` is \`stddev / mean\` as a percent.`,
	);
	push();
	push(`To use it: ${report.decisionRule}`);
	push();
	push(
		"So an A/B run showing +7% on task T is noise if T's A/A max is 12%, and worth investigating if T's A/A max is 0.4%.",
	);
	push();

	for (const [heading, severity] of [
		["Blocking problems", "blocking"],
		["Warnings", "warning"],
		["Findings", "note"],
	]) {
		const entries = report.problems.filter(
			(problem) => problem.severity === severity,
		);
		if (entries.length === 0) continue;
		push(`## ${heading} (${entries.length})`);
		push();
		for (const problem of entries.slice(0, MAX_LISTED_PROBLEMS))
			push(`- \`${problem.code}\` ${problem.message}`);
		if (entries.length > MAX_LISTED_PROBLEMS)
			push(
				`- …and ${entries.length - MAX_LISTED_PROBLEMS} more (see the JSON report)`,
			);
		push();
	}

	push("## Headline: how big is noise?");
	push();
	push(
		"Aggregated over the per-task **max** `|Δ%|`. This is the number to quote.",
	);
	push();
	push(
		"| metric | tasks measured | median max \\|Δ\\| | worst max \\|Δ\\| | worst task | max > 5% | max > 10% | no variation resolved |",
	);
	push("|---|---:|---:|---:|---|---:|---:|---:|");
	for (const metric of METRICS) {
		const stats = report.summary[metric.key];
		push(
			`| ${metric.label} | ${stats.tasksMeasured} | ${fmt(stats.medianOfMax, 2)}% | ${fmt(stats.worstMax, 2)}% | ${
				stats.worstTask
					? cell(`${stats.worstTask.suite} › ${stats.worstTask.task}`)
					: "-"
			} | ${stats.tasksAbove5Pct} | ${stats.tasksAbove10Pct} | ${stats.noVariationTasks} |`,
		);
	}
	push();
	push(
		`### Secondary: p95 over ${report.pairsPerTask} pair(s) per task — not authoritative`,
	);
	push();
	push(
		report.summary[METRICS[0].key].p95IsJustTheMaximum
			? `> [!NOTE]\n> \`ceil(0.95 × ${report.pairsPerTask}) = ${report.pairsPerTask}\`, so this p95 **is** the maximum above and adds nothing. At 7+ runs it would step below the maximum instead, which is why it is not the headline.`
			: `> [!NOTE]\n> With ${report.pairsPerTask} pairs this p95 sits below the observed maximum. It is shown for shape only; a floor quoted from it would understate the worst A/A gap actually measured.`,
	);
	push();
	push("| metric | median p95 \\|Δ\\| | worst p95 \\|Δ\\| |");
	push("|---|---:|---:|");
	for (const metric of METRICS) {
		const stats = report.summary[metric.key];
		push(
			`| ${metric.label} | ${fmt(stats.medianOfP95, 2)}% | ${fmt(stats.worstP95, 2)}% |`,
		);
	}
	push();

	for (const suite of report.suites) {
		push(`## ${suite.title} (\`${suite.file}\`)`);
		push();
		if (suite.status === "not-collected") {
			push(
				`_Not collected in any run (${suite.reason}); no floor is reported for this suite._`,
			);
			push();
			continue;
		}
		if (suite.status === "unreadable-everywhere") {
			push(
				`> [!CAUTION]\n> **Present but unreadable in every run** — ${suite.reason}. This suite contributed nothing to the headline, so the headline is understated by whatever its tasks would have added. Reasons: ${suite.missingReasons
					.map((entry) => `${entry.run} ${entry.reason} (${entry.detail})`)
					.join("; ")}.`,
			);
			push();
			continue;
		}
		if (suite.status === "partial") {
			push(
				`> [!WARNING]\n> Usable in ${suite.presentIn.length}/${report.runCount} run(s); missing or unreadable in ${suite.missingReasons
					.map((entry) => `${entry.run} (${entry.reason})`)
					.join(", ")}.`,
			);
			push();
		}
		for (const metric of METRICS) {
			push(`### ${metric.label}`);
			push();
			push(
				"| Task | n | mean | stddev | CV % | min | max | median \\|Δ\\| % | p95 \\|Δ\\| % | max \\|Δ\\| % | flags |",
			);
			push("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|");
			for (const task of suite.tasks) {
				const stats = task.metrics[metric.key];
				push(
					`| ${cell(task.name)} | ${task.n}/${report.runCount} | ${fmt(stats.mean, metric.digits)} | ${fmt(stats.stddev, metric.digits)} | ${fmt(stats.cv_pct, 2)} | ${fmt(stats.min, metric.digits)} | ${fmt(stats.max, metric.digits)} | ${fmt(stats.median_abs_delta_pct, 2)} | ${fmt(stats.p95_abs_delta_pct, 2)} | ${fmt(stats.max_abs_delta_pct, 2)} | ${flagsFor(task, stats, report.runCount)} |`,
				);
			}
			push();
		}
	}

	push("---");
	push();
	push(
		report.trustworthy
			? "Exit status 0: the report is trustworthy."
			: "Exit status 1: the report was produced but is **not** trustworthy — see the blocking problems above.",
	);
	return lines.join("\n");
};

export const runCli = (
	argv,
	{ log = console.log, logError = console.error } = {},
) => {
	let options;
	try {
		options = parseArgs(argv);
	} catch (error) {
		if (!(error instanceof UsageError)) throw error;
		logError(error.message);
		logError(USAGE);
		return 2;
	}

	const root = path.resolve(options.resultsDir);
	let stats;
	try {
		stats = fs.statSync(root);
	} catch {
		logError(`Results directory does not exist: ${root}`);
		logError(USAGE);
		return 2;
	}
	if (!stats.isDirectory()) {
		logError(`Not a directory: ${root}`);
		logError(USAGE);
		return 2;
	}

	const report = analyze({ resultsDir: root, minRuns: options.minRuns });
	const jsonOut = path.resolve(options.jsonOut);
	fs.mkdirSync(path.dirname(jsonOut), { recursive: true });
	fs.writeFileSync(jsonOut, `${JSON.stringify(report, null, 2)}\n`);
	log(renderMarkdown(report));
	if (!report.trustworthy) {
		logError(
			`noise-floor: ${report.blockingCount} blocking problem(s); the measured floor would be understated. Report written to ${jsonOut}`,
		);
		return 1;
	}
	logError(`noise-floor: report written to ${jsonOut}`);
	return 0;
};

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
)
	process.exitCode = runCli(process.argv.slice(2));
