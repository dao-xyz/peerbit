#!/usr/bin/env node
/**
 * summarize.mjs: medians, ranges and pass criteria over rc-driver.mjs runs.
 *
 *   node summarize.mjs <runs-dir> [--out <dir>] [--base base] [--pr1 pr1,both,fix]
 *     [--expect <matrix.json>]
 *
 * Reads every report.json under <runs-dir> (up to three levels deep) and
 * writes summary.md and summary.json to --out (default <runs-dir>).
 *
 * A cell is variant x n x min x phase (long-partition runs form their own
 * cells). Every number is the median over the cell's runs, with the range.
 * The pass/fail lines are the upstream-harness criteria of JUDGED_PLAN.md
 * section 7 that compare base with the fix (the inventory criteria belong to
 * the inventory change and are not judged here; their numbers are listed).
 * "Per peer" means each of A and B.
 *
 * --expect names the planned matrix:
 *   { "variants": ["base", "pr1"], "n": [9000, 27000], "min": [2, 3],
 *     "phases": ["rest", "writes", "control"], "runs": 3,
 *     "long": [{ "variant": "pr1", "min": [3], "phases": ["rest"], "runs": 1 }] }
 * ("long" entries take the top-level "n" unless they name their own). Each
 * criterion then lists every expected cell it covers that has no runs as
 * MISSING, and marks a cell with fewer runs than planned as short. A
 * criterion is FAIL when a check fails, else INCOMPLETE when a check is
 * missing, short or not applicable (n/a), else PASS. Without --expect only
 * the cells found are judged, so a missing cell goes unnoticed.
 */
import fs from "node:fs";
import path from "node:path";

const parseArgs = (list) => {
	const out = { _: [] };
	for (let i = 0; i < list.length; i++) {
		const key = list[i];
		if (!key.startsWith("--")) {
			out._.push(key);
			continue;
		}
		const next = list[i + 1];
		out[key.slice(2)] =
			next !== undefined && !next.startsWith("--") ? list[++i] : "true";
	}
	return out;
};

const opts = parseArgs(process.argv.slice(2));
const RUNS = opts._[0] ? path.resolve(opts._[0]) : null;
if (!RUNS) {
	console.error(
		"usage: summarize.mjs <runs-dir> [--out <dir>] [--base base] [--pr1 pr1,both,fix] [--expect <matrix.json>]",
	);
	process.exit(2);
}
const OUT = path.resolve(opts.out ?? RUNS);
const BASE = opts.base ?? "base";
const PR1 = new Set((opts.pr1 ?? "pr1,both,fix").split(",").map((v) => v.trim()));
const SIDES = ["A", "B"];
const PHASE_ORDER = ["rest", "writes", "control"];

const cellKeyOf = ({ variant, n, min, phase, long }) =>
	`${variant}|${n}|${min}|${phase}${long ? "|long" : ""}`;
const groupKeyOf = ({ variant, n, min, long }) =>
	`${variant}|${n}|${min}${long ? "|long" : ""}`;

/** The planned matrix (--expect): expected cells and runs per run group. */
const loadExpect = (file) => {
	const fail = (message) => {
		console.error(`--expect ${file}: ${message}`);
		process.exit(2);
	};
	let spec;
	try {
		spec = JSON.parse(fs.readFileSync(file, "utf8"));
	} catch (error) {
		fail(error.message);
	}
	const positiveInts = (list, name) => {
		if (!Array.isArray(list) || list.length === 0 || !list.every((v) => Number.isInteger(v) && v > 0)) {
			fail(`${name} must be a non-empty array of positive integers`);
		}
		return list;
	};
	const phases = (list, name) => {
		if (!Array.isArray(list) || list.length === 0 || !list.every((p) => PHASE_ORDER.includes(p))) {
			fail(`${name} must be a non-empty array of ${PHASE_ORDER.join(", ")}`);
		}
		return list;
	};
	const runs = (value, name) => {
		if (!Number.isInteger(value) || value < 1) fail(`${name} must be a positive integer`);
		return value;
	};
	if (!Array.isArray(spec?.variants) || spec.variants.length === 0 || !spec.variants.every((v) => typeof v === "string" && v)) {
		fail("variants must be a non-empty array of names");
	}
	const cells = new Map();
	const groups = new Map();
	const add = (variant, ns, mins, phaseList, runCount, long) => {
		for (const n of ns) {
			for (const min of mins) {
				groups.set(groupKeyOf({ variant, n, min, long }), { variant, n, min, long, runs: runCount });
				for (const phase of phaseList) {
					cells.set(cellKeyOf({ variant, n, min, phase, long }), { variant, n, min, phase, long, runs: runCount });
				}
			}
		}
	};
	const ns = positiveInts(spec.n, "n");
	const mins = positiveInts(spec.min, "min");
	const phaseList = phases(spec.phases, "phases");
	const runCount = runs(spec.runs, "runs");
	for (const variant of spec.variants) add(variant, ns, mins, phaseList, runCount, false);
	if (spec.long !== undefined && !Array.isArray(spec.long)) fail("long must be an array");
	for (const [index, entry] of (spec.long ?? []).entries()) {
		if (typeof entry?.variant !== "string" || !entry.variant) fail(`long[${index}].variant must be a name`);
		add(
			entry.variant,
			entry.n === undefined ? ns : positiveInts(entry.n, `long[${index}].n`),
			positiveInts(entry.min, `long[${index}].min`),
			phases(entry.phases, `long[${index}].phases`),
			runs(entry.runs, `long[${index}].runs`),
			true,
		);
	}
	return { file: path.resolve(file), spec, cells, groups };
};
const EXPECT = opts.expect ? loadExpect(opts.expect) : null;

// ---------------------------------------------------------------------------
// Load

const findReports = (dir, depth = 0) => {
	if (depth > 3 || !fs.existsSync(dir)) return [];
	const found = [];
	for (const name of fs.readdirSync(dir)) {
		const full = path.join(dir, name);
		let stat;
		try {
			stat = fs.statSync(full);
		} catch {
			continue;
		}
		if (stat.isDirectory()) found.push(...findReports(full, depth + 1));
		else if (name === "report.json") found.push(full);
	}
	return found.sort();
};

const reports = [];
for (const file of findReports(RUNS)) {
	try {
		const report = JSON.parse(fs.readFileSync(file, "utf8"));
		if (report.schema !== "rc-report/1") continue;
		report.__file = path.relative(RUNS, file);
		report.__run = path.basename(path.dirname(file));
		reports.push(report);
	} catch (error) {
		console.error(`skipping ${file}: ${error.message}`);
	}
}

// ---------------------------------------------------------------------------
// Helpers

const median = (values) => {
	const list = values.filter((v) => typeof v === "number" && !Number.isNaN(v));
	if (list.length === 0) return null;
	const sorted = [...list].sort((a, b) => a - b);
	const mid = Math.floor(sorted.length / 2);
	return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};
const range = (values) => {
	const list = values.filter((v) => typeof v === "number" && !Number.isNaN(v));
	if (list.length === 0) return null;
	return [Math.min(...list), Math.max(...list)];
};
const round = (value, digits = 1) =>
	value === null || value === undefined || Number.isNaN(value)
		? null
		: Math.round(value * 10 ** digits) / 10 ** digits;
const fmtNum = (value, digits = 1) => {
	if (value === null || value === undefined) return "-";
	if (typeof value !== "number") return String(value);
	const r = round(value, digits);
	return Number.isInteger(r) ? String(r) : r.toFixed(digits);
};
const fmtCell = (agg, digits = 1) => {
	if (!agg || agg.median === null) return "absent";
	const [lo, hi] = agg.range;
	return lo === hi
		? fmtNum(agg.median, digits)
		: `${fmtNum(agg.median, digits)} [${fmtNum(lo, digits)}-${fmtNum(hi, digits)}]`;
};
const agg = (values) => ({ median: median(values), range: range(values), n: values.length });

// ---------------------------------------------------------------------------
// Per-run, per-phase, per-side metrics

const sideMetrics = (side) => {
	if (!side) return null;
	const passes = side.passes ?? {};
	const refOther = passes.refOther ?? {};
	const park = side.park?.refOther ?? {};
	const sweeps = side.repair?.sweepByMode ?? null;
	const inv = side.inventory ?? {};
	const parkEvents = (side.park?.events ?? []).filter((e) => e.refOther);
	const parkedAt = parkEvents.find((e) => e.outcome === "parked")?.t ?? null;
	const graceAt =
		parkEvents.find((e) => e.outcome === "released" && e.reason === "grace")?.t ?? null;
	const refOtherRecords = (passes.records ?? []).filter((r) => r.refOther);
	const mark = side.cpuMark?.taken ? side.cpuMark : null;
	return {
		passCalls: passes.calls ?? null,
		passMs: passes.ms ?? null,
		refOtherCalls: refOther.calls ?? null,
		refOtherMs: refOther.ms ?? null,
		refOtherShapes: (refOther.shapes ?? []).join(","),
		removedOnlyWithPage: refOther.removedOnlyWithPage ?? null,
		addedOnlyWithPage: refOther.addedOnlyWithPage ?? null,
		refOtherExamined: refOther.examined ?? null,
		pages1048: side.coordinatePages ? side.coordinatePages.pages1048 : null,
		pages1048RefOther: refOther.pages1048 ?? null,
		cpuMs: side.cpuMs ?? null,
		windowMs: side.durationMs ?? null,
		// The snapshot at the dial + cpuMarkMs (after the first write in control).
		cpuMarkMs: mark ? mark.cpuMs : null,
		cpuMarkRelMs: mark ? mark.rel : null,
		cpuMarkLateMs: mark ? mark.lateMs : null,
		passMsAtMark: mark ? mark.passMs : null,
		passesInFlightAtMark: mark ? mark.passesInFlight : null,
		invPassesAtMark: mark ? mark.invPasses : null,
		invMsAtMark: mark ? mark.invMs : null,
		eldP50: side.eventLoopDelayMs?.p50 ?? null,
		eldP99: side.eventLoopDelayMs?.p99 ?? null,
		eldMax: side.eventLoopDelayMs?.max ?? null,
		parkSupported: side.park?.supported ?? false,
		parked: side.park?.supported ? (park.parked ?? 0) : null,
		restored: side.park?.supported ? (park.restored ?? 0) : null,
		released: side.park?.supported ? (park.released ?? 0) : null,
		releasedGrace: side.park?.supported ? (park.releasedByReason?.grace ?? 0) : null,
		parkSequence: (park.sequence ?? []).join(","),
		parkedAt,
		graceAt,
		graceDelayMs: parkedAt !== null && graceAt !== null ? graceAt - parkedAt : null,
		refOtherPassStarts: refOtherRecords.map((r) => ({ start: r.start, shape: r.otherTypes.join("+") })),
		sweepJaDirect: sweeps ? (sweeps["join-authoritative"]?.direct ?? 0) : null,
		sweepJaDirectRefOther: sweeps ? (sweeps["join-authoritative"]?.directRefOther ?? 0) : null,
		sweepJaDelayed: sweeps ? (sweeps["join-authoritative"]?.delayed ?? 0) : null,
		sweepWarmup: sweeps ? (sweeps["join-warmup"]?.calls ?? 0) : null,
		sweepChurn: sweeps ? (sweeps.churn?.calls ?? 0) : null,
		joinRepairRefOther: side.repair?.joinAuthoritative
			? side.repair.joinAuthoritative.refOther
			: null,
		invPasses: inv.passes ? inv.passes.count : null,
		invCompleted: inv.passes ? inv.passes.completed : null,
		invPages: inv.passes ? inv.passes.pages : null,
		invHashes: inv.pages ? inv.pages.hashes : null,
		invRequests: inv.pages ? inv.pages.requests : null,
		invHandledHashes: inv.handled ? inv.handled.hashes : null,
		invMs: inv.passes ? inv.passes.ms : null,
		invLastCompleteAt: inv.passes ? inv.passes.lastCompleteAt : null,
		unhandled: side.errors?.unhandled ?? null,
		uncaught: side.errors?.uncaught ?? null,
	};
};

const rows = []; // one per run x phase
for (const report of reports) {
	for (const phase of report.phaseResults ?? []) {
		rows.push({
			run: report.__run,
			file: report.__file,
			variant: report.variant,
			n: report.n,
			min: report.min,
			long: !!report.long,
			partitionMs: report.partitionMs ?? 0,
			phase: phase.phase,
			cell: cellKeyOf({ ...report, phase: phase.phase, long: !!report.long }),
			cpuMarkAfterMs: phase.cpuMark?.ms ?? report.options?.cpuMarkMs ?? null,
			observeEndedBy: phase.observe?.endedBy ?? null,
			quiesceOk: phase.quiesce?.ok ?? null,
			load1Start: phase.load1?.start ?? null,
			load1End: phase.load1?.end ?? null,
			writeP50: phase.writeLatency?.latencyMs?.p50 ?? null,
			writeP90: phase.writeLatency?.latencyMs?.p90 ?? null,
			writeMax: phase.writeLatency?.latencyMs?.max ?? null,
			writesMissing: phase.writeLatency?.missing ?? null,
			firstWriteAfterDialMs:
				phase.writeRecords?.length && phase.timeline?.dialStart
					? phase.writeRecords[0].t0 - phase.timeline.dialStart
					: null,
			firstWriteLatencyMs: (() => {
				const first = phase.writeRecords?.[0];
				if (!first) return null;
				const at = phase.B?.arrivals?.[first.hash] ?? phase.B?.polledArrivals?.[first.hash];
				return at === undefined ? null : at - first.t0;
			})(),
			partitionEntryMsFromHeal: phase.partitionEntry?.msFromHeal ?? null,
			partitionEntryArrived: phase.partitionEntry ? phase.partitionEntry.arrivedAt !== null : null,
			healRel: phase.timeline?.healed && phase.A?.resetAt
				? phase.timeline.healed - phase.A.resetAt
				: null,
			sides: Object.fromEntries(SIDES.map((s) => [s, sideMetrics(phase[s])])),
		});
	}
}

const cells = new Map();
for (const row of rows) {
	if (!cells.has(row.cell)) cells.set(row.cell, []);
	cells.get(row.cell).push(row);
}

const numericKeys = (object) =>
	Object.entries(object ?? {})
		.filter(([, value]) => typeof value === "number" || value === null)
		.map(([key]) => key);

const aggregateCell = (cellRows) => {
	const first = cellRows[0];
	const out = {
		variant: first.variant,
		n: first.n,
		min: first.min,
		phase: first.phase,
		long: first.long,
		runs: cellRows.map((row) => row.run),
		phaseLevel: {},
		sides: {},
	};
	for (const key of [
		"writeP50",
		"writeP90",
		"writeMax",
		"writesMissing",
		"firstWriteAfterDialMs",
		"firstWriteLatencyMs",
		"partitionEntryMsFromHeal",
		"load1Start",
		"load1End",
	]) {
		out.phaseLevel[key] = agg(cellRows.map((row) => row[key]));
	}
	out.phaseLevel.observeEndedBy = cellRows.map((row) => row.observeEndedBy);
	out.phaseLevel.quiesceOk = cellRows.map((row) => row.quiesceOk);
	out.cpuMarkAfterMs = [...new Set(cellRows.map((row) => row.cpuMarkAfterMs))];
	for (const side of SIDES) {
		const metrics = cellRows.map((row) => row.sides[side]).filter(Boolean);
		const sideAgg = {};
		for (const key of numericKeys(metrics[0])) {
			sideAgg[key] = agg(metrics.map((m) => m[key]));
		}
		sideAgg.refOtherShapes = metrics.map((m) => m.refOtherShapes);
		sideAgg.parkSequence = metrics.map((m) => m.parkSequence);
		sideAgg.parkSupported = metrics.some((m) => m.parkSupported);
		out.sides[side] = sideAgg;
	}
	return out;
};

const cellAggs = [...cells.entries()]
	.map(([key, cellRows]) => ({ key, ...aggregateCell(cellRows) }))
	.sort((a, b) =>
		a.phase === b.phase
			? a.n - b.n || a.min - b.min || Number(a.long) - Number(b.long) || a.variant.localeCompare(b.variant)
			: PHASE_ORDER.indexOf(a.phase) - PHASE_ORDER.indexOf(b.phase),
	);
const findCell = (variant, n, min, phase, long = false) =>
	cellAggs.find(
		(c) => c.variant === variant && c.n === n && c.min === min && c.phase === phase && c.long === long,
	);
const med = (cell, side, key) => cell?.sides?.[side]?.[key]?.median ?? null;

// ---------------------------------------------------------------------------
// Criteria (JUDGED_PLAN.md section 7, upstream harness, base vs the fix)

const criteria = [];
const isPr1 = (variant) => PR1.has(variant);
const plural = (count, word) => `${count} ${word}${count === 1 ? "" : "s"}`;
const expectedRuns = (key) => EXPECT?.cells.get(key)?.runs ?? null;
const runsNote = (found, expected) =>
	expected !== null && found < expected ? `${found} of ${expected} runs` : plural(found, "run");
const cellName = ({ variant, n, min, phase, long }) =>
	`${variant} n=${n} min=${min}${long ? " long" : ""} ${phase}`;
const cellLabel = (cell, side) =>
	`${cellName(cell)}${side ? ` ${side}` : ""} (${runsNote(cell.runs.length, expectedRuns(cell.key))})`;
const shortOf = (cell) => {
	const expected = cell ? expectedRuns(cell.key) : null;
	return expected !== null && cell.runs.length < expected
		? { found: cell.runs.length, expected }
		: null;
};

/** A check on a found cell; short when it (or the base cell it uses) has fewer runs than planned. */
const cellCheck = (cell, side, ok, detail, baseCell) => {
	const short = shortOf(cell) ?? shortOf(baseCell);
	return { cell: cellLabel(cell, side), ok, ...(short ? { short } : {}), detail };
};

/** One MISSING check per expected cell the criterion covers that has no runs. */
const missingCells = (covers) => {
	if (!EXPECT) return [];
	const out = [];
	for (const [key, expected] of EXPECT.cells) {
		if (!covers(expected) || cells.has(key)) continue;
		out.push({
			cell: `${cellName(expected)} (0 of ${expected.runs} runs)`,
			ok: null,
			missing: true,
			detail: "no run recorded this phase",
		});
	}
	return out;
};

/**
 * FAIL when a check fails; else INCOMPLETE when one is missing, short or n/a;
 * else PASS. A gapOnly check only reports a short or missing count.
 */
const judge = (name, checks, explanation) => {
	const failed = checks.filter((check) => check.ok === false).length;
	const missing = checks.filter((check) => check.missing).length;
	const short = checks.filter((check) => check.short).length;
	const notApplicable = checks.filter(
		(check) => check.ok === null && !check.missing && !check.gapOnly,
	).length;
	const status =
		failed > 0
			? "FAIL"
			: checks.length === 0
				? "N/A"
				: missing + short + notApplicable > 0
					? "INCOMPLETE"
					: "PASS";
	const gaps = [
		missing ? `${missing} missing` : null,
		short ? `${short} short of runs` : null,
		notApplicable ? `${notApplicable} n/a` : null,
	].filter(Boolean);
	criteria.push({ name, status, explanation, gaps, checks });
};

// 1. Harness reproduces.
{
	const covers = (c) => c.variant === BASE && c.phase === "rest" && !c.long;
	const checks = [];
	for (const cell of cellAggs.filter(covers)) {
		for (const side of SIDES) {
			const calls = med(cell, side, "refOtherCalls");
			const removed = med(cell, side, "removedOnlyWithPage");
			const added = med(cell, side, "addedOnlyWithPage");
			checks.push(
				cellCheck(
					cell,
					side,
					calls !== null ? calls >= 2 && removed >= 1 && added >= 1 : null,
					`passes referencing the other peer ${fmtCell(cell.sides[side].refOtherCalls, 0)}, removed-only with a next(1048) page ${fmtCell(cell.sides[side].removedOnlyWithPage, 0)}, added-only with a page ${fmtCell(cell.sides[side].addedOnlyWithPage, 0)}; shapes per run ${cell.sides[side].refOtherShapes.join(" | ")}`,
				),
			);
		}
	}
	checks.push(...missingCells(covers));
	judge(
		"Harness reproduces",
		checks,
		"base at rest: per peer at least 2 passes referencing the other peer, one removed-only and one added-only, each reading at least one next(1048) page",
	);
}

// 2. PR 1 skips both passes.
{
	const covers = (c) => isPr1(c.variant) && (c.phase === "rest" || c.phase === "writes") && !c.long;
	const checks = [];
	for (const cell of cellAggs.filter(covers)) {
		for (const side of SIDES) {
			const calls = med(cell, side, "refOtherCalls");
			const pages = med(cell, side, "pages1048");
			const parked = med(cell, side, "parked");
			const restored = med(cell, side, "restored");
			const released = med(cell, side, "released");
			checks.push(
				cellCheck(
					cell,
					side,
					calls === null
						? null
						: calls === 0 && pages === 0 && parked === 1 && restored === 1 && released === 0,
					`refOther calls ${fmtCell(cell.sides[side].refOtherCalls, 0)}, next(1048) pages ${fmtCell(cell.sides[side].pages1048, 0)}, parked ${fmtCell(cell.sides[side].parked, 0)}, restored ${fmtCell(cell.sides[side].restored, 0)}, released ${fmtCell(cell.sides[side].released, 0)}; holds per run ${cell.sides[side].parkSequence.join(" | ")}`,
				),
			);
		}
	}
	checks.push(...missingCells(covers));
	judge(
		"PR 1 skips both passes",
		checks,
		"fix in rest and writes: per peer 0 passes referencing the other peer, 0 next(1048) pages, one parked, one restored, no released",
	);
}

// 3. PR 1 CPU at rest. Judged on the CPU snapshot at the dial + cpuMarkMs,
// not on the end-of-observation total: the observation ends only once both
// sides' inventory is idle and at least 15 s have passed, so its length varies
// by run and variant, and the 15 s join-authoritative sweep falls on its edge.
{
	const covers = (c) => (isPr1(c.variant) || c.variant === BASE) && c.phase === "rest" && !c.long;
	const markAfter = (cell) => {
		const values = cell.cpuMarkAfterMs.filter((v) => typeof v === "number");
		if (values.length === 0) return "the CPU mark (not recorded)";
		return `the dial + ${values.map((v) => fmtNum(v / 1000)).join(" / ")} s${values.length < cell.cpuMarkAfterMs.length ? " (not recorded in every run)" : ""}`;
	};
	const seconds = (ms) => (typeof ms === "number" ? ms / 1000 : null);
	const perRun = (cell, side) =>
		(cells.get(cell.key) ?? [])
			.map((row) => {
				const m = row.sides[side];
				return `${row.run}: ${fmtNum(m?.cpuMarkMs, 0)} / ${fmtNum(m?.cpuMs, 0)} ms over ${fmtNum(seconds(m?.windowMs))} s, inventory ${fmtNum(m?.invPassesAtMark, 0)} / ${fmtNum(m?.invPasses, 0)} passes ${fmtNum(m?.invMsAtMark, 0)} / ${fmtNum(m?.invMs, 0)} ms`;
			})
			.join("; ");
	const checks = [];
	for (const cell of cellAggs.filter((c) => covers(c) && isPr1(c.variant))) {
		const base = findCell(BASE, cell.n, cell.min, "rest");
		for (const side of SIDES) {
			const cpu = med(cell, side, "cpuMarkMs");
			const baseCpu = base ? med(base, side, "cpuMarkMs") : null;
			const basePass = base ? med(base, side, "passMsAtMark") : null;
			const bound = baseCpu !== null && basePass !== null ? baseCpu - 0.8 * basePass : null;
			checks.push(
				cellCheck(
					cell,
					side,
					cpu !== null && bound !== null ? cpu <= bound : null,
					`fix CPU at ${markAfter(cell)} ${fmtCell(cell.sides[side].cpuMarkMs, 0)} ms; bound = base ${fmtNum(baseCpu, 0)} - 0.8 x base pass time by then ${fmtNum(basePass)} = ${fmtNum(bound, 0)} ms${base ? ` (base ${runsNote(base.runs.length, expectedRuns(base.key))})` : " (no base cell)"}. Per run, CPU at the mark / at the end, the end window, inventory passes and time inside them at the mark / at the end: fix ${perRun(cell, side) || "-"}; base ${base ? perRun(base, side) || "-" : "-"}`,
					base,
				),
			);
		}
	}
	checks.push(...missingCells(covers));
	judge(
		"PR 1 CPU at rest",
		checks,
		"per peer, fix CPU at rest at most base CPU minus 0.8 times the base time inside passes, every cell; both CPU and pass time are taken at the CPU mark (the dial + 12 s by default), the end-of-observation totals are listed per run",
	);
}

// 4. Write latency after a reconnect.
{
	const covers = (c) => isPr1(c.variant) && c.phase === "writes" && !c.long;
	const checks = [];
	for (const cell of cellAggs.filter(covers)) {
		const p90 = cell.phaseLevel.writeP90.median;
		const max = cell.phaseLevel.writeMax.median;
		const maxBound = cell.n <= 9_000 ? 1_000 : 2_000;
		checks.push(
			cellCheck(
				cell,
				null,
				p90 !== null && max !== null ? p90 <= 100 && max <= maxBound : null,
				`A-to-B p90 ${fmtCell(cell.phaseLevel.writeP90)} ms (bound 100), max ${fmtCell(cell.phaseLevel.writeMax)} ms (bound ${maxBound}); first write issued ${fmtCell(cell.phaseLevel.firstWriteAfterDialMs)} ms after the dial took ${fmtCell(cell.phaseLevel.firstWriteLatencyMs)} ms; missing ${fmtNum(cell.phaseLevel.writesMissing.median)}`,
			),
		);
	}
	checks.push(...missingCells(covers));
	judge(
		"Write latency after a reconnect",
		checks,
		"fix in writes: A-to-B p90 at most 100 ms; max at most 1 s up to 9,000 entries and 2 s above",
	);
}

// 5. Controls.
{
	const baseNote = (base) =>
		base ? ` (base ${runsNote(base.runs.length, expectedRuns(base.key))})` : " (no base cell)";
	const covers = (c) => c.phase === "control" && !c.long;
	const checks = [];
	for (const cell of cellAggs.filter((c) => covers(c) && c.variant !== BASE)) {
		const base = findCell(BASE, cell.n, cell.min, "control");
		const within = (value, reference) =>
			value !== null && reference !== null && reference !== 0
				? Math.abs(value / reference - 1) <= 0.2
				: null;
		for (const side of SIDES) {
			const cpu = med(cell, side, "cpuMs");
			const baseCpu = base ? med(base, side, "cpuMs") : null;
			checks.push(
				cellCheck(
					cell,
					side,
					within(cpu, baseCpu),
					`CPU ${fmtNum(cpu)} ms vs base ${fmtNum(baseCpu)} ms (${baseCpu ? fmtNum((cpu / baseCpu - 1) * 100) : "-"}%)${baseNote(base)}`,
					base,
				),
			);
		}
		const p90 = cell.phaseLevel.writeP90.median;
		const baseP90 = base ? base.phaseLevel.writeP90.median : null;
		checks.push(
			cellCheck(
				cell,
				null,
				within(p90, baseP90),
				`write p90 ${fmtNum(p90)} ms vs base ${fmtNum(baseP90)} ms (${baseP90 ? fmtNum((p90 / baseP90 - 1) * 100) : "-"}%)${baseNote(base)}`,
				base,
			),
		);
	}
	checks.push(...missingCells(covers));
	judge(
		"Controls",
		checks,
		"control phase: per-peer CPU and write p90 within 20% of base, every variant",
	);
}

// 6. The hold expires cleanly.
{
	const covers = (c) => isPr1(c.variant) && c.long && c.phase === "rest";
	const checks = [];
	for (const row of rows.filter(covers)) {
		for (const side of SIDES) {
			const m = row.sides[side];
			const shapes = m.refOtherPassStarts.map((p) => p.shape);
			const removedPass = m.refOtherPassStarts.find((p) => p.shape === "removed");
			const addedPass = m.refOtherPassStarts.find((p) => p.shape === "added");
			const ordered =
				m.graceAt !== null &&
				removedPass !== undefined &&
				addedPass !== undefined &&
				m.graceAt <= removedPass.start &&
				removedPass.start < addedPass.start &&
				(row.healRel === null || addedPass.start >= row.healRel);
			const ok =
				m.releasedGrace === 1 &&
				m.parked === 1 &&
				m.restored === 0 &&
				m.released === 1 &&
				shapes.join(",") === "removed,added" &&
				ordered;
			checks.push({
				cell: `${row.run} ${side}`,
				ok,
				detail: `holds ${m.parkSequence || "-"}; released:grace ${fmtNum(m.graceDelayMs)} ms after parked; passes ${shapes.join(",") || "-"} at ${m.refOtherPassStarts.map((p) => fmtNum(p.start)).join(",") || "-"} ms; heal at ${fmtNum(row.healRel)} ms`,
			});
		}
		checks.push({
			cell: `${row.run} partition-era entry`,
			ok:
				row.partitionEntryMsFromHeal !== null
					? row.partitionEntryMsFromHeal <= 5_000
					: row.partitionEntryArrived === false
						? false
						: null,
			detail: `on B ${fmtNum(row.partitionEntryMsFromHeal)} ms after the heal (bound 5000)`,
		});
	}
	for (const cell of cellAggs.filter(covers)) {
		const short = shortOf(cell);
		if (short) {
			checks.push({
				cell: cellLabel(cell),
				ok: null,
				short,
				gapOnly: true,
				detail: "fewer long-partition runs than planned",
			});
		}
	}
	checks.push(...missingCells(covers));
	judge(
		"The hold expires cleanly",
		checks,
		"25 s partition: per peer one released:grace, then the base pass shape (removed-only, then added-only after the heal); the partition-era entry on B within 5 s of the heal",
	);
}

// 7. Clean processes.
{
	const checks = [];
	for (const report of reports) {
		const errors = report.errors ?? {};
		const totals = SIDES.map((side) => errors[side]);
		const unhandled =
			totals.reduce((t, e) => t + (e?.unhandled ?? 0), 0) + (errors.driver?.unhandled ?? 0);
		const uncaught =
			totals.reduce((t, e) => t + (e?.uncaught ?? 0), 0) + (errors.driver?.uncaught ?? 0);
		const exits = SIDES.map((side) => report.stop?.[side]);
		const exitOk = exits.every((s) => s && !s.killed && s.exitMs !== null && s.exitMs <= 10_000);
		checks.push({
			cell: report.__run,
			ok: unhandled === 0 && uncaught === 0 && exitOk && report.ok === true,
			detail: `unhandled ${unhandled}, uncaught ${uncaught}, exit A ${fmtNum(exits[0]?.exitMs)} ms${exits[0]?.killed ? " killed" : ""}, B ${fmtNum(exits[1]?.exitMs)} ms${exits[1]?.killed ? " killed" : ""}, run ${report.ok ? "ok" : `failed: ${(report.failures ?? []).map((f) => f.error).join("; ")}`}`,
		});
	}
	// A run that never wrote report.json (its job died, or the step timed out
	// before it) shows up only against the plan.
	if (EXPECT) {
		const found = new Map();
		for (const report of reports) {
			const key = groupKeyOf({ ...report, long: !!report.long });
			found.set(key, (found.get(key) ?? 0) + 1);
		}
		for (const [key, group] of EXPECT.groups) {
			const count = found.get(key) ?? 0;
			if (count >= group.runs) continue;
			checks.push({
				cell: `${group.variant} n=${group.n} min=${group.min}${group.long ? " long" : ""}`,
				ok: null,
				...(count === 0 ? { missing: true } : { short: { found: count, expected: group.runs } }),
				gapOnly: true,
				detail: `${count} of ${group.runs} runs left a report.json`,
			});
		}
	}
	judge(
		"Clean processes",
		checks,
		"no unhandled rejections or uncaught exceptions; each worker exits within 10 s of stop; the run completed",
	);
}

// ---------------------------------------------------------------------------
// Output

const lines = [];
lines.push("# Reconnect-cost harness summary", "");
lines.push(
	`Generated ${new Date().toISOString()} from \`${RUNS}\`: ${reports.length} runs, ${cellAggs.length} cells. Base variant \`${BASE}\`; the fix's criteria apply to ${[...PR1].map((v) => `\`${v}\``).join(", ")}.`,
	"",
	"Numbers are medians over a cell's runs, with the range in brackets when runs differ. CPU is process user+system ms since the phase's counter reset: at the CPU mark (the dial + 12 s by default, the first write + 12 s in control) and at the end of the observation, whose window follows in parentheses. ELD is monitorEventLoopDelay (resolution 10 ms, raw histogram values, so an idle loop reads about 10-12 ms). \"refOther\" counts onReplicationChange calls whose changes include a range owned by the other peer. Pages are entry-coordinate iterator next(1048) calls (the rebalance scan).",
	"",
	EXPECT
		? `Planned matrix from \`${EXPECT.file}\`: a criterion is FAIL when a check fails, else INCOMPLETE when a check is MISSING (an expected cell with no runs), short of its planned runs or n/a, else PASS.`
		: "No planned matrix (--expect), so only the cells found are judged: a cell with no runs goes unnoticed. A criterion is FAIL when a check fails, else INCOMPLETE when a check is n/a, else PASS.",
	"",
);
const notes = [...new Set(reports.flatMap((report) => report.notes ?? []))];
if (notes.length > 0) {
	lines.push("## Method notes", "", ...notes.map((note) => `- ${note}`), "");
}
const timingsOf = (report) => {
	const timings = report.setup?.logTimings;
	if (!timings?.A || !timings?.B) return "not recorded";
	const one = (t) => `${fmtNum(t.distributionDebounceTime)} / ${fmtNum(t.timeUntilRoleMaturity)}`;
	return one(timings.A) === one(timings.B) ? one(timings.A) : `A ${one(timings.A)}, B ${one(timings.B)}`;
};
lines.push("## Runs", "");
lines.push(
	"| run | variant | n | min | partition ms | entries | debounce / role maturity ms | shared-log dist sha256 | commit | ok |",
);
lines.push("|---|---|---|---|---|---|---|---|---|---|");
for (const report of reports) {
	lines.push(
		`| ${report.__run} | ${report.variant} | ${report.n} | ${report.min} | ${report.partitionMs || "-"} | ${report.entries ?? "-"} | ${timingsOf(report)} | ${String(report.build?.sharedLogIndexSha256 ?? "").slice(0, 12)} | ${String(report.build?.commit ?? "-").slice(0, 10)} | ${report.ok ? "yes" : "NO"} |`,
	);
}

let matrix = null;
if (EXPECT) {
	matrix = [...EXPECT.cells.entries()].map(([key, expected]) => {
		const found = cells.get(key)?.length ?? 0;
		return {
			key,
			cell: cellName(expected),
			expected: expected.runs,
			found,
			status: found === 0 ? "MISSING" : found < expected.runs ? "short" : "complete",
		};
	});
	const extra = cellAggs.filter((cell) => !EXPECT.cells.has(cell.key));
	const count = (status) => matrix.filter((m) => m.status === status).length;
	lines.push("", "## Planned matrix", "");
	lines.push(
		`${plural(matrix.length, "expected cell")}: ${count("complete")} complete, ${count("short")} short of their runs, ${count("MISSING")} missing.${extra.length ? ` Found but not planned: ${extra.map((cell) => cellLabel(cell)).join(", ")}.` : ""}`,
		"",
	);
	lines.push("| cell | runs found / planned | status |", "|---|---|---|");
	for (const m of matrix) lines.push(`| ${m.cell} | ${m.found} / ${m.expected} | ${m.status} |`);
}

lines.push("", "## Criteria (JUDGED_PLAN.md section 7, base vs the fix)", "");
for (const criterion of criteria) {
	lines.push(
		`- **${criterion.name}: ${criterion.status}.**${criterion.gaps.length ? ` (${criterion.gaps.join(", ")})` : ""} ${criterion.explanation}.`,
	);
	for (const check of criterion.checks) {
		const mark = check.missing
			? "MISSING"
			: check.gapOnly
				? "short"
				: `${check.ok === null ? "n/a" : check.ok ? "ok" : "FAIL"}${check.short ? ", short" : ""}`;
		lines.push(`  - ${mark}: ${check.cell}: ${check.detail}`);
	}
}

const sideTable = (phase) => {
	const list = cellAggs.filter((c) => c.phase === phase);
	if (list.length === 0) return;
	lines.push("", `## Phase: ${phase}`, "");
	lines.push(
		"| variant | n | min | long | runs | side | refOther passes | shapes | pass ms (all) | next(1048) pages | examined (refOther) | CPU ms at mark / at end (window s) | ELD p50 / p99 / max | holds | sweeps ja direct+delayed / warmup / churn | inventory passes / hashes asked / handled | write p50 / p90 / max ms |",
	);
	lines.push("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
	for (const cell of list) {
		for (const side of SIDES) {
			const s = cell.sides[side];
			const write =
				side === "B" && cell.phaseLevel.writeP90.median !== null
					? `${fmtCell(cell.phaseLevel.writeP50)} / ${fmtCell(cell.phaseLevel.writeP90)} / ${fmtCell(cell.phaseLevel.writeMax)}`
					: "-";
			const window = s.windowMs?.median !== null && s.windowMs?.median !== undefined
				? fmtCell({ ...s.windowMs, median: s.windowMs.median / 1000, range: s.windowMs.range.map((v) => v / 1000) })
				: "-";
			lines.push(
				`| ${cell.variant} | ${cell.n} | ${cell.min} | ${cell.long ? "yes" : ""} | ${runsNote(cell.runs.length, expectedRuns(cell.key))} | ${side} | ${fmtCell(s.refOtherCalls, 0)} | ${[...new Set(s.refOtherShapes)].join(" / ") || "-"} | ${fmtCell(s.passMs)} | ${fmtCell(s.pages1048, 0)} | ${fmtCell(s.refOtherExamined, 0)} | ${fmtCell(s.cpuMarkMs, 0)} / ${fmtCell(s.cpuMs, 0)} (${window}) | ${fmtCell(s.eldP50)} / ${fmtCell(s.eldP99)} / ${fmtCell(s.eldMax)} | ${s.parkSupported ? [...new Set(s.parkSequence)].join(" / ") || "none" : "absent"} | ${fmtCell(s.sweepJaDirect, 0)}+${fmtCell(s.sweepJaDelayed, 0)} / ${fmtCell(s.sweepWarmup, 0)} / ${fmtCell(s.sweepChurn, 0)} | ${fmtCell(s.invPasses, 0)} / ${fmtCell(s.invHashes, 0)} / ${fmtCell(s.invHandledHashes, 0)} | ${write} |`,
			);
		}
	}
	const endings = list.map(
		(cell) =>
			`${cell.variant} n=${cell.n} min=${cell.min}${cell.long ? " long" : ""}: observation ended ${cell.phaseLevel.observeEndedBy.join(",")}; quiesce ${cell.phaseLevel.quiesceOk.map((v) => (v ? "ok" : "NOT settled")).join(",")}; load1 ${fmtCell(cell.phaseLevel.load1Start)} -> ${fmtCell(cell.phaseLevel.load1End)}${cell.phaseLevel.firstWriteLatencyMs.median !== null ? `; first write${cell.phaseLevel.firstWriteAfterDialMs.median !== null ? ` (issued ${fmtCell(cell.phaseLevel.firstWriteAfterDialMs)} ms after the dial)` : ""} on B after ${fmtCell(cell.phaseLevel.firstWriteLatencyMs)} ms` : ""}${cell.phaseLevel.partitionEntryMsFromHeal.median !== null ? `; partition-era entry on B ${fmtCell(cell.phaseLevel.partitionEntryMsFromHeal)} ms after the heal` : ""}`,
	);
	lines.push("", ...endings.map((line) => `- ${line}`));
};
for (const phase of PHASE_ORDER) sideTable(phase);
lines.push("");

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "summary.md"), `${lines.join("\n")}\n`);
fs.writeFileSync(
	path.join(OUT, "summary.json"),
	`${JSON.stringify(
		{
			generatedAt: new Date().toISOString(),
			runsDir: RUNS,
			base: BASE,
			pr1Variants: [...PR1],
			expect: EXPECT ? { file: EXPECT.file, spec: EXPECT.spec } : null,
			matrix,
			notes,
			runs: reports.map((report) => ({
				run: report.__run,
				file: report.__file,
				variant: report.variant,
				n: report.n,
				min: report.min,
				partitionMs: report.partitionMs,
				entries: report.entries ?? null,
				logTimings: report.setup?.logTimings ?? null,
				build: report.build,
				ok: report.ok,
				failures: report.failures,
				stop: Object.fromEntries(
					SIDES.map((side) => [
						side,
						report.stop?.[side]
							? {
									stopMs: report.stop[side].stopMs,
									exitMs: report.stop[side].exitMs,
									killed: report.stop[side].killed,
								}
							: null,
					]),
				),
				errors: report.errors,
			})),
			criteria,
			cells: cellAggs,
			rows,
		},
		null,
		2,
	)}\n`,
);
const verdicts = criteria
	.map((c) => `${c.name}: ${c.status}${c.gaps.length ? ` (${c.gaps.join(", ")})` : ""}`)
	.join("; ");
console.log(`summary: ${reports.length} runs, ${cellAggs.length} cells -> ${path.join(OUT, "summary.md")}`);
console.log(verdicts);
