// Asserts every top-level describe title in shared-log tests is matched by at
// least one test:ci:part-* grep, so new spec files cannot silently drop out of
// CI (in July 2026, 9 files / 82 tests were orphaned this way). Also asserts
// every test root referenced by a test:ci:* script exists on disk, and that
// every workspace package with a test:cov script is reachable from some
// test:ci:* script (aegir --roots entry, one-level glob, or pnpm --filter) —
// enumerated roots and pnpm filters silently match nothing when a package is
// renamed or added, so this is the loud failure that mechanism lacks.
// Target location: scripts/ci/check-shard-coverage.mjs, invoked from the Lint step.
import { existsSync, readFileSync, readdirSync } from "node:fs";
import path from "node:path";

const root = process.cwd();
const pkg = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));

const greps = [];
for (const [name, cmd] of Object.entries(pkg.scripts)) {
	if (!name.startsWith("test:ci:part-")) continue;
	for (const m of cmd.matchAll(/--grep "([^"]+)"/g)) {
		greps.push(new RegExp(m[1]));
	}
}
if (greps.length === 0) {
	console.error("No --grep patterns found in test:ci:part-* scripts");
	process.exit(1);
}

const testDir = path.join(root, "packages/programs/data/shared-log/test");

// A spec file's OUTERMOST describes are the ones a shard --grep must match:
// mocha builds a full title by joining the describe chain, and every shard grep
// is anchored with ^, so only the first link decides whether a suite is
// selected. Until 2026-08-14 "outermost" was approximated by "starts at column
// 0", which silently excluded every file whose outermost describe sits inside a
// `for`/`forEach` -- ranges, sharding, replication and durable-restart-
// conformance, i.e. the four largest suites in the package and the entire
// content of shards part-7a..7e. The guard reported "OK: 72" and never
// mentioned that 4 of 59 files contributed nothing to that number.
//
// Indentation of the shallowest describe is the right proxy: a nested describe
// is always more indented than the one containing it, so the minimum indent in
// a file is its top level whether or not a loop wraps it.
const outermostDescribes = (src) => {
	const hits = [...src.matchAll(/^([\t ]*)describe\(\s*(.*)$/gm)];
	if (hits.length === 0) return [];
	const top = Math.min(...hits.map((m) => m[1].length));
	return hits.filter((m) => m[1].length === top).map((m) => m[2]);
};

// Parameterized outermost describes cannot be read off the call site, so they
// are expanded from the array that drives the loop. Deriving them (rather than
// hardcoding the titles) means uncommenting a setup or adding a backend makes
// the guard demand a shard grep for it, instead of quietly widening the blind
// spot. Every entry is proven live below: a file listed here that no longer has
// a non-literal outermost describe is a hard failure, not a silent no-op.
const stripBlockComments = (src) => src.replace(/\/\*[\s\S]*?\*\//g, "");
const arrayStrings = (src, identifier) => {
	const m = stripBlockComments(src).match(
		new RegExp(`\\b${identifier}\\b[^=]*=\\s*\\[([\\s\\S]*?)\\]`),
	);
	return m ? [...m[1].matchAll(/["'`]([^"'`]+)["'`]/g)].map((x) => x[1]) : [];
};
const setupNames = (src) => {
	const m = stripBlockComments(src).match(
		/\btestSetups\b[^=]*=\s*\[([\s\S]*?)\n\];/,
	);
	return m ? [...m[1].matchAll(/name:\s*["'`]([^"'`]+)["'`]/g)].map((x) => x[1]) : [];
};
const PARAMETERIZED = {
	// describe("ranges: " + resolution, ...) over `resolutions`
	"ranges.spec.ts": (src) =>
		arrayStrings(src, "resolutions").map((r) => `ranges: ${r}`),
	// describe(setup.name, ...) over the file's own `testSetups`
	"sharding.spec.ts": setupNames,
	"replication.spec.ts": setupNames,
	// describe(`durable restart conformance (${backend})`, ...) over `BACKENDS`
	"durable-restart-conformance.spec.ts": (src) =>
		arrayStrings(src, "BACKENDS").map(
			(b) => `durable restart conformance (${b})`,
		),
};

const uncovered = [];
const unresolved = [];
const unusedParameterized = new Set(Object.keys(PARAMETERIZED));
let checked = 0;
for (const file of readdirSync(testDir).sort()) {
	if (!file.endsWith(".spec.ts")) continue;
	const src = readFileSync(path.join(testDir, file), "utf8");
	for (const call of outermostDescribes(src)) {
		const literal = call.match(/^(["'`])((?:(?!\1).)*)\1\s*,/);
		let titles;
		if (literal && !literal[2].includes("${")) {
			titles = [literal[2]];
		} else {
			// Non-literal outermost describe: it MUST be expandable, otherwise
			// this guard is blind to the file. Failing here is the whole point
			// -- the previous version skipped these and reported success.
			const expand = PARAMETERIZED[file];
			titles = expand ? expand(src) : [];
			unusedParameterized.delete(file);
			if (titles.length === 0) {
				unresolved.push(`${file}: describe(${call.slice(0, 60)}`);
				continue;
			}
		}
		for (const title of titles) {
			checked++;
			if (!greps.some((g) => g.test(title))) {
				uncovered.push(`${file}: "${title}"`);
			}
		}
	}
}

if (unresolved.length > 0) {
	console.error(
		"Outermost describes with a non-literal title that this guard cannot expand (it therefore cannot tell whether any shard runs them):",
	);
	for (const u of unresolved) console.error(`  ${u}`);
	console.error(
		"Add an expander for the file to PARAMETERIZED in this script, or give the describe a literal title.",
	);
	process.exit(1);
}
if (unusedParameterized.size > 0) {
	console.error(
		"PARAMETERIZED entries whose file no longer has a non-literal outermost describe (stale, remove them):",
	);
	for (const f of unusedParameterized) console.error(`  ${f}`);
	process.exit(1);
}
if (uncovered.length > 0) {
	console.error(
		"Top-level describes not matched by any test:ci:part-* grep (these tests never run in CI):",
	);
	for (const u of uncovered) console.error(`  ${u}`);
	process.exit(1);
}
console.log(
	`OK: ${checked} shared-log top-level describe titles (literal and expanded) are all covered by CI shard greps.`,
);

// --- Package-level reachability -------------------------------------------
// Pre-existing debt baseline: these packages have a test:cov script but have
// never run in the shard matrix (their parent dirs have no package.json, so
// the one-level --roots globs never matched them; some are exercised
// indirectly or in the Native job instead). Shrink this list when wiring a
// package into a shard — never grow it without a deliberate decision.
//
// Every Node-runnable package has now been wired in by explicit roots (the
// glob still cannot reach them): eleven into part-1, and the peerbit-server
// pair into part-5a because its 132 tests take ~33s and part-5a is light.
//
// What is left falls in two categories, and they are kept apart on purpose:
// "some other job runs it" is a claim that can rot, while "nothing runs it" is
// a claim that cannot. Writing both as one prose sentence is how the rot hid.
//
// NATIVE_JOB_TESTED: run by a hand-rolled `cd`-and-run step in the Native job,
// which this script's --roots/--filter parser cannot see. That excuse is now
// ENFORCED below by requiring a matching `name: Test <pkg>` step in ci.yml --
// until 2026-08-14 the comment claimed all three rust crates were covered this
// way, and @peerbit/indexer-rust had NO such step: 296 tests (108 local plus
// two runs of the shared @peerbit/indexer-tests conformance suite) covering the
// RustIndex backend had never executed in CI.
const NATIVE_JOB_TESTED = new Set([
	"packages/log/rust",
	"packages/utils/any-store/rust",
	"packages/utils/indexer/rust",
]);

// BROWSER_E2E_UNRUN: playwright suites with no runner wired up. These really
// are not executed anywhere -- an honest debt entry, not a redirection.
const BROWSER_E2E_UNRUN = new Set([
	"packages/programs/data/shared-log/proxy/e2e",
	"packages/transport/stream/e2e/browser",
	"packages/utils/any-store/proxy/e2e",
]);

const KNOWN_UNREACHABLE = new Set([
	...NATIVE_JOB_TESTED,
	...BROWSER_E2E_UNRUN,
]);

const rootTokens = new Set();
const filterNames = new Set();
for (const [name, cmd] of Object.entries(pkg.scripts)) {
	if (!name.startsWith("test:ci:")) continue;
	for (const m of cmd.matchAll(/\.\/[A-Za-z0-9_@./*-]+/g)) rootTokens.add(m[0]);
	for (const m of cmd.matchAll(/--filter\s+(\S+)/g)) {
		filterNames.add(m[1].replace(/\.\.\.$/, ""));
	}
}

// Every enumerated root must exist: aegir resolves --roots through fast-glob,
// which silently drops paths that match nothing (a renamed package would keep
// CI green while its tests stop running).
const missingRoots = [...rootTokens].filter(
	(t) => !existsSync(path.join(root, t.replace(/\/\*\*$/, ""))),
);
if (missingRoots.length > 0) {
	console.error("test:ci:* roots that do not exist on disk:");
	for (const t of missingRoots) console.error(`  ${t}`);
	process.exit(1);
}

// Dirs reachable via aegir --roots: literal entries plus one-level glob
// expansion (npm scripts run under /bin/sh, where ** expands one level).
const reachableDirs = new Set();
for (const t of rootTokens) {
	if (t.endsWith("/**")) {
		const base = t.slice(2, -3);
		for (const d of readdirSync(path.join(root, base), {
			withFileTypes: true,
		})) {
			if (d.isDirectory()) reachableDirs.add(`${base}/${d.name}`);
		}
	} else {
		reachableDirs.add(t.slice(2));
	}
}

const findPackages = (dir, acc) => {
	for (const entry of readdirSync(path.join(root, dir), {
		withFileTypes: true,
	})) {
		if (
			!entry.isDirectory() ||
			entry.name === "node_modules" ||
			entry.name === "dist" ||
			entry.name === "target" ||
			entry.name.startsWith(".")
		) {
			continue;
		}
		const rel = `${dir}/${entry.name}`;
		if (existsSync(path.join(root, rel, "package.json"))) acc.push(rel);
		findPackages(rel, acc);
	}
	return acc;
};

const unreachable = [];
let reachableCount = 0;
for (const dir of ["docs", ...findPackages("packages", [])]) {
	let manifest;
	try {
		manifest = JSON.parse(
			readFileSync(path.join(root, dir, "package.json"), "utf8"),
		);
	} catch {
		continue;
	}
	if (!manifest.scripts?.["test:cov"]) continue;
	if (reachableDirs.has(dir) || filterNames.has(manifest.name)) {
		reachableCount++;
	} else if (!KNOWN_UNREACHABLE.has(dir)) {
		unreachable.push(`${dir} (${manifest.name})`);
	}
}

for (const dir of KNOWN_UNREACHABLE) {
	if (!existsSync(path.join(root, dir))) {
		console.error(
			`KNOWN_UNREACHABLE entry no longer exists, remove it: ${dir}`,
		);
		process.exit(1);
	}
}

// A package excused as "the Native job runs it" must actually have a step in
// the Native job that runs it. Without this, the excuse is unfalsifiable prose
// and a package can sit in the baseline for months with nothing executing its
// tests -- which is exactly what happened to @peerbit/indexer-rust.
const ciWorkflow = readFileSync(
	path.join(root, ".github/workflows/ci.yml"),
	"utf8",
);
const unproven = [];
for (const dir of NATIVE_JOB_TESTED) {
	const name = JSON.parse(
		readFileSync(path.join(root, dir, "package.json"), "utf8"),
	).name;
	if (!ciWorkflow.includes(`name: Test ${name}`)) {
		unproven.push(`${dir} (${name})`);
	}
}
if (unproven.length > 0) {
	console.error(
		"Packages excused as NATIVE_JOB_TESTED with no `name: Test <pkg>` step in ci.yml (nothing runs their tests):",
	);
	for (const u of unproven) console.error(`  ${u}`);
	console.error(
		"Add the Native job step, or move the package to BROWSER_E2E_UNRUN / wire it into a shard.",
	);
	process.exit(1);
}

if (unreachable.length > 0) {
	console.error(
		"Packages with a test:cov script not reachable from any test:ci:* script (their tests never run in CI):",
	);
	for (const u of unreachable) console.error(`  ${u}`);
	console.error(
		"Add the package to a shard's --roots (or a pnpm --filter leg), or — only as a deliberate decision — to KNOWN_UNREACHABLE in this script.",
	);
	process.exit(1);
}
console.log(
	`OK: ${reachableCount} packages with test:cov are reachable from test:ci:* scripts (${KNOWN_UNREACHABLE.size} known-unreachable baseline entries).`,
);
