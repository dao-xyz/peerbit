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
const uncovered = [];
let checked = 0;
for (const file of readdirSync(testDir).sort()) {
	if (!file.endsWith(".spec.ts")) continue;
	const src = readFileSync(path.join(testDir, file), "utf8");
	for (const m of src.matchAll(/^describe\((["'`])((?:(?!\1).)*)\1/gm)) {
		const title = m[2];
		// Titles interpolating the setup name (`${setup.name} sharding` etc)
		// are parameterized and covered by the ^u32-simple/^u64-iblt part-7
		// greps; they cannot be resolved statically, so skip them here.
		if (title.includes("${")) continue;
		checked++;
		if (!greps.some((g) => g.test(title))) {
			uncovered.push(`${file}: "${title}"`);
		}
	}
}

if (uncovered.length > 0) {
	console.error(
		"Top-level describes not matched by any test:ci:part-* grep (these tests never run in CI):",
	);
	for (const u of uncovered) console.error(`  ${u}`);
	process.exit(1);
}
console.log(`OK: ${checked} literal shared-log describes are all covered by CI shard greps.`);

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
// What remains genuinely needs different infrastructure rather than a shard
// entry: the rust crates build in the Native job, and the three playwright
// suites need a browser runner.
const KNOWN_UNREACHABLE = new Set([
	"packages/log/rust",
	"packages/programs/data/shared-log/proxy/e2e",
	"packages/transport/stream/e2e/browser",
	"packages/utils/any-store/proxy/e2e",
	"packages/utils/any-store/rust",
	"packages/utils/indexer/rust",
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
