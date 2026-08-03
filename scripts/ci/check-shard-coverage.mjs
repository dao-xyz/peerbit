// Asserts every top-level describe title in shared-log tests is matched by at
// least one test:ci:part-* grep, so new spec files cannot silently drop out of
// CI (in July 2026, 9 files / 82 tests were orphaned this way).
// Target location: scripts/ci/check-shard-coverage.mjs, invoked from the Lint step.
import { readFileSync, readdirSync } from "node:fs";
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
