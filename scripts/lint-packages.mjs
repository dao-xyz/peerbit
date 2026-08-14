import eslintRisk from "eslint/use-at-your-own-risk";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { dirname, extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const gitCommand = process.platform === "win32" ? "git.exe" : "git";
const sourceExtensions = new Set([
	".cjs",
	".js",
	".jsx",
	".mjs",
	".ts",
	".tsx",
]);

const listedFiles = spawnSync(
	gitCommand,
	[
		"ls-files",
		"-z",
		"--cached",
		"--others",
		"--exclude-standard",
		"--",
		// Until 2026-08-14 this enumerated only `packages`, so 147 source files
		// were never linted -- every .ts/.tsx of the shipped website under
		// apps/, the docs examples, tools/, and all of scripts/ including the
		// CI guard scripts themselves. All four roots were already clean when
		// widened, so this cost no cleanup; it only stops the next regression.
		"packages",
		"apps",
		"docs",
		"scripts",
		"tools",
	],
	{
		cwd: repositoryRoot,
		encoding: "utf8",
	},
);
assert.equal(
	listedFiles.error,
	undefined,
	`git could not enumerate package sources: ${listedFiles.error?.message}`,
);
assert.equal(
	listedFiles.status,
	0,
	`git could not enumerate package sources:\n${listedFiles.stderr}`,
);

// The changeset guard runs under `pull_request_target` with elevated
// permissions, so check-changeset-required.mjs and its test are a frozen
// root-of-trust boundary: the guard itself fails any PR whose copy is not
// byte-identical to the trusted base ("frozen executable root-of-trust
// boundary"), which is what stops a PR from editing the check that decides
// whether that PR is safe. A lint autofix is still an edit, so these two files
// cannot be linted through a PR at all -- widening the scope in 2026-08 hit
// exactly that wall with three cosmetic `no-regex-spaces` findings. They are
// excluded rather than worked around; their real guard is the 2,604-line
// self-test, and their content can only change by a direct push to master.
const frozenRootOfTrust = new Set([
	["scripts", "ci", "check-changeset-required.mjs"].join(sep),
	["scripts", "ci", "check-changeset-required.test.mjs"].join(sep),
]);
const publicSegment = `${sep}public${sep}`;
const sourceFiles = listedFiles.stdout
	.split("\0")
	.filter(Boolean)
	.map((filePath) => filePath.split("/").join(sep))
	.filter(
		(filePath) =>
			sourceExtensions.has(extname(filePath)) &&
			!`${sep}${filePath}`.includes(publicSegment) &&
			!frozenRootOfTrust.has(filePath),
	)
	.sort();
assert(sourceFiles.length > 0, "package source discovery returned no files");

const { FlatESLint } = eslintRisk;
const eslint = new FlatESLint({
	cache: true,
	cwd: repositoryRoot,
	globInputPaths: false,
	overrideConfigFile: resolve(repositoryRoot, "eslint.config.js"),
	warnIgnored: false,
});
const results = await eslint.lintFiles(sourceFiles);
const formatter = await eslint.loadFormatter("stylish");
const output = formatter.format(results);
if (output) {
	process.stdout.write(output);
}

const errorCount = results.reduce(
	(count, result) => count + result.errorCount,
	0,
);
const warningCount = results.reduce(
	(count, result) => count + result.warningCount,
	0,
);
// Warning ratchet. This was 9_999 against an actual count of 15, which made
// every `warn`-level rule in eslint.config.js structurally non-blocking --
// unused vars and the rest could be added without limit and CI stayed green.
// Pin it at the real count so the budget is a ratchet rather than decoration.
// This number may go DOWN when warnings are fixed; raising it means deciding to
// accept a new warning, which should be a visible edit in a PR.
const MAX_WARNINGS = 15;
if (warningCount > MAX_WARNINGS) {
	process.stdout.write(
		`\nlint: ${warningCount} warnings exceeds the ratchet of ${MAX_WARNINGS}.\n` +
			"Fix the new warning, or lower/raise MAX_WARNINGS in scripts/lint-packages.mjs deliberately.\n",
	);
}
if (errorCount > 0 || warningCount > MAX_WARNINGS) {
	process.exitCode = 1;
}
