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
		"packages",
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

const publicSegment = `${sep}public${sep}`;
const sourceFiles = listedFiles.stdout
	.split("\0")
	.filter(Boolean)
	.map((filePath) => filePath.split("/").join(sep))
	.filter(
		(filePath) =>
			sourceExtensions.has(extname(filePath)) &&
			!`${sep}${filePath}`.includes(publicSegment),
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
if (errorCount > 0 || warningCount > 9_999) {
	process.exitCode = 1;
}
