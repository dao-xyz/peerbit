import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
	ChangesetGuardError,
	EXPECTED_CHANGESET_GUARD_WORKFLOW,
	GitTreeRepository,
	checkChangesetRequired,
	hasReleaseRelevantManifestChange,
	parseChangeset,
	parseLsTreeZ,
	parseNameStatusZ,
	runChangesetGuardFromEnvironment,
	validateChangesetGuardWorkflow,
	validatePullRequestPayload,
} from "./check-changeset-required.mjs";

const BASE_SHA = "a".repeat(40);
const HEAD_SHA = "b".repeat(40);
const OTHER_SHA = "c".repeat(40);
const WORKSPACE_PATTERNS = ["packages/*", "packages/a/e2e"];

const config = (ignore = []) =>
	JSON.stringify(
		{
			$schema: "https://unpkg.com/@changesets/config@3.1.4/schema.json",
			changelog: ["@changesets/changelog-github", { repo: "dao-xyz/peerbit" }],
			commit: false,
			access: "public",
			baseBranch: "master",
			fixed: [],
			ignore,
			linked: [],
			updateInternalDependencies: "patch",
		},
		null,
		2,
	);
const manifest = (name, properties = {}) =>
	JSON.stringify(
		{
			name,
			version: "1.0.0",
			type: "module",
			files: ["dist"],
			scripts: { build: "tsc" },
			...properties,
		},
		null,
		2,
	);
const changeset = (releases, summary = "Describe the user-facing change.") =>
	[
		"---",
		...Object.entries(releases).map(
			([name, bump]) => `${JSON.stringify(name)}: ${bump}`,
		),
		"---",
		"",
		summary,
		"",
	].join("\n");
const workspaceYaml = (patterns = WORKSPACE_PATTERNS) =>
	["packages:", ...patterns.map((pattern) => `  - ${pattern}`), ""].join("\n");
const rootManifest = (properties = {}) =>
	manifest("fixture-root", {
		private: true,
		workspaces: WORKSPACE_PATTERNS,
		...properties,
	});

const setWorkspacePatterns = (files, patterns) => {
	const root = JSON.parse(files.get("package.json"));
	root.workspaces = patterns;
	files.set("package.json", JSON.stringify(root, null, 2));
	files.set("pnpm-workspace.yaml", workspaceYaml(patterns));
};

const defaultFiles = () =>
	new Map([
		[
			".changeset/config.json",
			config(["@fixture/ignored", "@peerbit/test-lib", "@fixture/a-e2e"]),
		],
		["package.json", rootManifest()],
		["pnpm-workspace.yaml", workspaceYaml()],
		[
			".github/workflows/changeset-guard.yml",
			EXPECTED_CHANGESET_GUARD_WORKFLOW,
		],
		["scripts/ci/check-changeset-required.mjs", "// trusted guard\n"],
		[
			"scripts/ci/check-changeset-required.test.mjs",
			"// trusted guard tests\n",
		],
		["packages/a/package.json", manifest("@fixture/a")],
		["packages/a/src/index.ts", "export const a = 1;\n"],
		[
			"packages/a/e2e/package.json",
			manifest("@fixture/a-e2e", { private: true }),
		],
		["packages/a/e2e/src/index.ts", "export const fixture = 1;\n"],
		["packages/b/package.json", manifest("@fixture/b")],
		["packages/b/src/index.ts", "export const b = 1;\n"],
		[
			"packages/ignored/package.json",
			manifest("@fixture/ignored", { private: true }),
		],
		["packages/ignored/src/index.ts", "export const ignored = 1;\n"],
		["packages/test-lib/package.json", manifest("@peerbit/test-lib")],
		["packages/test-lib/src/index.ts", "export const frozen = 1;\n"],
	]);

const cloneFiles = (files) => new Map(files);

const automaticDiff = (baseFiles, headFiles) => {
	const entries = [];
	for (const path of [
		...new Set([...baseFiles.keys(), ...headFiles.keys()]),
	].sort()) {
		const base = baseFiles.get(path);
		const head = headFiles.get(path);
		if (base === undefined && head !== undefined) {
			entries.push({
				status: "A",
				code: "A",
				oldPath: undefined,
				newPath: path,
			});
		} else if (base !== undefined && head === undefined) {
			entries.push({
				status: "D",
				code: "D",
				oldPath: path,
				newPath: undefined,
			});
		} else if (base !== head) {
			entries.push({ status: "M", code: "M", oldPath: path, newPath: path });
		}
	}
	return entries;
};

class MemoryRepository {
	constructor({
		baseFiles,
		headFiles,
		diffEntries,
		mergeBase = BASE_SHA,
		knownCommits = [BASE_SHA, HEAD_SHA, OTHER_SHA],
	}) {
		this.trees = new Map([
			[BASE_SHA, baseFiles],
			[HEAD_SHA, headFiles],
		]);
		this.entries = diffEntries ?? automaticDiff(baseFiles, headFiles);
		this.mergeBaseSha = mergeBase;
		this.knownCommits = new Set(knownCommits);
	}

	assertCommit(sha) {
		if (!this.knownCommits.has(sha)) {
			throw new ChangesetGuardError(`unknown fixture commit ${sha}`);
		}
	}

	mergeBase() {
		return this.mergeBaseSha;
	}

	diffNameStatus() {
		return this.entries;
	}

	listFiles(sha) {
		const tree = this.trees.get(sha);
		if (!tree) throw new ChangesetGuardError(`missing fixture tree ${sha}`);
		return [...tree.keys()];
	}

	readFile(sha, path) {
		const value = this.trees.get(sha)?.get(path);
		if (value === undefined) {
			throw new ChangesetGuardError(`missing fixture file ${sha}:${path}`);
		}
		return value;
	}
}

const fixture = ({ mutate, baseMutate, diffEntries, mergeBase } = {}) => {
	const baseFiles = defaultFiles();
	baseMutate?.(baseFiles);
	const headFiles = cloneFiles(baseFiles);
	mutate?.(headFiles, baseFiles);
	return new MemoryRepository({
		baseFiles,
		headFiles,
		diffEntries,
		mergeBase,
	});
};

const check = (repository, properties = {}) =>
	checkChangesetRequired({
		baseSha: BASE_SHA,
		headSha: HEAD_SHA,
		repository,
		...properties,
	});

const expectGuardFailure = async (promise, pattern) => {
	await assert.rejects(promise, (error) => {
		assert(error instanceof ChangesetGuardError);
		assert.match(error.message, pattern);
		return true;
	});
};

test("parses NUL name-status output without corrupting whitespace or renames", () => {
	const oddPath = "packages/a/src/space and\ttab\nnewline.ts";
	const parsed = parseNameStatusZ(
		`M\0${oddPath}\0R100\0packages/a/src/old name.ts\0packages/b/src/new name.ts\0`,
	);
	assert.deepEqual(parsed, [
		{ status: "M", code: "M", oldPath: oddPath, newPath: oddPath },
		{
			status: "R100",
			code: "R",
			oldPath: "packages/a/src/old name.ts",
			newPath: "packages/b/src/new name.ts",
		},
	]);
	assert.throws(() => parseNameStatusZ("M\0unterminated"), /NUL terminated/);
});

test("parses only regular Git tree blobs and rejects every special mode", () => {
	const objectId = "d".repeat(40);
	assert.deepEqual(
		parseLsTreeZ(
			`100644 blob ${objectId}\tregular file\x00100755 blob ${objectId}\texecutable\x00`,
		),
		[
			{ mode: "100644", type: "blob", objectId, path: "regular file" },
			{ mode: "100755", type: "blob", objectId, path: "executable" },
		],
	);
	for (const [mode, type] of [
		["120000", "blob"],
		["160000", "commit"],
		["040000", "tree"],
	]) {
		assert.throws(
			() => parseLsTreeZ(`${mode} ${type} ${objectId}\tunsafe\x00`),
			/only regular 100644\/100755 blobs/,
		);
	}
});

test("real Git mode 120000 entries fail before package discovery", async () => {
	const directory = await mkdtemp(join(tmpdir(), "peerbit-tree-mode-"));
	const identityEnvironment = {
		...process.env,
		GIT_AUTHOR_NAME: "peerbit-org",
		GIT_AUTHOR_EMAIL: "273107789+peerbit-org@users.noreply.github.com",
		GIT_COMMITTER_NAME: "peerbit-org",
		GIT_COMMITTER_EMAIL: "273107789+peerbit-org@users.noreply.github.com",
	};
	const runFixtureGit = (args) => {
		const result = spawnSync("git", args, {
			cwd: directory,
			env: identityEnvironment,
			encoding: "utf8",
		});
		assert.equal(result.status, 0, result.stderr || result.stdout);
		return result.stdout.trim();
	};
	try {
		runFixtureGit(["init", "--quiet", "--initial-branch=master"]);
		runFixtureGit(["config", "user.name", "peerbit-org"]);
		runFixtureGit([
			"config",
			"user.email",
			"273107789+peerbit-org@users.noreply.github.com",
		]);
		assert.equal(runFixtureGit(["config", "user.name"]), "peerbit-org");
		assert.equal(
			runFixtureGit(["config", "user.email"]),
			"273107789+peerbit-org@users.noreply.github.com",
		);
		await writeFile(join(directory, "target.txt"), "target\n", "utf8");
		await symlink("target.txt", join(directory, "link.txt"));
		runFixtureGit(["add", "--all"]);
		runFixtureGit(["commit", "--quiet", "--message", "test: symlink mode"]);
		const sha = runFixtureGit(["rev-parse", "HEAD"]);
		assert.throws(
			() => new GitTreeRepository(directory).listFiles(sha),
			/link\.txt \(120000 blob\)/,
		);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("requires one new changeset for one release-relevant package", async () => {
	const repository = fixture({
		mutate: (head) => {
			head.set("packages/a/src/index.ts", "export const a = 2;\n");
			head.set(".changeset/a.md", changeset({ "@fixture/a": "patch" }));
		},
	});
	const result = await check(repository);
	assert.deepEqual(result.affected, ["@fixture/a"]);
	assert.deepEqual(result.changesets, [".changeset/a.md"]);
});

test("requires every affected package when one PR changes two packages", async () => {
	const missing = fixture({
		mutate: (head) => {
			head.set("packages/a/src/index.ts", "export const a = 2;\n");
			head.set("packages/b/src/index.ts", "export const b = 2;\n");
			head.set(".changeset/a.md", changeset({ "@fixture/a": "minor" }));
		},
	});
	await expectGuardFailure(check(missing), /@fixture\/b/);

	const covered = fixture({
		mutate: (head) => {
			head.set("packages/a/src/index.ts", "export const a = 2;\n");
			head.set("packages/b/src/index.ts", "export const b = 2;\n");
			head.set(
				".changeset/both.md",
				changeset({ "@fixture/a": "minor", "@fixture/b": "patch" }),
			);
		},
	});
	assert.deepEqual((await check(covered)).affected, [
		"@fixture/a",
		"@fixture/b",
	]);
});

test("rejects unrelated, empty, and malformed new changesets", async () => {
	for (const [name, body, pattern] of [
		["unrelated", changeset({ "@fixture/b": "patch" }), /@fixture\/a/],
		["empty releases", "---\n---\n\nSummary.\n", /no package releases/],
		[
			"empty summary",
			'---\n"@fixture/a": patch\n---\n',
			/no user-facing summary/,
		],
		[
			"bad bump",
			'---\n"@fixture/a": tiny\n---\n\nSummary.\n',
			/unsupported bump/,
		],
		[
			"uppercase bump",
			'---\n"@fixture/a": PATCH\n---\n\nSummary.\n',
			/unsupported bump/,
		],
	]) {
		const repository = fixture({
			mutate: (head) => {
				head.set("packages/a/src/index.ts", "export const a = 2;\n");
				head.set(`.changeset/${name}.md`, body);
			},
		});
		await expectGuardFailure(check(repository), pattern);
	}
});

test("rejects changeset summaries that can restructure generated changelogs", async () => {
	for (const [name, summary, pattern] of [
		["raw-html", "<pre>", /raw HTML/],
		["atx-h2", "ok\n## Injected", /ATX H1\/H2/],
		["atx-h1", "ok\n# Injected", /ATX H1\/H2/],
		["bare-cr-atx-h2", "ok\r## Injected", /ATX H1\/H2/],
		["bare-cr-atx-h1", "ok\r# Injected", /ATX H1\/H2/],
		["setext-h2", "Heading\n---", /Setext H1\/H2/],
		["four-space-setext-h2", "Fix.\n    Heading\n    ---", /Setext H1\/H2/],
		["tab-setext-h1", "Fix.\n\tHeading\n\t===", /Setext H1\/H2/],
		["unclosed-backtick", "ok\n```", /fence delimiters/],
		["four-space-backtick", "Fix.\n    ```text\n    code", /fence delimiters/],
		["four-space-tilde", "Fix.\n    ~~~text\n    code", /fence delimiters/],
		["tab-tilde", "Fix.\n\t~~~text\n\tcode", /fence delimiters/],
		[
			"balanced-backtick",
			"ok\n\n```js\n# Hidden H1\n## Hidden H2\n```",
			/fence delimiters/,
		],
		["balanced-tilde", "ok\n\n~~~text\nvalue\n~~~", /fence delimiters/],
	]) {
		const repository = fixture({
			mutate: (head) => {
				head.set("packages/a/src/index.ts", "export const a = 2;\n");
				head.set(
					`.changeset/${name}.md`,
					changeset({ "@fixture/a": "patch" }, summary),
				);
			},
		});
		await expectGuardFailure(check(repository), pattern);
	}

	const validMarkdown = fixture({
		mutate: (head) => {
			head.set("packages/a/src/index.ts", "export const a = 2;\n");
			head.set(
				".changeset/structured-summary.md",
				changeset(
					{ "@fixture/a": "patch" },
					"Fix behavior.\n\n- Preserves lists\n- Preserves **emphasis**.",
				),
			);
		},
	});
	assert.deepEqual((await check(validMarkdown)).affected, ["@fixture/a"]);
});

test("changeset frontmatter accepts standard quoting and rejects YAML ambiguity", () => {
	assert.deepEqual(
		[
			...parseChangeset(
				'---\n"@fixture/a": patch\nplain-package: minor\n---\n\nSummary.\n',
			).releases,
		],
		[
			["@fixture/a", "patch"],
			["plain-package", "minor"],
		],
	);
	assert.deepEqual(
		[...parseChangeset("---\n'@fixture/a': patch\n---\n\nSummary.\n").releases],
		[["@fixture/a", "patch"]],
	);
	assert.deepEqual(
		[...parseChangeset('---\r"@fixture/a": patch\r---\r\rSummary.\r').releases],
		[["@fixture/a", "patch"]],
	);
	for (const [body, pattern] of [
		["---\n@fixture/a: patch\n---\n\nSummary.\n", /quote scoped/],
		['---\n"@fixture/a":patch\n---\n\nSummary.\n', /flat package/],
		["---\nplain-package:patch\n---\n\nSummary.\n", /flat package/],
		['---\n"@fixture/a":\tpatch\n---\n\nSummary.\n', /tabs/],
		['---\n"@fixture/a": patch # forged\n---\n\nSummary.\n', /comments/],
		['---\n"@Fixture/a": patch\n---\n\nSummary.\n', /invalid flat/],
	]) {
		assert.throws(() => parseChangeset(body), pattern);
	}
});

test("never lets an existing changeset provide new PR coverage", async () => {
	const unchanged = fixture({
		baseMutate: (base) => {
			base.set(".changeset/existing.md", changeset({ "@fixture/a": "patch" }));
		},
		mutate: (head) => {
			head.set("packages/a/src/index.ts", "export const a = 2;\n");
		},
	});
	await expectGuardFailure(check(unchanged), /@fixture\/a/);

	const modified = fixture({
		baseMutate: (base) => {
			base.set(".changeset/existing.md", changeset({ "@fixture/a": "patch" }));
		},
		mutate: (head) => {
			head.set("packages/a/src/index.ts", "export const a = 2;\n");
			head.set(".changeset/existing.md", changeset({ "@fixture/a": "minor" }));
		},
	});
	await expectGuardFailure(check(modified), /not new at the merge base/);

	const deleted = fixture({
		baseMutate: (base) => {
			base.set(".changeset/existing.md", changeset({ "@fixture/a": "patch" }));
		},
		mutate: (head) => {
			head.delete(".changeset/existing.md");
		},
	});
	await expectGuardFailure(check(deleted), /not new at the merge base/);
});

test("allows valid changeset-only release repair PRs", async () => {
	const repository = fixture({
		mutate: (head) => {
			head.set(
				".changeset/release-repair.md",
				changeset({ "@fixture/a": "patch" }, "Release an already merged fix."),
			);
		},
	});
	const result = await check(repository);
	assert.deepEqual(result.affected, []);
	assert.deepEqual(result.changesets, [".changeset/release-repair.md"]);
});

test("changeset filenames mirror @changesets/read basename filtering", async () => {
	for (const path of [
		".changeset/.ignored.md",
		".changeset/.md",
		".changeset/readme.md",
		".changeset/ReAdMe.md",
		".changeset/not-a-change.MD",
		".changeset/nested/change.md",
	]) {
		const repository = fixture({
			mutate: (head) => {
				head.set("packages/a/src/index.ts", "export const a = 2;\n");
				head.set(path, changeset({ "@fixture/a": "patch" }));
			},
		});
		await expectGuardFailure(check(repository), /@fixture\/a/);
	}
});

test("exempts docs, tests, benchmarks, and tool-only configuration", async () => {
	const repository = fixture({
		mutate: (head) => {
			for (const [path, value] of [
				["packages/a/README.md", "New docs.\n"],
				["packages/a/docs/guide.md", "Guide.\n"],
				["packages/a/src/index.spec.ts", 'test("a", () => {});\n'],
				["packages/a/src/types.test.d.ts", "export {};\n"],
				["packages/a/test/integration.ts", "export {};\n"],
				["packages/a/e2e/browser.ts", "export {};\n"],
				["packages/a/benchmark/index.ts", "export {};\n"],
				["packages/a/tsconfig.build.json", "{}\n"],
				["packages/a/eslint.config.mjs", "export default [];\n"],
				["scripts/root-tool.mjs", "export {};\n"],
			]) {
				head.set(path, value);
			}
		},
	});
	assert.deepEqual((await check(repository)).affected, []);
});

test("uses the longest package root and exempts private and ignored packages", async () => {
	const repository = fixture({
		mutate: (head) => {
			head.set("packages/a/e2e/src/index.ts", "export const fixture = 2;\n");
			head.set("packages/ignored/src/index.ts", "export const ignored = 2;\n");
			head.set("packages/test-lib/src/index.ts", "export const frozen = 2;\n");
		},
	});
	assert.deepEqual((await check(repository)).affected, []);
});

test("treats scripts, exports, files, and packaged assets as release semantics", async () => {
	for (const [label, mutate] of [
		[
			"scripts",
			(value) => ({
				...value,
				scripts: { ...value.scripts, prepare: "node build.mjs" },
			}),
		],
		["exports", (value) => ({ ...value, exports: { ".": "./dist/index.js" } })],
		["files", (value) => ({ ...value, files: ["dist", "assets"] })],
	]) {
		const repository = fixture({
			mutate: (head) => {
				const value = JSON.parse(head.get("packages/a/package.json"));
				head.set(
					"packages/a/package.json",
					JSON.stringify(mutate(value), null, 2),
				);
			},
		});
		await expectGuardFailure(
			check(repository),
			new RegExp(`@fixture/a`),
			label,
		);
	}

	const asset = fixture({
		mutate: (head) => head.set("packages/a/assets/runtime.wasm", "bytes"),
	});
	await expectGuardFailure(check(asset), /@fixture\/a/);
	const cargo = fixture({
		mutate: (head) =>
			head.set("packages/a/Cargo.toml", "[package]\nname = 'a'\n"),
	});
	await expectGuardFailure(check(cargo), /@fixture\/a/);
	const srcJs = fixture({
		mutate: (head) =>
			head.set("packages/a/src_js/runtime.ts", "export const runtime = 1;\n"),
	});
	await expectGuardFailure(check(srcJs), /@fixture\/a/);
	const publishedFile = fixture({
		mutate: (head) =>
			head.set("packages/a/dist/runtime.js", "export const runtime = 1;\n"),
	});
	await expectGuardFailure(check(publishedFile), /@fixture\/a/);
	const dependency = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.dependencies = { runtime: "2.0.0" };
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(check(dependency), /@fixture\/a/);

	const devOnly = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.devDependencies = { typescript: "9.0.0" };
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
	});
	assert.deepEqual((await check(devOnly)).affected, []);
	assert.equal(
		hasReleaseRelevantManifestChange(
			{ name: "a", version: "1", devDependencies: { x: "1" } },
			{ name: "a", version: "1", devDependencies: { x: "2" } },
		),
		false,
	);
});

test("runtime scope cannot hide under docs, test, tool, or files negation segments", async () => {
	for (const path of [
		"packages/a/src/docs/runtime.ts",
		"packages/a/src/test/runtime.ts",
		"packages/a/lib/docs/runtime.js",
		"packages/a/dist/tests/runtime.js",
	]) {
		const repository = fixture({
			mutate: (head) => head.set(path, "export const runtime = 1;\n"),
		});
		await expectGuardFailure(check(repository), /@fixture\/a/);
	}

	const allNegations = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			value.files = ["!test/**"];
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
		mutate: (head) => head.set("packages/a/runtime.bin", "runtime bytes"),
	});
	await expectGuardFailure(check(allNegations), /@fixture\/a/);

	const defaultPacklist = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			delete value.files;
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
		mutate: (head) => head.set("packages/a/images/runtime.bin", "bytes"),
	});
	await expectGuardFailure(check(defaultPacklist), /@fixture\/a/);
});

test("all runtime/type entrypoints stay relevant in exempt-looking paths", async () => {
	const nestedExports = {
		exports: {
			".": {
				types: "tools/export-types",
				import: [
					"test/export-import",
					null,
					false,
					{ browser: "tools/export-browser" },
				],
			},
			"./disabled": false,
		},
	};
	for (const [entrypoint, path] of [
		[{ main: "test/runtime" }, "test/runtime"],
		[{ module: "test/module" }, "test/module"],
		[{ types: "tools/types" }, "tools/types"],
		[{ typings: "test/typings" }, "test/typings"],
		[{ browser: { "tools/browser": false } }, "tools/browser"],
		[
			{ browser: { "./dist/browser.js": "test/browser-runtime" } },
			"test/browser-runtime",
		],
		[{ bin: { peerbit: "tools/peerbit" } }, "tools/peerbit"],
		[nestedExports, "tools/export-types"],
		[nestedExports, "test/export-import"],
		[nestedExports, "tools/export-browser"],
	]) {
		const repository = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/a/package.json"));
				Object.assign(value, entrypoint);
				base.set("packages/a/package.json", JSON.stringify(value, null, 2));
				base.set(`packages/a/${path}`, "base runtime\n");
			},
			mutate: (head) => head.set(`packages/a/${path}`, "head runtime\n"),
		});
		await expectGuardFailure(check(repository), /@fixture\/a/);
	}

	for (const path of ["test/runtime.js", "test/runtime/index.js"]) {
		const extensionlessEntrypoint = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/a/package.json"));
				value.main = "test/runtime";
				base.set("packages/a/package.json", JSON.stringify(value, null, 2));
				base.set(`packages/a/${path}`, "base runtime\n");
			},
			mutate: (head) => head.set(`packages/a/${path}`, "head runtime\n"),
		});
		await expectGuardFailure(check(extensionlessEntrypoint), /@fixture\/a/);
	}
});

test("type entrypoints use TypeScript extension substitution without widening runtime entrypoints", async () => {
	for (const [field, target, path] of [
		["types", "test/runtime.js", "test/runtime.ts"],
		["types", "test/runtime.js", "test/runtime.tsx"],
		["types", "test/runtime.js", "test/runtime.d.ts"],
		["typings", "test/runtime.jsx", "test/runtime.d.ts"],
		["types", "test/runtime.mjs", "test/runtime.mts"],
		["typings", "test/runtime.mjs", "test/runtime.d.mts"],
		["types", "test/runtime.cjs", "test/runtime.cts"],
		["typings", "test/runtime.cjs", "test/runtime.d.cts"],
	]) {
		const typeEntrypoint = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/a/package.json"));
				value[field] = target;
				base.set("packages/a/package.json", JSON.stringify(value, null, 2));
				base.set(`packages/a/${path}`, "export type Runtime = 1;\n");
			},
			mutate: (head) =>
				head.set(`packages/a/${path}`, "export type Runtime = 2;\n"),
		});
		await expectGuardFailure(check(typeEntrypoint), /@fixture\/a/);
	}

	for (const [target, path] of [
		["test/runtime.js", "test/runtime-extra.d.ts"],
		["test/runtime.mjs", "test/runtime.d.ts"],
		["test/runtime.cjs", "test/runtime.d.ts"],
	]) {
		const unrelatedTypePath = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/a/package.json"));
				value.types = target;
				base.set("packages/a/package.json", JSON.stringify(value, null, 2));
				base.set(`packages/a/${path}`, "export type Runtime = 1;\n");
			},
			mutate: (head) =>
				head.set(`packages/a/${path}`, "export type Runtime = 2;\n"),
		});
		assert.deepEqual((await check(unrelatedTypePath)).affected, []);
	}

	for (const field of ["main", "module"]) {
		const runtimeEntrypoint = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/a/package.json"));
				value[field] = "test/runtime.js";
				base.set("packages/a/package.json", JSON.stringify(value, null, 2));
				base.set("packages/a/test/runtime.d.ts", "export type Runtime = 1;\n");
			},
			mutate: (head) =>
				head.set("packages/a/test/runtime.d.ts", "export type Runtime = 2;\n"),
		});
		assert.deepEqual((await check(runtimeEntrypoint)).affected, []);
	}
});

test("typesVersions targets stay relevant when their declaration files may be packed", async () => {
	const wildcardTarget = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			delete value.files;
			value.typesVersions = { "*": { "*": ["tools/types/*"] } };
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
			base.set("packages/a/tools/types/a.d.ts", "export type A = 1;\n");
		},
		mutate: (head) =>
			head.set("packages/a/tools/types/a.d.ts", "export type A = 2;\n"),
	});
	await expectGuardFailure(check(wildcardTarget), /@fixture\/a/);

	for (const path of [
		"test/runtime",
		"test/runtime.d.ts",
		"test/runtime/index.d.ts",
	]) {
		const exactTarget = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/a/package.json"));
				delete value.files;
				value.typesVersions = { "*": { runtime: ["test/runtime"] } };
				base.set("packages/a/package.json", JSON.stringify(value, null, 2));
				base.set(`packages/a/${path}`, "export type Runtime = 1;\n");
			},
			mutate: (head) =>
				head.set(`packages/a/${path}`, "export type Runtime = 2;\n"),
		});
		await expectGuardFailure(check(exactTarget), /@fixture\/a/);
	}

	for (const [typesVersions, path] of [
		[{ "*": { "*": ["tools/*/index"] } }, "tools/v4/index.d.ts"],
		[{ "*": { "*": ["test/*.js"] } }, "test/runtime.d.ts"],
		[{ "*": { "*": ["test/*.mjs"] } }, "test/runtime.mts"],
		[{ "*": { "*": ["test/*.mjs"] } }, "test/runtime.d.mts"],
		[{ "*": { "*": ["test/*.cjs"] } }, "test/runtime.cts"],
		[{ "*": { "*": ["test/*.cjs"] } }, "test/runtime.d.cts"],
		[{ "*": { "*": ["test/runtime.js"] } }, "test/runtime.tsx"],
		[
			{
				"<4": { "*": ["test/legacy/*"] },
				">=4": { "*": ["test/current/*"] },
			},
			"test/current/runtime.d.ts",
		],
		[
			{ "*": { "*": ["test/missing/*", "tools/fallback/*"] } },
			"tools/fallback/runtime.d.ts",
		],
	]) {
		const mappedTarget = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/a/package.json"));
				delete value.files;
				value.typesVersions = typesVersions;
				base.set("packages/a/package.json", JSON.stringify(value, null, 2));
				base.set(`packages/a/${path}`, "export type Runtime = 1;\n");
			},
			mutate: (head) =>
				head.set(`packages/a/${path}`, "export type Runtime = 2;\n"),
		});
		await expectGuardFailure(check(mappedTarget), /@fixture\/a/);
	}

	const narrowTarget = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			delete value.files;
			value.typesVersions = { "*": { "*": ["test/types/*"] } };
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
			base.set("packages/a/test/other.d.ts", "export type Other = 1;\n");
		},
		mutate: (head) =>
			head.set("packages/a/test/other.d.ts", "export type Other = 2;\n"),
	});
	assert.deepEqual((await check(narrowTarget)).affected, []);

	const wildcardKeepsDocsExempt = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			delete value.files;
			value.typesVersions = { "*": { "*": ["*"] } };
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
			base.set("packages/a/README.md", "Base docs.\n");
		},
		mutate: (head) => head.set("packages/a/README.md", "Head docs.\n"),
	});
	assert.deepEqual((await check(wildcardKeepsDocsExempt)).affected, []);
});

test("packlist metadata and malformed runtime paths fail conservatively", async () => {
	const wildcard = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			value.files = ["cli-*"];
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
		mutate: (head) => head.set("packages/a/cli-peerbit", "runtime\n"),
	});
	await expectGuardFailure(check(wildcard), /@fixture\/a/);

	const gitignore = fixture({
		mutate: (head) => head.set("packages/a/.gitignore", "dist/private\n"),
	});
	await expectGuardFailure(check(gitignore), /@fixture\/a/);

	const unsupportedExtglob = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			value.files = ["@(test|dist)/**"];
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
		mutate: (head) => head.set("packages/a/README.md", "packlist-sensitive\n"),
	});
	await expectGuardFailure(check(unsupportedExtglob), /@fixture\/a/);

	const evenBangPositive = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			value.files = ["!!test/**"];
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
		mutate: (head) =>
			head.set("packages/a/test/runtime.js", "export const runtime = 1;\n"),
	});
	await expectGuardFailure(check(evenBangPositive), /@fixture\/a/);

	for (const mutateManifest of [
		(value) => {
			value.main = "../escape";
		},
		(value) => {
			value.main = "test/*";
		},
		(value) => {
			value.module = "../escape";
		},
		(value) => {
			value.types = "/escape";
		},
		(value) => {
			value.exports = { ".": { import: ["./dist/index.js", "../escape"] } };
		},
		(value) => {
			value.browser = { "./dist/index.js": 42 };
		},
		(value) => {
			value.bin = { peerbit: "/escape" };
		},
		(value) => {
			value.files = ["../escape"];
		},
	]) {
		const malformed = fixture({
			mutate: (head) => {
				const value = JSON.parse(head.get("packages/a/package.json"));
				mutateManifest(value);
				head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			},
		});
		await expectGuardFailure(check(malformed), /(?:unsafe|non-empty)/);
	}

	for (const [typesVersions, pattern] of [
		[null, /typesVersions must be an object/],
		[[], /typesVersions must be an object/],
		[{ "*": [] }, /must be a path mapping object/],
		[{ "*": null }, /must be a path mapping object/],
		[{ "*": { "*": "tools/types/*" } }, /non-empty target array/],
		[{ "*": { "*": [] } }, /non-empty target array/],
		[{ "*": { "**": ["tools/types/*"] } }, /at most one \*/],
		[{ "*": { "*": ["tools/**/types"] } }, /at most one \*/],
		[{ "*": { "*": ["../escape/*"] } }, /unsafe or unsupported/],
		[{ "*": { "*": ["/escape/*"] } }, /unsafe or unsupported/],
		[{ "*": { "*": ["tools\\types/*"] } }, /unsafe or unsupported/],
		[{ "*": { "*": ["tools/\0types/*"] } }, /unsafe or unsupported/],
		[{ "*": { "*": [42] } }, /non-empty package-relative pattern/],
	]) {
		const malformedTypesVersions = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/a/package.json"));
				value.typesVersions = typesVersions;
				base.set("packages/a/package.json", JSON.stringify(value, null, 2));
			},
		});
		await expectGuardFailure(check(malformedTypesVersions), pattern);
	}
});

test("only authoritative workspace roots may own files", async () => {
	const shadow = fixture({
		baseMutate: (base) => {
			base.set(
				"packages/a/src/package.json",
				manifest("@fixture/private-shadow", { private: true }),
			);
		},
		mutate: (head) => {
			head.set("packages/a/src/index.ts", "export const a = 2;\n");
		},
	});
	await expectGuardFailure(check(shadow), /@fixture\/a/);

	const policyDrift = fixture({
		mutate: (head) => {
			head.set("pnpm-workspace.yaml", workspaceYaml(["packages/a"]));
		},
	});
	await expectGuardFailure(check(policyDrift), /workspace policy drift/);

	const samePrWorkspaceShadow = fixture({
		mutate: (head) => {
			const root = JSON.parse(head.get("package.json"));
			root.workspaces.push("packages/a/runtime");
			head.set("package.json", JSON.stringify(root, null, 2));
			head.set(
				"pnpm-workspace.yaml",
				workspaceYaml([...WORKSPACE_PATTERNS, "packages/a/runtime"]),
			);
			head.set(
				"packages/a/runtime/package.json",
				manifest("@fixture/runtime-shadow", { private: true }),
			);
			head.set("packages/a/runtime/index.js", "export const hidden = true;\n");
		},
	});
	await expectGuardFailure(check(samePrWorkspaceShadow), /@fixture\/a/);
});

test("head workspace policy cannot hide newly exposed public packages", async () => {
	const exposedPackage = (withChangeset) =>
		fixture({
			baseMutate: (base) => {
				base.set("plugins/new/package.json", manifest("@fixture/plugin"));
				base.set("plugins/new/src/index.ts", "export const plugin = true;\n");
			},
			mutate: (head) => {
				setWorkspacePatterns(head, [...WORKSPACE_PATTERNS, "plugins/*"]);
				if (withChangeset) {
					head.set(
						".changeset/plugin.md",
						changeset({ "@fixture/plugin": "minor" }),
					);
				}
			},
		});

	await expectGuardFailure(check(exposedPackage(false)), /@fixture\/plugin/);
	assert.deepEqual((await check(exposedPackage(true))).affected, [
		"@fixture/plugin",
	]);

	const deactivated = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			delete value.version;
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			head.set(".changeset/a.md", changeset({ "@fixture/a": "major" }));
		},
	});
	await expectGuardFailure(check(deactivated), /cannot be removed/);
});

test("ownership transitions include the new head-policy owner", async () => {
	const nestedRoot = "packages/a/nested";
	const transitioning = (mutation, withChangeset = false) =>
		fixture({
			baseMutate: (base) => {
				setWorkspacePatterns(base, [...WORKSPACE_PATTERNS, nestedRoot]);
				base.set(
					`${nestedRoot}/package.json`,
					manifest("@fixture/private-nested", { private: true }),
				);
				base.set(`${nestedRoot}/src/value.ts`, "export const value = 1;\n");
			},
			mutate: (head) => {
				setWorkspacePatterns(head, WORKSPACE_PATTERNS);
				mutation(head);
				if (withChangeset) {
					head.set(
						".changeset/parent.md",
						changeset({ "@fixture/a": "patch" }),
					);
				}
			},
		});

	for (const mutation of [
		(head) =>
			head.set(`${nestedRoot}/src/value.ts`, "export const value = 2;\n"),
		(head) => head.delete(`${nestedRoot}/src/value.ts`),
		(head) => {
			head.delete(`${nestedRoot}/src/value.ts`);
			head.set("packages/a/moved.ts", "export const value = 1;\n");
		},
	]) {
		await expectGuardFailure(check(transitioning(mutation)), /@fixture\/a/);
	}

	assert.deepEqual(
		(
			await check(
				transitioning(
					(head) =>
						head.set(`${nestedRoot}/src/value.ts`, "export const value = 2;\n"),
					true,
				),
			)
		).affected,
		["@fixture/a"],
	);
});

test("guard bootstrap is a both-files transition and the pair cannot be removed", async () => {
	const bootstrap = fixture({
		baseMutate: (base) => {
			base.delete(".github/workflows/changeset-guard.yml");
			base.delete("scripts/ci/check-changeset-required.mjs");
			base.delete("scripts/ci/check-changeset-required.test.mjs");
		},
		mutate: (head) => {
			head.set(
				".github/workflows/changeset-guard.yml",
				EXPECTED_CHANGESET_GUARD_WORKFLOW,
			);
			head.set(
				"scripts/ci/check-changeset-required.mjs",
				"// introducing guard\n",
			);
			head.set(
				"scripts/ci/check-changeset-required.test.mjs",
				"// introducing tests\n",
			);
		},
	});
	assert.equal((await check(bootstrap)).kind, "ordinary-pr");

	for (const removedPaths of [
		[".github/workflows/changeset-guard.yml"],
		["scripts/ci/check-changeset-required.mjs"],
		[
			".github/workflows/changeset-guard.yml",
			"scripts/ci/check-changeset-required.mjs",
			"scripts/ci/check-changeset-required.test.mjs",
		],
	]) {
		const removal = fixture({
			mutate: (head) => {
				for (const path of removedPaths) head.delete(path);
			},
		});
		await expectGuardFailure(check(removal), /required and cannot be removed/);
	}

	const partialBase = fixture({
		baseMutate: (base) => {
			base.delete("scripts/ci/check-changeset-required.test.mjs");
		},
		mutate: (head) => {
			head.set(
				"scripts/ci/check-changeset-required.test.mjs",
				"// repaired tests\n",
			);
		},
	});
	await expectGuardFailure(
		check(partialBase),
		/partial changeset guard trust boundary/,
	);
});

test("post-bootstrap executable guard files are byte-frozen to the base", async () => {
	for (const paths of [
		["scripts/ci/check-changeset-required.mjs"],
		["scripts/ci/check-changeset-required.test.mjs"],
		[
			"scripts/ci/check-changeset-required.mjs",
			"scripts/ci/check-changeset-required.test.mjs",
		],
	]) {
		const modified = fixture({
			mutate: (head) => {
				for (const path of paths) {
					head.set(path, `${head.get(path)}// untrusted rewrite\n`);
				}
			},
		});
		await expectGuardFailure(
			check(modified),
			/frozen executable root-of-trust boundary.*byte-identical/,
		);
	}
});

test("the base-owned target workflow is an exact data-only contract", async () => {
	assert.equal(
		validateChangesetGuardWorkflow(EXPECTED_CHANGESET_GUARD_WORKFLOW),
		true,
	);
	for (const mutated of [
		EXPECTED_CHANGESET_GUARD_WORKFLOW.replace(
			"permissions:\n  contents: read",
			"permissions:\n  contents: write",
		),
		EXPECTED_CHANGESET_GUARD_WORKFLOW.replace(
			"ref: ${{ github.event.pull_request.base.sha }}",
			"ref: ${{ github.event.pull_request.head.sha }}",
		),
		EXPECTED_CHANGESET_GUARD_WORKFLOW.replace(
			"actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020",
			"actions/setup-node@v4",
		),
		EXPECTED_CHANGESET_GUARD_WORKFLOW.replace(
			"EVENT_REPOSITORY: ${{ github.repository }}",
			"EVENT_REPOSITORY: ${{ secrets.REPOSITORY_TOKEN }}",
		),
		EXPECTED_CHANGESET_GUARD_WORKFLOW.replace(
			"      - name: Fetch the untrusted head as data and run the base guard",
			"      - name: Cache attacker state\n        uses: actions/cache@v4\n      - name: Fetch the untrusted head as data and run the base guard",
		),
		EXPECTED_CHANGESET_GUARD_WORKFLOW.replace(
			"node scripts/ci/check-changeset-required.mjs",
			"git checkout refs/remotes/pull-request/head\n          node scripts/ci/check-changeset-required.mjs",
		),
	]) {
		assert.throws(
			() => validateChangesetGuardWorkflow(mutated),
			/exactly match the canonical data-only/,
		);
	}

	const mutatedInTree = fixture({
		mutate: (head) => {
			head.set(
				".github/workflows/changeset-guard.yml",
				EXPECTED_CHANGESET_GUARD_WORKFLOW.replace(
					"contents: read",
					"contents: write",
				),
			);
		},
	});
	await expectGuardFailure(
		check(mutatedInTree),
		/exactly match the canonical data-only/,
	);
});

test("rejects every manual package version edit even with a changeset", async () => {
	const repository = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.version = "1.0.1";
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			head.set(".changeset/a.md", changeset({ "@fixture/a": "patch" }));
		},
	});
	await expectGuardFailure(
		check(repository),
		/only be changed by the generated/,
	);
});

test("a PR cannot exempt its own source change by extending Changesets ignore", async () => {
	const repository = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get(".changeset/config.json"));
			value.ignore.push("@fixture/a");
			head.set(".changeset/config.json", JSON.stringify(value, null, 2));
			head.set("packages/a/src/index.ts", "export const a = 2;\n");
		},
	});
	await expectGuardFailure(check(repository), /@fixture\/a/);
});

test("Changesets ignore entries are exact existing private package names", async () => {
	for (const ignoredName of ["@fixture/*", "../packages/a"]) {
		const malformed = fixture({
			mutate: (head) => {
				const value = JSON.parse(head.get(".changeset/config.json"));
				value.ignore.push(ignoredName);
				head.set(".changeset/config.json", JSON.stringify(value, null, 2));
			},
		});
		await expectGuardFailure(check(malformed), /exact package name/);
	}

	const duplicate = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get(".changeset/config.json"));
			value.ignore.push("@fixture/ignored");
			head.set(".changeset/config.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(check(duplicate), /must not repeat ignore entries/);

	const nonexistent = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get(".changeset/config.json"));
			value.ignore.push("@fixture/missing");
			head.set(".changeset/config.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(
		check(nonexistent),
		/does not name an authoritative/,
	);

	const publicIgnored = fixture({
		baseMutate: (base) => {
			base.set("packages/ignored/package.json", manifest("@fixture/ignored"));
		},
	});
	await expectGuardFailure(check(publicIgnored), /must remain private/);

	const madePublic = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/ignored/package.json"));
			value.private = false;
			value.publishConfig = { access: "public" };
			head.set("packages/ignored/package.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(check(madePublic), /must remain private/);
});

test("new ignore entries require a clean policy-only retirement transition", async () => {
	const retirement = (extraMutation) =>
		fixture({
			mutate: (head) => {
				const configValue = JSON.parse(head.get(".changeset/config.json"));
				configValue.ignore.push("@fixture/a");
				head.set(
					".changeset/config.json",
					JSON.stringify(configValue, null, 2),
				);
				const packageValue = JSON.parse(head.get("packages/a/package.json"));
				packageValue.private = true;
				head.set(
					"packages/a/package.json",
					JSON.stringify(packageValue, null, 2),
				);
				extraMutation?.(head);
			},
		});

	assert.deepEqual((await check(retirement())).affected, []);

	const sourceAndChangeset = retirement((head) => {
		head.set("packages/a/src/index.ts", "export const a = 2;\n");
		head.set(".changeset/a.md", changeset({ "@fixture/a": "major" }));
	});
	await expectGuardFailure(check(sourceAndChangeset), /policy-only transition/);

	const versioned = retirement((head) => {
		const value = JSON.parse(head.get("packages/a/package.json"));
		value.version = "1.0.1";
		head.set("packages/a/package.json", JSON.stringify(value, null, 2));
	});
	await expectGuardFailure(check(versioned), /keep its version frozen/);
});

test("release-plan policy stays within the guard's deterministic subset", async () => {
	for (const mutateConfig of [
		(value) => {
			value.changelog = false;
		},
		(value) => {
			value.changelog = ["./local-changelog.cjs", {}];
		},
		(value) => {
			value.changelog[1].repo = "attacker/peerbit";
		},
		(value) => {
			value.commit = true;
		},
		(value) => {
			value.access = "restricted";
		},
		(value) => {
			value.$schema = "https://example.invalid/future-schema.json";
		},
		(value) => {
			delete value.$schema;
		},
		(value) => {
			value.fixed = [["@fixture/a", "@fixture/b"]];
		},
		(value) => {
			value.changedFilePatterns = { "@fixture/a": ["src/**"] };
		},
		(value) => {
			value.snapshot = { useCalculatedVersion: true };
		},
		(value) => {
			value.prettier = false;
		},
		(value) => {
			value.futurePolicy = "silently-ignored";
		},
		(value) => {
			value.privatePackages = { version: true, tag: false };
		},
		(value) => {
			value.___experimentalUnsafeOptions_WILL_CHANGE_IN_PATCH = {
				updateInternalDependents: "always",
			};
		},
	]) {
		const repository = fixture({
			mutate: (head) => {
				const value = JSON.parse(head.get(".changeset/config.json"));
				mutateConfig(value);
				head.set(".changeset/config.json", JSON.stringify(value, null, 2));
			},
		});
		await expectGuardFailure(
			check(repository),
			/(?:supported release-plan policy|pinned .* key set)/,
		);
	}
	const prerelease = fixture({
		mutate: (head) => {
			head.set(
				".changeset/pre.json",
				JSON.stringify({ mode: "pre", tag: "next", initialVersions: {} }),
			);
		},
	});
	await expectGuardFailure(check(prerelease), /prerelease mode/);
});

test("internal dependencies preserve only supported workspace shorthands", async () => {
	const supported = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			value.dependencies = { "@fixture/b": "workspace:*" };
			value.optionalDependencies = { "@fixture/b": "workspace:^" };
			value.peerDependencies = { "@fixture/b": "workspace:~" };
			value.devDependencies = { "@fixture/b": "workspace:*" };
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
	});
	assert.deepEqual((await check(supported)).affected, []);

	for (const range of [
		"*",
		"^1.0.0",
		"workspace:^1.0.0",
		"workspace:1.0.0",
		"workspace:**",
		"file:../b",
		"link:../b",
	]) {
		const unsupported = fixture({
			mutate: (head) => {
				const value = JSON.parse(head.get("packages/a/package.json"));
				value.devDependencies = { "@fixture/b": range };
				head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			},
		});
		await expectGuardFailure(
			check(unsupported),
			/preserved internal workspace ranges/,
		);
	}
});

test("public release graphs cannot runtime-reference Changesets-skipped packages", async () => {
	const devOnly = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.devDependencies = {
				"@fixture/ignored": "workspace:*",
				"@peerbit/test-lib": "workspace:*",
			};
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
	});
	assert.deepEqual((await check(devOnly)).affected, []);

	for (const field of [
		"dependencies",
		"optionalDependencies",
		"peerDependencies",
	]) {
		const blocked = fixture({
			mutate: (head) => {
				const value = JSON.parse(head.get("packages/a/package.json"));
				value[field] = { "@fixture/ignored": "workspace:*" };
				head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			},
		});
		await expectGuardFailure(
			check(blocked),
			new RegExp(`release-blocking ${field}\\.@fixture/ignored`),
		);
	}

	const ignoredPublic = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.dependencies = { "@peerbit/test-lib": "workspace:*" };
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(
		check(ignoredPublic),
		/release-blocking dependencies\.@peerbit\/test-lib/,
	);

	const unignoredPrivate = fixture({
		baseMutate: (base) => {
			base.set(
				"packages/private/package.json",
				manifest("@fixture/private", { private: true }),
			);
		},
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.optionalDependencies = { "@fixture/private": "workspace:*" };
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(
		check(unignoredPrivate),
		/release-blocking optionalDependencies\.@fixture\/private/,
	);

	const missingVersion = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(manifest("@fixture/unversioned"));
			delete value.version;
			base.set(
				"packages/unversioned/package.json",
				JSON.stringify(value, null, 2),
			);
		},
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.peerDependencies = { "@fixture/unversioned": "workspace:*" };
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(
		check(missingVersion),
		/release-blocking peerDependencies\.@fixture\/unversioned/,
	);
});

test("unversioned workspace packages stay skipped until explicit activation", async () => {
	const unversionedBase = (base) => {
		const value = JSON.parse(manifest("@fixture/unversioned"));
		delete value.version;
		base.set(
			"packages/unversioned/package.json",
			JSON.stringify(value, null, 2),
		);
		base.set(
			"packages/unversioned/src/index.ts",
			"export const skipped = 1;\n",
		);
	};

	const skippedSource = fixture({
		baseMutate: unversionedBase,
		mutate: (head) => {
			head.set(
				"packages/unversioned/src/index.ts",
				"export const skipped = 2;\n",
			);
		},
	});
	assert.deepEqual((await check(skippedSource)).affected, []);
	for (const skippedVersion of [null, ""]) {
		const skippedVariant = fixture({
			baseMutate: (base) => {
				unversionedBase(base);
				const value = JSON.parse(base.get("packages/unversioned/package.json"));
				value.version = skippedVersion;
				base.set(
					"packages/unversioned/package.json",
					JSON.stringify(value, null, 2),
				);
			},
			mutate: (head) => {
				head.set(
					"packages/unversioned/src/index.ts",
					"export const skipped = 2;\n",
				);
			},
		});
		assert.deepEqual((await check(skippedVariant)).affected, []);
	}

	const skippedChangeset = fixture({
		baseMutate: unversionedBase,
		mutate: (head) => {
			head.set(
				".changeset/unversioned.md",
				changeset({ "@fixture/unversioned": "patch" }),
			);
		},
	});
	await expectGuardFailure(
		check(skippedChangeset),
		/not a publishable package/,
	);

	const activation = (withChangeset, version = "1.0.0") =>
		fixture({
			baseMutate: unversionedBase,
			mutate: (head) => {
				const value = JSON.parse(head.get("packages/unversioned/package.json"));
				value.version = version;
				head.set(
					"packages/unversioned/package.json",
					JSON.stringify(value, null, 2),
				);
				if (withChangeset) {
					head.set(
						".changeset/activate.md",
						changeset({ "@fixture/unversioned": "minor" }),
					);
				}
			},
		});

	await expectGuardFailure(check(activation(false)), /@fixture\/unversioned/);
	assert.deepEqual((await check(activation(true))).affected, [
		"@fixture/unversioned",
	]);

	for (const invalidVersion of [42, "next", ["1.0.0"], {}]) {
		const invalid = fixture({
			baseMutate: (base) => {
				const value = JSON.parse(base.get("packages/b/package.json"));
				value.version = invalidVersion;
				base.set("packages/b/package.json", JSON.stringify(value, null, 2));
			},
		});
		await expectGuardFailure(
			check(invalid),
			/canonical string major\.minor\.patch/,
		);
	}

	for (const invalidVersion of [["1.0.0"], {}]) {
		await expectGuardFailure(
			check(activation(true, invalidVersion)),
			/canonical string major\.minor\.patch/,
		);
		const invalidNewPackage = fixture({
			mutate: (head) => {
				head.set(
					"packages/new/package.json",
					manifest("@fixture/new", { version: invalidVersion }),
				);
				head.set("packages/new/src/index.ts", "export const value = 1;\n");
				head.set(".changeset/new.md", changeset({ "@fixture/new": "patch" }));
			},
		});
		await expectGuardFailure(
			check(invalidNewPackage),
			/canonical string major\.minor\.patch/,
		);
	}
});

test("keeps @peerbit/test-lib ignored and its version frozen", async () => {
	const unignored = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get(".changeset/config.json"));
			value.ignore = value.ignore.filter(
				(name) => name !== "@peerbit/test-lib",
			);
			head.set(".changeset/config.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(check(unignored), /must keep @peerbit\/test-lib/);

	const versioned = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/test-lib/package.json"));
			value.version = "1.0.1";
			head.set(
				"packages/test-lib/package.json",
				JSON.stringify(value, null, 2),
			);
		},
	});
	await expectGuardFailure(check(versioned), /test-lib.*frozen/);
});

test("handles additions but requires the explicit staged-removal policy", async () => {
	const added = fixture({
		mutate: (head) => {
			head.set("packages/new package/package.json", manifest("@fixture/new"));
			head.set(
				"packages/new package/src/index.ts",
				"export const value = 1;\n",
			);
			head.set(".changeset/new.md", changeset({ "@fixture/new": "minor" }));
		},
	});
	assert.deepEqual((await check(added)).affected, ["@fixture/new"]);

	const deleted = fixture({
		baseMutate: (base) => {
			base.set("packages/old/package.json", manifest("@fixture/old"));
			base.set("packages/old/src/index.ts", "export const value = 1;\n");
		},
		mutate: (head) => {
			head.delete("packages/old/package.json");
			head.delete("packages/old/src/index.ts");
			head.set(".changeset/old.md", changeset({ "@fixture/old": "major" }));
		},
	});
	await expectGuardFailure(check(deleted), /Stage each removal/);

	const moved = fixture({
		mutate: (head) => {
			head.delete("packages/a/package.json");
			head.delete("packages/a/src/index.ts");
			head.set("packages/a-moved/package.json", manifest("@fixture/a"));
			head.set("packages/a-moved/src/index.ts", "export const a = 1;\n");
		},
	});
	await expectGuardFailure(check(moved), /Stage each removal/);

	const renamed = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.name = "@fixture/a-renamed";
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(check(renamed), /Stage each removal/);

	const ignoredButStillConfigured = fixture({
		baseMutate: (base) => {
			base.set(
				"packages/old/package.json",
				manifest("@fixture/old", { private: true }),
			);
			base.set("packages/old/src/index.ts", "export const value = 1;\n");
			const value = JSON.parse(base.get(".changeset/config.json"));
			value.ignore.push("@fixture/old");
			base.set(".changeset/config.json", JSON.stringify(value, null, 2));
		},
		mutate: (head) => {
			head.delete("packages/old/package.json");
			head.delete("packages/old/src/index.ts");
		},
	});
	await expectGuardFailure(
		check(ignoredButStillConfigured),
		/does not name an authoritative workspace package/,
	);

	const staged = fixture({
		baseMutate: (base) => {
			base.set(
				"packages/old/package.json",
				manifest("@fixture/old", { private: true }),
			);
			base.set("packages/old/src/index.ts", "export const value = 1;\n");
			const value = JSON.parse(base.get(".changeset/config.json"));
			value.ignore.push("@fixture/old");
			base.set(".changeset/config.json", JSON.stringify(value, null, 2));
		},
		mutate: (head) => {
			head.delete("packages/old/package.json");
			head.delete("packages/old/src/index.ts");
			const value = JSON.parse(head.get(".changeset/config.json"));
			value.ignore = value.ignore.filter((name) => name !== "@fixture/old");
			head.set(".changeset/config.json", JSON.stringify(value, null, 2));
		},
	});
	assert.deepEqual((await check(staged)).affected, []);
});

test("a rename across package roots requires coverage for both owners", async () => {
	const baseFiles = defaultFiles();
	baseFiles.set("packages/a/src/moved.ts", "export const moved = 1;\n");
	const headFiles = cloneFiles(baseFiles);
	headFiles.delete("packages/a/src/moved.ts");
	headFiles.set("packages/b/src/moved.ts", "export const moved = 1;\n");
	headFiles.set(
		".changeset/both.md",
		changeset({ "@fixture/a": "patch", "@fixture/b": "patch" }),
	);
	const repository = new MemoryRepository({
		baseFiles,
		headFiles,
		diffEntries: [
			{
				status: "R100",
				code: "R",
				oldPath: "packages/a/src/moved.ts",
				newPath: "packages/b/src/moved.ts",
			},
			{
				status: "A",
				code: "A",
				oldPath: undefined,
				newPath: ".changeset/both.md",
			},
		],
	});
	assert.deepEqual((await check(repository)).affected, [
		"@fixture/a",
		"@fixture/b",
	]);
});

test("making a package private is staged removal; making one public is coverable", async () => {
	const madePrivate = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.private = true;
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			head.set(".changeset/privacy.md", changeset({ "@fixture/a": "major" }));
		},
	});
	await expectGuardFailure(check(madePrivate), /Stage each removal/);

	const madePublic = fixture({
		baseMutate: (base) => {
			const value = JSON.parse(base.get("packages/a/package.json"));
			value.private = true;
			base.set("packages/a/package.json", JSON.stringify(value, null, 2));
		},
		mutate: (head) => {
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.private = false;
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			head.set(".changeset/privacy.md", changeset({ "@fixture/a": "major" }));
		},
	});
	assert.deepEqual((await check(madePublic)).affected, ["@fixture/a"]);
});

test("accepts package and file paths containing whitespace", async () => {
	const repository = fixture({
		baseMutate: (base) => {
			base.set(
				"packages/space name/package.json",
				manifest("@fixture/space-name"),
			);
			base.set(
				"packages/space name/src/value with spaces.ts",
				"export const value = 1;\n",
			);
		},
		mutate: (head) => {
			head.set(
				"packages/space name/src/value with spaces.ts",
				"export const value = 2;\n",
			);
			head.set(
				".changeset/space.md",
				changeset({ "@fixture/space-name": "patch" }),
			);
		},
	});
	assert.deepEqual((await check(repository)).affected, ["@fixture/space-name"]);
});

test("frontend artifact inputs require an @peerbit/server changeset", async () => {
	const frontendRoot = "packages/clients/peerbit-server/frontend";
	const serverRoot = "packages/clients/peerbit-server/node";
	const addArtifactPackages = (base) => {
		setWorkspacePatterns(base, [
			...WORKSPACE_PATTERNS,
			frontendRoot,
			serverRoot,
		]);
		const configValue = JSON.parse(base.get(".changeset/config.json"));
		configValue.ignore.push("frontend");
		base.set(".changeset/config.json", JSON.stringify(configValue, null, 2));
		base.set(
			`${frontendRoot}/package.json`,
			manifest("frontend", { private: true }),
		);
		base.set(`${serverRoot}/package.json`, manifest("@peerbit/server"));
	};

	for (const path of [
		"src/App.tsx",
		"src/foo.test.js",
		"public/release-notes.md",
		"assets/logo.svg",
		"vite.config.ts",
	]) {
		const unversioned = fixture({
			baseMutate: addArtifactPackages,
			mutate: (head) => head.set(`${frontendRoot}/${path}`, "artifact input\n"),
		});
		await expectGuardFailure(check(unversioned), /@peerbit\/server/);
	}

	const covered = fixture({
		baseMutate: addArtifactPackages,
		mutate: (head) => {
			head.set(`${frontendRoot}/src/App.tsx`, "artifact input\n");
			head.set(
				".changeset/server-ui.md",
				changeset({ "@peerbit/server": "patch" }),
			);
		},
	});
	assert.deepEqual((await check(covered)).affected, ["@peerbit/server"]);

	const exempt = fixture({
		baseMutate: addArtifactPackages,
		mutate: (head) => {
			head.set(`${frontendRoot}/README.md`, "UI docs.\n");
			head.set(`${frontendRoot}/test/App.test.tsx`, "test only\n");
		},
	});
	assert.deepEqual((await check(exempt)).affected, []);
});

const versionFixture = ({
	actor = true,
	source = false,
	manifestExtra,
	changelog,
	existingChangelog = true,
	releaseBump = "patch",
	headVersion = "1.0.1",
	lockfileHead,
	baseExtra,
	headExtra,
} = {}) => {
	const repository = fixture({
		baseMutate: (base) => {
			base.set(
				".changeset/release.md",
				changeset({ "@fixture/a": releaseBump }),
			);
			if (existingChangelog) {
				base.set(
					"packages/a/CHANGELOG.md",
					"# Changelog\n\n## 1.0.0\n\nOld.\n",
				);
			}
			base.set(
				"pnpm-lock.yaml",
				"lockfileVersion: '9.0'\n\nsnapshots:\n  '@fixture/a@1.0.0': {}\n",
			);
			baseExtra?.(base);
		},
		mutate: (head) => {
			head.delete(".changeset/release.md");
			const value = JSON.parse(head.get("packages/a/package.json"));
			value.version = headVersion;
			manifestExtra?.(value);
			head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			head.set(
				"packages/a/CHANGELOG.md",
				changelog ??
					`# Changelog\n\n## ${headVersion}\n\n### Patch Changes\n\n- Fix.\n${
						existingChangelog ? "\n## 1.0.0\n\nOld.\n" : ""
					}`,
			);
			head.set(
				"pnpm-lock.yaml",
				lockfileHead ??
					`lockfileVersion: '9.0'\n\nsnapshots:\n  '@fixture/a@${headVersion}': {}\n`,
			);
			if (source) {
				head.set("packages/a/src/index.ts", "export const a = 2;\n");
			}
			headExtra?.(head);
		},
	});
	return { repository, actor };
};

test("accepts only a structurally generated peerbit-org Version Packages PR", async () => {
	const { repository } = versionFixture();
	const result = await check(repository, { versionPullRequestIdentity: true });
	assert.equal(result.kind, "generated-version-pr");
	assert.deepEqual(result.packages, ["@fixture/a"]);
	assert.deepEqual(result.changesets, [".changeset/release.md"]);
});

test("accepts a generated first package changelog", async () => {
	const firstChangelog = versionFixture({ existingChangelog: false });
	assert.equal(
		(
			await check(firstChangelog.repository, {
				versionPullRequestIdentity: true,
			})
		).kind,
		"generated-version-pr",
	);
	for (const [changelog, pattern] of [
		[
			"# Changelog\n\n## 1.0.1\n\nFix.\n\n## Injected release notes\n\nHidden.\n",
			/exactly one new top-level/,
		],
		[
			"# Changelog\n\n## 1.0.1\n\nFix.\n\n# Injected title\n",
			/exactly one top-level H1/,
		],
		[
			"# Changelog\n\n## 1.0.1\n\nFix.\r# Bare CR title\n",
			/exactly one top-level H1/,
		],
		[
			"# Changelog\n\n## 1.0.1\n\nFix.\r## Bare CR section\n",
			/exactly one new top-level/,
		],
		["# Changelog\n\n## 1.0.1\n\nInjected section\n---\n", /Setext H1\/H2/],
		[
			"# Changelog\n\n## 1.0.1\n\n```foo`bar\n# Injected title\n```\n",
			/exactly one top-level H1/,
		],
		["# Changelog\n\n## 1.0.1\n\n<details>\nHidden.\n", /raw HTML/],
	]) {
		const malformed = versionFixture({
			existingChangelog: false,
			changelog,
		});
		await expectGuardFailure(
			check(malformed.repository, { versionPullRequestIdentity: true }),
			pattern,
		);
	}
	const fencedHeadings = versionFixture({
		existingChangelog: false,
		changelog:
			"# Changelog\n\n## 1.0.1\n\n```text\n# Example H1\n## Example H2\n```\n",
	});
	assert.equal(
		(
			await check(fencedHeadings.repository, {
				versionPullRequestIdentity: true,
			})
		).kind,
		"generated-version-pr",
	);
	const tildeFenceWithBacktickInfo = versionFixture({
		existingChangelog: false,
		changelog:
			"# Changelog\n\n## 1.0.1\n\n~~~foo`bar\n# Example H1\n## Example H2\n~~~\n",
	});
	assert.equal(
		(
			await check(tildeFenceWithBacktickInfo.repository, {
				versionPullRequestIdentity: true,
			})
		).kind,
		"generated-version-pr",
	);
	const changesetDeletionIndex = firstChangelog.repository.entries.findIndex(
		(entry) => entry.code === "D" && entry.oldPath === ".changeset/release.md",
	);
	const changelogAdditionIndex = firstChangelog.repository.entries.findIndex(
		(entry) =>
			entry.code === "A" && entry.newPath === "packages/a/CHANGELOG.md",
	);
	firstChangelog.repository.entries.splice(
		Math.max(changesetDeletionIndex, changelogAdditionIndex),
		1,
	);
	firstChangelog.repository.entries.splice(
		Math.min(changesetDeletionIndex, changelogAdditionIndex),
		1,
		{
			status: "R065",
			code: "R",
			oldPath: ".changeset/release.md",
			newPath: "packages/a/CHANGELOG.md",
		},
	);
	assert.equal(
		(
			await check(firstChangelog.repository, {
				versionPullRequestIdentity: true,
			})
		).kind,
		"generated-version-pr",
	);
});

test("the repository root remains private workspace policy, never a package", async () => {
	const publicRoot = fixture({
		mutate: (head) => {
			const value = JSON.parse(head.get("package.json"));
			value.private = false;
			head.set("package.json", JSON.stringify(value, null, 2));
			head.set(".changeset/root.md", changeset({ "fixture-root": "major" }));
		},
	});
	await expectGuardFailure(
		check(publicRoot),
		/workspace policy only.*must remain private/,
	);
});

test("package private policy is boolean or absent", async () => {
	for (const invalidPrivate of [null, "false", 1, {}]) {
		const malformed = fixture({
			mutate: (head) => {
				const value = JSON.parse(head.get("packages/a/package.json"));
				value.private = invalidPrivate;
				head.set("packages/a/package.json", JSON.stringify(value, null, 2));
			},
		});
		await expectGuardFailure(check(malformed), /private must be a boolean/);
	}
});

test("recognizes the generated Version Packages identity only for the exact same-repo bot PR", () => {
	const payload = (properties = {}) => ({
		sender: {
			login: properties.sender ?? "peerbit-org",
			id: properties.senderId ?? 273107789,
		},
		repository: {
			full_name: properties.eventRepository ?? "dao-xyz/peerbit",
		},
		pull_request: {
			user: {
				login: properties.login ?? "peerbit-org",
				id: properties.authorId ?? 273107789,
			},
			base: {
				sha: BASE_SHA,
				ref: properties.baseRef ?? "master",
				repo: {
					full_name: properties.baseRepository ?? "dao-xyz/peerbit",
				},
			},
			head: {
				sha: HEAD_SHA,
				ref: properties.ref ?? "changeset-release/master",
				repo: {
					full_name: properties.headRepository ?? "dao-xyz/peerbit",
				},
			},
		},
	});
	assert.equal(
		validatePullRequestPayload(payload()).versionPullRequestIdentity,
		true,
	);
	assert.equal(
		validatePullRequestPayload(payload({ login: "github-actions[bot]" }))
			.versionPullRequestIdentity,
		false,
	);
	assert.equal(
		validatePullRequestPayload(payload({ ref: "lookalike-version-branch" }))
			.versionPullRequestIdentity,
		false,
	);
	assert.equal(
		validatePullRequestPayload(payload({ headRepository: "attacker/peerbit" }))
			.versionPullRequestIdentity,
		false,
	);
	assert.equal(
		validatePullRequestPayload(payload({ sender: "github-actions[bot]" }))
			.versionPullRequestIdentity,
		false,
	);
	assert.equal(
		validatePullRequestPayload(payload({ authorId: 1 }))
			.versionPullRequestIdentity,
		false,
	);
	assert.equal(
		validatePullRequestPayload(payload({ senderId: 1 }))
			.versionPullRequestIdentity,
		false,
	);
	assert.throws(
		() => validatePullRequestPayload(payload({ baseRef: "release" })),
		/targeting this repository's master branch/,
	);
	assert.throws(
		() =>
			validatePullRequestPayload(
				payload({ eventRepository: "attacker/peerbit" }),
			),
		/targeting this repository's master branch/,
	);
	assert.throws(
		() =>
			validatePullRequestPayload(
				payload({ baseRepository: "attacker/peerbit" }),
			),
		/targeting this repository's master branch/,
	);
});

test("rejects fake/manual and source-containing Version Packages PRs", async () => {
	const fake = versionFixture();
	await expectGuardFailure(
		check(fake.repository),
		/only be changed by the generated/,
	);

	const withSource = versionFixture({ source: true });
	await expectGuardFailure(
		check(withSource.repository, { versionPullRequestIdentity: true }),
		/non-generated change M packages\/a\/src\/index.ts/,
	);

	const withManifestEdit = versionFixture({
		manifestExtra: (value) => {
			value.exports = { ".": "./dist/index.js" };
		},
	});
	await expectGuardFailure(
		check(withManifestEdit.repository, { versionPullRequestIdentity: true }),
		/by more than its version/,
	);

	const mismatchedChangelog = versionFixture({
		changelog: "# Changelog\n\n## 9.9.9\n\nWrong.\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(mismatchedChangelog.repository, {
			versionPullRequestIdentity: true,
		}),
		/exactly one new top-level 1\.0\.1 version section/,
	);
});

test("Version Packages PR follows the exact changeset version plan", async () => {
	const wrongDirectBump = versionFixture({
		releaseBump: "minor",
		headVersion: "1.0.1",
	});
	await expectGuardFailure(
		check(wrongDirectBump.repository, { versionPullRequestIdentity: true }),
		/must bump exactly minor.*1\.1\.0/,
	);

	const missingConsumption = versionFixture({
		baseExtra: (base) => {
			base.set(".changeset/second.md", changeset({ "@fixture/b": "patch" }));
		},
	});
	await expectGuardFailure(
		check(missingConsumption.repository, { versionPullRequestIdentity: true }),
		/exact pending base changeset set.*second\.md/,
	);

	const arbitraryExtra = versionFixture({
		headExtra: (head) => {
			const value = JSON.parse(head.get("packages/b/package.json"));
			value.version = "1.0.1";
			head.set("packages/b/package.json", JSON.stringify(value, null, 2));
			head.set(
				"packages/b/CHANGELOG.md",
				"# Changelog\n\n## 1.0.1\n\n- Unplanned.\n",
			);
		},
	});
	await expectGuardFailure(
		check(arbitraryExtra.repository, { versionPullRequestIdentity: true }),
		/absent from plan: @fixture\/b/,
	);

	const validCascade = versionFixture({
		baseExtra: (base) => {
			const value = JSON.parse(base.get("packages/b/package.json"));
			value.dependencies = { "@fixture/a": "workspace:*" };
			base.set("packages/b/package.json", JSON.stringify(value, null, 2));
		},
		headExtra: (head) => {
			const value = JSON.parse(head.get("packages/b/package.json"));
			value.version = "1.0.1";
			head.set("packages/b/package.json", JSON.stringify(value, null, 2));
			head.set(
				"packages/b/CHANGELOG.md",
				"# Changelog\n\n## 1.0.1\n\n- Updated dependencies.\n",
			);
		},
	});
	assert.deepEqual(
		(
			await check(validCascade.repository, {
				versionPullRequestIdentity: true,
			})
		).packages,
		["@fixture/a", "@fixture/b"],
	);

	const missingCascade = versionFixture({
		baseExtra: (base) => {
			const value = JSON.parse(base.get("packages/b/package.json"));
			value.dependencies = { "@fixture/a": "workspace:*" };
			base.set("packages/b/package.json", JSON.stringify(value, null, 2));
		},
	});
	await expectGuardFailure(
		check(missingCascade.repository, { versionPullRequestIdentity: true }),
		/missing: @fixture\/b/,
	);

	const validPeerCascade = versionFixture({
		releaseBump: "minor",
		headVersion: "1.1.0",
		baseExtra: (base) => {
			const value = JSON.parse(base.get("packages/b/package.json"));
			value.peerDependencies = { "@fixture/a": "workspace:^" };
			base.set("packages/b/package.json", JSON.stringify(value, null, 2));
		},
		headExtra: (head) => {
			const value = JSON.parse(head.get("packages/b/package.json"));
			value.version = "2.0.0";
			head.set("packages/b/package.json", JSON.stringify(value, null, 2));
			head.set(
				"packages/b/CHANGELOG.md",
				"# Changelog\n\n## 2.0.0\n\n- Updated peer dependency.\n",
			);
		},
	});
	assert.deepEqual(
		(
			await check(validPeerCascade.repository, {
				versionPullRequestIdentity: true,
			})
		).packages,
		["@fixture/a", "@fixture/b"],
	);
});

test("Version Packages PR rejects arbitrary lockfile and changelog rewrites", async () => {
	const badLockfile = versionFixture({
		lockfileHead: "lockfileVersion: '9.0'\nattacker: true\n",
	});
	await expectGuardFailure(
		check(badLockfile.repository, { versionPullRequestIdentity: true }),
		/pnpm-lock\.yaml change is not an exact/,
	);

	const omittedLockProjection = versionFixture({
		lockfileHead:
			"lockfileVersion: '9.0'\n\nsnapshots:\n  '@fixture/a@1.0.0': {}\n",
	});
	await expectGuardFailure(
		check(omittedLockProjection.repository, {
			versionPullRequestIdentity: true,
		}),
		/pnpm-lock\.yaml change is not an exact/,
	);

	const rewrittenHistory = versionFixture({
		changelog: "# Changelog\n\n## 1.0.1\n\n- Replacement history.\n",
	});
	await expectGuardFailure(
		check(rewrittenHistory.repository, { versionPullRequestIdentity: true }),
		/rewrites or drops existing release history/,
	);

	const commentedCopyAndRewrite = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\n<!--\n## 1.0.0\n\nOld.\n-->\n\n## 1.0.0\n\nRewritten.\n",
	});
	await expectGuardFailure(
		check(commentedCopyAndRewrite.repository, {
			versionPullRequestIdentity: true,
		}),
		/terminal structural tail/,
	);

	const duplicatedCommentedHistory = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\n<!--\n## 1.0.0\n\nOld.\n-->\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(duplicatedCommentedHistory.repository, {
			versionPullRequestIdentity: true,
		}),
		/(?:copies or duplicates|comment out)/,
	);

	const multipleNewSections = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\nFix.\n\n## 9.9.9\n\nInjected.\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(multipleNewSections.repository, {
			versionPullRequestIdentity: true,
		}),
		/exactly one new top-level/,
	);

	const multiwordNewSection = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\nFix.\n\n## Injected release notes\n\nHidden.\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(multiwordNewSection.repository, {
			versionPullRequestIdentity: true,
		}),
		/exactly one new top-level/,
	);

	const extraTitle = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\nFix.\n\n# Injected title\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(extraTitle.repository, { versionPullRequestIdentity: true }),
		/exactly one top-level H1/,
	);

	const setextSection = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\nInjected section\n---\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(setextSection.repository, { versionPullRequestIdentity: true }),
		/Setext H1\/H2/,
	);

	const bareCrTitle = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\nFix.\r# Bare CR title\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(bareCrTitle.repository, { versionPullRequestIdentity: true }),
		/exactly one top-level H1/,
	);

	const bareCrSection = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\nFix.\r## Bare CR section\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(bareCrSection.repository, { versionPullRequestIdentity: true }),
		/exactly one new top-level/,
	);

	const invalidBacktickFenceInfo = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\n```foo`bar\n# Injected title\n```\n\n## 1.0.0\n\nOld.\n",
	});
	await expectGuardFailure(
		check(invalidBacktickFenceInfo.repository, {
			versionPullRequestIdentity: true,
		}),
		/exactly one top-level H1/,
	);

	for (const rawTag of ["<details>", "<pre"]) {
		const rawHtmlWrapper = versionFixture({
			changelog: `# Changelog\n\n## 1.0.1\n\n${rawTag}\n\n## 1.0.0\n\nOld.\n`,
		});
		await expectGuardFailure(
			check(rawHtmlWrapper.repository, {
				versionPullRequestIdentity: true,
			}),
			/raw HTML/,
		);
	}

	for (const fence of ["```", "~~~"]) {
		const fencedHistory = versionFixture({
			changelog: `# Changelog\n\n## 1.0.1\n\n${fence}\n\n## 1.0.0\n\nOld.\n`,
		});
		await expectGuardFailure(
			check(fencedHistory.repository, {
				versionPullRequestIdentity: true,
			}),
			/unclosed Markdown fence/,
		);
	}

	const balancedFence = versionFixture({
		changelog:
			"# Changelog\n\n## 1.0.1\n\n```text\n# Example H1\n## Example H2\n```\n\n## 1.0.0\n\nOld.\n",
	});
	assert.deepEqual(
		(
			await check(balancedFence.repository, {
				versionPullRequestIdentity: true,
			})
		).packages,
		["@fixture/a"],
	);

	for (const [oldChangelog, headChangelog, pattern] of [
		[
			"# Changelog\n\n## 1.0.0\n\nOld.\n\n# Legacy extra title\n",
			"# Changelog\n\n## 1.0.1\n\nFix.\n\n## 1.0.0\n\nOld.\n\n# Legacy extra title\n",
			/exactly one top-level H1/,
		],
		[
			"# Changelog\n\n## 1.0.0\n\nLegacy section\n---\n",
			"# Changelog\n\n## 1.0.1\n\nFix.\n\n## 1.0.0\n\nLegacy section\n---\n",
			/Setext H1\/H2/,
		],
	]) {
		const malformedExistingStructure = versionFixture({
			baseExtra: (base) => base.set("packages/a/CHANGELOG.md", oldChangelog),
			changelog: headChangelog,
		});
		await expectGuardFailure(
			check(malformedExistingStructure.repository, {
				versionPullRequestIdentity: true,
			}),
			pattern,
		);
	}

	const oldFencedHeadings = versionFixture({
		baseExtra: (base) =>
			base.set(
				"packages/a/CHANGELOG.md",
				"# Changelog\n\n## 1.0.0\n\n```text\n# Legacy H1 example\n## Legacy H2 example\n```\n",
			),
		changelog:
			"# Changelog\n\n## 1.0.1\n\nFix.\n\n## 1.0.0\n\n```text\n# Legacy H1 example\n## Legacy H2 example\n```\n",
	});
	assert.deepEqual(
		(
			await check(oldFencedHeadings.repository, {
				versionPullRequestIdentity: true,
			})
		).packages,
		["@fixture/a"],
	);
});

test("Version Packages PR must descend from the exact event master base", async () => {
	const stale = versionFixture();
	stale.repository.trees.set(OTHER_SHA, stale.repository.trees.get(BASE_SHA));
	stale.repository.mergeBaseSha = OTHER_SHA;
	await expectGuardFailure(
		check(stale.repository, { versionPullRequestIdentity: true }),
		/exact event base SHA/,
	);
});

test("ordinary pull requests must also descend from the exact event base", async () => {
	const stale = fixture({ mergeBase: OTHER_SHA });
	stale.trees.set(OTHER_SHA, stale.trees.get(BASE_SHA));
	await expectGuardFailure(check(stale), /exact event base SHA/);
});

test("fails closed on malformed full SHAs, merge bases, and frontmatter", async () => {
	await expectGuardFailure(
		checkChangesetRequired({
			baseSha: "abc123",
			headSha: HEAD_SHA,
			repository: fixture(),
		}),
		/full lowercase Git object id/,
	);
	await expectGuardFailure(
		check(fixture({ mergeBase: "deadbeef" })),
		/merge base must be a full lowercase Git object id/,
	);
	assert.throws(
		() => parseChangeset('---\n"@fixture/a" patch\n---\n\nSummary.\n'),
		/flat package: bump entry/,
	);
});

test("the executable guard skips non-PR events before touching Git", async () => {
	let touched = false;
	const result = await runChangesetGuardFromEnvironment({
		environment: { GITHUB_EVENT_NAME: "push" },
		repository: new Proxy(
			{},
			{
				get() {
					touched = true;
					throw new Error("repository must not be touched");
				},
			},
		),
		log: () => {},
	});
	assert.equal(result.kind, "skipped");
	assert.equal(touched, false);
});

test("the executable guard accepts the base-owned pull_request_target event", async () => {
	const directory = await mkdtemp(join(tmpdir(), "peerbit-target-guard-"));
	const eventPath = join(directory, "event.json");
	try {
		await writeFile(
			eventPath,
			JSON.stringify({
				sender: { login: "contributor", id: 1 },
				repository: { full_name: "dao-xyz/peerbit" },
				pull_request: {
					user: { login: "contributor", id: 1 },
					base: {
						sha: BASE_SHA,
						ref: "master",
						repo: { full_name: "dao-xyz/peerbit" },
					},
					head: {
						sha: HEAD_SHA,
						ref: "feature",
						repo: { full_name: "fork/peerbit" },
					},
				},
			}),
			"utf8",
		);
		const result = await runChangesetGuardFromEnvironment({
			environment: {
				GITHUB_EVENT_NAME: "pull_request_target",
				GITHUB_EVENT_PATH: eventPath,
			},
			repository: fixture(),
			log: () => {},
		});
		assert.equal(result.kind, "ordinary-pr");
		assert.deepEqual(result.affected, []);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
