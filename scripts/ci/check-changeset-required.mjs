#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

const SHA_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const CHANGESET_GUARD_PATHS = [
	"scripts/ci/check-changeset-required.mjs",
	"scripts/ci/check-changeset-required.test.mjs",
];
const CHANGESET_GUARD_WORKFLOW_PATH = ".github/workflows/changeset-guard.yml";
const CHANGESET_CHANGELOG_POLICY = [
	"@changesets/changelog-github",
	{ repo: "dao-xyz/peerbit" },
];
const CHANGESET_CONFIG_SCHEMA =
	"https://unpkg.com/@changesets/config@3.1.4/schema.json";
const CHANGESET_CONFIG_KEYS = [
	"$schema",
	"access",
	"baseBranch",
	"changelog",
	"commit",
	"fixed",
	"ignore",
	"linked",
	"updateInternalDependencies",
].sort();
const VERSION_BUMPS = new Set(["patch", "minor", "major"]);
const VERSION_BUMP_PRIORITY = new Map([
	["patch", 0],
	["minor", 1],
	["major", 2],
]);
const FROZEN_IGNORED_PACKAGE_NAMES = new Set(["@peerbit/test-lib"]);
const RUNTIME_DEPENDENCY_FIELDS = [
	"dependencies",
	"optionalDependencies",
	"peerDependencies",
];
const ALL_INTERNAL_DEPENDENCY_FIELDS = [
	...RUNTIME_DEPENDENCY_FIELDS,
	"devDependencies",
];
const SUPPORTED_INTERNAL_WORKSPACE_RANGES = new Set([
	"workspace:*",
	"workspace:^",
	"workspace:~",
]);
const RELEASE_ARTIFACT_OWNERS = new Map([
	["packages/clients/peerbit-server/frontend", "@peerbit/server"],
]);
const PACKAGE_NAME_PATTERN =
	/^(?:@[a-z0-9][a-z0-9._~-]*\/)?[a-z0-9][a-z0-9._~-]*$/;
const STABLE_VERSION_PATTERN = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const RUNTIME_FILE_PATTERN =
	/\.(?:cjs|css|cts|d\.ts|html|js|json|jsx|mjs|mts|node|rs|scss|ts|tsx|wasm)$/i;
const TOOL_ONLY_MANIFEST_KEYS = new Set([
	"devDependencies",
	"eslintConfig",
	"packageManager",
	"prettier",
	"volta",
]);
const TOOL_CONFIG_BASENAMES = [
	/^\.editorconfig$/,
	/^\.eslintrc(?:\..+)?$/,
	/^\.node-version$/,
	/^\.prettierignore$/,
	/^\.prettierrc(?:\..+)?$/,
	/^aegir(?:\.config)?\.[cm]?[jt]s$/,
	/^babel\.config\.[cm]?[jt]s$/,
	/^biome\.jsonc?$/,
	/^deno\.jsonc?$/,
	/^eslint\.config\.[cm]?[jt]s$/,
	/^playwright\.config\.[cm]?[jt]s$/,
	/^rollup\.config\.[cm]?[jt]s$/,
	/^tsconfig(?:\.[^/]+)?\.json$/,
	/^typedoc\.json$/,
	/^vite\.config\.[cm]?[jt]s$/,
	/^vitest\.config\.[cm]?[jt]s$/,
	/^webpack\.config\.[cm]?[jt]s$/,
];

export class ChangesetGuardError extends Error {
	constructor(message) {
		super(message);
		this.name = "ChangesetGuardError";
	}
}

const fail = (message) => {
	throw new ChangesetGuardError(message);
};

const normalizeLineEndings = (source) => source.replace(/\r\n?/g, "\n");

export const validateFullSha = (sha, label) => {
	if (typeof sha !== "string" || !SHA_PATTERN.test(sha)) {
		fail(
			`${label} must be a full lowercase Git object id (got ${JSON.stringify(sha)})`,
		);
	}
	return sha;
};

const validateRepositoryPath = (path, label = "Git path") => {
	if (
		typeof path !== "string" ||
		path.length === 0 ||
		path.startsWith("/") ||
		path.includes("\0") ||
		path.split("/").some((segment) => segment === "." || segment === "..")
	) {
		fail(
			`${label} is not a normalized repository-relative path: ${JSON.stringify(path)}`,
		);
	}
	return path;
};

/** Parse `git diff --name-status -z`, including rename/copy pairs. */
export const parseNameStatusZ = (raw) => {
	if (Buffer.isBuffer(raw)) {
		raw = raw.toString("utf8");
	}
	if (typeof raw !== "string") {
		fail("Git name-status output must be a string or Buffer");
	}
	if (raw.length === 0) {
		return [];
	}
	if (!raw.endsWith("\0")) {
		fail("Git name-status output is not NUL terminated");
	}
	const fields = raw.slice(0, -1).split("\0");
	const entries = [];
	for (let index = 0; index < fields.length; ) {
		const status = fields[index++];
		if (!/^[A-Z][0-9]*$/.test(status)) {
			fail(`Unexpected Git name-status token ${JSON.stringify(status)}`);
		}
		const code = status[0];
		if (code === "R" || code === "C") {
			if (index + 1 >= fields.length) {
				fail(`Git ${code === "R" ? "rename" : "copy"} record is truncated`);
			}
			const oldPath = validateRepositoryPath(fields[index++]);
			const newPath = validateRepositoryPath(fields[index++]);
			entries.push({ status, code, oldPath, newPath });
			continue;
		}
		if (index >= fields.length) {
			fail(`Git ${status} record is truncated`);
		}
		const path = validateRepositoryPath(fields[index++]);
		entries.push({
			status,
			code,
			oldPath: code === "A" ? undefined : path,
			newPath: code === "D" ? undefined : path,
		});
	}
	return entries;
};

/** Parse `git ls-tree -r -z` and reject non-regular tracked entries. */
export const parseLsTreeZ = (raw, label = "Git tree") => {
	if (Buffer.isBuffer(raw)) {
		raw = raw.toString("utf8");
	}
	if (typeof raw !== "string") {
		fail(`${label} output must be a string or Buffer`);
	}
	if (raw.length === 0) {
		return [];
	}
	if (!raw.endsWith("\0")) {
		fail(`${label} output was not NUL terminated`);
	}
	const entries = [];
	const seenPaths = new Set();
	for (const record of raw.slice(0, -1).split("\0")) {
		const separator = record.indexOf("\t");
		if (separator < 0) {
			fail(`${label} contains a malformed tree entry`);
		}
		const header = record.slice(0, separator);
		const path = validateRepositoryPath(record.slice(separator + 1));
		const match = header.match(
			/^(\d{6}) ([a-z]+) ([0-9a-f]{40}|[0-9a-f]{64})$/,
		);
		if (!match) {
			fail(`${label} contains malformed metadata for ${JSON.stringify(path)}`);
		}
		const [, mode, type, objectId] = match;
		if ((mode !== "100644" && mode !== "100755") || type !== "blob") {
			fail(
				`${label} contains unsupported tracked entry ${path} (${mode} ${type}); only regular 100644/100755 blobs are allowed`,
			);
		}
		if (seenPaths.has(path)) {
			fail(`${label} repeats tracked path ${JSON.stringify(path)}`);
		}
		seenPaths.add(path);
		entries.push({ mode, type, objectId, path });
	}
	return entries;
};

const runGit = (repositoryRoot, args, options = {}) => {
	const result = spawnSync("git", args, {
		cwd: repositoryRoot,
		encoding: options.buffer ? undefined : "utf8",
		maxBuffer: 64 * 1024 * 1024,
		stdio: ["ignore", "pipe", "pipe"],
	});
	if (result.error) {
		fail(`Unable to run git ${args[0]}: ${result.error.message}`);
	}
	if (result.status !== 0) {
		const stderr = Buffer.isBuffer(result.stderr)
			? result.stderr.toString("utf8")
			: result.stderr;
		fail(
			`git ${args[0]} failed (${result.status}): ${(stderr || "no diagnostic").trim()}`,
		);
	}
	return result.stdout;
};

export class GitTreeRepository {
	constructor(repositoryRoot) {
		this.repositoryRoot = repositoryRoot;
	}

	assertCommit(sha) {
		runGit(this.repositoryRoot, ["cat-file", "-e", `${sha}^{commit}`]);
	}

	mergeBase(baseSha, headSha) {
		return runGit(this.repositoryRoot, ["merge-base", baseSha, headSha]).trim();
	}

	diffNameStatus(baseSha, headSha) {
		return parseNameStatusZ(
			runGit(
				this.repositoryRoot,
				[
					"diff",
					"--name-status",
					"-z",
					"--find-renames",
					baseSha,
					headSha,
					"--",
				],
				{ buffer: true },
			),
		);
	}

	listFiles(sha) {
		const raw = runGit(this.repositoryRoot, ["ls-tree", "-r", "-z", sha], {
			buffer: true,
		});
		return parseLsTreeZ(raw, `git ls-tree for ${sha}`).map(
			(entry) => entry.path,
		);
	}

	readFile(sha, path) {
		validateRepositoryPath(path);
		return runGit(this.repositoryRoot, ["show", `${sha}:${path}`]);
	}
}

const parseJson = (source, label) => {
	try {
		const value = JSON.parse(source);
		if (!value || typeof value !== "object" || Array.isArray(value)) {
			fail(`${label} must contain a JSON object`);
		}
		return value;
	} catch (error) {
		if (error instanceof ChangesetGuardError) {
			throw error;
		}
		fail(`${label} is not valid JSON: ${error.message}`);
	}
};

const assertStringArray = (value, label) => {
	if (
		!Array.isArray(value) ||
		value.some((entry) => typeof entry !== "string")
	) {
		fail(`${label} must be a flat string array`);
	}
	return value;
};

const parseWorkspacePattern = (pattern, label) => {
	if (
		typeof pattern !== "string" ||
		pattern.length === 0 ||
		pattern.startsWith("!") ||
		pattern.startsWith("/") ||
		pattern.endsWith("/") ||
		pattern.includes("\\") ||
		pattern
			.split("/")
			.some(
				(segment) =>
					segment === "" ||
					segment === "." ||
					segment === ".." ||
					(segment.includes("*") && segment !== "*") ||
					/[?[\]{}]/.test(segment),
			)
	) {
		fail(
			`${label} contains unsupported workspace pattern ${JSON.stringify(pattern)}; only normalized paths and whole-segment * wildcards are allowed`,
		);
	}
	return pattern;
};

const parsePnpmWorkspacePatterns = (source, label) => {
	if (typeof source !== "string") {
		fail(`${label} must be UTF-8 text`);
	}
	if (source.includes("\t")) {
		fail(`${label} must not contain tabs`);
	}
	const patterns = [];
	let sawPackages = false;
	for (const [index, line] of normalizeLineEndings(source)
		.split("\n")
		.entries()) {
		if (line.trim() === "") {
			continue;
		}
		if (!sawPackages) {
			if (line !== "packages:") {
				fail(`${label}:${index + 1} must start with packages:`);
			}
			sawPackages = true;
			continue;
		}
		if (line.includes("#")) {
			fail(`${label}:${index + 1} must not contain comments`);
		}
		const match = line.match(/^ {2}- ([^\s][^\r\n]*)$/);
		if (!match) {
			fail(
				`${label}:${index + 1} must be a two-space-indented workspace entry`,
			);
		}
		let pattern = match[1].trimEnd();
		if (pattern.startsWith('"')) {
			try {
				pattern = JSON.parse(pattern);
			} catch {
				fail(`${label}:${index + 1} has an invalid quoted workspace entry`);
			}
		} else if (pattern.startsWith("'")) {
			if (!pattern.endsWith("'") || pattern.length < 2) {
				fail(`${label}:${index + 1} has an invalid quoted workspace entry`);
			}
			pattern = pattern.slice(1, -1).replaceAll("''", "'");
		}
		patterns.push(parseWorkspacePattern(pattern, `${label}:${index + 1}`));
	}
	if (!sawPackages || patterns.length === 0) {
		fail(`${label} has no workspace package entries`);
	}
	return patterns;
};

const sortedUnique = (values) => [...new Set(values)].sort();

const readWorkspacePolicy = async (repository, sha, fileSet) => {
	for (const path of ["package.json", "pnpm-workspace.yaml"]) {
		if (!fileSet.has(path)) {
			fail(`${path} is missing from ${sha}`);
		}
	}
	const rootManifest = parseJson(
		await repository.readFile(sha, "package.json"),
		`${sha}:package.json`,
	);
	const npmPatterns = sortedUnique(
		assertStringArray(
			rootManifest.workspaces,
			`${sha}:package.json workspaces`,
		).map((pattern) =>
			parseWorkspacePattern(pattern, `${sha}:package.json workspaces`),
		),
	);
	const pnpmPatterns = sortedUnique(
		parsePnpmWorkspacePatterns(
			await repository.readFile(sha, "pnpm-workspace.yaml"),
			`${sha}:pnpm-workspace.yaml`,
		),
	);
	if (!isDeepStrictEqual(npmPatterns, pnpmPatterns)) {
		fail(
			`${sha} workspace policy drift: package.json and pnpm-workspace.yaml must name the same roots`,
		);
	}
	return npmPatterns;
};

const parseIgnoreConfig = async (repository, sha, fileSet) => {
	const path = ".changeset/config.json";
	if (!fileSet.has(path)) {
		fail(`${path} is missing from ${sha}`);
	}
	const config = parseJson(
		await repository.readFile(sha, path),
		`${sha}:${path}`,
	);
	if (
		!isDeepStrictEqual(Object.keys(config).sort(), CHANGESET_CONFIG_KEYS) ||
		config.$schema !== CHANGESET_CONFIG_SCHEMA
	) {
		fail(
			`${sha}:${path} must contain exactly the pinned ${CHANGESET_CONFIG_SCHEMA} key set; unknown, missing, or future policy keys are unsupported`,
		);
	}
	if (
		!Array.isArray(config.ignore) ||
		config.ignore.some((name) => typeof name !== "string")
	) {
		fail(`${sha}:${path} must contain a flat string ignore array`);
	}
	for (const name of config.ignore) {
		if (!PACKAGE_NAME_PATTERN.test(name)) {
			fail(
				`${sha}:${path} ignore entry ${JSON.stringify(name)} must be an exact package name, not a wildcard or path`,
			);
		}
	}
	const ignoredNames = new Set(config.ignore);
	if (ignoredNames.size !== config.ignore.length) {
		fail(`${sha}:${path} must not repeat ignore entries`);
	}
	for (const name of FROZEN_IGNORED_PACKAGE_NAMES) {
		if (!ignoredNames.has(name)) {
			fail(`${sha}:${path} must keep ${name} ignored/frozen`);
		}
	}
	if (
		!isDeepStrictEqual(config.changelog, CHANGESET_CHANGELOG_POLICY) ||
		config.commit !== false ||
		config.access !== "public" ||
		config.baseBranch !== "master" ||
		config.updateInternalDependencies !== "patch" ||
		!Array.isArray(config.fixed) ||
		config.fixed.length !== 0 ||
		!Array.isArray(config.linked) ||
		config.linked.length !== 0
	) {
		fail(
			`${sha}:${path} must keep the supported release-plan policy: the exact @changesets/changelog-github dao-xyz/peerbit tuple, commit=false, access=public, baseBranch=master, updateInternalDependencies=patch, empty fixed/linked groups, and no private/experimental versioning`,
		);
	}
	return { config, ignoredNames };
};

const packageRootFromManifestPath = (path) =>
	path === "package.json" ? "" : dirname(path).replaceAll("\\", "/");

const isPackageManifestPath = (path) =>
	typeof path === "string" &&
	(path === "package.json" || path.endsWith("/package.json"));

const isPackageChangelogPath = (path) =>
	typeof path === "string" &&
	(path === "CHANGELOG.md" || path.endsWith("/CHANGELOG.md"));

const workspacePatternMatchesRoot = (pattern, root) => {
	const patternSegments = pattern.split("/");
	const rootSegments = root.split("/");
	return (
		patternSegments.length === rootSegments.length &&
		patternSegments.every(
			(segment, index) => segment === "*" || segment === rootSegments[index],
		)
	);
};

const isAuthoritativePackageRoot = (root, workspacePatterns) =>
	root === "" ||
	workspacePatterns.some((pattern) =>
		workspacePatternMatchesRoot(pattern, root),
	);

const discoverPackages = async (
	repository,
	sha,
	fileSet,
	workspacePatterns,
) => {
	const packages = new Map();
	const packageNames = new Map();
	for (const path of [...fileSet].sort()) {
		if (!isPackageManifestPath(path)) {
			continue;
		}
		const root = packageRootFromManifestPath(path);
		if (!isAuthoritativePackageRoot(root, workspacePatterns)) {
			continue;
		}
		const manifest = parseJson(
			await repository.readFile(sha, path),
			`${sha}:${path}`,
		);
		if (packages.has(root)) {
			fail(`Multiple package manifests resolve to ${JSON.stringify(root)}`);
		}
		if (
			typeof manifest.name !== "string" ||
			!PACKAGE_NAME_PATTERN.test(manifest.name)
		) {
			fail(`${sha}:${path} must declare a valid exact package name`);
		}
		const existing = packageNames.get(manifest.name);
		if (existing) {
			fail(
				`${sha} workspace package name ${manifest.name} occurs at both ${existing} and ${path}`,
			);
		}
		packageNames.set(manifest.name, path);
		packages.set(root, { root, path, manifest });
	}
	return packages;
};

const packagesByName = (packages) => {
	const result = new Map();
	for (const candidate of packages.values()) {
		const existing = result.get(candidate.manifest.name);
		if (existing) {
			fail(
				`Package name ${candidate.manifest.name} occurs at both ${existing.path} and ${candidate.path}`,
			);
		}
		result.set(candidate.manifest.name, candidate);
	}
	return result;
};

const findOwningPackage = (packages, path) => {
	let owner;
	for (const candidate of packages.values()) {
		if (
			(candidate.root === "" ||
				path === candidate.root ||
				path.startsWith(`${candidate.root}/`)) &&
			(!owner || candidate.root.length > owner.root.length)
		) {
			owner = candidate;
		}
	}
	return owner;
};

const isPublicManifest = (manifest) =>
	manifest?.private !== true &&
	typeof manifest?.name === "string" &&
	manifest.name.length > 0;

const hasChangesetsVersion = (manifest) => Boolean(manifest?.version);

const isVersionableManifest = (manifest, ignoredNames) =>
	isPublicManifest(manifest) &&
	hasChangesetsVersion(manifest) &&
	!ignoredNames.has(manifest.name);

const validateIgnoredPackagePolicy = (sha, ignoredNames, packages) => {
	const byName = packagesByName(packages);
	for (const name of ignoredNames) {
		const candidate = byName.get(name);
		if (!candidate) {
			fail(
				`${sha} Changesets ignore entry ${name} does not name an authoritative workspace package`,
			);
		}
		if (FROZEN_IGNORED_PACKAGE_NAMES.has(name)) {
			if (!isPublicManifest(candidate.manifest)) {
				fail(`${sha}:${candidate.path} must keep ${name} public and frozen`);
			}
		} else if (candidate.manifest.private !== true) {
			fail(
				`${sha}:${candidate.path} package ${name} is ignored by Changesets and must remain private`,
			);
		}
	}
};

const validateRootPackagePolicy = (sha, packages) => {
	const rootPackage = packages.get("");
	if (!rootPackage || rootPackage.manifest.private !== true) {
		fail(
			`${sha}:package.json is workspace policy only and must remain private; the repository root is not a Changesets package`,
		);
	}
};

const validateSkippedDependencyGraph = (sha, ignoredNames, packages) => {
	const byName = packagesByName(packages);
	const skippedNames = new Set(
		[...byName].flatMap(([name, candidate]) =>
			ignoredNames.has(name) ||
			candidate.manifest.private === true ||
			!hasChangesetsVersion(candidate.manifest)
				? [name]
				: [],
		),
	);
	for (const candidate of packages.values()) {
		if (
			skippedNames.has(candidate.manifest.name) ||
			!isPublicManifest(candidate.manifest)
		) {
			continue;
		}
		for (const field of RUNTIME_DEPENDENCY_FIELDS) {
			for (const name of Object.keys(candidate.manifest[field] ?? {})) {
				if (skippedNames.has(name)) {
					fail(
						`${sha}:${candidate.path} public package ${candidate.manifest.name} has release-blocking ${field}.${name}; runtime, optional, and peer dependencies may not target a Changesets-skipped workspace package (ignored, private, or missing a version)`,
					);
				}
			}
		}
	}
};

const validateInternalDependencyRanges = (sha, packages) => {
	const internalNames = new Set(packagesByName(packages).keys());
	for (const candidate of packages.values()) {
		for (const field of ALL_INTERNAL_DEPENDENCY_FIELDS) {
			const dependencies = candidate.manifest[field];
			if (dependencies === undefined) {
				continue;
			}
			if (
				!dependencies ||
				typeof dependencies !== "object" ||
				Array.isArray(dependencies)
			) {
				fail(`${sha}:${candidate.path} ${field} must be an object`);
			}
			for (const [name, range] of Object.entries(dependencies)) {
				if (
					internalNames.has(name) &&
					!SUPPORTED_INTERNAL_WORKSPACE_RANGES.has(range)
				) {
					fail(
						`${sha}:${candidate.path} ${field}.${name} must use one of the preserved internal workspace ranges: workspace:*, workspace:^, workspace:~`,
					);
				}
			}
		}
	}
};

const versionableNamesForPair = (pair, ignoredNames) => {
	const names = new Set();
	for (const candidate of [pair.base, pair.head]) {
		if (isVersionableManifest(candidate?.manifest, ignoredNames)) {
			names.add(candidate.manifest.name);
		}
	}
	return names;
};

const pairPackages = (basePackages, headPackages, ignoredNames) => {
	const pairs = new Map();
	for (const root of new Set([
		...basePackages.keys(),
		...headPackages.keys(),
	])) {
		const pair = {
			root,
			base: basePackages.get(root),
			head: headPackages.get(root),
		};
		pair.versionableNames = versionableNamesForPair(pair, ignoredNames);
		pairs.set(root, pair);
	}
	return pairs;
};

const relativeToPackage = (root, path) =>
	root === "" ? path : path.slice(root.length + 1);

const isDocumentationPath = (relativePath) => {
	const segments = relativePath.split("/");
	const basename = segments.at(-1);
	return (
		segments[0] === "docs" ||
		segments[0] === "doc" ||
		/^(?:README|CHANGELOG|CONTRIBUTING|SECURITY|LICENSE)(?:\..*)?$/i.test(
			basename,
		) ||
		/\.mdx?$/i.test(basename)
	);
};

const isTestOrBenchmarkPath = (relativePath) => {
	const segments = relativePath.split("/");
	const basename = segments.at(-1);
	const explicitTestRoot = new Set([
		"__fixtures__",
		"__tests__",
		"bench",
		"benches",
		"benchmark",
		"benchmarks",
		"e2e",
		"example",
		"examples",
		"fixture",
		"fixtures",
		"test",
		"tests",
	]);
	return (
		explicitTestRoot.has(segments[0]) ||
		/(?:^|\.)(?:spec|test)(?:\.[^.]+)+$/i.test(basename)
	);
};

const isToolConfigPath = (relativePath) => {
	const segments = relativePath.split("/");
	const basename = segments.at(-1);
	return (
		segments[0] === ".github" ||
		segments[0] === "tools" ||
		TOOL_CONFIG_BASENAMES.some((pattern) => pattern.test(basename))
	);
};

const normalizeManifestPackagePath = (rawPath, label) => {
	if (typeof rawPath !== "string" || rawPath.length === 0) {
		fail(`${label} must be a non-empty package-relative path`);
	}
	const normalized = rawPath.replace(/^\.\//, "");
	if (
		!normalized ||
		normalized.startsWith("/") ||
		normalized.includes("\\") ||
		normalized.includes("\0") ||
		/[?*[\]{}]/.test(normalized) ||
		normalized
			.split("/")
			.some((segment) => segment === "" || segment === "." || segment === "..")
	) {
		fail(`${label} has unsafe package path ${JSON.stringify(rawPath)}`);
	}
	return normalized;
};

const runtimePublishedPaths = (manifest) => {
	const paths = new Set();
	const add = (rawPath, label) =>
		paths.add(normalizeManifestPackagePath(rawPath, label));
	for (const field of ["main", "module"]) {
		if (manifest[field] !== undefined) {
			add(manifest[field], `${manifest.name ?? "package"} ${field}`);
		}
	}
	if (manifest.browser !== undefined) {
		if (typeof manifest.browser === "string") {
			add(manifest.browser, `${manifest.name ?? "package"} browser`);
		} else if (
			manifest.browser &&
			typeof manifest.browser === "object" &&
			!Array.isArray(manifest.browser)
		) {
			for (const [from, to] of Object.entries(manifest.browser)) {
				add(from, `${manifest.name ?? "package"} browser key`);
				if (to !== false) {
					add(to, `${manifest.name ?? "package"} browser target`);
				}
			}
		} else {
			fail(`${manifest.name ?? "package"} browser must be a string or object`);
		}
	}
	if (manifest.bin !== undefined) {
		if (typeof manifest.bin === "string") {
			add(manifest.bin, `${manifest.name ?? "package"} bin`);
		} else if (
			manifest.bin &&
			typeof manifest.bin === "object" &&
			!Array.isArray(manifest.bin)
		) {
			for (const [command, target] of Object.entries(manifest.bin)) {
				add(target, `${manifest.name ?? "package"} bin.${command}`);
			}
		} else {
			fail(`${manifest.name ?? "package"} bin must be a string or object`);
		}
	}
	const collectExportTargets = (value, label) => {
		if (value === null || value === false) {
			return;
		}
		if (typeof value === "string") {
			add(value, label);
			return;
		}
		if (Array.isArray(value)) {
			for (const [index, target] of value.entries()) {
				collectExportTargets(target, `${label}[${index}]`);
			}
			return;
		}
		if (value && typeof value === "object") {
			for (const [condition, target] of Object.entries(value)) {
				collectExportTargets(target, `${label}.${condition}`);
			}
			return;
		}
		fail(
			`${label} must contain only string, object, array, null, or false targets`,
		);
	};
	if (manifest.exports !== undefined) {
		collectExportTargets(
			manifest.exports,
			`${manifest.name ?? "package"} exports`,
		);
	}
	return paths;
};

const typePublishedPaths = (manifest) => {
	const paths = new Set();
	for (const field of ["types", "typings"]) {
		if (manifest[field] !== undefined) {
			paths.add(
				normalizeManifestPackagePath(
					manifest[field],
					`${manifest.name ?? "package"} ${field}`,
				),
			);
		}
	}
	return paths;
};

const matchesManifestTargetPath = (relativePath, target) =>
	relativePath === target ||
	relativePath.startsWith(`${target}.`) ||
	relativePath.startsWith(`${target}/`);

const typeResolutionTargetAlternatives = (target) => {
	for (const [extension, replacements] of [
		[".mjs", [".mts", ".d.mts", ".mjs"]],
		[".cjs", [".cts", ".d.cts", ".cjs"]],
		[".jsx", [".tsx", ".d.ts", ".jsx"]],
		[".js", [".ts", ".tsx", ".d.ts", ".js", ".jsx"]],
	]) {
		if (target.endsWith(extension)) {
			const stem = target.slice(0, -extension.length);
			return replacements.map((replacement) => `${stem}${replacement}`);
		}
	}
	return [target];
};

const matchesRuntimePublishedPath = (relativePath, manifest) =>
	[...runtimePublishedPaths(manifest)].some((target) =>
		matchesManifestTargetPath(relativePath, target),
	);

const matchesTypePublishedPath = (relativePath, manifest) =>
	[...typePublishedPaths(manifest)].some((target) =>
		typeResolutionTargetAlternatives(target).some((alternative) =>
			matchesManifestTargetPath(relativePath, alternative),
		),
	);

const normalizeTypesVersionsPattern = (rawPattern, label) => {
	if (typeof rawPattern !== "string" || rawPattern.length === 0) {
		fail(`${label} must be a non-empty package-relative pattern`);
	}
	const normalized = rawPattern.replace(/^\.\//, "");
	if (
		!normalized ||
		normalized.startsWith("/") ||
		normalized.includes("\\") ||
		normalized.includes("\0") ||
		/[?[\]{}]/.test(normalized) ||
		(normalized.match(/\*/g)?.length ?? 0) > 1 ||
		normalized
			.split("/")
			.some((segment) => segment === "" || segment === "." || segment === "..")
	) {
		fail(
			`${label} has unsafe or unsupported package pattern ${JSON.stringify(rawPattern)}; at most one * wildcard is allowed`,
		);
	}
	return normalized;
};

const normalizedTypesVersionsTargets = (manifest) => {
	if (manifest.typesVersions === undefined) {
		return [];
	}
	if (
		!manifest.typesVersions ||
		typeof manifest.typesVersions !== "object" ||
		Array.isArray(manifest.typesVersions)
	) {
		fail(`${manifest.name ?? "package"} typesVersions must be an object`);
	}
	const targets = [];
	for (const [selector, mappings] of Object.entries(manifest.typesVersions)) {
		const selectorLabel = `${manifest.name ?? "package"} typesVersions.${selector}`;
		if (!selector || /[\0\r\n]/.test(selector)) {
			fail(
				`${manifest.name ?? "package"} typesVersions selectors must be non-empty single-line strings`,
			);
		}
		if (!mappings || typeof mappings !== "object" || Array.isArray(mappings)) {
			fail(`${selectorLabel} must be a path mapping object`);
		}
		for (const [key, rawTargets] of Object.entries(mappings)) {
			normalizeTypesVersionsPattern(key, `${selectorLabel} key`);
			if (!Array.isArray(rawTargets) || rawTargets.length === 0) {
				fail(`${selectorLabel}.${key} must be a non-empty target array`);
			}
			for (const [index, target] of rawTargets.entries()) {
				targets.push(
					normalizeTypesVersionsPattern(
						target,
						`${selectorLabel}.${key}[${index}]`,
					),
				);
			}
		}
	}
	return targets;
};

const escapeRegularExpression = (source) =>
	source.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const matchesSingleTypesVersionsTarget = (relativePath, target) => {
	if (!target.includes("*")) {
		return matchesManifestTargetPath(relativePath, target);
	}
	if (
		!/(?:^|\/)(?:package\.json|[^/]+\.(?:ts|tsx|mts|cts|js|jsx|mjs|cjs|json))$/i.test(
			relativePath,
		)
	) {
		return false;
	}
	const [prefix, suffix] = target.split("*");
	return new RegExp(
		`^${escapeRegularExpression(prefix)}.*${escapeRegularExpression(suffix)}(?:$|[./])`,
	).test(relativePath);
};

const matchesTypesVersionsTarget = (relativePath, target) =>
	typeResolutionTargetAlternatives(target).some((alternative) =>
		matchesSingleTypesVersionsTarget(relativePath, alternative),
	);

const normalizedPublishedFilesEntries = (manifest) => {
	if (manifest?.files === undefined) {
		return [];
	}
	if (!Array.isArray(manifest.files)) {
		fail(`${manifest.name ?? "package"} files must be an array`);
	}
	const entries = [];
	for (const rawEntry of manifest.files) {
		if (typeof rawEntry !== "string") {
			fail(`${manifest.name ?? "package"} files entries must be strings`);
		}
		const bangCount = rawEntry.match(/^!*/)[0].length;
		const negated = bangCount % 2 === 1;
		const unsignedEntry = rawEntry.slice(bangCount);
		const entry = unsignedEntry.replace(/^\.\//, "").replace(/\/+$/, "");
		if (
			!entry ||
			entry.startsWith("/") ||
			entry.includes("\\") ||
			entry.includes("\0") ||
			entry
				.split("/")
				.some(
					(segment) => segment === "" || segment === "." || segment === "..",
				)
		) {
			fail(
				`${manifest.name ?? "package"} has unsafe files entry ${JSON.stringify(rawEntry)}`,
			);
		}
		const universal = /[()[\]{}!]/.test(entry);
		entries.push({ entry, negated, universal });
	}
	return entries;
};

const matchesPublishedFilesEntry = (relativePath, manifest) => {
	const entries = normalizedPublishedFilesEntries(manifest);
	let positiveEntries = 0;
	for (const { entry, negated, universal } of entries) {
		if (universal) {
			return true;
		}
		if (negated) {
			// A negation can narrow npm's packlist, but it is never trusted to hide a
			// runtime-affecting change from release review. Positive scope and runtime
			// classification below remain conservative.
			continue;
		}
		positiveEntries++;
		const wildcardIndex = entry.search(/[?*]/);
		if (wildcardIndex >= 0) {
			const literalPrefix = entry.slice(0, wildcardIndex);
			if (!literalPrefix || relativePath.startsWith(literalPrefix)) {
				return true;
			}
			continue;
		}
		if (relativePath === entry || relativePath.startsWith(`${entry}/`)) {
			return true;
		}
	}
	// An all-negation files array has tool-specific packlist semantics. Treat it
	// as broad scope rather than allowing it to suppress the guard.
	return entries.length > 0 && positiveEntries === 0;
};

const matchesPackedTypesVersionsPath = (relativePath, manifest) => {
	const targets = normalizedTypesVersionsTargets(manifest);
	if (targets.length === 0) {
		return false;
	}
	const mayBePacked =
		manifest.files === undefined ||
		matchesPublishedFilesEntry(relativePath, manifest);
	return (
		mayBePacked &&
		targets.some((target) => matchesTypesVersionsTarget(relativePath, target))
	);
};

const validatePackageReleaseMetadata = (packages) => {
	for (const candidate of packages.values()) {
		if (
			candidate.manifest.private !== undefined &&
			typeof candidate.manifest.private !== "boolean"
		) {
			fail(
				`${candidate.path} package ${candidate.manifest.name} private must be a boolean when present`,
			);
		}
		runtimePublishedPaths(candidate.manifest);
		typePublishedPaths(candidate.manifest);
		normalizedPublishedFilesEntries(candidate.manifest);
		normalizedTypesVersionsTargets(candidate.manifest);
		if (
			hasChangesetsVersion(candidate.manifest) &&
			(typeof candidate.manifest.version !== "string" ||
				!STABLE_VERSION_PATTERN.test(candidate.manifest.version))
		) {
			fail(
				`${candidate.path} non-skipped package ${candidate.manifest.name} must have a canonical string major.minor.patch version`,
			);
		}
	}
};

export const isReleaseRelevantPath = (relativePath, manifest = {}) => {
	validateRepositoryPath(relativePath, "package-relative path");
	if (relativePath === "package.json") {
		fail("package.json relevance must be decided from its semantic diff");
	}
	if (
		matchesRuntimePublishedPath(relativePath, manifest) ||
		matchesTypePublishedPath(relativePath, manifest)
	) {
		return true;
	}
	if (matchesPackedTypesVersionsPath(relativePath, manifest)) {
		return true;
	}
	if (matchesPublishedFilesEntry(relativePath, manifest)) {
		return true;
	}
	const segments = relativePath.split("/");
	if (segments[0] === "src" || segments[0] === "src_js") {
		// A filename carrying explicit test semantics stays exempt. Arbitrary
		// nested `docs`, `test`, or `tools` directories cannot hide source code.
		return !/(?:^|\.)(?:spec|test)(?:\.[^.]+)+$/i.test(segments.at(-1));
	}
	if (
		relativePath === "Cargo.toml" ||
		relativePath === "Cargo.lock" ||
		relativePath === "build.rs" ||
		relativePath === "rust-toolchain" ||
		relativePath === "rust-toolchain.toml"
	) {
		return true;
	}
	if (segments.includes("assets") || segments[0] === "wasm") {
		return true;
	}
	if (relativePath === ".npmignore" || relativePath === ".gitignore") {
		return true;
	}
	if (RUNTIME_FILE_PATTERN.test(relativePath)) {
		return !(
			isDocumentationPath(relativePath) ||
			isTestOrBenchmarkPath(relativePath) ||
			isToolConfigPath(relativePath)
		);
	}
	if (
		isDocumentationPath(relativePath) ||
		isTestOrBenchmarkPath(relativePath) ||
		isToolConfigPath(relativePath)
	) {
		return false;
	}
	// npm's default packlist is broad when `files` is absent. Unknown non-dev
	// files are therefore release-relevant instead of silently assumed private.
	return manifest.files === undefined;
};

const manifestWithoutKeys = (manifest, keys) =>
	Object.fromEntries(
		Object.entries(manifest).filter(([key]) => !keys.has(key)),
	);

export const hasReleaseRelevantManifestChange = (
	baseManifest,
	headManifest,
) => {
	if (!baseManifest || !headManifest) {
		return true;
	}
	const ignoredKeys = new Set(["version", ...TOOL_ONLY_MANIFEST_KEYS]);
	return !isDeepStrictEqual(
		manifestWithoutKeys(baseManifest, ignoredKeys),
		manifestWithoutKeys(headManifest, ignoredKeys),
	);
};

const hasOnlyVersionManifestChange = (baseManifest, headManifest) => {
	if (!baseManifest || !headManifest) {
		return false;
	}
	if (
		typeof baseManifest.version !== "string" ||
		typeof headManifest.version !== "string" ||
		baseManifest.version === headManifest.version
	) {
		return false;
	}
	return isDeepStrictEqual(
		manifestWithoutKeys(baseManifest, new Set(["version"])),
		manifestWithoutKeys(headManifest, new Set(["version"])),
	);
};

const parseChangesetKey = (source, path, lineNumber) => {
	let key;
	if (source.startsWith('"')) {
		try {
			key = JSON.parse(source);
		} catch (error) {
			if (error instanceof ChangesetGuardError) {
				throw error;
			}
			fail(`${path}:${lineNumber} has an invalid quoted package name`);
		}
	} else if (source.startsWith("'")) {
		if (!source.endsWith("'") || source.length < 2) {
			fail(`${path}:${lineNumber} has an invalid quoted package name`);
		}
		key = source.slice(1, -1).replaceAll("''", "'");
	} else {
		if (source.startsWith("@")) {
			fail(`${path}:${lineNumber} must quote scoped package names`);
		}
		key = source;
	}
	if (typeof key !== "string" || !PACKAGE_NAME_PATTERN.test(key)) {
		fail(`${path}:${lineNumber} has an invalid flat package name`);
	}
	return key;
};

export const parseChangeset = (source, path = "changeset.md") => {
	if (typeof source !== "string") {
		fail(`${path} must be UTF-8 text`);
	}
	const lines = normalizeLineEndings(source).split("\n");
	if (lines[0] !== "---") {
		fail(`${path} must start with a flat changeset frontmatter delimiter`);
	}
	const closingIndex = lines.indexOf("---", 1);
	if (closingIndex < 0) {
		fail(`${path} is missing its closing frontmatter delimiter`);
	}
	const releases = new Map();
	for (let index = 1; index < closingIndex; index++) {
		const line = lines[index];
		if (line.trim() === "") {
			continue;
		}
		if (line.includes("\t")) {
			fail(`${path}:${index + 1} must not contain tabs`);
		}
		if (line.includes("#")) {
			fail(`${path}:${index + 1} must not contain YAML comments`);
		}
		if (line.startsWith("@")) {
			fail(`${path}:${index + 1} must quote scoped package names`);
		}
		const match = line.match(
			/^("(?:[^"\\]|\\.)*"|'(?:[^']|'')*'|[a-z0-9][a-z0-9._~-]*) *: +([A-Za-z]+) *$/,
		);
		if (!match) {
			fail(`${path}:${index + 1} must be a flat package: bump entry`);
		}
		const packageName = parseChangesetKey(match[1], path, index + 1);
		const bump = match[2];
		if (!VERSION_BUMPS.has(bump)) {
			fail(
				`${path}:${index + 1} has unsupported bump ${JSON.stringify(match[2])}`,
			);
		}
		if (releases.has(packageName)) {
			fail(`${path}:${index + 1} repeats package ${packageName}`);
		}
		releases.set(packageName, bump);
	}
	if (releases.size === 0) {
		fail(`${path} has no package releases`);
	}
	const summary = lines
		.slice(closingIndex + 1)
		.join("\n")
		.trim();
	if (!summary) {
		fail(`${path} has no user-facing summary`);
	}
	assertChangesetSummaryCompatible(summary, path);
	return { releases, summary };
};

const isDirectChangesetPath = (path) => {
	if (typeof path !== "string" || !path.startsWith(".changeset/")) {
		return false;
	}
	const basename = path.slice(".changeset/".length);
	return (
		basename.length > ".md".length &&
		!basename.includes("/") &&
		!basename.startsWith(".") &&
		basename.endsWith(".md") &&
		basename.toLowerCase() !== "readme.md"
	);
};

const buildPackageState = async (repository, baseSha, headSha) => {
	const baseFiles = new Set(await repository.listFiles(baseSha));
	const headFiles = new Set(await repository.listFiles(headSha));
	for (const [sha, files] of [
		[baseSha, baseFiles],
		[headSha, headFiles],
	]) {
		if (files.has(".changeset/pre.json")) {
			fail(
				`${sha} uses unsupported Changesets prerelease mode; this guard validates the stable release plan only`,
			);
		}
	}
	const baseWorkspacePatterns = await readWorkspacePolicy(
		repository,
		baseSha,
		baseFiles,
	);
	const headWorkspacePatterns = await readWorkspacePolicy(
		repository,
		headSha,
		headFiles,
	);
	const baseChangesetConfig = await parseIgnoreConfig(
		repository,
		baseSha,
		baseFiles,
	);
	const headChangesetConfig = await parseIgnoreConfig(
		repository,
		headSha,
		headFiles,
	);
	const basePackages = await discoverPackages(
		repository,
		baseSha,
		baseFiles,
		baseWorkspacePatterns,
	);
	const headPackages = await discoverPackages(
		repository,
		headSha,
		headFiles,
		headWorkspacePatterns,
	);
	// Existing package ownership remains anchored to trusted base workspace
	// patterns. A same-PR nested workspace root therefore cannot shadow an
	// existing public owner. The independent head-policy graph above still makes
	// every newly authoritative/public package visible and coverable.
	const headOwnershipPackages = await discoverPackages(
		repository,
		headSha,
		headFiles,
		baseWorkspacePatterns,
	);
	validatePackageReleaseMetadata(basePackages);
	validatePackageReleaseMetadata(headPackages);
	validateIgnoredPackagePolicy(
		baseSha,
		baseChangesetConfig.ignoredNames,
		basePackages,
	);
	validateIgnoredPackagePolicy(
		headSha,
		headChangesetConfig.ignoredNames,
		headPackages,
	);
	validateRootPackagePolicy(baseSha, basePackages);
	validateRootPackagePolicy(headSha, headPackages);
	validateSkippedDependencyGraph(
		baseSha,
		baseChangesetConfig.ignoredNames,
		basePackages,
	);
	validateSkippedDependencyGraph(
		headSha,
		headChangesetConfig.ignoredNames,
		headPackages,
	);
	validateInternalDependencyRanges(baseSha, basePackages);
	validateInternalDependencyRanges(headSha, headPackages);
	// A PR must not be able to suppress its own package changes by adding that
	// package to Changesets' ignore list. Only policy present on both sides of
	// the comparison is authoritative for ordinary release ownership.
	const ignoredNames = new Set(
		[...baseChangesetConfig.ignoredNames].filter((name) =>
			headChangesetConfig.ignoredNames.has(name),
		),
	);
	const newlyIgnoredNames = new Set(
		[...headChangesetConfig.ignoredNames].filter(
			(name) => !baseChangesetConfig.ignoredNames.has(name),
		),
	);
	const removedIgnoredNames = new Set(
		[...baseChangesetConfig.ignoredNames].filter(
			(name) => !headChangesetConfig.ignoredNames.has(name),
		),
	);
	const pairs = pairPackages(basePackages, headPackages, ignoredNames);
	const versionableNames = new Set();
	for (const pair of pairs.values()) {
		for (const name of pair.versionableNames) {
			versionableNames.add(name);
		}
	}
	const baseByName = packagesByName(basePackages);
	const newPublicPackageNames = new Set();
	for (const candidate of headPackages.values()) {
		if (
			!isVersionableManifest(
				candidate.manifest,
				headChangesetConfig.ignoredNames,
			)
		) {
			continue;
		}
		const base = baseByName.get(candidate.manifest.name);
		if (
			!base ||
			base.root !== candidate.root ||
			!isVersionableManifest(base.manifest, baseChangesetConfig.ignoredNames)
		) {
			newPublicPackageNames.add(candidate.manifest.name);
		}
	}
	return {
		baseFiles,
		headFiles,
		basePackages,
		headPackages,
		headOwnershipPackages,
		baseWorkspacePatterns,
		headWorkspacePatterns,
		baseChangesetConfig,
		headChangesetConfig,
		pairs,
		ignoredNames,
		newlyIgnoredNames,
		removedIgnoredNames,
		newPublicPackageNames,
		versionableNames,
	};
};

const assertNoManualVersionEdits = (state) => {
	const errors = [];
	for (const pair of state.pairs.values()) {
		if (
			pair.versionableNames.size > 0 &&
			pair.base &&
			pair.head &&
			pair.base.manifest.version !== pair.head.manifest.version &&
			!(
				!hasChangesetsVersion(pair.base.manifest) &&
				hasChangesetsVersion(pair.head.manifest)
			)
		) {
			errors.push(
				`${pair.head.manifest.name ?? pair.base.manifest.name} (${pair.head.path})`,
			);
		}
	}
	const allBaseByName = packagesByName(state.basePackages);
	const allHeadByName = packagesByName(state.headPackages);
	for (const name of new Set([
		...state.baseChangesetConfig.ignoredNames,
		...state.headChangesetConfig.ignoredNames,
	])) {
		const base = allBaseByName.get(name);
		const head = allHeadByName.get(name);
		if (
			base &&
			head &&
			(isPublicManifest(base.manifest) || isPublicManifest(head.manifest)) &&
			base.manifest.version !== head.manifest.version
		) {
			errors.push(`${name} (${base.path} -> ${head.path}; ignored/frozen)`);
		}
	}
	const byName = (packages) => {
		const result = new Map();
		for (const candidate of packages.values()) {
			if (!isVersionableManifest(candidate.manifest, state.ignoredNames)) {
				continue;
			}
			const existing = result.get(candidate.manifest.name);
			if (existing) {
				fail(
					`Package name ${candidate.manifest.name} occurs at both ${existing.path} and ${candidate.path}`,
				);
			}
			result.set(candidate.manifest.name, candidate);
		}
		return result;
	};
	const baseByName = byName(state.basePackages);
	const headByName = byName(state.headPackages);
	for (const name of state.versionableNames) {
		const base = baseByName.get(name);
		const head = headByName.get(name);
		if (
			base &&
			head &&
			base.root !== head.root &&
			base.manifest.version !== head.manifest.version
		) {
			errors.push(`${name} (${base.path} -> ${head.path})`);
		}
	}
	if (errors.length > 0) {
		fail(
			"Publishable package versions may only be changed by the generated Version Packages PR:\n- " +
				[...new Set(errors)].join("\n- "),
		);
	}
};

const assertNoUnstagedPublishablePackageRemoval = (state) => {
	const headByName = packagesByName(state.headPackages);
	const removals = [];
	for (const basePackage of state.basePackages.values()) {
		const name = basePackage.manifest.name;
		if (
			!isVersionableManifest(
				basePackage.manifest,
				state.baseChangesetConfig.ignoredNames,
			)
		) {
			continue;
		}
		const headPackage = headByName.get(name);
		if (
			!headPackage ||
			headPackage.root !== basePackage.root ||
			!isVersionableManifest(
				headPackage.manifest,
				state.headChangesetConfig.ignoredNames,
			)
		) {
			removals.push(
				`${name} (${basePackage.path}${headPackage ? ` -> ${headPackage.path}` : " -> removed/private/renamed"})`,
			);
		}
	}
	if (removals.length > 0) {
		fail(
			"Publishable packages cannot be removed, renamed, moved, or made private in one changeset PR because Changesets cannot version a package absent from the head tree:\n- " +
				removals.join("\n- ") +
				"\nStage each removal: (1) keep the package at the same public root/name and release its final deprecation changeset; (2) after that Version Packages PR is merged, use a policy-only PR to add the old name to .changeset/config.json ignore and mark it private without changing its version or source; (3) in a later policy PR, remove the private package and its exact ignore entry together.",
		);
	}
};

const validateNewIgnorePolicyTransition = (diffEntries, state) => {
	if (state.newlyIgnoredNames.size === 0) {
		return false;
	}
	if (state.removedIgnoredNames.size > 0) {
		fail(
			"A policy PR that adds Changesets ignore entries must not remove other ignore entries",
		);
	}
	const basePolicy = {
		...state.baseChangesetConfig.config,
		ignore: [],
	};
	const headPolicy = {
		...state.headChangesetConfig.config,
		ignore: [],
	};
	if (!isDeepStrictEqual(basePolicy, headPolicy)) {
		fail(
			"A policy PR that adds Changesets ignore entries may not change other release-plan policy",
		);
	}
	const baseByName = packagesByName(state.basePackages);
	const headByName = packagesByName(state.headPackages);
	const allowedPaths = new Set([".changeset/config.json"]);
	for (const name of state.newlyIgnoredNames) {
		const base = baseByName.get(name);
		const head = headByName.get(name);
		if (!base || !head || base.root !== head.root) {
			fail(
				`New Changesets ignore entry ${name} must retire the same existing package root in both trees`,
			);
		}
		if (base.manifest.version !== head.manifest.version) {
			fail(`Newly ignored package ${name} must keep its version frozen`);
		}
		if (head.manifest.private !== true) {
			fail(`Newly ignored package ${name} must be private in the policy PR`);
		}
		const baseManifestPolicy = { ...base.manifest, private: true };
		if (!isDeepStrictEqual(baseManifestPolicy, head.manifest)) {
			fail(
				`Newly ignored package ${name} may change only private:false to private:true in its policy PR`,
			);
		}
		allowedPaths.add(head.path);
	}
	for (const entry of diffEntries) {
		const paths = [entry.oldPath, entry.newPath].filter(Boolean);
		if (
			(entry.code !== "M" && entry.code !== "T") ||
			paths.some((path) => !allowedPaths.has(path))
		) {
			fail(
				`Adding a Changesets ignore entry is a policy-only transition; unexpected ${entry.status} ${entry.oldPath ?? entry.newPath}`,
			);
		}
	}
	return true;
};

const EXPECTED_TARGET_WORKFLOW_EXPRESSIONS = [
	"github.event.pull_request.base.repo.full_name",
	"github.event.pull_request.base.sha",
	"github.event.pull_request.base.sha",
	"github.event.pull_request.head.sha",
	"github.event.pull_request.number",
	"github.repository",
	"github.workspace",
].sort();

const EXPECTED_TARGET_WORKFLOW_RUN_LINES = [
	"set -euo pipefail",
	'if [[ ! "$PR_NUMBER" =~ ^[1-9][0-9]*$ ]]; then',
	'  echo "Pull request number is not a positive integer" >&2',
	"  exit 1",
	"fi",
	'if [[ ! "$TRUSTED_BASE_SHA" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]]; then',
	'  echo "Trusted base SHA is not a full lowercase Git object id" >&2',
	"  exit 1",
	"fi",
	'if [[ ! "$UNTRUSTED_HEAD_SHA" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]]; then',
	'  echo "Untrusted head SHA is not a full lowercase Git object id" >&2',
	"  exit 1",
	"fi",
	'if [[ "$EVENT_REPOSITORY" != "dao-xyz/peerbit" || "$BASE_REPOSITORY" != "$EVENT_REPOSITORY" ]]; then',
	'  echo "Changeset guard repository identity mismatch" >&2',
	"  exit 1",
	"fi",
	'checked_out_base=$(git rev-parse --verify "HEAD^{commit}")',
	'if [[ "$checked_out_base" != "$TRUSTED_BASE_SHA" ]]; then',
	'  echo "Checkout does not match the trusted event base SHA" >&2',
	"  exit 1",
	"fi",
	"git fetch --no-tags --no-write-fetch-head --force origin \\",
	'  "refs/pull/${PR_NUMBER}/head:refs/remotes/pull-request/head"',
	'fetched_head=$(git rev-parse --verify "refs/remotes/pull-request/head^{commit}")',
	'if [[ "$fetched_head" != "$UNTRUSTED_HEAD_SHA" ]]; then',
	'  echo "Fetched pull request head does not match the event head SHA" >&2',
	"  exit 1",
	"fi",
	'git cat-file -e "${TRUSTED_BASE_SHA}^{commit}"',
	'git cat-file -e "${UNTRUSTED_HEAD_SHA}^{commit}"',
	"node --test scripts/ci/check-changeset-required.test.mjs",
	"node scripts/ci/check-changeset-required.mjs",
];

export const EXPECTED_CHANGESET_GUARD_WORKFLOW = [
	"name: Changeset Guard",
	"",
	"on:",
	"  pull_request_target:",
	"    types: [opened, reopened, synchronize, edited]",
	"",
	"permissions:",
	"  contents: read",
	"",
	"jobs:",
	"  changeset_guard:",
	"    name: Enforce Changeset Coverage",
	"    runs-on: ubuntu-22.04",
	"    timeout-minutes: 10",
	"    steps:",
	"      - name: Checkout the exact trusted base",
	"        uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4",
	"        with:",
	"          ref: ${{ github.event.pull_request.base.sha }}",
	"          fetch-depth: 0",
	"          persist-credentials: false",
	"      - name: Setup trusted Node runtime",
	"        uses: actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020 # v4",
	"        with:",
	"          node-version: 22.x",
	"      - name: Fetch the untrusted head as data and run the base guard",
	"        shell: bash",
	"        env:",
	"          PR_NUMBER: ${{ github.event.pull_request.number }}",
	"          TRUSTED_BASE_SHA: ${{ github.event.pull_request.base.sha }}",
	"          UNTRUSTED_HEAD_SHA: ${{ github.event.pull_request.head.sha }}",
	"          EVENT_REPOSITORY: ${{ github.repository }}",
	"          BASE_REPOSITORY: ${{ github.event.pull_request.base.repo.full_name }}",
	"          CHANGESET_GUARD_REPOSITORY_ROOT: ${{ github.workspace }}",
	"        run: |",
	...EXPECTED_TARGET_WORKFLOW_RUN_LINES.map((line) => `          ${line}`),
	"",
].join("\n");

export const validateChangesetGuardWorkflow = (
	source,
	path = CHANGESET_GUARD_WORKFLOW_PATH,
) => {
	if (typeof source !== "string") {
		fail(`${path} must be UTF-8 text`);
	}
	if (source !== EXPECTED_CHANGESET_GUARD_WORKFLOW) {
		fail(
			`${path} must exactly match the canonical data-only base-guard workflow`,
		);
	}
	const topLevelKeys = [...source.matchAll(/^([a-z_][a-z0-9_-]*):/gm)].map(
		(match) => match[1],
	);
	if (!isDeepStrictEqual(topLevelKeys, ["name", "on", "permissions", "jobs"])) {
		fail(`${path} must contain only name, on, permissions, and jobs`);
	}
	if (
		!source.startsWith("name: Changeset Guard\n\non:\n") ||
		!source.includes(
			"on:\n  pull_request_target:\n    types: [opened, reopened, synchronize, edited]\n\npermissions:\n  contents: read\n\njobs:\n",
		)
	) {
		fail(
			`${path} must be the dedicated read-only pull_request_target workflow`,
		);
	}
	const jobsSource = source.slice(source.indexOf("\njobs:\n") + 7);
	const jobNames = [...jobsSource.matchAll(/^  ([a-z_][a-z0-9_-]*):$/gm)].map(
		(match) => match[1],
	);
	if (!isDeepStrictEqual(jobNames, ["changeset_guard"])) {
		fail(`${path} must contain exactly the changeset_guard job`);
	}
	for (const required of [
		"    name: Enforce Changeset Coverage\n",
		"    runs-on: ubuntu-22.04\n",
		"    timeout-minutes: 10\n",
		"        uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4\n",
		"          ref: ${{ github.event.pull_request.base.sha }}\n",
		"          fetch-depth: 0\n",
		"          persist-credentials: false\n",
		"        uses: actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020 # v4\n",
		"          node-version: 22.x\n",
		"        shell: bash\n",
	]) {
		if (!source.includes(required)) {
			fail(
				`${path} is missing protected workflow line ${JSON.stringify(required.trim())}`,
			);
		}
	}
	const steps = [...source.matchAll(/^      - name: /gm)];
	if (steps.length !== 3) {
		fail(`${path} must contain exactly three named steps`);
	}
	const uses = [...source.matchAll(/^        uses: ([^\n]+)$/gm)].map(
		(match) => match[1],
	);
	if (
		!isDeepStrictEqual(uses, [
			"actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4",
			"actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020 # v4",
		])
	) {
		fail(`${path} may use only the two commit-pinned trusted actions`);
	}
	const expressions = [...source.matchAll(/\$\{\{\s*([^}]+?)\s*\}\}/g)]
		.map((match) => match[1].trim())
		.sort();
	if (!isDeepStrictEqual(expressions, EXPECTED_TARGET_WORKFLOW_EXPRESSIONS)) {
		fail(`${path} contains an unapproved GitHub expression`);
	}
	const runMarker = "        run: |\n";
	if (source.indexOf(runMarker) !== source.lastIndexOf(runMarker)) {
		fail(`${path} must contain exactly one shell run block`);
	}
	const runIndex = source.indexOf(runMarker);
	if (runIndex < 0) {
		fail(`${path} is missing its protected shell run block`);
	}
	const rawRunLines = source.slice(runIndex + runMarker.length).split("\n");
	if (rawRunLines.at(-1) === "") {
		rawRunLines.pop();
	}
	if (rawRunLines.some((line) => !line.startsWith("          "))) {
		fail(`${path} must end immediately after its protected shell run block`);
	}
	const runLines = rawRunLines.map((line) => line.slice(10));
	if (!isDeepStrictEqual(runLines, EXPECTED_TARGET_WORKFLOW_RUN_LINES)) {
		fail(
			`${path} shell commands differ from the data-only base-guard contract`,
		);
	}
	if (
		/\$\{\{\s*secrets\.|(?:contents|issues|pull-requests|id-token): write|continue-on-error:|environment:|container:|services:|actions\/cache|actions\/(?:upload|download)-artifact|\b(?:npm|pnpm|yarn|corepack)\b|\bgit (?:checkout|switch|reset|clean)\b/.test(
			source,
		)
	) {
		fail(
			`${path} contains forbidden credentials, mutation, installation, or head-execution behavior`,
		);
	}
	return true;
};

const assertChangesetGuardPair = async (
	repository,
	baseSha,
	headSha,
	state,
) => {
	const baseCount = CHANGESET_GUARD_PATHS.filter((path) =>
		state.baseFiles.has(path),
	).length;
	const missingFromHead = CHANGESET_GUARD_PATHS.filter(
		(path) => !state.headFiles.has(path),
	);
	const baseWorkflowPresent = state.baseFiles.has(
		CHANGESET_GUARD_WORKFLOW_PATH,
	);
	if (
		baseCount === 1 ||
		(baseCount === 0 && baseWorkflowPresent) ||
		(baseCount === CHANGESET_GUARD_PATHS.length && !baseWorkflowPresent)
	) {
		fail(
			"Trusted base contains a partial changeset guard trust boundary; repair the guard, tests, and target workflow together before validating package changes",
		);
	}
	if (
		missingFromHead.length > 0 ||
		!state.headFiles.has(CHANGESET_GUARD_WORKFLOW_PATH)
	) {
		fail(
			`The changeset guard, its tests, and its base-owned target workflow are required and cannot be removed: ${[
				...missingFromHead,
				...(state.headFiles.has(CHANGESET_GUARD_WORKFLOW_PATH)
					? []
					: [CHANGESET_GUARD_WORKFLOW_PATH]),
			].join(", ")}`,
		);
	}
	validateChangesetGuardWorkflow(
		await repository.readFile(headSha, CHANGESET_GUARD_WORKFLOW_PATH),
		CHANGESET_GUARD_WORKFLOW_PATH,
	);
	if (baseCount === CHANGESET_GUARD_PATHS.length && baseWorkflowPresent) {
		for (const path of CHANGESET_GUARD_PATHS) {
			if (
				(await repository.readFile(baseSha, path)) !==
				(await repository.readFile(headSha, path))
			) {
				fail(
					`${path} is a frozen executable root-of-trust boundary and must remain byte-identical to the trusted base`,
				);
			}
		}
	}
};

const isReleaseArtifactInputPath = (relativePath) => {
	validateRepositoryPath(relativePath, "artifact-input package-relative path");
	if (
		relativePath === "package.json" ||
		relativePath.startsWith("public/") ||
		relativePath.startsWith("src/")
	) {
		return true;
	}
	return !(
		isDocumentationPath(relativePath) || isTestOrBenchmarkPath(relativePath)
	);
};

const affectedPackages = (diffEntries, state) => {
	const affected = new Set();
	for (const entry of diffEntries) {
		for (const path of new Set([entry.oldPath, entry.newPath])) {
			if (!path) {
				continue;
			}
			const owners = [];
			const baseOwner = findOwningPackage(state.basePackages, path);
			if (baseOwner) {
				owners.push(baseOwner);
			}
			const basePolicyHeadOwner = findOwningPackage(
				state.headOwnershipPackages,
				path,
			);
			if (
				basePolicyHeadOwner &&
				state.basePackages.has(basePolicyHeadOwner.root) &&
				state.headPackages.has(basePolicyHeadOwner.root)
			) {
				owners.push(basePolicyHeadOwner);
			}
			const headPolicyOwner = findOwningPackage(state.headPackages, path);
			if (headPolicyOwner) {
				owners.push(headPolicyOwner);
			}
			for (const owner of owners) {
				const pair = state.pairs.get(owner.root);
				if (!pair || pair.versionableNames.size === 0) {
					continue;
				}
				const relativePath = relativeToPackage(owner.root, path);
				const relevant =
					relativePath === "package.json"
						? hasReleaseRelevantManifestChange(
								pair.base?.manifest,
								pair.head?.manifest,
							)
						: isReleaseRelevantPath(relativePath, owner.manifest);
				if (relevant) {
					for (const name of pair.versionableNames) {
						affected.add(name);
					}
				}
			}
		}
		for (const [producerRoot, consumerName] of RELEASE_ARTIFACT_OWNERS) {
			for (const path of [entry.oldPath, entry.newPath]) {
				if (
					path &&
					(path === producerRoot || path.startsWith(`${producerRoot}/`)) &&
					state.versionableNames.has(consumerName) &&
					isReleaseArtifactInputPath(relativeToPackage(producerRoot, path))
				) {
					affected.add(consumerName);
				}
			}
		}
	}
	for (const name of state.newPublicPackageNames) {
		if (!state.ignoredNames.has(name)) {
			affected.add(name);
		}
	}
	return affected;
};

const collectNewChangesets = async (
	repository,
	baseSha,
	headSha,
	diffEntries,
	state,
) => {
	const paths = [];
	for (const entry of diffEntries) {
		const touchesChangeset =
			isDirectChangesetPath(entry.oldPath) ||
			isDirectChangesetPath(entry.newPath);
		if (!touchesChangeset) {
			continue;
		}
		if (
			entry.code !== "A" ||
			!isDirectChangesetPath(entry.newPath) ||
			state.baseFiles.has(entry.newPath) ||
			!state.headFiles.has(entry.newPath)
		) {
			fail(
				`Only newly added .changeset/*.md files may provide PR coverage; ${entry.status} ${entry.oldPath ?? entry.newPath} is not new at the merge base`,
			);
		}
		paths.push(entry.newPath);
	}
	const coverage = new Set();
	for (const path of paths.sort()) {
		const parsed = parseChangeset(
			await repository.readFile(headSha, path),
			path,
		);
		for (const packageName of parsed.releases.keys()) {
			if (!state.versionableNames.has(packageName)) {
				fail(
					`${path} names ${packageName}, which is private, ignored, missing, or not a publishable package in this PR`,
				);
			}
			coverage.add(packageName);
		}
	}
	return { paths, coverage };
};

const highestVersionBump = (left, right) =>
	VERSION_BUMP_PRIORITY.get(left) >= VERSION_BUMP_PRIORITY.get(right)
		? left
		: right;

const parseStableVersion = (version, label) => {
	const match = version?.match(STABLE_VERSION_PATTERN);
	if (!match) {
		fail(
			`${label} has unsupported version ${JSON.stringify(version)}; generated stable Version Packages PRs require canonical major.minor.patch versions`,
		);
	}
	let major = Number(match[1]);
	let minor = Number(match[2]);
	let patch = Number(match[3]);
	if (![major, minor, patch].every(Number.isSafeInteger)) {
		fail(`${label} version components must be safe integers`);
	}
	return { major, minor, patch };
};

const incrementStableVersion = (version, bump, label) => {
	let { major, minor, patch } = parseStableVersion(version, label);
	if (bump === "major") {
		major++;
		minor = 0;
		patch = 0;
	} else if (bump === "minor") {
		minor++;
		patch = 0;
	} else {
		patch++;
	}
	if (![major, minor, patch].every(Number.isSafeInteger)) {
		fail(`${label} version bump exceeds JavaScript safe integers`);
	}
	return `${major}.${minor}.${patch}`;
};

const compareStableVersions = (left, right) => {
	for (const key of ["major", "minor", "patch"]) {
		if (left[key] !== right[key]) {
			return left[key] < right[key] ? -1 : 1;
		}
	}
	return 0;
};

const stableVersionSatisfiesInternalRange = (
	version,
	rawRange,
	oldVersion,
	label,
) => {
	if (!SUPPORTED_INTERNAL_WORKSPACE_RANGES.has(rawRange)) {
		fail(
			`${label} must use a preserved internal workspace range (workspace:*, workspace:^, or workspace:~)`,
		);
	}
	let range = rawRange.slice("workspace:".length);
	if (range === "*") {
		range = oldVersion;
	} else {
		range += oldVersion;
	}
	const operator = range[0] === "^" || range[0] === "~" ? range[0] : "";
	const minimumSource = operator ? range.slice(1) : range;
	const candidate = parseStableVersion(version, `${label} candidate`);
	const minimum = parseStableVersion(minimumSource, `${label} range`);
	if (compareStableVersions(candidate, minimum) < 0) {
		return false;
	}
	if (operator === "^") {
		if (minimum.major > 0) {
			return candidate.major === minimum.major;
		}
		if (minimum.minor > 0) {
			return candidate.major === 0 && candidate.minor === minimum.minor;
		}
		return (
			candidate.major === 0 &&
			candidate.minor === 0 &&
			candidate.patch === minimum.patch
		);
	}
	if (operator === "~") {
		return (
			candidate.major === minimum.major && candidate.minor === minimum.minor
		);
	}
	return compareStableVersions(candidate, minimum) === 0;
};

const deriveExpectedVersionPlan = (directBumps, state) => {
	const releases = new Map(directBumps);
	const baseByName = packagesByName(state.basePackages);
	let updated = true;
	while (updated) {
		updated = false;
		for (const candidate of state.basePackages.values()) {
			const candidateName = candidate.manifest.name;
			if (!isVersionableManifest(candidate.manifest, state.ignoredNames)) {
				continue;
			}
			for (const field of RUNTIME_DEPENDENCY_FIELDS) {
				const dependencies = candidate.manifest[field];
				if (dependencies === undefined) {
					continue;
				}
				if (
					!dependencies ||
					typeof dependencies !== "object" ||
					Array.isArray(dependencies)
				) {
					fail(`${candidateName} ${field} must be an object`);
				}
				for (const [dependencyName, dependencyRange] of Object.entries(
					dependencies,
				)) {
					const dependencyBump = releases.get(dependencyName);
					if (!dependencyBump) {
						continue;
					}
					const dependencyPackage = baseByName.get(dependencyName);
					if (!dependencyPackage) {
						fail(
							`${candidateName} references missing internal release ${dependencyName}`,
						);
					}
					let requiredBump;
					if (field === "peerDependencies" && dependencyBump !== "patch") {
						// Changesets' default policy promotes peer dependents to major for a
						// non-patch dependency release, even if the declared range still fits.
						requiredBump = "major";
					} else {
						const dependencyVersion = incrementStableVersion(
							dependencyPackage.manifest.version,
							dependencyBump,
							dependencyName,
						);
						const remainsInRange = stableVersionSatisfiesInternalRange(
							dependencyVersion,
							dependencyRange,
							dependencyPackage.manifest.version,
							`${candidateName} -> ${dependencyName}`,
						);
						if (!remainsInRange && !releases.has(candidateName)) {
							requiredBump = "patch";
						}
					}
					if (
						requiredBump &&
						(!releases.has(candidateName) ||
							VERSION_BUMP_PRIORITY.get(requiredBump) >
								VERSION_BUMP_PRIORITY.get(releases.get(candidateName)))
					) {
						releases.set(candidateName, requiredBump);
						updated = true;
					}
				}
			}
		}
	}
	return releases;
};

const validateVersionLockfileChange = async (
	repository,
	baseSha,
	headSha,
	versionPackages,
) => {
	const baseLockfile = await repository.readFile(baseSha, "pnpm-lock.yaml");
	const headLockfile = await repository.readFile(headSha, "pnpm-lock.yaml");
	let expected = baseLockfile;
	for (const [name, properties] of [...versionPackages].sort(
		([left], [right]) => left.localeCompare(right),
	)) {
		expected = expected.replaceAll(
			`${name}@${properties.baseVersion}`,
			`${name}@${properties.version}`,
		);
	}
	if (expected !== headLockfile) {
		fail(
			"Generated Version Packages PR pnpm-lock.yaml change is not an exact package-name@old-version to package-name@new-version projection; regenerate it from the base changesets",
		);
	}
};

const RAW_HTML_PATTERN =
	/<!--|-->|<\/?[A-Za-z][A-Za-z0-9-]*(?:[ \t][^>\n]*)?\/?>|<![A-Za-z][^>\n]*>|<\?[^>\n]*\?>|^ {0,3}<(?:\/?[A-Za-z][A-Za-z0-9-]*(?=[ \t]|\/?>|$)|!|\?)/m;

const parseAtxHeading = (content) => {
	const match = content.match(/^(#{1,6})(?:[ \t]+([^\r\n]*))?[ \t]*$/);
	if (!match) {
		return undefined;
	}
	return {
		level: match[1].length,
		text: (match[2] ?? "")
			.trimEnd()
			.replace(/[ \t]+#+[ \t]*$/, "")
			.trim(),
	};
};

const analyzeMarkdownStructure = (source) => {
	const normalized = normalizeLineEndings(source);
	const headings = [];
	const setextHeadings = [];
	let fence;
	let offset = 0;
	let previousParagraphLine = false;
	for (const line of normalized.split("\n")) {
		const lineOffset = offset;
		offset += line.length + 1;
		const content = line.match(/^ {0,3}(.*)$/)?.[1];
		if (content === undefined) {
			previousParagraphLine = false;
			continue;
		}
		if (fence) {
			const closing = content.match(/^(`+|~+)[ \t]*$/);
			if (
				closing &&
				closing[1][0] === fence.character &&
				closing[1].length >= fence.length
			) {
				fence = undefined;
			}
			previousParagraphLine = false;
			continue;
		}
		const opening = content.match(/^(`{3,}|~{3,})([^\r\n]*)$/);
		if (opening && !(opening[1][0] === "`" && opening[2].includes("`"))) {
			fence = {
				character: opening[1][0],
				length: opening[1].length,
			};
			previousParagraphLine = false;
			continue;
		}
		const atxHeading = parseAtxHeading(content);
		if (atxHeading) {
			headings.push({ ...atxHeading, index: lineOffset });
			previousParagraphLine = false;
			continue;
		}
		const setextUnderline = content.match(/^(=+|-+)[ \t]*$/);
		if (setextUnderline) {
			if (previousParagraphLine) {
				setextHeadings.push({
					level: setextUnderline[1][0] === "=" ? 1 : 2,
					index: lineOffset,
				});
			}
			previousParagraphLine = false;
			continue;
		}
		const trimmed = content.trim();
		previousParagraphLine =
			trimmed.length > 0 && !/^(?:>|[-+*][ \t]|\d+[.)][ \t])/.test(content);
	}
	return {
		headings,
		setextHeadings,
		unclosedFence: fence !== undefined,
	};
};

const allAtxHeadings = (source) =>
	normalizeLineEndings(source)
		.split("\n")
		.flatMap((line, index) => {
			const content = line.match(/^ {0,3}(.*)$/)?.[1];
			const heading =
				content === undefined ? undefined : parseAtxHeading(content);
			return heading ? [{ ...heading, line: index + 1 }] : [];
		});

const hasMarkdownFenceDelimiter = (source) =>
	normalizeLineEndings(source)
		.split("\n")
		.some((line) => /^[ \t]*(?:`{3,}|~{3,})/.test(line));

const hasMarkdownSetextDelimiter = (source) =>
	normalizeLineEndings(source)
		.split("\n")
		.some((line) => /^[ \t]*(?:=+|-+)[ \t]*$/.test(line));

const assertChangesetSummaryCompatible = (summary, path) => {
	const normalized = normalizeLineEndings(summary);
	if (RAW_HTML_PATTERN.test(normalized)) {
		fail(
			`${path} summary may not contain raw HTML that the generated changelog rejects`,
		);
	}
	if (hasMarkdownFenceDelimiter(normalized)) {
		fail(
			`${path} summary may not contain Markdown fence delimiters that changelog generation can restructure`,
		);
	}
	if (hasMarkdownSetextDelimiter(normalized)) {
		fail(
			`${path} summary may not contain Setext H1/H2 delimiters that changelog generation can restructure`,
		);
	}
	const structure = analyzeMarkdownStructure(normalized);
	if (structure.unclosedFence) {
		fail(`${path} summary may not contain an unclosed Markdown fence`);
	}
	if (allAtxHeadings(normalized).some((heading) => heading.level <= 2)) {
		fail(
			`${path} summary may not contain ATX H1/H2 headings that alter generated changelog structure`,
		);
	}
	if (structure.setextHeadings.length > 0) {
		fail(
			`${path} summary may not contain Setext H1/H2 headings that alter generated changelog structure`,
		);
	}
};

const assertGeneratedChangelogStructure = (
	source,
	path,
	expectedVersion,
	expectedTitle,
	generatedSource = source,
) => {
	const normalized = normalizeLineEndings(source);
	const normalizedGenerated = normalizeLineEndings(generatedSource);
	const titleEnd = normalized.indexOf("\n");
	if (titleEnd < 0) {
		fail(`${path} is missing its changelog title and version section`);
	}
	const title = normalized.slice(0, titleEnd);
	if (expectedTitle !== undefined) {
		if (title !== expectedTitle) {
			fail(`${path} rewrites its existing changelog title`);
		}
	} else if (!/^#(?!#)[ \t]+\S/.test(title)) {
		fail(`${path} must begin with one generated top-level changelog title`);
	}
	if (RAW_HTML_PATTERN.test(normalizedGenerated)) {
		fail(
			`${path} generated release content may not contain raw HTML that can hide or wrap changelog structure`,
		);
	}
	const structure = analyzeMarkdownStructure(normalized);
	if (structure.setextHeadings.length > 0) {
		fail(`${path} may not add Setext H1/H2 changelog sections`);
	}
	const h1Headings = structure.headings.filter(
		(heading) => heading.level === 1,
	);
	if (h1Headings.length !== 1 || h1Headings[0].index !== 0) {
		fail(
			`${path} must contain exactly one top-level H1 title as its first line`,
		);
	}
	if (structure.unclosedFence) {
		fail(
			`${path} may not place changelog history inside an unclosed Markdown fence`,
		);
	}
	const generatedStructure =
		normalizedGenerated === normalized
			? structure
			: analyzeMarkdownStructure(normalizedGenerated);
	if (generatedStructure.unclosedFence) {
		fail(
			`${path} may not place changelog history inside an unclosed Markdown fence`,
		);
	}
	const h2Headings = generatedStructure.headings.filter(
		(heading) => heading.level === 2,
	);
	if (
		h2Headings.length !== 1 ||
		(h2Headings[0].text !== expectedVersion &&
			h2Headings[0].text !== `[${expectedVersion}]`)
	) {
		fail(
			`${path} must add exactly one new top-level ${expectedVersion} version section before existing history`,
		);
	}
	if (
		normalizedGenerated.slice(titleEnd + 1, h2Headings[0].index).trim() !== ""
	) {
		fail(
			`${path} inserts content outside its single new top-level version section`,
		);
	}
};

const assertPreservesExistingChangelog = (
	baseSource,
	headSource,
	path,
	expectedVersion,
) => {
	const normalizedBase = normalizeLineEndings(baseSource);
	const normalizedHead = normalizeLineEndings(headSource);
	const firstLineEnd = normalizedBase.indexOf("\n");
	const baseTitle =
		firstLineEnd < 0 ? normalizedBase : normalizedBase.slice(0, firstLineEnd);
	const oldReleaseHistory =
		firstLineEnd < 0 ? "" : normalizedBase.slice(firstLineEnd + 1);
	const hasOldReleaseHistory = oldReleaseHistory.trim().length > 0;
	let newPrefix = normalizedHead;
	if (hasOldReleaseHistory) {
		if (!normalizedHead.endsWith(oldReleaseHistory)) {
			fail(
				`${path} rewrites or drops existing release history instead of preserving it as the exact terminal structural tail`,
			);
		}
		newPrefix = normalizedHead.slice(0, -oldReleaseHistory.length);
		if (newPrefix.includes(oldReleaseHistory)) {
			fail(
				`${path} copies or duplicates existing release history before its exact terminal tail`,
			);
		}
	}
	assertGeneratedChangelogStructure(
		normalizedHead,
		path,
		expectedVersion,
		baseTitle,
		newPrefix,
	);
};

const validateGeneratedVersionPullRequest = async (
	repository,
	baseSha,
	headSha,
	diffEntries,
	state,
) => {
	const deletedChangesets = [];
	const versionPackages = new Map();
	const changelogs = new Set();
	let lockfileChanged = false;
	for (const entry of diffEntries) {
		const path = entry.newPath ?? entry.oldPath;
		if (
			entry.code === "R" &&
			isDirectChangesetPath(entry.oldPath) &&
			isPackageChangelogPath(entry.newPath) &&
			state.baseFiles.has(entry.oldPath) &&
			!state.headFiles.has(entry.oldPath) &&
			!state.baseFiles.has(entry.newPath) &&
			state.headFiles.has(entry.newPath)
		) {
			// Rename detection is content-based. A first generated changelog can
			// legitimately be paired with the consumed changeset that supplied its
			// release note, even though the generator performed a delete plus add.
			deletedChangesets.push(entry.oldPath);
			changelogs.add(entry.newPath);
			continue;
		}
		if (
			entry.code === "D" &&
			isDirectChangesetPath(entry.oldPath) &&
			state.baseFiles.has(entry.oldPath) &&
			!state.headFiles.has(entry.oldPath)
		) {
			deletedChangesets.push(entry.oldPath);
			continue;
		}
		if (path === "pnpm-lock.yaml" && entry.code === "M") {
			if (lockfileChanged) {
				fail(
					"Generated Version Packages PR changed pnpm-lock.yaml more than once",
				);
			}
			lockfileChanged = true;
			continue;
		}
		if (entry.code === "M" && isPackageManifestPath(path)) {
			const root = packageRootFromManifestPath(path);
			const pair = state.pairs.get(root);
			if (
				!pair ||
				pair.versionableNames.size === 0 ||
				!hasOnlyVersionManifestChange(pair.base?.manifest, pair.head?.manifest)
			) {
				fail(
					`Generated Version Packages PR changed ${path} by more than its version`,
				);
			}
			const name = pair.head.manifest.name;
			if (versionPackages.has(name)) {
				fail(`Generated Version Packages PR versions ${name} more than once`);
			}
			versionPackages.set(name, {
				root,
				baseVersion: pair.base.manifest.version,
				version: pair.head.manifest.version,
				changelog: root ? `${root}/CHANGELOG.md` : "CHANGELOG.md",
			});
			continue;
		}
		if (
			(entry.code === "M" || entry.code === "A") &&
			isPackageChangelogPath(entry.newPath)
		) {
			changelogs.add(path);
			continue;
		}
		fail(
			`Generated Version Packages PR contains non-generated change ${entry.status} ${entry.oldPath ?? ""}${entry.newPath && entry.newPath !== entry.oldPath ? ` -> ${entry.newPath}` : ""}`,
		);
	}
	const baseChangesets = [...state.baseFiles]
		.filter(isDirectChangesetPath)
		.sort();
	if (baseChangesets.length === 0 || versionPackages.size === 0) {
		fail(
			"Generated Version Packages PR must delete at least one changeset and version at least one publishable package",
		);
	}
	const deletedUnique = [...new Set(deletedChangesets)].sort();
	if (!isDeepStrictEqual(deletedUnique, baseChangesets)) {
		const missing = baseChangesets.filter(
			(path) => !deletedUnique.includes(path),
		);
		const extra = deletedUnique.filter(
			(path) => !baseChangesets.includes(path),
		);
		fail(
			"Generated Version Packages PR must consume the exact pending base changeset set" +
				(missing.length > 0 ? `; missing: ${missing.join(", ")}` : "") +
				(extra.length > 0 ? `; unexpected: ${extra.join(", ")}` : ""),
		);
	}
	const baseByName = packagesByName(state.basePackages);
	const headByName = packagesByName(state.headPackages);
	const directBumps = new Map();
	for (const path of baseChangesets) {
		const parsed = parseChangeset(
			await repository.readFile(baseSha, path),
			path,
		);
		for (const [name, bump] of parsed.releases) {
			const basePackage = baseByName.get(name);
			const headPackage = headByName.get(name);
			if (
				!basePackage ||
				!headPackage ||
				basePackage.root !== headPackage.root ||
				!isVersionableManifest(basePackage.manifest, state.ignoredNames) ||
				!isVersionableManifest(headPackage.manifest, state.ignoredNames)
			) {
				fail(
					`${path} names ${name}, which is not the same publishable, non-ignored workspace package in both trees`,
				);
			}
			directBumps.set(
				name,
				directBumps.has(name)
					? highestVersionBump(directBumps.get(name), bump)
					: bump,
			);
		}
	}
	const expectedVersionPlan = deriveExpectedVersionPlan(directBumps, state);
	const missingVersionPackages = [...expectedVersionPlan.keys()].filter(
		(name) => !versionPackages.has(name),
	);
	const extraVersionPackages = [...versionPackages.keys()].filter(
		(name) => !expectedVersionPlan.has(name),
	);
	if (missingVersionPackages.length > 0 || extraVersionPackages.length > 0) {
		fail(
			"Generated Version Packages PR package set differs from the deterministic base changeset/dependency plan" +
				(missingVersionPackages.length > 0
					? `; missing: ${missingVersionPackages.sort().join(", ")}`
					: "") +
				(extraVersionPackages.length > 0
					? `; absent from plan: ${extraVersionPackages.sort().join(", ")}`
					: ""),
		);
	}
	for (const [name, bump] of expectedVersionPlan) {
		const properties = versionPackages.get(name);
		const expectedVersion = incrementStableVersion(
			properties.baseVersion,
			bump,
			name,
		);
		if (properties.version !== expectedVersion) {
			fail(
				`${name} must bump exactly ${bump} from ${properties.baseVersion} to ${expectedVersion}, got ${properties.version}`,
			);
		}
	}
	for (const [name, properties] of versionPackages) {
		if (!changelogs.delete(properties.changelog)) {
			fail(
				`${name}@${properties.version} is missing matching ${properties.changelog}`,
			);
		}
		const headChangelog = await repository.readFile(
			headSha,
			properties.changelog,
		);
		if (state.baseFiles.has(properties.changelog)) {
			assertPreservesExistingChangelog(
				await repository.readFile(baseSha, properties.changelog),
				headChangelog,
				properties.changelog,
				properties.version,
			);
		} else {
			assertGeneratedChangelogStructure(
				headChangelog,
				properties.changelog,
				properties.version,
			);
		}
	}
	if (changelogs.size > 0) {
		fail(
			`Generated Version Packages PR changed changelogs without matching package versions: ${[...changelogs].join(", ")}`,
		);
	}
	await validateVersionLockfileChange(
		repository,
		baseSha,
		headSha,
		versionPackages,
	);
	return {
		kind: "generated-version-pr",
		packages: [...versionPackages.keys()].sort(),
		changesets: deletedUnique,
	};
};

export const validatePullRequestPayload = (payload) => {
	const pullRequest = payload?.pull_request;
	if (!pullRequest || typeof pullRequest !== "object") {
		fail("GitHub pull_request payload is missing");
	}
	const baseSha = validateFullSha(
		pullRequest.base?.sha,
		"pull request base SHA",
	);
	const headSha = validateFullSha(
		pullRequest.head?.sha,
		"pull request head SHA",
	);
	if (baseSha === headSha) {
		fail("Pull request base and head SHAs must differ");
	}
	for (const [label, value] of [
		["base repository", pullRequest.base?.repo?.full_name],
		["head repository", pullRequest.head?.repo?.full_name],
		["event repository", payload.repository?.full_name],
	]) {
		if (typeof value !== "string" || !/^[^/\s]+\/[^/\s]+$/.test(value)) {
			fail(`${label} full_name is invalid`);
		}
	}
	if (
		pullRequest.base.repo.full_name !== payload.repository.full_name ||
		pullRequest.base.ref !== "master"
	) {
		fail(
			"Changeset coverage is valid only for pull requests targeting this repository's master branch",
		);
	}
	return {
		baseSha,
		headSha,
		versionPullRequestIdentity:
			pullRequest.user?.login === "peerbit-org" &&
			pullRequest.user?.id === 273107789 &&
			payload.sender?.login === "peerbit-org" &&
			payload.sender?.id === 273107789 &&
			pullRequest.base?.ref === "master" &&
			pullRequest.head?.ref === "changeset-release/master" &&
			pullRequest.head.repo.full_name === pullRequest.base.repo.full_name &&
			payload.repository.full_name === pullRequest.base.repo.full_name,
	};
};

export const checkChangesetRequired = async ({
	baseSha,
	headSha,
	versionPullRequestIdentity = false,
	repository,
}) => {
	validateFullSha(baseSha, "base SHA");
	validateFullSha(headSha, "head SHA");
	await repository.assertCommit(baseSha);
	await repository.assertCommit(headSha);
	const mergeBase = validateFullSha(
		await repository.mergeBase(baseSha, headSha),
		"merge base",
	);
	await repository.assertCommit(mergeBase);
	if (mergeBase !== baseSha) {
		fail(
			"Pull request head must descend from the exact event base SHA; update the branch before rerunning the changeset guard",
		);
	}
	const diffEntries = await repository.diffNameStatus(baseSha, headSha);
	for (const entry of diffEntries) {
		if (!["A", "C", "D", "M", "R", "T"].includes(entry.code)) {
			fail(`Unsupported Git diff status ${entry.status}`);
		}
		if (entry.oldPath) validateRepositoryPath(entry.oldPath);
		if (entry.newPath) validateRepositoryPath(entry.newPath);
	}
	const state = await buildPackageState(repository, baseSha, headSha);
	await assertChangesetGuardPair(repository, baseSha, headSha, state);
	if (
		!versionPullRequestIdentity &&
		validateNewIgnorePolicyTransition(diffEntries, state)
	) {
		return {
			kind: "ordinary-pr",
			mergeBase,
			affected: [],
			changesets: [],
		};
	}
	assertNoUnstagedPublishablePackageRemoval(state);
	if (versionPullRequestIdentity) {
		return validateGeneratedVersionPullRequest(
			repository,
			baseSha,
			headSha,
			diffEntries,
			state,
		);
	}
	assertNoManualVersionEdits(state);
	const affected = affectedPackages(diffEntries, state);
	const changesets = await collectNewChangesets(
		repository,
		baseSha,
		headSha,
		diffEntries,
		state,
	);
	const missing = [...affected].filter(
		(name) => !changesets.coverage.has(name),
	);
	if (missing.length > 0) {
		fail(
			"Release-relevant package changes are missing a NEW changeset entry:\n- " +
				missing.sort().join("\n- "),
		);
	}
	return {
		kind: "ordinary-pr",
		mergeBase,
		affected: [...affected].sort(),
		changesets: changesets.paths,
	};
};

export const runChangesetGuardFromEnvironment = async ({
	environment = process.env,
	repositoryRoot,
	repository,
	log = console.log,
} = {}) => {
	if (
		environment.GITHUB_EVENT_NAME !== "pull_request" &&
		environment.GITHUB_EVENT_NAME !== "pull_request_target"
	) {
		log(
			"Changeset coverage guard skipped: this is not a pull_request or pull_request_target event.",
		);
		return { kind: "skipped" };
	}
	if (!environment.GITHUB_EVENT_PATH) {
		fail(
			"GITHUB_EVENT_PATH is required for pull_request or pull_request_target validation",
		);
	}
	let payload;
	try {
		payload = JSON.parse(await readFile(environment.GITHUB_EVENT_PATH, "utf8"));
	} catch (error) {
		fail(`Unable to read the GitHub pull request payload: ${error.message}`);
	}
	const event = validatePullRequestPayload(payload);
	const effectiveRepositoryRoot = resolve(
		repositoryRoot ??
			environment.CHANGESET_GUARD_REPOSITORY_ROOT ??
			resolve(dirname(fileURLToPath(import.meta.url)), "../.."),
	);
	const result = await checkChangesetRequired({
		...event,
		repository: repository ?? new GitTreeRepository(effectiveRepositoryRoot),
	});
	if (result.kind === "generated-version-pr") {
		log(
			`Changeset coverage guard accepted generated Version Packages PR for ${result.packages.join(", ")}.`,
		);
	} else if (result.affected.length === 0) {
		log(
			`Changeset coverage guard found no release-relevant package changes (${result.changesets.length} new changeset file(s) validated).`,
		);
	} else {
		log(
			`Changeset coverage guard covered ${result.affected.length} package(s): ${result.affected.join(", ")}.`,
		);
	}
	return result;
};

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : undefined;
if (invokedPath === fileURLToPath(import.meta.url)) {
	runChangesetGuardFromEnvironment().catch((error) => {
		console.error(`Changeset coverage guard failed: ${error.message}`);
		process.exitCode = 1;
	});
}
