import { execFileSync } from "node:child_process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const repoRoot = path.resolve(__dirname, "..", "..");

export const defaultExamplesSource = () => {
	const sibling = path.resolve(repoRoot, "..", "peerbit-examples");
	if (
		fs.existsSync(path.join(sibling, "package.json")) &&
		fs.existsSync(path.join(sibling, "pnpm-lock.yaml")) &&
		fs.existsSync(
			path.join(sibling, "packages", "file-share", "frontend", "package.json"),
		)
	) {
		return sibling;
	}
	return "https://github.com/dao-xyz/peerbit-examples.git";
};

export const defaultExamplesDest = () => {
	const preferredParent = path.resolve(repoRoot, "..", "tmp");
	const parent = fs.existsSync(preferredParent) ? preferredParent : os.tmpdir();
	return path.join(parent, "peerbit-examples-local-peerbit");
};

export const defaultFileShareLocalPackages = [
	"peerbit",
	"@peerbit/document",
	"@peerbit/shared-log",
	"@peerbit/stream",
	"@peerbit/react",
	"@peerbit/trusted-network",
	"@peerbit/vite",
];

const BOOLEAN_ARGS = new Set([
	"fresh",
	"install",
	"build-peerbit",
	"fresh-each-run",
	"isolated-examples",
	"enable-visibility-probe",
	"verbose",
	"require-clean-peerbit",
]);

export const parseArgs = (argv) => {
	const out = {};
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i];
		if (arg === "--") {
			continue;
		}
		if (!arg.startsWith("--")) {
			continue;
		}
		const key = arg.slice(2);
		if (BOOLEAN_ARGS.has(key)) {
			out[key] = true;
			continue;
		}
		const value = argv[i + 1];
		if (value == null || value.startsWith("--")) {
			throw new Error(`Missing value for ${arg}`);
		}
		out[key] = value;
		i++;
	}
	return out;
};

export const run = (command, args, options = {}) => {
	const cwd = options.cwd ?? repoRoot;
	console.log(`$ ${command} ${args.join(" ")}`);
	execFileSync(command, args, {
		cwd,
		stdio: "inherit",
		env: {
			...process.env,
			...(options.env ?? {}),
		},
	});
};

const removeNodeModulesTrees = async (root) => {
	const entries = await fsp.readdir(root, { withFileTypes: true });
	await Promise.all(
		entries.map(async (entry) => {
			const entryPath = path.join(root, entry.name);
			if (entry.name === "node_modules") {
				await fsp.rm(entryPath, { recursive: true, force: true });
				return;
			}
			if (entry.name === ".git" || !entry.isDirectory()) {
				return;
			}
			await removeNodeModulesTrees(entryPath);
		}),
	);
};

export const installPinnedExamplesDependencies = async (examplesRoot) => {
	for (const required of ["package.json", "pnpm-lock.yaml"]) {
		if (!fs.existsSync(path.join(examplesRoot, required))) {
			throw new Error(
				`Cannot install pinned examples dependencies: missing ${required} in ${examplesRoot}`,
			);
		}
	}
	await removeNodeModulesTrees(examplesRoot);
	run("pnpm", ["install", "--frozen-lockfile"], { cwd: examplesRoot });
	if (!fs.existsSync(path.join(examplesRoot, "node_modules"))) {
		throw new Error(
			`Pinned examples install did not produce ${path.join(examplesRoot, "node_modules")}`,
		);
	}
};

export const buildPeerbitPackages = (peerbitRoot, packageNames) => {
	if (!Array.isArray(packageNames) || packageNames.length === 0) {
		throw new Error("A clean Peerbit build requires at least one package");
	}
	run(
		"pnpm",
		[...packageNames.flatMap((name) => ["--filter", `${name}...`]), "build"],
		{ cwd: peerbitRoot },
	);
};

export const getFileShareConsumerRoots = (examplesRoot) => [
	path.join(examplesRoot, "packages", "file-share", "frontend"),
	path.join(examplesRoot, "packages", "file-share", "library"),
];

const walkPackageJsonFiles = async (root) => {
	const pending = [path.join(root, "packages")];
	const packageJsons = [];
	while (pending.length > 0) {
		const current = pending.pop();
		if (!current || !fs.existsSync(current)) {
			continue;
		}
		const entries = await fsp.readdir(current, { withFileTypes: true });
		for (const entry of entries) {
			if (entry.name === "node_modules" || entry.name === "dist") {
				continue;
			}
			const fullPath = path.join(current, entry.name);
			if (entry.isDirectory()) {
				pending.push(fullPath);
				continue;
			}
			if (entry.isFile() && entry.name === "package.json") {
				packageJsons.push(fullPath);
			}
		}
	}
	return packageJsons;
};

const WORKSPACE_DEP_FIELDS = [
	"dependencies",
	"optionalDependencies",
	"peerDependencies",
];

const loadWorkspacePeerbitPackages = async (peerbitRoot = repoRoot) => {
	const packageJsonFiles = await walkPackageJsonFiles(peerbitRoot);
	const workspacePackages = new Map();
	for (const packageJsonFile of packageJsonFiles) {
		const packageJson = JSON.parse(await fsp.readFile(packageJsonFile, "utf8"));
		const name = packageJson?.name;
		if (
			name === "peerbit" ||
			(typeof name === "string" && name.startsWith("@peerbit/"))
		) {
			workspacePackages.set(name, {
				dir: path.dirname(packageJsonFile),
				packageJson,
			});
		}
	}
	return workspacePackages;
};

const isPublishablePeerbitPackage = (info) =>
	info?.packageJson?.private !== true;

const expandWorkspacePackageSelection = ({
	workspacePackages,
	selectedNames,
	includeTransitive = true,
}) => {
	const effectiveSelectedNames =
		selectedNames ??
		new Set(
			[...workspacePackages.entries()]
				.filter(([, info]) => isPublishablePeerbitPackage(info))
				.map(([name]) => name),
		);
	if (effectiveSelectedNames.size === 0) {
		return new Set();
	}
	if (!includeTransitive) {
		return new Set(
			[...effectiveSelectedNames].filter((name) => workspacePackages.has(name)),
		);
	}
	const expanded = new Set();
	const pending = [...effectiveSelectedNames];
	while (pending.length > 0) {
		const name = pending.pop();
		if (!name || expanded.has(name)) {
			continue;
		}
		const info = workspacePackages.get(name);
		if (!info) {
			continue;
		}
		expanded.add(name);
		for (const field of WORKSPACE_DEP_FIELDS) {
			for (const depName of Object.keys(info.packageJson?.[field] ?? {})) {
				const dependency = workspacePackages.get(depName);
				if (
					(depName === "peerbit" || depName.startsWith("@peerbit/")) &&
					dependency &&
					!expanded.has(depName)
				) {
					if (!isPublishablePeerbitPackage(dependency)) {
						throw new Error(
							`Local Peerbit package ${name} depends on private workspace package ${depName}, which is not publishable or linkable`,
						);
					}
					pending.push(depName);
				}
			}
		}
	}
	return expanded;
};

export const collectLocalPeerbitPackages = async (
	peerbitRoot = repoRoot,
	options = {},
) => {
	const workspacePackages = await loadWorkspacePeerbitPackages(peerbitRoot);
	const mappings = new Map();
	const normalizedNames = Array.isArray(options.names)
		? options.names.map((name) => {
				if (typeof name !== "string" || name.trim().length === 0) {
					throw new Error("Local package names must be non-empty strings");
				}
				return name.trim();
			})
		: undefined;
	if (
		normalizedNames &&
		new Set(normalizedNames).size !== normalizedNames.length
	) {
		throw new Error("Duplicate local Peerbit package names are not allowed");
	}
	const selectedNames = normalizedNames ? new Set(normalizedNames) : undefined;
	if (selectedNames) {
		const unknownNames = [...selectedNames]
			.filter((name) => !workspacePackages.has(name))
			.sort((left, right) => left.localeCompare(right));
		if (unknownNames.length > 0) {
			throw new Error(
				`Unknown local Peerbit package${unknownNames.length === 1 ? "" : "s"}: ${unknownNames.join(", ")}`,
			);
		}
		const privateNames = [...selectedNames]
			.filter(
				(name) => workspacePackages.get(name)?.packageJson?.private === true,
			)
			.sort((left, right) => left.localeCompare(right));
		if (privateNames.length > 0) {
			throw new Error(
				`Private local Peerbit package${privateNames.length === 1 ? " is" : "s are"} not publishable or linkable: ${privateNames.join(", ")}`,
			);
		}
	}
	const expandedSelection = expandWorkspacePackageSelection({
		workspacePackages,
		selectedNames,
		includeTransitive: options.includeTransitive !== false,
	});
	for (const [name, info] of workspacePackages.entries()) {
		if (!expandedSelection.has(name)) {
			continue;
		}
		mappings.set(name, info.dir);
	}
	return new Map(
		[...mappings.entries()].sort(([a], [b]) => a.localeCompare(b)),
	);
};

const BETTER_SQLITE3_PREFLIGHT_SCRIPT = String.raw`
const { createRequire } = require("node:module");
const packageJsonPath = process.argv[1];
const requireFromPackage = createRequire(packageJsonPath);
const resolvedPath = requireFromPackage.resolve("better-sqlite3");
const Database = requireFromPackage("better-sqlite3");
let database;
try {
	database = new Database(":memory:");
	const row = database.prepare("SELECT 1 AS ok").get();
	if (row?.ok !== 1) {
		throw new Error("in-memory query returned an unexpected result");
	}
} finally {
	database?.close();
}
process.stdout.write(JSON.stringify({
	node: process.version,
	modules: process.versions.modules,
	napi: process.versions.napi,
	resolvedPath,
}));
`;

const childProcessOutput = (value) => {
	if (typeof value === "string") {
		return value.trim();
	}
	if (Buffer.isBuffer(value)) {
		return value.toString("utf8").trim();
	}
	return "";
};

export const preflightBetterSqlite3Runtime = async (
	peerbitRoot = repoRoot,
	{ packageNames } = {},
) => {
	if (
		Array.isArray(packageNames) &&
		!packageNames.includes("@peerbit/indexer-sqlite3")
	) {
		return null;
	}
	const workspacePackages = await loadWorkspacePeerbitPackages(peerbitRoot);
	const sqliteIndexer = workspacePackages.get("@peerbit/indexer-sqlite3");
	if (!sqliteIndexer) {
		throw new Error(
			`better-sqlite3 runtime preflight could not find @peerbit/indexer-sqlite3 under ${peerbitRoot}`,
		);
	}
	const packageJsonPath = path.join(sqliteIndexer.dir, "package.json");
	try {
		const output = execFileSync(
			process.execPath,
			["--eval", BETTER_SQLITE3_PREFLIGHT_SCRIPT, packageJsonPath],
			{
				cwd: sqliteIndexer.dir,
				encoding: "utf8",
				stdio: ["ignore", "pipe", "pipe"],
			},
		);
		const evidence = JSON.parse(output);
		if (
			evidence.node !== process.version ||
			evidence.modules !== process.versions.modules ||
			typeof evidence.resolvedPath !== "string" ||
			evidence.resolvedPath.length === 0
		) {
			throw new Error("child runtime evidence was incomplete or inconsistent");
		}
		return evidence;
	} catch (error) {
		const detail =
			childProcessOutput(error?.stderr) ||
			childProcessOutput(error?.stdout) ||
			String(error?.message ?? error);
		const failure = new Error(
			`better-sqlite3 runtime preflight failed for ${process.version} (NODE_MODULE_VERSION ${process.versions.modules ?? "unknown"}) in ${sqliteIndexer.dir}. Reinstall the Peerbit workspace with this Node runtime before building. ${detail}`,
			{ cause: error },
		);
		failure.stopBenchmarkPlan = true;
		throw failure;
	}
};

export const cleanPeerbitBuildArtifacts = async ({
	peerbitRoot = repoRoot,
	packageNames,
}) => {
	const localPackages = await collectLocalPeerbitPackages(peerbitRoot, {
		names: packageNames,
	});
	await Promise.all(
		[...localPackages.values()].map((packageDir) =>
			fsp.rm(path.join(packageDir, "dist"), {
				recursive: true,
				force: true,
			}),
		),
	);
	return localPackages;
};

export const ensureExamplesAssetPackageLinks = async ({
	examplesRoot,
	peerbitRoot = repoRoot,
	packageNames,
	consumerRoots = [],
}) => {
	const localPackages = await collectLocalPeerbitPackages(peerbitRoot, {
		names: packageNames,
	});
	for (const [packageName, packageDir] of localPackages) {
		const builtDistPath = path.join(packageDir, "dist");
		const builtDist = await fsp.stat(builtDistPath).catch(() => undefined);
		if (!builtDist?.isDirectory()) {
			throw new Error(
				`Cannot link ${packageName}: the mandatory clean build did not produce ${builtDistPath}`,
			);
		}
	}

	const examplesRealPath = await fsp.realpath(examplesRoot);
	const linkRoots = [examplesRoot, ...consumerRoots].map((root) =>
		path.resolve(root),
	);
	for (const consumerRoot of linkRoots.slice(1)) {
		const consumerRealPath = await fsp.realpath(consumerRoot);
		const relative = path.relative(examplesRealPath, consumerRealPath);
		if (relative === ".." || relative.startsWith(`..${path.sep}`)) {
			throw new Error(
				`Refusing to install benchmark package links outside ${examplesRealPath}: ${consumerRealPath}`,
			);
		}
	}

	for (const linkRoot of [...new Set(linkRoots)]) {
		const nodeModulesDir = path.join(linkRoot, "node_modules");
		const scopedDir = path.join(nodeModulesDir, "@peerbit");
		await fsp.mkdir(scopedDir, { recursive: true });
		for (const [packageName, packageDir] of localPackages) {
			const linkPath =
				packageName === "peerbit"
					? path.join(nodeModulesDir, "peerbit")
					: path.join(scopedDir, packageName.split("/")[1]);
			const existing = await fsp.lstat(linkPath).catch(() => undefined);
			if (existing) {
				await fsp.rm(linkPath, {
					recursive: true,
					force: true,
				});
			}
			await fsp.symlink(packageDir, linkPath, "dir");
			const [linkedRealPath, packageRealPath] = await Promise.all([
				fsp.realpath(linkPath),
				fsp.realpath(packageDir),
			]);
			if (linkedRealPath !== packageRealPath) {
				throw new Error(
					`Linked package ${packageName} resolved to ${linkedRealPath}, not ${packageRealPath}`,
				);
			}
		}
	}
	return localPackages;
};

const copyExamplesTemplate = async ({ template, dest }) => {
	await fsp.mkdir(path.dirname(dest), { recursive: true });
	try {
		execFileSync("cp", ["-cR", template, dest], {
			stdio: "inherit",
		});
		execFileSync(
			"find",
			[
				dest,
				"(",
				"-name",
				".git",
				"-o",
				"-name",
				"node_modules",
				"-o",
				"-name",
				"test-results",
				"-o",
				"-name",
				"playwright-report",
				"-o",
				"-name",
				".playwright-artifacts-0",
				")",
				"-prune",
				"-exec",
				"rm",
				"-rf",
				"{}",
				"+",
			],
			{ stdio: "inherit" },
		);
		return;
	} catch {
		// Fall back to a filtered recursive copy on platforms without clonefile support.
		await fsp.rm(dest, { recursive: true, force: true });
	}
	await fsp.cp(template, dest, {
		recursive: true,
		filter: (source) => {
			const relative = path.relative(template, source);
			if (!relative || relative === "") {
				return true;
			}
			const segments = relative.split(path.sep);
			return !segments.some((segment) =>
				[
					".git",
					"node_modules",
					"test-results",
					"playwright-report",
					".playwright-artifacts-0",
				].includes(segment),
			);
		},
	});
};

const resolveCommit = (root, ref) =>
	execFileSync("git", ["rev-parse", "--verify", `${ref}^{commit}`], {
		cwd: root,
		encoding: "utf8",
		stdio: ["ignore", "pipe", "pipe"],
	}).trim();

const cloneExamplesRepo = async ({ source, dest, fresh, ref = "HEAD" }) => {
	if (fresh && fs.existsSync(dest)) {
		await fsp.rm(dest, { recursive: true, force: true });
	}
	if (fs.existsSync(dest)) {
		if (fs.existsSync(path.join(dest, ".git"))) {
			const currentCommit = resolveCommit(dest, "HEAD");
			const requestedCommit = resolveCommit(dest, ref);
			if (currentCommit !== requestedCommit) {
				throw new Error(
					`Existing examples checkout is ${currentCommit}, not requested ${ref} (${requestedCommit}); rerun with --fresh`,
				);
			}
		}
		return;
	}
	await fsp.mkdir(path.dirname(dest), { recursive: true });
	run("git", ["clone", "--no-checkout", source, dest]);
	const commit = resolveCommit(dest, ref);
	run("git", ["checkout", "--detach", commit], { cwd: dest });
};

export const prepareExamplesRepo = async ({
	source = defaultExamplesSource(),
	template,
	dest = defaultExamplesDest(),
	peerbitRoot = repoRoot,
	fresh = false,
	install = false,
	localPackageNames,
	applyOverrides = true,
	ref = "HEAD",
} = {}) => {
	if (fresh && fs.existsSync(dest)) {
		await fsp.rm(dest, { recursive: true, force: true });
	}
	if (!fs.existsSync(dest)) {
		if (template) {
			await copyExamplesTemplate({ template, dest });
		} else {
			await cloneExamplesRepo({ source, dest, fresh: false, ref });
		}
	} else if (template) {
		throw new Error(
			"A template-backed examples checkout must be recreated with --fresh so its pinned source cannot be confused with stale files",
		);
	} else {
		await cloneExamplesRepo({ source, dest, fresh: false, ref });
	}

	const allLocalPackages = await collectLocalPeerbitPackages(peerbitRoot);
	const localPackages = Array.isArray(localPackageNames)
		? await collectLocalPeerbitPackages(peerbitRoot, {
				names: localPackageNames,
			})
		: allLocalPackages;
	if (applyOverrides) {
		const packageJsonPath = path.join(dest, "package.json");
		const packageJson = JSON.parse(await fsp.readFile(packageJsonPath, "utf8"));
		const currentOverrides = packageJson?.pnpm?.overrides ?? {};
		const nextOverrides = { ...currentOverrides };
		for (const name of allLocalPackages.keys()) {
			delete nextOverrides[name];
		}
		for (const [name, packageDir] of localPackages) {
			nextOverrides[name] = `link:${packageDir}`;
		}
		packageJson.pnpm = packageJson.pnpm ?? {};
		packageJson.pnpm.overrides = Object.fromEntries(
			Object.entries(nextOverrides).sort(([a], [b]) => a.localeCompare(b)),
		);
		await fsp.writeFile(
			packageJsonPath,
			`${JSON.stringify(packageJson, null, 4)}\n`,
		);
		const patchesDir = path.join(dest, "patches");
		if (fs.existsSync(patchesDir)) {
			for (const entry of await fsp.readdir(patchesDir)) {
				if (/^@peerbit\+shared-log\+.*\.patch$/.test(entry)) {
					await fsp.rm(path.join(patchesDir, entry), { force: true });
				}
			}
		}
	}

	const metadataPath = path.join(dest, ".peerbit-local-overrides.json");
	await fsp.writeFile(
		metadataPath,
		`${JSON.stringify(
			{
				peerbitRoot,
				source,
				template,
				ref,
				packages: Object.fromEntries(localPackages),
			},
			null,
			4,
		)}\n`,
	);

	if (install) {
		if (applyOverrides) {
			run("pnpm", ["install"], { cwd: dest });
		} else {
			await installPinnedExamplesDependencies(dest);
		}
	}

	return {
		dest,
		source,
		peerbitRoot,
		localPackages,
	};
};

export const copyTemplate = async ({
	templatePath,
	outputPath,
	replacements = {},
}) => {
	let contents = await fsp.readFile(templatePath, "utf8");
	for (const [pattern, replacement] of Object.entries(replacements)) {
		contents = contents.replaceAll(pattern, replacement);
	}
	await fsp.mkdir(path.dirname(outputPath), { recursive: true });
	await fsp.writeFile(outputPath, contents);
	return outputPath;
};
