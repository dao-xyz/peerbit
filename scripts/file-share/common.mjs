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
	if (fs.existsSync(sibling)) {
		return sibling;
	}
	return "https://github.com/dao-xyz/peerbit-examples.git";
};

export const defaultExamplesDest = () => {
	const preferredParent = path.resolve(repoRoot, "..", "tmp");
	const parent = fs.existsSync(preferredParent) ? preferredParent : os.tmpdir();
	return path.join(parent, "peerbit-examples-local-peerbit");
};

const BOOLEAN_ARGS = new Set([
	"fresh",
	"install",
	"build-peerbit",
	"fresh-each-run",
	"isolated-examples",
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

const expandWorkspacePackageSelection = ({
	workspacePackages,
	selectedNames,
	includeTransitive = true,
}) => {
	if (!selectedNames || selectedNames.size === 0) {
		return new Set(workspacePackages.keys());
	}
	if (!includeTransitive) {
		return new Set(
			[...selectedNames].filter((name) => workspacePackages.has(name)),
		);
	}
	const expanded = new Set();
	const pending = [...selectedNames];
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
				if (
					(depName === "peerbit" || depName.startsWith("@peerbit/")) &&
					workspacePackages.has(depName) &&
					!expanded.has(depName)
				) {
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
	const selectedNames = Array.isArray(options.names)
		? new Set(options.names)
		: undefined;
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
	return new Map([...mappings.entries()].sort(([a], [b]) => a.localeCompare(b)));
};

const getInstalledPackagePath = ({ root, packageName }) => {
	if (packageName === "peerbit") {
		return path.join(root, "node_modules", "peerbit");
	}
	const [scope, name] = packageName.split("/");
	if (!scope || !name) {
		throw new Error(`Unsupported package name "${packageName}"`);
	}
	return path.join(root, "node_modules", scope, name);
};

export const ensureExamplesAssetPackageLinks = async ({
	examplesRoot,
	peerbitRoot = repoRoot,
	packageNames,
}) => {
	const localPackages = await collectLocalPeerbitPackages(peerbitRoot, {
		names: packageNames,
	});
	const nodeModulesDir = path.join(examplesRoot, "node_modules");
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
	}
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
					"test-results",
					"playwright-report",
					".playwright-artifacts-0",
				].includes(segment),
			);
		},
	});
};

const cloneExamplesRepo = async ({ source, dest, fresh }) => {
	if (fresh && fs.existsSync(dest)) {
		await fsp.rm(dest, { recursive: true, force: true });
	}
	if (fs.existsSync(dest)) {
		return;
	}
	await fsp.mkdir(path.dirname(dest), { recursive: true });
	run("git", ["clone", "--depth", "1", source, dest]);
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
} = {}) => {
	if (fresh && fs.existsSync(dest)) {
		await fsp.rm(dest, { recursive: true, force: true });
	}
	if (!fs.existsSync(dest)) {
		if (template) {
			await copyExamplesTemplate({ template, dest });
		} else {
			await cloneExamplesRepo({ source, dest, fresh: false });
		}
	}

	const allLocalPackages = await collectLocalPeerbitPackages(peerbitRoot);
	const localPackages = Array.isArray(localPackageNames)
		? await collectLocalPeerbitPackages(peerbitRoot, {
				names: localPackageNames,
			})
		: allLocalPackages;
	const packageJsonPath = path.join(dest, "package.json");
	const packageJson = JSON.parse(await fsp.readFile(packageJsonPath, "utf8"));
	const currentOverrides = packageJson?.pnpm?.overrides ?? {};
	const nextOverrides = { ...currentOverrides };
	for (const name of allLocalPackages.keys()) {
		delete nextOverrides[name];
	}
	if (applyOverrides) {
		for (const [name, packageDir] of localPackages) {
			nextOverrides[name] = `link:${packageDir}`;
		}
	}
	packageJson.pnpm = packageJson.pnpm ?? {};
	packageJson.pnpm.overrides = Object.fromEntries(
		Object.entries(nextOverrides).sort(([a], [b]) => a.localeCompare(b)),
	);
	await fsp.writeFile(packageJsonPath, `${JSON.stringify(packageJson, null, 4)}\n`);
	const patchesDir = path.join(dest, "patches");
	if (fs.existsSync(patchesDir)) {
		for (const entry of await fsp.readdir(patchesDir)) {
			if (/^@peerbit\+shared-log\+.*\.patch$/.test(entry)) {
				await fsp.rm(path.join(patchesDir, entry), { force: true });
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
				packages: Object.fromEntries(localPackages),
			},
			null,
			4,
		)}\n`,
	);

	if (install) {
		run("pnpm", ["install"], { cwd: dest });
	}

	return {
		dest,
		source,
		peerbitRoot,
		localPackages,
	};
};

export const overlayInstalledPackages = async ({
	examplesRoot,
	peerbitRoot = repoRoot,
	packageNames,
}) => {
	const localPackages = await collectLocalPeerbitPackages(peerbitRoot, {
		names: packageNames,
	});
	for (const [packageName, packageDir] of localPackages) {
		const installedPackagePath = getInstalledPackagePath({
			root: examplesRoot,
			packageName,
		});
		if (!fs.existsSync(installedPackagePath)) {
			throw new Error(
				`Cannot overlay ${packageName}: missing installed package at ${installedPackagePath}`,
			);
		}
		const installedRealPath = await fsp.realpath(installedPackagePath);
		const packageRealPath = await fsp.realpath(packageDir);
		if (installedRealPath === packageRealPath) {
			continue;
		}
		for (const entry of ["dist", "src"]) {
			const sourcePath = path.join(packageDir, entry);
			if (!fs.existsSync(sourcePath)) {
				continue;
			}
			const destPath = path.join(installedPackagePath, entry);
			await fsp.rm(destPath, { recursive: true, force: true });
			await fsp.cp(sourcePath, destPath, { recursive: true });
		}
	}
};

export const copyTemplate = async ({ templatePath, outputPath, replacements = {} }) => {
	let contents = await fsp.readFile(templatePath, "utf8");
	for (const [pattern, replacement] of Object.entries(replacements)) {
		contents = contents.replaceAll(pattern, replacement);
	}
	await fsp.mkdir(path.dirname(outputPath), { recursive: true });
	await fsp.writeFile(outputPath, contents);
	return outputPath;
};
