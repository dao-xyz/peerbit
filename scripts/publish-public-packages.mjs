#!/usr/bin/env node

import { spawn } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import process from "node:process";

const rootDir = process.cwd();
const packagesDir = path.join(rootDir, "packages");
const pnpmCmd = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const tag = readFlag("--tag");

function readFlag(name) {
	const index = args.indexOf(name);
	return index >= 0 ? args[index + 1] : undefined;
}

async function findPackageJsonFiles(directory) {
	const entries = await fs.readdir(directory, { withFileTypes: true });
	const results = [];
	for (const entry of entries) {
		if (entry.name === "node_modules" || entry.name === "dist") {
			continue;
		}
		const absolutePath = path.join(directory, entry.name);
		if (entry.isDirectory()) {
			results.push(...(await findPackageJsonFiles(absolutePath)));
		} else if (entry.isFile() && entry.name === "package.json") {
			results.push(absolutePath);
		}
	}
	return results;
}

async function loadWorkspacePackages() {
	const packageJsonFiles = await findPackageJsonFiles(packagesDir);
	const packages = [];
	for (const packageJsonFile of packageJsonFiles) {
		const manifest = JSON.parse(await fs.readFile(packageJsonFile, "utf8"));
		if (!manifest.name || manifest.private) {
			continue;
		}
		packages.push({
			dir: path.dirname(packageJsonFile),
			name: manifest.name,
			version: manifest.version,
			manifest,
		});
	}
	return packages;
}

function getInternalDependencies(manifest, packageNames) {
	const dependencySets = [
		manifest.dependencies,
		manifest.optionalDependencies,
		manifest.peerDependencies,
	];
	const internal = new Set();
	for (const dependencySet of dependencySets) {
		if (!dependencySet) {
			continue;
		}
		for (const name of Object.keys(dependencySet)) {
			if (packageNames.has(name)) {
				internal.add(name);
			}
		}
	}
	return internal;
}

function sortTopologically(packages) {
	const byName = new Map(packages.map((pkg) => [pkg.name, pkg]));
	const packageNames = new Set(byName.keys());
	const dependencies = new Map();
	const reverseEdges = new Map();
	const indegree = new Map();

	for (const pkg of packages) {
		const internalDependencies = getInternalDependencies(pkg.manifest, packageNames);
		dependencies.set(pkg.name, internalDependencies);
		indegree.set(pkg.name, internalDependencies.size);
		for (const dependency of internalDependencies) {
			const dependents = reverseEdges.get(dependency) ?? new Set();
			dependents.add(pkg.name);
			reverseEdges.set(dependency, dependents);
		}
	}

	const queue = packages
		.filter((pkg) => (indegree.get(pkg.name) ?? 0) === 0)
		.map((pkg) => pkg.name)
		.sort();
	const ordered = [];

	while (queue.length > 0) {
		const name = queue.shift();
		ordered.push(byName.get(name));
		for (const dependent of reverseEdges.get(name) ?? []) {
			const nextInDegree = (indegree.get(dependent) ?? 0) - 1;
			indegree.set(dependent, nextInDegree);
			if (nextInDegree === 0) {
				queue.push(dependent);
				queue.sort();
			}
		}
	}

	if (ordered.length !== packages.length) {
		throw new Error("Unable to topologically sort publishable workspace packages");
	}

	return ordered;
}

function run(command, commandArgs, cwd) {
	return new Promise((resolve, reject) => {
		const child = spawn(command, commandArgs, {
			cwd,
			env: process.env,
			stdio: "inherit",
		});
		child.on("exit", (code) => {
			if (code === 0) {
				resolve();
				return;
			}
			reject(new Error(`${command} ${commandArgs.join(" ")} exited with code ${code ?? "null"}`));
		});
		child.on("error", reject);
	});
}

function capture(command, commandArgs, cwd) {
	return new Promise((resolve) => {
		const child = spawn(command, commandArgs, {
			cwd,
			env: process.env,
			stdio: ["ignore", "pipe", "pipe"],
		});
		let stdout = "";
		let stderr = "";
		child.stdout.on("data", (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr.on("data", (chunk) => {
			stderr += chunk.toString();
		});
		child.on("exit", (code) => {
			resolve({ code: code ?? 1, stdout, stderr });
		});
		child.on("error", (error) => {
			resolve({ code: 1, stdout, stderr: `${stderr}\n${String(error)}` });
		});
	});
}

async function isPublished({ name, version }) {
	const result = await capture(npmCmd, ["view", `${name}@${version}`, "version"], rootDir);
	if (result.code === 0) {
		return true;
	}
	const combinedOutput = `${result.stdout}\n${result.stderr}`;
	if (combinedOutput.includes("E404") || combinedOutput.includes("No match found for version")) {
		return false;
	}
	throw new Error(`Failed to query npm for ${name}@${version}\n${combinedOutput}`);
}

async function verifyPublished(pkg) {
	// `pnpm publish` can exit 0 without the version actually landing on the
	// registry — most notably the FIRST publish of a brand-new scoped package
	// when the npm token / org lacks permission to create it. A silent
	// non-publish must fail the release loudly, not leave a green run that
	// shipped nothing. Re-query with a short backoff to tolerate registry
	// propagation delay.
	const delaysMs = [0, 2000, 4000, 8000, 15000];
	for (const delay of delaysMs) {
		if (delay) {
			await new Promise((r) => setTimeout(r, delay));
		}
		if (await isPublished(pkg)) {
			return;
		}
	}
	throw new Error(
		`${pkg.name}@${pkg.version}: publish command exited 0 but the version never appeared on the registry. ` +
			`For a brand-new package this usually means the npm token/org cannot create it — check the token scope ` +
			`(needs @peerbit scope-level publish, not just per-package access) and the org's new-package permissions.`,
	);
}

async function publishPackage(pkg) {
	const alreadyPublished = await isPublished(pkg);
	if (alreadyPublished) {
		console.log(`skip ${pkg.name}@${pkg.version} (already published)`);
		return;
	}

	const publishArgs = ["publish", "--no-git-checks", "--access", "public"];
	if (dryRun) {
		publishArgs.push("--dry-run");
	}
	if (tag) {
		publishArgs.push("--tag", tag);
	}
	console.log(`publish ${pkg.name}@${pkg.version}`);
	await run(pnpmCmd, publishArgs, pkg.dir);
	if (!dryRun) {
		await verifyPublished(pkg);
	}
}

const workspacePackages = await loadWorkspacePackages();
const publishOrder = sortTopologically(workspacePackages);

for (const pkg of publishOrder) {
	await publishPackage(pkg);
}
