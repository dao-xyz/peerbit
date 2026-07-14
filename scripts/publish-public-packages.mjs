#!/usr/bin/env node

import { spawn } from "node:child_process";
import process from "node:process";
import {
	discoverPublishableWorkspacePackages,
	sortPublishablePackages,
} from "./publishable-workspace-packages.mjs";

const rootDir = process.cwd();
const pnpmCmd = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const tag = readFlag("--tag");

function readFlag(name) {
	const index = args.indexOf(name);
	return index >= 0 ? args[index + 1] : undefined;
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
			reject(
				new Error(
					`${command} ${commandArgs.join(" ")} exited with code ${code ?? "null"}`,
				),
			);
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
	const result = await capture(
		npmCmd,
		["view", `${name}@${version}`, "version"],
		rootDir,
	);
	if (result.code === 0) {
		return true;
	}
	const combinedOutput = `${result.stdout}\n${result.stderr}`;
	if (
		combinedOutput.includes("E404") ||
		combinedOutput.includes("No match found for version")
	) {
		return false;
	}
	throw new Error(
		`Failed to query npm for ${name}@${version}\n${combinedOutput}`,
	);
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

const workspacePackages = await discoverPublishableWorkspacePackages({
	repositoryRoot: rootDir,
});
const publishOrder = sortPublishablePackages(workspacePackages);

for (const pkg of publishOrder) {
	await publishPackage(pkg);
}
