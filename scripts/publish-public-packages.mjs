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
// npm can accept a publish and keep returning 404 while the package version is
// still being processed. Processing has been observed to take more than twenty
// minutes (2026-09-30, 2026-10-01). Versions of packages that already exist on
// npm are therefore published first and verified together afterwards, within
// one shared window, instead of blocking the next publish on each one. A
// brand-new package is still verified before anything after it is published.
const REGISTRY_VERIFICATION_TIMEOUT_MS = readDurationEnv(
	"PUBLISH_VERIFY_TIMEOUT_MS",
	3_600_000,
);
const REGISTRY_VERIFICATION_POLL_MS = readDurationEnv(
	"PUBLISH_VERIFY_POLL_MS",
	30_000,
);

function readDurationEnv(name, fallback) {
	const raw = process.env[name];
	if (raw === undefined || raw.trim() === "") {
		return fallback;
	}
	const value = Number(raw);
	if (!Number.isInteger(value) || value <= 0 || value > 2_147_483_647) {
		throw new Error(`${name} must be a positive number of milliseconds`);
	}
	return value;
}

// A re-run while npm is still processing an earlier upload of the same version
// is rejected as a publish over an existing version. That upload was accepted,
// so the version is verified with the rest instead of failing the release.
const PUBLISH_CONFLICT_PATTERN =
	/cannot publish over the previously published version|EPUBLISHCONFLICT/i;

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const tag = readFlag("--tag");

function readFlag(name) {
	const index = args.indexOf(name);
	return index >= 0 ? args[index + 1] : undefined;
}

function runTee(command, commandArgs, cwd) {
	return new Promise((resolve) => {
		const child = spawn(command, commandArgs, {
			cwd,
			env: process.env,
			// stdin stays interactive so a manual release can answer npm's OTP prompt.
			stdio: ["inherit", "pipe", "pipe"],
		});
		let output = "";
		child.stdout.on("data", (chunk) => {
			output += chunk.toString();
			process.stdout.write(chunk);
		});
		child.stderr.on("data", (chunk) => {
			output += chunk.toString();
			process.stderr.write(chunk);
		});
		// "close" fires after both output streams have ended, unlike "exit".
		child.on("close", (code) => {
			resolve({ code: code ?? 1, output });
		});
		child.on("error", (error) => {
			resolve({ code: 1, output: `${output}\n${String(error)}` });
		});
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
		child.on("close", (code) => {
			resolve({ code: code ?? 1, stdout, stderr });
		});
		child.on("error", (error) => {
			resolve({ code: 1, stdout, stderr: `${stderr}\n${String(error)}` });
		});
	});
}

function isMissingOnRegistry(result) {
	const combinedOutput = `${result.stdout}\n${result.stderr}`;
	return (
		combinedOutput.includes("E404") ||
		combinedOutput.includes("No match found for version")
	);
}

async function isNewPackage({ name }) {
	const result = await capture(npmCmd, ["view", name, "name"], rootDir);
	if (result.code === 0) {
		return false;
	}
	if (isMissingOnRegistry(result)) {
		return true;
	}
	throw new Error(
		`Failed to query npm for ${name}\n${result.stdout}\n${result.stderr}`,
	);
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
	if (isMissingOnRegistry(result)) {
		return false;
	}
	throw new Error(
		`Failed to query npm for ${name}@${version}\n${result.stdout}\n${result.stderr}`,
	);
}

async function verifyPublished(packages) {
	// `pnpm publish` can exit 0 without the version actually landing on the
	// registry — most notably the FIRST publish of a brand-new scoped package
	// when the npm token / org lacks permission to create it. A silent
	// non-publish must fail the release loudly, not leave a green run that
	// shipped nothing. Re-query until every version is visible or the shared
	// window for registry processing and propagation runs out. Unexpected
	// registry errors are retried within the same window, since everything has
	// already been handed to npm at this point.
	let pending = packages;
	let lastError;
	const deadline = Date.now() + REGISTRY_VERIFICATION_TIMEOUT_MS;
	for (;;) {
		const stillPending = [];
		for (const pkg of pending) {
			try {
				if (await isPublished(pkg)) {
					continue;
				}
			} catch (error) {
				lastError = error;
			}
			stillPending.push(pkg);
		}
		pending = stillPending;
		if (pending.length === 0) {
			return;
		}
		const remainingMs = deadline - Date.now();
		if (remainingMs <= 0) {
			break;
		}
		console.log(
			`waiting for the registry to show ${pending.length} published version(s): ` +
				pending.map((pkg) => `${pkg.name}@${pkg.version}`).join(", "),
		);
		await new Promise((r) =>
			setTimeout(r, Math.min(REGISTRY_VERIFICATION_POLL_MS, remainingMs)),
		);
	}
	const conflictedPending = pending.filter((pkg) => conflicted.has(pkg));
	throw new Error(
		`${pending.map((pkg) => `${pkg.name}@${pkg.version}`).join(", ")}: publish was accepted but the version never appeared on the registry. ` +
			`For a brand-new package this usually means the npm token/org cannot create it — check the token scope ` +
			`(needs @peerbit scope-level publish, not just per-package access) and the org's new-package permissions.` +
			(conflictedPending.length > 0
				? ` npm rejected ${conflictedPending.map((pkg) => `${pkg.name}@${pkg.version}`).join(", ")} as already published; ` +
					`if that version was unpublished or deleted earlier, npm will never accept it again and it needs a new version.`
				: "") +
			(lastError ? `\nLast registry error: ${lastError.message}` : ""),
	);
}

const conflicted = new Set();

/**
 * Returns undefined when nothing was handed to the registry, otherwise whether
 * the package is brand new on npm (checked before publishing; afterwards a
 * still-processing upload would make it ambiguous).
 */
async function publishPackage(pkg) {
	const alreadyPublished = await isPublished(pkg);
	if (alreadyPublished) {
		console.log(`skip ${pkg.name}@${pkg.version} (already published)`);
		return undefined;
	}
	const brandNew = !dryRun && (await isNewPackage(pkg));

	const publishArgs = ["publish", "--no-git-checks", "--access", "public"];
	if (dryRun) {
		publishArgs.push("--dry-run");
	}
	if (tag) {
		publishArgs.push("--tag", tag);
	}
	console.log(`publish ${pkg.name}@${pkg.version}`);
	const result = await runTee(pnpmCmd, publishArgs, pkg.dir);
	if (result.code !== 0) {
		if (!dryRun && PUBLISH_CONFLICT_PATTERN.test(result.output)) {
			console.log(
				`${pkg.name}@${pkg.version} was already accepted by the registry and is still processing`,
			);
			conflicted.add(pkg);
			return { brandNew };
		}
		throw new Error(
			`${pnpmCmd} ${publishArgs.join(" ")} exited with code ${result.code}`,
		);
	}
	return dryRun ? undefined : { brandNew };
}

const workspacePackages = await discoverPublishableWorkspacePackages({
	repositoryRoot: rootDir,
});
const publishOrder = sortPublishablePackages(workspacePackages);

const published = [];
for (const pkg of publishOrder) {
	const handed = await publishPackage(pkg);
	if (!handed) {
		continue;
	}
	if (handed.brandNew) {
		// A brand-new package that silently fails to land must stop the release
		// before any package depending on it is published.
		await verifyPublished([pkg]);
	} else {
		published.push(pkg);
	}
}
await verifyPublished(published);
