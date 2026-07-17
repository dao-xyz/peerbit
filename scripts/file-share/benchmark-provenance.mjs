import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

const git = (root, args, { binary = false } = {}) => {
	const result = spawnSync("git", args, {
		cwd: root,
		env: process.env,
		encoding: binary ? undefined : "utf8",
		stdio: ["ignore", "pipe", "pipe"],
	});
	if (result.error) {
		throw new Error(`Could not start git in ${root}: ${result.error.message}`, {
			cause: result.error,
		});
	}
	if (result.status !== 0) {
		const stderr = binary
			? Buffer.from(result.stderr ?? []).toString("utf8")
			: result.stderr;
		throw new Error(
			`git ${args.join(" ")} failed in ${root}: ${String(stderr).trim()}`,
		);
	}
	return binary ? Buffer.from(result.stdout ?? []) : result.stdout.trim();
};

export const resolveGitCommitAt = (root, ref = "HEAD") =>
	git(root, ["rev-parse", "--verify", `${ref}^{commit}`]);

export const gitStatus = (root) =>
	git(root, ["status", "--porcelain=v1", "--untracked-files=all"]);

export const digestDirtyWorktree = async (root, status) => {
	const hash = createHash("sha256");
	hash.update("peerbit-worktree-v1\0");
	hash.update(status);
	hash.update("\0tracked-diff\0");
	hash.update(git(root, ["diff", "--binary", "HEAD"], { binary: true }));
	const untracked = git(
		root,
		["ls-files", "--others", "--exclude-standard", "-z"],
		{ binary: true },
	)
		.toString("utf8")
		.split("\0")
		.filter(Boolean)
		.sort();
	for (const relativePath of untracked) {
		const filePath = path.join(root, relativePath);
		const details = await fsp.lstat(filePath);
		hash.update("\0untracked\0");
		hash.update(relativePath);
		hash.update("\0");
		if (details.isSymbolicLink()) {
			hash.update(`symlink:${await fsp.readlink(filePath)}`);
		} else if (details.isFile()) {
			for await (const chunk of fs.createReadStream(filePath)) {
				hash.update(chunk);
			}
		} else {
			hash.update(`mode:${details.mode}:size:${details.size}`);
		}
	}
	return hash.digest("hex");
};

export const getPeerbitProvenance = async ({
	root,
	requestedRef = "worktree",
	requireClean = false,
	expectedResolvedCommit,
}) => {
	const resolvedCommit = resolveGitCommitAt(root, "HEAD");
	if (
		expectedResolvedCommit != null &&
		resolvedCommit !== expectedResolvedCommit
	) {
		throw new Error(
			`Peerbit HEAD ${resolvedCommit} does not match the requested ref commit ${expectedResolvedCommit}`,
		);
	}
	const status = gitStatus(root);
	const dirty = status.length > 0;
	if (requireClean && dirty) {
		throw new Error(
			`Matrix worktree variant is dirty and cannot be reproduced:\n${status}`,
		);
	}
	return {
		requestedRef,
		resolvedCommit,
		dirty,
		worktreeDigest: dirty ? await digestDirtyWorktree(root, status) : null,
	};
};

export const sha256FileHex = async (filePath) => {
	const hash = createHash("sha256");
	for await (const chunk of fs.createReadStream(filePath)) {
		hash.update(chunk);
	}
	return hash.digest("hex");
};

export const assertExamplesBenchmarkContract = async (examplesRoot) => {
	const requiredFiles = [
		"package.json",
		"pnpm-lock.yaml",
		path.join("packages", "file-share", "frontend", "tests", "helpers.ts"),
		path.join("packages", "file-share", "frontend", "src", "Drop.tsx"),
	];
	for (const relativePath of requiredFiles) {
		if (!fs.existsSync(path.join(examplesRoot, relativePath))) {
			throw new Error(
				`Examples source is missing benchmark contract file ${relativePath}`,
			);
		}
	}
	const helpers = await fsp.readFile(
		path.join(
			examplesRoot,
			"packages",
			"file-share",
			"frontend",
			"tests",
			"helpers.ts",
		),
		"utf8",
	);
	for (const helper of [
		"createSyntheticFileOnDisk",
		"sha256AndCrc32File",
		"armSavedViaPicker",
		"installNodeBackedMockSaveFilePicker",
		"waitForUploadComplete",
	]) {
		if (
			!new RegExp(
				`export\\s+(?:async\\s+)?(?:function|const)\\s+${helper}\\b`,
			).test(helpers)
		) {
			throw new Error(`Examples source is missing benchmark helper ${helper}`);
		}
	}
	const drop = await fsp.readFile(
		path.join(
			examplesRoot,
			"packages",
			"file-share",
			"frontend",
			"src",
			"Drop.tsx",
		),
		"utf8",
	);
	if (!drop.includes("getLightweightSnapshot")) {
		throw new Error(
			"Examples source lacks the ready-manifest benchmark snapshot contract",
		);
	}
};

export const getExamplesProvenance = async ({
	root,
	requestedRef,
	fallbackResolvedCommit,
	fallbackProvenance,
	requireClean = false,
	expectedResolvedCommit,
	expectedLockfileSha256,
}) => {
	await assertExamplesBenchmarkContract(root);
	let resolvedCommit =
		fallbackProvenance?.resolvedCommit ?? fallbackResolvedCommit;
	let dirty = fallbackProvenance?.dirty ?? false;
	let worktreeDigest = fallbackProvenance?.worktreeDigest ?? null;
	if (fs.existsSync(path.join(root, ".git"))) {
		resolvedCommit = resolveGitCommitAt(root, "HEAD");
		const status = gitStatus(root);
		dirty = status.length > 0;
		worktreeDigest = dirty ? await digestDirtyWorktree(root, status) : null;
	} else {
		// A copied template intentionally has no .git directory. Its source commit
		// must be supplied by the caller and remains part of the result envelope.
	}
	if (!/^[0-9a-f]{40}$/.test(resolvedCommit ?? "")) {
		throw new Error("Could not establish a pinned examples commit");
	}
	if (requireClean && dirty) {
		throw new Error(
			"Examples source is dirty before benchmark instrumentation; use a clean pinned checkout or record it without requireClean",
		);
	}
	if (dirty && !/^[0-9a-f]{64}$/.test(worktreeDigest ?? "")) {
		throw new Error("Dirty examples provenance is missing its worktree digest");
	}
	if (!dirty && worktreeDigest !== null) {
		throw new Error(
			"Clean examples provenance unexpectedly has a worktree digest",
		);
	}
	const lockfileSha256 = await sha256FileHex(path.join(root, "pnpm-lock.yaml"));
	if (
		expectedResolvedCommit != null &&
		resolvedCommit !== expectedResolvedCommit
	) {
		throw new Error(
			`Examples commit ${resolvedCommit} does not match pinned commit ${expectedResolvedCommit}`,
		);
	}
	if (
		expectedLockfileSha256 != null &&
		lockfileSha256 !== expectedLockfileSha256
	) {
		throw new Error(
			`Examples lockfile SHA-256 ${lockfileSha256} does not match pinned digest ${expectedLockfileSha256}`,
		);
	}
	return {
		requestedRef,
		resolvedCommit,
		lockfileSha256,
		dirty,
		worktreeDigest,
	};
};
