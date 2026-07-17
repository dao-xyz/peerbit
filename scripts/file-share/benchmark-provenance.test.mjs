import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	getExamplesProvenance,
	getPeerbitProvenance,
	sha256FileHex,
} from "./benchmark-provenance.mjs";

const git = (root, ...args) =>
	execFileSync("git", args, {
		cwd: root,
		stdio: ["ignore", "pipe", "pipe"],
		encoding: "utf8",
	}).trim();

const createExamplesRepository = async () => {
	const root = await mkdtemp(path.join(os.tmpdir(), "peerbit-provenance-"));
	const frontend = path.join(root, "packages", "file-share", "frontend");
	await mkdir(path.join(frontend, "tests"), { recursive: true });
	await mkdir(path.join(frontend, "src"), { recursive: true });
	await writeFile(path.join(root, "package.json"), '{"name":"fixture"}\n');
	await writeFile(
		path.join(root, "pnpm-lock.yaml"),
		"lockfileVersion: '9.0'\n",
	);
	await writeFile(
		path.join(frontend, "tests", "helpers.ts"),
		[
			"export const createSyntheticFileOnDisk = () => {};",
			"export const sha256AndCrc32File = () => {};",
			"export const armSavedViaPicker = () => {};",
			"export const installNodeBackedMockSaveFilePicker = () => {};",
			"export const waitForUploadComplete = () => {};",
			"",
		].join("\n"),
	);
	await writeFile(
		path.join(frontend, "src", "Drop.tsx"),
		"export const getLightweightSnapshot = () => ({});\n",
	);
	git(root, "init", "--quiet");
	git(root, "config", "user.name", "Benchmark Test");
	git(root, "config", "user.email", "benchmark@example.invalid");
	git(root, "add", ".");
	git(root, "commit", "--quiet", "-m", "fixture");
	return root;
};

test("binds exact Peerbit HEAD and digests dirty harness contents", async () => {
	const root = await createExamplesRepository();
	try {
		const head = git(root, "rev-parse", "HEAD");
		const clean = await getPeerbitProvenance({
			root,
			requestedRef: "requested-ref",
			expectedResolvedCommit: head,
		});
		assert.deepEqual(clean, {
			requestedRef: "requested-ref",
			resolvedCommit: head,
			dirty: false,
			worktreeDigest: null,
		});
		await assert.rejects(
			getPeerbitProvenance({
				root,
				expectedResolvedCommit: "f".repeat(40),
			}),
			/does not match the requested ref commit/,
		);
		await writeFile(path.join(root, "untracked.txt"), "first\n");
		const dirty = await getPeerbitProvenance({ root });
		assert.equal(dirty.dirty, true);
		assert.match(dirty.worktreeDigest, /^[0-9a-f]{64}$/);
		await writeFile(path.join(root, "untracked.txt"), "second\n");
		const changed = await getPeerbitProvenance({ root });
		assert.notEqual(changed.worktreeDigest, dirty.worktreeDigest);
		await assert.rejects(
			getPeerbitProvenance({ root, requireClean: true }),
			/cannot be reproduced/,
		);
	} finally {
		await rm(root, { recursive: true, force: true });
	}
});

test("binds pinned examples commit, lockfile, and pre-instrumentation dirt", async () => {
	const root = await createExamplesRepository();
	try {
		const head = git(root, "rev-parse", "HEAD");
		const lockfileSha256 = await sha256FileHex(
			path.join(root, "pnpm-lock.yaml"),
		);
		const clean = await getExamplesProvenance({
			root,
			requestedRef: "origin/master",
			expectedResolvedCommit: head,
			expectedLockfileSha256: lockfileSha256,
		});
		assert.equal(clean.dirty, false);
		await writeFile(path.join(root, "pre-instrumentation.txt"), "dirty\n");
		const dirty = await getExamplesProvenance({
			root,
			requestedRef: "origin/master",
			expectedResolvedCommit: head,
			expectedLockfileSha256: lockfileSha256,
		});
		assert.equal(dirty.dirty, true);
		assert.match(dirty.worktreeDigest, /^[0-9a-f]{64}$/);
		await assert.rejects(
			getExamplesProvenance({
				root,
				requestedRef: "origin/master",
				requireClean: true,
			}),
			/dirty before benchmark instrumentation/,
		);
		await assert.rejects(
			getExamplesProvenance({
				root,
				requestedRef: "origin/master",
				expectedLockfileSha256: "0".repeat(64),
			}),
			/does not match pinned digest/,
		);
	} finally {
		await rm(root, { recursive: true, force: true });
	}
});
