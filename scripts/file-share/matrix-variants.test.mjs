import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	MATRIX_SESSION_MARKER,
	assertMatrixIntegrationMode,
	assertMatrixPackageRequest,
	assertUniqueResolvedVariantCommits,
	createCounterbalancedInvocationPlan,
	createCounterbalancedModePlan,
	createExclusiveMatrixSession,
	createVariantMaterializationPlan,
	normalizeVariantSpecs,
} from "./matrix-variants.mjs";

test("defaults to one explicit real baseline ref", () => {
	assert.deepEqual(normalizeVariantSpecs(), [
		{ name: "baseline", kind: "ref", ref: "origin/master" },
	]);
});

test("parses worktree, HEAD compatibility, and named real refs", () => {
	assert.deepEqual(
		normalizeVariantSpecs("current,head,candidate=refs/heads/feature"),
		[
			{ name: "current", kind: "worktree" },
			{ name: "head", kind: "ref", ref: "HEAD" },
			{ name: "candidate", kind: "ref", ref: "refs/heads/feature" },
		],
	);
});

test("rejects the obsolete invented downstream variant and unsafe labels", () => {
	assert.throws(() => normalizeVariantSpecs("downstream"), /explicit real ref/);
	assert.throws(() => normalizeVariantSpecs("../candidate=HEAD"), /label/);
	assert.throws(() => normalizeVariantSpecs("candidate=--help"), /git ref/);
	assert.throws(
		() => normalizeVariantSpecs("candidate=HEAD,candidate=origin/master"),
		/Duplicate/,
	);
});

test("rejects two labels that resolve to the same commit", () => {
	assert.throws(
		() =>
			assertUniqueResolvedVariantCommits([
				{ name: "baseline", resolvedCommit: "a".repeat(40) },
				{ name: "candidate", resolvedCommit: "a".repeat(40) },
			]),
		/both resolve/,
	);
	assert.doesNotThrow(() =>
		assertUniqueResolvedVariantCommits([
			{ name: "baseline", resolvedCommit: "a".repeat(40) },
			{ name: "candidate", resolvedCommit: "b".repeat(40) },
		]),
	);
});

test("materializes current and ref variants into isolated clone roots", () => {
	const variantRoot = path.join(
		os.tmpdir(),
		"matrix-session",
		"variants",
		"current",
	);
	const commit = "a".repeat(40);
	assert.deepEqual(
		createVariantMaterializationPlan({
			variantSpec: {
				kind: "worktree",
				resolvedCommit: commit,
			},
			variantRoot,
		}),
		{
			cloneCommit: commit,
			peerbitRoot: path.resolve(variantRoot),
			requestedRef: "worktree",
			sourceWorktreeMustBeClean: true,
		},
	);
	const refRoot = path.join(
		os.tmpdir(),
		"matrix-session",
		"variants",
		"baseline",
	);
	assert.deepEqual(
		createVariantMaterializationPlan({
			variantSpec: {
				kind: "ref",
				ref: "origin/master",
				resolvedCommit: commit,
			},
			variantRoot: refRoot,
		}),
		{
			cloneCommit: commit,
			peerbitRoot: path.resolve(refRoot),
			requestedRef: "origin/master",
			sourceWorktreeMustBeClean: false,
		},
	);
});

test("matrix modes and direct package requests cannot become no-ops", () => {
	assert.equal(assertMatrixIntegrationMode("link"), "link");
	assert.throws(
		() => assertMatrixIntegrationMode("overlay"),
		/requires.*link.*dependency graph/,
	);
	assert.throws(
		() => assertMatrixIntegrationMode("none"),
		/requires.*link.*dependency graph/,
	);
	const requiredNames = ["peerbit", "@peerbit/document"];
	assert.doesNotThrow(() =>
		assertMatrixPackageRequest({
			requestedNames: ["peerbit", "@peerbit/document", "@peerbit/react"],
			requiredNames,
		}),
	);
	assert.doesNotThrow(() =>
		assertMatrixPackageRequest({
			requestedNames: undefined,
			requiredNames,
		}),
	);
	assert.throws(
		() =>
			assertMatrixPackageRequest({
				requestedNames: ["peerbit"],
				requiredNames,
			}),
		/missing required.*@peerbit\/document/,
	);
	assert.throws(
		() =>
			assertMatrixPackageRequest({
				requestedNames: ["peerbit", "peerbit"],
				requiredNames,
			}),
		/unique, non-empty/,
	);
});

test("interleaves and counterbalances variant/mode order deterministically", () => {
	const options = {
		variants: ["baseline", "candidate"],
		modes: ["adaptive", "fixed1"],
		runs: 2,
	};
	const plan = createCounterbalancedInvocationPlan(options);
	assert.deepEqual(
		plan.map(({ run, variant, mode }) => [run, variant, mode]),
		[
			[1, "baseline", "adaptive"],
			[1, "candidate", "fixed1"],
			[1, "baseline", "fixed1"],
			[1, "candidate", "adaptive"],
			[2, "candidate", "fixed1"],
			[2, "baseline", "adaptive"],
			[2, "candidate", "adaptive"],
			[2, "baseline", "fixed1"],
		],
	);
	assert.deepEqual(createCounterbalancedInvocationPlan(options), plan);
	for (const run of [1, 2]) {
		for (const variant of options.variants) {
			for (const mode of options.modes) {
				assert.equal(
					plan.filter(
						(item) =>
							item.run === run &&
							item.variant === variant &&
							item.mode === mode,
					).length,
					1,
				);
			}
		}
	}
});

test("alternates standalone mode order between repetitions", () => {
	assert.deepEqual(
		createCounterbalancedModePlan({
			modes: ["adaptive", "fixed1"],
			runs: 2,
		}).map(({ run, mode }) => [run, mode]),
		[
			[1, "adaptive"],
			[1, "fixed1"],
			[2, "fixed1"],
			[2, "adaptive"],
		],
	);
});

test("creates UUID-owned exclusive matrix sessions without reusing the base", async () => {
	const fixtureRoot = await mkdtemp(
		path.join(os.tmpdir(), "peerbit-matrix-root-"),
	);
	const repository = path.join(fixtureRoot, "repository");
	const baseDir = path.join(fixtureRoot, "output");
	const nonce = "123e4567-e89b-42d3-a456-426614174000";
	const now = new Date("2026-07-17T12:34:56.789Z");
	await mkdir(repository);
	try {
		const session = await createExclusiveMatrixSession({
			baseDir,
			repoRoot: repository,
			nonce,
			now,
		});
		assert.equal(path.dirname(session.matrixRoot), await realpath(baseDir));
		assert.equal(path.basename(session.markerFile), MATRIX_SESSION_MARKER);
		const marker = JSON.parse(await readFile(session.markerFile, "utf8"));
		assert.equal(marker.nonce, nonce);
		assert.equal(marker.matrixRoot, session.matrixRoot);
		await assert.rejects(
			createExclusiveMatrixSession({
				baseDir,
				repoRoot: repository,
				nonce,
				now,
			}),
			/already exists|EEXIST/,
		);
		await assert.rejects(
			createExclusiveMatrixSession({
				baseDir: path.join(repository, "unsafe-output"),
				repoRoot: repository,
			}),
			/inside the Peerbit worktree/,
		);
		await assert.rejects(
			createExclusiveMatrixSession({
				baseDir: path.parse(baseDir).root,
				repoRoot: repository,
			}),
			/filesystem root/,
		);
	} finally {
		await rm(fixtureRoot, { recursive: true, force: true });
	}
});
