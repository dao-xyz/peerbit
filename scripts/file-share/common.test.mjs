import assert from "node:assert/strict";
import {
	lstat,
	mkdir,
	mkdtemp,
	readFile,
	realpath,
	rm,
	writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	cleanPeerbitBuildArtifacts,
	collectLocalPeerbitPackages,
	ensureExamplesAssetPackageLinks,
	prepareExamplesRepo,
} from "./common.mjs";

const createFixture = async () => {
	const root = await mkdtemp(path.join(os.tmpdir(), "peerbit-link-test-"));
	const packageDir = path.join(root, "packages", "peerbit");
	const installedDir = path.join(root, "examples", "node_modules", "peerbit");
	const consumerRoot = path.join(
		root,
		"examples",
		"packages",
		"file-share",
		"frontend",
	);
	const consumerInstalledDir = path.join(
		consumerRoot,
		"node_modules",
		"peerbit",
	);
	await mkdir(path.join(packageDir, "dist"), { recursive: true });
	await mkdir(path.join(packageDir, "src"), { recursive: true });
	await mkdir(path.join(installedDir, "dist"), { recursive: true });
	await mkdir(path.join(consumerInstalledDir, "dist"), { recursive: true });
	await writeFile(
		path.join(packageDir, "package.json"),
		'{"name":"peerbit","dependencies":{}}\n',
	);
	await writeFile(path.join(packageDir, "dist", "index.js"), "current\n");
	await writeFile(path.join(packageDir, "src", "index.ts"), "current source\n");
	await writeFile(path.join(installedDir, "dist", "index.js"), "stale\n");
	await writeFile(
		path.join(consumerInstalledDir, "dist", "index.js"),
		"nested stale\n",
	);
	return {
		root,
		packageDir,
		installedDir,
		consumerRoot,
		consumerInstalledDir,
	};
};

test("links refuse stale artifacts and resolve to the clean-built package", async () => {
	const fixture = await createFixture();
	try {
		await cleanPeerbitBuildArtifacts({
			peerbitRoot: fixture.root,
			packageNames: ["peerbit"],
		});
		await assert.rejects(
			ensureExamplesAssetPackageLinks({
				examplesRoot: path.join(fixture.root, "examples"),
				peerbitRoot: fixture.root,
				packageNames: ["peerbit"],
				consumerRoots: [fixture.consumerRoot],
			}),
			/mandatory clean build did not produce/,
		);
		await mkdir(path.join(fixture.packageDir, "dist"), { recursive: true });
		await writeFile(
			path.join(fixture.packageDir, "dist", "index.js"),
			"clean build\n",
		);
		await ensureExamplesAssetPackageLinks({
			examplesRoot: path.join(fixture.root, "examples"),
			peerbitRoot: fixture.root,
			packageNames: ["peerbit"],
			consumerRoots: [fixture.consumerRoot],
		});
		assert.equal((await lstat(fixture.installedDir)).isSymbolicLink(), true);
		assert.equal(
			await realpath(fixture.installedDir),
			await realpath(fixture.packageDir),
		);
		assert.equal(
			await readFile(
				path.join(fixture.installedDir, "dist", "index.js"),
				"utf8",
			),
			"clean build\n",
		);
		assert.equal(
			await realpath(fixture.consumerInstalledDir),
			await realpath(fixture.packageDir),
		);
	} finally {
		await rm(fixture.root, { recursive: true, force: true });
	}
});

test("local package selection is exact and rejects unknown names", async () => {
	const fixture = await createFixture();
	try {
		assert.deepEqual(
			[
				...(
					await collectLocalPeerbitPackages(fixture.root, { names: [] })
				).keys(),
			],
			[],
		);
		assert.deepEqual(
			[
				...(
					await collectLocalPeerbitPackages(fixture.root, {
						names: ["peerbit"],
					})
				).keys(),
			],
			["peerbit"],
		);
		await assert.rejects(
			collectLocalPeerbitPackages(fixture.root, {
				names: ["@peerbit/not-a-real-package"],
			}),
			/Unknown local Peerbit package: @peerbit\/not-a-real-package/,
		);
		await assert.rejects(
			collectLocalPeerbitPackages(fixture.root, {
				names: ["peerbit", "peerbit"],
			}),
			/Duplicate local Peerbit package names/,
		);
	} finally {
		await rm(fixture.root, { recursive: true, force: true });
	}
});

test("link preflight leaves every installed package untouched when a later build artifact is missing", async () => {
	const fixture = await createFixture();
	const documentPackageDir = path.join(fixture.root, "packages", "document");
	const installedDocumentDir = path.join(
		fixture.root,
		"examples",
		"node_modules",
		"@peerbit",
		"document",
	);
	try {
		await mkdir(path.join(documentPackageDir, "dist"), { recursive: true });
		await mkdir(path.join(installedDocumentDir, "dist"), { recursive: true });
		await writeFile(
			path.join(documentPackageDir, "package.json"),
			'{"name":"@peerbit/document","dependencies":{}}\n',
		);
		await writeFile(
			path.join(documentPackageDir, "dist", "index.js"),
			"current document\n",
		);
		await writeFile(
			path.join(installedDocumentDir, "dist", "index.js"),
			"stale document\n",
		);
		await writeFile(
			path.join(fixture.packageDir, "package.json"),
			'{"name":"peerbit","dependencies":{"@peerbit/document":"workspace:*"}}\n',
		);
		await rm(path.join(fixture.packageDir, "dist"), {
			recursive: true,
			force: true,
		});
		await assert.rejects(
			ensureExamplesAssetPackageLinks({
				examplesRoot: path.join(fixture.root, "examples"),
				peerbitRoot: fixture.root,
				packageNames: ["peerbit"],
			}),
			/mandatory clean build did not produce/,
		);
		assert.equal((await lstat(installedDocumentDir)).isDirectory(), true);
		assert.equal(
			await readFile(
				path.join(installedDocumentDir, "dist", "index.js"),
				"utf8",
			),
			"stale document\n",
		);
	} finally {
		await rm(fixture.root, { recursive: true, force: true });
	}
});

test("no-override preparation preserves the pinned dependency graph byte-for-byte", async () => {
	const root = await mkdtemp(path.join(os.tmpdir(), "peerbit-prepare-test-"));
	const peerbitRoot = path.join(root, "core");
	const template = path.join(root, "template");
	const dest = path.join(root, "prepared");
	const packageJson =
		'{"name":"examples","pnpm":{"overrides":{"peerbit":"5.3.3"}}}\n';
	const lockfile = "lockfileVersion: '9.0'\n\nimporters: {}\n";
	const patch = "pinned patch bytes\n";
	try {
		await mkdir(path.join(peerbitRoot, "packages", "peerbit"), {
			recursive: true,
		});
		await writeFile(
			path.join(peerbitRoot, "packages", "peerbit", "package.json"),
			'{"name":"peerbit","dependencies":{}}\n',
		);
		await mkdir(path.join(template, "patches"), { recursive: true });
		await mkdir(path.join(template, "node_modules", "stale"), {
			recursive: true,
		});
		await writeFile(path.join(template, "package.json"), packageJson);
		await writeFile(path.join(template, "pnpm-lock.yaml"), lockfile);
		await writeFile(
			path.join(template, "patches", "@peerbit+shared-log+1.0.0.patch"),
			patch,
		);
		await writeFile(
			path.join(template, "node_modules", "stale", "index.js"),
			"stale install\n",
		);
		await prepareExamplesRepo({
			template,
			dest,
			peerbitRoot,
			localPackageNames: ["peerbit"],
			applyOverrides: false,
		});
		assert.equal(
			await readFile(path.join(dest, "package.json"), "utf8"),
			packageJson,
		);
		assert.equal(
			await readFile(path.join(dest, "pnpm-lock.yaml"), "utf8"),
			lockfile,
		);
		assert.equal(
			await readFile(
				path.join(dest, "patches", "@peerbit+shared-log+1.0.0.patch"),
				"utf8",
			),
			patch,
		);
		await assert.rejects(lstat(path.join(dest, "node_modules")), /ENOENT/);
	} finally {
		await rm(root, { recursive: true, force: true });
	}
});
