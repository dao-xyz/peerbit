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
	preflightBetterSqlite3Runtime,
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

test("automatic local package selection excludes private workspace packages", async () => {
	const fixture = await createFixture();
	const privatePackageDir = path.join(fixture.root, "packages", "private-e2e");
	const publicPackageDir = path.join(fixture.root, "packages", "public");
	try {
		await mkdir(privatePackageDir, { recursive: true });
		await mkdir(publicPackageDir, { recursive: true });
		await writeFile(
			path.join(privatePackageDir, "package.json"),
			'{"name":"@peerbit/private-e2e","private":true}\n',
		);
		await writeFile(
			path.join(publicPackageDir, "package.json"),
			'{"name":"@peerbit/public"}\n',
		);

		assert.deepEqual(
			[...(await collectLocalPeerbitPackages(fixture.root)).keys()],
			["@peerbit/public", "peerbit"],
		);
		await assert.rejects(
			collectLocalPeerbitPackages(fixture.root, {
				names: ["@peerbit/private-e2e"],
			}),
			/Private local Peerbit package is not publishable or linkable: @peerbit\/private-e2e/,
		);
		await writeFile(
			path.join(publicPackageDir, "package.json"),
			'{"name":"@peerbit/public","dependencies":{"@peerbit/private-e2e":"workspace:*"}}\n',
		);
		await assert.rejects(
			collectLocalPeerbitPackages(fixture.root, {
				names: ["@peerbit/public"],
			}),
			/@peerbit\/public depends on private workspace package @peerbit\/private-e2e/,
		);
	} finally {
		await rm(fixture.root, { recursive: true, force: true });
	}
});

const createBetterSqlite3Fixture = async ({ loadFailure } = {}) => {
	const root = await mkdtemp(
		path.join(os.tmpdir(), "peerbit-sqlite-preflight-test-"),
	);
	const indexerDir = path.join(root, "packages", "indexer-sqlite3");
	const moduleDir = path.join(indexerDir, "node_modules", "better-sqlite3");
	await mkdir(moduleDir, { recursive: true });
	await writeFile(
		path.join(indexerDir, "package.json"),
		'{"name":"@peerbit/indexer-sqlite3"}\n',
	);
	await writeFile(
		path.join(moduleDir, "package.json"),
		'{"name":"better-sqlite3","main":"index.js"}\n',
	);
	await writeFile(
		path.join(moduleDir, "index.js"),
		loadFailure
			? `throw new Error(${JSON.stringify(loadFailure)});\n`
			: `module.exports = class Database {
	constructor(filename) {
		if (filename !== ":memory:") throw new Error("unexpected filename");
	}
	prepare(sql) {
		if (sql !== "SELECT 1 AS ok") throw new Error("unexpected query");
		return { get: () => ({ ok: 1 }) };
	}
	close() {}
};\n`,
	);
	return root;
};

test("better-sqlite3 preflight loads and queries the native runtime", async () => {
	const root = await createBetterSqlite3Fixture();
	try {
		const evidence = await preflightBetterSqlite3Runtime(root);
		assert.equal(evidence.node, process.version);
		assert.equal(evidence.modules, process.versions.modules);
		assert.match(evidence.resolvedPath, /better-sqlite3\/index\.js$/);
	} finally {
		await rm(root, { recursive: true, force: true });
	}
});

test("better-sqlite3 preflight fails clearly on a runtime ABI load error", async () => {
	const root = await createBetterSqlite3Fixture({
		loadFailure:
			"compiled against a different Node.js version using NODE_MODULE_VERSION 137",
	});
	try {
		await assert.rejects(preflightBetterSqlite3Runtime(root), (error) => {
			assert.match(error.message, /better-sqlite3 runtime preflight failed/);
			assert.match(error.message, /NODE_MODULE_VERSION/);
			assert.match(error.message, /Reinstall the Peerbit workspace/);
			assert.match(
				error.message,
				/compiled against a different Node\.js version/,
			);
			return true;
		});
	} finally {
		await rm(root, { recursive: true, force: true });
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
