import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	discoverPublishableWorkspacePackages,
	runtimeDependencyFields,
} from "./publishable-workspace-packages.mjs";
import {
	packageDirectories,
	validatePublishedSecurityCoverage,
} from "./published-security-coverage.mjs";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const npxCommand = process.platform === "win32" ? "npx.cmd" : "npx";

const run = (command, args, options = {}) => {
	const result = spawnSync(command, args, {
		cwd: options.cwd ?? repositoryRoot,
		encoding: "utf8",
		env: {
			...process.env,
			FORCE_COLOR: "0",
			NO_COLOR: "1",
			...options.env,
		},
		timeout: options.timeout ?? 600_000,
	});
	assert.equal(
		result.error,
		undefined,
		`${command} could not run: ${result.error?.message}`,
	);
	if (options.status !== undefined) {
		assert.equal(
			result.status,
			options.status,
			[
				`${command} ${args.join(" ")} exited with ${result.status}`,
				result.stdout,
				result.stderr,
			].join("\n"),
		);
	}
	return result;
};

const publishablePackages = await discoverPublishableWorkspacePackages({
	repositoryRoot,
});
const workspaceByDirectory = new Map(
	publishablePackages.map((workspacePackage) => [
		workspacePackage.directory,
		workspacePackage,
	]),
);
const workspaceByName = new Map(
	publishablePackages.map((workspacePackage) => [
		workspacePackage.manifest.name,
		workspacePackage,
	]),
);
const publishableNames = new Set(
	publishablePackages.map(({ manifest }) => manifest.name),
);
const rootPackageNames = packageDirectories.map((packageDirectory) => {
	const workspacePackage = workspaceByDirectory.get(packageDirectory);
	assert(
		workspacePackage,
		`${packageDirectory}: security root is not publishable`,
	);
	return workspacePackage.manifest.name;
});

const temporaryRoot = await mkdtemp(
	join(tmpdir(), "peerbit-published-security-"),
);
const tarballDirectory = join(temporaryRoot, "tarballs");
const consumerDirectory = join(temporaryRoot, "consumer");

try {
	await mkdir(tarballDirectory);
	await mkdir(consumerDirectory);
	const dependencies = {};
	const packedPackages = new Map();
	for (const { directory: packageDirectory, manifest } of publishablePackages) {
		const packed = run(
			"pnpm",
			[
				"--dir",
				join(repositoryRoot, packageDirectory),
				"pack",
				"--pack-destination",
				tarballDirectory,
				"--json",
			],
			{ status: 0 },
		);
		const packResult = JSON.parse(packed.stdout);
		assert.equal(packResult.name, manifest.name);
		assert.equal(packResult.version, manifest.version);
		const packedManifest = JSON.parse(
			run("tar", ["-xOf", packResult.filename, "package/package.json"], {
				status: 0,
			}).stdout,
		);
		assert.equal(packedManifest.name, manifest.name);
		assert.equal(packedManifest.version, manifest.version);
		assert.notEqual(packedManifest.private, true);
		assert.equal(packedManifest.publishConfig?.access, "public");
		for (const dependencyField of runtimeDependencyFields) {
			for (const [dependencyName, dependencyRange] of Object.entries(
				packedManifest[dependencyField] ?? {},
			)) {
				if (!workspaceByName.has(dependencyName)) {
					continue;
				}
				assert(
					publishableNames.has(dependencyName),
					`${manifest.name}: packed ${dependencyField} dependency ${dependencyName} escaped the publishable package set`,
				);
				assert.doesNotMatch(
					dependencyRange,
					/^workspace:/,
					`${manifest.name}: pnpm pack did not rewrite ${dependencyName}`,
				);
			}
		}
		packedPackages.set(manifest.name, {
			filename: packResult.filename,
			manifest: packedManifest,
		});
		dependencies[manifest.name] = `file:${packResult.filename}`;
	}
	await validatePublishedSecurityCoverage({
		packageNames: rootPackageNames,
		changesetPath: join(
			repositoryRoot,
			".changeset",
			"secure-dependency-lines.md",
		),
	});
	const packedDocument = packedPackages.get("@peerbit/document");
	const workspaceTime = workspaceByName.get("@peerbit/time");
	for (const packageName of ["peerbit", "@peerbit/stream"]) {
		const packedPackage = packedPackages.get(packageName);
		assert(
			packedPackage,
			`${packageName} must be included in the package smoke`,
		);
		assert.equal(
			packedPackage.manifest.engines?.node,
			">=22",
			`${packageName}: packed Node engine must match its runtime dependency floor`,
		);
	}
	assert(
		packedDocument,
		"@peerbit/document must be included in the package smoke",
	);
	assert(workspaceTime, "@peerbit/time must be included in the package smoke");
	assert.equal(
		packedDocument.manifest.dependencies?.["@peerbit/time"],
		workspaceTime.manifest.version,
		"packed @peerbit/document must retain its exact runtime @peerbit/time edge",
	);
	assert.equal(
		packedDocument.manifest.devDependencies?.["@peerbit/time"],
		undefined,
		"packed @peerbit/document must not hide @peerbit/time in devDependencies",
	);
	const isolatedCryptoSmoke = run(
		process.execPath,
		[
			join(
				repositoryRoot,
				"scripts",
				"test-published-crypto-package-smoke.mjs",
			),
		],
		{ status: 0, timeout: 1_200_000 },
	);
	assert.match(isolatedCryptoSmoke.stdout, /isolated nested install/);
	const cryptoSmokeFixture = await readFile(
		join(
			repositoryRoot,
			"scripts",
			"fixtures",
			"published-crypto-node18-smoke.mjs",
		),
		"utf8",
	);
	await writeFile(
		join(consumerDirectory, "package.json"),
		JSON.stringify(
			{
				name: "peerbit-published-security-consumer",
				private: true,
				type: "module",
				dependencies,
			},
			null,
			2,
		) + "\n",
	);
	await writeFile(
		join(consumerDirectory, "smoke.mjs"),
		[
			'import assert from "node:assert/strict";',
			'import { createRequire } from "node:module";',
			'import { fileURLToPath, pathToFileURL } from "node:url";',
			"",
			"const { PreHash, Secp256k1Keypair, verify } = await import('@peerbit/crypto');",
			"const keypair = await Secp256k1Keypair.create();",
			"const digest = new Uint8Array(32);",
			"digest[31] = 1;",
			"const signature = await keypair.sign(digest, PreHash.NONE);",
			"assert.equal(await verify(signature, digest), true);",
			"await assert.rejects(keypair.sign(new Uint8Array(31), PreHash.NONE), /exactly 32-byte/);",
			"",
			"const { createStore } = await import('@peerbit/any-store');",
			"const store = createStore();",
			"await store.open();",
			"await store.put('key', new Uint8Array([1, 2, 3]));",
			"assert.deepEqual(await store.get('key'), new Uint8Array([1, 2, 3]));",
			"await store.close();",
			"",
			"const { Documents } = await import('@peerbit/document');",
			"assert.equal(typeof Documents, 'function');",
			"",
			"const { getPort } = await import('@peerbit/server');",
			"assert.equal(getPort('http:'), 8082);",
			"const { default: peerbitVite } = await import('@peerbit/vite');",
			"assert.equal(typeof peerbitVite, 'function');",
			"const peerbitViteEntry = fileURLToPath(import.meta.resolve('@peerbit/vite'));",
			"const requireFromPeerbitVite = createRequire(peerbitViteEntry);",
			"const vitePath = requireFromPeerbitVite.resolve('vite');",
			"const vite = await import(pathToFileURL(vitePath).href);",
			"const transformed = await vite.transformWithEsbuild('const answer: number = 42', 'probe.ts', { loader: 'ts' });",
			"assert.match(transformed.code, /answer/);",
			"for (const packageName of ['@peerbit/any-store-proxy', '@peerbit/document-react', '@peerbit/indexer-tests', '@peerbit/react']) {",
			"  assert.match(import.meta.resolve(packageName), /dist\\/src\\/index\\.js$/);",
			"}",
			"console.log('Published package runtime smoke passed.');",
			"",
		].join("\n"),
	);
	await writeFile(
		join(consumerDirectory, "crypto-node18-smoke.mjs"),
		cryptoSmokeFixture,
	);

	run("npm", ["install", "--no-audit", "--no-fund", "--loglevel=error"], {
		cwd: consumerDirectory,
		env: { NPM_CONFIG_ENGINE_STRICT: "true" },
		status: 0,
		timeout: 900_000,
	});
	const consumerManifest = JSON.parse(
		await readFile(join(consumerDirectory, "package.json"), "utf8"),
	);
	assert.equal(consumerManifest.overrides, undefined);
	assert.deepEqual(consumerManifest.dependencies, dependencies);
	assert.equal(packedPackages.size, publishablePackages.length);

	const audit = run("npm", ["audit", "--omit=dev", "--json"], {
		cwd: consumerDirectory,
		timeout: 300_000,
	});
	const auditReport = JSON.parse(audit.stdout);
	const vulnerabilities = auditReport.metadata?.vulnerabilities;
	assert(vulnerabilities, "npm audit did not return vulnerability metadata");
	assert.equal(
		vulnerabilities.total,
		0,
		`external production audit found vulnerabilities:\n${audit.stdout}`,
	);
	assert.equal(audit.status, 0, audit.stderr || audit.stdout);

	const lockfile = JSON.parse(
		await readFile(join(consumerDirectory, "package-lock.json"), "utf8"),
	);
	assert.deepEqual(lockfile.packages[""].dependencies, dependencies);
	const lockfilePackages = Object.entries(lockfile.packages);
	for (const { manifest } of publishablePackages) {
		const topLevelPath = `node_modules/${manifest.name}`;
		const topLevelEntry = lockfile.packages[topLevelPath];
		assert(
			topLevelEntry,
			`${manifest.name}: missing local top-level lock entry`,
		);
		assert.equal(topLevelEntry.version, manifest.version);
		assert.match(
			topLevelEntry.resolved,
			/^file:/,
			`${manifest.name}: npm resolved the workspace package from a registry`,
		);
		assert(
			topLevelEntry.resolved.endsWith(
				basename(packedPackages.get(manifest.name).filename),
			),
			`${manifest.name}: npm did not resolve the expected local tarball`,
		);
		const internalLockEntries = lockfilePackages.filter(
			([packagePath]) =>
				packagePath === topLevelPath ||
				packagePath.endsWith(`/node_modules/${manifest.name}`),
		);
		assert.equal(
			internalLockEntries.length,
			1,
			`${manifest.name}: npm did not dedupe the internal workspace package to one local tarball`,
		);
		for (const [packagePath, entry] of internalLockEntries) {
			assert.equal(
				entry.version,
				manifest.version,
				`${packagePath}: unexpected internal workspace version`,
			);
			assert.match(
				entry.resolved,
				/^file:/,
				`${packagePath}: internal workspace dependency fell back to a registry`,
			);
		}
	}
	const esbuildVersions = Object.entries(lockfile.packages)
		.filter(([path]) => path.endsWith("node_modules/esbuild"))
		.map(([, entry]) => entry.version);
	assert(esbuildVersions.length > 0);
	assert(
		esbuildVersions.some((version) => /^0\.(27|28)\./.test(version)),
		`expected Vite to install a compatible esbuild 0.27/0.28 line, got ${esbuildVersions.join(", ")}`,
	);

	run(process.execPath, [join(consumerDirectory, "smoke.mjs")], {
		cwd: consumerDirectory,
		status: 0,
		timeout: 120_000,
	});
	const node18 = run(
		npxCommand,
		["--yes", "node@18", join(consumerDirectory, "crypto-node18-smoke.mjs")],
		{
			cwd: consumerDirectory,
			status: 0,
			timeout: 300_000,
		},
	);
	assert.match(node18.stdout, /Node 18\./);
	console.log(
		`Clean published-package consumer passed with ${packageDirectories.length} security roots and all ${publishablePackages.length} publishable workspace packages as exact local tarballs, zero production audit findings, esbuild ${esbuildVersions.join(", ")}, the isolated nested crypto install, and the Node 18 crypto wire contract.`,
	);
} finally {
	await rm(temporaryRoot, { recursive: true, force: true });
}
