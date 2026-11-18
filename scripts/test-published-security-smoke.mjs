import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import {
	appendFile,
	mkdir,
	mkdtemp,
	readFile,
	rm,
	writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { classifyNpmAuditResult } from "./npm-audit-result.mjs";
import {
	discoverPublishableWorkspacePackages,
	runtimeDependencyFields,
} from "./publishable-workspace-packages.mjs";
import {
	packageDirectories,
	validatePublishedSecurityCoverage,
} from "./published-security-coverage.mjs";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cliArguments = process.argv.slice(2);
assert(
	cliArguments.length === 0 ||
		(cliArguments.length === 1 && cliArguments[0] === "--advisory-only"),
	"usage: node scripts/test-published-security-smoke.mjs [--advisory-only]",
);
const advisoryOnly = cliArguments[0] === "--advisory-only";
const node18Executable = advisoryOnly
	? undefined
	: process.versions.node.startsWith("18.")
		? process.execPath
		: process.env.PEERBIT_NODE18_EXECUTABLE?.trim();
if (!advisoryOnly) {
	assert(
		node18Executable,
		"PEERBIT_NODE18_EXECUTABLE must name a real Node 18 executable; CI and release workflows provision Node 18.20.8 explicitly",
	);
}
const viteNodeEngine = "^20.19.0 || >=22.12.0";

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
		killSignal: options.killSignal,
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

if (!advisoryOnly) {
	assert.match(
		run(node18Executable, ["--version"], {
			status: 0,
			timeout: 30_000,
		}).stdout.trim(),
		/^v18\./,
		"PEERBIT_NODE18_EXECUTABLE must run Node 18",
	);
}

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
	const packedPeerbitVite = packedPackages.get("@peerbit/vite");
	assert(
		packedPeerbitVite,
		"@peerbit/vite must be included in the package smoke",
	);
	assert.equal(
		packedPeerbitVite.manifest.engines?.node,
		viteNodeEngine,
		"packed @peerbit/vite must declare the Node.js floor of its Vite peers",
	);
	// A Vite plugin runs inside the application's own Vite, so the packed
	// plugin declares the range it supports as a peer and nothing ships Vite.
	const vitePeerRange = packedPeerbitVite.manifest.peerDependencies?.vite;
	assert(
		typeof vitePeerRange === "string" && vitePeerRange.length > 0,
		"packed @peerbit/vite must take the application's Vite as a peer dependency",
	);
	assert.notEqual(
		packedPeerbitVite.manifest.peerDependenciesMeta?.vite?.optional,
		true,
		"packed @peerbit/vite must not make its Vite peer optional",
	);
	for (const [packageName, { manifest: packedManifest }] of packedPackages) {
		for (const dependencyField of ["dependencies", "optionalDependencies"]) {
			assert.equal(
				packedManifest[dependencyField]?.vite,
				undefined,
				`${packageName}: packed ${dependencyField} must not ship Vite; applications install their own`,
			);
		}
	}
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
	// An application installs its own Vite as a development tool; the consumer
	// does the same with the plugin's peer range, so npm picks the newest Vite
	// that @peerbit/vite admits.
	const devDependencies = { vite: vitePeerRange };
	await writeFile(
		join(consumerDirectory, "package.json"),
		JSON.stringify(
			{
				name: "peerbit-published-security-consumer",
				private: true,
				type: "module",
				dependencies,
				devDependencies,
			},
			null,
			2,
		) + "\n",
	);
	if (!advisoryOnly) {
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
			join(consumerDirectory, "smoke.mjs"),
			[
				'import assert from "node:assert/strict";',
				'import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";',
				'import { createRequire } from "node:module";',
				'import { join } from "node:path";',
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
				"const vitePath = createRequire(peerbitViteEntry).resolve('vite');",
				"assert.equal(vitePath, createRequire(import.meta.url).resolve('vite'), 'the Vite peer must resolve to the consumer Vite');",
				"const vite = await import(pathToFileURL(vitePath).href);",
				"const viteProject = await mkdtemp(join(process.cwd(), 'vite-project-'));",
				"await mkdir(join(viteProject, 'public'));",
				"await writeFile(join(viteProject, 'index.html'), '<!doctype html><title>Peerbit</title>');",
				"// No app code is built, so alias events as the plugin's integration test does instead of resolving the browser polyfill.",
				"const viteConfig = {",
				"  root: viteProject,",
				"  configFile: false,",
				"  logLevel: 'silent',",
				"  publicDir: join(viteProject, 'public'),",
				"  resolve: { alias: { events: 'node:events' } },",
				"};",
				"await vite.build({ ...viteConfig, plugins: peerbitVite(), build: { outDir: join(viteProject, 'dist') } });",
				"const sqliteWasm = await readFile(join(viteProject, 'dist', 'peerbit', 'sqlite3', 'sqlite3.wasm'));",
				"assert.deepEqual([...sqliteWasm.subarray(0, 4)], [0, 97, 115, 109], 'Vite ' + vite.version + ' must emit the SQLite Wasm asset through the packed plugin');",
				"// Creating a dev server runs the plugin's middleware placement, which throws if Vite's internal anchors moved.",
				"const viteServer = await vite.createServer({ ...viteConfig, plugins: peerbitVite(), server: { middlewareMode: true, ws: false } });",
				"await viteServer.close();",
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
	}

	// --legacy-peer-deps keeps npm from auto-installing peer dependencies, so
	// react-native-webrtc's "react-native >=0.60.0" peer (and its metro /
	// image-size subtree) never enters the published consumer closure. The Node
	// implementation of @libp2p/webrtc uses node-datachannel; the subtree is
	// react-native-only dead weight here. @peerbit/vite's Vite peer is satisfied
	// by the consumer's own development dependency, as in an application.
	run(
		"npm",
		[
			"install",
			...(advisoryOnly ? ["--package-lock-only", "--ignore-scripts"] : []),
			"--legacy-peer-deps",
			"--no-audit",
			"--no-fund",
			"--loglevel=error",
		],
		{
			cwd: consumerDirectory,
			env: { NPM_CONFIG_ENGINE_STRICT: "true" },
			status: 0,
			timeout: 900_000,
		},
	);
	const consumerManifest = JSON.parse(
		await readFile(join(consumerDirectory, "package.json"), "utf8"),
	);
	assert.equal(consumerManifest.overrides, undefined);
	assert.deepEqual(consumerManifest.dependencies, dependencies);
	assert.deepEqual(consumerManifest.devDependencies, devDependencies);
	assert.equal(packedPackages.size, publishablePackages.length);

	const lockfile = JSON.parse(
		await readFile(join(consumerDirectory, "package-lock.json"), "utf8"),
	);
	for (const [packagePath, manifest] of Object.entries(lockfile.packages)) {
		const name = packagePath.split("node_modules/").pop();
		assert(
			name !== "vite-plugin-static-copy" &&
				name !== "braces" &&
				!(name === "chokidar" && Number(manifest.version.split(".")[0]) < 4),
			`the retired glob-watching asset dependency returned: ${packagePath}`,
		);
	}
	const forbiddenPeerSubtreePackages = [
		"react-native",
		"metro",
		"metro-config",
		"metro-transform-worker",
		"image-size",
	];
	const isForbiddenPackageName = (packageName) =>
		forbiddenPeerSubtreePackages.includes(packageName) ||
		packageName.startsWith("@react-native/");
	const forbiddenLockPaths = Object.keys(lockfile.packages).filter(
		(packagePath) =>
			packagePath !== "" &&
			isForbiddenPackageName(packagePath.split("node_modules/").pop()),
	);
	assert.deepEqual(
		forbiddenLockPaths,
		[],
		"the react-native peer subtree leaked into the consumer lock: " +
			forbiddenLockPaths.join(", "),
	);
	if (!advisoryOnly) {
		const forbiddenInstalledPaths = [
			...forbiddenPeerSubtreePackages,
			"@react-native",
		]
			.map((packageName) =>
				join(consumerDirectory, "node_modules", packageName),
			)
			.filter((packagePath) => existsSync(packagePath));
		assert.deepEqual(
			forbiddenInstalledPaths,
			[],
			"the react-native peer subtree leaked into the consumer node_modules: " +
				forbiddenInstalledPaths.join(", "),
		);
	}
	let advisorySummary;
	if (advisoryOnly) {
		const audit = spawnSync(
			"npm",
			[
				"audit",
				"--package-lock-only",
				"--omit=dev",
				"--json",
				"--fetch-timeout=60000",
			],
			{
				cwd: consumerDirectory,
				encoding: "utf8",
				env: {
					...process.env,
					FORCE_COLOR: "0",
					NO_COLOR: "1",
				},
				timeout: 150_000,
			},
		);
		const classification = classifyNpmAuditResult(audit);
		if (classification.outcome === "findings") {
			process.stderr.write(audit.stdout || "");
			throw new Error(
				"the packed production dependency graph contains npm advisory findings: " +
					classification.reason,
			);
		}
		if (classification.outcome === "invalid") {
			throw new Error(
				"npm audit returned an unrecognized or internally inconsistent result: " +
					classification.reason,
			);
		}
		if (classification.outcome === "unavailable") {
			advisorySummary = "scanner unavailable: " + classification.reason;
			const escapedWarning = advisorySummary
				.replaceAll("%", "%25")
				.replaceAll("\r", "%0D")
				.replaceAll("\n", "%0A");
			console.warn(
				process.env.GITHUB_ACTIONS === "true"
					? `::warning title=Published dependency scanner unavailable::${escapedWarning}`
					: `Published dependency ${advisorySummary}`,
			);
			const stepSummary = process.env.GITHUB_STEP_SUMMARY?.trim();
			if (stepSummary) {
				await appendFile(
					stepSummary,
					"### Published dependency advisories\n\n⚠️ " +
						advisorySummary.replaceAll("\n", " ") +
						"\n",
				);
			}
		} else {
			advisorySummary = "zero production advisory findings";
		}
	}
	assert.deepEqual(lockfile.packages[""].dependencies, dependencies);
	// The production audit omits only what the consumer's own Vite brings.
	assert.deepEqual(lockfile.packages[""].devDependencies, devDependencies);
	const lockfilePackages = Object.entries(lockfile.packages);
	const installedViteEntries = lockfilePackages.filter(([packagePath]) =>
		packagePath.endsWith("node_modules/vite"),
	);
	assert.deepEqual(
		installedViteEntries.map(([packagePath]) => packagePath),
		["node_modules/vite"],
		"the clean consumer must install exactly one Vite runtime, its own",
	);
	const installedVite = installedViteEntries[0][1];
	assert.equal(
		installedVite.dev,
		true,
		"Vite must stay a development tool of the consumer, outside the published production closure",
	);
	assert.equal(
		installedVite.engines?.node,
		viteNodeEngine,
		"the clean consumer must retain Vite's Node.js engine contract",
	);
	const viteEdges = lockfilePackages.flatMap(([packagePath, entry]) =>
		[
			"dependencies",
			"devDependencies",
			"optionalDependencies",
			"peerDependencies",
		]
			.filter((dependencyField) => entry[dependencyField]?.vite !== undefined)
			.map(
				(dependencyField) =>
					`${packagePath || "(consumer)"} ${dependencyField} ${entry[dependencyField].vite}`,
			),
	);
	assert.deepEqual(
		viteEdges.sort(),
		[
			`(consumer) devDependencies ${vitePeerRange}`,
			`node_modules/@peerbit/vite peerDependencies ${vitePeerRange}`,
		],
		"only the consumer and @peerbit/vite's peer edge may reach Vite",
	);
	const developmentOnlyEntries = lockfilePackages.filter(
		([, entry]) => entry.dev === true,
	).length;
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
	// Vite 7 installs esbuild; Vite 8 leaves it an optional peer. Wherever the
	// consumer installs esbuild, it must stay on the patched 0.27/0.28 line.
	const esbuildVersions = lockfilePackages
		.filter(([packagePath]) => packagePath.endsWith("node_modules/esbuild"))
		.map(([, entry]) => entry.version);
	assert(
		esbuildVersions.every((version) => /^0\.(27|28)\./.test(version)),
		`expected only the patched esbuild 0.27/0.28 line, got ${esbuildVersions.join(", ")}`,
	);

	if (!advisoryOnly) {
		run(process.execPath, [join(consumerDirectory, "smoke.mjs")], {
			cwd: consumerDirectory,
			status: 0,
			timeout: 120_000,
		});
		const nativeWebrtc = run(
			process.execPath,
			[
				join(
					repositoryRoot,
					"scripts",
					"ci",
					"peerbit-native-webrtc.worker.mjs",
				),
				join(consumerDirectory, "node_modules", "peerbit", "package.json"),
			],
			{
				cwd: consumerDirectory,
				status: 0,
				timeout: 90_000,
				killSignal: "SIGKILL",
			},
		);
		assert.equal(nativeWebrtc.signal, null);
		assert.match(nativeWebrtc.stdout, /"phase":"native-webrtc-loaded"/);
		assert.match(nativeWebrtc.stdout, /"phase":"native-webrtc-closed"/);
		console.log(nativeWebrtc.stdout);
		const node18 = run(
			node18Executable,
			[join(consumerDirectory, "crypto-node18-smoke.mjs")],
			{
				cwd: consumerDirectory,
				status: 0,
				timeout: 300_000,
			},
		);
		assert.match(node18.stdout, /Node 18\./);
		console.log(
			"Published-package consumer passed with " +
				packageDirectories.length +
				" security roots and all " +
				publishablePackages.length +
				" publishable workspace packages as exact local tarballs, " +
				"a peer-free production install, the consumer's own Vite " +
				installedVite.version +
				" building with the packed plugin, the isolated nested crypto install, and the Node 18 crypto wire contract.",
		);
	} else {
		console.log(
			"Published-package advisory scan completed for all " +
				publishablePackages.length +
				" publishable workspace packages as exact local tarballs: " +
				advisorySummary +
				". The consumer's own Vite " +
				installedVite.version +
				" and its " +
				(developmentOnlyEntries - 1) +
				" other development-only lock entries are outside the production audit.",
		);
	}
} finally {
	await rm(temporaryRoot, { recursive: true, force: true });
}
