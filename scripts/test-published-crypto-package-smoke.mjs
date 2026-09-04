import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cryptoDirectory = join(repositoryRoot, "packages", "utils", "crypto");
const node18Executable = process.versions.node.startsWith("18.")
	? process.execPath
	: process.env.PEERBIT_NODE18_EXECUTABLE?.trim();
assert(
	node18Executable,
	"PEERBIT_NODE18_EXECUTABLE must name a real Node 18 executable; CI and release workflows provision Node 18.20.8 explicitly",
);

const run = (command, args, options = {}) => {
	const result = spawnSync(command, args, {
		cwd: options.cwd ?? repositoryRoot,
		encoding: "utf8",
		env: {
			...process.env,
			FORCE_COLOR: "0",
			NO_COLOR: "1",
		},
		timeout: options.timeout ?? 600_000,
	});
	assert.equal(
		result.error,
		undefined,
		`${command} could not run: ${result.error?.message}`,
	);
	assert.equal(
		result.status,
		0,
		[
			`${command} ${args.join(" ")} exited with ${result.status}`,
			result.stdout,
			result.stderr,
		].join("\n"),
	);
	return result;
};

assert.match(
	run(node18Executable, ["--version"], { timeout: 30_000 }).stdout.trim(),
	/^v18\./,
	"PEERBIT_NODE18_EXECUTABLE must run Node 18",
);

const repositoryManifest = JSON.parse(
	await readFile(join(repositoryRoot, "package.json"), "utf8"),
);
assert.equal(
	repositoryManifest.engines?.node,
	">=22",
	"the isolated crypto smoke must track the repository's advertised Node floor",
);
const cryptoManifest = JSON.parse(
	await readFile(join(cryptoDirectory, "package.json"), "utf8"),
);
const temporaryRoot = await mkdtemp(
	join(tmpdir(), "peerbit-published-crypto-"),
);
const tarballDirectory = join(temporaryRoot, "tarball");
const consumerDirectory = join(temporaryRoot, "consumer");

try {
	await mkdir(tarballDirectory);
	await mkdir(consumerDirectory);
	const packed = run("pnpm", [
		"--dir",
		cryptoDirectory,
		"pack",
		"--pack-destination",
		tarballDirectory,
		"--json",
	]);
	const packResult = JSON.parse(packed.stdout);
	assert.equal(packResult.name, "@peerbit/crypto");
	assert.equal(packResult.version, cryptoManifest.version);
	const packedManifest = JSON.parse(
		run("tar", ["-xOf", packResult.filename, "package/package.json"]).stdout,
	);
	for (const dependencyName of ["multiformats", "uint8arrays"]) {
		const dependencyRange = cryptoManifest.dependencies?.[dependencyName];
		assert.equal(
			typeof dependencyRange,
			"string",
			`@peerbit/crypto must declare ${dependencyName} as a direct runtime dependency`,
		);
		assert.equal(
			packedManifest.dependencies?.[dependencyName],
			dependencyRange,
			`the packed package must retain its direct ${dependencyName} runtime edge`,
		);
		assert.equal(
			packedManifest.devDependencies?.[dependencyName],
			undefined,
			`the packed package must not duplicate ${dependencyName} in devDependencies`,
		);
	}
	assert.equal(
		packedManifest.dependencies?.["@peerbit/cache"],
		undefined,
		"the packed package must not retain the unused @peerbit/cache runtime edge",
	);

	const dependencies = {
		"@peerbit/crypto": `file:${packResult.filename}`,
	};
	await writeFile(
		join(consumerDirectory, "package.json"),
		JSON.stringify(
			{
				name: "peerbit-published-crypto-isolated-consumer",
				private: true,
				type: "module",
				dependencies,
			},
			null,
			2,
		) + "\n",
	);
	await writeFile(
		join(consumerDirectory, "crypto-node18-smoke.mjs"),
		await readFile(
			join(
				repositoryRoot,
				"scripts",
				"fixtures",
				"published-crypto-node18-smoke.mjs",
			),
			"utf8",
		),
	);
	run(
		"npm",
		[
			"install",
			"--install-strategy=nested",
			"--no-audit",
			"--no-fund",
			"--loglevel=error",
		],
		{ cwd: consumerDirectory, timeout: 900_000 },
	);
	const lockfile = JSON.parse(
		await readFile(join(consumerDirectory, "package-lock.json"), "utf8"),
	);
	assert.deepEqual(lockfile.packages[""].dependencies, dependencies);
	for (const dependencyName of ["multiformats", "uint8arrays"]) {
		assert(
			lockfile.packages[
				`node_modules/@peerbit/crypto/node_modules/${dependencyName}`
			],
			`nested install must place @peerbit/crypto's direct ${dependencyName} dependency below the package`,
		);
	}
	const node18 = run(
		node18Executable,
		[join(consumerDirectory, "crypto-node18-smoke.mjs")],
		{ cwd: consumerDirectory, timeout: 300_000 },
	);
	assert.match(node18.stdout, /Node 18\./);
	console.log(
		"Packed @peerbit/crypto passed an isolated nested install and runtime import on Node 18.",
	);
} finally {
	await rm(temporaryRoot, { recursive: true, force: true });
}
