import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
	appendFile,
	mkdir,
	mkdtemp,
	readFile,
	readdir,
	realpath,
	rm,
	writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const rootRequire = createRequire(join(repositoryRoot, "package.json"));

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
		timeout: options.timeout ?? 120_000,
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

const readPackage = async (packagePath) =>
	JSON.parse(await readFile(packagePath, "utf8"));

const testMocha = async (temporaryRoot) => {
	const mochaDirectory = join(temporaryRoot, "mocha");
	await mkdir(mochaDirectory);
	const firstTest = join(mochaDirectory, "first.spec.cjs");
	const secondTest = join(mochaDirectory, "second.spec.cjs");
	const failingTest = join(mochaDirectory, "failure.spec.cjs");
	await writeFile(
		firstTest,
		'require("node:assert/strict").equal(2 + 2, 4);\n',
	);
	await writeFile(
		secondTest,
		'require("node:assert/strict").deepEqual([1, 2], [1, 2]);\n',
	);
	await writeFile(
		failingTest,
		[
			'const assert = require("node:assert/strict");',
			'describe("diff contract", () => {',
			'  it("renders the actual and expected values", () => {',
			'    assert.deepEqual({ value: "before", nested: [1, 2] }, { value: "after", nested: [1, 3] });',
			"  });",
			"});",
			"",
		].join("\n"),
	);

	const mochaPackage = rootRequire.resolve("mocha/package.json");
	const mochaCli = join(dirname(mochaPackage), "bin", "mocha.js");
	run(
		process.execPath,
		[
			mochaCli,
			"--no-config",
			"--parallel",
			"--jobs",
			"2",
			"--reporter",
			"dot",
			firstTest,
			secondTest,
		],
		{ cwd: mochaDirectory, status: 0 },
	);

	const failure = run(
		process.execPath,
		[mochaCli, "--no-config", "--reporter", "spec", failingTest],
		{ cwd: mochaDirectory, status: 1 },
	);
	const failureOutput = `${failure.stdout}\n${failure.stderr}`;
	assert.match(failureOutput, /before/);
	assert.match(failureOutput, /after/);
	assert.match(failureOutput, /actual|expected/i);
};

const testProcessInfo = async (temporaryRoot) => {
	const coverageDirectory = join(temporaryRoot, "nyc-output");
	const coverageTarget = join(temporaryRoot, "coverage-target.cjs");
	await writeFile(
		coverageTarget,
		"module.exports = value => (value > 0 ? value * 2 : 0);\nmodule.exports(4);\n",
	);

	const nycPackage = rootRequire.resolve("nyc/package.json");
	const nycRequire = createRequire(nycPackage);
	const processInfo3Package = nycRequire.resolve(
		"istanbul-lib-processinfo/package.json",
	);
	const processInfo3 = await readPackage(processInfo3Package);
	assert.equal(processInfo3.version, "3.0.1");
	assert.equal(processInfo3.dependencies?.uuid, undefined);

	const nycCli = join(dirname(nycPackage), "bin", "nyc.js");
	run(
		process.execPath,
		[
			nycCli,
			"--silent",
			"--temp-dir",
			coverageDirectory,
			process.execPath,
			coverageTarget,
		],
		{ cwd: temporaryRoot, status: 0 },
	);
	const processInfoDirectory = join(coverageDirectory, "processinfo");
	const processInfoFiles = await readdir(processInfoDirectory);
	assert(processInfoFiles.includes("index.json"));
	assert(processInfoFiles.some((file) => file !== "index.json"));
	const index = JSON.parse(
		await readFile(join(processInfoDirectory, "index.json"), "utf8"),
	);
	assert(Object.keys(index).length > 0);

	const aegirDirectory = await realpath(
		join(repositoryRoot, "node_modules/aegir"),
	);
	const aegirRequire = createRequire(join(aegirDirectory, "package.json"));
	const nyc17Package = aegirRequire.resolve("nyc/package.json");
	const nyc17Require = createRequire(nyc17Package);
	const processInfo2Package = nyc17Require.resolve(
		"istanbul-lib-processinfo/package.json",
	);
	const processInfo2 = await readPackage(processInfo2Package);
	assert.match(processInfo2.version, /^2\./);
	const processInfo2Require = createRequire(processInfo2Package);
	const uuidPackage = await readPackage(
		processInfo2Require.resolve("uuid/package.json"),
	);
	assert.equal(uuidPackage.version, "11.1.1");

	const processInfo2Directory = join(temporaryRoot, "processinfo-v2");
	await mkdir(processInfo2Directory);
	const { ProcessDB, ProcessInfo } = processInfo2Require(
		"istanbul-lib-processinfo",
	);
	const processInfo = new ProcessInfo({
		argv: ["security-dependency-contract"],
		directory: processInfo2Directory,
		files: [],
	});
	await processInfo.save();
	const database = new ProcessDB(processInfo2Directory);
	await database.writeIndex();
	const processInfo2Files = await readdir(processInfo2Directory);
	assert(processInfo2Files.includes("index.json"));
	assert(processInfo2Files.includes(`${processInfo.uuid}.json`));
};

const testPatchPackage = async (temporaryRoot) => {
	const fixtureDirectory = join(temporaryRoot, "patch-package");
	await mkdir(fixtureDirectory);
	await writeFile(
		join(fixtureDirectory, "package.json"),
		JSON.stringify(
			{
				name: "peerbit-patch-package-contract",
				private: true,
				dependencies: { "is-number": "7.0.0" },
			},
			null,
			2,
		) + "\n",
	);

	const aegirDirectory = await realpath(
		join(repositoryRoot, "node_modules/aegir"),
	);
	const aegirRequire = createRequire(join(aegirDirectory, "package.json"));
	const runnerPackage = aegirRequire.resolve(
		"react-native-test-runner/package.json",
	);
	const runnerRequire = createRequire(runnerPackage);
	const patchPackage = runnerRequire.resolve("patch-package/package.json");
	const patchPackageManifest = await readPackage(patchPackage);
	assert.equal(patchPackageManifest.version, "6.5.1");
	const patchPackageCli = resolve(
		dirname(patchPackage),
		patchPackageManifest.bin["patch-package"],
	);
	const install = () =>
		run(
			"npm",
			[
				"install",
				"--ignore-scripts",
				"--no-audit",
				"--no-fund",
				"--loglevel=error",
			],
			{
				cwd: fixtureDirectory,
				status: 0,
				timeout: 180_000,
			},
		);

	install();
	const fixtureModule = join(
		fixtureDirectory,
		"node_modules",
		"is-number",
		"index.js",
	);
	const marker = "// peerbit patch-package contract";
	await appendFile(fixtureModule, `\n${marker}\n`);
	run(process.execPath, [patchPackageCli, "is-number"], {
		cwd: fixtureDirectory,
		status: 0,
		timeout: 180_000,
	});
	const patchPath = join(fixtureDirectory, "patches", "is-number+7.0.0.patch");
	assert.match(await readFile(patchPath, "utf8"), new RegExp(marker));

	await rm(join(fixtureDirectory, "node_modules"), {
		recursive: true,
		force: true,
	});
	install();
	run(process.execPath, [patchPackageCli], {
		cwd: fixtureDirectory,
		status: 0,
		timeout: 180_000,
	});
	assert.match(await readFile(fixtureModule, "utf8"), new RegExp(marker));
};

const temporaryRoot = await mkdtemp(
	join(tmpdir(), "peerbit-security-contracts-"),
);

try {
	await testMocha(temporaryRoot);
	await testProcessInfo(temporaryRoot);
	await testPatchPackage(temporaryRoot);
	console.log(
		"Security dependency contracts passed: Mocha parallel/diff, NYC processinfo v2/v3, and patch-package create/apply.",
	);
} finally {
	await rm(temporaryRoot, { recursive: true, force: true });
}
