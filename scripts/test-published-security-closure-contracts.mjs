import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
	copyFile,
	mkdir,
	mkdtemp,
	readFile,
	rm,
	writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import {
	discoverPublishableWorkspacePackages,
	sortPublishablePackages,
	validateAndSelectPublishablePackages,
} from "./publishable-workspace-packages.mjs";
import {
	packageDirectories,
	validatePublishedSecurityCoverage,
} from "./published-security-coverage.mjs";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const gitCommand = process.platform === "win32" ? "git.exe" : "git";
const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";
const pnpmCommand = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
const require = createRequire(import.meta.url);
const changesetsCliPath = require.resolve("@changesets/cli/bin.js");
const compareStrings = (left, right) =>
	left < right ? -1 : left > right ? 1 : 0;

const run = (command, args, options = {}) => {
	const result = spawnSync(command, args, {
		cwd: options.cwd ?? repositoryRoot,
		encoding: "utf8",
		env: { ...process.env, ...options.env },
		timeout: options.timeout ?? 120_000,
	});
	assert.equal(
		result.error,
		undefined,
		`${command} could not run: ${result.error?.message}`,
	);
	assert.equal(
		result.status,
		0,
		[`${command} ${args.join(" ")} failed`, result.stdout, result.stderr].join(
			"\n",
		),
	);
	return result;
};

const publishablePackages = await discoverPublishableWorkspacePackages({
	repositoryRoot,
});
const publishableNames = publishablePackages.map(({ name }) => name);
assert.deepEqual(
	publishableNames,
	[...publishableNames].sort(compareStrings),
	"publishable package discovery must be deterministic",
);
assert.equal(
	new Set(publishableNames).size,
	publishableNames.length,
	"publishable package names must be unique",
);
assert(
	publishablePackages.every(
		({ manifest }) =>
			manifest.private !== true && manifest.publishConfig?.access === "public",
	),
	"every selected package must explicitly be public",
);

const workspaceSummaries = JSON.parse(
	run(pnpmCommand, ["--recursive", "list", "--depth", "-1", "--json"]).stdout,
);
const publicPackageWorkspaceNames = workspaceSummaries
	.filter((summary) => {
		const relativeDirectory = relative(repositoryRoot, resolve(summary.path));
		return (
			relativeDirectory.startsWith(`packages${sep}`) &&
			!isAbsolute(relativeDirectory) &&
			summary.private !== true
		);
	})
	.map(({ name }) => name)
	.sort(compareStrings);
assert.deepEqual(
	publishableNames,
	publicPackageWorkspaceNames,
	"publisher discovery must select every public package workspace",
);

const reverseDependentNames = Object.freeze([
	"@peerbit/clock-service",
	"@peerbit/document-proxy",
	"@peerbit/identity-access-controller",
	"@peerbit/libp2p-test-utils",
	"@peerbit/shared-log-proxy",
	"@peerbit/string",
	"@peerbit/test-utils",
	"@peerbit/trusted-network",
]);
for (const packageName of reverseDependentNames) {
	assert(
		publishableNames.includes(packageName),
		`${packageName}: reverse-dependent release must remain in consumer coverage`,
	);
}

assert.equal(packageDirectories.length, 13);
const publishableByDirectory = new Map(
	publishablePackages.map((workspacePackage) => [
		workspacePackage.directory,
		workspacePackage,
	]),
);
const rootPackageNames = packageDirectories.map((packageDirectory) => {
	const workspacePackage = publishableByDirectory.get(packageDirectory);
	assert(
		workspacePackage,
		`${packageDirectory}: security root is not publishable`,
	);
	return workspacePackage.name;
});
const changesetIsPresent = await validatePublishedSecurityCoverage({
	packageNames: rootPackageNames,
	changesetPath: join(
		repositoryRoot,
		".changeset",
		"secure-dependency-lines.md",
	),
});

if (changesetIsPresent) {
	const statusRoot = await mkdtemp(join(tmpdir(), "peerbit-changeset-status-"));
	try {
		await copyFile(
			join(repositoryRoot, "package.json"),
			join(statusRoot, "package.json"),
		);
		await copyFile(
			join(repositoryRoot, "pnpm-workspace.yaml"),
			join(statusRoot, "pnpm-workspace.yaml"),
		);
		for (const workspaceSummary of workspaceSummaries) {
			const workspaceDirectory = resolve(workspaceSummary.path);
			const relativeDirectory = relative(repositoryRoot, workspaceDirectory);
			if (!relativeDirectory) {
				continue;
			}
			assert(
				!isAbsolute(relativeDirectory) &&
					!relativeDirectory.startsWith(`..${sep}`),
				`${workspaceSummary.path}: workspace escaped the repository root`,
			);
			const fixtureDirectory = join(statusRoot, relativeDirectory);
			await mkdir(fixtureDirectory, { recursive: true });
			await copyFile(
				join(workspaceDirectory, "package.json"),
				join(fixtureDirectory, "package.json"),
			);
		}

		const changesetDirectory = join(statusRoot, ".changeset");
		await mkdir(changesetDirectory, { recursive: true });
		await copyFile(
			join(repositoryRoot, ".changeset", "config.json"),
			join(changesetDirectory, "config.json"),
		);
		run(gitCommand, ["init", "--initial-branch=master"], { cwd: statusRoot });
		run(gitCommand, ["config", "user.name", "Peerbit Release Plan Test"], {
			cwd: statusRoot,
		});
		run(gitCommand, ["config", "user.email", "release-plan-test@peerbit.org"], {
			cwd: statusRoot,
		});
		run(gitCommand, ["config", "commit.gpgsign", "false"], {
			cwd: statusRoot,
		});
		run(gitCommand, ["add", "."], { cwd: statusRoot });
		run(gitCommand, ["commit", "-m", "fixture baseline"], {
			cwd: statusRoot,
		});

		await copyFile(
			join(repositoryRoot, ".changeset", "secure-dependency-lines.md"),
			join(changesetDirectory, "secure-dependency-lines.md"),
		);
		run(gitCommand, ["add", ".changeset/secure-dependency-lines.md"], {
			cwd: statusRoot,
		});
		run(gitCommand, ["commit", "-m", "add security release plan"], {
			cwd: statusRoot,
		});

		const statusPath = join(statusRoot, "status.json");
		run(
			process.execPath,
			[
				changesetsCliPath,
				"status",
				"--since",
				"HEAD~1",
				"--output",
				statusPath,
			],
			{ cwd: statusRoot },
		);
		const releasePlan = JSON.parse(await readFile(statusPath, "utf8"));
		const releasesByName = new Map(
			releasePlan.releases.map((release) => [release.name, release]),
		);
		for (const packageName of reverseDependentNames) {
			assert.equal(
				releasesByName.get(packageName)?.type,
				"patch",
				`${packageName}: real changeset plan lost the reverse-dependent patch release`,
			);
		}
	} finally {
		await rm(statusRoot, { recursive: true, force: true });
	}
}

const fixturePackage = ({
	directory,
	name,
	version = "1.0.0",
	private: isPrivate = false,
	...runtimeFields
}) => {
	const dir = resolve(repositoryRoot, directory);
	const manifest = {
		name,
		version,
		...(isPrivate
			? { private: true }
			: { publishConfig: { access: "public" } }),
		...runtimeFields,
	};
	return { dir, directory, manifest, name, version };
};

const cyclePackages = [
	fixturePackage({
		directory: "packages/fixtures/root",
		name: "@fixture/root",
		dependencies: { "@fixture/optional": "workspace:*" },
	}),
	fixturePackage({
		directory: "packages/fixtures/optional",
		name: "@fixture/optional",
		optionalDependencies: { "@fixture/peer": "workspace:*" },
	}),
	fixturePackage({
		directory: "packages/fixtures/peer",
		name: "@fixture/peer",
		peerDependencies: { "@fixture/root": "workspace:*" },
	}),
];
assert.deepEqual(
	validateAndSelectPublishablePackages([...cyclePackages].reverse()).map(
		({ name }) => name,
	),
	["@fixture/optional", "@fixture/peer", "@fixture/root"],
	"consumer selection must include every public package even across a cycle",
);
assert.throws(
	() => sortPublishablePackages(cyclePackages),
	/Unable to topologically sort/,
	"the publisher must fail closed when no safe dependency order exists",
);

const reverseDependencyFixture = [
	fixturePackage({
		directory: "packages/fixtures/changed",
		name: "@fixture/changed",
	}),
	fixturePackage({
		directory: "packages/fixtures/reverse-dependent",
		name: "@fixture/reverse-dependent",
		dependencies: { "@fixture/changed": "workspace:^" },
	}),
];
assert.deepEqual(
	validateAndSelectPublishablePackages(reverseDependencyFixture).map(
		({ name }) => name,
	),
	["@fixture/changed", "@fixture/reverse-dependent"],
	"consumer selection must not drop reverse dependents outside a forward closure",
);

assert.throws(
	() =>
		validateAndSelectPublishablePackages([
			fixturePackage({
				directory: "packages/fixtures/root",
				name: "@fixture/root",
				dependencies: { "@fixture/missing": "workspace:*" },
			}),
		]),
	/missing workspace dependency @fixture\/missing/,
);
assert.throws(
	() =>
		validateAndSelectPublishablePackages([
			fixturePackage({
				directory: "packages/fixtures/root",
				name: "@fixture/root",
				dependencies: { "@fixture/private": "1.0.0" },
			}),
			fixturePackage({
				directory: "packages/fixtures/private",
				name: "@fixture/private",
				private: true,
			}),
		]),
	/runtime dependency @fixture\/private is private/,
);
assert.throws(
	() =>
		validateAndSelectPublishablePackages([
			fixturePackage({
				directory: "apps/fixtures/outside",
				name: "@fixture/outside",
			}),
		]),
	/outside the publishable packages tree/,
);
assert.throws(() => {
	const unversioned = fixturePackage({
		directory: "packages/fixtures/unversioned",
		name: "@fixture/unversioned",
	});
	delete unversioned.manifest.version;
	unversioned.version = undefined;
	return validateAndSelectPublishablePackages([unversioned]);
}, /package has no version/);
assert.throws(() => {
	const missingAccess = fixturePackage({
		directory: "packages/fixtures/missing-access",
		name: "@fixture/missing-access",
	});
	delete missingAccess.manifest.publishConfig;
	return validateAndSelectPublishablePackages([missingAccess]);
}, /publishConfig.access=public/);
assert.throws(
	() =>
		validateAndSelectPublishablePackages([
			fixturePackage({
				directory: "packages/fixtures/duplicate-a",
				name: "@fixture/duplicate",
			}),
			fixturePackage({
				directory: "packages/fixtures/duplicate-b",
				name: "@fixture/duplicate",
			}),
		]),
	/duplicate workspace package name/,
);
assert.throws(() => {
	const first = fixturePackage({
		directory: "packages/fixtures/duplicate-directory",
		name: "@fixture/first",
	});
	return validateAndSelectPublishablePackages([
		first,
		{
			...first,
			name: "@fixture/second",
			manifest: { ...first.manifest, name: "@fixture/second" },
		},
	]);
}, /duplicate workspace package directory/);

const postVersionRoot = await mkdtemp(
	join(tmpdir(), "peerbit-post-version-publishable-"),
);
try {
	const nonce = `${process.pid}-${Date.now()}`;
	const version = `0.0.0-security-smoke.${Date.now()}`;
	const fixtureDefinitions = [
		{
			directory: "packages/post-version/root",
			name: `@peerbit/security-smoke-root-${nonce}`,
			dependencies: {},
		},
		{
			directory: "packages/post-version/optional",
			name: `@peerbit/security-smoke-optional-${nonce}`,
			optionalDependencies: {},
		},
		{
			directory: "packages/post-version/peer",
			name: `@peerbit/security-smoke-peer-${nonce}`,
			peerDependencies: {},
		},
	];
	const [rootFixture, optionalFixture, peerFixture] = fixtureDefinitions;
	rootFixture.dependencies[optionalFixture.name] = version;
	optionalFixture.optionalDependencies[peerFixture.name] = version;
	peerFixture.peerDependencies[rootFixture.name] = version;

	for (const fixture of fixtureDefinitions) {
		const absoluteDirectory = join(postVersionRoot, fixture.directory);
		await mkdir(absoluteDirectory, { recursive: true });
		const manifest = {
			name: fixture.name,
			version,
			type: "module",
			exports: "./index.js",
			publishConfig: { access: "public" },
			...(fixture.dependencies ? { dependencies: fixture.dependencies } : {}),
			...(fixture.optionalDependencies
				? { optionalDependencies: fixture.optionalDependencies }
				: {}),
			...(fixture.peerDependencies
				? { peerDependencies: fixture.peerDependencies }
				: {}),
		};
		await writeFile(
			join(absoluteDirectory, "package.json"),
			`${JSON.stringify(manifest, null, 2)}\n`,
		);
		await writeFile(join(absoluteDirectory, "index.js"), "export default 1;\n");
	}

	const postVersionPackages = await discoverPublishableWorkspacePackages({
		repositoryRoot: postVersionRoot,
	});
	assert.equal(postVersionPackages.length, fixtureDefinitions.length);
	assert.deepEqual(
		postVersionPackages.map(({ name }) => name),
		fixtureDefinitions.map(({ name }) => name).sort(compareStrings),
	);
	const consumedChangesetPath = join(
		postVersionRoot,
		".changeset",
		"consumed-security-changeset.md",
	);
	assert.equal(
		await validatePublishedSecurityCoverage({
			packageNames: [rootFixture.name],
			changesetPath: consumedChangesetPath,
		}),
		false,
	);

	const tarballDirectory = join(postVersionRoot, "tarballs");
	const consumerDirectory = join(postVersionRoot, "consumer");
	await mkdir(tarballDirectory);
	await mkdir(consumerDirectory);
	const localDependencies = {};
	for (const { dir, manifest } of postVersionPackages) {
		const packed = JSON.parse(
			run(
				npmCommand,
				["pack", dir, "--pack-destination", tarballDirectory, "--json"],
				{ cwd: postVersionRoot },
			).stdout,
		)[0];
		assert.equal(packed.id, `${manifest.name}@${manifest.version}`);
		localDependencies[manifest.name] = `file:${join(
			tarballDirectory,
			packed.filename,
		)}`;
	}
	await writeFile(
		join(consumerDirectory, "package.json"),
		`${JSON.stringify(
			{
				name: "post-version-security-consumer",
				private: true,
				dependencies: localDependencies,
			},
			null,
			2,
		)}\n`,
	);
	const isolatedCache = join(postVersionRoot, "npm-cache");
	run(
		npmCommand,
		[
			"install",
			"--ignore-scripts",
			"--offline",
			"--no-audit",
			"--no-fund",
			"--loglevel=error",
		],
		{
			cwd: consumerDirectory,
			timeout: 300_000,
			env: {
				npm_config_cache: isolatedCache,
				npm_config_registry: "http://127.0.0.1:9/",
			},
		},
	);
	const lockfile = JSON.parse(
		await readFile(join(consumerDirectory, "package-lock.json"), "utf8"),
	);
	const lockfilePackages = Object.entries(lockfile.packages);
	for (const { manifest } of postVersionPackages) {
		const topLevelPath = `node_modules/${manifest.name}`;
		const lockEntry = lockfile.packages[topLevelPath];
		assert(lockEntry, `${manifest.name}: missing post-version lock entry`);
		assert.equal(lockEntry.version, version);
		assert.match(lockEntry.resolved, /^file:/);
		assert.doesNotMatch(lockEntry.resolved, /^https?:/);
		assert.equal(
			lockfilePackages.filter(
				([packagePath]) =>
					packagePath === topLevelPath ||
					packagePath.endsWith(`/node_modules/${manifest.name}`),
			).length,
			1,
			`${manifest.name}: offline fixture did not dedupe to one local tarball`,
		);
	}
} finally {
	await rm(postVersionRoot, { recursive: true, force: true });
}

console.log(
	`Published security contracts passed: ${packageDirectories.length} roots, every one of ${publishablePackages.length} publishable packages, all ${reverseDependentNames.length} reverse-dependent regressions, fail-closed metadata, and offline cyclic local resolution.`,
);
