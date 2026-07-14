import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";

export const runtimeDependencyFields = Object.freeze([
	"dependencies",
	"optionalDependencies",
	"peerDependencies",
]);

const compareStrings = (left, right) =>
	left < right ? -1 : left > right ? 1 : 0;

const assertNonEmptyString = (value, message) => {
	assert.equal(typeof value, "string", message);
	assert(value.trim().length > 0, message);
};

const findPackageJsonFiles = async (directory) => {
	const entries = (await readdir(directory, { withFileTypes: true })).sort(
		(left, right) => compareStrings(left.name, right.name),
	);
	const packageJsonFiles = [];
	for (const entry of entries) {
		if (entry.name === "node_modules" || entry.name === "dist") {
			continue;
		}
		const absolutePath = join(directory, entry.name);
		if (entry.isDirectory()) {
			packageJsonFiles.push(...(await findPackageJsonFiles(absolutePath)));
		} else if (entry.isFile() && entry.name === "package.json") {
			packageJsonFiles.push(absolutePath);
		}
	}
	return packageJsonFiles;
};

export const validateAndSelectPublishablePackages = (workspacePackages) => {
	assert(
		Array.isArray(workspacePackages),
		"workspace packages must be an array",
	);
	const byName = new Map();
	const byDirectory = new Map();
	const byAbsoluteDirectory = new Map();
	for (const workspacePackage of workspacePackages) {
		assertNonEmptyString(
			workspacePackage?.dir,
			"workspace package has no absolute directory",
		);
		assertNonEmptyString(
			workspacePackage?.directory,
			"workspace package has no repository-relative directory",
		);
		assert(
			isAbsolute(workspacePackage.dir),
			`${workspacePackage.directory}: workspace package directory must be absolute`,
		);
		assert(
			workspacePackage.directory.startsWith("packages/") &&
				workspacePackage.directory
					.split("/")
					.every(
						(segment) => segment !== "" && segment !== "." && segment !== "..",
					),
			`${workspacePackage.directory}: package is outside the publishable packages tree`,
		);
		assert(
			workspacePackage.manifest &&
				typeof workspacePackage.manifest === "object" &&
				!Array.isArray(workspacePackage.manifest),
			`${workspacePackage.directory}: package has no manifest object`,
		);
		const { manifest } = workspacePackage;
		assertNonEmptyString(
			manifest.name,
			`${workspacePackage.directory}: package has no name`,
		);
		assertNonEmptyString(
			manifest.version,
			`${manifest.name}: package has no version`,
		);
		assert.equal(
			workspacePackage.name,
			manifest.name,
			`${workspacePackage.directory}: package name drift`,
		);
		assert.equal(
			workspacePackage.version,
			manifest.version,
			`${manifest.name}: package version drift`,
		);
		if (Object.hasOwn(manifest, "private")) {
			assert.equal(
				typeof manifest.private,
				"boolean",
				`${manifest.name}: private metadata must be boolean`,
			);
		}
		if (manifest.private !== true) {
			assert.equal(
				manifest.publishConfig?.access,
				"public",
				`${manifest.name}: publishable package must declare publishConfig.access=public`,
			);
		}
		assert(
			!byName.has(manifest.name),
			`duplicate workspace package name: ${manifest.name}`,
		);
		assert(
			!byDirectory.has(workspacePackage.directory),
			`duplicate workspace package directory: ${workspacePackage.directory}`,
		);
		assert(
			!byAbsoluteDirectory.has(resolve(workspacePackage.dir)),
			`duplicate absolute workspace package directory: ${workspacePackage.dir}`,
		);
		byName.set(manifest.name, workspacePackage);
		byDirectory.set(workspacePackage.directory, workspacePackage);
		byAbsoluteDirectory.set(resolve(workspacePackage.dir), workspacePackage);
	}

	const publishablePackages = workspacePackages
		.filter(({ manifest }) => manifest.private !== true)
		.sort((left, right) =>
			compareStrings(left.manifest.name, right.manifest.name),
		);
	const publishableNames = new Set(
		publishablePackages.map(({ manifest }) => manifest.name),
	);
	for (const workspacePackage of publishablePackages) {
		for (const dependencyField of runtimeDependencyFields) {
			const dependencySet = workspacePackage.manifest[dependencyField];
			if (dependencySet === undefined) {
				continue;
			}
			assert(
				dependencySet &&
					typeof dependencySet === "object" &&
					!Array.isArray(dependencySet),
				`${workspacePackage.name}: ${dependencyField} must be an object`,
			);
			for (const [dependencyName, dependencyRange] of Object.entries(
				dependencySet,
			).sort(([left], [right]) => compareStrings(left, right))) {
				assertNonEmptyString(
					dependencyRange,
					`${workspacePackage.name}: invalid ${dependencyField} range for ${dependencyName}`,
				);
				const dependencyPackage = byName.get(dependencyName);
				if (!dependencyPackage) {
					if (dependencyRange.startsWith("workspace:")) {
						throw new Error(
							`${workspacePackage.name}: missing workspace dependency ${dependencyName}`,
						);
					}
					continue;
				}
				if (!publishableNames.has(dependencyName)) {
					throw new Error(
						`${workspacePackage.name}: runtime dependency ${dependencyName} is private`,
					);
				}
			}
		}
	}
	return publishablePackages;
};

export const discoverPublishableWorkspacePackages = async ({
	repositoryRoot,
}) => {
	const absoluteRepositoryRoot = resolve(repositoryRoot);
	const packageJsonFiles = await findPackageJsonFiles(
		join(absoluteRepositoryRoot, "packages"),
	);
	const workspacePackages = await Promise.all(
		packageJsonFiles.map(async (packageJsonFile) => {
			const absoluteDirectory = dirname(packageJsonFile);
			const directory = relative(absoluteRepositoryRoot, absoluteDirectory)
				.split(sep)
				.join("/");
			const manifest = JSON.parse(await readFile(packageJsonFile, "utf8"));
			return {
				dir: absoluteDirectory,
				directory,
				manifest,
				name: manifest.name,
				packageJsonFile,
				version: manifest.version,
			};
		}),
	);
	return validateAndSelectPublishablePackages(workspacePackages);
};

const getInternalDependencies = (manifest, packageNames) => {
	const internalDependencies = new Set();
	for (const dependencyField of runtimeDependencyFields) {
		for (const dependencyName of Object.keys(manifest[dependencyField] ?? {})) {
			if (packageNames.has(dependencyName)) {
				internalDependencies.add(dependencyName);
			}
		}
	}
	return internalDependencies;
};

export const sortPublishablePackages = (publishablePackages) => {
	const packages = validateAndSelectPublishablePackages(publishablePackages);
	const byName = new Map(
		packages.map((workspacePackage) => [
			workspacePackage.name,
			workspacePackage,
		]),
	);
	const packageNames = new Set(byName.keys());
	const reverseEdges = new Map();
	const indegree = new Map();

	for (const workspacePackage of packages) {
		const internalDependencies = getInternalDependencies(
			workspacePackage.manifest,
			packageNames,
		);
		indegree.set(workspacePackage.name, internalDependencies.size);
		for (const dependencyName of internalDependencies) {
			const dependents = reverseEdges.get(dependencyName) ?? new Set();
			dependents.add(workspacePackage.name);
			reverseEdges.set(dependencyName, dependents);
		}
	}

	const queue = packages
		.filter(({ name }) => (indegree.get(name) ?? 0) === 0)
		.map(({ name }) => name)
		.sort(compareStrings);
	const ordered = [];
	while (queue.length > 0) {
		const packageName = queue.shift();
		ordered.push(byName.get(packageName));
		for (const dependentName of [...(reverseEdges.get(packageName) ?? [])].sort(
			compareStrings,
		)) {
			const nextIndegree = (indegree.get(dependentName) ?? 0) - 1;
			indegree.set(dependentName, nextIndegree);
			if (nextIndegree === 0) {
				queue.push(dependentName);
				queue.sort(compareStrings);
			}
		}
	}
	if (ordered.length !== packages.length) {
		throw new Error(
			"Unable to topologically sort publishable workspace packages",
		);
	}
	return ordered;
};
