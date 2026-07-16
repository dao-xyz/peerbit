import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	packageDirectories,
	validatePublishedSecurityCoverage,
} from "./published-security-coverage.mjs";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const readRepositoryFile = (path) =>
	readFile(resolve(repositoryRoot, path), "utf8");

const packageManifest = JSON.parse(await readRepositoryFile("package.json"));
const scripts = packageManifest.scripts;
const documentManifest = JSON.parse(
	await readRepositoryFile(
		"packages/programs/data/document/document/package.json",
	),
);

assert.equal(
	documentManifest.dependencies?.["@peerbit/time"],
	"workspace:*",
	"@peerbit/document must own its runtime @peerbit/time import",
);
assert.equal(
	documentManifest.devDependencies?.["@peerbit/time"],
	undefined,
	"@peerbit/document must not hide @peerbit/time in devDependencies",
);

assert.equal(
	scripts["release:security-gate"],
	"pnpm run test:release-security-contracts && pnpm run test:security-dependencies && pnpm run test:security-published-closure && pnpm run test:security-published && pnpm dlx pnpm@11.13.0 with current audit --prod && pnpm dlx pnpm@11.13.0 with current audit",
	"the shared release gate must fail closed on its contract test, dependency probe, focused publication-closure proof, full published-package smoke, and both root audits",
);
assert.equal(
	scripts.release,
	"pnpm run build && pnpm run release:security-gate && node ./scripts/publish-public-packages.mjs",
	"stable publication must build and pass the shared gate before publishing",
);
assert.equal(
	scripts["release:publish"],
	"pnpm run release && changeset tag && git push origin --tags",
	"the changesets path must delegate publication to the guarded stable release",
);
assert.equal(
	scripts["release:rc"],
	"pnpm run build && pnpm run release:security-gate && AEGIR_PACKAGE_MANAGER=pnpm aegir release-rc",
	"release-candidate publication must build and pass the shared gate first",
);

const releaseWorkflow = await readRepositoryFile(
	".github/workflows/release.yml",
);
const frozenInstalls = releaseWorkflow.match(
	/pnpm install --frozen-lockfile(?:\s|$)/g,
);
assert.equal(
	frozenInstalls?.length,
	2,
	"stable and RC release jobs must install the committed lockfile exactly",
);
assert.doesNotMatch(
	releaseWorkflow,
	/pnpm install --frozen-lockfile=false/,
	"release jobs may not relax the committed lockfile",
);
assert.match(
	releaseWorkflow,
	/publish: pnpm run release:publish/,
	"changesets publication must use the guarded release:publish script",
);
assert.match(
	releaseWorkflow,
	/name: Build, gate, and publish stable packages[\s\S]*?run: pnpm run release(?:\n|$)/,
	"the manual stable escape hatch must use the guarded release script",
);
assert.match(
	releaseWorkflow,
	/name: Build, gate, and publish RC to NPM[\s\S]*?run: pnpm run release:rc(?:\n|$)/,
	"the RC workflow must use the guarded release:rc script",
);
assert.doesNotMatch(
	releaseWorkflow,
	/run: pnpm run --if-present release(?::rc)?/,
	"release workflows must not silently skip a missing guarded script",
);

const ciWorkflow = await readRepositoryFile(".github/workflows/ci.yml");
const securityJobStart = ciWorkflow.indexOf("  security_dependency_contracts:");
const securityJobEnd = ciWorkflow.indexOf("\n  test_push:", securityJobStart);
assert(securityJobStart >= 0 && securityJobEnd > securityJobStart);
const securityJob = ciWorkflow.slice(securityJobStart, securityJobEnd);
assert.match(securityJob, /needs: build_workspace/);
assert.match(securityJob, /node-version: \[22\.x, 24\.x\]/);
assert.match(securityJob, /node-version: \$\{\{ matrix\.node-version \}\}/);
assert.match(securityJob, /pnpm install --frozen-lockfile/);
const restoreIndex = securityJob.indexOf("Restore workspace build outputs");
const gateIndex = securityJob.indexOf("pnpm run release:security-gate");
assert(
	restoreIndex >= 0 && gateIndex > restoreIndex,
	"CI must run the same release gate only after restoring built artifacts",
);

const publishedSecuritySmoke = await readRepositoryFile(
	"scripts/test-published-security-smoke.mjs",
);
const coverageValidationIndex = publishedSecuritySmoke.indexOf(
	"await validatePublishedSecurityCoverage({",
);
const cleanConsumerIndex = publishedSecuritySmoke.indexOf(
	"await writeFile(",
	coverageValidationIndex,
);
assert(
	coverageValidationIndex >= 0 && cleanConsumerIndex > coverageValidationIndex,
	"the real package smoke must validate optional changeset coverage and then continue into the clean consumer",
);
const coverageInvocation = publishedSecuritySmoke.slice(
	coverageValidationIndex,
	cleanConsumerIndex,
);
assert.match(coverageInvocation, /packageNames: rootPackageNames/);
assert.match(coverageInvocation, /changesetPath: join\(/);
assert.match(
	publishedSecuritySmoke,
	/const publishablePackages = await discoverPublishableWorkspacePackages\(/,
);
assert.match(
	publishedSecuritySmoke,
	/for \(const \{ directory: packageDirectory, manifest \} of publishablePackages\)/,
);
assert.doesNotMatch(
	publishedSecuritySmoke,
	/expectedPublishedSecurityClosureNames|resolvePublishedSecurityClosure/,
	"the consumer proof must not use a static forward closure",
);
assert.match(
	publishedSecuritySmoke,
	/test-published-crypto-package-smoke\.mjs/,
	"the full published-package gate must include the isolated crypto package smoke",
);
assert.match(
	publishedSecuritySmoke,
	/NPM_CONFIG_ENGINE_STRICT: "true"/,
	"the clean published-package install must reject unsupported Node engines",
);
for (const packagePath of [
	"packages/clients/peerbit/package.json",
	"packages/transport/stream/package.json",
]) {
	const manifest = JSON.parse(await readRepositoryFile(packagePath));
	assert.equal(
		manifest.engines?.node,
		">=22",
		`${manifest.name}: declared Node engine must match its runtime dependency floor`,
	);
}
const postReleaseWorkflow = await readRepositoryFile(
	".github/workflows/post-release.yml",
);
assert.match(
	postReleaseWorkflow,
	/name: Use Node\.js[\s\S]*?node-version: 22/,
	"post-release automation must use the supported Node floor",
);
const publishedCryptoPackageSmoke = await readRepositoryFile(
	"scripts/test-published-crypto-package-smoke.mjs",
);
assert.match(publishedCryptoPackageSmoke, /--install-strategy=nested/);
assert.match(publishedCryptoPackageSmoke, /node@18/);
assert.match(publishedCryptoPackageSmoke, /"multiformats", "uint8arrays"/);
const publicPackagePublisher = await readRepositoryFile(
	"scripts/publish-public-packages.mjs",
);
assert.match(
	publicPackagePublisher,
	/from "\.\/publishable-workspace-packages\.mjs"/,
	"the publisher and consumer proof must share one package discovery boundary",
);
assert.match(
	publicPackagePublisher,
	/await discoverPublishableWorkspacePackages\(/,
);
assert.doesNotMatch(
	publicPackagePublisher,
	/function findPackageJsonFiles|function loadWorkspacePackages/,
	"the publisher must not carry an independent permissive package scanner",
);
assert.equal(
	packageDirectories.length,
	13,
	"published security coverage must retain all 13 package candidates after the changeset is consumed",
);
const publishedPackageNames = await Promise.all(
	packageDirectories.map(async (packageDirectory) => {
		const manifest = JSON.parse(
			await readRepositoryFile(join(packageDirectory, "package.json")),
		);
		return manifest.name;
	}),
);
assert.equal(
	new Set(publishedPackageNames).size,
	publishedPackageNames.length,
	"published security package candidates must be unique",
);

const postVersionRoot = await mkdtemp(
	join(tmpdir(), "peerbit-post-version-security-"),
);
const consumedChangesetPath = join(
	postVersionRoot,
	".changeset",
	"secure-dependency-lines.md",
);
try {
	assert.equal(
		await validatePublishedSecurityCoverage({
			packageNames: publishedPackageNames,
			changesetPath: consumedChangesetPath,
		}),
		false,
		"a consumed security changeset must not block the post-version release gate",
	);

	await mkdir(dirname(consumedChangesetPath), { recursive: true });
	await writeFile(consumedChangesetPath, "---\n---\n", "utf8");
	await assert.rejects(
		validatePublishedSecurityCoverage({
			packageNames: publishedPackageNames,
			changesetPath: consumedChangesetPath,
		}),
		/security changeset has no packages/,
		"an existing malformed changeset must still fail closed",
	);
	await assert.rejects(
		validatePublishedSecurityCoverage({
			packageNames: publishedPackageNames,
			changesetPath: dirname(consumedChangesetPath),
		}),
		(error) => error?.code === "EISDIR",
		"only ENOENT may be treated as a consumed changeset",
	);
} finally {
	await rm(postVersionRoot, { recursive: true, force: true });
}

console.log(
	"Release security contracts passed: frozen installs, post-version changeset consumption, and every stable/RC publication path use the shared post-build gate.",
);
