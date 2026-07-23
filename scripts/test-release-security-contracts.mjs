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
const workflowJob = (workflow, jobName) => {
	const marker = `  ${jobName}:\n`;
	const start = workflow.indexOf(marker);
	assert(start >= 0, `workflow must contain the ${jobName} job`);
	const remainder = workflow.slice(start + marker.length);
	const nextJob = remainder.search(/^  [A-Za-z0-9_-]+:\n/m);
	return nextJob < 0
		? workflow.slice(start)
		: workflow.slice(start, start + marker.length + nextJob);
};
const workflowSteps = (job) => {
	const lines = job.split("\n");
	const starts = lines.flatMap((line, index) =>
		/^      - /.test(line) ? [index] : [],
	);
	return starts.map((start, index) =>
		lines.slice(start, starts[index + 1] ?? lines.length).join("\n"),
	);
};
const actionSteps = (job, action) =>
	workflowSteps(job).filter((step) => step.includes(`uses: ${action}@`));

const packageManifest = JSON.parse(await readRepositoryFile("package.json"));
const scripts = packageManifest.scripts;
const documentManifest = JSON.parse(
	await readRepositoryFile(
		"packages/programs/data/document/document/package.json",
	),
);
const viteManifest = JSON.parse(
	await readRepositoryFile("packages/clients/vite/package.json"),
);
const viteNodeEngine = "^20.19.0 || >=22.12.0";

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
	viteManifest.engines?.node,
	viteNodeEngine,
	"@peerbit/vite must declare the Node.js floor imposed by Vite 7",
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
assert.match(
	releaseWorkflow,
	/^permissions:\n  contents: read$/m,
	"release workflow jobs must inherit a read-only GITHUB_TOKEN by default",
);
assert.doesNotMatch(
	releaseWorkflow,
	/^\s+(?:contents|issues|pull-requests): write$/m,
	"release jobs must perform GitHub writes only through the explicit bot PAT",
);
assert.match(
	releaseWorkflow,
	/release:\n    if: \$\{\{ vars\.ACTIONS_SECRETS_MIGRATED == 'true' && github\.ref == 'refs\/heads\/master'/,
	"stable releases must require completed credential migration and the master ref",
);
assert.match(
	releaseWorkflow,
	/release-rc:\n    if: \$\{\{ vars\.ACTIONS_SECRETS_MIGRATED == 'true' && github\.ref == 'refs\/heads\/master'/,
	"release candidates must require completed credential migration and the master ref",
);
const stableReleaseJob = workflowJob(releaseWorkflow, "release");
const releaseCandidateJob = workflowJob(releaseWorkflow, "release-rc");
for (const [name, job] of [
	["stable", stableReleaseJob],
	["release candidate", releaseCandidateJob],
]) {
	assert.match(
		job,
		/^    environment: npm-release$/m,
		`${name} publication must obtain secrets from the master-restricted release environment`,
	);
	const checkouts = actionSteps(job, "actions/checkout");
	assert.equal(
		checkouts.length,
		1,
		`${name} publication must have one checkout`,
	);
	assert.match(
		checkouts[0],
		/persist-credentials: false/,
		`${name} checkout must not persist the Peerbit Bot PAT`,
	);
	assert.doesNotMatch(
		checkouts[0],
		/^\s+token:/m,
		`${name} checkout must use only the read-only workflow token`,
	);
	assert.match(
		job,
		/git config --local credential\.helper '!gh auth git-credential'/,
		`${name} publication must resolve git credentials from the step-scoped bot token`,
	);
}
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
assert.match(
	ciWorkflow,
	/^permissions:\n  contents: read$/m,
	"ordinary CI must use a read-only GITHUB_TOKEN",
);
assert.doesNotMatch(
	ciWorkflow,
	/\$\{\{\s*secrets\./,
	"CI must not depend on long-lived repository secrets",
);
const pullRequestJob = workflowJob(ciWorkflow, "test_pr");
assert.doesNotMatch(
	pullRequestJob,
	/\$\{\{\s*secrets\./,
	"pull-request tests must not receive repository secrets",
);
assert.doesNotMatch(
	pullRequestJob,
	/codecov-action/,
	"pull-request coverage must not upload with the repository Codecov token",
);
const pushTestJob = workflowJob(ciWorkflow, "test_push");
assert.doesNotMatch(
	pushTestJob,
	/id-token: write|codecov-action/,
	"repository tests and dependency installation must not receive an OIDC identity",
);
assert.match(
	pushTestJob,
	/name: Upload coverage artifact[\s\S]*?include-hidden-files: true/,
	"trusted push tests must hand hidden coverage files to the isolated uploader",
);
const coverageJob = workflowJob(ciWorkflow, "coverage_push");
assert.match(
	coverageJob,
	/permissions:\n      contents: read\n      id-token: write/,
	"the isolated coverage upload must receive only read access and an OIDC identity",
);
assert.match(
	coverageJob,
	/uses: codecov\/codecov-action@[0-9a-f]{40} # v5[\s\S]*?use_oidc: true/,
	"the isolated coverage upload must use a commit-pinned Codecov action with OIDC",
);
assert.doesNotMatch(
	coverageJob,
	/merge-multiple: true/,
	"coverage artifacts must stay in separate directories to avoid filename collisions",
);
assert.doesNotMatch(
	coverageJob,
	/(?:CODECOV_TOKEN|^\s+token:)/m,
	"the isolated coverage upload must not use a long-lived Codecov token",
);
const securityJob = workflowJob(ciWorkflow, "security_dependency_contracts");
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

const nightlyWorkflow = await readRepositoryFile(
	".github/workflows/nightly-sims.yml",
);
assert.match(
	nightlyWorkflow,
	/^permissions:\n  contents: read$/m,
	"nightly simulations must use a read-only GITHUB_TOKEN",
);

const siteWorkflow = await readRepositoryFile(".github/workflows/site.yml");
assert.match(
	siteWorkflow,
	/^permissions:\n  contents: read$/m,
	"site builds must inherit only read access",
);
assert.match(
	siteWorkflow,
	/group: pages-\$\{\{ github\.event\.pull_request\.number \|\| github\.ref \}\}/,
	"pull requests must not share a cancellation group with production deploys",
);
const sitePullRequestJob = workflowJob(siteWorkflow, "build_pr");
assert.doesNotMatch(
	sitePullRequestJob,
	/^\s+(?:pages|id-token): write$/m,
	"the pull-request site build must not receive deployment permissions",
);
assert.match(
	sitePullRequestJob,
	/^    if: github\.event_name == 'pull_request'$/m,
	"the secret-free site build must be pull-request-only",
);
assert.doesNotMatch(
	sitePullRequestJob,
	/\$\{\{\s*secrets\./,
	"the pull-request site build must not reference any secrets",
);
const siteProductionJob = workflowJob(siteWorkflow, "build");
assert.match(
	siteProductionJob,
	/^    if: github\.event_name == 'push' && github\.ref == 'refs\/heads\/master'$/m,
	"the secret-bearing production build must be push-only on master",
);
assert.match(
	siteProductionJob,
	/environment:\n      name: github-pages\n      deployment: false/,
	"production site secrets must come from the master-restricted Pages environment",
);
const siteDeployJob = workflowJob(siteWorkflow, "deploy");
assert.match(
	siteDeployJob,
	/permissions:\n      pages: write\n      id-token: write/,
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
	/^permissions:\n  contents: read$/m,
	"post-release jobs must inherit only read access",
);
assert.doesNotMatch(
	postReleaseWorkflow,
	/^\s+(?:contents|pull-requests): write$/m,
	"post-release GitHub writes must use the explicit bot PAT",
);
for (const [name, job] of [
	["workspace restore", workflowJob(postReleaseWorkflow, "restore")],
	[
		"bootstrap rollout",
		workflowJob(postReleaseWorkflow, "bootstrap-rollout-pr"),
	],
]) {
	assert.match(
		job,
		/head_branch == 'master'/,
		`${name} must require a successful master release`,
	);
	assert.match(
		job,
		/vars\.ACTIONS_SECRETS_MIGRATED == 'true'/,
		`${name} must wait for repository-wide secret migration`,
	);
	assert.match(
		job,
		/^    environment: post-release$/m,
		`${name} must obtain bot credentials from the master-restricted post-release environment`,
	);
}
const postReleaseCheckouts = actionSteps(
	postReleaseWorkflow,
	"actions/checkout",
);
assert.equal(postReleaseCheckouts.length, 3);
for (const checkout of postReleaseCheckouts) {
	assert.match(
		checkout,
		/persist-credentials: false/,
		"every post-release checkout must avoid persisting default or bot credentials",
	);
	if (checkout.includes("PEERBIT_BOOTSTRAP_PR_TOKEN")) {
		assert.match(
			checkout,
			/uses: actions\/checkout@[0-9a-f]{40} # v4/,
			"the token-bearing cross-repository checkout must be commit-pinned",
		);
	}
}
const pullRequestActions = actionSteps(
	postReleaseWorkflow,
	"peter-evans/create-pull-request",
);
assert.equal(pullRequestActions.length, 2);
for (const pullRequestAction of pullRequestActions) {
	assert.match(
		pullRequestAction,
		/uses: peter-evans\/create-pull-request@[0-9a-f]{40} # v6/,
		"every bot-credentialed pull-request action must be commit-pinned",
	);
}
assert.match(
	postReleaseWorkflow,
	/name: Use Node\.js[\s\S]*?node-version: 22/,
	"post-release automation must use the supported Node floor",
);
assert.match(
	publishedSecuritySmoke,
	/packedPackages\.get\("@peerbit\/vite"\)/,
	"the published-package gate must inspect the packed @peerbit/vite manifest",
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
