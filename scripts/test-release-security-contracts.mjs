import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { validateChangesetGuardWorkflow } from "./ci/check-changeset-required.mjs";
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
	const nextJob = remainder.search(/^ {2}[A-Za-z0-9_-]+:\n/m);
	return nextJob < 0
		? workflow.slice(start)
		: workflow.slice(start, start + marker.length + nextJob);
};
const workflowSteps = (job) => {
	const lines = job.split("\n");
	const starts = lines.flatMap((line, index) =>
		/^ {6}- /.test(line) ? [index] : [],
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
const releasingGuide = await readRepositoryFile("RELEASING.md");
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
for (const marker of [
	"CVE-2025-71330",
	"CVE-2025-71329",
	"autoInstallPeers",
	"--legacy-peer-deps",
	"node-datachannel",
	"strict zero-finding `npm audit --omit=dev`",
]) {
	assert(
		releasingGuide.includes(marker),
		"RELEASING.md must document the workspace-only image-size ignore marker " +
			marker,
	);
}

assert.equal(
	scripts["release:security-gate"],
	"pnpm run test:release-security-contracts && pnpm run test:security-dependencies && pnpm run test:security-published-closure && pnpm run test:security-published && pnpm dlx pnpm@11.13.0 with current audit --prod --ignore CVE-2025-71330 --ignore CVE-2025-71329 && pnpm dlx pnpm@11.13.0 with current audit --ignore CVE-2025-71330 --ignore CVE-2025-71329",
	"the shared release gate must fail closed on its contract tests, dependency probe, focused publication-closure proof, full published-package smoke, and both workspace root audits, whose only ignores are the two documented workspace-only image-size CVEs",
);
assert.equal(
	scripts["test:security-image-size-exception"],
	undefined,
	"the retired image-size audit exception must not return as a package script",
);
assert.doesNotMatch(
	scripts["release:security-gate"],
	/--ignore-unfixable/,
	"the release gate must never suppress all unfixable advisories",
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
	/^permissions:\n {2}contents: read$/m,
	"release workflow jobs must inherit a read-only GITHUB_TOKEN by default",
);
assert.doesNotMatch(
	releaseWorkflow,
	/^\s+(?:contents|issues|pull-requests): write$/m,
	"release jobs must perform GitHub writes only through the explicit bot PAT",
);
assert.match(
	releaseWorkflow,
	/release:\n {4}if: \$\{\{ vars\.ACTIONS_SECRETS_MIGRATED == 'true' && github\.ref == 'refs\/heads\/master'/,
	"stable releases must require completed credential migration and the master ref",
);
assert.match(
	releaseWorkflow,
	/release-rc:\n {4}if: \$\{\{ vars\.ACTIONS_SECRETS_MIGRATED == 'true' && github\.ref == 'refs\/heads\/master'/,
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
		/^ {4}environment: npm-release$/m,
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
	/^permissions:\n {2}contents: read$/m,
	"ordinary CI must use a read-only GITHUB_TOKEN",
);
assert.doesNotMatch(
	ciWorkflow,
	/\$\{\{\s*secrets\./,
	"CI must not depend on long-lived repository secrets",
);
// Master pushes must land in a UNIQUE concurrency group. Grouping them by
// github.ref does not queue them: GitHub keeps only one pending run per group
// and cancels the waiting one when a third arrives, so a burst of merges
// silently leaves master commits with no CI verdict (this happened to the
// #1244 publish commit on 2026-08-13). run_id is unique per run, so no master
// push can ever evict another.
assert.match(
	ciWorkflow,
	/group: \$\{\{ github\.workflow \}\}-\$\{\{ github\.event\.pull_request\.number \|\| github\.run_id \}\}/,
	"CI concurrency must fall back to github.run_id, not github.ref, so master pushes cannot evict each other",
);
assert.doesNotMatch(
	ciWorkflow,
	/^\s*pull_request_target:/m,
	"ordinary build/test CI must remain on pull_request; only the dedicated data-only changeset guard may use pull_request_target",
);
assert.match(
	ciWorkflow,
	/^ {2}pull_request:\n {4}types: \["opened", "reopened", "synchronize", "edited"\]$/m,
	"ordinary pull-request CI must rerun when edited activity can change the trusted base/ref contract",
);
const buildWorkspaceJob = workflowJob(ciWorkflow, "build_workspace");
const buildWorkspaceCheckouts = actionSteps(
	buildWorkspaceJob,
	"actions/checkout",
);
assert.equal(
	buildWorkspaceCheckouts.length,
	1,
	"the workspace build must have exactly one checkout",
);
assert.match(
	buildWorkspaceCheckouts[0],
	/fetch-depth: 0/,
	"the PR changeset guard must receive full history for an exact merge-base diff",
);
assert.match(
	buildWorkspaceCheckouts[0],
	/persist-credentials: false/,
	"the untrusted PR checkout must not persist even the read-only workflow credential",
);
const changesetNodeSteps = workflowSteps(buildWorkspaceJob).filter((step) =>
	step.includes("name: Setup Node for changeset coverage"),
);
assert.equal(
	changesetNodeSteps.length,
	1,
	"the workspace build must prepare the guard's Node runtime exactly once",
);
assert.match(
	changesetNodeSteps[0],
	/if: github\.event_name == 'pull_request'[\s\S]*uses: actions\/setup-node@v4[\s\S]*node-version: 22\.x/,
	"the changeset guard fixtures and production check must run on Node 22 for PRs",
);
const changesetCoverageSteps = workflowSteps(buildWorkspaceJob).filter((step) =>
	step.includes("name: Enforce per-package changeset coverage"),
);
assert.equal(
	changesetCoverageSteps.length,
	1,
	"the workspace build must enforce per-package changeset coverage exactly once",
);
const changesetCoverageStep = changesetCoverageSteps[0];
assert.match(
	changesetCoverageStep,
	/if: github\.event_name == 'pull_request'/,
	"changeset coverage must be pull-request-only so master pushes cannot be rejected",
);
assert.match(
	changesetCoverageStep,
	/TRUSTED_BASE_SHA: \$\{\{ github\.event\.pull_request\.base\.sha \}\}[\s\S]*trusted_guard_dir=\$\(mktemp -d\)[\s\S]*git show "\$\{TRUSTED_BASE_SHA\}:scripts\/ci\/check-changeset-required\.mjs"[\s\S]*git show "\$\{TRUSTED_BASE_SHA\}:scripts\/ci\/check-changeset-required\.test\.mjs"[\s\S]*node --test "\$trusted_guard_dir\/check-changeset-required\.test\.mjs"[\s\S]*node "\$trusted_guard_dir\/check-changeset-required\.mjs"/,
	"CI must extract and execute the trusted base tree's guard and fixtures before evaluating the real PR",
);
assert.match(
	changesetCoverageStep,
	/base_guard_present=0[\s\S]*git cat-file -e "\$\{TRUSTED_BASE_SHA\}:scripts\/ci\/check-changeset-required\.mjs"[\s\S]*base_guard_test_present=1[\s\S]*if \[\[ "\$base_guard_present" == 1 && "\$base_guard_test_present" == 1 \]\]; then[\s\S]*elif \[\[ "\$base_guard_present" == 0 && "\$base_guard_test_present" == 0 \]\]; then[\s\S]*cp scripts\/ci\/check-changeset-required\.mjs[\s\S]*cp scripts\/ci\/check-changeset-required\.test\.mjs[\s\S]*else[\s\S]*refusing a partial trust boundary[\s\S]*exit 1/,
	"only the one-time both-files-absent bootstrap may use the introducing PR's guard pair; a partial base must fail",
);
assert.match(
	changesetCoverageStep,
	/CHANGESET_GUARD_REPOSITORY_ROOT: \$\{\{ github\.workspace \}\}/,
	"the extracted trusted guard must inspect the checked-out repository instead of its temporary directory",
);
assert.doesNotMatch(
	changesetCoverageStep,
	/node (?:--test )?scripts\/ci\/check-changeset-required/,
	"CI must never execute the PR head's guard or guard tests",
);
assert.doesNotMatch(
	changesetCoverageStep,
	/\$\{\{\s*secrets\./,
	"the PR changeset guard must not receive repository secrets",
);

const targetChangesetWorkflow = await readRepositoryFile(
	".github/workflows/changeset-guard.yml",
);
assert.equal(
	validateChangesetGuardWorkflow(targetChangesetWorkflow),
	true,
	"the pull_request_target changeset workflow must exactly match the canonical base-owned data-only contract",
);
assert.match(
	targetChangesetWorkflow,
	/^on:\n {2}pull_request_target:\n {4}types: \[opened, reopened, synchronize, edited\]$/m,
	"the base-owned guard must run only for pull request lifecycle events that can change the head",
);
assert.match(
	targetChangesetWorkflow,
	/^permissions:\n {2}contents: read$/m,
	"the base-owned guard must have only read access to repository contents",
);
const targetChangesetJob = workflowJob(
	targetChangesetWorkflow,
	"changeset_guard",
);
const targetChangesetSteps = workflowSteps(targetChangesetJob);
assert.equal(
	targetChangesetSteps.length,
	3,
	"the target guard must contain only base checkout, Node setup, and data validation",
);
assert.match(
	targetChangesetSteps[0],
	/uses: actions\/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4[\s\S]*ref: \$\{\{ github\.event\.pull_request\.base\.sha \}\}[\s\S]*fetch-depth: 0[\s\S]*persist-credentials: false/,
	"the target guard must check out only the exact trusted event base without persisted credentials",
);
assert.match(
	targetChangesetSteps[1],
	/uses: actions\/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020 # v4[\s\S]*node-version: 22\.x/,
	"the target guard must use the commit-pinned Node 22 setup action",
);
assert.match(
	targetChangesetSteps[2],
	/PR_NUMBER: \$\{\{ github\.event\.pull_request\.number \}\}[\s\S]*TRUSTED_BASE_SHA: \$\{\{ github\.event\.pull_request\.base\.sha \}\}[\s\S]*UNTRUSTED_HEAD_SHA: \$\{\{ github\.event\.pull_request\.head\.sha \}\}[\s\S]*refs\/pull\/\$\{PR_NUMBER\}\/head:refs\/remotes\/pull-request\/head[\s\S]*fetched_head[\s\S]*\$UNTRUSTED_HEAD_SHA[\s\S]*node --test scripts\/ci\/check-changeset-required\.test\.mjs[\s\S]*node scripts\/ci\/check-changeset-required\.mjs/,
	"the target guard must fetch the PR head to a remote data ref, verify the exact event SHA, and execute only trusted-base guard files",
);
assert.doesNotMatch(
	targetChangesetWorkflow,
	/\$\{\{\s*secrets\.|(?:contents|issues|pull-requests|id-token): write|actions\/cache|actions\/(?:upload|download)-artifact|\b(?:npm|pnpm|yarn|corepack)\b|\bgit (?:checkout|switch|reset|clean)\b|ref: \$\{\{[^\n]*head/,
	"the target guard must never receive secrets, mutate GitHub, install head dependencies, cache head state, or check out/execute the untrusted head",
);
const changesetGuard = await readRepositoryFile(
	"scripts/ci/check-changeset-required.mjs",
);
for (const marker of [
	'"merge-base"',
	'"--name-status"',
	'"--find-renames"',
	'pullRequest.user?.login === "peerbit-org"',
	"pullRequest.user?.id === 273107789",
	'payload.sender?.login === "peerbit-org"',
	"payload.sender?.id === 273107789",
	'pullRequest.base?.ref === "master"',
	'pullRequest.head?.ref === "changeset-release/master"',
	"pullRequest.head.repo.full_name === pullRequest.base.repo.full_name",
	"payload.repository.full_name === pullRequest.base.repo.full_name",
	'new Set(["@peerbit/test-lib"])',
	"const CHANGESET_GUARD_PATHS = [",
	"const CHANGESET_CONFIG_SCHEMA =",
	'["ls-tree", "-r", "-z", sha]',
	"await assertChangesetGuardPair(repository, baseSha, headSha, state)",
	"frozen executable root-of-trust boundary",
	"analyzeMarkdownStructure(normalized)",
	"assertChangesetSummaryCompatible(summary, path)",
	"hasMarkdownFenceDelimiter(normalized)",
	"hasMarkdownSetextDelimiter(normalized)",
	'const normalizeLineEndings = (source) => source.replace(/\\r\\n?/g, "\\n");',
	"const RAW_HTML_PATTERN =",
	"candidate.manifest.private !== undefined &&",
	'typeof candidate.manifest.version !== "string"',
	"normalizedTypesVersionsTargets(candidate.manifest)",
	"matchesPackedTypesVersionsPath(relativePath, manifest)",
	"typeResolutionTargetAlternatives(target)",
	'opening[2].includes("`")',
	'GITHUB_EVENT_NAME !== "pull_request_target"',
	"mergeBase !== baseSha",
]) {
	assert(
		changesetGuard.includes(marker),
		`the changeset guard must retain its protected contract marker ${marker}`,
	);
}
assert.match(
	changesetGuard,
	/\[\s*"diff",[\s\S]*"-z",[\s\S]*baseSha,[\s\S]*headSha/,
	"the guard must consume an exact NUL-delimited base/head Git diff",
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
	/name: Pack coverage files[\s\S]*?tar -czf[\s\S]*?name: Upload coverage artifact/,
	"trusted push tests must pack the exact coverage file list and hand it to the isolated uploader",
);
const coverageJob = workflowJob(ciWorkflow, "coverage_push");
assert.match(
	coverageJob,
	/permissions:\n {6}contents: read\n {6}id-token: write/,
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
// Both supported majors must stay in this matrix. Node 24 was dropped once
// (#1239) on the incorrect premise that a consumer resolving our published
// manifests lands on a prebuild-less node-datachannel; it does not (see the
// ci.yml comment for the resolution walk-through). Dropping a runtime from the
// published-closure smoke silently narrows what "we support Node 24" means, so
// it has to be a conscious edit here too.
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
	/^permissions:\n {2}contents: read$/m,
	"nightly simulations must use a read-only GITHUB_TOKEN",
);

const siteWorkflow = await readRepositoryFile(".github/workflows/site.yml");
assert.match(
	siteWorkflow,
	/^permissions:\n {2}contents: read$/m,
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
	/^ {4}if: github\.event_name == 'pull_request'$/m,
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
	/^ {4}if: github\.event_name == 'push' && github\.ref == 'refs\/heads\/master'$/m,
	"the secret-bearing production build must be push-only on master",
);
assert.match(
	siteProductionJob,
	/environment:\n {6}name: github-pages\n {6}deployment: false/,
	"production site secrets must come from the master-restricted Pages environment",
);
const siteDeployJob = workflowJob(siteWorkflow, "deploy");
assert.match(
	siteDeployJob,
	/permissions:\n {6}pages: write\n {6}id-token: write/,
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
assert.match(
	publishedSecuritySmoke,
	/"install",\n\t{3}"--legacy-peer-deps",/,
	"the audited consumer install must never auto-install npm peer dependencies",
);
for (const forbiddenPackage of [
	"react-native",
	"metro",
	"metro-config",
	"metro-transform-worker",
	"image-size",
]) {
	assert(
		publishedSecuritySmoke.includes(`\t\t"${forbiddenPackage}",`),
		"the audited consumer must assert the absence of " + forbiddenPackage,
	);
}
assert.match(
	publishedSecuritySmoke,
	/packageName\.startsWith\("@react-native\/"\)/,
	"the audited consumer must assert the absence of every @react-native/* package",
);
assert.match(
	publishedSecuritySmoke,
	/\["audit", "--omit=dev", "--json"\], \{\n\t\tcwd: consumerDirectory,\n\t\tstatus: 0,/,
	"the published consumer npm audit must demand a zero-finding exit status",
);
assert.doesNotMatch(
	publishedSecuritySmoke,
	/validateImageSizeAuditException|image-size-advisory-exception|exception/i,
	"the published consumer audit must not carry an audit exception branch",
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
	/^permissions:\n {2}contents: read$/m,
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
		/head_repository\.full_name == github\.repository/,
		`${name} must accept release runs only from this repository`,
	);
	assert.match(
		job,
		/vars\.ACTIONS_SECRETS_MIGRATED == 'true'/,
		`${name} must wait for repository-wide secret migration`,
	);
	assert.match(
		job,
		/peerbit-org:273107789/,
		`${name} must authenticate the immutable Peerbit Bot identity`,
	);
	assert.match(
		job,
		/^ {4}environment: post-release$/m,
		`${name} must obtain bot credentials from the master-restricted post-release environment`,
	);
	const peerbitCheckout = actionSteps(job, "actions/checkout").find(
		(checkout) => !checkout.includes("repository: dao-xyz/peerbit-bootstrap"),
	);
	assert(peerbitCheckout, `${name} must check out peerbit`);
	assert.match(
		peerbitCheckout,
		/ref: \$\{\{ github\.event\.workflow_run\.head_sha \}\}/,
		`${name} must use the exact completed release SHA`,
	);
	const setupNode = actionSteps(job, "actions/setup-node");
	assert.equal(setupNode.length, 1, `${name} must set up Node exactly once`);
	assert.match(
		setupNode[0],
		/uses: actions\/setup-node@[0-9a-f]{40} # v4\.4\.0/,
		`${name} must pin the Node setup action`,
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
	assert.match(
		checkout,
		/uses: actions\/checkout@[0-9a-f]{40} # v4\.3\.1/,
		"every post-release checkout must be commit-pinned",
	);
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
	assert.match(
		pullRequestAction,
		/^ {10}base: master$/m,
		"every post-release pull request must name its base after exact-SHA checkout",
	);
}
const bootstrapRolloutJob = workflowJob(
	postReleaseWorkflow,
	"bootstrap-rollout-pr",
);
assert.match(
	bootstrapRolloutJob,
	/name: Validate generated rollout[\s\S]*?npm ci --ignore-scripts --no-audit --no-fund[\s\S]*?npm run validate:rollout[\s\S]*?npm run test:rollout/,
	"generated bootstrap rollouts must pass the downstream repository's validators",
);
const bootstrapRolloutPullRequest = actionSteps(
	bootstrapRolloutJob,
	"peter-evans/create-pull-request",
)[0];
assert.match(
	bootstrapRolloutPullRequest,
	/^ {10}draft: true$/m,
	"bootstrap rollout pull requests must remain draft until production source state is verified",
);
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
