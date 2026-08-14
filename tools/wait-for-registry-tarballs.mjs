// Waits until every registry tarball this rollout freshly pinned is actually
// downloadable, before `npm ci` tries to download it.
//
// Why this exists: run 31805881609 failed in "Validate generated rollout" with
//
//   npm error 404 Not Found - GET https://registry.npmjs.org/peerbit/-/peerbit-5.3.24.tgz
//
// ~16 minutes after Release had published that version. The tarball URL served
// 200 later, so nothing was missing -- the job raced registry propagation.
//
// Note what that says about the *shape* of the guard. The preceding step runs
// tools/sync-bootstrap-rollout.mjs, whose installPackageLock() shells out to
// `npm install --package-lock-only`; readServerLockState() then requires a
// canonical `sha512-...` on every pinned entry (assertIntegrity). npm can only
// write that integrity by reading `dist.integrity` out of the registry
// packument. So the packument for peerbit@5.3.24 was already live and correct
// at that moment -- only the tarball blob was not yet servable. A guard that
// polls metadata (`npm view <pkg>@<version> version`, a packument GET, ...)
// would have returned instantly and the 404 would have happened anyway. The
// probe has to hit the same artifact `npm ci` fetches: the tarball URL.
//
// Which URLs: the ones this sync just introduced, computed as the registry
// `resolved` URLs present in the generated package-lock.json but absent from
// the committed one (`git show HEAD:package-lock.json`), unioned with every
// registry entry pinned at the released version. Both halves are load-bearing.
// The released version is @peerbit/server's (8.x); the tarball that 404'd was
// `peerbit` (5.x), a transitive dependency published by the same release run --
// version-equality alone would not have probed it, and the newly-added-URL set
// alone depends on git being available.
//
// The probe deliberately sends no cache-busting headers and no query string: it
// must observe exactly what `npm ci` will observe, edge cache included. A probe
// that asks the CDN to revalidate could go green while npm ci still reads a
// cached 404.
//
// Usage: node tools/wait-for-registry-tarballs.mjs <bootstrap-root> <version>
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const REGISTRY_ORIGIN = "https://registry.npmjs.org";
const USER_AGENT = "peerbit-post-release-tarball-probe";
const MAX_ATTEMPTS = 20;
const DELAY_STEP_MS = 5_000;
const MAX_DELAY_MS = 30_000;
const REQUEST_TIMEOUT_MS = 30_000;
const PROBE_CONCURRENCY = 8;

const ensure = (condition, message) => {
	if (!condition) throw new Error(message);
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const readJson = (file) => JSON.parse(fs.readFileSync(file, "utf8"));

const mapWithConcurrency = async (items, limit, worker) => {
	const results = new Array(items.length);
	let cursor = 0;
	const runners = Array.from(
		{ length: Math.min(limit, items.length) },
		async () => {
			while (cursor < items.length) {
				const index = cursor++;
				results[index] = await worker(items[index]);
			}
		},
	);
	await Promise.all(runners);
	return results;
};

// "https://registry.npmjs.org/@peerbit/server/-/server-8.0.14.tgz" -> "@peerbit/server"
const packageNameFromTarball = (url) => {
	const marker = "/-/";
	const index = url.pathname.indexOf(marker);
	if (index <= 0) return undefined;
	return decodeURIComponent(url.pathname.slice(1, index));
};

const registryTarballs = (lock) => {
	const entries = new Map();
	for (const [lockPath, entry] of Object.entries(lock.packages ?? {})) {
		if (typeof entry?.resolved !== "string") continue;
		let resolved;
		try {
			resolved = new URL(entry.resolved);
		} catch {
			continue;
		}
		if (resolved.origin !== REGISTRY_ORIGIN) continue;
		entries.set(entry.resolved, {
			url: entry.resolved,
			name: packageNameFromTarball(resolved),
			version: typeof entry.version === "string" ? entry.version : undefined,
			lockPath: lockPath || "package-lock root",
		});
	}
	return entries;
};

// Returns the committed lockfile's registry URLs, or undefined when the
// previous revision cannot be read. spawnSync is used (not a shell pipeline) so
// the exit status belongs to git itself and not to whatever ran last in a pipe.
//
// `git -C <root>` walks *up* to the nearest enclosing repository when <root>
// has no .git of its own. Here <root> is the bootstrap checkout nested inside
// the peerbit checkout, so a missing bootstrap .git would silently answer with
// peerbit's own lockfile -- a completely unrelated set of URLs. Pin the answer
// to <root> itself before trusting it.
const committedRegistryUrls = (root) => {
	const git = (args) =>
		spawnSync("git", ["-C", root, ...args], {
			encoding: "utf8",
			maxBuffer: 64 * 1024 * 1024,
		});
	const toplevel = git(["rev-parse", "--show-toplevel"]);
	if (toplevel.error || toplevel.status !== 0) return undefined;
	if (fs.realpathSync(toplevel.stdout.trim()) !== root) return undefined;
	const result = git(["show", "HEAD:package-lock.json"]);
	if (result.error || result.status !== 0) return undefined;
	try {
		return new Set(registryTarballs(JSON.parse(result.stdout)).keys());
	} catch {
		return undefined;
	}
};

export const selectProbeTargets = ({ lock, previousUrls, releasedVersion }) => {
	// Required, not optional. A missing set must never quietly reduce this to
	// version-equality matching -- see the ensure() in waitForRegistryTarballs.
	ensure(
		previousUrls instanceof Set,
		"selectProbeTargets requires the committed lockfile's URL set",
	);
	const current = registryTarballs(lock);
	const targets = new Map();
	for (const [url, entry] of current) {
		const freshlyPinned = !previousUrls.has(url);
		if (freshlyPinned || entry.version === releasedVersion)
			targets.set(url, entry);
	}
	return targets;
};

const probeTarball = async (url) => {
	const request = async (method) => {
		const response = await fetch(url, {
			method,
			redirect: "follow",
			headers: { "user-agent": USER_AGENT },
			signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
		});
		// Release the socket without downloading the payload.
		await response.body?.cancel();
		return response.status;
	};
	try {
		let status = await request("HEAD");
		// Some edges refuse HEAD on blobs; fall back to the exact GET npm issues.
		if (status === 403 || status === 405 || status === 501)
			status = await request("GET");
		return { ok: status === 200, detail: `HTTP ${status}` };
	} catch (error) {
		return { ok: false, detail: `request failed: ${error.message}` };
	}
};

// Distinguishes "the registry never heard of this version" from "the version is
// published and the blob has not propagated". Only ever called on the failure
// path, to explain a timeout -- never to decide that the wait succeeded.
const classifyFailure = async (target) => {
	if (!target.name) return "could not derive a package name from the URL";
	const packument = `${REGISTRY_ORIGIN}/${target.name.replace(/\//g, "%2f")}`;
	try {
		const response = await fetch(packument, {
			headers: {
				accept: "application/vnd.npm.install-v1+json",
				"user-agent": USER_AGENT,
			},
			signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
		});
		if (response.status === 404) {
			await response.body?.cancel();
			return `package ${target.name} does not exist in the registry`;
		}
		if (!response.ok) {
			await response.body?.cancel();
			return `packument lookup failed with HTTP ${response.status}`;
		}
		const body = await response.json();
		return body?.versions?.[target.version]
			? `${target.name}@${target.version} is published; the tarball has not propagated`
			: `${target.name}@${target.version} is NOT published (packument does not list it)`;
	} catch (error) {
		return `packument lookup failed: ${error.message}`;
	}
};

export const waitForRegistryTarballs = async ({
	bootstrapRoot,
	releasedVersion,
	attempts = MAX_ATTEMPTS,
	probe = probeTarball,
}) => {
	const root = fs.realpathSync(path.resolve(bootstrapRoot));
	const lockFile = path.join(root, "package-lock.json");
	ensure(fs.existsSync(lockFile), `Missing package-lock.json: ${lockFile}`);

	// Not a warning. Without the committed lockfile there is no freshly-pinned
	// set, so selectProbeTargets falls back to version-equality alone -- and the
	// tarball this guard exists for (`peerbit` 5.x, a transitive published by the
	// same run) is on a different version line from the released @peerbit/server
	// 8.x, so it would not be probed at all. targets.size stays > 0 because the
	// server entry always matches, so the run would go green having checked
	// nothing that matters. Degrading to a guard that names a property it does
	// not check is the failure this file exists to prevent; refuse instead.
	const previousUrls = committedRegistryUrls(root);
	ensure(
		previousUrls,
		`Could not read HEAD:package-lock.json from ${root}. Refusing to continue: without it only tarballs already at ${releasedVersion} would be probed, which silently skips freshly-pinned transitive dependencies on other version lines.`,
	);

	const targets = selectProbeTargets({
		lock: readJson(lockFile),
		previousUrls,
		releasedVersion,
	});
	// sync-bootstrap-rollout.mjs always leaves node_modules/@peerbit/server
	// pinned at the released version, so an empty set means the lockfile is not
	// the one that step produced.
	ensure(
		targets.size > 0,
		`No registry tarball in ${lockFile} is newly pinned or pinned at ${releasedVersion}; refusing to run a wait that checks nothing`,
	);

	let pending = [...targets.values()];
	console.log(
		`Waiting for ${pending.length} registry tarball(s) to become fetchable:`,
	);
	for (const target of pending) console.log(`  ${target.url}`);

	const lastDetail = new Map();
	for (let attempt = 1; attempt <= attempts; attempt++) {
		const results = await mapWithConcurrency(
			pending,
			PROBE_CONCURRENCY,
			async (target) => ({ target, result: await probe(target.url) }),
		);
		const stillPending = [];
		for (const { target, result } of results) {
			lastDetail.set(target.url, result.detail);
			if (result.ok) console.log(`  ok ${target.url} (${result.detail})`);
			else stillPending.push(target);
		}
		pending = stillPending;
		if (pending.length === 0) {
			console.log(
				`All ${targets.size} tarball(s) are fetchable after attempt ${attempt}.`,
			);
			return { attempts: attempt, probed: targets.size };
		}
		if (attempt === attempts) break;
		const delay = Math.min(DELAY_STEP_MS * attempt, MAX_DELAY_MS);
		console.log(
			`  attempt ${attempt}/${attempts}: ${pending.length} tarball(s) not fetchable yet; retrying in ${delay / 1000}s`,
		);
		for (const target of pending)
			console.log(`    ${target.url} -> ${lastDetail.get(target.url)}`);
		await sleep(delay);
	}

	const diagnoses = await mapWithConcurrency(
		pending,
		PROBE_CONCURRENCY,
		async (target) => ({ target, reason: await classifyFailure(target) }),
	);
	const missing = diagnoses.filter(({ reason }) =>
		reason.includes("NOT published"),
	);
	const lines = diagnoses.map(
		({ target, reason }) =>
			`  ${target.url}\n    last probe: ${lastDetail.get(target.url)}\n    registry says: ${reason}`,
	);
	throw new Error(
		[
			missing.length > 0
				? `${missing.length} of ${pending.length} unfetchable tarball(s) are genuinely missing from the registry -- this is not a propagation delay, the release did not publish them.`
				: `${pending.length} tarball(s) are published but still not downloadable after ${attempts} attempts -- registry propagation timed out. Re-run this job.`,
			...lines,
		].join("\n"),
	);
};

const run = async () => {
	const bootstrapRoot = process.argv[2];
	const releasedVersion = process.argv[3];
	ensure(
		bootstrapRoot,
		"Usage: node tools/wait-for-registry-tarballs.mjs <bootstrap-root> <released-version>",
	);
	ensure(releasedVersion, "Missing released version argument");
	await waitForRegistryTarballs({ bootstrapRoot, releasedVersion });
};

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
)
	run().catch((error) => {
		console.error(error.message);
		process.exitCode = 1;
	});
