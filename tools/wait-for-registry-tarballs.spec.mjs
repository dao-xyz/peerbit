import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	selectProbeTargets,
	waitForRegistryTarballs,
} from "./wait-for-registry-tarballs.mjs";

// The scenario every case here is built from is the one that actually failed
// (run 31805881609): Release publishes @peerbit/server 8.0.14 *and* its
// transitive dependency peerbit 5.3.99, the rollout pins both, and only the
// transitive one is still unfetchable. The two live on different version lines,
// which is exactly why version-equality matching alone is not sufficient.
const RELEASED_VERSION = "8.0.14";
const SERVER_TARBALL =
	"https://registry.npmjs.org/@peerbit/server/-/server-8.0.14.tgz";
const PEERBIT_TARBALL =
	"https://registry.npmjs.org/peerbit/-/peerbit-5.3.99.tgz";
const LODASH_TARBALL = "https://registry.npmjs.org/lodash/-/lodash-4.17.21.tgz";

const lockWith = (entries) => ({
	name: "peerbit-bootstrap",
	lockfileVersion: 3,
	packages: Object.fromEntries(
		entries.map(({ lockPath, name, version, resolved }) => [
			lockPath,
			{ name, version, resolved, integrity: `sha512-${"a".repeat(86)}==` },
		]),
	),
});

const PREVIOUS = [
	{
		lockPath: "node_modules/@peerbit/server",
		name: "@peerbit/server",
		version: "8.0.13",
		resolved: "https://registry.npmjs.org/@peerbit/server/-/server-8.0.13.tgz",
	},
	{
		lockPath: "node_modules/peerbit",
		name: "peerbit",
		version: "5.3.98",
		resolved: "https://registry.npmjs.org/peerbit/-/peerbit-5.3.98.tgz",
	},
	{
		lockPath: "node_modules/lodash",
		name: "lodash",
		version: "4.17.21",
		resolved: LODASH_TARBALL,
	},
];

const CURRENT = [
	{
		lockPath: "node_modules/@peerbit/server",
		name: "@peerbit/server",
		version: RELEASED_VERSION,
		resolved: SERVER_TARBALL,
	},
	{
		lockPath: "node_modules/peerbit",
		name: "peerbit",
		version: "5.3.99",
		resolved: PEERBIT_TARBALL,
	},
	{
		lockPath: "node_modules/lodash",
		name: "lodash",
		version: "4.17.21",
		resolved: LODASH_TARBALL,
	},
];

const previousUrls = () => new Set(PREVIOUS.map((entry) => entry.resolved));

test("probes a freshly pinned transitive on a different version line", () => {
	const targets = selectProbeTargets({
		lock: lockWith(CURRENT),
		previousUrls: previousUrls(),
		releasedVersion: RELEASED_VERSION,
	});
	// peerbit@5.3.99 does NOT equal the released 8.0.14, so it is selected only
	// because it is newly pinned. This is the tarball that 404'd in production.
	assert.ok(targets.has(PEERBIT_TARBALL));
	assert.ok(targets.has(SERVER_TARBALL));
	// Unchanged dependency: already on the registry long ago, nothing to wait for.
	assert.ok(!targets.has(LODASH_TARBALL));
});

test("selectProbeTargets refuses to run without the committed URL set", () => {
	// Regression guard. Defaulting previousUrls to "nothing was freshly pinned"
	// silently degrades this to version-equality matching, which drops
	// peerbit@5.3.99 while still selecting the server entry -- so targets stays
	// non-empty and the wait passes having skipped the one URL that mattered.
	for (const previous of [undefined, null, [...previousUrls()]]) {
		assert.throws(
			() =>
				selectProbeTargets({
					lock: lockWith(CURRENT),
					previousUrls: previous,
					releasedVersion: RELEASED_VERSION,
				}),
			/requires the committed lockfile's URL set/,
		);
	}
});

test("ignores non-registry resolved entries", () => {
	const targets = selectProbeTargets({
		lock: lockWith([
			...CURRENT,
			{
				lockPath: "node_modules/private-thing",
				name: "private-thing",
				version: RELEASED_VERSION,
				resolved: "https://npm.example.com/private-thing/-/x-8.0.14.tgz",
			},
		]),
		previousUrls: previousUrls(),
		releasedVersion: RELEASED_VERSION,
	});
	assert.ok(
		[...targets.keys()].every((url) =>
			url.startsWith("https://registry.npmjs.org/"),
		),
	);
});

const withBootstrapRepo = (entries, run) => {
	const root = fs.realpathSync(
		fs.mkdtempSync(path.join(os.tmpdir(), "wait-tarballs-")),
	);
	const git = (...args) =>
		spawnSync("git", ["-C", root, ...args], { encoding: "utf8" });
	try {
		git("init", "--quiet");
		git("config", "user.email", "test@example.com");
		git("config", "user.name", "test");
		fs.writeFileSync(
			path.join(root, "package-lock.json"),
			`${JSON.stringify(lockWith(PREVIOUS), null, 2)}\n`,
		);
		git("add", "-A");
		git("commit", "--quiet", "-m", "base");
		// Post-sync state: tracked file rewritten, not committed -- exactly what
		// sync-bootstrap-rollout.mjs leaves behind for `npm ci` to consume.
		fs.writeFileSync(
			path.join(root, "package-lock.json"),
			`${JSON.stringify(lockWith(entries), null, 2)}\n`,
		);
		return run(root);
	} finally {
		fs.rmSync(root, { recursive: true, force: true });
	}
};

test("returns once every probed tarball is fetchable", async () => {
	await withBootstrapRepo(CURRENT, async (root) => {
		const probed = [];
		const result = await waitForRegistryTarballs({
			bootstrapRoot: root,
			releasedVersion: RELEASED_VERSION,
			attempts: 3,
			probe: async (url) => {
				probed.push(url);
				return { ok: true, detail: "HTTP 200" };
			},
		});
		assert.equal(result.probed, 2);
		assert.equal(result.attempts, 1);
		assert.deepEqual(probed.sort(), [SERVER_TARBALL, PEERBIT_TARBALL].sort());
	});
});

test("does not return success while a tarball still 404s", async () => {
	await withBootstrapRepo(CURRENT, async (root) => {
		let calls = 0;
		await assert.rejects(
			waitForRegistryTarballs({
				bootstrapRoot: root,
				releasedVersion: RELEASED_VERSION,
				attempts: 2,
				probe: async (url) => {
					calls++;
					return url === PEERBIT_TARBALL
						? { ok: false, detail: "HTTP 404" }
						: { ok: true, detail: "HTTP 200" };
				},
			}),
			// It must name the still-missing tarball, not just fail vaguely.
			(error) => error.message.includes(PEERBIT_TARBALL),
		);
		assert.ok(calls > 2, "the failing tarball should be retried");
	});
});

test("refuses to run when the committed lockfile cannot be read", async () => {
	const root = fs.realpathSync(
		fs.mkdtempSync(path.join(os.tmpdir(), "wait-tarballs-nogit-")),
	);
	try {
		fs.writeFileSync(
			path.join(root, "package-lock.json"),
			`${JSON.stringify(lockWith(CURRENT), null, 2)}\n`,
		);
		let probeCalls = 0;
		await assert.rejects(
			waitForRegistryTarballs({
				bootstrapRoot: root,
				releasedVersion: RELEASED_VERSION,
				attempts: 1,
				probe: async () => {
					probeCalls++;
					return { ok: true, detail: "HTTP 200" };
				},
			}),
			/Could not read HEAD:package-lock\.json/,
		);
		// The point is that it fails instead of probing a reduced target set.
		assert.equal(probeCalls, 0);
	} finally {
		fs.rmSync(root, { recursive: true, force: true });
	}
});

test("does not borrow an enclosing repository's lockfile", async () => {
	// `git -C <dir>` walks up when <dir> has no .git. The bootstrap checkout is
	// nested inside the peerbit checkout, so walking up would answer with
	// peerbit's lockfile and produce a meaningless "freshly pinned" set.
	const outer = fs.realpathSync(
		fs.mkdtempSync(path.join(os.tmpdir(), "wait-tarballs-outer-")),
	);
	try {
		const git = (...args) =>
			spawnSync("git", ["-C", outer, ...args], { encoding: "utf8" });
		git("init", "--quiet");
		git("config", "user.email", "test@example.com");
		git("config", "user.name", "test");
		fs.writeFileSync(
			path.join(outer, "package-lock.json"),
			`${JSON.stringify(lockWith(PREVIOUS), null, 2)}\n`,
		);
		git("add", "-A");
		git("commit", "--quiet", "-m", "outer");

		const nested = path.join(outer, "peerbit-bootstrap");
		fs.mkdirSync(nested);
		fs.writeFileSync(
			path.join(nested, "package-lock.json"),
			`${JSON.stringify(lockWith(CURRENT), null, 2)}\n`,
		);

		await assert.rejects(
			waitForRegistryTarballs({
				bootstrapRoot: nested,
				releasedVersion: RELEASED_VERSION,
				attempts: 1,
				probe: async () => ({ ok: true, detail: "HTTP 200" }),
			}),
			/Could not read HEAD:package-lock\.json/,
		);
	} finally {
		fs.rmSync(outer, { recursive: true, force: true });
	}
});
