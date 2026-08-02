import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { syncBootstrapRollout } from "./sync-bootstrap-rollout.mjs";

const integrity = (byte) =>
	`sha512-${Buffer.alloc(64, byte).toString("base64")}`;
const currentFingerprint = {
	peerbit: "5.3.10",
	"@peerbit/blocks": "4.2.6",
	"@peerbit/crypto": "3.1.4",
	"@peerbit/program": "6.0.39",
	"@peerbit/pubsub": "5.3.4",
	"@peerbit/time": "3.0.1",
};
const targetFingerprint = {
	peerbit: "5.3.15",
	"@peerbit/blocks": "4.2.8",
	"@peerbit/crypto": "3.1.5",
	"@peerbit/program": "6.0.43",
	"@peerbit/pubsub": "5.3.8",
	"@peerbit/time": "3.0.1",
};

const writeJson = (file, value) =>
	fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
const readJson = (file) => JSON.parse(fs.readFileSync(file, "utf8"));
const canonicalRegistryTarball = (name, version) => {
	const basename = name.includes("/")
		? name.slice(name.lastIndexOf("/") + 1)
		: name;
	return `https://registry.npmjs.org/${name}/-/${basename}-${version}.tgz`;
};

const writeLock = (
	root,
	{ version, packageDependencies, fingerprint, packageIntegrity },
) => {
	const fingerprintPackages = Object.fromEntries(
		Object.entries(fingerprint).map(([name, dependencyVersion], index) => [
			`node_modules/${name}`,
			{
				version: dependencyVersion,
				resolved: canonicalRegistryTarball(name, dependencyVersion),
				integrity: integrity(index + 10),
			},
		]),
	);
	writeJson(path.join(root, "package-lock.json"), {
		lockfileVersion: 3,
		requires: true,
		packages: {
			"": { dependencies: packageDependencies },
			...fingerprintPackages,
			"node_modules/@peerbit/server": {
				version,
				resolved: canonicalRegistryTarball("@peerbit/server", version),
				integrity: packageIntegrity,
				dependencies: fingerprint,
			},
		},
	});
};

const createFixture = (t) => {
	const root = fs.mkdtempSync(
		path.join(os.tmpdir(), "peerbit-bootstrap-sync-"),
	);
	t.after(() => fs.rmSync(root, { recursive: true, force: true }));
	fs.mkdirSync(path.join(root, "rollouts"));
	const packageDependencies = {
		"@dao-xyz/borsh": "^6.0.1",
		"@peerbit/crypto": currentFingerprint["@peerbit/crypto"],
		"@peerbit/server": "8.0.0",
	};
	writeJson(path.join(root, "package.json"), {
		dependencies: packageDependencies,
	});
	writeJson(path.join(root, "rollouts", "bootstrap-5.json"), {
		bootstrapFile: "bootstrap-5.env",
		expectedCurrentVersion: "6.0.36",
		targetVersion: "8.0.0",
		rollbackVersion: "6.0.36",
		targetIntegrity: integrity(1),
		rollbackIntegrity: integrity(2),
		targetFingerprint: currentFingerprint,
		rollbackFingerprint: { ...currentFingerprint, peerbit: "5.3.0" },
		batchSize: 1,
		waitReadyTimeoutMs: 180000,
		waitReadyDelayMs: 3000,
		rollbackOnFailure: true,
		rerollReason: "completed-rollout",
	});
	writeLock(root, {
		version: "8.0.0",
		packageDependencies,
		fingerprint: currentFingerprint,
		packageIntegrity: integrity(1),
	});
	return root;
};

test("promotes the reviewed target and derives the new exact lock cohort", (t) => {
	const root = createFixture(t);
	const installs = [];
	const installLockfile = () => {
		const packageJson = readJson(path.join(root, "package.json"));
		installs.push({ ...packageJson.dependencies });
		writeLock(root, {
			version: "8.0.5",
			packageDependencies: packageJson.dependencies,
			fingerprint: targetFingerprint,
			packageIntegrity: integrity(3),
		});
		if (
			packageJson.dependencies["@peerbit/crypto"] !==
			targetFingerprint["@peerbit/crypto"]
		) {
			const lockFile = path.join(root, "package-lock.json");
			const lock = readJson(lockFile);
			lock.packages[
				"node_modules/@peerbit/server/node_modules/@peerbit/crypto"
			] = lock.packages["node_modules/@peerbit/crypto"];
			const rootCrypto = packageJson.dependencies["@peerbit/crypto"];
			lock.packages["node_modules/@peerbit/crypto"] = {
				version: rootCrypto,
				resolved: canonicalRegistryTarball("@peerbit/crypto", rootCrypto),
				integrity: integrity(20),
			};
			writeJson(lockFile, lock);
		}
	};

	const result = syncBootstrapRollout({
		bootstrapRoot: root,
		targetVersion: "8.0.5",
		installLockfile,
	});

	assert.equal(
		installs.length,
		2,
		"crypto pin changes require a second lock resolution",
	);
	assert.equal(installs[0]["@peerbit/server"], "8.0.5");
	assert.equal(installs[0]["@peerbit/crypto"], "3.1.4");
	assert.equal(installs[1]["@peerbit/crypto"], "3.1.5");
	assert.equal(result.changed, true);
	assert.deepEqual(result.targetFingerprint, targetFingerprint);

	const config = readJson(path.join(root, "rollouts", "bootstrap-5.json"));
	assert.equal(config.expectedCurrentVersion, "8.0.0");
	assert.equal(config.rollbackVersion, "8.0.0");
	assert.equal(config.rollbackIntegrity, integrity(1));
	assert.deepEqual(config.rollbackFingerprint, currentFingerprint);
	assert.equal(config.targetVersion, "8.0.5");
	assert.equal(config.targetIntegrity, integrity(3));
	assert.deepEqual(config.targetFingerprint, targetFingerprint);
	assert.equal(config.rerollReason, "peerbit-server-8.0.5-release-rollout");
});

test("rejects stale reviewed metadata before changing files", (t) => {
	const root = createFixture(t);
	const configFile = path.join(root, "rollouts", "bootstrap-5.json");
	const config = readJson(configFile);
	config.targetIntegrity = integrity(9);
	writeJson(configFile, config);
	const beforePackage = fs.readFileSync(
		path.join(root, "package.json"),
		"utf8",
	);

	assert.throws(
		() =>
			syncBootstrapRollout({
				bootstrapRoot: root,
				targetVersion: "8.0.5",
				installLockfile: () => assert.fail(),
			}),
		/current target integrity does not match/,
	);
	assert.equal(
		fs.readFileSync(path.join(root, "package.json"), "utf8"),
		beforePackage,
	);
});

test("an already-current exact target is an unchanged no-op", (t) => {
	const root = createFixture(t);
	const files = [
		path.join(root, "package.json"),
		path.join(root, "package-lock.json"),
		path.join(root, "rollouts", "bootstrap-5.json"),
	];
	const before = files.map((file) => fs.readFileSync(file));
	const result = syncBootstrapRollout({
		bootstrapRoot: root,
		targetVersion: "8.0.0",
		installLockfile: () => assert.fail("a no-op must not run npm"),
	});

	assert.equal(result.changed, false);
	assert.equal(result.targetVersion, "8.0.0");
	assert.equal(result.targetIntegrity, integrity(1));
	assert.deepEqual(result.targetFingerprint, currentFingerprint);
	for (const [index, file] of files.entries()) {
		assert.deepEqual(fs.readFileSync(file), before[index]);
	}
});

test("rejects downgrade, prerelease, and non-canonical versions", (t) => {
	const root = createFixture(t);
	for (const targetVersion of ["7.9.9", "8.0.5-rc.1", "08.0.5"]) {
		assert.throws(
			() =>
				syncBootstrapRollout({
					bootstrapRoot: root,
					targetVersion,
					installLockfile: () => assert.fail(),
				}),
			/(newer than|v8 or newer|stable exact semver)/,
		);
	}
});

test("restores all managed files after partial lock generation", (t) => {
	const root = createFixture(t);
	const files = [
		path.join(root, "package.json"),
		path.join(root, "package-lock.json"),
		path.join(root, "rollouts", "bootstrap-5.json"),
	];
	const before = files.map((file) => fs.readFileSync(file));

	assert.throws(
		() =>
			syncBootstrapRollout({
				bootstrapRoot: root,
				targetVersion: "8.0.5",
				installLockfile: () => {
					fs.writeFileSync(path.join(root, "package-lock.json"), "partial\n");
					throw new Error("simulated npm failure");
				},
			}),
		/simulated npm failure/,
	);
	for (const [index, file] of files.entries()) {
		assert.deepEqual(fs.readFileSync(file), before[index]);
	}
});

test("rejects a non-canonical generated lock and restores the fixture", (t) => {
	const root = createFixture(t);
	const files = [
		path.join(root, "package.json"),
		path.join(root, "package-lock.json"),
		path.join(root, "rollouts", "bootstrap-5.json"),
	];
	const before = files.map((file) => fs.readFileSync(file));

	assert.throws(
		() =>
			syncBootstrapRollout({
				bootstrapRoot: root,
				targetVersion: "8.0.5",
				installLockfile: () => {
					const packageJson = readJson(path.join(root, "package.json"));
					writeLock(root, {
						version: "8.0.5",
						packageDependencies: packageJson.dependencies,
						fingerprint: targetFingerprint,
						packageIntegrity: integrity(3),
					});
					const lockFile = path.join(root, "package-lock.json");
					const lock = readJson(lockFile);
					lock.packages["node_modules/peerbit"].resolved += "?mirror=1";
					writeJson(lockFile, lock);
				},
			}),
		/must resolve through credential-free/,
	);
	for (const [index, file] of files.entries()) {
		assert.deepEqual(fs.readFileSync(file), before[index]);
	}
});

test("rejects a nested package that can shadow the fingerprint cohort", (t) => {
	const root = createFixture(t);
	const nestedFingerprint = {
		...targetFingerprint,
		"@peerbit/crypto": currentFingerprint["@peerbit/crypto"],
	};
	const installLockfile = () => {
		const packageJson = readJson(path.join(root, "package.json"));
		writeLock(root, {
			version: "8.0.5",
			packageDependencies: packageJson.dependencies,
			fingerprint: nestedFingerprint,
			packageIntegrity: integrity(3),
		});
		const lockFile = path.join(root, "package-lock.json");
		const lock = readJson(lockFile);
		lock.packages["node_modules/@peerbit/server/node_modules/@peerbit/crypto"] =
			{
				version: currentFingerprint["@peerbit/crypto"],
				resolved: canonicalRegistryTarball(
					"@peerbit/crypto",
					currentFingerprint["@peerbit/crypto"],
				),
				integrity: integrity(20),
			};
		writeJson(lockFile, lock);
	};

	assert.throws(
		() =>
			syncBootstrapRollout({
				bootstrapRoot: root,
				targetVersion: "8.0.5",
				installLockfile,
			}),
		/must not be shadowed below @peerbit\/server/,
	);
});
