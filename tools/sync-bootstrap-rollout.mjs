import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const FINGERPRINT_KEYS = [
	"peerbit",
	"@peerbit/blocks",
	"@peerbit/crypto",
	"@peerbit/program",
	"@peerbit/pubsub",
	"@peerbit/time",
];

const ensure = (condition, message) => {
	if (!condition) throw new Error(message);
};

const readJson = (file) => JSON.parse(fs.readFileSync(file, "utf8"));
const writeJson = (file, value) =>
	fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);

const parseStableVersion = (version, label) => {
	ensure(
		typeof version === "string" &&
			/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(version),
		`${label} must be a stable exact semver`,
	);
	const parts = version.split(".").map(Number);
	ensure(
		parts.every(Number.isSafeInteger),
		`${label} components must be safe integers`,
	);
	return parts;
};

const compareVersions = (left, right) => {
	for (let index = 0; index < 3; index++) {
		if (left[index] !== right[index]) return left[index] - right[index];
	}
	return 0;
};

const assertIntegrity = (integrity, label) => {
	ensure(
		typeof integrity === "string" &&
			/^sha512-[A-Za-z0-9+/]{86}==$/.test(integrity),
		`${label} must be a canonical SHA-512 integrity`,
	);
	const encoded = integrity.slice("sha512-".length);
	ensure(
		Buffer.from(encoded, "base64").toString("base64") === encoded,
		`${label} is not canonical Base64`,
	);
};

const readFingerprint = (entry, label) => {
	ensure(
		entry && typeof entry === "object" && !Array.isArray(entry),
		`${label} is missing`,
	);
	ensure(
		entry.dependencies && typeof entry.dependencies === "object",
		`${label}.dependencies is missing`,
	);
	const fingerprint = {};
	for (const key of FINGERPRINT_KEYS) {
		const version = entry.dependencies[key];
		parseStableVersion(version, `${label}.dependencies.${key}`);
		fingerprint[key] = version;
	}
	return fingerprint;
};

const sameFingerprint = (left, right) =>
	FINGERPRINT_KEYS.every((key) => left?.[key] === right?.[key]);

const canonicalRegistryTarball = (name, version) => {
	const basename = name.includes("/")
		? name.slice(name.lastIndexOf("/") + 1)
		: name;
	return `https://registry.npmjs.org/${name}/-/${basename}-${version}.tgz`;
};

const assertCanonicalRegistryEntry = (entry, name, version, label) => {
	ensure(entry?.version === version, `${label}.version must be ${version}`);
	ensure(
		entry.resolved === canonicalRegistryTarball(name, version),
		`${label} must use the canonical npm tarball`,
	);
	assertIntegrity(entry.integrity, `${label} integrity`);
};

const assertCanonicalResolvedEntries = (lock) => {
	for (const [lockPath, entry] of Object.entries(lock.packages ?? {})) {
		if (!entry?.resolved) continue;
		let resolved;
		try {
			resolved = new URL(entry.resolved);
		} catch {
			throw new Error(
				`${lockPath || "package-lock root"} has an invalid resolved URL`,
			);
		}
		ensure(
			resolved.protocol === "https:" &&
				resolved.origin === "https://registry.npmjs.org" &&
				resolved.username === "" &&
				resolved.password === "" &&
				resolved.search === "" &&
				resolved.hash === "",
			`${lockPath || "package-lock root"} must resolve through credential-free https://registry.npmjs.org`,
		);
		assertIntegrity(
			entry.integrity,
			`${lockPath || "package-lock root"} integrity`,
		);
	}
};

const readServerLockState = (
	bootstrapRoot,
	version,
	{ allowNestedFingerprint = false } = {},
) => {
	const lock = readJson(path.join(bootstrapRoot, "package-lock.json"));
	ensure(
		lock.lockfileVersion === 3,
		"package-lock.json must use lockfileVersion 3",
	);
	const root = lock.packages?.[""];
	const entry = lock.packages?.["node_modules/@peerbit/server"];
	ensure(root?.dependencies, "package-lock.json root dependencies are missing");
	assertCanonicalResolvedEntries(lock);
	assertCanonicalRegistryEntry(
		entry,
		"@peerbit/server",
		version,
		"package-lock @peerbit/server",
	);
	const fingerprint = readFingerprint(entry, "package-lock @peerbit/server");
	for (const [name, dependencyVersion] of Object.entries(fingerprint)) {
		const nested =
			lock.packages?.[`node_modules/@peerbit/server/node_modules/${name}`];
		ensure(
			allowNestedFingerprint || !nested,
			`package-lock ${name} must not be shadowed below @peerbit/server`,
		);
		assertCanonicalRegistryEntry(
			nested ?? lock.packages?.[`node_modules/${name}`],
			name,
			dependencyVersion,
			`package-lock ${name}`,
		);
	}
	return {
		fingerprint,
		integrity: entry.integrity,
		root,
	};
};

const assertReviewedCurrentTarget = ({
	bootstrapRoot,
	config,
	packageJson,
}) => {
	const current = parseStableVersion(
		config.targetVersion,
		"current targetVersion",
	);
	ensure(
		current[0] >= 8,
		"current reviewed target must use signed-request v8 or newer",
	);
	assertIntegrity(config.targetIntegrity, "current targetIntegrity");
	const lockState = readServerLockState(bootstrapRoot, config.targetVersion);
	ensure(
		packageJson.dependencies?.["@peerbit/server"] === config.targetVersion,
		"package.json must exactly pin the current targetVersion",
	);
	ensure(
		lockState.root.dependencies?.["@peerbit/server"] === config.targetVersion,
		"package-lock root must exactly pin the current targetVersion",
	);
	const currentCrypto = lockState.fingerprint["@peerbit/crypto"];
	ensure(
		packageJson.dependencies?.["@peerbit/crypto"] === currentCrypto &&
			lockState.root.dependencies?.["@peerbit/crypto"] === currentCrypto,
		"package and lock roots must pin the current target crypto version",
	);
	ensure(
		lockState.integrity === config.targetIntegrity,
		"current target integrity does not match package-lock.json",
	);
	ensure(
		sameFingerprint(lockState.fingerprint, config.targetFingerprint),
		"current target fingerprint does not match package-lock.json",
	);
	return lockState;
};

const installPackageLock = (bootstrapRoot) => {
	const command = process.platform === "win32" ? "npm.cmd" : "npm";
	const result = spawnSync(
		command,
		[
			"install",
			"--package-lock-only",
			"--ignore-scripts",
			"--no-audit",
			"--no-fund",
		],
		{
			cwd: bootstrapRoot,
			env: { ...process.env, npm_config_ignore_scripts: "true" },
			stdio: "inherit",
			timeout: 300_000,
		},
	);
	if (result.error) throw result.error;
	ensure(
		result.status === 0,
		`npm lockfile install failed with status ${result.status ?? "unknown"}`,
	);
};

export const syncBootstrapRollout = ({
	bootstrapRoot,
	targetVersion,
	installLockfile = installPackageLock,
}) => {
	const root = fs.realpathSync(path.resolve(bootstrapRoot));
	const configFile = path.join(root, "rollouts", "bootstrap-5.json");
	const packageFile = path.join(root, "package.json");
	const lockFile = path.join(root, "package-lock.json");
	ensure(fs.existsSync(configFile), `Missing rollout config: ${configFile}`);
	ensure(fs.existsSync(packageFile), `Missing package.json: ${packageFile}`);
	ensure(fs.existsSync(lockFile), `Missing package-lock.json: ${lockFile}`);

	const config = readJson(configFile);
	const packageJson = readJson(packageFile);
	assertReviewedCurrentTarget({ bootstrapRoot: root, config, packageJson });

	const currentVersion = parseStableVersion(
		config.targetVersion,
		"current targetVersion",
	);
	const nextVersion = parseStableVersion(targetVersion, "targetVersion");
	ensure(
		nextVersion[0] >= 8,
		"targetVersion must use signed-request v8 or newer",
	);
	const comparison = compareVersions(nextVersion, currentVersion);
	if (comparison === 0) {
		return {
			changed: false,
			rollbackVersion: config.rollbackVersion,
			targetVersion,
			targetIntegrity: config.targetIntegrity,
			targetFingerprint: { ...config.targetFingerprint },
		};
	}
	ensure(
		comparison > 0,
		"targetVersion must be newer than the reviewed current target",
	);

	const rollbackVersion = config.targetVersion;
	const rollbackIntegrity = config.targetIntegrity;
	const rollbackFingerprint = { ...config.targetFingerprint };

	const originals = new Map(
		[configFile, packageFile, lockFile].map((file) => [
			file,
			fs.readFileSync(file),
		]),
	);

	try {
		packageJson.dependencies["@peerbit/server"] = targetVersion;
		writeJson(packageFile, packageJson);
		installLockfile(root);

		let targetState = readServerLockState(root, targetVersion, {
			allowNestedFingerprint: true,
		});
		const targetCrypto = targetState.fingerprint["@peerbit/crypto"];
		if (packageJson.dependencies["@peerbit/crypto"] !== targetCrypto) {
			packageJson.dependencies["@peerbit/crypto"] = targetCrypto;
			writeJson(packageFile, packageJson);
			installLockfile(root);
		}
		targetState = readServerLockState(root, targetVersion);

		ensure(
			targetState.root.dependencies?.["@peerbit/server"] === targetVersion &&
				targetState.root.dependencies?.["@peerbit/crypto"] ===
					targetState.fingerprint["@peerbit/crypto"],
			"package-lock root does not match the exact target server/crypto cohort",
		);

		config.expectedCurrentVersion = rollbackVersion;
		config.rollbackVersion = rollbackVersion;
		config.rollbackIntegrity = rollbackIntegrity;
		config.rollbackFingerprint = rollbackFingerprint;
		config.targetVersion = targetVersion;
		config.targetIntegrity = targetState.integrity;
		config.targetFingerprint = targetState.fingerprint;
		config.rerollReason = `peerbit-server-${targetVersion}-release-rollout`;
		writeJson(configFile, config);

		return {
			changed: true,
			rollbackVersion,
			targetVersion,
			targetIntegrity: targetState.integrity,
			targetFingerprint: targetState.fingerprint,
		};
	} catch (error) {
		for (const [file, contents] of originals) fs.writeFileSync(file, contents);
		throw error;
	}
};

const run = () => {
	const bootstrapRoot = process.argv[2];
	const targetVersion = process.argv[3];
	ensure(
		bootstrapRoot,
		"Usage: node tools/sync-bootstrap-rollout.mjs <bootstrap-root> <server-version>",
	);
	ensure(targetVersion, "Missing server version argument");
	syncBootstrapRollout({ bootstrapRoot, targetVersion });
};

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
)
	run();
