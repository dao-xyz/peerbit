import assert from "node:assert/strict";

export const IMAGE_SIZE_EXCEPTION_EXPIRES_AT = "2026-08-22T00:00:00Z";
export const IMAGE_SIZE_EXCEPTION_CVES = Object.freeze([
	"CVE-2025-71330",
	"CVE-2025-71329",
]);

const expectedAuditCounts = Object.freeze({
	info: 0,
	low: 0,
	moderate: 0,
	high: 7,
	critical: 0,
	total: 7,
});
const zeroAuditCounts = Object.freeze({
	info: 0,
	low: 0,
	moderate: 0,
	high: 0,
	critical: 0,
	total: 0,
});
const expectedImageSizeAdvisories = Object.freeze([
	{
		source: 1138808,
		name: "image-size",
		dependency: "image-size",
		url: "https://github.com/advisories/GHSA-w3rx-r6r6-pgpr",
		severity: "high",
		range: "<=2.0.2",
	},
	{
		source: 1138809,
		name: "image-size",
		dependency: "image-size",
		url: "https://github.com/advisories/GHSA-5p2g-fcmc-qvqq",
		severity: "high",
		range: "<=2.0.2",
	},
]);
const expectedAuditVulnerabilities = Object.freeze({
	"@react-native/community-cli-plugin": {
		severity: "high",
		isDirect: false,
		via: ["metro", "metro-config"],
		effects: ["react-native"],
		range: "*",
		nodes: ["node_modules/@react-native/community-cli-plugin"],
	},
	"@react-native/virtualized-lists": {
		severity: "high",
		isDirect: false,
		via: ["react-native"],
		effects: ["react-native"],
		range: ">=0.85.0-nightly-20260108-1236b6be4",
		nodes: ["node_modules/@react-native/virtualized-lists"],
	},
	"image-size": {
		severity: "high",
		isDirect: false,
		via: expectedImageSizeAdvisories,
		effects: ["metro"],
		range: "*",
		nodes: ["node_modules/image-size"],
	},
	metro: {
		severity: "high",
		isDirect: false,
		via: ["image-size", "metro-config", "metro-transform-worker"],
		effects: [
			"@react-native/community-cli-plugin",
			"metro-config",
			"metro-transform-worker",
		],
		range: ">=0.22.1",
		nodes: ["node_modules/metro"],
	},
	"metro-config": {
		severity: "high",
		isDirect: false,
		via: ["metro"],
		effects: ["@react-native/community-cli-plugin", "metro"],
		range: "*",
		nodes: ["node_modules/metro-config"],
	},
	"metro-transform-worker": {
		severity: "high",
		isDirect: false,
		via: ["metro"],
		effects: ["metro"],
		range: ">=0.60.0",
		nodes: ["node_modules/metro-transform-worker"],
	},
	"react-native": {
		severity: "high",
		isDirect: false,
		via: [
			"@react-native/community-cli-plugin",
			"@react-native/virtualized-lists",
		],
		effects: ["@react-native/virtualized-lists"],
		range: ">=0.73.0-nightly-20230506-1af868c52",
		nodes: ["node_modules/react-native"],
	},
});

const packageLockSpine = Object.freeze([
	{
		path: "node_modules/@libp2p/webrtc",
		version: "6.0.29",
		field: "dependencies",
		dependency: "react-native-webrtc",
		range: "^124.0.6",
	},
	{
		path: "node_modules/react-native-webrtc",
		version: "124.0.8",
		field: "peerDependencies",
		dependency: "react-native",
		range: ">=0.60.0",
	},
	{
		path: "node_modules/react-native",
		version: "0.82.1",
		field: "dependencies",
		dependency: "@react-native/community-cli-plugin",
		range: "0.82.1",
	},
	{
		path: "node_modules/@react-native/community-cli-plugin",
		version: "0.82.1",
		field: "dependencies",
		dependency: "metro",
		range: "^0.83.1",
	},
	{
		path: "node_modules/metro",
		version: "0.83.7",
		field: "dependencies",
		dependency: "image-size",
		range: "^1.0.2",
	},
	{
		path: "node_modules/image-size",
		version: "1.2.1",
	},
]);

const pnpmSpine = Object.freeze([
	{
		name: "@libp2p/webrtc",
		version: "6.0.15",
		dependency: "react-native-webrtc",
		dependencyVersion: "124.0.7",
	},
	{
		name: "react-native-webrtc",
		version: "124.0.7",
		dependency: "react-native",
		dependencyVersion: "0.82.1",
	},
	{
		name: "react-native",
		version: "0.82.1",
		dependency: "@react-native/community-cli-plugin",
		dependencyVersion: "0.82.1",
	},
	{
		name: "@react-native/community-cli-plugin",
		version: "0.82.1",
		dependency: "metro",
		dependencyVersion: "0.83.7",
	},
	{
		name: "metro",
		version: "0.83.7",
		dependency: "image-size",
		dependencyVersion: "1.2.1",
	},
	{
		name: "image-size",
		version: "1.2.1",
	},
]);

const dependencyFields = [
	"dependencies",
	"optionalDependencies",
	"peerDependencies",
	"devDependencies",
];
const sortStrings = (values) =>
	[...values].sort((left, right) => left.localeCompare(right));
const auditCounts = (metadata) =>
	Object.fromEntries(
		Object.keys(zeroAuditCounts).map((key) => [key, metadata?.[key]]),
	);
const normalizedAdvisory = (advisory) => ({
	source: advisory?.source,
	name: advisory?.name,
	dependency: advisory?.dependency,
	url: advisory?.url,
	severity: advisory?.severity,
	range: advisory?.range,
});
const normalizeVia = (via) =>
	[...via]
		.map((item) => (typeof item === "string" ? item : normalizedAdvisory(item)))
		.sort((left, right) =>
			JSON.stringify(left).localeCompare(JSON.stringify(right)),
		);
const normalizeVulnerability = (vulnerability) => ({
	name: vulnerability?.name,
	severity: vulnerability?.severity,
	isDirect: vulnerability?.isDirect,
	via: normalizeVia(vulnerability?.via ?? []),
	effects: sortStrings(vulnerability?.effects ?? []),
	range: vulnerability?.range,
	nodes: sortStrings(vulnerability?.nodes ?? []),
});

const normalizeExpectedVulnerability = (name, vulnerability) => ({
	name,
	...vulnerability,
	via: normalizeVia(vulnerability.via),
	effects: sortStrings(vulnerability.effects),
	nodes: sortStrings(vulnerability.nodes),
});
const nowMilliseconds = (now) => {
	const value = now instanceof Date ? now.getTime() : new Date(now).getTime();
	assert(Number.isFinite(value), "the image-size exception clock is invalid");
	return value;
};
const assertExceptionActive = (now) => {
	assert(
		nowMilliseconds(now) < Date.parse(IMAGE_SIZE_EXCEPTION_EXPIRES_AT),
		"the temporary image-size advisory exception expired at " +
			IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
	);
};

export const validateImageSizePackageLock = (packageLock) => {
	assert.equal(
		packageLock?.lockfileVersion,
		3,
		"the temporary exception only accepts npm package-lock v3",
	);
	assert(
		packageLock.packages && typeof packageLock.packages === "object",
		"npm package-lock v3 must contain a packages object",
	);
	const packages = packageLock.packages;
	const rootPackage = packages[""] ?? {};
	for (const { path } of packageLockSpine) {
		const packageName = path.slice("node_modules/".length);
		for (const field of dependencyFields) {
			assert.equal(
				rootPackage[field]?.[packageName],
				undefined,
				"the packed consumer must not depend directly on " + packageName,
			);
		}
		const matchingPaths = Object.keys(packages).filter(
			(candidate) =>
				candidate === "node_modules/" + packageName ||
				candidate.endsWith("/node_modules/" + packageName),
		);
		assert.deepEqual(
			matchingPaths,
			[path],
			"the temporary exception requires exactly one installed " +
				packageName +
				" at " +
				path,
		);
	}
	for (const edge of packageLockSpine) {
		const entry = packages[edge.path];
		assert(entry, "missing exception spine package " + edge.path);
		assert.equal(
			entry.version,
			edge.version,
			edge.path + " must remain at " + edge.version,
		);
		if (edge.dependency) {
			assert.equal(
				entry[edge.field]?.[edge.dependency],
				edge.range,
				edge.path +
					" must retain its exact " +
					edge.field +
					" edge to " +
					edge.dependency +
					"@" +
					edge.range,
			);
		}
	}
	for (const [dependencyName, expectedOwner] of [
		[
			"@libp2p/webrtc",
			[
				{
					path: "node_modules/@peerbit/libp2p-test-utils",
					field: "dependencies",
					range: "^6.0.15",
				},
				{
					path: "node_modules/@peerbit/react",
					field: "dependencies",
					range: "^6.0.15",
				},
				{
					path: "node_modules/peerbit",
					field: "dependencies",
					range: "^6.0.15",
				},
			],
		],
		[
			"react-native-webrtc",
			[
				{
					path: "node_modules/@libp2p/webrtc",
					field: "dependencies",
					range: "^124.0.6",
				},
			],
		],
		[
			"image-size",
			[
				{
					path: "node_modules/metro",
					field: "dependencies",
					range: "^1.0.2",
				},
			],
		],
	]) {
		const owners = [];
		for (const [path, entry] of Object.entries(packages)) {
			for (const field of dependencyFields) {
				if (entry?.[field]?.[dependencyName] !== undefined) {
					owners.push({
						path,
						field,
						range: entry[field][dependencyName],
					});
				}
			}
		}
		owners.sort((left, right) =>
			(left.path + "\0" + left.field).localeCompare(
				right.path + "\0" + right.field,
			),
		);
		assert.deepEqual(
			owners,
			expectedOwner,
			"the temporary exception rejects alternate " +
				dependencyName +
				" dependency edges",
		);
	}
	return {
		status: "validated-exception-graph",
		spine: packageLockSpine.map(({ path, version }) => ({ path, version })),
	};
};

export const validateImageSizeAuditException = ({
	auditReport,
	packageLock,
	now = new Date(),
}) => {
	assert.equal(
		auditReport?.auditReportVersion,
		2,
		"the image-size validator only understands npm audit report v2",
	);
	assert(
		auditReport.vulnerabilities &&
			typeof auditReport.vulnerabilities === "object" &&
			!Array.isArray(auditReport.vulnerabilities),
		"npm audit v2 must contain a vulnerabilities object",
	);
	const counts = auditCounts(auditReport.metadata?.vulnerabilities);
	if (counts.total === 0) {
		assert.deepEqual(
			counts,
			zeroAuditCounts,
			"a zero-finding npm audit must have zero counts at every severity",
		);
		assert.deepEqual(
			Object.keys(auditReport.vulnerabilities),
			[],
			"a zero-finding npm audit must not contain vulnerability nodes",
		);
		return { status: "clean" };
	}

	assertExceptionActive(now);
	assert.deepEqual(
		counts,
		expectedAuditCounts,
		"the temporary exception accepts exactly seven high-severity nodes",
	);
	const actualNames = sortStrings(Object.keys(auditReport.vulnerabilities));
	const expectedNames = sortStrings(Object.keys(expectedAuditVulnerabilities));
	assert.deepEqual(
		actualNames,
		expectedNames,
		"the npm audit vulnerability closure changed",
	);
	for (const name of expectedNames) {
		assert.deepEqual(
			normalizeVulnerability(auditReport.vulnerabilities[name]),
			normalizeExpectedVulnerability(name, expectedAuditVulnerabilities[name]),
			"unexpected npm audit v2 node for " + name,
		);
	}
	validateImageSizePackageLock(packageLock);
	return {
		status: "temporary-exception",
		cves: [...IMAGE_SIZE_EXCEPTION_CVES],
		expiresAt: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
	};
};

const unquoteYamlScalar = (value) => {
	const trimmed = value.trim();
	if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
		return trimmed.slice(1, -1).replaceAll("''", "'");
	}
	if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
		return JSON.parse(trimmed);
	}
	return trimmed;
};

const parsePnpmLockSection = (lockText, sectionName) => {
	const lines = lockText.split(/\r?\n/);
	const sectionStart = lines.findIndex((line) => line === sectionName + ":");
	assert(sectionStart >= 0, "pnpm lockfile must contain " + sectionName);
	const entries = new Map();
	let entry;
	let field;
	for (const line of lines.slice(sectionStart + 1)) {
		if (/^\S/.test(line)) {
			break;
		}
		const entryMatch = line.match(/^  (\S.*?):(?: \{\})?$/);
		if (entryMatch) {
			entry = {
				name: unquoteYamlScalar(entryMatch[1]),
				fields: new Map(),
			};
			entries.set(entry.name, entry);
			field = undefined;
			continue;
		}
		const fieldMatch = line.match(/^    ([A-Za-z][A-Za-z]+):$/);
		if (fieldMatch && entry) {
			field = fieldMatch[1];
			if (!entry.fields.has(field)) {
				entry.fields.set(field, new Map());
			}
			continue;
		}
		const dependencyMatch = line.match(/^ {6}(?! )(.+?):(?: (.*))?$/);
		if (dependencyMatch && entry && field) {
			entry.fields
				.get(field)
				.set(
					unquoteYamlScalar(dependencyMatch[1]),
					unquoteYamlScalar(dependencyMatch[2] ?? ""),
				);
		}
	}
	return entries;
};

const basePnpmVersion = (value) => value.split("(")[0];
const pnpmSnapshotMatches = (snapshotName, packageName, version) =>
	snapshotName === packageName + "@" + version ||
	snapshotName.startsWith(packageName + "@" + version + "(");

export const validateImageSizePnpmLockException = (
	lockText,
	{ now = new Date() } = {},
) => {
	assertExceptionActive(now);
	assert.match(
		lockText,
		/^lockfileVersion: ['"]9\.0['"]$/m,
		"the temporary exception only accepts pnpm lockfile v9",
	);
	const snapshots = parsePnpmLockSection(lockText, "snapshots");
	const importers = parsePnpmLockSection(lockText, "importers");
	const selectedSnapshots = new Map();
	for (const edge of pnpmSpine) {
		const allVersions = [...snapshots.keys()].filter((snapshotName) =>
			snapshotName.startsWith(edge.name + "@"),
		);
		const exactVersions = allVersions.filter((snapshotName) =>
			pnpmSnapshotMatches(snapshotName, edge.name, edge.version),
		);
		assert.deepEqual(
			exactVersions,
			allVersions,
			"the pnpm lockfile contains an alternate " + edge.name + " version",
		);
		assert.equal(
			exactVersions.length,
			1,
			"the pnpm lockfile must contain exactly one " +
				edge.name +
				"@" +
				edge.version +
				" snapshot",
		);
		selectedSnapshots.set(edge.name, snapshots.get(exactVersions[0]));
	}
	for (const edge of pnpmSpine) {
		if (!edge.dependency) {
			continue;
		}
		const dependencyVersion = selectedSnapshots
			.get(edge.name)
			.fields.get("dependencies")
			?.get(edge.dependency);
		assert.equal(
			basePnpmVersion(dependencyVersion ?? ""),
			edge.dependencyVersion,
			edge.name +
				"@" +
				edge.version +
				" must retain its exact dependency on " +
				edge.dependency +
				"@" +
				edge.dependencyVersion,
		);
	}
	const imageSizeOwners = [];
	const reactNativeWebrtcOwners = [];
	for (const [snapshotName, snapshot] of snapshots) {
		for (const field of ["dependencies", "optionalDependencies"]) {
			const values = snapshot.fields.get(field);
			if (values?.has("image-size")) {
				imageSizeOwners.push({
					snapshotName,
					field,
					version: basePnpmVersion(values.get("image-size")),
				});
			}
			if (values?.has("react-native-webrtc")) {
				reactNativeWebrtcOwners.push({
					snapshotName,
					field,
					version: basePnpmVersion(values.get("react-native-webrtc")),
				});
			}
		}
	}
	assert.deepEqual(
		imageSizeOwners,
		[
			{
				snapshotName: selectedSnapshots.get("metro").name,
				field: "dependencies",
				version: "1.2.1",
			},
		],
		"the pnpm lockfile rejects alternate image-size dependency edges",
	);
	assert.deepEqual(
		reactNativeWebrtcOwners,
		[
			{
				snapshotName: selectedSnapshots.get("@libp2p/webrtc").name,
				field: "dependencies",
				version: "124.0.7",
			},
		],
		"the pnpm lockfile rejects alternate react-native-webrtc dependency edges",
	);
	const forbiddenDirectDependencies = new Set(
		pnpmSpine
			.map(({ name }) => name)
			.filter((name) => name !== "@libp2p/webrtc"),
	);
	for (const importer of importers.values()) {
		for (const field of dependencyFields) {
			for (const dependencyName of importer.fields.get(field)?.keys() ?? []) {
				assert(
					!forbiddenDirectDependencies.has(dependencyName),
					"pnpm importer " +
						importer.name +
						" must not depend directly on " +
						dependencyName,
				);
			}
		}
	}
	const directWebrtcImporters = sortStrings(
		[...importers.values()]
			.filter((importer) =>
				dependencyFields.some((field) =>
					importer.fields.get(field)?.has("@libp2p/webrtc"),
				),
			)
			.map((importer) => importer.name),
	);
	assert.deepEqual(
		directWebrtcImporters,
		[
			"packages/clients/peerbit",
			"packages/clients/peerbit-react",
			"packages/transport/libp2p-test-utils",
			"packages/transport/stream/e2e/browser/browser-node",
		],
		"the pnpm lockfile must retain only the reviewed direct @libp2p/webrtc importers",
	);
	return {
		status: "temporary-exception",
		cves: [...IMAGE_SIZE_EXCEPTION_CVES],
		expiresAt: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
	};
};
