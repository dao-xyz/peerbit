import assert from "node:assert/strict";
import test from "node:test";
import {
	IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
	validateImageSizeAuditException,
	validateImageSizePackageLock,
	validateImageSizePnpmLockException,
} from "./image-size-advisory-exception.mjs";

const activeClock = new Date("2026-08-09T00:00:00Z");
const advisory = (source, ghsa) => ({
	source,
	name: "image-size",
	dependency: "image-size",
	title: "image-size advisory fixture",
	url: "https://github.com/advisories/" + ghsa,
	severity: "high",
	cwe: ["CWE-400"],
	cvss: { score: 7.5, vectorString: "fixture" },
	range: "<=2.0.2",
});
const vulnerability = ({ via, effects, range, node, isDirect = false }) => ({
	name: node.slice("node_modules/".length),
	severity: "high",
	isDirect,
	via,
	effects,
	range,
	nodes: [node],
	fixAvailable: true,
});

const exactAuditReport = () => ({
	auditReportVersion: 2,
	vulnerabilities: {
		"@react-native/community-cli-plugin": vulnerability({
			via: ["metro", "metro-config"],
			effects: ["react-native"],
			range: "*",
			node: "node_modules/@react-native/community-cli-plugin",
		}),
		"@react-native/virtualized-lists": vulnerability({
			via: ["react-native"],
			effects: ["react-native"],
			range: ">=0.85.0-nightly-20260108-1236b6be4",
			node: "node_modules/@react-native/virtualized-lists",
		}),
		"image-size": vulnerability({
			via: [
				advisory(1138808, "GHSA-w3rx-r6r6-pgpr"),
				advisory(1138809, "GHSA-5p2g-fcmc-qvqq"),
			],
			effects: ["metro"],
			range: "*",
			node: "node_modules/image-size",
		}),
		metro: vulnerability({
			via: ["image-size", "metro-config", "metro-transform-worker"],
			effects: [
				"@react-native/community-cli-plugin",
				"metro-config",
				"metro-transform-worker",
			],
			range: ">=0.22.1",
			node: "node_modules/metro",
		}),
		"metro-config": vulnerability({
			via: ["metro"],
			effects: ["@react-native/community-cli-plugin", "metro"],
			range: "*",
			node: "node_modules/metro-config",
		}),
		"metro-transform-worker": vulnerability({
			via: ["metro"],
			effects: ["metro"],
			range: ">=0.60.0",
			node: "node_modules/metro-transform-worker",
		}),
		"react-native": vulnerability({
			via: [
				"@react-native/community-cli-plugin",
				"@react-native/virtualized-lists",
			],
			effects: ["@react-native/virtualized-lists"],
			range: ">=0.73.0-nightly-20230506-1af868c52",
			node: "node_modules/react-native",
		}),
	},
	metadata: {
		vulnerabilities: {
			info: 0,
			low: 0,
			moderate: 0,
			high: 7,
			critical: 0,
			total: 7,
		},
		dependencies: {
			prod: 321,
			dev: 0,
			optional: 28,
			peer: 10,
			peerOptional: 2,
			total: 360,
		},
	},
});

const exactPackageLock = () => ({
	name: "peerbit-published-security-consumer",
	lockfileVersion: 3,
	requires: true,
	packages: {
		"": {
			dependencies: {
				peerbit: "file:peerbit.tgz",
			},
		},
		"node_modules/@peerbit/libp2p-test-utils": {
			version: "0.0.1",
			dependencies: {
				"@libp2p/webrtc": "^6.0.15",
			},
		},
		"node_modules/@peerbit/react": {
			version: "1.0.0",
			dependencies: {
				"@libp2p/webrtc": "^6.0.15",
			},
		},
		"node_modules/peerbit": {
			version: "13.0.0",
			dependencies: {
				"@libp2p/webrtc": "^6.0.15",
			},
		},
		"node_modules/@libp2p/webrtc": {
			version: "6.0.29",
			dependencies: {
				"react-native-webrtc": "^124.0.6",
			},
		},
		"node_modules/react-native-webrtc": {
			version: "124.0.7",
			peerDependencies: {
				"react-native": ">=0.60.0",
			},
		},
		"node_modules/react-native": {
			version: "0.82.1",
			dependencies: {
				"@react-native/community-cli-plugin": "0.82.1",
			},
		},
		"node_modules/@react-native/community-cli-plugin": {
			version: "0.82.1",
			dependencies: {
				metro: "^0.83.1",
			},
		},
		"node_modules/metro": {
			version: "0.83.7",
			dependencies: {
				"image-size": "^1.0.2",
			},
		},
		"node_modules/image-size": {
			version: "1.2.1",
		},
		"node_modules/unrelated": {
			version: "1.0.0",
		},
	},
});

const exactPnpmLock = () =>
	[
		"lockfileVersion: '9.0'",
		"",
		"importers:",
		"",
		"  .:",
		"    dependencies:",
		"      peerbit:",
		"        specifier: workspace:*",
		"        version: link:packages/clients/peerbit",
		"",
		"  packages/clients/peerbit:",
		"    dependencies:",
		"      '@libp2p/webrtc':",
		"        specifier: ^6.0.15",
		"        version: 6.0.15(react-native@0.82.1)",
		"",
		"  packages/clients/peerbit-react:",
		"    dependencies:",
		"      '@libp2p/webrtc':",
		"        specifier: ^6.0.15",
		"        version: 6.0.15(react-native@0.82.1)",
		"",
		"  packages/transport/libp2p-test-utils:",
		"    dependencies:",
		"      '@libp2p/webrtc':",
		"        specifier: ^6.0.15",
		"        version: 6.0.15(react-native@0.82.1)",
		"",
		"  packages/transport/stream/e2e/browser/browser-node:",
		"    dependencies:",
		"      '@libp2p/webrtc':",
		"        specifier: ^6.0.15",
		"        version: 6.0.15(react-native@0.82.1)",
		"",
		"packages:",
		"",
		"  image-size@1.2.1:",
		"    resolution: {integrity: fixture}",
		"",
		"snapshots:",
		"",
		"  '@libp2p/webrtc@6.0.15(react-native@0.82.1)':",
		"    dependencies:",
		"      react-native-webrtc: 124.0.7(react-native@0.82.1)",
		"",
		"  react-native-webrtc@124.0.7(react-native@0.82.1):",
		"    dependencies:",
		"      react-native: 0.82.1",
		"",
		"  react-native@0.82.1:",
		"    dependencies:",
		"      '@react-native/community-cli-plugin': 0.82.1",
		"",
		"  '@react-native/community-cli-plugin@0.82.1':",
		"    dependencies:",
		"      metro: 0.83.7",
		"",
		"  metro@0.83.7:",
		"    dependencies:",
		"      image-size: 1.2.1",
		"",
		"  image-size@1.2.1: {}",
		"",
	].join("\n");

const validateExactFixture = (overrides = {}) =>
	validateImageSizeAuditException({
		auditReport: exactAuditReport(),
		packageLock: exactPackageLock(),
		now: activeClock,
		...overrides,
	});

test("accepts only the exact npm v2 seven-node, two-advisory closure", () => {
	assert.deepEqual(validateExactFixture(), {
		status: "temporary-exception",
		cves: ["CVE-2025-71330", "CVE-2025-71329"],
		expiresAt: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
	});
});

test("zero findings always pass, including after the exception expires", () => {
	const auditReport = exactAuditReport();
	auditReport.vulnerabilities = {};
	auditReport.metadata.vulnerabilities = {
		info: 0,
		low: 0,
		moderate: 0,
		high: 0,
		critical: 0,
		total: 0,
	};
	assert.deepEqual(
		validateExactFixture({
			auditReport,
			packageLock: undefined,
			now: new Date("2030-01-01T00:00:00Z"),
		}),
		{ status: "clean" },
	);
});

test("rejects an extra image-size advisory", () => {
	const auditReport = exactAuditReport();
	auditReport.vulnerabilities["image-size"].via.push(
		advisory(9999999, "GHSA-fixture-extra"),
	);
	assert.throws(
		() => validateExactFixture({ auditReport }),
		/unexpected npm audit v2 node for image-size/,
	);
});

test("rejects a missing image-size advisory", () => {
	const auditReport = exactAuditReport();
	auditReport.vulnerabilities["image-size"].via.pop();
	assert.throws(
		() => validateExactFixture({ auditReport }),
		/unexpected npm audit v2 node for image-size/,
	);
});

test("rejects vulnerability closure drift", () => {
	const auditReport = exactAuditReport();
	auditReport.vulnerabilities.metro.effects.push("unexpected-consumer");
	assert.throws(
		() => validateExactFixture({ auditReport }),
		/unexpected npm audit v2 node for metro/,
	);
});

test("rejects a direct vulnerable dependency", () => {
	const auditReport = exactAuditReport();
	auditReport.vulnerabilities["image-size"].isDirect = true;
	assert.throws(
		() => validateExactFixture({ auditReport }),
		/unexpected npm audit v2 node for image-size/,
	);
});

test("rejects direct image-size in the generated consumer lock", () => {
	const packageLock = exactPackageLock();
	packageLock.packages[""].dependencies["image-size"] = "1.2.1";
	assert.throws(
		() => validateImageSizePackageLock(packageLock),
		/must not depend directly on image-size/,
	);
});

test("rejects an alternate image-size edge in the generated consumer lock", () => {
	const packageLock = exactPackageLock();
	packageLock.packages["node_modules/unrelated"].dependencies = {
		"image-size": "^1.0.2",
	};
	assert.throws(
		() => validateImageSizePackageLock(packageLock),
		/rejects alternate image-size dependency edges/,
	);
});

test("rejects an alternate published @libp2p/webrtc introducer", () => {
	const packageLock = exactPackageLock();
	packageLock.packages["node_modules/unrelated"].dependencies = {
		"@libp2p/webrtc": "^6.0.15",
	};
	assert.throws(
		() => validateImageSizePackageLock(packageLock),
		/rejects alternate @libp2p\/webrtc dependency edges/,
	);
});

test("rejects an alternate image-size version in the generated consumer lock", () => {
	const packageLock = exactPackageLock();
	packageLock.packages["node_modules/unrelated/node_modules/image-size"] = {
		version: "2.0.2",
	};
	assert.throws(
		() => validateImageSizePackageLock(packageLock),
		/requires exactly one installed image-size/,
	);
});

test("rejects an unknown npm audit schema even with zero findings", () => {
	const auditReport = exactAuditReport();
	auditReport.auditReportVersion = 3;
	auditReport.vulnerabilities = {};
	auditReport.metadata.vulnerabilities.total = 0;
	assert.throws(
		() => validateExactFixture({ auditReport }),
		/only understands npm audit report v2/,
	);
});

test("rejects findings at the hard expiry boundary", () => {
	assert.throws(
		() =>
			validateExactFixture({
				now: new Date(IMAGE_SIZE_EXCEPTION_EXPIRES_AT),
			}),
		/expired at 2026-08-22T00:00:00Z/,
	);
});

test("validates the exact committed pnpm dependency spine", () => {
	assert.deepEqual(
		validateImageSizePnpmLockException(exactPnpmLock(), {
			now: activeClock,
		}),
		{
			status: "temporary-exception",
			cves: ["CVE-2025-71330", "CVE-2025-71329"],
			expiresAt: IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
		},
	);
});

test("rejects pnpm lock alternate edges and versions", () => {
	const alternateEdge = exactPnpmLock().replace(
		"  image-size@1.2.1: {}",
		[
			"  unrelated@1.0.0:",
			"    dependencies:",
			"      image-size: 1.2.1",
			"",
			"  image-size@1.2.1: {}",
		].join("\n"),
	);
	assert.throws(
		() =>
			validateImageSizePnpmLockException(alternateEdge, {
				now: activeClock,
			}),
		/rejects alternate image-size dependency edges/,
	);
	const alternateVersion = exactPnpmLock().replace(
		"  image-size@1.2.1: {}",
		"  image-size@2.0.2: {}\n\n  image-size@1.2.1: {}",
	);
	assert.throws(
		() =>
			validateImageSizePnpmLockException(alternateVersion, {
				now: activeClock,
			}),
		/contains an alternate image-size version/,
	);
});

test("rejects an alternate pnpm @libp2p/webrtc importer", () => {
	const alternateImporter = exactPnpmLock().replace(
		"packages:\n",
		[
			"  packages/unrelated:",
			"    dependencies:",
			"      '@libp2p/webrtc':",
			"        specifier: ^6.0.15",
			"        version: 6.0.15(react-native@0.82.1)",
			"",
			"packages:",
		].join("\n"),
	);
	assert.throws(
		() =>
			validateImageSizePnpmLockException(alternateImporter, {
				now: activeClock,
			}),
		/only the reviewed direct @libp2p\/webrtc importers/,
	);
});
