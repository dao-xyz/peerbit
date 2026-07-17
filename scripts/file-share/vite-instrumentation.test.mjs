import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import { injectViteBenchmarkResolveGuards } from "./vite-instrumentation.mjs";

const config = (resolvePrefix = "") => `export default {
    resolve: {
${resolvePrefix}        dedupe: [
        ],
    },
    optimizeDeps: {
        include: [
            "react",
            "react-dom",
        ],
    },
};
`;

const aliases = (root) => `        /* peerbit-benchmark-vite */
        preserveSymlinks: false,
        alias: {
            react: ${JSON.stringify(path.join(root, "react"))},
            "react-dom": ${JSON.stringify(path.join(root, "react-dom"))},
            "@dao-xyz/borsh": ${JSON.stringify(path.join(root, "@dao-xyz", "borsh"))},
        },
`;

const assertOneCurrentGuard = (contents, examplesNodeModules) => {
	assert.equal(contents.match(/\/\* peerbit-benchmark-vite \*\//g)?.length, 1);
	assert.equal(contents.match(/preserveSymlinks\s*:/g)?.length, 1);
	assert.equal(contents.match(/alias: \{/g)?.length, 1);
	assert.ok(contents.includes(aliases(examplesNodeModules)));
	assert.ok(!contents.includes("preserveSymlinks: true"));
};

test("Vite guard injection is complete and idempotent", () => {
	const frontendRoot = path.join("/tmp", "benchmark", "packages", "frontend");
	const examplesNodeModules = path.resolve(
		frontendRoot,
		"..",
		"..",
		"..",
		"node_modules",
	);
	const injected = injectViteBenchmarkResolveGuards(
		config(),
		"vite.config.ts",
		frontendRoot,
	);
	assertOneCurrentGuard(injected, examplesNodeModules);
	assert.equal(
		injectViteBenchmarkResolveGuards(injected, "vite.config.ts", frontendRoot),
		injected,
	);
});

test("Vite guard migration replaces legacy alias blocks atomically", () => {
	const frontendRoot = path.join("/tmp", "new", "packages", "frontend");
	const examplesNodeModules = path.resolve(
		frontendRoot,
		"..",
		"..",
		"..",
		"node_modules",
	);
	for (const preserveLine of ["", "        preserveSymlinks: true,\n"]) {
		const legacy = `        /* peerbit-benchmark-vite */
${preserveLine}        alias: {
            react: "/tmp/old/node_modules/react",
            "react-dom": "/tmp/old/node_modules/react-dom",
            "@dao-xyz/borsh": "/tmp/old/node_modules/@dao-xyz/borsh",
        },
`;
		const migrated = injectViteBenchmarkResolveGuards(
			config(legacy),
			"vite.config.ts",
			frontendRoot,
		);
		assertOneCurrentGuard(migrated, examplesNodeModules);
		assert.ok(!migrated.includes("/tmp/old"));
	}
});

test("Vite guard refreshes aliases when an instrumented template moves", () => {
	const oldFrontendRoot = path.join(
		"/tmp",
		"old",
		"examples",
		"packages",
		"file-share",
		"frontend",
	);
	const oldNodeModules = path.resolve(
		oldFrontendRoot,
		"..",
		"..",
		"..",
		"node_modules",
	);
	const newFrontendRoot = path.join(
		"/tmp",
		"new",
		"examples",
		"packages",
		"file-share",
		"frontend",
	);
	const newNodeModules = path.resolve(
		newFrontendRoot,
		"..",
		"..",
		"..",
		"node_modules",
	);
	const refreshed = injectViteBenchmarkResolveGuards(
		config(aliases(oldNodeModules)),
		"vite.config.ts",
		newFrontendRoot,
	);
	assertOneCurrentGuard(refreshed, newNodeModules);
	assert.ok(!refreshed.includes(oldNodeModules));
});

test("Vite guard rejects unattributed preserveSymlinks settings", () => {
	for (const property of [
		"preserveSymlinks",
		'"preserveSymlinks"',
		"'preserveSymlinks'",
	]) {
		assert.throws(
			() =>
				injectViteBenchmarkResolveGuards(
					config(`        ${property}: true,\n`),
					"vite.config.ts",
					path.join("/tmp", "benchmark", "packages", "frontend"),
				),
			/enables preserveSymlinks/,
		);
	}
});

test("Vite guard rejects unattributed alias blocks", () => {
	for (const property of ["alias", '"alias"', "'alias'"]) {
		assert.throws(
			() =>
				injectViteBenchmarkResolveGuards(
					config(`        ${property}: { react: "/tmp/wrong" },\n`),
					"vite.config.ts",
					path.join("/tmp", "benchmark", "packages", "frontend"),
				),
			/does not contain exactly one attributable benchmark resolve guard/,
		);
	}
});
