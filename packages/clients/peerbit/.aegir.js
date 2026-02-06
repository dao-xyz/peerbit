import { findLibraryInNodeModules } from "@peerbit/build-assets";
import * as findUp from "find-up";
import fs from "fs";
import { createRequire } from "module";
import path from "path";

// In git worktrees, `.git` is a *file* (not a directory), so don't constrain the type.
// Fall back to workspace markers for non-git environments.
const rootMarker =
	(await findUp.findUp(".git")) ??
	(await findUp.findUp("pnpm-workspace.yaml")) ??
	(await findUp.findUp("package.json"));
if (!rootMarker) {
	throw new Error("Unable to locate repo root (no .git/workspace marker found)");
}
const root = path.dirname(rootMarker);
const resolverFromRoot = createRequire(path.join(root, "package.json"));
const resolverFromLocal = createRequire(import.meta.url);

const resolveAsset = (library) =>
	findLibraryInNodeModules(library, {
		resolvers: [resolverFromRoot, resolverFromLocal],
	});

const assetsRoot = path.join(root, "tmp", "aegir-assets", "peerbit");
const assets = [assetsRoot, "./dist"];

export default {
	// test cmd options
	build: {
		bundle: true,
		bundlesize: false,
		bundlesizeMax: "100kB",
		types: true,
	},
	test: {
		build: true,
		runner: "node",
		target: ["node", "browser", "webworker"],
		watch: false,
		files: [],
		timeout: 60000,
		grep: "",
		bail: false,
		debug: true,
		progress: false,
		cov: false,
		covTimeout: 60000,
		browser: {
			debug: true,
			config: {
				debug: true,
				assets,
				buildConfig: {
					conditions: ["production"],
				},
			},
		},
		before: (argv) => {
			if (argv?.runner === "browser" || argv?.runner === "webworker") {
				const opfsSrc = resolveAsset(
					"@peerbit/any-store-opfs/dist/assets/opfs",
				);
				const sqliteSrc = resolveAsset(
					"@peerbit/indexer-sqlite3/dist/assets/sqlite3",
				);

				const opfsDest = path.join(assetsRoot, "peerbit", "opfs");
				const sqliteDest = path.join(assetsRoot, "peerbit", "sqlite3");

				fs.rmSync(opfsDest, { recursive: true, force: true });
				fs.rmSync(sqliteDest, { recursive: true, force: true });

				fs.mkdirSync(opfsDest, { recursive: true });
				fs.mkdirSync(sqliteDest, { recursive: true });

				fs.cpSync(opfsSrc, opfsDest, { recursive: true });
				fs.cpSync(sqliteSrc, sqliteDest, { recursive: true });
			}
			return {
				env: { TS_NODE_PROJECT: path.join(root, "tsconfig.test.json") },
			};
		},
	},
};
