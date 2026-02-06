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

const opfsAssetsDir = findLibraryInNodeModules(
	"@peerbit/any-store-opfs/dist/assets/opfs",
	{
		resolvers: [resolverFromRoot, resolverFromLocal],
	},
);

const assetsDir = path.join(root, "tmp", "aegir-assets", "any-store");
const opfsDestDir = path.join(assetsDir, "peerbit", "opfs");
fs.mkdirSync(opfsDestDir, { recursive: true });
for (const file of fs.readdirSync(opfsAssetsDir)) {
	fs.copyFileSync(path.join(opfsAssetsDir, file), path.join(opfsDestDir, file));
}

export default {
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
			config: {
				assets: assetsDir,
				buildConfig: {
					conditions: ["production"],
				},
			},
		},
		before: () => {
			return {
				env: { TS_NODE_PROJECT: path.join(root, "tsconfig.test.json") },
			};
		},
	},
};
