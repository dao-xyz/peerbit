import { findLibraryInNodeModules } from "@peerbit/build-assets";
import * as findUp from "find-up";
import { createRequire } from "module";
import path from "path";

const root = path.dirname(await findUp.findUp(".git", { type: "directory" }));
const resolverFromRoot = createRequire(path.join(root, "package.json"));
const resolverFromLocal = createRequire(import.meta.url);

const opfsAssetsDir = findLibraryInNodeModules("@peerbit/any-store-opfs/dist", {
	resolvers: [resolverFromRoot, resolverFromLocal],
});

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
				assets: opfsAssetsDir,
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
