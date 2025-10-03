import { findLibraryInNodeModules } from "@peerbit/build-assets";
import * as findUp from "find-up";
import { createRequire } from "module";
import path from "path";

const root = path.dirname(await findUp.findUp(".git", { type: "directory" }));
const resolverFromRoot = createRequire(path.join(root, "package.json"));
const resolverFromLocal = createRequire(import.meta.url);

const resolveAsset = (library) =>
	findLibraryInNodeModules(library, {
		resolvers: [resolverFromRoot, resolverFromLocal],
	});

const assets = [
	resolveAsset("@peerbit/any-store-opfs/dist"),
	resolveAsset("@peerbit/indexer-sqlite3/dist"),
	resolveAsset("@sqlite.org/sqlite-wasm/sqlite-wasm/jswasm"),
	"./dist",
];

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
		before: () => {
			return {
				env: { TS_NODE_PROJECT: path.join(root, "tsconfig.test.json") },
			};
		},
	},
};
