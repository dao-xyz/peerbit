import * as findUp from "find-up";
import fs from "fs";
import path from "path";

const root = path.dirname(await findUp.findUp(".git", { type: "directory" }));

export default {
	// test cmd options
	build: {
		bundle: true,
		bundlesize: false,
		bundlesizeMax: "100kB",
		types: true,
		config: {
			format: "esm",
			minify: false,
			outfile: "dist/peerbit/sqlite3.min.js",
			banner: { js: "" },
			footer: { js: "" },
		},
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
			config: {
				assets: [
					path.join(root, "tmp", "aegir-assets", "indexer-sqlite3"),
					"./dist",
				],
				/* path.join(dirname(import.meta.url), "../", './xyz') ,*/ /* 
				headers: {
					'Cross-Origin-Opener-Policy': 'same-origin',
					'Cross-Origin-Embedder-Policy': 'require-corp',
				}, */
				buildConfig: {
					conditions: ["production"],
				},
			},
		},
		before: (argv) => {
			if (argv?.runner === "browser" || argv?.runner === "webworker") {
				const assetsRoot = path.join(
					root,
					"tmp",
					"aegir-assets",
					"indexer-sqlite3",
				);
				const peerbitSqlite3Assets = path.join(
					assetsRoot,
					"peerbit",
					"sqlite3",
				);
				const src = path.resolve("dist", "assets", "sqlite3");
				if (!fs.existsSync(src)) {
					throw new Error(
						`Missing sqlite3 browser assets at ${src}. Run \"pnpm --filter @peerbit/indexer-sqlite3 build\" before browser tests.`,
					);
				}
				fs.rmSync(peerbitSqlite3Assets, { recursive: true, force: true });
				fs.mkdirSync(peerbitSqlite3Assets, { recursive: true });
				fs.cpSync(src, peerbitSqlite3Assets, { recursive: true });
			}
			return {
				env: { TS_NODE_PROJECT: path.join(root, "tsconfig.test.json") },
			};
		},
	},
};
