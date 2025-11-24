import { expect } from "chai";
import fs from "fs";
import os from "os";
import path from "path";
import {
	type FileSystemLike,
	type ModuleResolver,
	TEST_EXPORTS,
} from "../src/index.js";

// Simulate a pnpm-like path resolution where resolver returns a .pnpm store path
describe("pnpm-style symlink resolution", () => {
	it("realpathSync resolves .pnpm store to actual file", () => {
		const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "pnpm-sim-"));
		const storeRoot = path.join(
			tmp,
			".pnpm",
			"@peerbit+indexer-sqlite3@1.0.0",
			"node_modules",
			"@peerbit",
			"indexer-sqlite3",
		);
		const distPeerbit = path.join(storeRoot, "dist", "peerbit");
		fs.mkdirSync(distPeerbit, { recursive: true });
		const asset = path.join(distPeerbit, "sqlite3.worker.min.js");
		fs.writeFileSync(asset, "");

		const resolver: ModuleResolver = {
			resolve: (id: string) => {
				if (id === "@peerbit/indexer-sqlite3/package.json")
					return path.join(storeRoot, "package.json");
				throw new Error("not found");
			},
		} as any;

		const fsLike: FileSystemLike = {
			existsSync: (p: string) => fs.existsSync(p),
			statSync: (p: string) => fs.statSync(p) as any,
			readdirSync: (p: string) => fs.readdirSync(p),
			mkdirSync: (p: string, o: any) => fs.mkdirSync(p, o),
			copyFileSync: (a: string, b: string) => fs.copyFileSync(a, b),
			realpathSync: (p: string) => fs.realpathSync(p),
		};

		const resolvedDir = TEST_EXPORTS.findLibraryInNodeModules(
			"@peerbit/indexer-sqlite3/dist/assets/sqlite3",
			{
				fs: fsLike,
				resolvers: [resolver],
			},
		);
		// Normalize potential /private prefix on macOS temp dirs
		const normalize = (p: string) => p.replace("/private", "");
		expect(normalize(resolvedDir)).to.equal(normalize(distPeerbit));
	});
});
