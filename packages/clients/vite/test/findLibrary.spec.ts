import { expect } from "chai";
import path from "path";
import {
	type FileSystemLike,
	type ModuleResolver,
	__test__,
} from "../src/index.js";

describe("findLibraryInNodeModules", () => {
	const fsMock = (existingPaths: Set<string>): FileSystemLike => ({
		existsSync: (p: string) => existingPaths.has(path.resolve(p)),
		statSync: (p: string) => ({
			isDirectory: () => existingPaths.has(path.resolve(p) + "/"),
		}),
		readdirSync: (_p: string) => [],
		mkdirSync: () => void 0,
		copyFileSync: () => void 0,
		realpathSync: (p: string) => path.resolve(p),
	});

	it("resolves direct asset via resolver", () => {
		const asset =
			"/app/node_modules/@peerbit/indexer-sqlite3/dist/peerbit/sqlite3.worker.min.js";
		const resolver: ModuleResolver = {
			resolve: (id: string) => {
				if (
					id === "@peerbit/indexer-sqlite3/dist/peerbit/sqlite3.worker.min.js"
				)
					return asset;
				throw new Error("not found");
			},
		} as any;
		const got = __test__.findLibraryInNodeModules(
			"@peerbit/indexer-sqlite3/dist/peerbit/sqlite3.worker.min.js",
			{ fs: fsMock(new Set([path.resolve(asset)])), resolvers: [resolver] },
		);
		expect(got).to.equal(path.resolve(asset));
	});

	it("resolves via package.json root + dist suffix", () => {
		const pkgRoot =
			"/store/.pnpm/@peerbit+indexer-sqlite3@1.0.0/node_modules/@peerbit/indexer-sqlite3";
		const resolver: ModuleResolver = {
			resolve: (id: string) => {
				if (id === "@peerbit/indexer-sqlite3/package.json")
					return path.join(pkgRoot, "package.json");
				throw new Error("not found");
			},
		} as any;
		const candidate = path.join(pkgRoot, "dist/peerbit");
		const got = __test__.findLibraryInNodeModules(
			"@peerbit/indexer-sqlite3/dist/peerbit",
			{ fs: fsMock(new Set([candidate])), resolvers: [resolver] },
		);
		expect(got).to.equal(candidate);
	});

	it("falls back to walking up node_modules", () => {
		const nm = path.resolve(
			process.cwd(),
			"node_modules/@peerbit/any-store-opfs/dist/peerbit",
		);
		const existing = new Set([nm]);
		const got = __test__.findLibraryInNodeModules(
			"@peerbit/any-store-opfs/dist/peerbit",
			{
				fs: fsMock(existing),
				resolvers: [],
			},
		);
		expect(path.resolve(got)).to.equal(nm);
	});
});
