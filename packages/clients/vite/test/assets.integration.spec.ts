import { expect } from "chai";
import fs from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import os from "node:os";
import path from "node:path";
import { gzipSync } from "node:zlib";
import {
	type InlineConfig,
	type ViteDevServer,
	build,
	createServer,
} from "vite";

// These explicit fixture bytes test asset resolution/serving/copying,
// not worker or Wasm initialization.
const fixtures = {
	"@peerbit/indexer-sqlite3/dist/assets/sqlite3/sqlite3.wasm":
		"fixture-sqlite-wasm",
	"@peerbit/indexer-sqlite3/dist/assets/sqlite3/sqlite3.mjs":
		'export default "fixture-sqlite";',
	"@peerbit/any-store-opfs/dist/assets/opfs/fixture.bin": "fixture-opfs",
	"@peerbit/riblt/dist/assets/riblt/rateless_iblt_bg.wasm":
		"fixture-riblt-wasm",
};
const sqlite = "peerbit/sqlite3/sqlite3.wasm";
const alias = "node_modules/.vite/deps/sqlite3.wasm";
let fixtureId = 0;

const write = async (file: string, bytes: string) => {
	await fs.mkdir(path.dirname(file), { recursive: true });
	await fs.writeFile(file, bytes);
};

const withProject = async (
	run: (
		root: string,
		peerbit: typeof import("../src/index.js").default,
	) => Promise<void>,
) => {
	const previousCwd = process.cwd();
	const fixtureRoot = await fs.realpath(
		await fs.mkdtemp(path.join(os.tmpdir(), "peerbit-vite-assets-")),
	);
	const root = path.join(fixtureRoot, "project");
	try {
		await write(path.join(root, "package.json"), '{"type":"module"}');
		await write(path.join(root, "index.html"), "<h1>fixture</h1>");
		await fs.mkdir(path.join(root, "public"));
		for (const [asset, bytes] of Object.entries(fixtures)) {
			const packageName = asset.split("/dist/")[0];
			await write(
				path.join(root, "node_modules", packageName, "package.json"),
				JSON.stringify({ name: packageName, version: "0.0.0", type: "module" }),
			);
			await write(path.join(root, "node_modules", asset), bytes);
		}
		process.chdir(root);
		// The plugin captures its cwd resolver at module evaluation. A fresh
		// module per project avoids dependence on Mocha file/import order.
		const moduleUrl = new URL("../src/index.js", import.meta.url);
		moduleUrl.searchParams.set("fixture", String(++fixtureId));
		const { default: peerbit } = await import(moduleUrl.href);
		await run(root, peerbit);
	} finally {
		process.chdir(previousCwd);
		await fs.rm(fixtureRoot, { recursive: true, force: true });
	}
};

const config = (
	root: string,
	plugins: InlineConfig["plugins"],
): InlineConfig => ({
	root,
	plugins,
	configFile: false,
	logLevel: "silent",
	// No app imports are under test; avoid an unrelated events package resolver.
	resolve: { alias: { events: "node:events" } },
	server: { host: "127.0.0.1", port: 0 },
});

const serve = async (
	options: InlineConfig,
	run: (server: ViteDevServer, origin: string) => Promise<void>,
) => {
	const server = await createServer(options);
	try {
		await server.listen();
		await run(server, new URL(server.resolvedUrls!.local[0]!).origin);
	} finally {
		await server.close();
	}
};

const readResponse = async (origin: string, route: string) => {
	const response = await fetch(origin + route, {
		headers: { accept: "application/octet-stream" },
	});
	return { status: response.status, bytes: await response.text() };
};
const expectResponse = async (origin: string, route: string, bytes: string) => {
	expect(await readResponse(origin, route)).to.deep.equal({
		status: 200,
		bytes,
	});
};
const exists = async (file: string) =>
	fs.stat(file).then(
		() => true,
		() => false,
	);

describe("Vite literal asset integration", () => {
	for (const assets of [undefined, [], null] as const) {
		it(`serves and builds ${assets === undefined ? "default" : assets === null ? "null" : "empty"} assets with the legacy SQLite alias`, async () => {
			await withProject(async (root, peerbit) => {
				const plugins = () =>
					peerbit({ assets: assets as [] | null | undefined });
				await serve(config(root, plugins()), async (_server, origin) => {
					await expectResponse(origin, `/${sqlite}`, "fixture-sqlite-wasm");
					await expectResponse(origin, `/${alias}`, "fixture-sqlite-wasm");
					for (const [route, bytes] of [
						["peerbit/opfs/fixture.bin", "fixture-opfs"],
						["peerbit/riblt/rateless_iblt_bg.wasm", "fixture-riblt-wasm"],
					]) {
						if (assets === undefined)
							await expectResponse(origin, `/${route}`, bytes);
						else
							expect((await readResponse(origin, `/${route}`)).status).to.equal(
								404,
							);
					}
				});
				const outDir = path.join(root, "output");
				await build({
					...config(root, plugins()),
					build: { outDir, copyPublicDir: false },
				});
				expect(await fs.readFile(path.join(outDir, alias), "utf8")).to.equal(
					"fixture-sqlite-wasm",
				);
				for (const [route, bytes] of [
					[sqlite, "fixture-sqlite-wasm"],
					["peerbit/opfs/fixture.bin", "fixture-opfs"],
					["peerbit/riblt/rateless_iblt_bg.wasm", "fixture-riblt-wasm"],
				]) {
					if (assets === undefined)
						expect(
							await fs.readFile(path.join(outDir, route), "utf8"),
						).to.equal(bytes);
					else expect(await exists(path.join(outDir, route))).to.equal(false);
				}
			});
		});
	}

	it("serves live directory additions, keeps public precedence, and confines literal routes under a base URL", async () => {
		await withProject(async (root, peerbit) => {
			// External sources ensure traversal cannot be mistaken for Vite serving
			// a legitimately accessible file from its own application root.
			const source = path.join(root, "..", "external-source");
			const dependency = path.join(
				root,
				"node_modules",
				"fixture-library",
				"assets",
			);
			await write(
				path.join(source, "directory", "existing.txt"),
				"source-at-start",
			);
			await write(path.join(source, "selected.bin"), "selected-file");
			await write(path.join(source, "sibling.bin"), "private-sibling");
			await write(path.join(source, "secret.txt"), "private-parent");
			await write(
				path.join(source, "root-assets", "root-existing.bin"),
				"root-at-start",
			);
			await write(
				path.join(source, "root-assets", "one", "renamed.bin"),
				"broad-shadow",
			);
			await write(path.join(dependency, "existing.txt"), "dependency-at-start");
			const assets = [
				{ src: path.join(source, "directory"), dest: "custom/" },
				{ src: dependency, dest: "dependency" },
				// Directory prefixes retain configured order; exact files take priority.
				{ src: path.join(source, "root-assets"), dest: "." },
				{ src: path.join(source, "selected.bin"), dest: "one/renamed.bin" },
			];
			await serve(
				{ ...config(root, peerbit({ assets })), base: "/app/" },
				async (_server, origin) => {
					await expectResponse(
						origin,
						"/app/custom/existing.txt",
						"source-at-start",
					);
					await expectResponse(origin, "/app/one/renamed.bin", "selected-file");
					await fs.rm(path.join(root, "public", "one", "renamed.bin"));
					await expectResponse(origin, "/app/one/renamed.bin", "selected-file");
					const head = await fetch(origin + "/app/one/renamed.bin", {
						method: "HEAD",
					});
					expect(head.status).to.equal(200);
					expect(head.headers.get("content-length")).to.equal("13");
					expect(await head.text()).to.equal("");
					const range = await fetch(origin + "/app/one/renamed.bin", {
						headers: { range: "bytes=0-7" },
					});
					expect(range.status).to.equal(206);
					expect(range.headers.get("content-range")).to.equal("bytes 0-7/13");
					expect(await range.text()).to.equal("selected");
					await expectResponse(origin, `/app/${alias}`, "fixture-sqlite-wasm");
					await write(
						path.join(source, "directory", "existing.txt"),
						"source-edited",
					);
					await expectResponse(
						origin,
						"/app/custom/existing.txt",
						"source-at-start",
					);
					await write(
						path.join(root, "public", "custom", "existing.txt"),
						"public-copy-wins",
					);
					await expectResponse(
						origin,
						"/app/custom/existing.txt",
						"public-copy-wins",
					);
					await write(
						path.join(source, "directory", "added.txt"),
						"directory-added",
					);
					await write(path.join(dependency, "added.txt"), "dependency-added");
					await write(
						path.join(source, "root-assets", "root-added.bin"),
						"root-added",
					);
					// Fetch immediately: no watcher delay, sleep, or retry is allowed.
					await expectResponse(
						origin,
						"/app/custom/added.txt",
						"directory-added",
					);
					await expectResponse(
						origin,
						"/app/dependency/added.txt",
						"dependency-added",
					);
					await expectResponse(origin, "/app/root-added.bin", "root-added");
					for (const route of [
						"/app/one/sibling.bin",
						"/app/one/renamed.bin/sibling.bin",
					])
						expect((await readResponse(origin, route)).status, route).to.equal(
							404,
						);
					for (const route of [
						"/app/custom/%2e%2e%2fsecret.txt",
						"/app/custom/%2e%2e%5csecret.txt",
						"/app/custom/%252e%252e%252fsecret.txt",
					]) {
						const response = await readResponse(origin, route);
						expect(response.status, route).to.be.oneOf([400, 403, 404]);
						expect(response.bytes).not.to.contain("private-parent");
					}
				},
			);
		});
	});

	it("keeps Vite proxy routes ahead of overlapping literal and public assets", async () => {
		await withProject(async (root, peerbit) => {
			const source = path.join(root, "..", "proxy-assets");
			await write(path.join(source, "item.bin"), "local-asset");
			const requests: Array<string | undefined> = [];
			const upstream = createHttpServer((request, response) => {
				requests.push(request.url);
				response.end("upstream-proxy");
			});
			await new Promise<void>((resolve, reject) => {
				upstream.once("error", reject);
				upstream.listen(0, "127.0.0.1", () => {
					upstream.off("error", reject);
					resolve();
				});
			});
			try {
				const address = upstream.address();
				if (!address || typeof address === "string")
					throw new Error("Missing proxy port");
				const options = config(
					root,
					peerbit({
						assets: [{ src: source, dest: "custom" }],
					}),
				);
				await serve(
					{
						...options,
						server: {
							...options.server,
							proxy: { "/custom": `http://127.0.0.1:${address.port}` },
						},
					},
					async (_server, origin) => {
						expect(
							await fs.readFile(
								path.join(root, "public", "custom", "item.bin"),
								"utf8",
							),
						).to.equal("local-asset");
						await expectResponse(
							origin,
							"/custom/item.bin?test=proxy",
							"upstream-proxy",
						);
						expect(requests).to.deep.equal(["/custom/item.bin?test=proxy"]);
					},
				);
			} finally {
				await new Promise<void>((resolve, reject) => {
					upstream.close((error) => (error ? reject(error) : resolve()));
				});
			}
		});
	});

	it("uses destination MIME types for renamed and newly added fallback assets", async () => {
		await withProject(async (root, peerbit) => {
			const source = path.join(root, "source");
			await write(path.join(source, "module.bin"), "wasm-bytes");
			await serve(
				config(
					root,
					peerbit({
						assets: [
							{ src: source, dest: "custom" },
							{ src: path.join(source, "module.bin"), dest: "renamed.wasm" },
						],
					}),
				),
				async (_server, origin) => {
					await fs.rm(path.join(root, "public", "renamed.wasm"));
					await write(
						path.join(source, "added.mts"),
						"export const value = 1;",
					);
					for (const [url, type] of [
						["/renamed.wasm", "application/wasm"],
						["/custom/added.mts", "text/javascript"],
					]) {
						const response = await fetch(origin + url);
						expect(response.status).to.equal(200);
						expect(response.headers.get("content-type")).to.equal(type);
						await response.arrayBuffer();
					}
				},
			);
		});
	});

	it("serves compressed custom assets as opaque bytes unless encoding is configured", async () => {
		await withProject(async (root, peerbit) => {
			const source = path.join(root, "source");
			await fs.mkdir(source);
			await serve(
				config(root, peerbit({ assets: [{ src: source, dest: "custom" }] })),
				async (_server, origin) => {
					const bytes = gzipSync("opaque-compressed-file");
					await fs.writeFile(path.join(source, "added.gz"), bytes);
					const response = await fetch(origin + "/custom/added.gz");
					expect(response.status).to.equal(200);
					expect(Buffer.from(await response.arrayBuffer())).to.deep.equal(
						bytes,
					);
				},
			);
		});
	});

	it("rejects a newly added directory symlink escaping its literal asset root", async () => {
		await withProject(async (root, peerbit) => {
			const source = path.join(root, "..", "linked-assets");
			const outside = path.join(root, "..", "private-assets");
			await write(path.join(source, "safe.bin"), "allowed-asset");
			await write(path.join(outside, "secret.bin"), "outside-secret");
			await serve(
				config(
					root,
					peerbit({
						assets: [{ src: source, dest: "custom" }],
					}),
				),
				async (_server, origin) => {
					await expectResponse(origin, "/custom/safe.bin", "allowed-asset");
					// Create after the public copy: this probes the live route's boundary,
					// not the separate, pre-existing recursive public-copy behavior.
					await fs.symlink(
						outside,
						path.join(source, "escape"),
						process.platform === "win32" ? "junction" : "dir",
					);
					const response = await readResponse(
						origin,
						"/custom/escape/secret.bin",
					);
					expect(response.status).to.equal(403);
					expect(response.bytes).not.to.contain("outside-secret");
				},
			);
		});
	});

	it("builds explicit files and directories when Vite public copying is disabled", async () => {
		await withProject(async (root, peerbit) => {
			await write(
				path.join(root, "source", "directory", "nested", "item.bin"),
				"custom-directory",
			);
			await write(path.join(root, "source", "file.bin"), "custom-file");
			await write(path.join(root, "public", "unrelated.bin"), "not-requested");
			const outDir = path.join(root, "output");
			await build({
				...config(
					root,
					peerbit({
						assets: [
							{ src: path.join(root, "source", "directory"), dest: "custom" },
							{
								src: path.join(root, "source", "file.bin"),
								dest: "renamed.bin",
							},
						],
					}),
				),
				build: { outDir, copyPublicDir: false },
			});
			for (const [route, bytes] of [
				["custom/nested/item.bin", "custom-directory"],
				["renamed.bin", "custom-file"],
				[alias, "fixture-sqlite-wasm"],
			])
				expect(await fs.readFile(path.join(outDir, route), "utf8")).to.equal(
					bytes,
				);
			expect(await exists(path.join(outDir, sqlite))).to.equal(false);
			expect(await exists(path.join(outDir, "unrelated.bin"))).to.equal(false);
		});
	});

	it("keeps public output precedence and merges newly added assets in a normal build", async () => {
		await withProject(async (root, peerbit) => {
			const source = path.join(root, "source");
			await write(path.join(source, "existing.bin"), "copied-to-public");
			await write(path.join(root, "public", "unrelated.bin"), "public-only");
			const plugins = peerbit({ assets: [{ src: source, dest: "custom" }] });
			plugins.push({
				name: "edit-after-public-copy",
				async buildStart() {
					await write(path.join(source, "existing.bin"), "source-edited");
					await write(path.join(source, "added.bin"), "source-added");
				},
			});
			const outDir = path.join(root, "output");
			await build({ ...config(root, plugins), build: { outDir } });
			for (const [route, bytes] of [
				["custom/existing.bin", "copied-to-public"],
				["custom/added.bin", "source-added"],
				["unrelated.bin", "public-only"],
				[sqlite, "fixture-sqlite-wasm"],
				[alias, "fixture-sqlite-wasm"],
			])
				expect(await fs.readFile(path.join(outDir, route), "utf8")).to.equal(
					bytes,
				);
		});
	});

	for (const mode of ["relative", "absolute", "disabled"] as const) {
		it(`preserves ${mode} publicDir configuration behavior`, async () => {
			await withProject(async (root, peerbit) => {
				const nested = path.join(root, "nested");
				await write(path.join(nested, "index.html"), "<h1>nested fixture</h1>");
				// Preserve the factory-time legacy alias gate: there is no default
				// cwd public/ here, so an explicit publicDir does not request that alias.
				await fs.rmdir(path.join(root, "public"));
				const publicDir =
					mode === "disabled"
						? false
						: mode === "absolute"
							? path.join(root, "absolute-public")
							: "relative-public";
				const options: InlineConfig = {
					...config(nested, peerbit({ assets: [] })),
					publicDir,
				};
				if (mode === "disabled") {
					const outcome = await createServer(options).then(
						async (server): Promise<void> => {
							await server.close();
							return undefined;
						},
						(error: unknown) => error,
					);
					expect(outcome).to.be.instanceOf(Error);
					expect((outcome as Error).message).to.contain(
						"No public or static directory found",
					);
				} else {
					await serve(options, async (server, origin) => {
						// Existing contract: relative publicDir is cwd-relative, not root-relative.
						expect(server.config.publicDir).to.equal(
							path.resolve(root, publicDir as string),
						);
						await expectResponse(origin, `/${sqlite}`, "fixture-sqlite-wasm");
					});
				}
			});
		});
	}
});
