import {
	type FindLibraryOptions,
	type ModuleResolver,
	findLibraryInNodeModules as baseFindLibraryInNodeModules,
	resolveAssetLocations as baseResolveAssetLocations,
	defaultAssetSources,
} from "@peerbit/build-assets";
import fs from "fs";
import { createRequire } from "module";
import path from "path";
import sirv from "sirv";
import {
	type Connect,
	type Plugin,
	type PluginOption,
	type ResolvedConfig,
} from "vite";

export type { ModuleResolver } from "@peerbit/build-assets";

export interface FileSystemLike {
	existsSync(path: string): boolean;
	statSync(path: string): { isDirectory(): boolean };
	readdirSync(path: string): string[];
	mkdirSync(path: string, options: { recursive: boolean }): void;
	copyFileSync(src: string, dest: string): void;
	realpathSync(path: string): string;
}

const requireFromPlugin = createRequire(import.meta.url);

let requireFromCwd: ModuleResolver | undefined;
try {
	requireFromCwd = createRequire(path.join(process.cwd(), "package.json"));
} catch (err) {
	// ignore if no package.json – fall back to plugin resolver
}

const createFindLibraryOptions = (deps?: {
	fs?: FileSystemLike;
	resolvers?: ModuleResolver[];
}): FindLibraryOptions => {
	const resolverCandidates: (ModuleResolver | undefined)[] =
		deps?.resolvers && deps.resolvers.length > 0
			? deps.resolvers
			: [requireFromCwd, requireFromPlugin];

	const mergedResolvers = resolverCandidates.filter(
		(resolver): resolver is ModuleResolver => resolver != null,
	);

	const options: FindLibraryOptions = {
		resolvers: mergedResolvers,
	};

	if (deps?.fs) {
		options.fs = deps.fs as unknown as FindLibraryOptions["fs"];
	}

	return options;
};

const findLibraryInNodeModules = (
	library: string,
	deps?: { fs?: FileSystemLike; resolvers?: ModuleResolver[] },
) => {
	return baseFindLibraryInNodeModules(library, createFindLibraryOptions(deps));
};

const resolveAssetLocations = (
	sources: string[],
	deps?: { fs?: FileSystemLike; resolvers?: ModuleResolver[] },
) => {
	const rewritten = sources.map((s) =>
		s
			.replace("/dist/peerbit", "/dist/src")
			.replace(/\\dist\\peerbit/g, "\\dist\\src"),
	);
	return baseResolveAssetLocations(rewritten, createFindLibraryOptions(deps));
};

function dontMinimizeCertainPackagesPlugin(
	options: { packages?: string[] } = {},
) {
	options.packages = [
		...(options.packages || []),
		"@sqlite.org/sqlite-wasm",
		"@peerbit/any-store",
		"@peerbit/any-store-opfs",
	];
	return {
		name: "dont-minimize-certain-packages",
		config(config: any, { command }: any) {
			if (command === "build") {
				config.optimizeDeps = config.optimizeDeps || {};
				config.optimizeDeps.exclude = config.optimizeDeps.exclude || [];
				const pkgs: string[] = options.packages ?? [];
				config.optimizeDeps.exclude.push(...pkgs);
			}
		},
	};
}

function copyToPublicPlugin(
	options: { assets?: { src: string; dest: string }[] } = {},
	legacyAliasSource?: string,
): Plugin {
	const [sqlite3Assets] = resolveAssetLocations([
		"@peerbit/indexer-sqlite3/dist/assets/sqlite3",
	]);
	const targets = [
		...(options.assets ?? []),
		...(legacyAliasSource
			? [
					{
						src: legacyAliasSource,
						dest: "node_modules/.vite/deps/sqlite3.wasm",
					},
				]
			: []),
	];
	let resolvedConfig: ResolvedConfig;

	return {
		name: "copy-to-public",
		enforce: "pre",
		config(config) {
			const publicDir = resolveOrCreatePublicDir(config?.publicDir);
			config.publicDir = publicDir;

			if (!publicDir) {
				throw new Error(
					"[peerbit/vite] No public or static directory found. Please create a public/ or static/ directory or configure publicDir explicitly.",
				);
			}

			// Ensure worker exists in public/ for dev server.
			const destDir = path.resolve(publicDir, sqlite3Assets.dest);
			copyAssets(sqlite3Assets.src, destDir);

			options?.assets?.forEach(({ src, dest }) => {
				const sourcePath = path.resolve(src);
				const destinationPath = path.resolve(publicDir, dest);
				copyAssets(sourcePath, destinationPath);
			});
		},
		configResolved(config) {
			resolvedConfig = config;
		},
		configureServer(server) {
			const routes = targets.map(({ src, dest }) => {
				const source = fs.realpathSync(path.resolve(server.config.root, src));
				const directory = fs.statSync(source).isDirectory();
				const root = directory ? source : path.dirname(source);
				return {
					source,
					directory,
					root,
					url: path.posix
						.normalize("/" + dest.replace(/\\/g, "/"))
						.replace(/\/$/, ""),
					serve: sirv(root, {
						dev: true,
						etag: true,
						extensions: [],
						setHeaders(res) {
							for (const [name, value] of Object.entries(
								server.config.server.headers ?? {},
							)) {
								if (value !== undefined) res.setHeader(name, value);
							}
						},
					}),
				};
			});
			const middleware: Connect.NextHandleFunction = (req, res, next) => {
				if (!req.url || (req.method !== "GET" && req.method !== "HEAD"))
					return next();
				let pathname: string;
				try {
					pathname = decodeURIComponent(req.url.split("?")[0]!);
				} catch {
					res.statusCode = 400;
					return res.end();
				}
				const route =
					routes.find(({ url }) => pathname === url) ??
					routes.find(
						({ url, directory }) => directory && pathname.startsWith(url + "/"),
					);
				if (!route) return next();
				if (
					pathname.includes("\\") ||
					pathname.includes("\0") ||
					pathname.split("/").some((part) => part === "." || part === "..")
				) {
					res.statusCode = 403;
					return res.end();
				}
				// Existing public copies retain overwrite:false precedence.
				if (fs.existsSync(path.join(server.config.publicDir, pathname)))
					return next();
				const relative = route.directory
					? pathname.slice(route.url.length + 1)
					: path.basename(route.source);
				const source = path.join(route.root, relative);
				try {
					if (!fs.statSync(source, { throwIfNoEntry: false })?.isFile())
						return next();
					const actual = fs.realpathSync(source);
					const relativeActual = path.relative(route.root, actual);
					if (
						route.directory
							? relativeActual === ".." ||
								relativeActual.startsWith(".." + path.sep) ||
								path.isAbsolute(relativeActual)
							: actual !== route.source
					) {
						res.statusCode = 403;
						return res.end();
					}
					const originalUrl = req.url;
					// Encode again so sirv decodes exactly once, including literal '%' names.
					req.url = "/" + relative.split("/").map(encodeURIComponent).join("/");
					try {
						route.serve(req, res, () => {
							req.url = originalUrl;
							next();
						});
					} finally {
						req.url = originalUrl;
					}
				} catch (error) {
					next(error);
				}
			};
			return () => {
				// Retain static-copy's ordering: proxy/base first, then our fallback
				// before Vite's public-file cache and module transforms.
				const index = server.middlewares.stack.findIndex(
					({ handle }) =>
						typeof handle === "function" &&
						["viteServePublicMiddleware", "viteTransformMiddleware"].includes(
							handle.name,
						),
				);
				if (index < 0)
					throw new Error(
						"[peerbit/vite] Asset middleware insertion point not found",
					);
				server.middlewares.stack.splice(index, 0, {
					route: "",
					handle: middleware,
				});
			};
		},
		writeBundle(output) {
			const outDir = path.resolve(
				resolvedConfig.root,
				output.dir ?? resolvedConfig.build.outDir,
			);
			for (const { src, dest } of targets) {
				copyAssets(
					path.resolve(resolvedConfig.root, src),
					path.resolve(outDir, dest),
					false,
				);
			}
		},
	};
}

const resolveOrCreatePublicDir = (configured?: string | false) => {
	if (configured === false) return undefined;
	if (configured) return path.resolve(configured);

	const publicPath = path.resolve(process.cwd(), "public");
	if (fs.existsSync(publicPath)) return publicPath;

	const staticPath = path.resolve(process.cwd(), "static");
	if (fs.existsSync(staticPath)) return staticPath;

	return undefined;
};

function nodePolyfillsPlugin() {
	const resolveEvents = () => {
		try {
			const req = createRequire(import.meta.url);
			return req.resolve("events/");
		} catch {
			// fallback: attempt via cwd
			const req = createRequire(path.join(process.cwd(), "package.json"));
			return req.resolve("events/");
		}
	};

	return {
		name: "peerbit-node-polyfills",
		config(config: any) {
			config.resolve = config.resolve || {};
			config.resolve.alias = config.resolve.alias || {};
			if (!config.resolve.alias.events) {
				config.resolve.alias.events = resolveEvents();
			}

			config.optimizeDeps = config.optimizeDeps || {};
			config.optimizeDeps.include = config.optimizeDeps.include || [];
			if (!config.optimizeDeps.include.includes("events")) {
				config.optimizeDeps.include.push("events");
			}
		},
	};
}

export default (
	options: {
		packages?: string[];
		assets?: { src: string; dest: string }[] | null;
	} = {},
): PluginOption[] => {
	const includeDefaultAssets = options.assets === undefined;
	const userAssets = Array.isArray(options.assets) ? options.assets : [];
	const assetsToCopy = includeDefaultAssets
		? [...resolveAssetLocations(defaultAssetSources), ...userAssets]
		: userAssets;
	const publicDir = resolveOrCreatePublicDir();

	return [
		dontMinimizeCertainPackagesPlugin({ packages: options.packages }),
		copyToPublicPlugin(
			{ assets: assetsToCopy },
			publicDir
				? path.join(publicDir, "peerbit", "sqlite3", "sqlite3.wasm")
				: undefined,
		),
		nodePolyfillsPlugin(),
	];
};

function copyAssets(srcPath: string, destPath: string, overwrite = true) {
	if (!fs.existsSync(srcPath)) {
		throw new Error(`File ${srcPath} does not exist`);
	}

	fs.mkdirSync(path.dirname(destPath), { recursive: true });

	if (fs.statSync(srcPath).isDirectory()) {
		fs.mkdirSync(destPath, { recursive: true });
		fs.readdirSync(srcPath).forEach((file) => {
			const srcFilePath = path.join(srcPath, file);
			const destFilePath = path.join(destPath, file);

			copyAssets(srcFilePath, destFilePath, overwrite);
		});
	} else {
		let destPathAsFile = destPath;
		if (fs.existsSync(destPath) && fs.statSync(destPath).isDirectory()) {
			// get file ending and add it
			destPathAsFile = path.join(destPath, path.basename(srcPath));
		}

		if (overwrite || !fs.existsSync(destPathAsFile)) {
			fs.copyFileSync(srcPath, destPathAsFile);
		}
	}
}

// Expose internals for testing
export const TEST_EXPORTS = {
	findLibraryInNodeModules,
	defaultAssetSources,
	resolveAssetLocations,
};
