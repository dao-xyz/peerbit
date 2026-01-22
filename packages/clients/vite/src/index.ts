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
import { type PluginOption } from "vite";
import { viteStaticCopy } from "vite-plugin-static-copy";

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
) {
	const [sqlite3Assets] = resolveAssetLocations([
		"@peerbit/indexer-sqlite3/dist/assets/sqlite3",
	]);

	return {
		name: "copy-to-public",
		enforce: "pre" as const,
		config(config: any) {
			const publicDir = resolveOrCreatePublicDir(config?.publicDir);
			config.publicDir = publicDir;

			if (!publicDir) {
				throw new Error(
					"[peerbit/vite] No public or static directory found. Please create a public/ or static/ directory or configure publicDir explicitly.",
				);
			}

			// Ensure worker exists in public/ for dev server.
			const destDir = path.resolve(publicDir, sqlite3Assets.dest);
			copyAssets(sqlite3Assets.src, destDir, "/");

			options?.assets?.forEach(({ src, dest }) => {
				const sourcePath = path.resolve(src);
				const destinationPath = path.resolve(publicDir, dest);
				copyAssets(sourcePath, destinationPath, "/");
			});
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

	const staticCopyTargets = assetsToCopy.map(({ src, dest }) => ({
		src,
		dest: path.dirname(dest),
		rename: path.basename(dest),
		overwrite: false,
	}));

	const publicDir = resolveOrCreatePublicDir();

	return [
		dontMinimizeCertainPackagesPlugin({ packages: options.packages }),
		copyToPublicPlugin({
			assets: assetsToCopy,
		}),
		nodePolyfillsPlugin(),
		viteStaticCopy({
			targets: [
				...staticCopyTargets,
				...(publicDir
					? [
							{
								src: path.join(publicDir, "peerbit", "sqlite3", "sqlite3.wasm"),
								dest: "node_modules/.vite/deps",
								overwrite: false,
							},
						]
					: []),
			],
		}),
	];
};

function copyAssets(srcPath: string, destPath: string, base: string) {
	if (!fs.existsSync(srcPath)) {
		throw new Error(`File ${srcPath} does not exist`);
	}

	fs.mkdirSync(path.dirname(destPath), { recursive: true });

	if (fs.statSync(srcPath).isDirectory()) {
		fs.mkdirSync(destPath, { recursive: true });
		fs.readdirSync(srcPath).forEach((file) => {
			const srcFilePath = path.join(srcPath, file);
			const destFilePath = path.join(destPath, file);

			copyAssets(srcFilePath, destFilePath, base);
		});
	} else {
		let destPathAsFile = destPath;
		if (fs.existsSync(destPath) && fs.statSync(destPath).isDirectory()) {
			// get file ending and add it
			destPathAsFile = path.join(destPath, path.basename(srcPath));
		}

		fs.copyFileSync(srcPath, destPathAsFile);
	}
}

// Expose internals for testing
export const TEST_EXPORTS = {
	findLibraryInNodeModules,
	defaultAssetSources,
	resolveAssetLocations,
};
