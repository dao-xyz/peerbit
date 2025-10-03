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
	return baseResolveAssetLocations(sources, createFindLibraryOptions(deps));
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
	return {
		name: "copy-to-public",
		enforce: "pre" as const,
		buildStart() {
			// Ensure worker exists in public/ as a last-resort (CI safety), even if assets disabled
			try {
				// Copy the entire dist/peerbit directory from @peerbit/indexer-sqlite3
				const peerbitDistDir = findLibraryInNodeModules(
					"@peerbit/indexer-sqlite3/dist/peerbit",
				);
				const destDir = path.resolve(resolveStaticPath(), "peerbit");
				copyAssets(peerbitDistDir, destDir, "/");
			} catch (_err) {
				// ignore; optional best-effort
			}
			if (options?.assets) {
				options.assets.forEach(({ src, dest }) => {
					const sourcePath = path.resolve(src);

					let destinationPath = path.resolve(resolveStaticPath(), dest);
					copyAssets(sourcePath, destinationPath, "/");
				});
			}
		},
	};
}

const resolveStaticPath = () => {
	// if public folder exist, then put files there (react)
	// else put in static folder if (svelte)
	// else throw error

	const publicPath = path.resolve(process.cwd(), "public");
	const staticPath = path.resolve(process.cwd(), "static");
	if (fs.existsSync(publicPath)) {
		return publicPath;
	} else if (fs.existsSync(staticPath)) {
		return staticPath;
	} else {
		throw new Error("Could not find public or static folder");
	}
};

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

	return [
		dontMinimizeCertainPackagesPlugin({ packages: options.packages }),
		copyToPublicPlugin({
			assets: assetsToCopy,
		}),
		viteStaticCopy({
			targets: [
				{
					src: `${resolveStaticPath()}/peerbit/sqlite3.wasm`,
					dest: "node_modules/.vite/deps",
				},
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
