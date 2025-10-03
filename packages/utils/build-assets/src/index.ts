import fs from "fs";
import { createRequire } from "module";
import path from "path";
import { fileURLToPath } from "url";

export type ModuleResolver = {
	resolve(id: string): string;
};

export type FileSystemLike = Pick<
	typeof fs,
	"existsSync" | "statSync" | "realpathSync"
>;

export interface FindLibraryOptions {
	fs?: FileSystemLike;
	resolvers?: (ModuleResolver | string | URL | undefined)[];
}
export interface ResolveAssetLocation {
	src: string;
	dest: string;
}

const normalizeResolvers = (
	resolvers?: (ModuleResolver | string | URL | undefined)[],
): ModuleResolver[] => {
	if (!resolvers?.length) {
		return [];
	}
	return resolvers
		.filter(
			(resolver): resolver is ModuleResolver | string | URL => resolver != null,
		)
		.map((resolver) => {
			if (typeof resolver === "string") {
				return createRequire(path.resolve(resolver));
			}
			if (resolver instanceof URL) {
				return createRequire(fileURLToPath(resolver));
			}
			return resolver;
		});
};

const defaultResolvers: ModuleResolver[] = normalizeResolvers([
	path.join(process.cwd(), "package.json"),
]);

export const findLibraryInNodeModules = (
	library: string,
	opts: FindLibraryOptions = {},
) => {
	const fsLike: FileSystemLike = opts.fs || fs;
	const resolvers = [
		...normalizeResolvers(opts.resolvers),
		...defaultResolvers,
	];
	const [packageName, distSuffix] = library.split("/dist/");

	const resolveWithResolvers = (attemptResolvers: ModuleResolver[]) => {
		for (const resolver of attemptResolvers) {
			try {
				const resolved = resolver.resolve(library);
				if (fsLike.existsSync(resolved)) {
					return fsLike.realpathSync(resolved);
				}
			} catch (_err) {
				// ignore and fallback to package root resolution
			}

			try {
				const packageJsonPath = resolver.resolve(`${packageName}/package.json`);
				const packageRoot = path.dirname(packageJsonPath);
				const candidatePaths = distSuffix
					? [
						path.join(packageRoot, "dist", distSuffix),
						path.join(packageRoot, distSuffix),
					]
					: [packageRoot];

				for (const candidate of candidatePaths) {
					if (fsLike.existsSync(candidate)) {
						return fsLike.realpathSync(candidate);
					}
				}
			} catch (_err) {
				// try next resolver
			}
		}
		return undefined;
	};

	const attempt = resolveWithResolvers(
		resolvers.length ? resolvers : defaultResolvers,
	);
	if (attempt) {
		return attempt;
	}

	let currentDir = process.cwd();
	let safety = 10;
	while (safety-- > 0) {
		const nodeModulesDir = path.join(currentDir, "node_modules");
		const directCandidate = path.join(nodeModulesDir, library);
		if (fsLike.existsSync(directCandidate)) {
			return fsLike.realpathSync(directCandidate);
		}

		const packageJsonCandidate = path.join(
			nodeModulesDir,
			packageName,
			"package.json",
		);
		if (fsLike.existsSync(packageJsonCandidate)) {
			const packageRoot = path.dirname(packageJsonCandidate);
			const candidatePaths = distSuffix
				? [
					path.join(packageRoot, "dist", distSuffix),
					path.join(packageRoot, distSuffix),
				]
				: [packageRoot];
			for (const candidate of candidatePaths) {
				if (fsLike.existsSync(candidate)) {
					return fsLike.realpathSync(candidate);
				}
			}
		}

		const parentDir = path.resolve(currentDir, "..");
		if (parentDir === currentDir) {
			break;
		}
		currentDir = parentDir;
	}

	throw new Error(`Could not locate ${library} in node_modules`);
};

export const defaultAssetSources = [
	"@peerbit/any-store-opfs/dist/peerbit",
	"@peerbit/indexer-sqlite3/dist/peerbit",
	"@peerbit/riblt/dist/rateless_iblt_bg.wasm",
];

export const resolveAssetLocations = (
	sources: string[],
	opts?: FindLibraryOptions,
): ResolveAssetLocation[] => {
	return sources.map((source) => ({
		src: findLibraryInNodeModules(source, opts),
		dest: "peerbit/",
	}));
};
