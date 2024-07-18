import fs from "fs";
import path from "path";
import { type PluginOption } from "vite";

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
				config.optimizeDeps.exclude.push(...options.packages);
			}
		},
	};
}

function copyToPublicPlugin(
	options: { assets?: { src: string; dest: string }[] } = {},
) {
	return {
		name: "copy-to-public",
		buildStart() {
			if (options?.assets) {
				options.assets.forEach(({ src, dest }) => {
					const sourcePath = path.resolve(src);
					const destinationPath = path.resolve(process.cwd(), "public", dest);

					copyAssets(sourcePath, destinationPath);
				});
			}
		},
	};
}

const findLibraryInNodeModules = (library: string) => {
	// scan upwards until we find the node_modules folder
	let currentDir = process.cwd();
	let nodeModulesDir = path.join(currentDir, "node_modules");

	while (!fs.existsSync(path.join(nodeModulesDir, library))) {
		currentDir = path.resolve(currentDir, "..");
		nodeModulesDir = path.join(currentDir, "node_modules");

		// we have found a .git folder, so we are at the root
		// then stop
		if (fs.existsSync(path.join(currentDir, ".git"))) {
			break;
		}
	}
	const libraryPath = path.join(nodeModulesDir, library);
	if (!fs.existsSync(libraryPath)) {
		throw new Error(`Library ${library} not found in node_modules`);
	}

	return libraryPath;
};

let pathsToCopy = [
	"@peerbit/any-store-opfs/dist/peerbit",
	"@peerbit/indexer-sqlite3/dist/peerbit",
];
export default (
	options: {
		packages?: string[];
		assets?: { src: string; dest: string }[];
	} = {},
): PluginOption[] => {
	let defaultAssets = pathsToCopy.map((path) => {
		return {
			src: findLibraryInNodeModules(path),
			dest: "peerbit/",
		};
	});
	return [
		dontMinimizeCertainPackagesPlugin({ packages: options.packages }),
		copyToPublicPlugin({
			assets: [...defaultAssets, ...(options.assets || [])],
		}),
	];
};

function copyAssets(srcPath: string, destPath: string) {
	if (!fs.existsSync(srcPath)) {
		throw new Error(`File ${srcPath} does not exist`);
	}

	if (fs.statSync(srcPath).isDirectory()) {
		// Ensure the directory exists in the public folder
		fs.mkdirSync(destPath, { recursive: true });

		// Copy each file/directory inside the current directory
		fs.readdirSync(srcPath).forEach((file) => {
			const srcFilePath = path.join(srcPath, file);
			const destFilePath = path.join(destPath, file);
			// eslint-disable-next-line no-console
			console.log(`Copying ${srcFilePath} to ${destFilePath}`);

			copyAssets(srcFilePath, destFilePath); // Recursion for directories
		});
	} else {
		// eslint-disable-next-line no-console
		console.log(`Copying ${srcPath} to ${destPath}`);

		// Ensure the destination directory exists
		fs.mkdirSync(path.dirname(destPath), { recursive: true });

		// Copy the file
		fs.copyFileSync(srcPath, destPath);
	}
}
