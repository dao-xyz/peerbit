import fs from "fs";
import path from "path";
import { type PluginOption } from "vite";
import { viteStaticCopy } from "vite-plugin-static-copy";

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
const findLibraryInNodeModules = (library: string) => {
	// scan upwards until we find the node_modules folder
	let maxSearchDepth = 10;
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

		maxSearchDepth--;
		if (maxSearchDepth <= 0) {
			throw new Error(`Could not find ${library} node_modules folder`);
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
	"@peerbit/riblt/dist/rateless_iblt_bg.wasm",
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
