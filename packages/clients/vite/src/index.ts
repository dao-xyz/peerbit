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
			if (options && options.assets) {
				options.assets.forEach(({ src, dest }) => {
					const sourcePath = path.resolve(src);
					const destinationPath = path.resolve(process.cwd(), "public", dest);

					copyAssets(sourcePath, destinationPath);
				});
			}
		},
	};
}

export default (
	options: {
		packages?: string[];
		assets?: { src: string; dest: string }[];
	} = {},
): PluginOption[] => {
	return [
		dontMinimizeCertainPackagesPlugin({ packages: options.packages }),
		copyToPublicPlugin({ assets: options.assets }),
	];
};

function copyAssets(srcPath: string, destPath: string) {
	if (fs.statSync(srcPath).isDirectory()) {
		// Ensure the directory exists in the public folder
		fs.mkdirSync(destPath, { recursive: true });

		// Copy each file/directory inside the current directory
		fs.readdirSync(srcPath).forEach((file) => {
			const srcFilePath = path.join(srcPath, file);
			const destFilePath = path.join(destPath, file);
			copyAssets(srcFilePath, destFilePath); // Recursion for directories
		});
	} else {
		// Ensure the destination directory exists
		fs.mkdirSync(path.dirname(destPath), { recursive: true });

		// Copy the file
		fs.copyFileSync(srcPath, destPath);
	}
}
