import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";

const require = createRequire(import.meta.url);
const { createCoverageMap } = require("istanbul-lib-coverage");
const { createSourceMapStore } = require("istanbul-lib-source-maps");

const rootDir = process.cwd();
const coverageBasenames = new Set(["coverage-final.json", "coverage-pw.json"]);

const toPosix = (p) => p.split(path.sep).join("/");

const toRepoRelativePath = (filePath) => {
	if (path.isAbsolute(filePath)) {
		return toPosix(path.relative(rootDir, filePath));
	}
	return toPosix(path.normalize(filePath));
};

const findCoverageFiles = (dir, out = []) => {
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		const fullPath = path.join(dir, entry.name);
		if (entry.isDirectory()) {
			if (entry.name === "node_modules" || entry.name === ".git") continue;
			findCoverageFiles(fullPath, out);
			continue;
		}
		if (!fullPath.includes(`${path.sep}.coverage${path.sep}`)) continue;
		if (coverageBasenames.has(entry.name)) out.push(fullPath);
	}
	return out;
};

const remapCoverageFile = async (coverageFile) => {
	const raw = JSON.parse(fs.readFileSync(coverageFile, "utf8"));
	const coverageMap = createCoverageMap(raw);
	const remappedCoverage = createCoverageMap({});
	let registeredMaps = 0;
	let skippedMaps = 0;
	let remapFailed = false;

	for (const coveredFile of coverageMap.files()) {
		const singleCoverage = createCoverageMap({
			[coveredFile]: coverageMap.fileCoverageFor(coveredFile).toJSON(),
		});
		if (
			!coveredFile.endsWith(".js") &&
			!coveredFile.endsWith(".cjs") &&
			!coveredFile.endsWith(".mjs")
		) {
			remappedCoverage.merge(singleCoverage);
			continue;
		}

		const sourceMapPath = `${coveredFile}.map`;
		if (!fs.existsSync(sourceMapPath)) {
			remappedCoverage.merge(singleCoverage);
			continue;
		}

		try {
			const rawMap = JSON.parse(fs.readFileSync(sourceMapPath, "utf8"));
			const sourceMapStore = createSourceMapStore();
			sourceMapStore.registerMap(coveredFile, rawMap);
			const transformed = await sourceMapStore.transformCoverage(singleCoverage);
			remappedCoverage.merge(transformed);
			registeredMaps += 1;
		} catch (error) {
			skippedMaps += 1;
			console.warn(
				`Warning: skipping invalid source map ${toRepoRelativePath(sourceMapPath)}: ${error.message}`,
			);
			remappedCoverage.merge(singleCoverage);
		}
	}

	const output = {};
	for (const coveredFile of remappedCoverage.files()) {
		const normalizedPath = toRepoRelativePath(coveredFile);
		const entry = remappedCoverage.fileCoverageFor(coveredFile).toJSON();
		entry.path = normalizedPath;
		output[normalizedPath] = entry;
	}

	const outFile = path.join(
		path.dirname(coverageFile),
		"coverage-remapped.json",
	);
	fs.writeFileSync(outFile, JSON.stringify(output));

	return {
		coverageFile,
		outFile,
		filesIn: coverageMap.files().length,
		filesOut: remappedCoverage.files().length,
		sourceMaps: registeredMaps,
		skippedMaps,
		remapFailed,
	};
};

const main = async () => {
	const coverageFiles = findCoverageFiles(rootDir).sort();
	if (coverageFiles.length === 0) {
		console.log("No coverage files found under */.coverage/");
		return;
	}

	let successCount = 0;
	for (const coverageFile of coverageFiles) {
		try {
			const result = await remapCoverageFile(coverageFile);
			successCount += 1;
			console.log(
				[
					`Remapped ${toRepoRelativePath(result.coverageFile)} -> ${toRepoRelativePath(result.outFile)}`,
					`(files ${result.filesIn} -> ${result.filesOut}`,
					`source maps: ${result.sourceMaps}${result.skippedMaps ? `, skipped: ${result.skippedMaps}` : ""}${result.remapFailed ? ", fallback: original paths" : ""})`,
				].join(" "),
			);
		} catch (error) {
			console.error(
				`Failed to remap ${toRepoRelativePath(coverageFile)}: ${error.stack || error.message}`,
			);
			process.exitCode = 1;
		}
	}

	console.log(
		`Remapped ${successCount}/${coverageFiles.length} coverage reports`,
	);
};

await main();
