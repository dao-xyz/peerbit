import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

export const VITE_BENCHMARK_MARKER = "/* peerbit-benchmark-vite */";

const markedResolveGuardPattern = () =>
	new RegExp(
		` {8}\\/\\* peerbit-benchmark-vite \\*\\/\\n(?: {8}["']?preserveSymlinks["']?\\s*:\\s*(?:true|false),\\n)? {8}alias: \\{\\n[\\s\\S]*? {8}\\},\\n`,
	);

const countMatches = (contents, pattern) =>
	contents.match(pattern)?.length ?? 0;

const ensureRequiredDedupeEntries = (contents, filePath) => {
	const dedupePattern = /( {8}dedupe: \[\n)([\s\S]*?)( {8}\],)/;
	const match = contents.match(dedupePattern);
	if (!match) {
		throw new Error(`Could not find dedupe block in ${filePath}`);
	}
	const required = ["react", "react-dom", "@dao-xyz/borsh"];
	const requiredLines = new Set(required.map((name) => `"${name}",`));
	const retainedBody = match[2]
		.split("\n")
		.filter((line) => !requiredLines.has(line.trim()))
		.join("\n");
	const normalizedBody = `${required
		.map((name) => `            "${name}",\n`)
		.join("")}${retainedBody}`;
	return contents.replace(
		dedupePattern,
		`${match[1]}${normalizedBody}${match[3]}`,
	);
};

export const injectViteBenchmarkResolveGuards = (
	contents,
	filePath,
	frontendRoot,
) => {
	let next = contents;
	if (!next.includes("resolve: {")) {
		throw new Error(`Could not find resolve block in ${filePath}`);
	}
	const examplesNodeModules = path.resolve(
		frontendRoot,
		"..",
		"..",
		"..",
		"node_modules",
	);
	const resolveGuardBlock = `        ${VITE_BENCHMARK_MARKER}
        preserveSymlinks: false,
        alias: {
            react: ${JSON.stringify(path.join(examplesNodeModules, "react"))},
            "react-dom": ${JSON.stringify(path.join(examplesNodeModules, "react-dom"))},
            "@dao-xyz/borsh": ${JSON.stringify(
							path.join(examplesNodeModules, "@dao-xyz", "borsh"),
						)},
        },\n`;
	const markerCount = countMatches(next, /\/\* peerbit-benchmark-vite \*\//g);
	if (markerCount > 1) {
		throw new Error(`${filePath} contains multiple benchmark resolve guards`);
	}
	if (markerCount === 1) {
		const markedBlock = markedResolveGuardPattern();
		if (!markedBlock.test(next)) {
			throw new Error(
				`${filePath} contains a malformed benchmark resolve guard`,
			);
		}
		next = next.replace(markedResolveGuardPattern(), resolveGuardBlock);
	} else {
		next = next.replace(
			"    resolve: {\n",
			`    resolve: {\n${resolveGuardBlock}`,
		);
	}
	if (/["']?preserveSymlinks["']?\s*:\s*true/.test(next)) {
		throw new Error(
			`${filePath} enables preserveSymlinks, which would detach linked Peerbit packages from their pinned dependency graph`,
		);
	}
	if (
		countMatches(next, /\/\* peerbit-benchmark-vite \*\//g) !== 1 ||
		countMatches(next, /["']?preserveSymlinks["']?\s*:/g) !== 1 ||
		countMatches(next, /["']?alias["']?\s*:/g) !== 1 ||
		!markedResolveGuardPattern().test(next)
	) {
		throw new Error(
			`${filePath} does not contain exactly one attributable benchmark resolve guard`,
		);
	}
	next = ensureRequiredDedupeEntries(next, filePath);
	next = next.replace(
		'        include: [\n            "react",\n            "react-dom",\n',
		"        include: [\n",
	);
	return next;
};

export const instrumentFileShareViteConfigs = async (frontendRoot) => {
	for (const configName of ["vite.config.ts", "vite.config.remote.ts"]) {
		const configPath = path.join(frontendRoot, configName);
		if (!fs.existsSync(configPath)) {
			continue;
		}
		const contents = await fsp.readFile(configPath, "utf8");
		const next = injectViteBenchmarkResolveGuards(
			contents,
			configPath,
			frontendRoot,
		);
		if (next !== contents) {
			await fsp.writeFile(configPath, next);
		}
	}
};
