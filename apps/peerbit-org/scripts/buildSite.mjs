import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
	APP_DIRECTORY,
	SUBSCRIBE_VARIABLE,
	loadSiteDeploymentEnvironment,
} from "./siteDeploymentEnvironment.mjs";

const vitePackage = fileURLToPath(import.meta.resolve("vite/package.json"));
const viteCli = path.join(path.dirname(vitePackage), "bin", "vite.js");

function modeFromArguments(args) {
	for (let index = 0; index < args.length; index += 1) {
		if (args[index] === "--mode" && args[index + 1]) return args[index + 1];
		if (args[index].startsWith("--mode=")) return args[index].slice(7);
	}
	return "production";
}

export function createSiteBuildEnvironment({
	args = [],
	processEnv = process.env,
	loadEnvImpl,
} = {}) {
	const mode = modeFromArguments(args);
	const resolved = loadSiteDeploymentEnvironment({
		mode,
		processEnv,
		loadEnvImpl,
	});
	const buildEnv = { ...processEnv };
	if (resolved.env[SUBSCRIBE_VARIABLE] !== undefined) {
		buildEnv[SUBSCRIBE_VARIABLE] = resolved.env[SUBSCRIBE_VARIABLE];
	}
	return { ...resolved, buildEnv, mode };
}

async function runViteBuild(args = process.argv.slice(2)) {
	const { buildEnv } = createSiteBuildEnvironment({ args });
	await new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [viteCli, "build", ...args], {
			cwd: APP_DIRECTORY,
			env: buildEnv,
			stdio: "inherit",
		});
		child.once("error", reject);
		child.once("exit", (code, signal) => {
			if (code === 0) resolve();
			else {
				reject(
					new Error(
						signal
							? `Vite build terminated by ${signal}.`
							: `Vite build exited with status ${code}.`,
					),
				);
			}
		});
	});
}

const entrypoint = process.argv[1]
	? pathToFileURL(path.resolve(process.argv[1])).href
	: undefined;
if (entrypoint === import.meta.url) {
	try {
		await runViteBuild();
	} catch (error) {
		process.stderr.write(
			`${error instanceof Error ? error.message : String(error)}\n`,
		);
		process.exitCode = 1;
	}
}
