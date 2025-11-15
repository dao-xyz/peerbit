import { spawn } from "node:child_process";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const commands = [
	["--dir", "./shared", "dev"],
	["--dir", "./browser-node", "dev"],
];

const children = commands.map((args) =>
	spawn("pnpm", args, {
		cwd: ROOT,
		stdio: "inherit",
		env: process.env,
	})
);

let exiting = false;
const shutdown = (code = 0) => {
	if (exiting) return;
	exiting = true;
	for (const child of children) {
		if (!child.killed) {
			child.kill("SIGINT");
		}
	}
	process.exit(code);
};

for (const child of children) {
	child.on("exit", (code) => {
		if (!exiting) {
			shutdown(code ?? 0);
		}
	});
	child.on("error", (err) => {
		console.error("Failed to start child process:", err);
		shutdown(1);
	});
}

process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));
