// Runs a test command and fails if it reported ZERO passing tests.
//
// The part-7a..7e shards select their suites with `--grep`. A grep that matches
// nothing is not an error to mocha: it runs no tests and exits 0, so the shard
// goes green having executed nothing. Those shards are also the ones excluded
// from ci.yml's "Fail if no coverage files were produced" check, because they
// run `aegir run test` (no --cov) and legitimately produce no coverage files.
// Between the two, part-7 had no did-anything-run signal of any kind: renaming
// a setup name or narrowing a grep silently emptied a whole leg.
//
// check-shard-coverage.mjs closes most of that statically (every top-level
// describe must be matched by some shard grep), but it checks the union of all
// greps -- a title can migrate from one shard's grep to another's and leave the
// first shard empty while the guard stays green. This is the runtime backstop
// for that case.
//
// Usage: node scripts/ci/assert-tests-ran.mjs <binary> [args...]
// The binary is resolved from ./node_modules/.bin when present, so the caller
// does not need a shell (which would mangle the --grep regex).
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";

const [command, ...args] = process.argv.slice(2);
if (!command) {
	console.error("usage: assert-tests-ran.mjs <binary> [args...]");
	process.exit(1);
}

const local = path.join(process.cwd(), "node_modules/.bin", command);
const child = spawn(existsSync(local) ? local : command, args, {
	stdio: ["inherit", "pipe", "inherit"],
});

let passing = 0;
let sawSummary = false;
let buffered = "";
child.stdout.on("data", (chunk) => {
	process.stdout.write(chunk);
	buffered += chunk.toString();
	// Keep the tail only; mocha prints one summary per package.
	const lines = buffered.split("\n");
	buffered = lines.pop() ?? "";
	for (const line of lines) {
		const m = line.match(/(\d+) passing/);
		if (m) {
			sawSummary = true;
			passing += Number(m[1]);
		}
	}
});

child.on("close", (code, signal) => {
	if (signal) {
		console.error(`assert-tests-ran: killed by ${signal}`);
		process.exit(1);
	}
	if (code !== 0) {
		process.exit(code ?? 1);
	}
	if (!sawSummary || passing === 0) {
		console.error(
			`\nassert-tests-ran: the command exited 0 but reported ${sawSummary ? "0 passing tests" : "no test summary at all"}.`,
		);
		console.error(
			`  ${command} ${args.join(" ")}\n` +
				"A shard that runs nothing must not be green. Either its --grep no longer " +
				"matches any describe title (check scripts/ci/check-shard-coverage.mjs), " +
				"or the suites it selects moved to another shard.",
		);
		process.exit(1);
	}
	console.log(`assert-tests-ran: ${passing} passing`);
});
