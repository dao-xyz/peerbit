import { use } from "chai";
import chaiAsPromised from "chai-as-promised";

use(chaiAsPromised);

const truthyEnv = (value) => {
	if (!value) return false;
	const normalized = String(value).trim().toLowerCase();
	return normalized === "1" || normalized === "true" || normalized === "yes";
};

// Optional Mocha debug logging to help diagnose hangs in CI:
// - Prints which test starts next (before hooks run)
// - Periodically prints "still running" for the current test
//
// Enable with `PEERBIT_MOCHA_LOG_TEST_START=1` (or `MOCHA_LOG_TEST_START=1`).
if (
	truthyEnv(process.env.PEERBIT_MOCHA_LOG_TEST_START) ||
	truthyEnv(process.env.MOCHA_LOG_TEST_START)
) {
	const quiet =
		truthyEnv(process.env.PEERBIT_MOCHA_LOG_QUIET) ||
		truthyEnv(process.env.MOCHA_LOG_QUIET);

	const mocha = await import("mocha");
	const Runner = mocha.Runner ?? mocha.default?.Runner;
	if (!Runner) {
		throw new Error("Failed to load Mocha Runner for debug logging");
	}

	const PATCH_KEY = Symbol.for("@peerbit/mocha-test-start-logger");
	if (!globalThis[PATCH_KEY]) {
		globalThis[PATCH_KEY] = true;

		let currentTestTitle = "";
		let currentTestStartMs = 0;

		let currentHookTitle = "";
		let currentHookStartMs = 0;

		let currentPhase = "";

		const write = (line) => {
			process.stderr.write(`${line}\n`);
		};

		const now = () => Date.now();

		const intervalMsRaw = process.env.PEERBIT_MOCHA_LOG_INTERVAL_MS ?? "30000";
		const intervalMs = Number(intervalMsRaw);
		let interval;
		if (Number.isFinite(intervalMs) && intervalMs > 0) {
			interval = setInterval(() => {
				const title =
					currentPhase === "hook" ? currentHookTitle : currentTestTitle;
				const startMs =
					currentPhase === "hook" ? currentHookStartMs : currentTestStartMs;
				if (!title || !startMs) return;

				const elapsedMs = now() - startMs;
				if (elapsedMs < intervalMs) return;
				const elapsedSeconds = Math.round(elapsedMs / 1000);
				write(
					`[mocha] … still running (${currentPhase || "unknown"}): ${title} (${elapsedSeconds}s)`,
				);
			}, intervalMs);
			interval.unref?.();
		}

		write(`[mocha] pid=${process.pid} start`);
		const timeoutArg = process.argv.find((a) => a.startsWith("--timeout="));
		if (timeoutArg) {
			write(`[mocha] ${timeoutArg}`);
		}

		const originalEmit = Runner.prototype.emit;
		Runner.prototype.emit = function patchedEmit(event, ...args) {
			if (event === "test") {
				const test = args[0];
				const title =
					typeof test?.fullTitle === "function"
						? test.fullTitle()
						: test?.title;
				currentTestTitle = title || "<unknown test>";
				currentTestStartMs = now();
				currentPhase = "test";
				if (!quiet) {
					write(`[mocha] → ${currentTestTitle}`);
				}
			} else if (event === "hook") {
				const hook = args[0];
				const title =
					typeof hook?.fullTitle === "function"
						? hook.fullTitle()
						: hook?.title;
				currentHookTitle = title || "<unknown hook>";
				currentHookStartMs = now();
				currentPhase = "hook";
				if (!quiet) {
					write(`[mocha] ↪ ${currentHookTitle}`);
				}
			} else if (event === "hook end") {
				currentHookTitle = "";
				currentHookStartMs = 0;
				currentPhase = currentTestTitle ? "test" : "";
			} else if (event === "test end") {
				currentHookTitle = "";
				currentHookStartMs = 0;
				currentTestTitle = "";
				currentTestStartMs = 0;
				currentPhase = "";
			} else if (event === "end") {
				if (interval) {
					clearInterval(interval);
				}
				write(`[mocha] end`);
			}
			return originalEmit.call(this, event, ...args);
		};
	}
}
