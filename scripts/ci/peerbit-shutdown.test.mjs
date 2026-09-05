import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

// A runner's --exit and a child's beforeExit are not process-exit evidence.
// Observe OS close in a fresh child, with no retries or forced-success exit.
for (const mode of ["memory", "disk"]) {
	test(`three default Peerbit peers exit naturally after ${mode} replication`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "peerbit-shutdown-"));
		try {
			const result = await new Promise((resolve, reject) => {
				const child = spawn(
					process.execPath,
					[
						fileURLToPath(
							new URL("./peerbit-shutdown.worker.mjs", import.meta.url),
						),
						mode,
						directory,
					],
					{ stdio: ["ignore", "pipe", "pipe"] },
				);
				let output = "";
				let failure;
				const stop = (reason) => {
					failure ??= reason;
					child.kill("SIGKILL");
				};
				const deadline = setTimeout(
					() => stop("child exceeded unchanged 90-second deadline"),
					90_000,
				);
				const capture = (chunk) => {
					output += chunk.toString();
					if (output.length > 1_048_576) {
						output = output.slice(-1_048_576);
						stop("child exceeded diagnostic output limit");
					}
				};
				child.stdout.on("data", capture);
				child.stderr.on("data", capture);
				child.once("error", (error) => {
					clearTimeout(deadline);
					reject(error);
				});
				child.once("close", (code, signal) => {
					clearTimeout(deadline);
					resolve({ code, signal, failure, output });
				});
			});
			assert.equal(result.failure, undefined, result.output);
			assert.equal(result.signal, null, result.output);
			assert.equal(result.code, 0, result.output);
			assert.match(result.output, /"phase":"converged"/);
			assert.match(result.output, /"phase":"stopped"/);
			console.log(result.output);
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}
