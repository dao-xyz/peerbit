import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const within = async <T>(promise: Promise<T>, step: string): Promise<T> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error(`Timed out waiting for ${step}`)),
					15_000,
				);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
};

type WorkerReport = {
	event: "paused" | "recovered";
	phase?: string;
	address: string;
	proposal?: string;
	checkpoint?: string;
	currentCheckpoint: string;
	expected?: unknown;
	restored?: unknown;
	recoveredFrozen?: boolean;
	committedEntries?: number;
	connections: number;
};

const launch = (workerPath: string, args: string[]) => {
	const child = spawn(process.execPath, [workerPath, ...args], {
		stdio: ["ignore", "pipe", "pipe", "ipc"],
	});
	let output = "";
	child.stderr?.on("data", (chunk) => {
		output += chunk.toString();
	});
	child.stdout?.on("data", (chunk) => {
		output += chunk.toString();
	});
	const exited = once(child, "close") as Promise<
		[number | null, NodeJS.Signals | null]
	>;
	void exited.catch(() => undefined);
	const report = new Promise<WorkerReport>((resolve, reject) => {
		child.once("message", (message) => resolve(message as WorkerReport));
		child.once("error", reject);
		child.once("close", (code, signal) =>
			reject(
				new Error(
					`Worker closed before reporting (code=${String(code)}, signal=${String(signal)}): ${output}`,
				),
			),
		);
	});
	void report.catch(() => undefined);
	return { child, exited, report, output: () => output };
};

const stop = async (worker: ReturnType<typeof launch> | undefined) => {
	if (!worker) return;
	if (worker.child.exitCode === null && worker.child.signalCode === null) {
		worker.child.kill("SIGKILL");
	}
	await within(worker.exited, "worker cleanup");
};

const workerPath = async () => {
	const besideTest = new URL(
		"./utils/checkpoint-documents-hard-kill-worker.mjs",
		import.meta.url,
	);
	try {
		await fs.access(besideTest);
		return fileURLToPath(besideTest);
	} catch {
		return fileURLToPath(
			new URL(
				"../../test/utils/checkpoint-documents-hard-kill-worker.mjs",
				import.meta.url,
			),
		);
	}
};

describe("checkpoint Documents runtime SIGKILL recovery (Node)", function () {
	this.timeout(60_000);
	for (const phase of [
		"put-committed",
		"prepared",
		"published",
		"successor-put",
		"merge-committed",
	]) {
		it(`recovers exact documents after SIGKILL at ${phase}`, async function () {
			if (process.platform === "win32") this.skip();
			const directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-checkpoint-documents-kill-"),
			);
			let writer: ReturnType<typeof launch> | undefined;
			let reader: ReturnType<typeof launch> | undefined;
			try {
				const script = await workerPath();
				writer = launch(script, ["write", directory, phase]);
				const paused = await within(writer.report, `${phase} durable boundary`);
				assert.equal(paused.event, "paused");
				assert.equal(paused.phase, phase);
				assert.equal(paused.connections, 0);
				if (phase === "merge-committed")
					assert.equal(paused.committedEntries, 1);
				assert.equal(writer.child.kill("SIGKILL"), true);
				assert.deepEqual(await within(writer.exited, "writer SIGKILL"), [
					null,
					"SIGKILL",
				]);

				// No expected data, signed entries or snapshot bundle is passed back
				// into the reader. Only the resource and necessary authority anchors.
				const anchors = {
					address: paused.address,
					...(phase === "prepared" ? { proposal: paused.proposal } : {}),
					...(phase === "published" ? { checkpoint: paused.checkpoint } : {}),
				};
				reader = launch(script, ["read", directory, JSON.stringify(anchors)]);
				const recovered = await within(
					reader.report,
					"fresh offline process recovery",
				);
				assert.equal(recovered.event, "recovered");
				assert.equal(recovered.connections, 0);
				const resumedSeal = phase === "prepared" || phase === "merge-committed";
				assert.equal(recovered.recoveredFrozen, resumedSeal);
				assert.deepEqual(recovered.restored, paused.expected);
				if (!resumedSeal) {
					assert.equal(
						recovered.currentCheckpoint,
						paused.checkpoint ?? paused.currentCheckpoint,
					);
				} else {
					assert.notEqual(
						recovered.currentCheckpoint,
						paused.currentCheckpoint,
					);
				}
				assert.deepEqual(
					await within(reader.exited, "reader natural exit"),
					[0, null],
					reader.output(),
				);
			} finally {
				try {
					await stop(writer);
				} finally {
					try {
						await stop(reader);
					} finally {
						await fs.rm(directory, { recursive: true, force: true });
					}
				}
			}
		});
	}
});
