import { expect } from "chai";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { CrashSafeTwoSlotCheckpoint } from "../src/checkpoint.js";
import { createStore } from "../src/store.js";

const within = async <T>(promise: Promise<T>, timeoutMs = 15_000) => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error("Timed out waiting for durability worker")),
					timeoutMs,
				);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
};

describe("LevelStore crash-safe durability", function () {
	this.timeout(30_000);

	it("reopens root and sublevel deletes acknowledged before SIGKILL", async function () {
		if (process.platform === "win32") {
			this.skip();
		}

		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-level-crash-safe-"),
		);
		let worker: ReturnType<typeof spawn> | undefined;
		let reopened: ReturnType<typeof createStore> | undefined;
		try {
			const initial = createStore(directory);
			await initial.open();
			const initialSublevel = await initial.sublevel("hard-kill-sublevel");
			await initialSublevel.open();
			await initial.put("root-deleted", new Uint8Array([1]));
			await initialSublevel.put("sublevel-deleted", new Uint8Array([2]));
			await initial.close();

			const workerPath = path.join(
				process.cwd(),
				"test/crash-safe-hard-kill-worker.mjs",
			);
			worker = spawn(process.execPath, [workerPath, directory], {
				stdio: ["ignore", "pipe", "pipe"],
			});
			const exited = once(worker, "exit");
			let stdout = "";
			let stderr = "";
			worker.stderr?.on("data", (chunk) => {
				stderr += chunk.toString();
			});
			await within(
				new Promise<void>((resolve, reject) => {
					worker!.stdout?.on("data", (chunk) => {
						stdout += chunk.toString();
						if (stdout.includes('{"event":"ack"}')) resolve();
					});
					worker!.once("exit", (code, signal) => {
						reject(
							new Error(
								`Worker exited before acknowledgement (code=${String(code)}, signal=${String(signal)}): ${stderr}`,
							),
						);
					});
				}),
			);

			expect(worker.kill("SIGKILL")).to.equal(true);
			await within(exited.then((): void => undefined));

			reopened = createStore(directory);
			await reopened.open();
			const reopenedSublevel = await reopened.sublevel("hard-kill-sublevel");
			await reopenedSublevel.open();
			expect(await reopened.get("root-deleted")).to.equal(undefined);
			expect(await reopenedSublevel.get("sublevel-deleted")).to.equal(
				undefined,
			);
		} finally {
			if (worker && worker.exitCode === null && worker.signalCode === null) {
				worker.kill("SIGKILL");
			}
			await reopened?.close();
			await fs.rm(directory, { recursive: true, force: true });
		}
	});

	it("reopens the exact acknowledged checkpoint generation after SIGKILL", async function () {
		if (process.platform === "win32") {
			this.skip();
		}

		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-checkpoint-crash-safe-"),
		);
		let worker: ReturnType<typeof spawn> | undefined;
		let reopened: ReturnType<typeof createStore> | undefined;
		try {
			const initial = createStore(directory);
			await initial.open();
			const initialCheckpoint = await CrashSafeTwoSlotCheckpoint.open({
				store: initial,
				scope: new TextEncoder().encode("hard-kill-checkpoint"),
				maxPayloadBytes: 16,
			});
			await initialCheckpoint.commit(new Uint8Array([1]));
			await initial.close();

			const workerPath = path.join(
				process.cwd(),
				"test/crash-safe-hard-kill-worker.mjs",
			);
			worker = spawn(process.execPath, [workerPath, directory, "checkpoint"], {
				stdio: ["ignore", "pipe", "pipe"],
			});
			const exited = once(worker, "exit");
			let stdout = "";
			let stderr = "";
			worker.stderr?.on("data", (chunk) => {
				stderr += chunk.toString();
			});
			await within(
				new Promise<void>((resolve, reject) => {
					worker!.stdout?.on("data", (chunk) => {
						stdout += chunk.toString();
						if (stdout.includes('{"event":"ack","generation":"2"}')) {
							resolve();
						}
					});
					worker!.once("exit", (code, signal) => {
						reject(
							new Error(
								`Worker exited before checkpoint acknowledgement (code=${String(code)}, signal=${String(signal)}): ${stderr}`,
							),
						);
					});
				}),
			);

			expect(worker.kill("SIGKILL")).to.equal(true);
			await within(exited.then((): void => undefined));

			reopened = createStore(directory);
			await reopened.open();
			const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
				store: reopened,
				scope: new TextEncoder().encode("hard-kill-checkpoint"),
				maxPayloadBytes: 16,
			});
			expect(checkpoint.current).to.deep.equal({
				generation: 2n,
				payload: new Uint8Array([2, 3, 4]),
			});
		} finally {
			if (worker && worker.exitCode === null && worker.signalCode === null) {
				worker.kill("SIGKILL");
			}
			await reopened?.close();
			await fs.rm(directory, { recursive: true, force: true });
		}
	});
});
