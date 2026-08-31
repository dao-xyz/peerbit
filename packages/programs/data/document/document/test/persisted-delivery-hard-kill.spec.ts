import { expect } from "chai";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

describe("persisted document delivery crash proof", function () {
	this.timeout(120_000);

	let directory: string | undefined;

	const within = async <T>(
		promise: Promise<T>,
		step: string,
		timeoutMs = 30_000,
	): Promise<T> => {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				promise,
				new Promise<never>((_, reject) => {
					timer = setTimeout(
						() => reject(new Error(`Timed out waiting for ${step}`)),
						timeoutMs,
					);
				}),
			]);
		} finally {
			if (timer) clearTimeout(timer);
		}
	};

	const waitForMessage = <T>(
		child: ReturnType<typeof spawn>,
		step: string,
	): Promise<T> => {
		let stdout = "";
		let stderr = "";
		child.stderr?.on("data", (chunk) => {
			stderr += chunk.toString();
		});
		return within(
			new Promise<T>((resolve, reject) => {
				child.stdout?.on("data", (chunk) => {
					stdout += chunk.toString();
					const lines = stdout.split("\n");
					stdout = lines.pop() ?? "";
					for (const line of lines) {
						if (!line.startsWith("{")) continue;
						try {
							resolve(JSON.parse(line) as T);
							return;
						} catch {
							// Ignore non-protocol output and continue reading.
						}
					}
				});
				child.once("exit", (code, signal) => {
					reject(
						new Error(
							`Worker exited before ${step} (code=${String(code)}, signal=${String(signal)}): ${stderr}`,
						),
					);
				});
			}),
			step,
		);
	};

	afterEach(async () => {
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});

	it("reopens the exact remote replica after receipt and SIGKILL", async function () {
		if (process.platform === "win32") this.skip();
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-persisted-receipt-hard-kill-"),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/persisted-delivery-hard-kill-worker.mjs",
		);
		const writer = spawn(process.execPath, [workerPath, "write", directory], {
			stdio: ["ignore", "pipe", "pipe"],
		});
		const writerExit = once(writer, "exit");
		let receipt: { event: string; hash: string } | undefined;
		try {
			const message = await waitForMessage<{ event: string; hash: string }>(
				writer,
				"persisted remote receipt",
			);
			receipt = message;
			expect(message.event).equal("receipt");
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"receipt worker exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!receipt) throw new Error("Writer returned no receipt");

		const reader = spawn(
			process.execPath,
			[workerPath, "read", directory, receipt.hash],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForMessage<{
				event: string;
				documentName?: string;
				blockPresent: boolean;
				lowerIndexed: boolean;
				coordinatePresent: boolean;
				headHashes: string[];
			}>(reader, "durable receiver reopen");
			expect(reopened).deep.equal({
				event: "reopened",
				documentName: "durable remote receipt",
				blockPresent: true,
				lowerIndexed: true,
				coordinatePresent: true,
				headHashes: [receipt.hash],
			});
			const [exitCode] = await within(readerExit, "reopen worker exit");
			expect(exitCode).equal(0);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});
});
