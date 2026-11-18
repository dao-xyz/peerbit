import { expect } from "chai";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { CrashSafeTwoSlotCheckpoint } from "../src/checkpoint.js";
import type { LevelStore } from "../src/level.js";
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

	it("reopens every putMany entry acknowledged before SIGKILL", async function () {
		if (process.platform === "win32") {
			this.skip();
		}

		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-level-put-many-crash-safe-"),
		);
		let worker: ReturnType<typeof spawn> | undefined;
		let reopened: ReturnType<typeof createStore> | undefined;
		try {
			const workerPath = path.join(
				process.cwd(),
				"test/crash-safe-hard-kill-worker.mjs",
			);
			worker = spawn(process.execPath, [workerPath, directory, "putMany"], {
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
						if (stdout.includes('{"event":"ack","entries":200}')) resolve();
					});
					worker!.once("exit", (code, signal) => {
						reject(
							new Error(
								`Worker exited before putMany acknowledgement (code=${String(code)}, signal=${String(signal)}): ${stderr}`,
							),
						);
					});
				}),
			);

			expect(worker.kill("SIGKILL")).to.equal(true);
			await within(exited.then((): void => undefined));

			reopened = createStore(directory);
			await reopened.open();
			for (let i = 0; i < 200; i++) {
				// The worker writes the same keys and values.
				expect(
					await reopened.get(`put-many-${i}`),
					`put-many-${i}`,
				).to.deep.equal(new Uint8Array(64).fill(i));
			}
		} finally {
			if (worker && worker.exitCode === null && worker.signalCode === null) {
				worker.kill("SIGKILL");
			}
			await reopened?.close();
			await fs.rm(directory, { recursive: true, force: true });
		}
	});
});

describe("LevelStore putMany", function () {
	this.timeout(60_000);

	const MiB = 1024 * 1024;
	let directory: string;
	let store: LevelStore;

	const bytes = (byteLength: number, fill: number) =>
		new Uint8Array(byteLength).fill(fill);

	/** Records the write batches the LevelDB instance receives. */
	const recordBatches = () => {
		const level = store.store as any;
		const batch = level.batch;
		const calls: { keys: string[]; options: any }[] = [];
		level.batch = function (operations: { key: string }[], options: any) {
			calls.push({ keys: operations.map(({ key }) => key), options });
			return batch.call(this, operations, options);
		};
		return {
			calls,
			restore: () => {
				delete level.batch;
			},
		};
	};

	beforeEach(async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-level-put-many-"),
		);
		store = createStore(directory) as LevelStore;
		await store.open();
	});

	afterEach(async () => {
		await store.close();
		await fs.rm(directory, { recursive: true, force: true });
	});

	it("writes entries readable on the root store and on a sublevel", async () => {
		const sublevel = await store.sublevel("put-many");
		await sublevel.open();
		await store.putMany([
			["a", bytes(3, 1)],
			["b", bytes(5, 2)],
		]);
		await sublevel.putMany([
			["a", bytes(4, 3)],
			["c", bytes(6, 4)],
		]);

		expect(await store.get("a")).to.deep.equal(bytes(3, 1));
		expect(await store.get("b")).to.deep.equal(bytes(5, 2));
		expect(await store.get("c")).to.equal(undefined);
		expect(await sublevel.get("a")).to.deep.equal(bytes(4, 3));
		expect(await sublevel.get("b")).to.equal(undefined);
		expect(await sublevel.get("c")).to.deep.equal(bytes(6, 4));
	});

	it("writes a sublevel's entries as synced batches of the root database", async () => {
		// Block stores write through a sublevel. Its batch reaches the root
		// ClassicLevel's own batch implementation, which must still get sync.
		const level = store.store as any;
		const rootBatch = level._batch;
		const calls: { keys: string[]; options: any }[] = [];
		level._batch = function (operations: { key: string }[], options: any) {
			calls.push({ keys: operations.map(({ key }) => key), options });
			return rootBatch.call(this, operations, options);
		};
		const sublevel = await store.sublevel("put-many-sync");
		await sublevel.open();
		try {
			await sublevel.putMany([
				["a", bytes(4, 1)],
				["b", bytes(4, 2)],
			]);
		} finally {
			delete level._batch;
		}

		expect(calls).to.have.length(1);
		expect(calls[0].options?.sync).to.equal(true);
		expect(calls[0].keys).to.deep.equal([
			"!put-many-sync!a",
			"!put-many-sync!b",
		]);
		expect(await sublevel.get("a")).to.deep.equal(bytes(4, 1));
		expect(await sublevel.get("b")).to.deep.equal(bytes(4, 2));
	});

	it("writes a small input as one synced write batch", async () => {
		const entries = Array.from(
			{ length: 10 },
			(_, i) => [`key-${i}`, bytes(16, i)] as const,
		);
		const batches = recordBatches();
		try {
			await store.putMany(entries);
		} finally {
			batches.restore();
		}

		expect(batches.calls).to.have.length(1);
		expect(batches.calls[0].options).to.include({ sync: true });
		expect(batches.calls[0].keys).to.deep.equal(entries.map(([key]) => key));
		for (const [key, value] of entries) {
			expect(await store.get(key), key).to.deep.equal(value);
		}
	});

	it("splits batches at 4 MiB of values and writes a larger value alone", async () => {
		const entries = [
			["a", bytes(1.5 * MiB, 1)],
			["b", bytes(1.5 * MiB, 2)],
			// Would take the first batch to 4.5 MiB.
			["c", bytes(1.5 * MiB, 3)],
			// Over 4 MiB on its own.
			["d", bytes(5 * MiB, 4)],
			["e", bytes(16, 5)],
			["f", bytes(2 * MiB, 6)],
			// Takes the batch to exactly 4 MiB.
			["g", bytes(2 * MiB - 16, 7)],
		] as const;
		const batches = recordBatches();
		try {
			await store.putMany(entries);
		} finally {
			batches.restore();
		}

		expect(batches.calls.map(({ keys }) => keys)).to.deep.equal([
			["a", "b"],
			["c"],
			["d"],
			["e", "f", "g"],
		]);
		for (const { options } of batches.calls) {
			expect(options).to.include({ sync: true });
		}
		for (const [key, value] of entries) {
			expect(await store.get(key), key).to.deep.equal(value);
		}
	});

	it("makes no write for empty input", async () => {
		const batches = recordBatches();
		try {
			await store.putMany([]);
		} finally {
			batches.restore();
		}

		expect(batches.calls).to.have.length(0);
	});

	it("rejects on a closed store with LEVEL_DATABASE_NOT_OPEN", async () => {
		await store.close();
		let error: any;
		try {
			await store.putMany([["a", bytes(1, 1)]]);
		} catch (e) {
			error = e;
		}

		expect(error?.code).to.equal("LEVEL_DATABASE_NOT_OPEN");
	});
});
