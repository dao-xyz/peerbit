import { createStore } from "@peerbit/any-store";
import {
	CheckpointAmbiguousCommitError,
	CrashSafeTwoSlotCheckpoint,
	isCrashSafeAtomicReplaceStore,
} from "@peerbit/any-store/checkpoint";
import { expect } from "chai";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { makeRecoveryData } from "./utils/checkpoint-recovery-data.js";
import {
	CheckpointRecoveryFixture,
	MAX_RECOVERY_RECORD_BYTES,
	encodeRecoveryInput,
	hex,
} from "./utils/checkpoint-recovery.js";

const within = async <T>(
	promise: Promise<T>,
	step: string,
	timeoutMs = 15_000,
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

type WorkerMessage = {
	event: string;
	phase?: string;
	generationBefore?: string;
	generation?: string;
	gateViolations: number;
	omittedPrefixReads: number;
	checkedPhases: string[];
	view?: unknown;
	snapshot?: unknown;
};

const launchWorker = (workerPath: string, args: string[]) => {
	const child = spawn(process.execPath, [workerPath, ...args], {
		stdio: ["ignore", "pipe", "pipe"],
	});
	const exited = once(child, "close") as Promise<
		[number | null, NodeJS.Signals | null]
	>;
	// A failed spawn can reject this before the caller starts waiting for exit.
	void exited.catch(() => undefined);
	let stdout = "";
	let stderr = "";
	child.stderr?.on("data", (chunk) => {
		stderr += chunk.toString();
	});
	const message = new Promise<WorkerMessage>((resolve, reject) => {
		child.stdout?.on("data", (chunk) => {
			stdout += chunk.toString();
			const lines = stdout.split("\n");
			stdout = lines.pop() ?? "";
			for (const line of lines) {
				if (!line.startsWith("{")) continue;
				let parsed: WorkerMessage;
				try {
					parsed = JSON.parse(line) as WorkerMessage;
				} catch {
					continue;
				}
				if (parsed.event === "paused" || parsed.event === "recovered") {
					resolve(parsed);
					return;
				}
			}
		});
		child.once("error", reject);
		child.once("close", (code, signal) => {
			reject(
				new Error(
					`Recovery worker closed before its report (code=${String(code)}, signal=${String(signal)}): ${stderr}`,
				),
			);
		});
	});
	return { child, exited, message, stderr: () => stderr };
};

const stopWorker = async (worker: ReturnType<typeof launchWorker>) => {
	if (worker.child.exitCode === null && worker.child.signalCode === null) {
		worker.child.kill("SIGKILL");
	}
	await within(worker.exited, "recovery worker cleanup");
};

const resolveWorkerPath = async () => {
	// Work both from source and dist/test, independently of the invoking cwd.
	const besideTest = new URL(
		"./checkpoint-recovery-hard-kill-worker.mjs",
		import.meta.url,
	);
	try {
		await fs.access(besideTest);
		return fileURLToPath(besideTest);
	} catch {
		return fileURLToPath(
			new URL(
				"../../test/checkpoint-recovery-hard-kill-worker.mjs",
				import.meta.url,
			),
		);
	}
};

// Node-only first-bundle publication tests. The fixture exposes an immutable
// recovered view, not a live Documents handle or durable post-publication writes.
describe("Documents checkpoint first-bundle recovery after SIGKILL (Node)", function () {
	this.timeout(120_000);

	for (const phase of ["retained", "entry:1", "replayed", "published"]) {
		it(`rebuilds from retained evidence after SIGKILL at ${phase}`, async function () {
			if (process.platform === "win32") this.skip();
			const directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-checkpoint-recovery-hard-kill-"),
			);
			let writer: ReturnType<typeof launchWorker> | undefined;
			let reader: ReturnType<typeof launchWorker> | undefined;
			try {
				// The source donor is already stopped when this returns. Neither
				// subprocess has a donor connection or receives the omitted prefix.
				const data = await makeRecoveryData();
				const ingressPath = path.join(directory, "ingress.json");
				const trustPath = path.join(directory, "trust.json");
				const storePath = path.join(directory, "checkpoint-store");
				await fs.writeFile(
					ingressPath,
					JSON.stringify({ input: hex(encodeRecoveryInput(data.input)) }),
				);
				await fs.writeFile(
					trustPath,
					JSON.stringify({ ...data.trust, owner: hex(data.trust.owner) }),
				);
				const workerPath = await resolveWorkerPath();
				writer = launchWorker(workerPath, [
					"write",
					storePath,
					trustPath,
					ingressPath,
					phase,
				]);
				const paused = await within(writer.message, `writer phase ${phase}`);
				expect(paused.event).equal("paused");
				expect(paused.phase).equal(phase);
				expect(paused.gateViolations).equal(0);
				expect(paused.omittedPrefixReads).equal(0);
				expect(paused.checkedPhases).to.include("initial");
				expect(paused.checkedPhases.at(-1)).equal(phase);
				expect(paused.generation).equal(phase === "published" ? "2" : "1");
				expect(writer.child.kill("SIGKILL")).equal(true);
				expect(await within(writer.exited, "writer SIGKILL")).deep.equal([
					null,
					"SIGKILL",
				]);

				// Remove ingress entirely: the fresh reader must recover from the
				// two-slot store, not replay an accidentally retained ingress file.
				await fs.rm(ingressPath);
				reader = launchWorker(workerPath, ["read", storePath, trustPath]);
				const recovered = await within(
					reader.message,
					"fresh process recovery",
				);
				expect(recovered.event).equal("recovered");
				expect(recovered.generationBefore).equal(paused.generation);
				expect(recovered.gateViolations).equal(0);
				expect(recovered.omittedPrefixReads).equal(0);
				expect(recovered.checkedPhases).to.include("initial");
				expect(recovered.checkedPhases).to.include("published");
				expect(recovered.view).deep.equal(data.expected);
				expect(recovered.snapshot).deep.equal(data.expected);
				const [code, signal] = await within(
					reader.exited,
					"reader natural exit",
				);
				expect(code, reader.stderr()).equal(0);
				expect(signal).equal(null);
			} finally {
				try {
					if (writer) await stopWorker(writer);
				} finally {
					try {
						if (reader) await stopWorker(reader);
					} finally {
						await fs.rm(directory, { recursive: true, force: true });
					}
				}
			}
		});
	}
});

describe("Documents checkpoint first-bundle ambiguous publication (Node)", function () {
	this.timeout(120_000);

	for (const commitNumber of [1, 2]) {
		for (const failure of ["before", "after"] as const) {
			it(`keeps the view closed when commit ${commitNumber} rejects ${failure} atomic replacement`, async () => {
				const directory = await fs.mkdtemp(
					path.join(os.tmpdir(), "peerbit-checkpoint-recovery-ambiguous-"),
				);
				const store = createStore(directory);
				let reopened: ReturnType<typeof createStore> | undefined;
				try {
					const data = await makeRecoveryData();
					const scope = new TextEncoder().encode(
						`documents-checkpoint-recovery-test:v1:${data.trust.logId}`,
					);
					await store.open();
					const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
						store,
						scope,
						maxPayloadBytes: MAX_RECOVERY_RECORD_BYTES,
					});
					assert(isCrashSafeAtomicReplaceStore(store));
					const durability = store.crashSafeDurability;
					const originalReplace = durability.atomicReplace;
					let calls = 0;
					const fixture = new CheckpointRecoveryFixture();
					try {
						durability.atomicReplace = async (key, bytes) => {
							calls++;
							const fail = calls === commitNumber;
							if (fail && failure === "before") {
								throw new Error("Injected rejection before atomic replacement");
							}
							await originalReplace.call(durability, key, bytes);
							if (fail && failure === "after") {
								throw new Error("Injected rejection after atomic replacement");
							}
						};
						await assert.rejects(
							fixture.recover(data.input, data.trust, checkpoint, {
								phase: () => {
									expect(() => fixture.snapshot()).to.throw("view unavailable");
								},
							}),
							CheckpointAmbiguousCommitError,
						);
						expect(calls).equal(commitNumber);
						expect(() => fixture.snapshot()).to.throw("view unavailable");
						expect(fixture.omittedPrefixReads).equal(0);
						expect(() => checkpoint.current).to.throw(
							CheckpointAmbiguousCommitError,
						);
						await assert.rejects(
							checkpoint.commit(new Uint8Array()),
							CheckpointAmbiguousCommitError,
						);
					} finally {
						durability.atomicReplace = originalReplace;
					}
					await store.close();

					reopened = createStore(directory);
					await reopened.open();
					const recoveredCheckpoint = await CrashSafeTwoSlotCheckpoint.open({
						store: reopened,
						scope,
						maxPayloadBytes: MAX_RECOVERY_RECORD_BYTES,
					});
					const expectedGeneration =
						commitNumber - (failure === "before" ? 1 : 0);
					expect(recoveredCheckpoint.current?.generation).equal(
						expectedGeneration === 0 ? undefined : BigInt(expectedGeneration),
					);
					if (expectedGeneration === 0) {
						const empty = new CheckpointRecoveryFixture();
						await assert.rejects(
							empty.recover(undefined, data.trust, recoveredCheckpoint),
							/No retained checkpoint input/,
						);
						expect(() => empty.snapshot()).to.throw("view unavailable");
					}
					const recovered = new CheckpointRecoveryFixture();
					const view = await recovered.recover(
						expectedGeneration === 0 ? data.input : undefined,
						data.trust,
						recoveredCheckpoint,
						{
							phase: () => {
								expect(() => recovered.snapshot()).to.throw("view unavailable");
							},
						},
					);
					expect(view).deep.equal(data.expected);
					expect(recovered.snapshot()).deep.equal(data.expected);
					expect(recovered.omittedPrefixReads).equal(0);
				} finally {
					try {
						await store.close();
						await reopened?.close();
					} finally {
						await fs.rm(directory, { recursive: true, force: true });
					}
				}
			});
		}
	}
});
