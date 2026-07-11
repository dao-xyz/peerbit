import { expect } from "chai";
import { type ChildProcess, fork } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
	type NativeDurabilityLease,
	NativeDurabilityLeaseClosedError,
	NativeDurabilityLeaseUnavailableError,
} from "../src/durability/lease.js";
import { acquireNativeDurabilityNodeLease } from "../src/durability/node-lease.js";

type WorkerFence = {
	epoch: string;
	ownerId: string;
	domainId: string;
};

type WorkerMessage =
	| { event: "held"; fence: WorkerFence }
	| { event: "error"; name?: string; code?: string; message: string };

const workerPath = fileURLToPath(
	new URL("./fixtures/directory-lease-worker.js", import.meta.url),
);

const waitForWorkerMessage = async (
	child: ChildProcess,
): Promise<WorkerMessage> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			new Promise<WorkerMessage>((resolve, reject) => {
				child.once("message", (message) => resolve(message as WorkerMessage));
				child.once("error", reject);
				child.once("exit", (code, signal) => {
					reject(
						new Error(
							`Lease worker exited before replying (code=${String(code)}, signal=${String(signal)})`,
						),
					);
				});
			}),
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error("Timed out waiting for lease worker")),
					10_000,
				);
			}),
		]);
	} finally {
		if (timer) {
			clearTimeout(timer);
		}
	}
};

describe("native durability directory lease", () => {
	const directories: string[] = [];
	const children = new Set<ChildProcess>();
	const leases = new Set<NativeDurabilityLease>();

	beforeEach(function () {
		if (process.platform === "win32") {
			this.skip();
		}
	});

	afterEach(async () => {
		const childExits = [...children]
			.filter((child) => child.exitCode === null && child.signalCode === null)
			.map((child) => {
				const exited = once(child, "exit");
				child.kill("SIGKILL");
				return exited;
			});
		await Promise.allSettled([...leases].map((lease) => lease.close()));
		await Promise.allSettled(childExits);
		children.clear();
		leases.clear();
		await Promise.all(
			directories
				.splice(0)
				.map((directory) => rm(directory, { recursive: true, force: true })),
		);
	});

	const temporaryDirectory = async (): Promise<string> => {
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-durability-lease-"),
		);
		directories.push(directory);
		return directory;
	};

	it("rejects a second opener and advances its persistent fence", async () => {
		const directory = await temporaryDirectory();
		const first = await acquireNativeDurabilityNodeLease(directory);
		leases.add(first);

		let secondError: unknown;
		try {
			await acquireNativeDurabilityNodeLease(directory);
		} catch (error) {
			secondError = error;
		}
		expect(secondError).to.be.instanceOf(NativeDurabilityLeaseUnavailableError);

		await first.close();
		leases.delete(first);
		let closedError: unknown;
		try {
			await first.assertHeld();
		} catch (error) {
			closedError = error;
		}
		expect(closedError).to.be.instanceOf(NativeDurabilityLeaseClosedError);

		const reopened = await acquireNativeDurabilityNodeLease(directory);
		leases.add(reopened);
		expect(reopened.fence.epoch).to.equal(first.fence.epoch + 1n);
		expect(reopened.fence.domainId).to.equal(first.fence.domainId);
		expect(reopened.fence.ownerId).to.not.equal(first.fence.ownerId);
	});

	it("keeps the OS lock until guarded operations drain", async () => {
		const directory = await temporaryDirectory();
		const first = await acquireNativeDurabilityNodeLease(directory);
		leases.add(first);
		let releaseOperation!: () => void;
		const operationGate = new Promise<void>((resolve) => {
			releaseOperation = resolve;
		});
		const operation = first.runWhileHeld(() => operationGate);
		const closing = first.close();

		let concurrentError: unknown;
		try {
			await acquireNativeDurabilityNodeLease(directory);
		} catch (error) {
			concurrentError = error;
		}
		expect(concurrentError).to.be.instanceOf(
			NativeDurabilityLeaseUnavailableError,
		);

		releaseOperation();
		await operation;
		await closing;
		leases.delete(first);

		const reopened = await acquireNativeDurabilityNodeLease(directory);
		leases.add(reopened);
		expect(reopened.fence.epoch).to.equal(first.fence.epoch + 1n);
	});

	it("reacquires immediately with a higher fence after SIGKILL", async function () {
		const directory = await temporaryDirectory();
		const child = fork(workerPath, ["hold", directory], {
			stdio: ["ignore", "ignore", "pipe", "ipc"],
		});
		children.add(child);
		const held = await waitForWorkerMessage(child);
		if (held.event === "error") {
			throw new Error(
				`Lease worker failed (${held.code ?? held.name ?? "unknown"}): ${held.message}`,
			);
		}

		let concurrentError: unknown;
		try {
			await acquireNativeDurabilityNodeLease(directory);
		} catch (error) {
			concurrentError = error;
		}
		expect(concurrentError).to.be.instanceOf(
			NativeDurabilityLeaseUnavailableError,
		);

		const exited = once(child, "exit");
		expect(child.kill("SIGKILL")).to.equal(true);
		await exited;
		children.delete(child);

		const recovered = await acquireNativeDurabilityNodeLease(directory);
		leases.add(recovered);
		expect(recovered.fence.epoch).to.equal(BigInt(held.fence.epoch) + 1n);
		expect(recovered.fence.domainId).to.equal(held.fence.domainId);
		expect(recovered.fence.ownerId).to.not.equal(held.fence.ownerId);
	});
});
