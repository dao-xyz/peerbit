import { expect } from "chai";
import { type ChildProcess, spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

type WireFixture = {
	descriptor: string;
	authority: string;
	alice: string;
	bob: string;
	activeDigest: string;
	childEntries: string[];
	canonicalChildDigests: string[];
	roles: {
		authority: number;
		alice: number;
		bob: number;
	};
};

type WriterMessage = {
	event: "acknowledged-active" | "fenced-fork-prefix";
	fixture: WireFixture;
};

type ActiveRecoveryMessage = {
	event: "reopened-active";
	state: string;
	sequence?: string;
	digest?: string;
	resolverCalls: number;
	generationCount: number;
	authorityRoles: number;
	aliceRoles: number;
	bobRoles: number;
	authorityAdmin: boolean;
	aliceReader: boolean;
	aliceWriter: boolean;
	bobWriter: boolean;
};

type ForkRecoveryMessage = {
	event: "reopened-fork-prefix";
	state: string;
	commonParentSequence?: string;
	canonicalDigests?: string[];
	resolverCalls: number;
	generationCountBeforeReplay: number;
	replayStatuses: string[];
	generationCountAfterReplay: number;
	finalState: string;
	finalCanonicalDigests?: string[];
};

const waitForMessage = <T extends { event: string }>(
	child: ChildProcess,
	step: string,
	timeoutMs = 30_000,
): Promise<T> =>
	new Promise<T>((resolve, reject) => {
		let stdout = "";
		let stderr = "";
		let timer: ReturnType<typeof setTimeout>;
		const cleanup = (): void => {
			clearTimeout(timer);
			child.stdout?.off("data", onStdout);
			child.stderr?.off("data", onStderr);
			child.off("error", onError);
			child.off("exit", onExit);
		};
		const succeed = (message: T): void => {
			cleanup();
			resolve(message);
		};
		const fail = (error: Error): void => {
			cleanup();
			reject(error);
		};
		const onStdout = (chunk: Buffer): void => {
			stdout += chunk.toString();
			const lines = stdout.split("\n");
			stdout = lines.pop() ?? "";
			for (const line of lines) {
				if (!line.startsWith("{")) continue;
				try {
					succeed(JSON.parse(line) as T);
					return;
				} catch {
					// Ignore non-protocol output and continue reading.
				}
			}
		};
		const onStderr = (chunk: Buffer): void => {
			stderr += chunk.toString();
		};
		const onError = (error: Error): void => {
			fail(
				new Error(`Worker failed to start before ${step}: ${error.message}`, {
					cause: error,
				}),
			);
		};
		const onExit = (
			code: number | null,
			signal: NodeJS.Signals | null,
		): void => {
			fail(
				new Error(
					`Worker exited before ${step} (code=${String(code)}, signal=${String(signal)}): ${stderr}`,
				),
			);
		};

		timer = setTimeout(
			() => fail(new Error(`Timed out waiting for ${step}: ${stderr}`)),
			timeoutMs,
		);
		child.stdout?.on("data", onStdout);
		child.stderr?.on("data", onStderr);
		child.once("error", onError);
		child.once("exit", onExit);
	});

const waitForExit = (
	child: ChildProcess,
	step: string,
	timeoutMs = 30_000,
): Promise<[number | null, NodeJS.Signals | null]> =>
	new Promise((resolve, reject) => {
		let timer: ReturnType<typeof setTimeout>;
		const cleanup = (): void => {
			clearTimeout(timer);
			child.off("error", onError);
			child.off("exit", onExit);
		};
		const onError = (error: Error): void => {
			cleanup();
			reject(
				new Error(`${step} emitted an error: ${error.message}`, {
					cause: error,
				}),
			);
		};
		const onExit = (
			code: number | null,
			signal: NodeJS.Signals | null,
		): void => {
			cleanup();
			resolve([code, signal]);
		};

		timer = setTimeout(() => {
			cleanup();
			reject(new Error(`Timed out waiting for ${step}`));
		}, timeoutMs);
		child.once("error", onError);
		child.once("exit", onExit);
	});

const killWithSigkill = async (
	child: ChildProcess,
	label: string,
): Promise<void> => {
	if (child.exitCode !== null || child.signalCode !== null) {
		throw new Error(`${label} exited before SIGKILL could be sent`);
	}
	if (!child.kill("SIGKILL")) {
		throw new Error(`${label} rejected SIGKILL signaling`);
	}
	const [code, signal] = await waitForExit(child, `${label} SIGKILL exit`);
	if (code !== null || signal !== "SIGKILL") {
		throw new Error(
			`${label} did not exit from SIGKILL (code=${String(code)}, signal=${String(signal)})`,
		);
	}
};

const terminate = async (child: ChildProcess | undefined): Promise<void> => {
	if (
		child === undefined ||
		child.exitCode !== null ||
		child.signalCode !== null
	) {
		return;
	}
	await killWithSigkill(child, "cleanup worker");
};

const runScenario = async <T extends { event: string }>(properties: {
	writeMode: "write-active" | "write-fork-prefix";
	writeEvent: WriterMessage["event"];
	readMode: "read-active" | "read-fork-prefix";
	readEvent: T["event"];
}): Promise<{ fixture: WireFixture; reopened: T }> => {
	const directory = await fs.mkdtemp(
		path.join(os.tmpdir(), "peerbit-v2-policy-anchor-hard-kill-"),
	);
	const workerPath = path.join(
		process.cwd(),
		"test/v2-policy-anchor-hard-kill-worker.mjs",
	);
	let writer: ChildProcess | undefined;
	let reader: ChildProcess | undefined;
	let outcome: { fixture: WireFixture; reopened: T } | undefined;
	let originalFailure: unknown;
	try {
		writer = spawn(
			process.execPath,
			[workerPath, properties.writeMode, directory],
			{
				stdio: ["ignore", "pipe", "pipe"],
			},
		);
		const written = await waitForMessage<WriterMessage>(
			writer,
			properties.writeEvent,
		);
		expect(written.event).to.equal(properties.writeEvent);
		await killWithSigkill(writer, "writer");

		reader = spawn(
			process.execPath,
			[
				workerPath,
				properties.readMode,
				directory,
				JSON.stringify(written.fixture),
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = waitForExit(reader, "reopen worker exit");
		void readerExit.catch((): void => undefined);
		const reopened = await waitForMessage<T>(reader, properties.readEvent);
		expect(reopened.event).to.equal(properties.readEvent);
		const [readerCode, readerSignal] = await readerExit;
		expect(readerCode).to.equal(0);
		expect(readerSignal).to.equal(null);
		outcome = { fixture: written.fixture, reopened };
	} catch (error) {
		originalFailure = error;
	}

	const cleanupErrors: unknown[] = [];
	for (const cleanup of [
		() => terminate(writer),
		() => terminate(reader),
		() => fs.rm(directory, { recursive: true, force: true }),
	]) {
		try {
			await cleanup();
		} catch (error) {
			cleanupErrors.push(error);
		}
	}
	if (originalFailure !== undefined) {
		if (
			originalFailure instanceof Error &&
			originalFailure.cause === undefined &&
			cleanupErrors.length !== 0
		) {
			originalFailure.cause = new AggregateError(
				cleanupErrors,
				"Hard-kill scenario cleanup also failed",
			);
		}
		throw originalFailure;
	}
	if (cleanupErrors.length !== 0) {
		throw new AggregateError(
			cleanupErrors,
			"Hard-kill scenario cleanup failed",
		);
	}
	if (outcome === undefined)
		throw new Error("Hard-kill scenario returned no result");
	return outcome;
};

describe("TrustedNetwork v2 durable policy anchor hard-kill recovery", function () {
	this.timeout(120_000);

	it("reopens an acknowledged ACTIVE generation after SIGKILL", async function () {
		if (process.platform === "win32") this.skip();
		const { fixture, reopened } = await runScenario<ActiveRecoveryMessage>({
			writeMode: "write-active",
			writeEvent: "acknowledged-active",
			readMode: "read-active",
			readEvent: "reopened-active",
		});

		expect(reopened).to.deep.equal({
			event: "reopened-active",
			state: "ACTIVE",
			sequence: "1",
			digest: fixture.activeDigest,
			resolverCalls: 0,
			generationCount: 2,
			authorityRoles: fixture.roles.authority,
			aliceRoles: fixture.roles.alice,
			bobRoles: fixture.roles.bob,
			authorityAdmin: true,
			aliceReader: true,
			aliceWriter: false,
			bobWriter: true,
		});
	});

	it("reopens and extends a fully fenced FORKED prefix after SIGKILL", async function () {
		if (process.platform === "win32") this.skip();
		const { fixture, reopened } = await runScenario<ForkRecoveryMessage>({
			writeMode: "write-fork-prefix",
			writeEvent: "fenced-fork-prefix",
			readMode: "read-fork-prefix",
			readEvent: "reopened-fork-prefix",
		});

		expect(reopened).to.deep.equal({
			event: "reopened-fork-prefix",
			state: "FORKED",
			commonParentSequence: "0",
			canonicalDigests: fixture.canonicalChildDigests,
			resolverCalls: 0,
			generationCountBeforeReplay: 2,
			replayStatuses: ["halted", "halted", "halted", "halted"],
			generationCountAfterReplay: 3,
			finalState: "FORKED",
			finalCanonicalDigests: fixture.canonicalChildDigests,
		});
	});
});
