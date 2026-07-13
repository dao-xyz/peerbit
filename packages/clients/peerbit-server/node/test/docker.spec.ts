import { expect } from "chai";
import {
	DOCKER_REPLACEMENT_LABEL,
	DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL,
	DOCKER_REPLACEMENT_ORIGINAL_RUNNING_LABEL,
	type DockerExecutor,
	replaceDockerContainer,
	withQuiescedDockerContainer,
} from "../src/docker.js";

type FakeContainer = {
	healthStatus?: string;
	id: string;
	labels?: Record<string, string>;
	mounts?: Array<{ Destination?: string; Source?: string }>;
	restarting?: boolean;
	restartCount?: number;
	running: boolean;
};

const createDocker = (initial: Record<string, FakeContainer>) => {
	const containers = new Map(Object.entries(initial));
	const calls: string[][] = [];
	const newInspectionStates: Array<Partial<FakeContainer>> = [];
	let nextId = 1;
	let failingRuns = 0;
	let failedRunForeignReplacements = 0;
	let failingRemovalsAfterEffect = 0;
	let failingRemovalsBeforeEffect = 0;
	let failingStopsAfterEffect = 0;

	const find = (reference: string) => {
		const byName = containers.get(reference);
		if (byName) return { container: byName, name: reference };
		for (const [name, container] of containers) {
			if (container.id === reference) return { container, name };
		}
		return undefined;
	};

	const missing = (reference: string) => {
		const error: any = new Error("No such object");
		error.stderr = `Error: No such object: ${reference}`;
		return error;
	};

	const execute: DockerExecutor = async (args) => {
		const call = [...args];
		calls.push(call);
		if (call[0] === "container" && call[1] === "inspect") {
			const found = find(call[2]);
			if (!found) throw missing(call[2]);
			if (
				found.container.id.startsWith("new-") &&
				newInspectionStates.length > 0
			) {
				Object.assign(found.container, newInspectionStates.shift());
			}
			return {
				stdout: JSON.stringify([
					{
						Config: { Labels: found.container.labels || {} },
						Id: found.container.id,
						Mounts: found.container.mounts,
						Name: `/${found.name}`,
						RestartCount: found.container.restartCount || 0,
						State: {
							Health: found.container.healthStatus
								? { Status: found.container.healthStatus }
								: undefined,
							Restarting: found.container.restarting || false,
							Running: found.container.running,
						},
					},
				]),
				stderr: "",
			};
		}

		if (call[0] === "container" && call[1] === "rename") {
			const found = find(call[2]);
			if (!found) throw missing(call[2]);
			const destination = containers.get(call[3]);
			if (destination && destination !== found.container) {
				throw new Error(`Container name ${call[3]} is already in use`);
			}
			containers.delete(found.name);
			containers.set(call[3], found.container);
		} else if (call[0] === "container" && call[1] === "stop") {
			const found = find(call[2]);
			if (!found) throw missing(call[2]);
			found.container.running = false;
			if (failingStopsAfterEffect > 0) {
				failingStopsAfterEffect -= 1;
				throw new Error("simulated ambiguous docker stop failure");
			}
		} else if (call[0] === "container" && call[1] === "start") {
			const found = find(call[2]);
			if (!found) throw missing(call[2]);
			found.container.running = true;
		} else if (call[0] === "container" && call[1] === "rm") {
			const reference = call.at(-1)!;
			const found = find(reference);
			if (!found) throw missing(reference);
			if (failingRemovalsBeforeEffect > 0) {
				failingRemovalsBeforeEffect -= 1;
				throw new Error("simulated docker remove failure");
			}
			containers.delete(found.name);
			if (failingRemovalsAfterEffect > 0) {
				failingRemovalsAfterEffect -= 1;
				throw new Error("simulated ambiguous docker remove failure");
			}
		} else if (call[0] === "run") {
			const imageIndex = call.findIndex(
				(argument, index) =>
					index > 0 &&
					!argument.startsWith("-") &&
					call[index - 1] !== "--label" &&
					call[index - 1] !== "--name",
			);
			const nameIndex = call.findIndex(
				(argument, index) => argument === "--name" && index < imageIndex,
			);
			const id = `new-${nextId++}`;
			const name = nameIndex === -1 ? `anonymous-${id}` : call[nameIndex + 1];
			if (containers.has(name)) {
				throw new Error(`Container name ${name} is already in use`);
			}
			const labels: Record<string, string> = {};
			for (let index = 0; index < call.length; index++) {
				if (call[index] !== "--label") continue;
				const label = call[index + 1];
				const separator = label.indexOf("=");
				labels[label.slice(0, separator)] = label.slice(separator + 1);
			}
			const container = {
				id,
				labels,
				running: true,
			};
			containers.set(name, container);
			if (failedRunForeignReplacements > 0) {
				failedRunForeignReplacements -= 1;
				containers.set(name, {
					id: "foreign-after-failure",
					labels: {},
					running: true,
				});
				throw new Error("simulated run failure with foreign replacement");
			}
			if (failingRuns > 0) {
				failingRuns -= 1;
				throw new Error("simulated docker run failure after create");
			}
			return { stdout: `${container.id}\n`, stderr: "" };
		}
		return { stdout: "", stderr: "" };
	};

	return {
		calls,
		containers,
		execute,
		failNextRuns: (count: number) => {
			failingRuns = count;
		},
		failNextRemovalsAfterEffect: (count: number) => {
			failingRemovalsAfterEffect = count;
		},
		failNextRemovalsBeforeEffect: (count: number) => {
			failingRemovalsBeforeEffect = count;
		},
		failNextStopsAfterEffect: (count: number) => {
			failingStopsAfterEffect = count;
		},
		queueNewInspectionStates: (states: Array<Partial<FakeContainer>>) => {
			newInspectionStates.push(...states);
		},
		replaceFailedRunWithForeign: (count: number) => {
			failedRunForeignReplacements = count;
		},
	};
};

describe("withQuiescedDockerContainer", () => {
	const name = "nginx-certbot";

	it("stops and restarts only the immutable recognized container ID", async () => {
		const docker = createDocker({
			[name]: { id: "managed-id", labels: { managed: "yes" }, running: true },
		});
		await expect(
			withQuiescedDockerContainer(
				name,
				(inspection) => inspection.Config?.Labels?.managed === "yes",
				async () => {
					expect(docker.containers.get(name)?.running).to.be.false;
					throw new Error("verification failed");
				},
				{ execute: docker.execute },
			),
		).rejectedWith("verification failed");
		expect(docker.containers.get(name)?.running).to.be.true;
		expect(docker.calls).to.deep.include(["container", "stop", "managed-id"]);
		expect(docker.calls).to.deep.include(["container", "start", "managed-id"]);
	});

	it("refuses to stop an unrelated container", async () => {
		const docker = createDocker({
			[name]: { id: "foreign-id", running: true },
		});
		await expect(
			withQuiescedDockerContainer(
				name,
				() => false,
				async () => undefined,
				{ execute: docker.execute },
			),
		).rejectedWith("refusing to stop anything");
		expect(docker.containers.get(name)?.running).to.be.true;
		expect(
			docker.calls.some(
				(call) => call[0] === "container" && call[1] === "stop",
			),
		).to.be.false;
	});
});

describe("replaceDockerContainer", () => {
	const name = "nginx-certbot";
	const backupName = "peerbit-certbot-backup-test";
	const runningBackupName = `${backupName}-running`;
	const stoppedBackupName = `${backupName}-stopped`;
	const replacementToken = "replacement-test";
	const runArgs = ["run", "--name", name, "image@sha256:digest"];
	const options = (docker: ReturnType<typeof createDocker>) => ({
		backupName,
		execute: docker.execute,
		replacementToken,
		retryDelayMs: 0,
		startupProbeAttempts: 1,
		startupProbeIntervalMs: 0,
	});

	it("keeps the old container through commit and addresses it by ID", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		let committed = false;

		await replaceDockerContainer(name, runArgs, {
			...options(docker),
			expectedContainerId: "legacy",
			onCommitted: () => {
				committed = true;
				expect(docker.containers.has(runningBackupName)).to.be.false;
			},
			onStarted: () => {
				expect(docker.containers.get(runningBackupName)?.id).equal("legacy");
				expect(docker.containers.get(runningBackupName)?.running).to.be.false;
				expect(docker.containers.get(name)?.id).equal("new-1");
			},
		});

		expect(committed).to.be.true;
		expect(docker.containers.get(name)?.id).equal("new-1");
		expect(docker.calls).to.deep.include([
			"container",
			"rename",
			"legacy",
			runningBackupName,
		]);
		expect(docker.calls).to.deep.include(["container", "stop", "legacy"]);
		expect(docker.calls).to.deep.include([
			"container",
			"rm",
			"--force",
			"legacy",
		]);
		const run = docker.calls.find((call) => call[0] === "run")!;
		expect(run).to.include(`${DOCKER_REPLACEMENT_LABEL}=${replacementToken}`);
		expect(run).to.include(`${DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL}=legacy`);
		expect(run).to.include(`${DOCKER_REPLACEMENT_ORIGINAL_RUNNING_LABEL}=true`);
	});

	it("restores a running previous container when both starts fail", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		docker.failNextRuns(2);

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, options(docker));
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("previous container restored");
		expect(docker.containers.get(name)).deep.equal({
			id: "legacy",
			running: true,
		});
		expect(docker.containers.has(runningBackupName)).to.be.false;
	});

	it("retries when a detached container exits after an initially good probe", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		docker.queueNewInspectionStates([{}, {}, { running: false }]);

		await replaceDockerContainer(name, runArgs, {
			...options(docker),
			startupProbeAttempts: 2,
		});

		expect(docker.containers.get(name)?.id).equal("new-2");
		expect(docker.calls.filter((call) => call[0] === "run")).to.have.length(2);
	});

	it("retries a replacement that enters a restart loop", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		docker.queueNewInspectionStates([{}, { restartCount: 1 }]);

		await replaceDockerContainer(name, runArgs, options(docker));

		expect(docker.containers.get(name)?.id).equal("new-2");
		expect(docker.calls.filter((call) => call[0] === "run")).to.have.length(2);
	});

	it("rolls back when the atomic commit callback fails", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		let committed = false;
		let rollbackInspected = false;

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				onCommit: () => {
					throw new Error("activation failed");
				},
				onCommitted: () => {
					committed = true;
				},
				onRollbackInspected: () => {
					rollbackInspected = true;
				},
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("previous container restored");
		expect(committed).to.be.false;
		expect(rollbackInspected).to.be.true;
		expect(docker.containers.get(name)).deep.equal({
			id: "legacy",
			running: true,
		});
	});

	it("reports a replacement that survives a removal-before-effect rollback failure", async () => {
		const generationRoot = "/state/generations/new";
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		docker.queueNewInspectionStates([
			{
				mounts: [
					{
						Destination: "/etc/nginx/user_conf.d",
						Source: `${generationRoot}/nginx`,
					},
				],
			},
		]);
		docker.failNextRemovalsBeforeEffect(1);
		let remainingIds: string[] = [];

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				onRollbackInspected: (remainingContainers) => {
					remainingIds = remainingContainers.flatMap((container) =>
						container.Id ? [container.Id] : [],
					);
				},
				onStarted: () => {
					throw new Error("readiness failed");
				},
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("replacement cleanup failed");
		expect(error?.message).to.include("previous-container restore failed");
		expect(remainingIds).to.include("new-1");
		expect(docker.containers.get(name)?.mounts?.[0]?.Source).equal(
			`${generationRoot}/nginx`,
		);
	});

	it("restores the previous container when it becomes unstable during activation", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		let commitCalled = false;
		let committed = false;

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				onCommit: () => {
					commitCalled = true;
				},
				onCommitted: () => {
					committed = true;
				},
				onStarted: () => {
					const replacement = docker.containers.get(name)!;
					replacement.restartCount = 1;
					replacement.restarting = true;
				},
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include(
			"did not remain stably running through activation",
		);
		expect(error?.message).to.include("previous container restored");
		expect(commitCalled).to.be.false;
		expect(committed).to.be.false;
		expect(docker.containers.get(name)).deep.equal({
			id: "legacy",
			running: true,
		});
		expect(docker.containers.has(runningBackupName)).to.be.false;
	});

	it("refuses an unexpected initial container ID without mutating it", async () => {
		const docker = createDocker({
			[name]: { id: "unexpected", running: true },
		});

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				expectedContainerId: "expected",
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("changed during configuration");
		expect(docker.containers.get(name)?.id).equal("unexpected");
		expect(
			docker.calls.some((call) =>
				["rename", "rm", "start", "stop"].includes(call[1]),
			),
		).to.be.false;
	});

	it("restores an old container when stop takes effect but reports failure", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		docker.failNextStopsAfterEffect(1);

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, options(docker));
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("previous container restored");
		expect(docker.containers.get(name)).deep.equal({
			id: "legacy",
			running: true,
		});
		expect(docker.calls).to.deep.include(["container", "start", "legacy"]);
	});

	it("keeps an originally stopped container stopped after rollback", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: false },
		});

		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				onStarted: () => {
					throw new Error("activation failed");
				},
			});
		} catch {
			// Expected: the assertion below verifies rollback state.
		}

		expect(docker.containers.get(name)).deep.equal({
			id: "legacy",
			running: false,
		});
	});

	it("serializes same-name replacements while the commit callback is pending", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		let enter!: () => void;
		const entered = new Promise<void>((resolve) => (enter = resolve));
		let release!: () => void;
		const barrier = new Promise<void>((resolve) => (release = resolve));
		const first = replaceDockerContainer(name, runArgs, {
			...options(docker),
			onStarted: async () => {
				enter();
				await barrier;
			},
		});
		await entered;
		const callCount = docker.calls.length;

		let error: Error | undefined;
		let callsWhileLocked = 0;
		try {
			await replaceDockerContainer(name, runArgs, options(docker));
		} catch (caught) {
			error = caught as Error;
		} finally {
			callsWhileLocked = docker.calls.length;
			release();
		}
		await first;

		expect(error?.message).to.include("already replacing");
		expect(callsWhileLocked).equal(callCount);
	});

	it("does not remove a foreign container that appears after a failed run", async () => {
		const docker = createDocker({});
		docker.replaceFailedRunWithForeign(1);

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, options(docker));
		} catch (caught) {
			error = caught as Error;
		}

		expect(error).to.be.instanceOf(Error);
		expect(docker.containers.get(name)?.id).equal("foreign-after-failure");
		expect(
			docker.calls.some(
				(call) => call[1] === "rm" && call.at(-1) === "foreign-after-failure",
			),
		).to.be.false;
	});

	it("recovers an originally running interrupted backup before replacing it", async () => {
		const docker = createDocker({
			[runningBackupName]: { id: "legacy", running: false },
		});

		await replaceDockerContainer(name, runArgs, {
			...options(docker),
			validateExisting: () => true,
		});

		expect(docker.calls).to.deep.include([
			"container",
			"rename",
			"legacy",
			name,
		]);
		expect(docker.calls).to.deep.include(["container", "start", "legacy"]);
		expect(docker.containers.get(name)?.id).equal("new-1");
	});

	it("keeps an originally stopped interrupted backup stopped", async () => {
		const docker = createDocker({
			[stoppedBackupName]: { id: "legacy", running: false },
		});

		await replaceDockerContainer(name, runArgs, {
			...options(docker),
			validateExisting: () => true,
		});

		expect(
			docker.calls.some(
				(call) => call[1] === "start" && call.at(-1) === "legacy",
			),
		).to.be.false;
		expect(docker.containers.get(name)?.id).equal("new-1");
	});

	it("refuses an unsuffixed legacy backup without mutating it", async () => {
		const docker = createDocker({
			[backupName]: { id: "legacy", running: false },
		});

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				validateExisting: () => true,
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("predates crash-safe state metadata");
		expect(docker.containers.get(backupName)).deep.equal({
			id: "legacy",
			running: false,
		});
		expect(
			docker.calls.some((call) =>
				["rename", "rm", "start", "stop"].includes(call[1]),
			),
		).to.be.false;
	});

	const interruptedPair = (running = true) => ({
		[name]: {
			id: "replacement",
			labels: {
				[DOCKER_REPLACEMENT_LABEL]: "interrupted-token",
				[DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL]: "legacy",
				[DOCKER_REPLACEMENT_ORIGINAL_RUNNING_LABEL]: String(running),
			},
			running: true,
		},
		[running ? runningBackupName : stoppedBackupName]: {
			id: "legacy",
			running: false,
		},
	});

	it("restores a recognized pre-commit pair before replacing it", async () => {
		const docker = createDocker(interruptedPair());

		await replaceDockerContainer(name, runArgs, {
			...options(docker),
			resolveInterruptedPair: () => "restore-backup",
			validateExisting: () => true,
		});

		expect(docker.calls).to.deep.include([
			"container",
			"rm",
			"--force",
			"replacement",
		]);
		expect(docker.calls).to.deep.include(["container", "start", "legacy"]);
		expect(docker.containers.get(name)?.id).equal("new-1");
	});

	it("finishes a recognized committed pair before replacing it", async () => {
		const docker = createDocker(interruptedPair());

		await replaceDockerContainer(name, runArgs, {
			...options(docker),
			resolveInterruptedPair: () => "keep-current",
			validateExisting: () => true,
		});

		const removeBackup = docker.calls.findIndex(
			(call) => call[1] === "rm" && call.at(-1) === "legacy",
		);
		const renameCurrent = docker.calls.findIndex(
			(call) => call[1] === "rename" && call[2] === "replacement",
		);
		expect(removeBackup).to.be.greaterThan(-1);
		expect(renameCurrent).to.be.greaterThan(removeBackup);
		expect(docker.containers.get(name)?.id).equal("new-1");
	});

	it("leaves an ambiguous interrupted pair unchanged", async () => {
		const initial = interruptedPair();
		const docker = createDocker(initial);

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				validateExisting: () => true,
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("unambiguous");
		expect(docker.containers.get(name)).deep.equal(initial[name]);
		expect(docker.containers.get(runningBackupName)).deep.equal(
			initial[runningBackupName],
		);
		expect(
			docker.calls.some((call) =>
				["rename", "rm", "start", "stop"].includes(call[1]),
			),
		).to.be.false;
	});

	it("leaves a metadata-mismatched interrupted pair unchanged", async () => {
		const initial = interruptedPair();
		initial[name].labels![DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL] = "other";
		const docker = createDocker(initial);

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				resolveInterruptedPair: () => "restore-backup",
				validateExisting: () => true,
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("do not form a recognized");
		expect(docker.containers.get(name)).deep.equal(initial[name]);
		expect(
			docker.calls.some((call) =>
				["rename", "rm", "start", "stop"].includes(call[1]),
			),
		).to.be.false;
	});

	it("refuses multiple backup candidates without mutating them", async () => {
		const docker = createDocker({
			[runningBackupName]: { id: "running-backup", running: false },
			[stoppedBackupName]: { id: "stopped-backup", running: false },
		});

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				validateExisting: () => true,
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("Multiple Docker backups");
		expect(docker.containers.size).equal(2);
		expect(
			docker.calls.some((call) =>
				["rename", "rm", "start", "stop"].includes(call[1]),
			),
		).to.be.false;
	});

	it("does not stop current when committed-backup cleanup fails", async () => {
		const docker = createDocker(interruptedPair());
		docker.failNextRemovalsBeforeEffect(1);

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				resolveInterruptedPair: () => "keep-current",
				validateExisting: () => true,
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error).to.be.instanceOf(Error);
		expect(docker.containers.get(name)?.running).to.be.true;
		expect(docker.containers.get(runningBackupName)?.id).equal("legacy");
	});

	it("restores the backup when current removal takes effect but fails", async () => {
		const docker = createDocker(interruptedPair());
		docker.failNextRemovalsAfterEffect(1);

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, runArgs, {
				...options(docker),
				resolveInterruptedPair: () => "restore-backup",
				validateExisting: () => true,
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("previous container restored");
		expect(docker.containers.get(name)).deep.equal({
			id: "legacy",
			running: true,
		});
	});

	it("rejects reserved replacement labels before mutating", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});

		let error: Error | undefined;
		try {
			await replaceDockerContainer(
				name,
				[
					"run",
					"--label",
					`${DOCKER_REPLACEMENT_LABEL}=caller-value`,
					"--name",
					name,
					"image",
				],
				options(docker),
			);
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("reserved label");
		expect(docker.containers.get(name)?.running).to.be.true;
		expect(
			docker.calls.some((call) =>
				["rename", "rm", "start", "stop"].includes(call[1]),
			),
		).to.be.false;
	});

	it("rejects compact reserved-label shorthand before mutating", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});

		let error: Error | undefined;
		try {
			await replaceDockerContainer(
				name,
				[
					"run",
					`-l${DOCKER_REPLACEMENT_LABEL}=caller-value`,
					"--name",
					name,
					"image",
				],
				options(docker),
			);
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("reserved label");
		expect(docker.containers.get(name)?.running).to.be.true;
		expect(
			docker.calls.some((call) =>
				["rename", "rm", "start", "stop"].includes(call[1]),
			),
		).to.be.false;
	});

	it("requires the replacement to use the canonical name", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, ["run", "image"], options(docker));
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("canonical --name");
		expect(docker.containers.get(name)?.running).to.be.true;
		expect(
			docker.calls.some((call) =>
				["rename", "rm", "start", "stop"].includes(call[1]),
			),
		).to.be.false;
	});

	it("rejects --name after the image without mutating containers", async () => {
		const docker = createDocker({
			[name]: { id: "legacy", running: true },
		});
		let commitCalled = false;

		let error: Error | undefined;
		try {
			await replaceDockerContainer(name, ["run", "image", "--name", name], {
				...options(docker),
				onCommit: () => {
					commitCalled = true;
				},
			});
		} catch (caught) {
			error = caught as Error;
		}

		expect(error?.message).to.include("immediately after 'run'");
		expect(commitCalled).to.be.false;
		expect(docker.containers).deep.equal(
			new Map([[name, { id: "legacy", running: true }]]),
		);
	});

	it("records an absent original explicitly", async () => {
		const docker = createDocker({});

		await replaceDockerContainer(name, runArgs, options(docker));

		expect(docker.containers.get(name)?.labels).to.include({
			[DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL]: "none",
			[DOCKER_REPLACEMENT_ORIGINAL_RUNNING_LABEL]: "false",
		});
	});

	it("reclaims a stale non-Linux process lock after a hard exit", async function () {
		if (process.platform === "linux" || process.platform === "win32") {
			this.skip();
		}
		const { createHash } = await import("crypto");
		const fs = await import("fs");
		const path = await import("path");
		const { pathToFileURL } = await import("url");
		const { spawn } = await import("child_process");
		const moduleUrl = pathToFileURL(
			path.join(process.cwd(), "dist", "src", "docker.js"),
		).href;
		const child = spawn(
			process.execPath,
			[
				"--input-type=module",
				"--eval",
				`import { replaceDockerContainer } from ${JSON.stringify(moduleUrl)};
await replaceDockerContainer(${JSON.stringify(name)}, ["run"], {
  execute: async () => {
    process.stdout.write("locked\\n");
	setInterval(() => {}, 1_000);
    await new Promise(() => {});
    return { stdout: "", stderr: "" };
  }
});`,
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const digest = createHash("sha256").update(name).digest("hex").slice(0, 32);
		const lockPath = path.join(
			"/tmp",
			`peerbit-docker-replace-${digest}.sock.lock`,
		);
		try {
			await new Promise<void>((resolve, reject) => {
				const onData = (chunk: Buffer) => {
					if (chunk.toString().includes("locked")) resolve();
				};
				child.stdout.on("data", onData);
				child.once("error", reject);
				child.stderr.on("data", (chunk) => {
					const message = chunk.toString();
					if (message) reject(new Error(message));
				});
			});
			const exited = new Promise<void>((resolve) =>
				child.once("exit", () => resolve()),
			);
			child.kill("SIGKILL");
			await exited;
			expect(fs.existsSync(lockPath)).to.be.true;
			const stale = new Date(Date.now() - 20_000);
			fs.utimesSync(lockPath, stale, stale);

			const docker = createDocker({});
			await replaceDockerContainer(name, runArgs, options(docker));
			expect(docker.containers.get(name)?.id).equal("new-1");
		} finally {
			if (!child.killed) child.kill("SIGKILL");
			fs.rmSync(lockPath, { force: true, recursive: true });
		}
	});
});
