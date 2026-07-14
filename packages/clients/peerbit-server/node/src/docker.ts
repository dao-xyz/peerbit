import { delay, waitFor } from "@peerbit/time";

export type ExecResult = { stdout: string; stderr: string };

export type DockerExecutor = (args: readonly string[]) => Promise<ExecResult>;

export type DockerContainerInspection = {
	Id?: string;
	Name?: string;
	RestartCount?: number;
	Config?: {
		Env?: string[];
		Image?: string;
		Labels?: Record<string, string> | null;
	};
	Mounts?: Array<{
		Destination?: string;
		Source?: string;
	}>;
	State?: {
		Health?: {
			Status?: string;
		};
		Restarting?: boolean;
		Running?: boolean;
	};
};

const execCommand = async (cmd: string): Promise<ExecResult> => {
	const { exec } = await import("child_process");
	return new Promise((resolve, reject) => {
		exec(cmd, (error, stdout, stderr) => {
			if (error) {
				(error as any).stdout = stdout;
				(error as any).stderr = stderr;
				reject(error);
				return;
			}
			resolve({ stdout, stderr });
		});
	});
};

const execDockerCommand: DockerExecutor = async (
	args: readonly string[],
): Promise<ExecResult> => {
	const { execFile } = await import("child_process");
	return new Promise((resolve, reject) => {
		execFile("docker", [...args], (error, stdout, stderr) => {
			if (error) {
				(error as any).stdout = stdout;
				(error as any).stderr = stderr;
				reject(error);
				return;
			}
			resolve({ stdout, stderr });
		});
	});
};

const commandExists = async (command: string): Promise<boolean> => {
	try {
		await execCommand(`command -v ${command}`);
		return true;
	} catch {
		return false;
	}
};

const getSudoPrefix = async (): Promise<string> => {
	const isRoot = typeof process.getuid === "function" && process.getuid() === 0;
	if (isRoot) {
		return "";
	}
	if (await commandExists("sudo")) {
		return "sudo ";
	}
	throw new Error(
		"Docker installation requires elevated privileges (sudo not found)",
	);
};

const dockerCliExists = async (): Promise<boolean> => {
	try {
		await execCommand("docker --version");
		return true;
	} catch {
		return false;
	}
};

const dockerDaemonAccessible = async (): Promise<boolean> => {
	try {
		await execCommand("docker info");
		return true;
	} catch (error: any) {
		const stderr: string = error?.stderr || "";
		if (
			stderr.includes("Got permission denied") ||
			stderr.toLowerCase().includes("permission denied")
		) {
			throw new Error(
				"Docker is installed but the current user cannot access the Docker daemon. Add the user to the 'docker' group or run with elevated privileges.",
			);
		}
		return false;
	}
};

const startDockerDaemon = async (sudoPrefix: string) => {
	if (await commandExists("snap")) {
		try {
			await execCommand(`${sudoPrefix}snap start docker`);
		} catch {}
	}

	if (await commandExists("systemctl")) {
		try {
			await execCommand(`${sudoPrefix}systemctl enable --now docker`);
			return;
		} catch {}
		try {
			await execCommand(`${sudoPrefix}systemctl start docker`);
			return;
		} catch {}
	}

	if (await commandExists("service")) {
		try {
			await execCommand(`${sudoPrefix}service docker start`);
		} catch {}
	}
};

const installDockerWithSnap = async (sudoPrefix: string) => {
	await execCommand(`${sudoPrefix}snap install docker`);
};

const installDockerWithApt = async (sudoPrefix: string) => {
	await execCommand(`${sudoPrefix}apt-get update`);
	await execCommand(
		`${sudoPrefix}DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io`,
	);
};

export const installDocker = async () => {
	const sudoPrefix = await getSudoPrefix();

	if (!(await dockerCliExists())) {
		let lastError: unknown;

		if (await commandExists("snap")) {
			try {
				await installDockerWithSnap(sudoPrefix);
			} catch (error) {
				lastError = error;
			}
		}

		if (!(await dockerCliExists()) && (await commandExists("apt-get"))) {
			try {
				await installDockerWithApt(sudoPrefix);
			} catch (error) {
				lastError = error;
			}
		}

		if (!(await dockerCliExists())) {
			const suffix =
				lastError instanceof Error
					? `: ${lastError.message}`
					: lastError
						? `: ${String(lastError)}`
						: "";
			throw new Error(
				`Failed to install docker (no supported installer succeeded)${suffix}`,
			);
		}
	}

	await startDockerDaemon(sudoPrefix);

	try {
		await waitFor(async () => dockerDaemonAccessible(), {
			timeout: 2 * 60 * 1000,
			delayInterval: 2000,
		});
	} catch (error: any) {
		throw new Error(
			`Docker is installed but not available: ${error?.message || "unknown error"}`,
		);
	}
};

const runDockerWithRetry = async (
	args: readonly string[],
	execute: DockerExecutor,
) => {
	try {
		return await execute(args);
	} catch {
		await delay(10000);
		return execute(args);
	}
};

const isMissingContainerError = (error: any): boolean => {
	const details = `${error?.stderr || ""}\n${error?.message || ""}`;
	return /no such (object|container)/i.test(details);
};

export const inspectDockerContainer = async (
	name: string,
	execute: DockerExecutor = execDockerCommand,
): Promise<DockerContainerInspection | undefined> => {
	try {
		const result = await execute(["container", "inspect", name]);
		const parsed = JSON.parse(result.stdout);
		if (!Array.isArray(parsed) || parsed.length !== 1) {
			throw new Error(`Unexpected Docker inspection result for ${name}`);
		}
		return parsed[0] as DockerContainerInspection;
	} catch (error: any) {
		if (isMissingContainerError(error)) {
			return undefined;
		}
		throw error;
	}
};

export const pullDockerImage = async (
	image: string,
	execute: DockerExecutor = execDockerCommand,
) => {
	try {
		await runDockerWithRetry(["pull", image], execute);
	} catch (error: any) {
		throw new Error(`Failed to pull Docker image ${image}: ${error?.message}`);
	}
};

const errorMessage = (error: any): string =>
	error?.message || error?.stderr || String(error);

export const DOCKER_REPLACEMENT_LABEL = "org.peerbit.replacement";
export const DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL =
	"org.peerbit.replacement.original-id";
export const DOCKER_REPLACEMENT_ORIGINAL_RUNNING_LABEL =
	"org.peerbit.replacement.original-running";

const getContainerReplacementLockAddress = async (name: string) => {
	const { createHash } = await import("crypto");
	const digest = createHash("sha256").update(name).digest("hex").slice(0, 32);
	if (process.platform === "linux") {
		// Linux abstract sockets have no filesystem entry and are released by the
		// kernel if this process exits, including on an unclean exit.
		return `\0peerbit-docker-replace-${digest}`;
	}
	if (process.platform === "win32") {
		return `\\\\.\\pipe\\peerbit-docker-replace-${digest}`;
	}
	const path = await import("path");
	return path.join("/tmp", `peerbit-docker-replace-${digest}.sock`);
};

const acquireContainerReplacementLock = async (name: string) => {
	if (process.platform !== "linux" && process.platform !== "win32") {
		const { createRequire } = await import("module");
		const properLockfile = createRequire(import.meta.url)(
			"proper-lockfile",
		) as {
			lock: (
				file: string,
				options: { realpath: boolean; stale: number; update: number },
			) => Promise<() => Promise<void>>;
		};
		try {
			return await properLockfile.lock(
				await getContainerReplacementLockAddress(name),
				{
					realpath: false,
					stale: 10_000,
					update: 2_000,
				},
			);
		} catch (error: any) {
			if (error?.code === "ELOCKED") {
				throw new Error(
					`Another Peerbit configuration is already replacing Docker container ${name}`,
				);
			}
			throw error;
		}
	}

	const { createServer } = await import("net");
	const address = await getContainerReplacementLockAddress(name);
	const server = createServer();
	try {
		await new Promise<void>((resolve, reject) => {
			const onError = (error: Error) => {
				server.off("listening", onListening);
				reject(error);
			};
			const onListening = () => {
				server.off("error", onError);
				resolve();
			};
			server.once("error", onError);
			server.once("listening", onListening);
			server.listen(address);
		});
	} catch (error: any) {
		if (error?.code === "EADDRINUSE") {
			throw new Error(
				`Another Peerbit configuration is already replacing Docker container ${name}`,
			);
		}
		throw error;
	}

	return () =>
		new Promise<void>((resolve) => {
			server.close(() => resolve());
		});
};

/**
 * Briefly stop a known running container while holding the same lock used by
 * replacement transactions. The immutable container ID is checked before and
 * after both mutations so an unrelated or concurrently replaced container is
 * never stopped or started by name.
 */
export const withQuiescedDockerContainer = async <T>(
	name: string,
	validate: (inspection: DockerContainerInspection) => boolean,
	action: () => Promise<T>,
	options: { execute?: DockerExecutor } = {},
): Promise<T> => {
	const execute = options.execute || execDockerCommand;
	const releaseLock = await acquireContainerReplacementLock(name);
	try {
		const initial = await inspectDockerContainer(name, execute);
		if (
			!initial?.Id ||
			initial.Name !== `/${name}` ||
			initial.State?.Running !== true ||
			!validate(initial)
		) {
			throw new Error(
				`Port owner is not the running recognized Peerbit-managed Docker container ${name}; refusing to stop anything`,
			);
		}
		const id = initial.Id;
		let shouldRestart = false;
		let result: T | undefined;
		let operationError: unknown;
		try {
			try {
				await execute(["container", "stop", id]);
				shouldRestart = true;
			} catch (error) {
				const afterFailedStop = await inspectDockerContainer(id, execute);
				if (
					afterFailedStop?.Id !== id ||
					afterFailedStop.State?.Running !== false
				) {
					throw error;
				}
				shouldRestart = true;
			}
			const stoppedById = await inspectDockerContainer(id, execute);
			const stoppedByName = await inspectDockerContainer(name, execute);
			if (
				stoppedById?.Id !== id ||
				stoppedById.State?.Running !== false ||
				stoppedByName?.Id !== id ||
				stoppedByName.Name !== `/${name}` ||
				!validate(stoppedByName)
			) {
				throw new Error(
					`Docker container ${name} could not be verified after stopping`,
				);
			}
			result = await action();
		} catch (error) {
			operationError = error;
		}

		let restartError: unknown;
		if (shouldRestart) {
			try {
				const beforeRestartById = await inspectDockerContainer(id, execute);
				const beforeRestartByName = await inspectDockerContainer(name, execute);
				if (
					beforeRestartById?.Id !== id ||
					beforeRestartByName?.Id !== id ||
					beforeRestartByName.Name !== `/${name}` ||
					!validate(beforeRestartByName)
				) {
					throw new Error(
						`Docker container ${name} changed while quiesced; refusing an unsafe restart`,
					);
				}
				try {
					await execute(["container", "start", id]);
				} catch (error) {
					const afterFailedStart = await inspectDockerContainer(id, execute);
					if (
						afterFailedStart?.Id !== id ||
						afterFailedStart.State?.Running !== true
					) {
						throw error;
					}
				}
				const restarted = await inspectDockerContainer(id, execute);
				const restartedByName = await inspectDockerContainer(name, execute);
				if (
					restarted?.Id !== id ||
					restarted.State?.Running !== true ||
					restartedByName?.Id !== id ||
					restartedByName.Name !== `/${name}` ||
					!validate(restartedByName)
				) {
					throw new Error(
						`Docker container ${name} did not restart after DNS verification`,
					);
				}
			} catch (error) {
				restartError = error;
			}
		}
		if (operationError && restartError) {
			const error = new Error(
				`DNS verification failed and Docker container ${name} could not be safely restarted: ${errorMessage(restartError)}`,
			);
			(error as any).cause = operationError;
			throw error;
		}
		if (restartError) throw restartError;
		if (operationError) throw operationError;
		return result as T;
	} finally {
		await releaseLock();
	}
};

const inspectOwnedReplacement = async (
	reference: string,
	replacementToken: string,
	execute: DockerExecutor,
) => {
	const inspection = await inspectDockerContainer(reference, execute);
	if (
		inspection?.Config?.Labels?.[DOCKER_REPLACEMENT_LABEL] !== replacementToken
	) {
		return undefined;
	}
	if (!inspection.Id) {
		throw new Error(
			`Owned Docker replacement ${reference} has no inspection ID`,
		);
	}
	return inspection;
};

const isCanonicalReplacement = (
	inspection: DockerContainerInspection | undefined,
	replacementId: string,
	name: string,
) => inspection?.Id === replacementId && inspection.Name === `/${name}`;

const removeOwnedReplacement = async (
	name: string,
	replacementId: string | undefined,
	replacementToken: string,
	execute: DockerExecutor,
) => {
	const owned = await inspectOwnedReplacement(
		replacementId || name,
		replacementToken,
		execute,
	);
	if (owned?.Id) {
		await execute(["container", "rm", "--force", owned.Id]);
	}
};

const restoreDockerContainer = async (
	name: string,
	existing: DockerContainerInspection,
	wasRunning: boolean,
	execute: DockerExecutor,
) => {
	if (!existing.Id) {
		throw new Error(`Previous Docker container ${name} has no inspection ID`);
	}

	// Restore the state while the state-encoded backup name still exists. If the
	// process exits between these operations, the next attempt can still infer the
	// intended state from that name. Rename to the canonical name last.
	let target = await inspectDockerContainer(existing.Id, execute);
	if (!target || target.Id !== existing.Id) {
		throw new Error(`Previous Docker container ${name} could not be inspected`);
	}
	if (wasRunning && target.State?.Running !== true) {
		await execute(["container", "start", existing.Id]);
	} else if (!wasRunning && target.State?.Running === true) {
		await execute(["container", "stop", existing.Id]);
	}
	target = await inspectDockerContainer(existing.Id, execute);
	if (
		target?.Id !== existing.Id ||
		(wasRunning && target.State?.Running !== true) ||
		(!wasRunning && target.State?.Running === true)
	) {
		throw new Error(
			`Previous Docker container ${name} did not return to its original state`,
		);
	}

	const canonical = await inspectDockerContainer(name, execute);
	if (canonical?.Id !== existing.Id) {
		await execute(["container", "rename", existing.Id, name]);
	}

	target = await inspectDockerContainer(name, execute);
	if (
		target?.Id !== existing.Id ||
		(wasRunning && target.State?.Running !== true) ||
		(!wasRunning && target.State?.Running === true)
	) {
		throw new Error(
			`Previous Docker container ${name} did not return to its original state`,
		);
	}
};

type DockerRunArguments =
	| readonly string[]
	| ((
			existing: DockerContainerInspection | undefined,
	  ) => Promise<readonly string[]> | readonly string[]);

export type InterruptedDockerReplacementResolution =
	| "keep-current"
	| "restore-backup";

const RESERVED_REPLACEMENT_LABELS = new Set([
	DOCKER_REPLACEMENT_LABEL,
	DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL,
	DOCKER_REPLACEMENT_ORIGINAL_RUNNING_LABEL,
]);

const assertNoReservedReplacementLabels = (args: readonly string[]) => {
	for (let index = 0; index < args.length; index++) {
		let label: string | undefined;
		if (
			args[index] === "--label-file" ||
			args[index].startsWith("--label-file=")
		) {
			throw new Error(
				"Docker replacement arguments cannot use --label-file because it could override reserved transaction labels",
			);
		}
		if (args[index] === "--label" || args[index] === "-l") {
			label = args[index + 1];
			index += 1;
		} else if (args[index].startsWith("--label=")) {
			label = args[index].slice("--label=".length);
		} else if (args[index].startsWith("-l=")) {
			label = args[index].slice("-l=".length);
		} else if (args[index].startsWith("-l") && args[index].length > 2) {
			label = args[index].slice(2);
		}
		if (!label) continue;
		const separator = label.indexOf("=");
		const key = separator === -1 ? label : label.slice(0, separator);
		if (RESERVED_REPLACEMENT_LABELS.has(key)) {
			throw new Error(
				`Docker replacement arguments cannot override reserved label ${key}`,
			);
		}
	}
};

const assertDockerRunName = (args: readonly string[], expectedName: string) => {
	const nameOptions = args.filter(
		(argument) => argument === "--name" || argument.startsWith("--name="),
	);
	const hasCanonicalPrefix =
		(args[1] === "--name" && args[2] === expectedName) ||
		args[1] === `--name=${expectedName}`;
	if (!hasCanonicalPrefix || nameOptions.length !== 1) {
		throw new Error(
			`Docker replacement arguments must place exactly one canonical --name ${expectedName} immediately after 'run'`,
		);
	}
};

/**
 * Replace a named container with rollback. An existing container is renamed and
 * stopped, not deleted, until the replacement has remained stable and the
 * caller's commit callback has completed. A cross-working-directory process
 * lock serializes cooperative Peerbit replacements of the same Docker name.
 */
export const replaceDockerContainer = async (
	name: string,
	runArgs: DockerRunArguments,
	options: {
		backupName?: string;
		execute?: DockerExecutor;
		expectedContainerId?: string | null;
		onCommit?: () => Promise<void> | void;
		onCommitted?: () => Promise<void> | void;
		/**
		 * Runs under the replacement lock after rollback attempts, and only after
		 * all current, backup, and known immutable container references have been
		 * inspected successfully. Callers may use the complete remaining set to
		 * decide whether transaction-scoped resources are safe to discard.
		 */
		onRollbackInspected?: (
			remainingContainers: readonly DockerContainerInspection[],
		) => Promise<void> | void;
		onStarted?: () => Promise<void> | void;
		resolveInterruptedPair?: (
			current: DockerContainerInspection,
			backup: DockerContainerInspection,
		) =>
			| Promise<InterruptedDockerReplacementResolution | undefined>
			| InterruptedDockerReplacementResolution
			| undefined;
		replacementToken?: string;
		retryDelayMs?: number;
		startupProbeAttempts?: number;
		startupProbeIntervalMs?: number;
		validateExisting?: (inspection: DockerContainerInspection) => boolean;
	} = {},
) => {
	const execute = options.execute || execDockerCommand;
	const { randomUUID } = await import("crypto");
	const replacementToken = options.replacementToken || randomUUID();
	const releaseLock = await acquireContainerReplacementLock(name);
	const backupNameBase = options.backupName || `${name}-peerbit-backup`;
	const runningBackupName = `${backupNameBase}-running`;
	const stoppedBackupName = `${backupNameBase}-stopped`;
	let backupName = backupNameBase;
	let existing: DockerContainerInspection | undefined;
	let wasRunning = false;
	let replacementId: string | undefined;
	let rollbackExisting: DockerContainerInspection | undefined;
	let rollbackWasRunning = false;
	let transactionStarted = false;
	let committed = false;
	try {
		existing = await inspectDockerContainer(name, execute);
		const interruptedBackups = (
			await Promise.all(
				[runningBackupName, stoppedBackupName, backupNameBase].map(
					async (candidateName) => ({
						inspection: await inspectDockerContainer(candidateName, execute),
						name: candidateName,
					}),
				),
			)
		).filter(
			(
				candidate,
			): candidate is {
				inspection: DockerContainerInspection;
				name: string;
			} => candidate.inspection !== undefined,
		);
		if (interruptedBackups.length > 1) {
			throw new Error(
				`Multiple Docker backups exist for ${name}; refusing ambiguous recovery`,
			);
		}
		const interrupted = interruptedBackups[0];
		const interruptedBackup = interrupted?.inspection;
		if (interruptedBackup) {
			backupName = interrupted.name;
			const backupWasRunning =
				backupName === runningBackupName
					? true
					: backupName === stoppedBackupName
						? false
						: undefined;
			if (
				!options.validateExisting?.(interruptedBackup) ||
				!interruptedBackup.Id
			) {
				throw new Error(
					`Docker backup ${backupName} is not a recognized recoverable container`,
				);
			}
			if (existing) {
				if (!options.validateExisting?.(existing) || !existing.Id) {
					throw new Error(
						`Docker container ${name} is not a recognized recoverable replacement`,
					);
				}
				const labels = existing.Config?.Labels;
				const interruptedToken = labels?.[DOCKER_REPLACEMENT_LABEL];
				const originalId = labels?.[DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL];
				const originalRunning =
					labels?.[DOCKER_REPLACEMENT_ORIGINAL_RUNNING_LABEL];
				if (
					!interruptedToken ||
					originalId !== interruptedBackup.Id ||
					(originalRunning !== "true" && originalRunning !== "false") ||
					backupWasRunning === undefined ||
					(originalRunning === "true") !== backupWasRunning
				) {
					throw new Error(
						`Docker containers ${name} and ${backupName} do not form a recognized interrupted replacement; refusing to mutate them`,
					);
				}
				const resolution = await options.resolveInterruptedPair?.(
					existing,
					interruptedBackup,
				);
				if (resolution === "keep-current") {
					await execute(["container", "rm", "--force", interruptedBackup.Id]);
				} else if (resolution === "restore-backup") {
					rollbackExisting = interruptedBackup;
					rollbackWasRunning = backupWasRunning;
					await removeOwnedReplacement(
						name,
						existing.Id,
						interruptedToken,
						execute,
					);
					await restoreDockerContainer(
						name,
						interruptedBackup,
						backupWasRunning,
						execute,
					);
					existing = await inspectDockerContainer(name, execute);
				} else {
					throw new Error(
						`Docker containers ${name} and ${backupName} require an unambiguous interrupted-replacement decision; refusing to mutate them`,
					);
				}
			} else {
				if (backupWasRunning === undefined) {
					throw new Error(
						`Docker backup ${backupName} predates crash-safe state metadata; resolve it manually before retrying`,
					);
				}
				rollbackExisting = interruptedBackup;
				rollbackWasRunning = backupWasRunning;
				await restoreDockerContainer(
					name,
					interruptedBackup,
					backupWasRunning,
					execute,
				);
				existing = await inspectDockerContainer(name, execute);
			}
		}
		wasRunning = existing?.State?.Running === true;
		backupName = wasRunning ? runningBackupName : stoppedBackupName;

		if (
			existing &&
			options.validateExisting &&
			!options.validateExisting(existing)
		) {
			throw new Error(
				`Docker container ${name} is not a recognized Peerbit-managed container; refusing to replace it`,
			);
		}
		if (existing && !existing.Id) {
			throw new Error(
				`Docker container ${name} has no inspection ID; refusing to replace it`,
			);
		}
		if (options.expectedContainerId !== undefined) {
			const actualId = existing?.Id || null;
			if (actualId !== options.expectedContainerId) {
				throw new Error(
					`Docker container ${name} changed during configuration; refusing to replace it`,
				);
			}
		}

		const baseRunArgs =
			typeof runArgs === "function" ? await runArgs(existing) : runArgs;
		if (baseRunArgs[0] !== "run") {
			throw new Error("Docker replacement arguments must start with 'run'");
		}
		assertNoReservedReplacementLabels(baseRunArgs);
		assertDockerRunName(baseRunArgs, name);
		const labeledRunArgs = [
			"run",
			"--label",
			`${DOCKER_REPLACEMENT_LABEL}=${replacementToken}`,
			"--label",
			`${DOCKER_REPLACEMENT_ORIGINAL_ID_LABEL}=${existing?.Id || "none"}`,
			"--label",
			`${DOCKER_REPLACEMENT_ORIGINAL_RUNNING_LABEL}=${wasRunning}`,
			...baseRunArgs.slice(1),
		];
		const startupProbeAttempts = options.startupProbeAttempts ?? 4;
		const startupProbeIntervalMs = options.startupProbeIntervalMs ?? 1000;
		if (!Number.isInteger(startupProbeAttempts) || startupProbeAttempts < 1) {
			throw new Error(
				"Docker startup probe attempts must be a positive integer",
			);
		}
		if (startupProbeIntervalMs < 0) {
			throw new Error("Docker startup probe interval must not be negative");
		}

		const startReplacement = async () => {
			const result = await execute(labeledRunArgs);
			const returnedId = result.stdout.trim();
			if (returnedId) replacementId = returnedId;
			const started = await inspectOwnedReplacement(
				returnedId || name,
				replacementToken,
				execute,
			);
			if (!started?.Id) {
				throw new Error(
					`Docker container ${name} did not return a verifiable replacement ID`,
				);
			}
			replacementId = started.Id;
			if (!isCanonicalReplacement(started, replacementId, name)) {
				throw new Error(
					`Docker replacement ${replacementId} did not use canonical name ${name}`,
				);
			}

			for (let attempt = 0; attempt < startupProbeAttempts; attempt++) {
				if (attempt > 0 && startupProbeIntervalMs > 0) {
					await delay(startupProbeIntervalMs);
				}
				const probe = await inspectOwnedReplacement(
					replacementId,
					replacementToken,
					execute,
				);
				if (
					!isCanonicalReplacement(probe, replacementId, name) ||
					probe?.State?.Running !== true ||
					probe.State.Restarting === true ||
					(probe.RestartCount ?? 0) > 0 ||
					probe.State.Health?.Status === "unhealthy"
				) {
					throw new Error(
						`Docker container ${name} did not remain stably running`,
					);
				}
			}
		};

		if (existing) {
			rollbackExisting = existing;
			rollbackWasRunning = wasRunning;
			transactionStarted = true;
			await execute(["container", "rename", existing.Id!, backupName]);
			if (wasRunning) {
				await execute(["container", "stop", existing.Id!]);
			}
		} else {
			transactionStarted = true;
		}

		try {
			await startReplacement();
		} catch {
			await removeOwnedReplacement(
				name,
				replacementId,
				replacementToken,
				execute,
			);
			replacementId = undefined;
			if ((options.retryDelayMs ?? 10000) > 0) {
				await delay(options.retryDelayMs ?? 10000);
			}
			await startReplacement();
		}
		await options.onStarted?.();
		const finalProbe = replacementId
			? await inspectOwnedReplacement(replacementId, replacementToken, execute)
			: undefined;
		if (
			!finalProbe ||
			!replacementId ||
			!isCanonicalReplacement(finalProbe, replacementId, name) ||
			finalProbe.State?.Running !== true ||
			finalProbe.State.Restarting === true ||
			(finalProbe.RestartCount ?? 0) > 0 ||
			finalProbe.State.Health?.Status === "unhealthy"
		) {
			throw new Error(
				`Docker container ${name} did not remain stably running through activation`,
			);
		}
		await options.onCommit?.();
		committed = true;

		if (existing?.Id) {
			try {
				await execute(["container", "rm", "--force", existing.Id]);
			} catch (error: any) {
				throw new Error(
					`Docker container ${name} started, but backup ${backupName} could not be removed: ${errorMessage(error)}`,
				);
			}
		}
		await options.onCommitted?.();
	} catch (error: any) {
		if (committed) {
			throw error;
		}
		const rollbackFailures: string[] = [];
		try {
			if (transactionStarted) {
				await removeOwnedReplacement(
					name,
					replacementId,
					replacementToken,
					execute,
				);
			}
		} catch (rollbackError) {
			rollbackFailures.push(
				`replacement cleanup failed: ${errorMessage(rollbackError)}`,
			);
		}
		try {
			if (rollbackExisting) {
				await restoreDockerContainer(
					name,
					rollbackExisting,
					rollbackWasRunning,
					execute,
				);
			}
		} catch (rollbackError) {
			rollbackFailures.push(
				`previous-container restore failed: ${errorMessage(rollbackError)}`,
			);
		}
		if (options.onRollbackInspected) {
			try {
				const references = new Set(
					[
						name,
						backupNameBase,
						runningBackupName,
						stoppedBackupName,
						backupName,
						existing?.Id,
						rollbackExisting?.Id,
						replacementId,
					].filter((reference): reference is string => Boolean(reference)),
				);
				const inspected = await Promise.all(
					[...references].map((reference) =>
						inspectDockerContainer(reference, execute),
					),
				);
				const seen = new Set<string>();
				const remainingContainers = inspected.filter(
					(inspection): inspection is DockerContainerInspection => {
						if (!inspection) return false;
						const identity = inspection.Id || inspection.Name;
						if (identity && seen.has(identity)) return false;
						if (identity) seen.add(identity);
						return true;
					},
				);
				await options.onRollbackInspected(remainingContainers);
			} catch (rollbackError) {
				rollbackFailures.push(
					`rollback inspection callback failed: ${errorMessage(rollbackError)}`,
				);
			}
		}
		const rollbackDetails = rollbackFailures.length
			? `; rollback also failed: ${rollbackFailures.join("; ")}`
			: rollbackExisting
				? "; previous container restored"
				: "";
		throw new Error(
			`Failed to replace Docker container ${name}: ${errorMessage(error)}${rollbackDetails}`,
		);
	} finally {
		await releaseLock();
	}
};
