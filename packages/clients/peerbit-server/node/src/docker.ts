import { delay, waitFor } from "@peerbit/time";

type ExecResult = { stdout: string; stderr: string };

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

export const startContainer = async (cmd: string, errorMessage?: string) => {
	const { exec } = await import("child_process");
	const startContainer = () =>
		new Promise((resolve, reject) => {
			exec(cmd, (error, stdout, stderr) => {
				if (error) {
					reject(
						(errorMessage || "Failed to start docker container: ") +
							error.message,
					);
				}
				resolve(stdout);
			});
		});
	try {
		await startContainer();
	} catch (error) {
		// try again no matter what?
		// or
		//  typeof error === "string" && error.indexOf("Cannot connect to the Docker daemon") != -1
		await delay(10000);
		await startContainer();
	}
};
