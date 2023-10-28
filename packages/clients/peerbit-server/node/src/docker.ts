import { delay, waitFor } from "@peerbit/time";

export const installDocker = async () => {
	const { exec } = await import("child_process");

	// check if docker is installed
	const dockerExist = async () => {
		try {
			const out = await new Promise((resolve, reject) => {
				exec("docker --version", (error, stdout, stderr) => {
					if (error || stderr) {
						reject();
					}
					resolve(stdout);
				});
			});
			return true;
		} catch (error) {
			return false;
		}
	};

	if (!(await dockerExist())) {
		await new Promise((resolve, reject) => {
			exec("sudo snap install docker", (error, stdout, stderr) => {
				if (error || stderr) {
					reject();
				}
				resolve(stdout);
			});
		});

		try {
			await waitFor(() => dockerExist(), {
				timeout: 30 * 1000,
				delayInterval: 1000
			});
		} catch (error) {
			throw new Error("Failed to install docker");
		}
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
							error.message
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
