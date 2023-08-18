import { waitFor, waitForAsync } from "@peerbit/time";

const isNode = typeof window === undefined || typeof window === "undefined";

const validateEmail = (email) => {
	return String(email)
		.toLowerCase()
		.match(
			/^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
		);
};

const getConfigFileTemplate = async (): Promise<string> => {
	const url = await import("url");
	const __filename = url.fileURLToPath(import.meta.url);
	const fs = await import("fs");
	const path = await import("path");
	const file = fs.readFileSync(
		path.join(__filename, "../nginx-template.conf"),
		"utf-8"
	);
	return file;
};

const getNginxFolderPath = async () => {
	const { exec } = await import("child_process");
	const pwd: string = await new Promise((resolve, reject) => {
		exec("pwd", (error, stdout, stderr) => {
			if (error || stderr) {
				reject("Failed to get current directory");
			}
			resolve(stdout.trimEnd());
		});
	});

	const path = await import("path");
	const nginxConfigPath = path.join(pwd, "nginx");
	return nginxConfigPath;
};

const getNginxConfigPath = async (folder: string) => {
	const path = await import("path");
	return path.join(folder, "default.conf");
};

const createConfig = async (
	outputPath: string,
	domain: string
): Promise<{ domain: string }> => {
	if (!isNode) {
		throw new Error("Config can only be created with node");
	}

	let file = await getConfigFileTemplate();
	file = file.replaceAll("%DOMAIN%", domain);

	const fs = await import("fs");
	const path = await import("path");

	fs.mkdir(outputPath, { recursive: true }, (err) => {
		if (err) throw err;
	});

	await waitFor(() => fs.existsSync(outputPath));

	fs.writeFileSync(await getNginxConfigPath(outputPath), file);
	return { domain };
};

export const loadConfig = async () => {
	const configFilePath = await getNginxConfigPath(await getNginxFolderPath());
	const fs = await import("fs");
	if (!fs.existsSync(configFilePath)) {
		return undefined;
	}
	const file = fs.readFileSync(configFilePath, "utf-8");
	return file;
};

export const getDomainFromConfig = async (config: string) => {
	const pattern = "/etc/letsencrypt/live/(.*)/fullchain.pem";
	const match = config.match(pattern);
	const domain = match?.[1];
	return domain === "%DOMAIN%" ? undefined : domain;
};
const getUIPath = async (): Promise<string> => {
	const url = await import("url");
	const path = await import("path");
	const __filename = url.fileURLToPath(import.meta.url);
	const p1 = path.join(__filename, "../../", "ui");

	const fs = await import("fs");

	if (fs.existsSync(p1) && fs.lstatSync(p1).isDirectory()) {
		return p1; // build
	} else {
		const p2 = path.join(__filename, "../../", "lib/ui");
		if (fs.existsSync(p2) && fs.lstatSync(p2).isDirectory()) {
			return p2;
		}
		throw new Error("Failed to find UI path");
	}
};
export const getMyIp = async (): Promise<string> => {
	const { exec } = await import("child_process");
	const ipv4: string = await new Promise((resolve, reject) => {
		exec(
			"dig @resolver4.opendns.com myip.opendns.com +short",
			(error, stdout, stderr) => {
				if (error || stderr) {
					reject("DNS lookup failed");
				}
				resolve(stdout.trimEnd());
			}
		);
	});
	return ipv4;
};

export const createTestDomain = async () => {
	const { default: axios } = await import("axios");
	const domain: string = (
		await axios.post(
			"https://bfbbnhwpfj2ptcmurz6lit4xlu0vjajw.lambda-url.us-east-1.on.aws",
			await getMyIp(),
			{ headers: { "Content-Type": "application/json" } }
		)
	).data.domain;
	return domain;
};

/**
 *
 * @param email
 * @param nginxConfigPath
 * @param dockerProcessName
 * @returns domain
 */
export const startCertbot = async (
	domain: string,
	email: string,
	waitForUp = false,
	dockerProcessName = "nginx-certbot"
): Promise<void> => {
	if (!validateEmail(email)) {
		throw new Error("Email for SSL renenewal is invalid");
	}
	const { installDocker, startContainer } = await import("./docker.js");

	const nginxConfigPath = await getNginxFolderPath();
	await createConfig(nginxConfigPath, domain);

	await installDocker();

	// run
	const isTest = process.env.JEST_WORKER_ID !== undefined;

	const uiPath = await getUIPath();

	// copy ui from node_modules to home for permission reasons (volume will not work otherwise)
	const certbotDockerCommand = `cp -r ${uiPath} $(pwd)/ui && docker pull jonasal/nginx-certbot:latest && docker run -d --net=host \
    --env CERTBOT_EMAIL=${email} ${isTest ? "--env STAGING=1" : ""}\
    -v $(pwd)/nginx_secrets:/etc/letsencrypt \
    -v ${nginxConfigPath}:/etc/nginx/user_conf.d:ro \
    -v $(pwd)/ui:/usr/share/nginx/html:ro \
    --name ${dockerProcessName} jonasal/nginx-certbot:latest`;

	console.log("Starting Certbot");
	// try two times with some delay, because sometimes the docker daemon is not available immidatel
	await startContainer(
		certbotDockerCommand,
		"Failed to start certbot container"
	);

	console.log("Certbot started succesfully!");
	console.log("You domain is: ");
	console.log(domain);
	if (waitForUp) {
		const { default: axios } = await import("axios");

		console.log("Waiting for domain to be ready ...");
		await waitForAsync(
			async () => {
				try {
					const status = (await axios.get("https://" + domain)).status;
					return status >= 200 && status < 400;
				} catch (error) {
					return false;
				}
			},
			{ timeout: 5 * 60 * 10000, delayInterval: 5000 }
		);
		console.log("Domain is ready");
	} else {
		console.log(
			"The domain is not available immediately as it takes some time to request SSL certificate."
		);
	}
};
