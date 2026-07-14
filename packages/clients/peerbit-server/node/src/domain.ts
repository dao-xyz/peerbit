/* eslint-disable no-console */
import { delay } from "@peerbit/time";
import type { DockerContainerInspection } from "./docker.js";

const isNode = typeof process !== "undefined" && process.versions?.node != null;

export const CERTBOT_IMAGE =
	"jonasal/nginx-certbot:6.2.0-nginx1.31.2@sha256:59c217749aa27effc5109249902e1f5cdbfdfa26721d7dbd4e7162380a07c2cc";
export const CERTBOT_MANAGED_LABEL = "org.peerbit.managed";
export const CERTBOT_MANAGED_LABEL_VALUE = "nginx-certbot";

const CERTBOT_MOUNT_DESTINATIONS = [
	"/etc/letsencrypt",
	"/etc/nginx/user_conf.d",
	"/usr/share/nginx/html",
] as const;
const DOMAIN_STATE_FOLDER = ".peerbit-domain";
const DOMAIN_GENERATIONS_FOLDER = "generations";
const ACTIVE_DOMAIN_GENERATION_FILE = "active";

const getConfigFileTemplate = async (): Promise<string> => {
	const url = await import("url");
	const filename = url.fileURLToPath(import.meta.url);
	const fs = await import("fs");
	const path = await import("path");
	const file = fs.readFileSync(
		path.join(filename, "../nginx-template.conf"),
		"utf-8",
	);
	return file;
};

const getLegacyNginxFolderPath = async () => {
	const path = await import("path");
	const nginxConfigPath = path.join(process.cwd(), "nginx");
	return nginxConfigPath;
};

const getNginxConfigPath = async (folder: string) => {
	const path = await import("path");
	return path.join(folder, "default.conf");
};

const createConfig = async (
	outputPath: string,
	domain: string,
): Promise<{ domain: string }> => {
	if (!isNode) {
		throw new Error("Config can only be created with node");
	}

	let file = await getConfigFileTemplate();
	file = file.replaceAll("%DOMAIN%", domain);

	const fs = await import("fs");
	fs.mkdirSync(outputPath, { recursive: true });
	fs.writeFileSync(await getNginxConfigPath(outputPath), file);
	return { domain };
};

export const loadConfig = async () => {
	const fs = await import("fs");
	const path = await import("path");
	const stateRoot = path.join(process.cwd(), DOMAIN_STATE_FOLDER);
	const activeGenerationPath = path.join(
		stateRoot,
		ACTIVE_DOMAIN_GENERATION_FILE,
	);
	if (fs.existsSync(activeGenerationPath)) {
		const generation = normalizeDomainGenerationName(
			fs.readFileSync(activeGenerationPath, "utf8").trim(),
		);
		const activeConfigPath = await getNginxConfigPath(
			path.join(stateRoot, DOMAIN_GENERATIONS_FOLDER, generation, "nginx"),
		);
		if (!fs.existsSync(activeConfigPath)) {
			throw new Error(
				`Active domain configuration ${generation} is missing ${activeConfigPath}; repair or remove ${activeGenerationPath}`,
			);
		}
		return fs.readFileSync(activeConfigPath, "utf-8");
	}

	const legacyConfigPath = await getNginxConfigPath(
		await getLegacyNginxFolderPath(),
	);
	return fs.existsSync(legacyConfigPath)
		? fs.readFileSync(legacyConfigPath, "utf-8")
		: undefined;
};

export const getDomainFromConfig = async (config: string) => {
	const pattern = "/etc/letsencrypt/live/(.*)/fullchain.pem";
	const match = config.match(pattern);
	const domain = match?.[1];
	return domain === "%DOMAIN%" ? undefined : domain;
};

export const normalizeDomain = (domain: string): string => {
	const normalized = domain.trim().toLowerCase().replace(/\.$/, "");
	const labels = normalized.split(".");
	const isValidLabel = (label: string) =>
		label.length > 0 &&
		label.length <= 63 &&
		/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(label);

	if (
		normalized.length > 253 ||
		labels.length < 2 ||
		!labels.every(isValidLabel)
	) {
		throw new Error(`Invalid domain: ${domain}`);
	}

	return normalized;
};

type DomainReadinessResponse = {
	data: unknown;
	status: number;
};

type DomainReadinessRequest = (
	url: string,
	options: { signal: AbortSignal; timeoutMs: number },
) => Promise<DomainReadinessResponse>;

export const isPeerbitDomainReadyResponse = (
	response: DomainReadinessResponse,
) =>
	response.status === 200 &&
	typeof response.data === "string" &&
	response.data.includes("<title>Peerbit</title>") &&
	response.data.includes('content="Peerbit node"');

export const isPeerbitDomainChallengeResponse = (
	response: DomainReadinessResponse,
	challengeToken: string,
) => response.status === 200 && response.data === challengeToken;

export const waitForPeerbitDomain = async (
	domain: string,
	options: {
		challengeToken?: string;
		delayIntervalMs?: number;
		request?: DomainReadinessRequest;
		requestTimeoutMs?: number;
		timeoutMs?: number;
	} = {},
) => {
	domain = normalizeDomain(domain);
	const challengeToken = options.challengeToken;
	if (challengeToken && !/^[a-f0-9]{64}$/.test(challengeToken)) {
		throw new Error("Domain readiness challenge token is invalid");
	}
	const timeoutMs = options.timeoutMs ?? 5 * 60 * 1000;
	const delayIntervalMs = options.delayIntervalMs ?? 5000;
	const requestTimeoutMs = options.requestTimeoutMs ?? 5000;
	if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
		throw new Error("Domain readiness timeout must be positive");
	}
	if (!Number.isFinite(requestTimeoutMs) || requestTimeoutMs <= 0) {
		throw new Error("Domain readiness request timeout must be positive");
	}
	if (!Number.isFinite(delayIntervalMs) || delayIntervalMs < 0) {
		throw new Error("Domain readiness delay must not be negative");
	}
	let request = options.request;
	if (!request) {
		const { default: axios } = await import("axios");
		request = (url, requestOptions) =>
			axios.get(url, {
				headers: {
					"Cache-Control": "no-cache, no-store",
					Pragma: "no-cache",
				},
				maxRedirects: 0,
				responseType: "text",
				signal: requestOptions.signal,
				timeout: requestOptions.timeoutMs,
				validateStatus: () => true,
			});
	}

	const deadline = Date.now() + timeoutMs;
	let attempt = 0;
	while (Date.now() < deadline) {
		const remainingMs = deadline - Date.now();
		const attemptTimeoutMs = Math.min(requestTimeoutMs, remainingMs);
		const controller = new AbortController();
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			attempt += 1;
			const requestUrl = challengeToken
				? `https://${domain}/.well-known/peerbit-generation/${challengeToken}?attempt=${attempt}`
				: `https://${domain}`;
			const response = await Promise.race([
				request(requestUrl, {
					signal: controller.signal,
					timeoutMs: attemptTimeoutMs,
				}),
				new Promise<never>((_resolve, reject) => {
					timer = setTimeout(() => {
						controller.abort();
						reject(new Error("Domain readiness request timed out"));
					}, attemptTimeoutMs);
				}),
			]);
			if (
				challengeToken
					? isPeerbitDomainChallengeResponse(response, challengeToken)
					: isPeerbitDomainReadyResponse(response)
			) {
				return;
			}
		} catch {
			// DNS, TLS, HTTP, and per-request timeout failures are retried until the
			// overall deadline. The old container remains available for rollback.
		} finally {
			if (timer) clearTimeout(timer);
			controller.abort();
		}

		const sleepMs = Math.min(delayIntervalMs, deadline - Date.now());
		if (sleepMs > 0) await delay(sleepMs);
	}

	throw new Error(`Timed out waiting for Peerbit at https://${domain}`);
};

const getUIPath = async (): Promise<string> => {
	const url = await import("url");
	const path = await import("path");
	const filename = url.fileURLToPath(import.meta.url);
	const p1 = path.join(filename, "../../", "ui");

	const fs = await import("fs");

	if (fs.existsSync(p1) && fs.lstatSync(p1).isDirectory()) {
		return p1; // build
	} else {
		const p2 = path.join(filename, "../../", "dist/ui");
		if (fs.existsSync(p2) && fs.lstatSync(p2).isDirectory()) {
			return p2;
		}
		throw new Error("Failed to find UI path");
	}
};
const validateEmail = (email: any) => {
	return String(email)
		.toLowerCase()
		.match(
			/^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/,
		);
};

export const normalizeDockerContainerName = (name: string): string => {
	const normalized = name.trim();
	if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(normalized)) {
		throw new Error(`Invalid Docker container name: ${name}`);
	}
	return normalized;
};

export const normalizeDomainGenerationName = (name: string): string => {
	if (!/^[a-zA-Z0-9][a-zA-Z0-9-]{0,127}$/.test(name)) {
		throw new Error(`Invalid domain configuration generation: ${name}`);
	}
	return name;
};

export const activateDomainGeneration = async (
	stateRoot: string,
	generation: string,
) => {
	generation = normalizeDomainGenerationName(generation);
	const fs = await import("fs");
	const path = await import("path");
	fs.mkdirSync(stateRoot, { recursive: true });
	const activePath = path.join(stateRoot, ACTIVE_DOMAIN_GENERATION_FILE);
	const stagingPath = `${activePath}.peerbit-${process.pid}-${Date.now()}`;
	try {
		fs.writeFileSync(stagingPath, generation, "utf8");
		fs.renameSync(stagingPath, activePath);
	} finally {
		fs.rmSync(stagingPath, { force: true });
	}
};

export const pruneDomainGenerations = async (
	stateRoot: string,
	activeGeneration: string,
	keepPrevious = 2,
) => {
	activeGeneration = normalizeDomainGenerationName(activeGeneration);
	if (!Number.isInteger(keepPrevious) || keepPrevious < 0) {
		throw new Error(
			"Domain generation retention must be a non-negative integer",
		);
	}
	const fs = await import("fs");
	const path = await import("path");
	const generationsRoot = path.join(stateRoot, DOMAIN_GENERATIONS_FOLDER);
	if (!fs.existsSync(generationsRoot)) return;

	const generations = fs
		.readdirSync(generationsRoot, { withFileTypes: true })
		.filter((entry) => entry.isDirectory())
		.map((entry) => entry.name)
		.filter((name) => {
			try {
				normalizeDomainGenerationName(name);
				return true;
			} catch {
				return false;
			}
		})
		.sort()
		.reverse();
	const retained = new Set([
		activeGeneration,
		...generations
			.filter((generation) => generation !== activeGeneration)
			.slice(0, keepPrevious),
	]);
	for (const generation of generations) {
		if (!retained.has(generation)) {
			fs.rmSync(path.join(generationsRoot, generation), {
				recursive: true,
				force: true,
			});
		}
	}
};

export const buildCertbotContainerArguments = (properties: {
	containerName: string;
	email: string;
	nginxConfigPath: string;
	secretsPath: string;
	uiPath: string;
	staging?: boolean;
}): string[] => [
	"run",
	"--name",
	normalizeDockerContainerName(properties.containerName),
	"-d",
	"--net=host",
	"--restart",
	"unless-stopped",
	"--label",
	`${CERTBOT_MANAGED_LABEL}=${CERTBOT_MANAGED_LABEL_VALUE}`,
	"--env",
	`CERTBOT_EMAIL=${properties.email}`,
	...(properties.staging ? ["--env", "STAGING=1"] : []),
	"-v",
	`${properties.secretsPath}:/etc/letsencrypt`,
	"-v",
	`${properties.nginxConfigPath}:/etc/nginx/user_conf.d:ro`,
	"-v",
	`${properties.uiPath}:/usr/share/nginx/html:ro`,
	CERTBOT_IMAGE,
];

export const isPeerbitCertbotContainer = (
	inspection: DockerContainerInspection,
): boolean => {
	const destinations = new Set(
		inspection.Mounts?.map((mount) => mount.Destination).filter(Boolean),
	);
	const hasExpectedMounts = CERTBOT_MOUNT_DESTINATIONS.every((destination) =>
		destinations.has(destination),
	);
	if (!hasExpectedMounts) return false;

	if (
		inspection.Config?.Labels?.[CERTBOT_MANAGED_LABEL] ===
		CERTBOT_MANAGED_LABEL_VALUE
	) {
		return true;
	}

	return (
		/^jonasal\/nginx-certbot(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?(?:@sha256:[a-f0-9]{64})?$/.test(
			inspection.Config?.Image || "",
		) &&
		inspection.Config?.Env?.some((value) =>
			value.startsWith("CERTBOT_EMAIL="),
		) === true
	);
};

type MountedDomainGeneration = {
	configPath: string;
	generation: string;
	nginxPath: string;
	stateRoot: string;
};

const getUniqueMountSources = (
	inspection: DockerContainerInspection,
): Map<string, string> | undefined => {
	const sources = new Map<string, string>();
	for (const destination of CERTBOT_MOUNT_DESTINATIONS) {
		const matching =
			inspection.Mounts?.filter(
				(mount) => mount.Destination === destination && mount.Source,
			) || [];
		if (matching.length !== 1 || !matching[0].Source) return undefined;
		sources.set(destination, matching[0].Source);
	}
	return sources;
};

/**
 * Discard a staged domain generation only after Docker rollback has produced a
 * complete inventory proving that no remaining transaction container mounts
 * the generation. The active-generation marker is an additional durable guard.
 */
export const discardDomainGenerationAfterRollback = async (
	generationRoot: string,
	remainingContainers: readonly DockerContainerInspection[],
): Promise<boolean> => {
	const fs = await import("fs");
	const path = await import("path");
	if (!fs.existsSync(generationRoot)) return true;

	const resolvedGenerationRoot = fs.realpathSync(generationRoot);
	const generation = path.basename(resolvedGenerationRoot);
	const stateRoot = path.dirname(path.dirname(resolvedGenerationRoot));
	const activePath = path.join(stateRoot, ACTIVE_DOMAIN_GENERATION_FILE);
	if (fs.existsSync(activePath)) {
		let activeGeneration: string;
		try {
			activeGeneration = normalizeDomainGenerationName(
				fs.readFileSync(activePath, "utf8").trim(),
			);
		} catch {
			return false;
		}
		if (activeGeneration === generation) return false;
	}

	const contains = (parent: string, candidate: string) => {
		const relative = path.relative(parent, candidate);
		return (
			relative === "" ||
			(!relative.startsWith(`..${path.sep}`) &&
				relative !== ".." &&
				!path.isAbsolute(relative))
		);
	};
	for (const container of remainingContainers) {
		for (const mount of container.Mounts || []) {
			if (!mount.Source) continue;
			let resolvedSource: string;
			try {
				resolvedSource = fs.realpathSync(mount.Source);
			} catch {
				resolvedSource = path.resolve(mount.Source);
			}
			if (
				contains(resolvedGenerationRoot, resolvedSource) ||
				contains(resolvedSource, resolvedGenerationRoot)
			) {
				return false;
			}
		}
	}

	fs.rmSync(resolvedGenerationRoot, { recursive: true, force: true });
	return true;
};

const getMountedDomainGeneration = async (
	inspection: DockerContainerInspection,
): Promise<MountedDomainGeneration | undefined> => {
	const path = await import("path");
	const sources = getUniqueMountSources(inspection);
	const source = sources?.get("/etc/nginx/user_conf.d");
	if (!source) return undefined;
	const nginxPath = path.resolve(source);
	if (path.basename(nginxPath) !== "nginx") return undefined;
	const generationRoot = path.dirname(nginxPath);
	const generation = path.basename(generationRoot);
	const generationsRoot = path.dirname(generationRoot);
	const stateRoot = path.dirname(generationsRoot);
	if (
		path.basename(generationsRoot) !== DOMAIN_GENERATIONS_FOLDER ||
		path.basename(stateRoot) !== DOMAIN_STATE_FOLDER
	) {
		return undefined;
	}
	try {
		normalizeDomainGenerationName(generation);
	} catch {
		return undefined;
	}
	return {
		configPath: path.join(nginxPath, "default.conf"),
		generation,
		nginxPath,
		stateRoot,
	};
};

const readActiveDomainGeneration = async (
	stateRoot: string,
): Promise<
	| { kind: "missing" }
	| { kind: "invalid" }
	| { generation: string; kind: "value" }
> => {
	const fs = await import("fs");
	const path = await import("path");
	const activePath = path.join(stateRoot, ACTIVE_DOMAIN_GENERATION_FILE);
	if (!fs.existsSync(activePath)) return { kind: "missing" };
	try {
		return {
			generation: normalizeDomainGenerationName(
				fs.readFileSync(activePath, "utf8").trim(),
			),
			kind: "value",
		};
	} catch {
		return { kind: "invalid" };
	}
};

/**
 * Reconcile a crash that left both the replacement and its backup. The active
 * generation file is the commit marker; malformed or cross-root state is left
 * untouched for an operator rather than guessed at.
 */
export const resolvePeerbitCertbotInterruptedPair = async (
	current: DockerContainerInspection,
	backup: DockerContainerInspection,
): Promise<"keep-current" | "restore-backup" | undefined> => {
	if (
		!isPeerbitCertbotContainer(current) ||
		!isPeerbitCertbotContainer(backup)
	) {
		return undefined;
	}
	const currentSources = getUniqueMountSources(current);
	const backupSources = getUniqueMountSources(backup);
	if (!currentSources || !backupSources) return undefined;
	const path = await import("path");
	if (
		path.resolve(currentSources.get("/etc/letsencrypt")!) !==
		path.resolve(backupSources.get("/etc/letsencrypt")!)
	) {
		return undefined;
	}

	const currentGeneration = await getMountedDomainGeneration(current);
	if (!currentGeneration) return undefined;
	const backupGeneration = await getMountedDomainGeneration(backup);
	const legacyNginxPath = path.resolve(
		path.dirname(currentGeneration.stateRoot),
		"nginx",
	);
	if (
		backupGeneration
			? backupGeneration.stateRoot !== currentGeneration.stateRoot
			: path.resolve(backupSources.get("/etc/nginx/user_conf.d")!) !==
				legacyNginxPath
	) {
		return undefined;
	}
	const active = await readActiveDomainGeneration(currentGeneration.stateRoot);
	const fs = await import("fs");
	if (
		active.kind === "value" &&
		active.generation === currentGeneration.generation
	) {
		const currentIsStable =
			current.State?.Running === true &&
			current.State.Restarting !== true &&
			(current.RestartCount ?? 0) === 0 &&
			current.State.Health?.Status !== "unhealthy";
		return currentIsStable && fs.existsSync(currentGeneration.configPath)
			? "keep-current"
			: undefined;
	}
	if (active.kind !== "value" && active.kind !== "missing") return undefined;

	if (
		backupGeneration &&
		backupGeneration.stateRoot === currentGeneration.stateRoot &&
		active.kind === "value" &&
		active.generation === backupGeneration.generation &&
		fs.existsSync(backupGeneration.configPath)
	) {
		return "restore-backup";
	}
	if (active.kind !== "missing" || backupGeneration) return undefined;

	return "restore-backup";
};

export const getCertbotSecretsPath = (
	inspection: DockerContainerInspection | undefined,
	fallback: string,
): string => {
	if (!inspection) return fallback;
	const source = inspection.Mounts?.find(
		(mount) => mount.Destination === "/etc/letsencrypt",
	)?.Source;
	if (!source) {
		throw new Error(
			"Existing Peerbit certificate container has no host certificate mount",
		);
	}
	return source;
};

export const replaceUiFolder = async (source: string, destination: string) => {
	const fs = await import("fs");
	const path = await import("path");
	fs.mkdirSync(path.dirname(destination), { recursive: true });
	const staging = `${destination}.peerbit-${process.pid}-${Date.now()}`;
	fs.rmSync(staging, { recursive: true, force: true });
	try {
		fs.cpSync(source, staging, { recursive: true });
		fs.rmSync(destination, { recursive: true, force: true });
		fs.renameSync(staging, destination);
	} finally {
		fs.rmSync(staging, { recursive: true, force: true });
	}
};

export const startCertbot = async (
	domain: string,
	email: string,
	waitForUp = false,
	dockerProcessName = "nginx-certbot",
): Promise<void> => {
	domain = normalizeDomain(domain);
	dockerProcessName = normalizeDockerContainerName(dockerProcessName);
	if (!validateEmail(email)) {
		throw new Error("Email for SSL renewal is invalid");
	}
	const { installDocker, pullDockerImage, replaceDockerContainer } =
		await import("./docker.js");

	await installDocker();
	await pullDockerImage(CERTBOT_IMAGE);

	const isTest = process.env.JEST_WORKER_ID !== undefined;
	const fs = await import("fs");
	const path = await import("path");
	const { randomBytes, randomUUID } = await import("crypto");
	const stateRoot = path.join(process.cwd(), DOMAIN_STATE_FOLDER);
	const generation = normalizeDomainGenerationName(
		`${Date.now()}-${randomUUID()}`,
	);
	const generationRoot = path.join(
		stateRoot,
		DOMAIN_GENERATIONS_FOLDER,
		generation,
	);
	const nginxConfigPath = path.join(generationRoot, "nginx");
	const uiPath = path.join(generationRoot, "ui");
	const readinessChallenge = randomBytes(32).toString("hex");
	let activated = false;
	let replacementPrepared = false;
	let rollbackDispositionHandled = false;
	try {
		await replaceUiFolder(await getUIPath(), uiPath);
		const readinessChallengeDirectory = path.join(
			uiPath,
			".well-known",
			"peerbit-generation",
		);
		fs.mkdirSync(readinessChallengeDirectory, { recursive: true });
		fs.writeFileSync(
			path.join(readinessChallengeDirectory, readinessChallenge),
			readinessChallenge,
			"utf8",
		);
		await createConfig(nginxConfigPath, domain);

		// Reconfiguration keeps the old container and generation as a stopped
		// backup until the new container and active-generation pointer both succeed.
		console.log("Starting Certbot");
		await replaceDockerContainer(
			dockerProcessName,
			(existingContainer) => {
				replacementPrepared = true;
				return buildCertbotContainerArguments({
					containerName: dockerProcessName,
					email,
					nginxConfigPath,
					secretsPath: getCertbotSecretsPath(
						existingContainer,
						path.join(stateRoot, "nginx_secrets"),
					),
					uiPath,
					staging: isTest,
				});
			},
			{
				onCommitted: async () => {
					try {
						await pruneDomainGenerations(stateRoot, generation);
					} catch (error: any) {
						console.warn(
							`Domain configuration succeeded, but stale generations could not be pruned: ${error?.message || error}`,
						);
					}
				},
				onCommit: async () => {
					await activateDomainGeneration(stateRoot, generation);
					activated = true;
				},
				onRollbackInspected: async (remainingContainers) => {
					rollbackDispositionHandled = true;
					await discardDomainGenerationAfterRollback(
						generationRoot,
						remainingContainers,
					);
				},
				onStarted: async () => {
					if (waitForUp) {
						console.log("Waiting for domain to be ready ...");
						await waitForPeerbitDomain(domain, {
							challengeToken: readinessChallenge,
						});
					}
				},
				resolveInterruptedPair: resolvePeerbitCertbotInterruptedPair,
				validateExisting: isPeerbitCertbotContainer,
			},
		);
	} catch (error) {
		if (!activated && !replacementPrepared && !rollbackDispositionHandled) {
			fs.rmSync(generationRoot, { recursive: true, force: true });
		}
		throw error;
	}

	console.log("Certbot started succesfully!");
	console.log("You domain is: ");
	console.log(domain);
	if (waitForUp) {
		console.log("Domain is ready");
	} else {
		console.log(
			"The domain is not available immediately as it takes some time to request SSL certificate.",
		);
	}
};
