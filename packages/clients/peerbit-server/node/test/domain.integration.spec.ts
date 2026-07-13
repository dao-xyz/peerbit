import { expect } from "chai";
import dotenv from "dotenv";
import fs from "fs";
import os from "os";
import path from "path";
import type { DockerContainerInspection } from "../src/docker.js";
import {
	CERTBOT_IMAGE,
	CERTBOT_MANAGED_LABEL,
	CERTBOT_MANAGED_LABEL_VALUE,
	activateDomainGeneration,
	buildCertbotContainerArguments,
	discardDomainGenerationAfterRollback,
	getCertbotSecretsPath,
	getDomainFromConfig,
	isPeerbitCertbotContainer,
	isPeerbitDomainChallengeResponse,
	isPeerbitDomainReadyResponse,
	loadConfig,
	normalizeDockerContainerName,
	normalizeDomain,
	pruneDomainGenerations,
	replaceUiFolder,
	resolvePeerbitCertbotInterruptedPair,
	waitForPeerbitDomain,
} from "../src/domain.js";

describe("getDomainFromConfig", () => {
	before(() => {
		dotenv.config();
	});

	it("%DOMAIN%", async () => {
		const config =
			" ssl_certificate         /etc/letsencrypt/live/%DOMAIN%/fullchain.pem; \nssl_certificate_key     /etc/letsencrypt/live/%DOMAIN%/privkey.pem; ";
		const domain = await getDomainFromConfig(config);
		expect(domain).equal(undefined);
	});

	it("specified domain", async () => {
		const expectedDomain = "a.b-c.d";
		const config = ` ssl_certificate         /etc/letsencrypt/live/${expectedDomain}/fullchain.pem; \nssl_certificate_key     /etc/letsencrypt/live/${expectedDomain}/privkey.pem; `;
		const domain = await getDomainFromConfig(config);
		expect(domain).equal(expectedDomain);
	});

	it("normalizes a fully qualified domain", () => {
		expect(normalizeDomain(" Node.Example.COM. ")).equal("node.example.com");
	});

	for (const domain of [
		"localhost",
		"-node.example.com",
		"node-.example.com",
		"node..example.com",
		"node.example.com; reboot",
	]) {
		it(`rejects invalid domain: ${domain}`, () => {
			expect(() => normalizeDomain(domain)).to.throw("Invalid domain");
		});
	}

	it("requires a direct Peerbit UI response before declaring readiness", async () => {
		const expectedBody =
			'<html><head><meta name="description" content="Peerbit node"><title>Peerbit</title></head></html>';
		expect(isPeerbitDomainReadyResponse({ data: expectedBody, status: 200 })).to
			.be.true;
		expect(isPeerbitDomainReadyResponse({ data: expectedBody, status: 302 })).to
			.be.false;
		expect(
			isPeerbitDomainReadyResponse({
				data: "<html><title>Another service</title></html>",
				status: 200,
			}),
		).to.be.false;

		const requested: string[] = [];
		await waitForPeerbitDomain("Node.Example.com.", {
			delayIntervalMs: 0,
			request: async (url) => {
				requested.push(url);
				return requested.length === 1
					? { data: "not ready", status: 200 }
					: { data: expectedBody, status: 200 };
			},
			timeoutMs: 100,
		});
		expect(requested).deep.equal([
			"https://node.example.com",
			"https://node.example.com",
		]);
	});

	it("requires the exact staged-generation challenge", async () => {
		const challengeToken = "a".repeat(64);
		const staticPeerbitPage =
			'<html><meta name="description" content="Peerbit node"><title>Peerbit</title></html>';
		expect(
			isPeerbitDomainChallengeResponse(
				{ data: challengeToken, status: 200 },
				challengeToken,
			),
		).to.be.true;
		expect(
			isPeerbitDomainChallengeResponse(
				{ data: staticPeerbitPage, status: 200 },
				challengeToken,
			),
		).to.be.false;

		const requested: string[] = [];
		await waitForPeerbitDomain("node.example.com", {
			challengeToken,
			delayIntervalMs: 0,
			request: async (url) => {
				requested.push(url);
				if (requested.length === 1) {
					return { data: staticPeerbitPage, status: 200 };
				}
				if (requested.length === 2) {
					return { data: "wrong-generation", status: 200 };
				}
				return { data: challengeToken, status: 200 };
			},
			timeoutMs: 100,
		});
		expect(requested).deep.equal([
			`https://node.example.com/.well-known/peerbit-generation/${challengeToken}?attempt=1`,
			`https://node.example.com/.well-known/peerbit-generation/${challengeToken}?attempt=2`,
			`https://node.example.com/.well-known/peerbit-generation/${challengeToken}?attempt=3`,
		]);
	});

	it("rejects unsafe readiness challenge tokens", async () => {
		let error: Error | undefined;
		try {
			await waitForPeerbitDomain("node.example.com", {
				challengeToken: "../wrong-node",
			});
		} catch (caught) {
			error = caught as Error;
		}
		expect(error?.message).to.include("challenge token is invalid");
	});

	it("bounds a request that ignores cancellation and retries", async () => {
		const expectedBody =
			'<html><meta name="description" content="Peerbit node"><title>Peerbit</title></html>';
		let attempts = 0;
		let firstSignal: AbortSignal | undefined;
		await waitForPeerbitDomain("node.example.com", {
			delayIntervalMs: 0,
			request: async (_url, requestOptions) => {
				attempts += 1;
				if (attempts === 1) firstSignal = requestOptions.signal;
				return attempts === 1
					? new Promise<never>(() => {})
					: { data: expectedBody, status: 200 };
			},
			requestTimeoutMs: 2,
			timeoutMs: 100,
		});
		expect(attempts).equal(2);
		expect(firstSignal?.aborted).to.be.true;
	});

	it("does not let request or delay settings exceed the overall deadline", async () => {
		const started = Date.now();
		let error: Error | undefined;
		try {
			await waitForPeerbitDomain("node.example.com", {
				delayIntervalMs: 1000,
				request: async () => new Promise<never>(() => {}),
				requestTimeoutMs: 1000,
				timeoutMs: 20,
			});
		} catch (caught) {
			error = caught as Error;
		}
		expect(error?.message).to.include("Timed out waiting");
		expect(Date.now() - started).to.be.lessThan(250);
	});

	it("pins the certificate container to an immutable image", () => {
		expect(CERTBOT_IMAGE).to.match(
			/^jonasal\/nginx-certbot:[^@]+@sha256:[a-f0-9]{64}$/,
		);
		expect(CERTBOT_IMAGE).not.to.include(":latest");
	});

	it("passes certificate settings as isolated Docker arguments", () => {
		const email = "x`touch${IFS}/tmp/pwn`@example.com";
		const args = buildCertbotContainerArguments({
			containerName: "nginx-certbot",
			email,
			nginxConfigPath: "/tmp/a path/nginx",
			secretsPath: "/tmp/a path/secrets",
			uiPath: "/tmp/a path/ui",
		});

		expect(args.slice(0, 3)).deep.equal(["run", "--name", "nginx-certbot"]);
		expect(args[args.indexOf("--env") + 1]).equal(`CERTBOT_EMAIL=${email}`);
		expect(args).to.include("/tmp/a path/nginx:/etc/nginx/user_conf.d:ro");
		expect(args).to.include("/tmp/a path/secrets:/etc/letsencrypt");
		expect(args).to.include("/tmp/a path/ui:/usr/share/nginx/html:ro");
		expect(args).to.include(
			`${CERTBOT_MANAGED_LABEL}=${CERTBOT_MANAGED_LABEL_VALUE}`,
		);
	});

	it("recognizes only managed or legacy Peerbit certificate containers", () => {
		const mounts = [
			{ Source: "/old/secrets", Destination: "/etc/letsencrypt" },
			{ Source: "/old/nginx", Destination: "/etc/nginx/user_conf.d" },
			{ Source: "/old/ui", Destination: "/usr/share/nginx/html" },
		];
		const legacy: DockerContainerInspection = {
			Config: {
				Env: ["CERTBOT_EMAIL=operator@example.com"],
				Image: "jonasal/nginx-certbot:latest",
			},
			Mounts: mounts,
		};
		const managed: DockerContainerInspection = {
			Config: {
				Image: CERTBOT_IMAGE,
				Labels: {
					[CERTBOT_MANAGED_LABEL]: CERTBOT_MANAGED_LABEL_VALUE,
				},
			},
			Mounts: mounts,
		};

		expect(isPeerbitCertbotContainer(legacy)).to.be.true;
		expect(isPeerbitCertbotContainer(managed)).to.be.true;
		expect(
			isPeerbitCertbotContainer({
				Config: {
					Env: ["CERTBOT_EMAIL=operator@example.com"],
					Image: "jonasal/nginx-certbot-evil:latest",
				},
				Mounts: mounts,
			}),
		).to.be.false;
		expect(
			isPeerbitCertbotContainer({
				Config: { Image: "unrelated/server:latest" },
				Mounts: mounts,
			}),
		).to.be.false;
		expect(
			isPeerbitCertbotContainer({
				Config: {
					Labels: {
						[CERTBOT_MANAGED_LABEL]: CERTBOT_MANAGED_LABEL_VALUE,
					},
				},
			}),
		).to.be.false;
	});

	it("reuses the inspected certificate mount across working directories", () => {
		const existing: DockerContainerInspection = {
			Mounts: [
				{ Source: "/original/nginx_secrets", Destination: "/etc/letsencrypt" },
			],
		};
		expect(getCertbotSecretsPath(existing, "/different/nginx_secrets")).equal(
			"/original/nginx_secrets",
		);
		expect(getCertbotSecretsPath(undefined, "/new/nginx_secrets")).equal(
			"/new/nginx_secrets",
		);
	});

	it("resolves only unambiguous interrupted domain generations", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "peerbit-recovery-"));
		const stateRoot = path.join(root, ".peerbit-domain");
		const generationsRoot = path.join(stateRoot, "generations");
		const secrets = path.join(root, "nginx_secrets");
		const generationPath = (generation: string) =>
			path.join(generationsRoot, generation, "nginx");
		const container = (
			nginxPath: string,
			state: DockerContainerInspection["State"] = { Running: true },
		): DockerContainerInspection => ({
			Config: {
				Image: CERTBOT_IMAGE,
				Labels: {
					[CERTBOT_MANAGED_LABEL]: CERTBOT_MANAGED_LABEL_VALUE,
				},
			},
			Mounts: [
				{ Destination: "/etc/letsencrypt", Source: secrets },
				{ Destination: "/etc/nginx/user_conf.d", Source: nginxPath },
				{
					Destination: "/usr/share/nginx/html",
					Source: path.join(path.dirname(nginxPath), "ui"),
				},
			],
			RestartCount: 0,
			State: state,
		});

		try {
			for (const generation of ["100-backup", "200-current"]) {
				fs.mkdirSync(generationPath(generation), { recursive: true });
				fs.writeFileSync(
					path.join(generationPath(generation), "default.conf"),
					generation,
				);
			}
			const current = container(generationPath("200-current"));
			const backup = container(generationPath("100-backup"), {
				Running: false,
			});

			fs.writeFileSync(path.join(stateRoot, "active"), "200-current");
			expect(await resolvePeerbitCertbotInterruptedPair(current, backup)).equal(
				"keep-current",
			);
			expect(
				await resolvePeerbitCertbotInterruptedPair(
					container(generationPath("200-current"), { Running: false }),
					backup,
				),
			).equal(undefined);
			const mismatchedSecrets = container(generationPath("100-backup"), {
				Running: false,
			});
			mismatchedSecrets.Mounts!.find(
				(mount) => mount.Destination === "/etc/letsencrypt",
			)!.Source = path.join(root, "other-secrets");
			expect(
				await resolvePeerbitCertbotInterruptedPair(current, mismatchedSecrets),
			).equal(undefined);

			fs.writeFileSync(path.join(stateRoot, "active"), "100-backup");
			expect(await resolvePeerbitCertbotInterruptedPair(current, backup)).equal(
				"restore-backup",
			);

			fs.writeFileSync(path.join(stateRoot, "active"), "300-other");
			expect(await resolvePeerbitCertbotInterruptedPair(current, backup)).equal(
				undefined,
			);

			fs.writeFileSync(path.join(stateRoot, "active"), "../corrupt");
			expect(await resolvePeerbitCertbotInterruptedPair(current, backup)).equal(
				undefined,
			);

			fs.rmSync(path.join(stateRoot, "active"));
			const legacyNginx = path.join(root, "nginx");
			fs.mkdirSync(legacyNginx);
			expect(
				await resolvePeerbitCertbotInterruptedPair(
					current,
					container(legacyNginx, { Running: false }),
				),
			).equal("restore-backup");

			const otherRoot = fs.mkdtempSync(
				path.join(os.tmpdir(), "peerbit-recovery-other-"),
			);
			try {
				const crossRootNginx = path.join(
					otherRoot,
					".peerbit-domain",
					"generations",
					"old",
					"nginx",
				);
				fs.mkdirSync(crossRootNginx, { recursive: true });
				fs.writeFileSync(
					path.join(crossRootNginx, "default.conf"),
					"cross-root",
				);
				expect(
					await resolvePeerbitCertbotInterruptedPair(
						current,
						container(crossRootNginx, { Running: false }),
					),
				).equal(undefined);
			} finally {
				fs.rmSync(otherRoot, { recursive: true, force: true });
			}
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("activates a complete generation atomically and keeps legacy fallback", async () => {
		const originalCwd = process.cwd();
		const root = fs.mkdtempSync(
			path.join(os.tmpdir(), "peerbit-domain-state-"),
		);
		try {
			const legacyNginx = path.join(root, "nginx");
			const generatedNginx = path.join(
				root,
				".peerbit-domain",
				"generations",
				"generation-1",
				"nginx",
			);
			fs.mkdirSync(legacyNginx, { recursive: true });
			fs.mkdirSync(generatedNginx, { recursive: true });
			fs.writeFileSync(path.join(legacyNginx, "default.conf"), "legacy");
			fs.writeFileSync(path.join(generatedNginx, "default.conf"), "current");
			process.chdir(root);

			expect(await loadConfig()).equal("legacy");
			await activateDomainGeneration(
				path.join(root, ".peerbit-domain"),
				"generation-1",
			);
			expect(await loadConfig()).equal("current");
			fs.rmSync(path.join(generatedNginx, "default.conf"));
			let error: Error | undefined;
			try {
				await loadConfig();
			} catch (caught) {
				error = caught as Error;
			}
			expect(error?.message).to.include("Active domain configuration");
			expect(error?.message).to.include("is missing");
		} finally {
			process.chdir(originalCwd);
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("prunes stale generations while retaining the active and recent history", async () => {
		const root = fs.mkdtempSync(
			path.join(os.tmpdir(), "peerbit-domain-prune-"),
		);
		const stateRoot = path.join(root, ".peerbit-domain");
		const generationsRoot = path.join(stateRoot, "generations");
		try {
			for (const generation of ["100-old", "200-recent", "300-active"]) {
				fs.mkdirSync(path.join(generationsRoot, generation), {
					recursive: true,
				});
			}
			fs.writeFileSync(path.join(generationsRoot, "operator-notes"), "keep");

			await pruneDomainGenerations(stateRoot, "300-active", 1);

			expect(fs.existsSync(path.join(generationsRoot, "100-old"))).to.be.false;
			expect(fs.existsSync(path.join(generationsRoot, "200-recent"))).to.be
				.true;
			expect(fs.existsSync(path.join(generationsRoot, "300-active"))).to.be
				.true;
			expect(fs.existsSync(path.join(generationsRoot, "operator-notes"))).to.be
				.true;
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("keeps a generation until rollback proves no container mounts or activates it", async () => {
		const root = fs.mkdtempSync(
			path.join(os.tmpdir(), "peerbit-domain-rollback-"),
		);
		const stateRoot = path.join(root, ".peerbit-domain");
		const generationRoot = path.join(
			stateRoot,
			"generations",
			"generation-new",
		);
		try {
			fs.mkdirSync(path.join(generationRoot, "nginx"), { recursive: true });
			const survivingReplacement: DockerContainerInspection = {
				Id: "replacement",
				Mounts: [
					{
						Destination: "/etc/nginx/user_conf.d",
						Source: path.join(generationRoot, "nginx"),
					},
				],
			};

			expect(
				await discardDomainGenerationAfterRollback(generationRoot, [
					survivingReplacement,
				]),
			).to.be.false;
			expect(fs.existsSync(generationRoot)).to.be.true;

			fs.writeFileSync(path.join(stateRoot, "active"), "generation-new");
			expect(await discardDomainGenerationAfterRollback(generationRoot, [])).to
				.be.false;
			expect(fs.existsSync(generationRoot)).to.be.true;

			fs.writeFileSync(path.join(stateRoot, "active"), "generation-old");
			expect(await discardDomainGenerationAfterRollback(generationRoot, [])).to
				.be.true;
			expect(fs.existsSync(generationRoot)).to.be.false;
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("validates the Docker container name", () => {
		expect(normalizeDockerContainerName(" nginx-certbot ")).equal(
			"nginx-certbot",
		);
		expect(() => normalizeDockerContainerName("nginx; reboot")).to.throw(
			"Invalid Docker container name",
		);
	});

	it("replaces UI assets without nesting or retaining stale files", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "peerbit-certbot-ui-"));
		const source = path.join(root, "source");
		const destination = path.join(root, "ui");
		try {
			fs.mkdirSync(source);
			fs.mkdirSync(destination);
			fs.writeFileSync(path.join(source, "index.html"), "current");
			fs.writeFileSync(path.join(destination, "stale.html"), "stale");

			await replaceUiFolder(source, destination);
			await replaceUiFolder(source, destination);

			expect(
				fs.readFileSync(path.join(destination, "index.html"), "utf8"),
			).equal("current");
			expect(fs.existsSync(path.join(destination, "stale.html"))).to.be.false;
			expect(fs.existsSync(path.join(destination, "source"))).to.be.false;
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});
});
