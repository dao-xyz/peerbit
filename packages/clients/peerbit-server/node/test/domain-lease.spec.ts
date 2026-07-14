import { expect } from "chai";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import {
	DNS_LEASE_ACCESS_TOKEN_ENV,
	DnsLeaseClient,
	type DnsLeaseFetch,
	type DnsLeaseState,
	provisionDnsLease,
	readDnsLeaseState,
	releaseDnsLease,
	renewDnsLease,
	serveDnsLeaseChallenge,
	startDnsLeaseRenewal,
} from "../src/domain-lease.js";

const ACCESS_TOKEN = "access_token_abcdefghijklmnopqrstuvwxyz";
const ADDRESS = "1.1.1.1";
const DOMAIN = "p-0123456789abcdefabcd.nodes.peerchecker.com";
const LEASE_ID = "lease_1234";
const CHALLENGE_TOKEN = "c".repeat(43);
const RENEW_CHALLENGE_TOKEN = "d".repeat(43);
const CHALLENGE_EXPIRY = "2099-01-01T00:05:00.000Z";
const PENDING_EXPIRY = "2099-01-01T00:10:00.000Z";
const ACTIVE_EXPIRY = "2099-01-02T00:00:00.000Z";
const SERVICE_URL = "https://example.supabase.co/functions/v1/dns-lease";

const jsonResponse = (value: unknown, status = 200) => ({
	status,
	text: async () => JSON.stringify(value),
});

const actionFromUrl = (url: string) => new URL(url).pathname.split("/").pop();

const pendingResponse = () => ({
	id: LEASE_ID,
	domain: DOMAIN,
	recordType: "A",
	address: ADDRESS,
	status: "pending",
	challengeToken: CHALLENGE_TOKEN,
	challengeUrl: `http://${ADDRESS}/.well-known/peerbit-dns/${LEASE_ID}`,
	challengeExpiresAt: CHALLENGE_EXPIRY,
	pendingExpiresAt: PENDING_EXPIRY,
});

const activeResponse = (expiresAt = ACTIVE_EXPIRY) => ({
	id: LEASE_ID,
	domain: DOMAIN,
	recordType: "A",
	address: ADDRESS,
	status: "active",
	expiresAt,
});

const renewChallengeResponse = () => ({
	...activeResponse(),
	challengeToken: RENEW_CHALLENGE_TOKEN,
	challengeUrl: `http://${ADDRESS}/.well-known/peerbit-dns/${LEASE_ID}`,
	challengeExpiresAt: CHALLENGE_EXPIRY,
});

describe("managed DNS leases", () => {
	let root: string;
	let statePath: string;

	beforeEach(() => {
		root = fs.mkdtempSync(path.join(os.tmpdir(), "peerbit-dns-lease-"));
		statePath = path.join(root, ".peerbit-domain", "lease.json");
	});

	afterEach(() => {
		fs.rmSync(root, { force: true, recursive: true });
	});

	it("uses access auth only for claim and lease auth for lifecycle calls", async () => {
		const calls: Array<{
			action: string | undefined;
			body: Record<string, string>;
			authorization: string | undefined;
		}> = [];
		const request: DnsLeaseFetch = async (url, init) => {
			const action = actionFromUrl(url);
			const headers = init.headers as Record<string, string>;
			calls.push({
				action,
				body: JSON.parse(init.body as string),
				authorization: headers.Authorization,
			});
			if (action === "claim") {
				return jsonResponse(pendingResponse());
			}
			if (action === "renew-challenge") {
				return jsonResponse(renewChallengeResponse());
			}
			if (action === "release") {
				return jsonResponse({ id: LEASE_ID, status: "expired" });
			}
			return jsonResponse(activeResponse());
		};
		let configuredDomain: string | undefined;
		const active = await provisionDnsLease({
			accessToken: ACCESS_TOKEN,
			address: ADDRESS,
			configure: async (domain) => {
				configuredDomain = domain;
			},
			request,
			serveChallenge: async (state, verify) => {
				expect(state.challengeToken).equal(CHALLENGE_TOKEN);
				return verify();
			},
			serviceUrl: SERVICE_URL,
			statePath,
		});

		expect(active.domain).equal(DOMAIN);
		expect(configuredDomain).equal(DOMAIN);
		expect(calls.map((call) => call.action)).deep.equal(["claim", "verify"]);
		expect(calls[0].authorization).equal(`Bearer ${ACCESS_TOKEN}`);
		expect(calls[0].body).to.have.keys([
			"address",
			"idempotencyKey",
			"leaseToken",
			"recordType",
		]);
		expect(calls[0].body).not.to.have.property("owner");
		expect(calls[1].authorization).equal(`Bearer ${calls[0].body.leaseToken}`);
		expect(calls[1].body).deep.equal({ id: LEASE_ID });
		expect(fs.statSync(statePath).mode & 0o777).equal(0o600);
		const storedActive = readDnsLeaseState(statePath)!;
		expect(storedActive.challengeToken).equal(undefined);
		expect(storedActive.challengeUrl).equal(undefined);
		expect(storedActive.challengeExpiresAt).equal(undefined);
		const serialized = fs.readFileSync(statePath, "utf8");
		expect(serialized).not.to.include(ACCESS_TOKEN);
		expect(serialized).not.to.include(DNS_LEASE_ACCESS_TOKEN_ENV);

		await renewDnsLease({
			request,
			serveChallenge: async (state, renew, listen) => {
				expect(state.challengeToken).equal(RENEW_CHALLENGE_TOKEN);
				expect(state.challengeExpiresAt).equal(CHALLENGE_EXPIRY);
				expect(listen?.preferManagedProxy).equal(true);
				return renew();
			},
			statePath,
		});
		expect(calls.slice(-2).map((call) => call.action)).deep.equal([
			"renew-challenge",
			"renew",
		]);
		expect(calls.at(-2)?.body).deep.equal({ id: LEASE_ID });
		expect(calls.at(-1)?.body).deep.equal({ id: LEASE_ID });
		expect(calls.at(-2)?.authorization).equal(
			`Bearer ${calls[0].body.leaseToken}`,
		);
		expect(calls.at(-1)?.authorization).equal(
			`Bearer ${calls[0].body.leaseToken}`,
		);

		await releaseDnsLease({ request, statePath });
		expect(calls.at(-1)?.action).equal("release");
		const released = readDnsLeaseState(statePath)!;
		expect(released.status).equal("released");
		expect(released.leaseToken).equal(undefined);
		expect(released.challengeToken).equal(undefined);
		let renewedReleasedLease = false;
		const stop = await startDnsLeaseRenewal({
			request: async () => {
				renewedReleasedLease = true;
				throw new Error("must not run");
			},
			serveChallenge: async (_state, renew) => renew(),
			statePath,
		});
		await new Promise((resolve) => setTimeout(resolve, 5));
		stop();
		expect(renewedReleasedLease).to.be.false;
	});

	it("reuses client-generated lease identity after a lost claim response", async () => {
		let firstBody: Record<string, string> | undefined;
		const lostResponse: DnsLeaseFetch = async (_url, init) => {
			firstBody = JSON.parse(init.body as string);
			throw new Error("response was lost");
		};
		await expect(
			provisionDnsLease({
				accessToken: ACCESS_TOKEN,
				address: ADDRESS,
				request: lostResponse,
				serviceUrl: SERVICE_URL,
				statePath,
			}),
		).rejectedWith("DNS lease claim request failed");
		const claiming = readDnsLeaseState(statePath)!;
		expect(claiming.status).equal("claiming");

		let retriedBody: Record<string, string> | undefined;
		const retry: DnsLeaseFetch = async (url, init) => {
			if (actionFromUrl(url) === "claim") {
				retriedBody = JSON.parse(init.body as string);
				return jsonResponse(pendingResponse());
			}
			return jsonResponse(activeResponse());
		};
		await provisionDnsLease({
			accessToken: ACCESS_TOKEN,
			request: retry,
			serveChallenge: async (_state, verify) => verify(),
			statePath,
		});
		expect(retriedBody).deep.equal(firstBody);
	});

	it("does not create claiming state without a valid access token", async () => {
		await expect(
			provisionDnsLease({
				address: ADDRESS,
				serviceUrl: SERVICE_URL,
				statePath,
			}),
		).rejectedWith("access token is required");
		expect(fs.existsSync(statePath)).to.be.false;
	});

	it("preserves a pending lease after a lost verify response", async () => {
		const actions: Array<string | undefined> = [];
		let verifyAttempts = 0;
		const request: DnsLeaseFetch = async (url) => {
			const action = actionFromUrl(url);
			actions.push(action);
			if (action === "claim") {
				return jsonResponse(pendingResponse());
			}
			if (action === "verify" && verifyAttempts++ === 0) {
				throw new Error("response was lost");
			}
			return jsonResponse(activeResponse());
		};
		await expect(
			provisionDnsLease({
				accessToken: ACCESS_TOKEN,
				address: ADDRESS,
				request,
				serveChallenge: async (_state, verify) => verify(),
				serviceUrl: SERVICE_URL,
				statePath,
			}),
		).rejectedWith("DNS lease verify request failed");
		expect(actions).deep.equal(["claim", "verify"]);
		expect(readDnsLeaseState(statePath)?.status).equal("pending");
		await provisionDnsLease({
			request,
			serveChallenge: async (_state, verify) => verify(),
			statePath,
		});
		expect(actions).deep.equal(["claim", "verify", "verify"]);
		expect(readDnsLeaseState(statePath)?.status).equal("active");
	});

	it("preserves an active lease when local configuration fails", async () => {
		const actions: Array<string | undefined> = [];
		const request: DnsLeaseFetch = async (url) => {
			const action = actionFromUrl(url);
			actions.push(action);
			if (action === "claim") {
				return jsonResponse(pendingResponse());
			}
			if (action === "verify") {
				return jsonResponse(activeResponse());
			}
			return jsonResponse({ id: LEASE_ID, status: "released" });
		};
		await expect(
			provisionDnsLease({
				accessToken: ACCESS_TOKEN,
				address: ADDRESS,
				configure: async () => {
					throw new Error("certbot failed");
				},
				request,
				serveChallenge: async (_state, verify) => verify(),
				serviceUrl: SERVICE_URL,
				statePath,
			}),
		).rejectedWith("certbot failed");
		expect(actions).deep.equal(["claim", "verify"]);
		expect(readDnsLeaseState(statePath)?.status).equal("active");
		let configured = false;
		await provisionDnsLease({
			configure: async () => {
				configured = true;
			},
			request,
			statePath,
		});
		expect(configured).to.be.true;
		expect(actions).deep.equal(["claim", "verify"]);
	});

	it("renews configured leases immediately when node renewal starts", async () => {
		const request: DnsLeaseFetch = async (url) => {
			const action = actionFromUrl(url);
			if (action === "claim") {
				return jsonResponse(pendingResponse());
			}
			return jsonResponse(activeResponse());
		};
		await provisionDnsLease({
			accessToken: ACCESS_TOKEN,
			address: ADDRESS,
			configure: async () => undefined,
			request,
			serveChallenge: async (_state, verify) => verify(),
			serviceUrl: SERVICE_URL,
			statePath,
		});
		let renewals = 0;
		const renewalActions: Array<string | undefined> = [];
		const stop = await startDnsLeaseRenewal({
			request: async (url) => {
				const action = actionFromUrl(url);
				renewalActions.push(action);
				if (action === "renew-challenge") {
					return jsonResponse(renewChallengeResponse());
				}
				expect(action).equal("renew");
				renewals += 1;
				return jsonResponse(activeResponse("2099-01-03T00:00:00.000Z"));
			},
			serveChallenge: async (state, renew, listen) => {
				expect(state.challengeToken).equal(RENEW_CHALLENGE_TOKEN);
				expect(listen?.preferManagedProxy).equal(true);
				return renew();
			},
			statePath,
		});
		for (let attempt = 0; renewals === 0 && attempt < 50; attempt++) {
			await new Promise((resolve) => setTimeout(resolve, 2));
		}
		stop();
		expect(renewals).equal(1);
		expect(renewalActions).deep.equal(["renew-challenge", "renew"]);
		expect(readDnsLeaseState(statePath)?.expiresAt).equal(
			"2099-01-03T00:00:00.000Z",
		);
	});

	it("redacts response bodies, URLs, and bearer tokens from failures", async () => {
		const secret = "do_not_echo_this_response";
		const client = new DnsLeaseClient({
			request: async () => jsonResponse({ error: secret }, 403),
			serviceUrl: SERVICE_URL,
		});
		let message = "";
		try {
			await client.claim(
				{
					address: ADDRESS,
					idempotencyKey: "idempotency_1234",
					leaseToken: "b".repeat(43),
					recordType: "A",
				},
				ACCESS_TOKEN,
			);
		} catch (error: any) {
			message = error.message;
		}
		expect(message).equal("DNS lease claim failed with HTTP 403");
		expect(message).not.to.include(secret);
		expect(message).not.to.include(ACCESS_TOKEN);
		expect(message).not.to.include("example.supabase.co");
	});

	it("stops reading oversized streamed responses", async () => {
		const body = new ReadableStream<Uint8Array>({
			start(controller) {
				controller.enqueue(new Uint8Array(64 * 1024));
				controller.enqueue(new Uint8Array(1));
				controller.close();
			},
		});
		const client = new DnsLeaseClient({
			request: async () => ({
				body,
				status: 200,
				text: async () => {
					throw new Error("streaming should be used");
				},
			}),
			serviceUrl: SERVICE_URL,
		});
		await expect(
			client.claim(
				{
					address: ADDRESS,
					idempotencyKey: "idempotency_1234",
					leaseToken: "b".repeat(43),
					recordType: "A",
				},
				ACCESS_TOKEN,
			),
		).rejectedWith("response is too large");
	});

	it("rejects insecure service URLs and broadly readable state", async () => {
		expect(
			() => new DnsLeaseClient({ serviceUrl: "http://example.com/dns-lease" }),
		).to.throw("must use HTTPS");
		fs.mkdirSync(path.dirname(statePath), { recursive: true });
		fs.writeFileSync(statePath, "{}", { mode: 0o644 });
		expect(() => readDnsLeaseState(statePath)).to.throw(
			"permissions are too broad",
		);
	});

	it("rejects malformed persisted expiry and a symlinked lock target", async () => {
		const request: DnsLeaseFetch = async (url) => {
			if (actionFromUrl(url) === "claim") {
				return jsonResponse(pendingResponse());
			}
			return jsonResponse(activeResponse());
		};
		await provisionDnsLease({
			accessToken: ACCESS_TOKEN,
			address: ADDRESS,
			request,
			serveChallenge: async (_state, verify) => verify(),
			serviceUrl: SERVICE_URL,
			statePath,
		});

		const serialized = JSON.parse(fs.readFileSync(statePath, "utf8"));
		serialized.expiresAt = "not-a-timestamp";
		fs.writeFileSync(statePath, JSON.stringify(serialized), { mode: 0o600 });
		expect(() => readDnsLeaseState(statePath)).to.throw("invalid expiresAt");

		fs.rmSync(`${statePath}.lock-target`, { force: true });
		fs.symlinkSync(statePath, `${statePath}.lock-target`);
		await expect(renewDnsLease({ request, statePath })).rejectedWith(
			"lock target must be a regular file",
		);
	});

	it("serves only the exact direct-IP challenge route", async () => {
		const reservation = net.createServer();
		await new Promise<void>((resolve) =>
			reservation.listen(0, "127.0.0.1", resolve),
		);
		const port = (reservation.address() as net.AddressInfo).port;
		await new Promise<void>((resolve) => reservation.close(() => resolve()));
		const now = new Date().toISOString();
		const pending: DnsLeaseState = {
			version: 1,
			serviceUrl: SERVICE_URL,
			idempotencyKey: "idempotency_1234",
			recordType: "A",
			address: "127.0.0.1",
			leaseToken: "a".repeat(43),
			challengeToken: "b".repeat(43),
			id: LEASE_ID,
			domain: DOMAIN,
			status: "pending",
			challengeUrl: `http://127.0.0.1/.well-known/peerbit-dns/${LEASE_ID}`,
			challengeExpiresAt: CHALLENGE_EXPIRY,
			pendingExpiresAt: PENDING_EXPIRY,
			createdAt: now,
			updatedAt: now,
		};
		await serveDnsLeaseChallenge(
			pending,
			async () => {
				const response = await fetch(
					`http://127.0.0.1:${port}/.well-known/peerbit-dns/${LEASE_ID}`,
				);
				expect(response.status).equal(200);
				expect(await response.text()).equal(pending.challengeToken);
				const missing = await fetch(`http://127.0.0.1:${port}/other`);
				expect(missing.status).equal(404);
			},
			{ host: "127.0.0.1", port },
		);
	});

	it("uses the configured loopback proxy without inspecting Docker", async () => {
		const now = new Date().toISOString();
		const pending: DnsLeaseState = {
			version: 1,
			serviceUrl: SERVICE_URL,
			idempotencyKey: "idempotency_1234",
			recordType: "A",
			address: "127.0.0.1",
			leaseToken: "a".repeat(43),
			challengeToken: "b".repeat(43),
			id: LEASE_ID,
			domain: DOMAIN,
			status: "pending",
			challengeUrl: `http://127.0.0.1/.well-known/peerbit-dns/${LEASE_ID}`,
			challengeExpiresAt: CHALLENGE_EXPIRY,
			pendingExpiresAt: PENDING_EXPIRY,
			createdAt: now,
			updatedAt: now,
		};
		let inspected = false;
		await serveDnsLeaseChallenge(
			pending,
			async () => {
				const response = await fetch(
					`http://127.0.0.1:8093/.well-known/peerbit-dns/${LEASE_ID}`,
				);
				expect(response.status).equal(200);
				expect(await response.text()).equal(pending.challengeToken);
			},
			{
				dockerExecute: async () => {
					inspected = true;
					throw new Error("Docker must not be inspected");
				},
				preferManagedProxy: true,
			},
		);
		expect(inspected).equal(false);
	});

	it("uses the managed NGINX loopback proxy without stopping it", async () => {
		const configDirectory = path.join(root, "nginx");
		fs.mkdirSync(configDirectory, { recursive: true });
		fs.writeFileSync(
			path.join(configDirectory, "default.conf"),
			"location ^~ /.well-known/peerbit-dns/ { proxy_pass http://127.0.0.1:8093; }",
		);
		const execute = async (args: readonly string[]) => {
			expect(args.slice(0, 2)).deep.equal(["container", "inspect"]);
			return {
				stderr: "",
				stdout: JSON.stringify([
					{
						Config: {
							Labels: { "org.peerbit.managed": "nginx-certbot" },
						},
						Id: "managed-container-id",
						Mounts: [
							{ Destination: "/etc/letsencrypt", Source: root },
							{
								Destination: "/etc/nginx/user_conf.d",
								Source: configDirectory,
							},
							{ Destination: "/usr/share/nginx/html", Source: root },
						],
						Name: "/nginx-certbot",
						State: { Running: true },
					},
				]),
			};
		};
		const now = new Date().toISOString();
		const pending: DnsLeaseState = {
			version: 1,
			serviceUrl: SERVICE_URL,
			idempotencyKey: "idempotency_1234",
			recordType: "A",
			address: "127.0.0.1",
			leaseToken: "a".repeat(43),
			challengeToken: "b".repeat(43),
			id: LEASE_ID,
			domain: DOMAIN,
			status: "pending",
			challengeUrl: `http://127.0.0.1/.well-known/peerbit-dns/${LEASE_ID}`,
			challengeExpiresAt: CHALLENGE_EXPIRY,
			pendingExpiresAt: PENDING_EXPIRY,
			createdAt: now,
			updatedAt: now,
		};
		await serveDnsLeaseChallenge(
			pending,
			async () => {
				const response = await fetch(
					`http://127.0.0.1:8093/.well-known/peerbit-dns/${LEASE_ID}`,
				);
				expect(response.status).equal(200);
				expect(await response.text()).equal(pending.challengeToken);
			},
			{ dockerExecute: execute },
		);
	});

	it("fails safely when the challenge port is already in use", async () => {
		const occupied = net.createServer();
		await new Promise<void>((resolve) =>
			occupied.listen(0, "127.0.0.1", resolve),
		);
		const port = (occupied.address() as net.AddressInfo).port;
		const now = new Date().toISOString();
		const pending: DnsLeaseState = {
			version: 1,
			serviceUrl: SERVICE_URL,
			idempotencyKey: "idempotency_1234",
			recordType: "A",
			address: "127.0.0.1",
			leaseToken: "a".repeat(43),
			challengeToken: "b".repeat(43),
			id: LEASE_ID,
			domain: DOMAIN,
			status: "pending",
			challengeUrl: `http://127.0.0.1/.well-known/peerbit-dns/${LEASE_ID}`,
			challengeExpiresAt: CHALLENGE_EXPIRY,
			pendingExpiresAt: PENDING_EXPIRY,
			createdAt: now,
			updatedAt: now,
		};
		try {
			await expect(
				serveDnsLeaseChallenge(pending, async () => undefined, {
					host: "127.0.0.1",
					port,
				}),
			).rejectedWith(`needs port ${port}`);
		} finally {
			await new Promise<void>((resolve) => occupied.close(() => resolve()));
		}
	});
});
