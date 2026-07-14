import { randomBytes, randomUUID } from "crypto";
import fs from "fs";
import http from "http";
import { isIP } from "net";
import path from "path";
import type { DockerExecutor } from "./docker.js";
import { isPeerbitCertbotContainer, normalizeDomain } from "./domain.js";

export const DNS_LEASE_STATE_VERSION = 1 as const;
export const DNS_LEASE_ACCESS_TOKEN_ENV = "PEERBIT_DNS_LEASE_ACCESS_TOKEN";
export const DNS_LEASE_SERVICE_URL_ENV = "PEERBIT_DNS_LEASE_SERVICE_URL";
export const DNS_LEASE_STATE_FILE_ENV = "PEERBIT_DNS_LEASE_STATE_FILE";

const MAX_RESPONSE_BYTES = 64 * 1024;
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const TOKEN_PATTERN = /^[A-Za-z0-9._~-]{32,512}$/;
const GENERATED_TOKEN_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const LEASE_ID_PATTERN = /^[A-Za-z0-9_-]{8,128}$/;
const MANAGED_DOMAIN_PATTERN = /^p-[a-f0-9]{20}\.nodes\.peerchecker\.com$/;
const DNS_LEASE_CHALLENGE_PROXY_PORT = 8093;
const DNS_LEASE_CHALLENGE_PROXY_MARKER = `proxy_pass http://127.0.0.1:${DNS_LEASE_CHALLENGE_PROXY_PORT};`;

export type DnsLeaseRecordType = "A" | "AAAA";
export type DnsLeaseStatus = "claiming" | "pending" | "active" | "released";

export interface DnsLeaseState {
	version: typeof DNS_LEASE_STATE_VERSION;
	serviceUrl: string;
	idempotencyKey: string;
	recordType: DnsLeaseRecordType;
	address: string;
	leaseToken?: string;
	challengeToken?: string;
	id?: string;
	domain?: string;
	status: DnsLeaseStatus;
	challengeUrl?: string;
	challengeExpiresAt?: string;
	pendingExpiresAt?: string;
	expiresAt?: string;
	createdAt: string;
	updatedAt: string;
	configuredAt?: string;
}

export interface DnsLeaseClaimResponse {
	id: string;
	domain: string;
	recordType: DnsLeaseRecordType;
	address: string;
	status: "pending";
	challengeToken: string;
	challengeUrl: string;
	challengeExpiresAt: string;
	pendingExpiresAt: string;
}

export interface DnsLeaseActiveResponse {
	id: string;
	domain: string;
	recordType: DnsLeaseRecordType;
	address: string;
	status: "active";
	pendingExpiresAt?: string;
	expiresAt: string;
}

export interface DnsLeaseRenewChallengeResponse extends DnsLeaseActiveResponse {
	challengeToken: string;
	challengeUrl: string;
	challengeExpiresAt: string;
}

export interface DnsLeaseReleaseResponse {
	id: string;
	status: "expired" | "released";
}

type DnsLeaseAction =
	| "claim"
	| "verify"
	| "renew-challenge"
	| "renew"
	| "release";

export type DnsLeaseFetch = (
	url: string,
	init: RequestInit,
) => Promise<{
	body?: ReadableStream<Uint8Array> | null;
	headers?: { get(name: string): string | null };
	status: number;
	text: () => Promise<string>;
}>;

interface DnsLeaseAuthorization {
	apply(headers: Record<string, string>): void;
}

class BearerAuthorization implements DnsLeaseAuthorization {
	constructor(private readonly token: string) {
		if (!TOKEN_PATTERN.test(token)) {
			throw new Error("DNS lease authorization token is invalid");
		}
	}

	apply(headers: Record<string, string>) {
		headers.Authorization = `Bearer ${this.token}`;
	}
}

const normalizeServiceUrl = (input: string): string => {
	let url: URL;
	try {
		url = new URL(input);
	} catch {
		throw new Error("DNS lease service URL is invalid");
	}
	if (url.username || url.password || url.search || url.hash) {
		throw new Error(
			"DNS lease service URL must not contain credentials or query data",
		);
	}
	const localHttp =
		url.protocol === "http:" &&
		["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
	if (url.protocol !== "https:" && !localHttp) {
		throw new Error("DNS lease service URL must use HTTPS");
	}
	url.pathname = url.pathname.replace(/\/+$/, "");
	return `${url.origin}${url.pathname}`;
};

const normalizeAddress = (
	address: string,
	recordType?: DnsLeaseRecordType,
): { address: string; recordType: DnsLeaseRecordType } => {
	const normalized = address.trim();
	const version = isIP(normalized);
	if (version === 0) throw new Error("DNS lease address must be an IP address");
	const inferred = version === 4 ? "A" : "AAAA";
	if (recordType && recordType !== inferred) {
		throw new Error(
			`DNS lease ${recordType} record does not match its address`,
		);
	}
	const canonicalAddress =
		version === 6
			? new URL(`http://[${normalized}]/`).hostname.replace(/^\[|\]$/g, "")
			: normalized;
	return { address: canonicalAddress, recordType: inferred };
};

const requireString = (
	value: unknown,
	label: string,
	pattern?: RegExp,
): string => {
	if (typeof value !== "string" || (pattern && !pattern.test(value))) {
		throw new Error(`DNS lease service returned an invalid ${label}`);
	}
	return value;
};

const requireTimestamp = (value: unknown, label: string): string => {
	const timestamp = requireString(value, label);
	if (!Number.isFinite(Date.parse(timestamp))) {
		throw new Error(`DNS lease service returned an invalid ${label}`);
	}
	return timestamp;
};

const requireStoredTimestamp = (value: unknown, label: string): string => {
	if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) {
		throw new Error(`DNS lease state has an invalid ${label}`);
	}
	return value;
};

const parseLeaseBase = (
	value: unknown,
): {
	response: Record<string, unknown>;
	id: string;
	domain: string;
	recordType: DnsLeaseRecordType;
	address: string;
} => {
	if (!value || typeof value !== "object" || Array.isArray(value)) {
		throw new Error("DNS lease service returned an invalid response");
	}
	const response = value as Record<string, unknown>;
	const rawRecordType = requireString(response.recordType, "record type");
	if (rawRecordType !== "A" && rawRecordType !== "AAAA") {
		throw new Error("DNS lease service returned an invalid record type");
	}
	const recordType: DnsLeaseRecordType = rawRecordType;
	const { address } = normalizeAddress(
		requireString(response.address, "address"),
		recordType,
	);
	let domain: string;
	try {
		domain = normalizeDomain(requireString(response.domain, "domain"));
	} catch {
		throw new Error("DNS lease service returned an invalid domain");
	}
	if (!MANAGED_DOMAIN_PATTERN.test(domain)) {
		throw new Error(
			"DNS lease service returned a domain outside the managed zone",
		);
	}
	return {
		response,
		id: requireString(response.id, "lease id", LEASE_ID_PATTERN),
		domain,
		recordType,
		address,
	};
};

const parseClaimResponse = (value: unknown): DnsLeaseClaimResponse => {
	const parsed = parseLeaseBase(value);
	if (parsed.response.status !== "pending") {
		throw new Error("DNS lease service returned an invalid claim status");
	}
	return {
		id: parsed.id,
		domain: parsed.domain,
		recordType: parsed.recordType,
		address: parsed.address,
		status: "pending",
		challengeToken: requireString(
			parsed.response.challengeToken,
			"challenge token",
			GENERATED_TOKEN_PATTERN,
		),
		challengeUrl: requireString(parsed.response.challengeUrl, "challenge URL"),
		challengeExpiresAt: requireTimestamp(
			parsed.response.challengeExpiresAt,
			"challenge expiry",
		),
		pendingExpiresAt: requireTimestamp(
			parsed.response.pendingExpiresAt,
			"pending expiry",
		),
	};
};

const parseRenewChallengeResponse = (
	value: unknown,
): DnsLeaseRenewChallengeResponse => {
	const active = parseActiveResponse(value);
	const response = value as Record<string, unknown>;
	return {
		...active,
		challengeToken: requireString(
			response.challengeToken,
			"challenge token",
			GENERATED_TOKEN_PATTERN,
		),
		challengeUrl: requireString(response.challengeUrl, "challenge URL"),
		challengeExpiresAt: requireTimestamp(
			response.challengeExpiresAt,
			"challenge expiry",
		),
	};
};

const parseActiveResponse = (value: unknown): DnsLeaseActiveResponse => {
	const parsed = parseLeaseBase(value);
	if (parsed.response.status !== "active") {
		throw new Error("DNS lease service returned an invalid active status");
	}
	return {
		id: parsed.id,
		domain: parsed.domain,
		recordType: parsed.recordType,
		address: parsed.address,
		status: "active",
		pendingExpiresAt:
			typeof parsed.response.pendingExpiresAt === "string"
				? requireTimestamp(parsed.response.pendingExpiresAt, "pending expiry")
				: undefined,
		expiresAt: requireTimestamp(parsed.response.expiresAt, "lease expiry"),
	};
};

const parseReleaseResponse = (value: unknown): DnsLeaseReleaseResponse => {
	if (!value || typeof value !== "object" || Array.isArray(value)) {
		throw new Error("DNS lease service returned an invalid response");
	}
	const response = value as Record<string, unknown>;
	if (response.status !== "released" && response.status !== "expired") {
		throw new Error("DNS lease service returned an invalid release status");
	}
	return {
		id: requireString(response.id, "lease id", LEASE_ID_PATTERN),
		status: response.status,
	};
};

export class DnsLeaseClient {
	readonly serviceUrl: string;
	private readonly request: DnsLeaseFetch;
	private readonly timeoutMs: number;

	constructor(properties: {
		serviceUrl: string;
		request?: DnsLeaseFetch;
		timeoutMs?: number;
	}) {
		this.serviceUrl = normalizeServiceUrl(properties.serviceUrl);
		this.request = properties.request || ((url, init) => fetch(url, init));
		this.timeoutMs = properties.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
		if (!Number.isFinite(this.timeoutMs) || this.timeoutMs <= 0) {
			throw new Error("DNS lease request timeout must be positive");
		}
	}

	private async post(
		action: DnsLeaseAction,
		body: Record<string, string>,
		authorization: DnsLeaseAuthorization,
	): Promise<unknown> {
		const url = `${this.serviceUrl}/${action}`;
		const headers: Record<string, string> = {
			Accept: "application/json",
			"Content-Type": "application/json",
		};
		authorization.apply(headers);
		const controller = new AbortController();
		let timeout: ReturnType<typeof setTimeout> | undefined;
		try {
			const timeoutPromise = new Promise<never>((_resolve, reject) => {
				timeout = setTimeout(() => {
					controller.abort();
					reject(new Error("timeout"));
				}, this.timeoutMs);
			});
			const response = await Promise.race([
				this.request(url, {
					body: JSON.stringify(body),
					headers,
					method: "POST",
					redirect: "error",
					signal: controller.signal,
				}),
				timeoutPromise,
			]);
			const declaredLength = Number(response.headers?.get("content-length"));
			if (
				Number.isFinite(declaredLength) &&
				declaredLength > MAX_RESPONSE_BYTES
			) {
				throw new Error(`DNS lease ${action} response is too large`);
			}
			let text: string;
			if (response.body?.getReader) {
				const reader = response.body.getReader();
				const chunks: Uint8Array[] = [];
				let size = 0;
				try {
					while (true) {
						const result = await Promise.race([reader.read(), timeoutPromise]);
						if (result.done) break;
						size += result.value.byteLength;
						if (size > MAX_RESPONSE_BYTES) {
							throw new Error(`DNS lease ${action} response is too large`);
						}
						chunks.push(result.value);
					}
				} finally {
					reader.releaseLock();
				}
				text = Buffer.concat(chunks).toString("utf8");
			} else {
				text = await Promise.race([response.text(), timeoutPromise]);
			}
			if (response.status < 200 || response.status >= 300) {
				throw new Error(
					`DNS lease ${action} failed with HTTP ${response.status}`,
				);
			}
			if (Buffer.byteLength(text) > MAX_RESPONSE_BYTES) {
				throw new Error(`DNS lease ${action} response is too large`);
			}
			try {
				return JSON.parse(text);
			} catch {
				throw new Error(`DNS lease ${action} returned invalid JSON`);
			}
		} catch (error: any) {
			if (error?.message?.startsWith("DNS lease ")) throw error;
			if (controller.signal.aborted || error?.message === "timeout") {
				throw new Error(`DNS lease ${action} timed out`);
			}
			throw new Error(`DNS lease ${action} request failed`);
		} finally {
			if (timeout) clearTimeout(timeout);
			controller.abort();
		}
	}

	async claim(
		request: {
			idempotencyKey: string;
			recordType: DnsLeaseRecordType;
			address: string;
			leaseToken: string;
		},
		accessToken: string,
	): Promise<DnsLeaseClaimResponse> {
		return parseClaimResponse(
			await this.post("claim", request, new BearerAuthorization(accessToken)),
		);
	}

	async verify(
		id: string,
		leaseToken: string,
	): Promise<DnsLeaseActiveResponse> {
		return parseActiveResponse(
			await this.post("verify", { id }, new BearerAuthorization(leaseToken)),
		);
	}

	async renew(id: string, leaseToken: string): Promise<DnsLeaseActiveResponse> {
		return parseActiveResponse(
			await this.post("renew", { id }, new BearerAuthorization(leaseToken)),
		);
	}

	async renewChallenge(
		id: string,
		leaseToken: string,
	): Promise<DnsLeaseRenewChallengeResponse> {
		return parseRenewChallengeResponse(
			await this.post(
				"renew-challenge",
				{ id },
				new BearerAuthorization(leaseToken),
			),
		);
	}

	async release(
		id: string,
		leaseToken: string,
	): Promise<DnsLeaseReleaseResponse> {
		return parseReleaseResponse(
			await this.post("release", { id }, new BearerAuthorization(leaseToken)),
		);
	}
}

export const getDnsLeaseStatePath = (cwd = process.cwd()) =>
	path.join(cwd, ".peerbit-domain", "lease.json");

const validateStoredState = (value: unknown): DnsLeaseState => {
	if (!value || typeof value !== "object" || Array.isArray(value)) {
		throw new Error("DNS lease state is invalid");
	}
	const state = value as Record<string, unknown>;
	if (state.version !== DNS_LEASE_STATE_VERSION) {
		throw new Error("DNS lease state version is unsupported");
	}
	if (
		state.status !== "claiming" &&
		state.status !== "pending" &&
		state.status !== "active" &&
		state.status !== "released"
	) {
		throw new Error("DNS lease state has an invalid status");
	}
	const normalized = normalizeAddress(
		requireString(state.address, "stored address"),
		state.recordType as DnsLeaseRecordType,
	);
	const stored: DnsLeaseState = {
		version: DNS_LEASE_STATE_VERSION,
		serviceUrl: normalizeServiceUrl(
			requireString(state.serviceUrl, "stored service URL"),
		),
		idempotencyKey: requireString(
			state.idempotencyKey,
			"stored idempotency key",
			LEASE_ID_PATTERN,
		),
		recordType: normalized.recordType,
		address: normalized.address,
		status: state.status,
		createdAt: requireStoredTimestamp(state.createdAt, "creation time"),
		updatedAt: requireStoredTimestamp(state.updatedAt, "update time"),
	};
	for (const key of [
		"leaseToken",
		"challengeToken",
		"id",
		"domain",
		"challengeUrl",
		"challengeExpiresAt",
		"pendingExpiresAt",
		"expiresAt",
		"configuredAt",
	] as const) {
		if (typeof state[key] === "string") stored[key] = state[key] as never;
	}
	if (stored.domain) stored.domain = normalizeDomain(stored.domain);
	if (stored.id) requireString(stored.id, "stored lease id", LEASE_ID_PATTERN);
	for (const key of [
		"challengeExpiresAt",
		"pendingExpiresAt",
		"expiresAt",
		"configuredAt",
	] as const) {
		if (stored[key]) requireStoredTimestamp(stored[key], key);
	}
	if (stored.status !== "released") {
		requireString(
			stored.leaseToken,
			"stored lease token",
			GENERATED_TOKEN_PATTERN,
		);
	}
	if (stored.status === "pending") {
		requireString(
			stored.challengeToken,
			"stored challenge token",
			GENERATED_TOKEN_PATTERN,
		);
	}
	if (
		(stored.status === "pending" || stored.status === "active") &&
		!stored.id
	) {
		throw new Error("DNS lease state is missing its lease id");
	}
	if (
		stored.status === "pending" &&
		(!stored.challengeUrl || !stored.challengeExpiresAt)
	) {
		throw new Error("DNS lease state is missing challenge data");
	}
	if (stored.status === "active" && (!stored.domain || !stored.expiresAt)) {
		throw new Error("DNS lease state is missing active lease data");
	}
	return stored;
};

export const readDnsLeaseState = (statePath = getDnsLeaseStatePath()) => {
	if (!fs.existsSync(statePath)) return undefined;
	const stat = fs.lstatSync(statePath);
	if (!stat.isFile() || stat.isSymbolicLink()) {
		throw new Error("DNS lease state must be a regular file");
	}
	if (process.platform !== "win32" && (stat.mode & 0o077) !== 0) {
		throw new Error(
			"DNS lease state permissions are too broad; expected mode 0600",
		);
	}
	try {
		return validateStoredState(JSON.parse(fs.readFileSync(statePath, "utf8")));
	} catch (error: any) {
		if (error?.message?.startsWith("DNS lease state")) throw error;
		throw new Error("DNS lease state is invalid");
	}
};

export const writeDnsLeaseState = (
	state: DnsLeaseState,
	statePath = getDnsLeaseStatePath(),
) => {
	const validated = validateStoredState(state);
	const parent = path.dirname(statePath);
	fs.mkdirSync(parent, { recursive: true, mode: 0o700 });
	const temporary = path.join(
		parent,
		`.${path.basename(statePath)}.${process.pid}.${randomUUID()}.tmp`,
	);
	let descriptor: number | undefined;
	try {
		descriptor = fs.openSync(temporary, "wx", 0o600);
		fs.writeFileSync(
			descriptor,
			`${JSON.stringify(validated, undefined, 2)}\n`,
		);
		fs.fsyncSync(descriptor);
		fs.closeSync(descriptor);
		descriptor = undefined;
		fs.renameSync(temporary, statePath);
		fs.chmodSync(statePath, 0o600);
	} finally {
		if (descriptor !== undefined) fs.closeSync(descriptor);
		fs.rmSync(temporary, { force: true });
	}
};

const withStateLock = async <T>(
	statePath: string,
	fn: () => Promise<T>,
): Promise<T> => {
	fs.mkdirSync(path.dirname(statePath), { recursive: true, mode: 0o700 });
	const lockTarget = `${statePath}.lock-target`;
	try {
		fs.closeSync(fs.openSync(lockTarget, "wx", 0o600));
	} catch (error: any) {
		if (error?.code !== "EEXIST") throw error;
	}
	const lockStat = fs.lstatSync(lockTarget);
	if (!lockStat.isFile() || lockStat.isSymbolicLink()) {
		throw new Error("DNS lease lock target must be a regular file");
	}
	if (process.platform !== "win32" && (lockStat.mode & 0o077) !== 0) {
		throw new Error("DNS lease lock target permissions are too broad");
	}
	const { createRequire } = await import("module");
	const properLockfile = createRequire(import.meta.url)("proper-lockfile") as {
		lock: (
			file: string,
			options: { realpath: boolean; stale: number; update: number },
		) => Promise<() => Promise<void>>;
	};
	let release: (() => Promise<void>) | undefined;
	try {
		release = await properLockfile.lock(lockTarget, {
			realpath: false,
			stale: 30_000,
			update: 5_000,
		});
	} catch (error: any) {
		if (error?.code === "ELOCKED") {
			throw new Error("Another DNS lease operation is already running");
		}
		throw error;
	}
	try {
		return await fn();
	} finally {
		await release();
	}
};

const assertLeaseMatches = (
	state: DnsLeaseState,
	lease: DnsLeaseClaimResponse | DnsLeaseActiveResponse,
) => {
	if (
		(state.id && state.id !== lease.id) ||
		state.address !== lease.address ||
		state.recordType !== lease.recordType ||
		(state.domain && state.domain !== lease.domain)
	) {
		throw new Error("DNS lease service returned conflicting lease data");
	}
};

const toActiveResponse = (state: DnsLeaseState): DnsLeaseActiveResponse => ({
	id: state.id!,
	domain: state.domain!,
	recordType: state.recordType,
	address: state.address,
	status: "active",
	pendingExpiresAt: state.pendingExpiresAt,
	expiresAt: state.expiresAt!,
});

const markReleased = (state: DnsLeaseState): DnsLeaseState => {
	const released = {
		...state,
		status: "released" as const,
		updatedAt: new Date().toISOString(),
	};
	delete released.leaseToken;
	delete released.challengeToken;
	delete released.challengeUrl;
	delete released.challengeExpiresAt;
	return released;
};

const validateChallengeUrl = (state: DnsLeaseState): URL => {
	let challengeUrl: URL;
	try {
		challengeUrl = new URL(state.challengeUrl!);
	} catch {
		throw new Error("DNS lease service returned an invalid challenge URL");
	}
	const hostname = challengeUrl.hostname.replace(/^\[|\]$/g, "");
	if (
		challengeUrl.protocol !== "http:" ||
		(challengeUrl.port && challengeUrl.port !== "80") ||
		challengeUrl.username ||
		challengeUrl.password ||
		challengeUrl.search ||
		challengeUrl.hash ||
		hostname.toLowerCase() !== state.address.toLowerCase() ||
		challengeUrl.pathname !== `/.well-known/peerbit-dns/${state.id}`
	) {
		throw new Error("DNS lease service returned an unsafe challenge URL");
	}
	return challengeUrl;
};

const managedChallengeProxyIsAvailable = async (
	execute?: DockerExecutor,
): Promise<boolean> => {
	try {
		const { inspectDockerContainer } = await import("./docker.js");
		const inspection = await inspectDockerContainer("nginx-certbot", execute);
		if (
			!inspection ||
			inspection.State?.Running !== true ||
			!isPeerbitCertbotContainer(inspection)
		) {
			return false;
		}
		const source = inspection.Mounts?.find(
			(mount) => mount.Destination === "/etc/nginx/user_conf.d",
		)?.Source;
		if (!source) return false;
		const sourceStat = fs.lstatSync(source);
		if (!sourceStat.isDirectory() || sourceStat.isSymbolicLink()) return false;
		const configPath = path.join(source, "default.conf");
		const configStat = fs.lstatSync(configPath);
		if (
			!configStat.isFile() ||
			configStat.isSymbolicLink() ||
			configStat.size > 1024 * 1024
		) {
			return false;
		}
		return fs
			.readFileSync(configPath, "utf8")
			.includes(DNS_LEASE_CHALLENGE_PROXY_MARKER);
	} catch {
		return false;
	}
};

export const serveDnsLeaseChallenge = async <T>(
	state: DnsLeaseState,
	whileServing: () => Promise<T>,
	listen: {
		dockerExecute?: DockerExecutor;
		host?: string;
		port?: number;
		preferManagedProxy?: boolean;
		quiesceManagedContainer?: boolean;
	} = {},
): Promise<T> => {
	if (listen.port === undefined && listen.preferManagedProxy) {
		return serveDnsLeaseChallenge(state, whileServing, {
			...listen,
			host: "127.0.0.1",
			port: DNS_LEASE_CHALLENGE_PROXY_PORT,
			preferManagedProxy: false,
			quiesceManagedContainer: false,
		});
	}
	if (
		listen.port === undefined &&
		listen.quiesceManagedContainer !== false &&
		(await managedChallengeProxyIsAvailable(listen.dockerExecute))
	) {
		return serveDnsLeaseChallenge(state, whileServing, {
			...listen,
			host: "127.0.0.1",
			port: DNS_LEASE_CHALLENGE_PROXY_PORT,
			preferManagedProxy: false,
			quiesceManagedContainer: false,
		});
	}
	const challengeUrl = validateChallengeUrl(state);
	const expectedPath = challengeUrl.pathname;
	const challengeToken = state.challengeToken!;
	const server = http.createServer((request, response) => {
		let requestPath: string | undefined;
		try {
			requestPath = new URL(request.url || "", "http://localhost").pathname;
		} catch {
			// Handled as not found below.
		}
		if (request.method !== "GET" || requestPath !== expectedPath) {
			response.writeHead(404, { "Cache-Control": "no-store" });
			response.end("Not found");
			return;
		}
		response.writeHead(200, {
			"Cache-Control": "no-store",
			"Content-Length": Buffer.byteLength(challengeToken),
			"Content-Type": "text/plain; charset=utf-8",
			"X-Content-Type-Options": "nosniff",
		});
		response.end(challengeToken);
	});
	server.headersTimeout = 5_000;
	server.requestTimeout = 5_000;
	const listenPort = listen.port ?? 80;
	if (!Number.isInteger(listenPort) || listenPort < 1 || listenPort > 65_535) {
		throw new Error("DNS lease challenge server port is invalid");
	}
	try {
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(
				{
					host: listen.host || (state.recordType === "A" ? "0.0.0.0" : "::"),
					port: listenPort,
				},
				resolve,
			);
		});
	} catch (error: any) {
		if (error?.code === "EACCES") {
			throw new Error(
				`DNS lease verification must bind port ${listenPort}; rerun with permission to use that port`,
			);
		}
		if (error?.code === "EADDRINUSE") {
			if (listenPort === 80 && listen.quiesceManagedContainer !== false) {
				const { withQuiescedDockerContainer } = await import("./docker.js");
				return withQuiescedDockerContainer(
					"nginx-certbot",
					isPeerbitCertbotContainer,
					() =>
						serveDnsLeaseChallenge(state, whileServing, {
							...listen,
							preferManagedProxy: false,
							quiesceManagedContainer: false,
						}),
					{ execute: listen.dockerExecute },
				);
			}
			throw new Error(
				`DNS lease verification needs port ${listenPort}, but another service is using it; if it is the managed nginx-certbot container, stop that container and rerun this command`,
			);
		}
		throw new Error("DNS lease challenge server could not start");
	}
	try {
		return await whileServing();
	} finally {
		server.closeAllConnections?.();
		await new Promise<void>((resolve) => server.close(() => resolve()));
	}
};

export const provisionDnsLease = async (properties: {
	statePath?: string;
	serviceUrl?: string;
	accessToken?: string;
	address?: string;
	recordType?: DnsLeaseRecordType;
	request?: DnsLeaseFetch;
	timeoutMs?: number;
	configure?: (domain: string) => Promise<void>;
	serveChallenge?: typeof serveDnsLeaseChallenge;
}): Promise<DnsLeaseActiveResponse> => {
	const statePath = properties.statePath || getDnsLeaseStatePath();
	return withStateLock(statePath, async () => {
		let state = readDnsLeaseState(statePath);
		if (!state || state.status === "released") {
			if (!properties.serviceUrl) {
				throw new Error("DNS lease service URL is required for a new claim");
			}
			if (!properties.address) {
				throw new Error(
					"DNS lease public IP address is required for a new claim",
				);
			}
			if (!properties.accessToken) {
				throw new Error("DNS lease access token is required for a new claim");
			}
			new BearerAuthorization(properties.accessToken);
			const normalized = normalizeAddress(
				properties.address,
				properties.recordType,
			);
			const now = new Date().toISOString();
			state = {
				version: DNS_LEASE_STATE_VERSION,
				serviceUrl: normalizeServiceUrl(properties.serviceUrl),
				idempotencyKey: randomUUID(),
				recordType: normalized.recordType,
				address: normalized.address,
				leaseToken: randomBytes(32).toString("base64url"),
				status: "claiming",
				createdAt: now,
				updatedAt: now,
			};
			writeDnsLeaseState(state, statePath);
		}

		if (
			properties.serviceUrl &&
			normalizeServiceUrl(properties.serviceUrl) !== state.serviceUrl
		) {
			throw new Error(
				"DNS lease service URL does not match stored lease state",
			);
		}
		if (properties.address && properties.address.trim() !== state.address) {
			throw new Error("DNS lease address does not match stored lease state");
		}
		const client = new DnsLeaseClient({
			serviceUrl: state.serviceUrl,
			request: properties.request,
			timeoutMs: properties.timeoutMs,
		});
		if (state.status === "claiming") {
			if (!properties.accessToken) {
				throw new Error(
					"DNS lease access token is required to finish the claim",
				);
			}
			const claim = await client.claim(
				{
					idempotencyKey: state.idempotencyKey,
					recordType: state.recordType,
					address: state.address,
					leaseToken: state.leaseToken!,
				},
				properties.accessToken,
			);
			assertLeaseMatches(state, claim);
			state = {
				...state,
				...claim,
				updatedAt: new Date().toISOString(),
			};
			writeDnsLeaseState(state, statePath);
		}

		if (state.status === "pending") {
			const active = await (
				properties.serveChallenge || serveDnsLeaseChallenge
			)(state, () => client.verify(state!.id!, state!.leaseToken!));
			assertLeaseMatches(state, active);
			state = {
				...state,
				...active,
				updatedAt: new Date().toISOString(),
			};
			delete state.challengeToken;
			delete state.challengeUrl;
			delete state.challengeExpiresAt;
			writeDnsLeaseState(state, statePath);
		}

		if (state.status !== "active") {
			throw new Error("DNS lease did not become active");
		}
		if (properties.configure && !state.configuredAt) {
			try {
				await properties.configure(state.domain!);
				state = {
					...state,
					configuredAt: new Date().toISOString(),
					updatedAt: new Date().toISOString(),
				};
				writeDnsLeaseState(state, statePath);
			} catch (error) {
				throw error;
			}
		}
		return toActiveResponse(state);
	});
};

export const renewDnsLease = async (properties: {
	statePath?: string;
	serviceUrl?: string;
	request?: DnsLeaseFetch;
	timeoutMs?: number;
	serveChallenge?: typeof serveDnsLeaseChallenge;
}): Promise<DnsLeaseActiveResponse> => {
	const statePath = properties.statePath || getDnsLeaseStatePath();
	return withStateLock(statePath, async () => {
		let state = readDnsLeaseState(statePath);
		if (state?.status !== "active" || !state.id || !state.leaseToken) {
			throw new Error("No active DNS lease is available to renew");
		}
		if (
			properties.serviceUrl &&
			normalizeServiceUrl(properties.serviceUrl) !== state.serviceUrl
		) {
			throw new Error(
				"DNS lease service URL does not match stored lease state",
			);
		}
		const client = new DnsLeaseClient({
			serviceUrl: state.serviceUrl,
			request: properties.request,
			timeoutMs: properties.timeoutMs,
		});
		const challenge = await client.renewChallenge(state.id, state.leaseToken);
		assertLeaseMatches(state, challenge);
		const challengeState: DnsLeaseState = {
			...state,
			...challenge,
		};
		const active = await (properties.serveChallenge || serveDnsLeaseChallenge)(
			challengeState,
			() => client.renew(state!.id!, state!.leaseToken!),
			{ preferManagedProxy: Boolean(state.configuredAt) },
		);
		assertLeaseMatches(state, active);
		state = { ...state, ...active, updatedAt: new Date().toISOString() };
		writeDnsLeaseState(state, statePath);
		return active;
	});
};

export const releaseDnsLease = async (properties: {
	statePath?: string;
	serviceUrl?: string;
	request?: DnsLeaseFetch;
	timeoutMs?: number;
}): Promise<void> => {
	const statePath = properties.statePath || getDnsLeaseStatePath();
	await withStateLock(statePath, async () => {
		const state = readDnsLeaseState(statePath);
		if (!state || state.status === "released") return;
		if (!state.id || !state.leaseToken) {
			throw new Error(
				"DNS lease claim has no server id; retry the claim before releasing it",
			);
		}
		if (
			properties.serviceUrl &&
			normalizeServiceUrl(properties.serviceUrl) !== state.serviceUrl
		) {
			throw new Error(
				"DNS lease service URL does not match stored lease state",
			);
		}
		const client = new DnsLeaseClient({
			serviceUrl: state.serviceUrl,
			request: properties.request,
			timeoutMs: properties.timeoutMs,
		});
		const released = await client.release(state.id, state.leaseToken);
		if (released.id !== state.id) {
			throw new Error("DNS lease service returned conflicting lease data");
		}
		writeDnsLeaseState(markReleased(state), statePath);
	});
};

export const startDnsLeaseRenewal = async (
	properties: {
		statePath?: string;
		request?: DnsLeaseFetch;
		timeoutMs?: number;
		onError?: (message: string) => void;
		serveChallenge?: typeof serveDnsLeaseChallenge;
	} = {},
): Promise<() => void> => {
	const statePath = properties.statePath || getDnsLeaseStatePath();
	const initialState = readDnsLeaseState(statePath);
	if (initialState?.status !== "active" || !initialState.configuredAt) {
		return () => undefined;
	}
	let stopped = false;
	let timer: ReturnType<typeof setTimeout> | undefined;
	const schedule = (delayMs: number) => {
		if (stopped) return;
		timer = setTimeout(run, delayMs);
		timer.unref?.();
	};
	const run = async () => {
		try {
			const active = await renewDnsLease({
				statePath,
				request: properties.request,
				timeoutMs: properties.timeoutMs,
				serveChallenge: properties.serveChallenge,
			});
			const remaining = Date.parse(active.expiresAt) - Date.now();
			const baseDelay = Math.min(
				12 * 60 * 60 * 1000,
				Math.max(5 * 60 * 1000, remaining / 2),
			);
			schedule(baseDelay * (0.9 + Math.random() * 0.2));
		} catch {
			let retryMs = 5 * 60 * 1000;
			try {
				const currentState = readDnsLeaseState(statePath);
				if (currentState?.status !== "active") {
					properties.onError?.(
						"Managed DNS lease renewal stopped because the lease is no longer active",
					);
					return;
				}
				const expiresAt = currentState.expiresAt;
				const remaining = expiresAt ? Date.parse(expiresAt) - Date.now() : 0;
				if (remaining <= 0) {
					properties.onError?.(
						"Managed DNS lease renewal stopped because the stored lease has expired",
					);
					return;
				}
				retryMs = Math.max(5_000, Math.min(retryMs, remaining / 4));
			} catch {
				// Keep the bounded fallback when local state cannot be re-read.
			}
			properties.onError?.(
				"Managed DNS lease renewal failed; a bounded retry has been scheduled",
			);
			schedule(retryMs * (0.9 + Math.random() * 0.2));
		}
	};
	void run();
	return () => {
		stopped = true;
		if (timer) clearTimeout(timer);
	};
};
