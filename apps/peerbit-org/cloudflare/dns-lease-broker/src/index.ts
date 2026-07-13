import {
	type DnsRecordType,
	type ExpectedLeaseRecord,
	MANAGED_ZONE_NAME,
	type ManagedDnsRecord,
	RecordConflictError,
	ValidationError,
	managedComment,
	parseManagedDnsRecord,
	recordMatchesLease,
	requireAllowedTtl,
	requireExactKeys,
	requireLeaseId,
	requireManagedName,
	requireObject,
	requirePublicAddress,
	requireRecordId,
	requireRecordType,
} from "./validation.ts";

const CLOUDFLARE_API_ROOT = "https://api.cloudflare.com/client/v4";
const API_PATHS = new Set([
	"/zone",
	"/records/list",
	"/records/get",
	"/records/create",
	"/records/delete",
]);
const MAX_REQUEST_BYTES = 8 * 1024;
const MAX_UPSTREAM_BYTES = 128 * 1024;
const MAX_LIST_RESULTS = 10;
const OVERALL_DEADLINE_MS = 35_000;
const CLOUDFLARE_CALL_TIMEOUT_MS = 5_000;
const RESPONSE_HEADROOM_MS = 1_000;
const SEQUENCE_OVERHEAD_HEADROOM_MS = 1_000;
const DELETE_SEQUENCE_BUDGET_MS =
	CLOUDFLARE_CALL_TIMEOUT_MS * 2 +
	RESPONSE_HEADROOM_MS +
	SEQUENCE_OVERHEAD_HEADROOM_MS;
const CREATE_MUTATION_BUDGET_MS =
	CLOUDFLARE_CALL_TIMEOUT_MS * 4 +
	RESPONSE_HEADROOM_MS +
	SEQUENCE_OVERHEAD_HEADROOM_MS;
const DELETE_MUTATION_BUDGET_MS =
	CLOUDFLARE_CALL_TIMEOUT_MS + RESPONSE_HEADROOM_MS;

type TimerHandle = ReturnType<typeof setTimeout>;

export interface Env {
	BROKER_SHARED_SECRET: string;
	CLOUDFLARE_API_TOKEN: string;
	CLOUDFLARE_ZONE_ID: string;
}

type FetchFunction = (
	input: RequestInfo | URL,
	init?: RequestInit,
) => Promise<Response>;

type AuditEntry = {
	event: "dns_lease_broker_request";
	requestId: string;
	route: string;
	outcome: "success" | "error";
	status: number;
	durationMs: number;
	name?: string;
	recordId?: string;
	errorCode?: string;
	upstreamStatus?: number;
};

export type BrokerDependencies = {
	fetch: FetchFunction;
	crypto: Crypto;
	now: () => number;
	log: (entry: AuditEntry) => void;
	setTimer: (callback: () => void, delayMs: number) => TimerHandle;
	clearTimer: (handle: TimerHandle) => void;
};

class HttpError extends Error {
	readonly status: number;
	readonly code: string;

	constructor(status: number, code: string, message: string) {
		super(message);
		this.name = "HttpError";
		this.status = status;
		this.code = code;
	}
}

class CloudflareError extends Error {
	readonly upstreamStatus: number;
	readonly timedOut: boolean;

	constructor(upstreamStatus: number, timedOut = false) {
		super("Cloudflare API request failed");
		this.name = "CloudflareError";
		this.upstreamStatus = upstreamStatus;
		this.timedOut = timedOut;
	}
}

class ZoneIdentityError extends Error {
	constructor() {
		super("Configured Cloudflare zone is not peerchecker.com");
		this.name = "ZoneIdentityError";
	}
}

class BrokerDeadlineError extends Error {
	constructor() {
		super("Broker request deadline exceeded");
		this.name = "BrokerDeadlineError";
	}
}

type DeadlineContext = Pick<
	BrokerDependencies,
	"now" | "setTimer" | "clearTimer"
> & {
	deadlineAt: number;
	signal: AbortSignal;
};

type CloudflareEnvelope = {
	success?: unknown;
	result?: unknown;
	result_info?: unknown;
};

function jsonResponse(status: number, body: unknown): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: {
			"Cache-Control": "no-store",
			"Content-Security-Policy": "default-src 'none'",
			"Content-Type": "application/json; charset=utf-8",
			"X-Content-Type-Options": "nosniff",
		},
	});
}

function emitAudit(dependencies: BrokerDependencies, entry: AuditEntry): void {
	try {
		dependencies.log(entry);
	} catch {
		// Observability must never change the result of a DNS mutation or expose a
		// caught exception to the caller.
	}
}

function validateEnvironment(env: Env): void {
	if (
		typeof env.BROKER_SHARED_SECRET !== "string" ||
		!/^[A-Za-z0-9_-]{43}$/.test(env.BROKER_SHARED_SECRET)
	) {
		throw new HttpError(
			503,
			"BROKER_MISCONFIGURED",
			"Broker authentication is not configured",
		);
	}
	if (
		typeof env.CLOUDFLARE_API_TOKEN !== "string" ||
		env.CLOUDFLARE_API_TOKEN.length < 20
	) {
		throw new HttpError(
			503,
			"BROKER_MISCONFIGURED",
			"Cloudflare authentication is not configured",
		);
	}
	if (
		typeof env.CLOUDFLARE_ZONE_ID !== "string" ||
		!/^[0-9a-f]{32}$/.test(env.CLOUDFLARE_ZONE_ID)
	) {
		throw new HttpError(
			503,
			"BROKER_MISCONFIGURED",
			"Cloudflare zone is not configured",
		);
	}
}

function bearerToken(request: Request): string {
	const authorization = request.headers.get("Authorization") ?? "";
	if (
		authorization.length > 519 ||
		!authorization.startsWith("Bearer ") ||
		authorization.slice(7).length === 0 ||
		/\s/.test(authorization.slice(7))
	) {
		return "";
	}
	return authorization.slice(7);
}

async function constantTimeSecretEqual(
	expected: string,
	presented: string,
	cryptoImplementation: Crypto,
): Promise<boolean> {
	const encoder = new TextEncoder();
	const [expectedDigest, presentedDigest] = await Promise.all([
		cryptoImplementation.subtle.digest("SHA-256", encoder.encode(expected)),
		cryptoImplementation.subtle.digest("SHA-256", encoder.encode(presented)),
	]);
	const expectedBytes = new Uint8Array(expectedDigest);
	const presentedBytes = new Uint8Array(presentedDigest);
	let difference = 0;
	for (let index = 0; index < expectedBytes.length; index += 1) {
		difference |= expectedBytes[index] ^ presentedBytes[index];
	}
	return difference === 0;
}

async function readBoundedText(
	body: ReadableStream<Uint8Array> | null,
	contentLength: string | null,
	maximumBytes: number,
	deadline?: DeadlineContext,
): Promise<string> {
	if (
		deadline &&
		(deadline.signal.aborted || deadline.now() >= deadline.deadlineAt)
	) {
		throw new BrokerDeadlineError();
	}
	if (contentLength !== null) {
		const declared = Number(contentLength);
		if (!Number.isSafeInteger(declared) || declared < 0) {
			throw new HttpError(400, "INVALID_REQUEST", "Invalid Content-Length");
		}
		if (declared > maximumBytes) {
			throw new HttpError(413, "PAYLOAD_TOO_LARGE", "JSON body is too large");
		}
	}
	if (!body) return "";

	const reader = body.getReader();
	const chunks: Uint8Array[] = [];
	let size = 0;
	let deadlineTimer: TimerHandle | undefined;
	let rejectDeadline: ((error: BrokerDeadlineError) => void) | undefined;
	const deadlinePromise = deadline
		? new Promise<never>((_resolve, reject) => {
				rejectDeadline = reject;
				const remainingMs = deadline.deadlineAt - deadline.now();
				if (remainingMs <= 0 || deadline.signal.aborted) {
					reject(new BrokerDeadlineError());
					return;
				}
				deadlineTimer = deadline.setTimer(() => {
					reject(new BrokerDeadlineError());
					void reader.cancel().catch(() => undefined);
				}, remainingMs);
			})
		: undefined;
	const abortFromCaller = () => {
		rejectDeadline?.(new BrokerDeadlineError());
		void reader.cancel().catch(() => undefined);
	};
	deadline?.signal.addEventListener("abort", abortFromCaller, { once: true });
	if (deadline?.signal.aborted) abortFromCaller();
	try {
		while (true) {
			const read = reader.read();
			const { done, value } = deadlinePromise
				? await Promise.race([read, deadlinePromise])
				: await read;
			if (done) break;
			size += value.byteLength;
			if (size > maximumBytes) {
				await reader.cancel();
				throw new HttpError(413, "PAYLOAD_TOO_LARGE", "JSON body is too large");
			}
			chunks.push(value);
		}
	} finally {
		if (deadlineTimer !== undefined) deadline?.clearTimer(deadlineTimer);
		deadline?.signal.removeEventListener("abort", abortFromCaller);
		reader.releaseLock();
	}

	const joined = new Uint8Array(size);
	let offset = 0;
	for (const chunk of chunks) {
		joined.set(chunk, offset);
		offset += chunk.byteLength;
	}
	try {
		return new TextDecoder("utf-8", { fatal: true }).decode(joined);
	} catch {
		throw new HttpError(
			400,
			"INVALID_REQUEST",
			"JSON body must be valid UTF-8",
		);
	}
}

async function readRequestJson(
	request: Request,
	deadline: DeadlineContext,
): Promise<Record<string, unknown>> {
	const text = await readBoundedText(
		request.body,
		request.headers.get("Content-Length"),
		MAX_REQUEST_BYTES,
		deadline,
	);
	let value: unknown;
	try {
		value = JSON.parse(text);
	} catch {
		throw new HttpError(400, "INVALID_JSON", "Request body must be valid JSON");
	}
	return requireObject(value);
}

async function readUpstreamJson(
	response: Response,
): Promise<CloudflareEnvelope> {
	let text: string;
	try {
		text = await readBoundedText(
			response.body,
			response.headers.get("Content-Length"),
			MAX_UPSTREAM_BYTES,
		);
	} catch (error) {
		if (error instanceof HttpError) throw new CloudflareError(response.status);
		throw error;
	}
	try {
		const parsed = JSON.parse(text) as unknown;
		if (
			typeof parsed !== "object" ||
			parsed === null ||
			Array.isArray(parsed)
		) {
			throw new Error("not an object");
		}
		return parsed as CloudflareEnvelope;
	} catch {
		throw new CloudflareError(response.status);
	}
}

class CloudflareClient {
	private readonly env: Env;
	private readonly fetchFunction: FetchFunction;
	private readonly deadline: DeadlineContext;

	constructor(
		env: Env,
		fetchFunction: FetchFunction,
		deadline: DeadlineContext,
	) {
		this.env = env;
		this.fetchFunction = fetchFunction;
		this.deadline = deadline;
	}

	get zoneId(): string {
		return this.env.CLOUDFLARE_ZONE_ID;
	}

	private async request(
		path: string,
		init: RequestInit,
		allowNotFound = false,
		minimumRemainingMs = RESPONSE_HEADROOM_MS + 1,
	): Promise<CloudflareEnvelope | null> {
		const remainingMs = this.deadline.deadlineAt - this.deadline.now();
		if (
			this.deadline.signal.aborted ||
			remainingMs < minimumRemainingMs ||
			remainingMs <= RESPONSE_HEADROOM_MS
		) {
			throw new BrokerDeadlineError();
		}
		const callTimeoutMs = Math.min(
			CLOUDFLARE_CALL_TIMEOUT_MS,
			remainingMs - RESPONSE_HEADROOM_MS,
		);
		const controller = new AbortController();
		let callTimedOut = false;
		const timer = this.deadline.setTimer(() => {
			callTimedOut = true;
			controller.abort();
		}, callTimeoutMs);
		const abortFromCaller = () => controller.abort();
		this.deadline.signal.addEventListener("abort", abortFromCaller, {
			once: true,
		});
		if (this.deadline.signal.aborted) abortFromCaller();
		try {
			const response = await this.fetchFunction(
				`${CLOUDFLARE_API_ROOT}${path}`,
				{
					...init,
					signal: controller.signal,
					headers: {
						Accept: "application/json",
						Authorization: `Bearer ${this.env.CLOUDFLARE_API_TOKEN}`,
						...(init.body === undefined
							? {}
							: { "Content-Type": "application/json" }),
					},
				},
			);
			const envelope = await readUpstreamJson(response);
			if (callTimedOut) throw new CloudflareError(0, true);
			if (
				this.deadline.signal.aborted ||
				this.deadline.now() >= this.deadline.deadlineAt
			) {
				throw new BrokerDeadlineError();
			}
			if (allowNotFound && response.status === 404) return null;
			if (!response.ok || envelope.success !== true) {
				throw new CloudflareError(response.status);
			}
			return envelope;
		} catch (error) {
			if (
				error instanceof CloudflareError ||
				error instanceof BrokerDeadlineError
			) {
				throw error;
			}
			if (this.deadline.signal.aborted) throw new BrokerDeadlineError();
			const aborted =
				callTimedOut ||
				(error instanceof DOMException && error.name === "AbortError");
			throw new CloudflareError(0, aborted);
		} finally {
			this.deadline.clearTimer(timer);
			this.deadline.signal.removeEventListener("abort", abortFromCaller);
		}
	}

	async verifyZone(): Promise<void> {
		const envelope = await this.request(
			`/zones/${encodeURIComponent(this.zoneId)}`,
			{ method: "GET" },
		);
		const result = envelope?.result;
		if (
			typeof result !== "object" ||
			result === null ||
			Array.isArray(result) ||
			(result as Record<string, unknown>).id !== this.zoneId ||
			(result as Record<string, unknown>).name !== MANAGED_ZONE_NAME
		) {
			throw new ZoneIdentityError();
		}
	}

	async getRecord(
		recordId: string,
		minimumRemainingMs?: number,
	): Promise<ManagedDnsRecord | null> {
		const envelope = await this.request(
			`/zones/${encodeURIComponent(this.zoneId)}/dns_records/${encodeURIComponent(recordId)}`,
			{ method: "GET" },
			true,
			minimumRemainingMs,
		);
		if (envelope === null) return null;
		const record = parseManagedDnsRecord(envelope.result);
		if (record.id !== recordId) {
			throw new RecordConflictError(
				"Cloudflare returned a different DNS record than requested",
			);
		}
		return record;
	}

	async listRecords(
		name: string,
		type?: DnsRecordType,
	): Promise<ManagedDnsRecord[]> {
		const query = new URLSearchParams({
			name,
			match: "all",
			page: "1",
			per_page: String(MAX_LIST_RESULTS),
		});
		if (type) query.set("type", type);
		const envelope = await this.request(
			`/zones/${encodeURIComponent(this.zoneId)}/dns_records?${query}`,
			{ method: "GET" },
		);
		if (!Array.isArray(envelope?.result)) throw new CloudflareError(200);

		const resultInfo = envelope.result_info;
		const totalCount =
			typeof resultInfo === "object" &&
			resultInfo !== null &&
			!Array.isArray(resultInfo)
				? (resultInfo as Record<string, unknown>).total_count
				: undefined;
		if (
			!Number.isSafeInteger(totalCount) ||
			(totalCount as number) < 0 ||
			totalCount !== envelope.result.length ||
			envelope.result.length > MAX_LIST_RESULTS
		) {
			throw new RecordConflictError(
				"Too many DNS records exist at the managed name",
			);
		}
		const records = envelope.result.map(parseManagedDnsRecord);
		if (
			records.some(
				(record) =>
					record.name !== name || (type !== undefined && record.type !== type),
			)
		) {
			throw new RecordConflictError(
				"Cloudflare returned DNS records outside the requested managed name",
			);
		}
		return records;
	}

	async createRecord(
		expected: ExpectedLeaseRecord & { ttl: number },
	): Promise<ManagedDnsRecord> {
		const envelope = await this.request(
			`/zones/${encodeURIComponent(this.zoneId)}/dns_records`,
			{
				method: "POST",
				body: JSON.stringify({
					name: expected.name,
					type: expected.type,
					content: expected.address,
					ttl: expected.ttl,
					proxied: false,
					comment: managedComment(expected.leaseId),
				}),
			},
			false,
			CREATE_MUTATION_BUDGET_MS,
		);
		return parseManagedDnsRecord(envelope?.result);
	}

	async deleteRecord(recordId: string): Promise<string> {
		const envelope = await this.request(
			`/zones/${encodeURIComponent(this.zoneId)}/dns_records/${encodeURIComponent(recordId)}`,
			{ method: "DELETE" },
			false,
			DELETE_MUTATION_BUDGET_MS,
		);
		const result = envelope?.result;
		if (
			typeof result !== "object" ||
			result === null ||
			Array.isArray(result) ||
			(result as Record<string, unknown>).id !== recordId
		) {
			throw new CloudflareError(200);
		}
		return recordId;
	}
}

type ParsedRequest =
	| { route: "/zone" }
	| { route: "/records/list"; name: string; type?: DnsRecordType }
	| { route: "/records/get"; recordId: string }
	| {
			route: "/records/create";
			expected: ExpectedLeaseRecord & { ttl: number };
	  }
	| {
			route: "/records/delete";
			recordId: string;
			expected: ExpectedLeaseRecord;
	  };

function parseBody(
	route: string,
	body: Record<string, unknown>,
): ParsedRequest {
	switch (route) {
		case "/zone":
			requireExactKeys(body, []);
			return { route };
		case "/records/list": {
			requireExactKeys(body, ["name"], ["type"]);
			return {
				route,
				name: requireManagedName(body.name),
				...(body.type === undefined
					? {}
					: { type: requireRecordType(body.type) }),
			};
		}
		case "/records/get":
			requireExactKeys(body, ["recordId"]);
			return { route, recordId: requireRecordId(body.recordId) };
		case "/records/create": {
			requireExactKeys(body, ["leaseId", "name", "type", "address", "ttl"]);
			const type = requireRecordType(body.type);
			return {
				route,
				expected: {
					leaseId: requireLeaseId(body.leaseId),
					name: requireManagedName(body.name),
					type,
					address: requirePublicAddress(body.address, type),
					ttl: requireAllowedTtl(body.ttl),
				},
			};
		}
		case "/records/delete": {
			requireExactKeys(body, [
				"recordId",
				"leaseId",
				"name",
				"type",
				"address",
			]);
			const type = requireRecordType(body.type);
			return {
				route,
				recordId: requireRecordId(body.recordId),
				expected: {
					leaseId: requireLeaseId(body.leaseId),
					name: requireManagedName(body.name),
					type,
					address: requirePublicAddress(body.address, type),
				},
			};
		}
		default:
			throw new HttpError(404, "NOT_FOUND", "Route not found");
	}
}

function requestMetadata(
	parsed: ParsedRequest,
): Pick<AuditEntry, "name" | "recordId"> {
	switch (parsed.route) {
		case "/records/list":
			return { name: parsed.name };
		case "/records/get":
			return { recordId: parsed.recordId };
		case "/records/create":
			return { name: parsed.expected.name };
		case "/records/delete":
			return { name: parsed.expected.name, recordId: parsed.recordId };
		default:
			return {};
	}
}

async function rollbackCreatedRecord(
	cloudflare: CloudflareClient,
	recordId: string,
	expected: ExpectedLeaseRecord & { ttl: number },
): Promise<void> {
	try {
		// A separate GET is intentional: every delete, including race cleanup, is
		// authorized from the current Cloudflare object rather than a stale create
		// response.
		const current = await cloudflare.getRecord(
			recordId,
			DELETE_SEQUENCE_BUDGET_MS,
		);
		if (current !== null && recordMatchesLease(current, expected)) {
			await cloudflare.deleteRecord(recordId);
		}
	} catch {
		// The Supabase lease cleanup state machine will retry. Expanding this into
		// a name-based delete would be unsafe.
	}
}

async function dispatch(
	parsed: ParsedRequest,
	cloudflare: CloudflareClient,
): Promise<Record<string, unknown>> {
	// This preflight prevents a typo or stale secret from turning the broker into
	// a confused deputy for a different Cloudflare zone.
	await cloudflare.verifyZone();

	switch (parsed.route) {
		case "/zone":
			return { zoneId: cloudflare.zoneId, zoneName: MANAGED_ZONE_NAME };
		case "/records/list": {
			const records = await cloudflare.listRecords(parsed.name, parsed.type);
			return { zoneId: cloudflare.zoneId, records };
		}
		case "/records/get": {
			const record = await cloudflare.getRecord(parsed.recordId);
			return { zoneId: cloudflare.zoneId, record };
		}
		case "/records/create": {
			const before = await cloudflare.listRecords(parsed.expected.name);
			if (before.length > 0) {
				if (
					before.length === 1 &&
					recordMatchesLease(before[0], parsed.expected)
				) {
					return { zoneId: cloudflare.zoneId, record: before[0] };
				}
				throw new RecordConflictError("The managed DNS name is already in use");
			}

			const created = await cloudflare.createRecord(parsed.expected);
			if (!recordMatchesLease(created, parsed.expected)) {
				throw new RecordConflictError(
					"Cloudflare created a record that does not match the lease",
				);
			}

			let after: ManagedDnsRecord[];
			try {
				after = await cloudflare.listRecords(parsed.expected.name);
			} catch (error) {
				await rollbackCreatedRecord(cloudflare, created.id, parsed.expected);
				throw error;
			}
			if (
				after.length !== 1 ||
				after[0].id !== created.id ||
				!recordMatchesLease(after[0], parsed.expected)
			) {
				await rollbackCreatedRecord(cloudflare, created.id, parsed.expected);
				throw new RecordConflictError(
					"The managed DNS name changed while it was being provisioned",
				);
			}
			return { zoneId: cloudflare.zoneId, record: created };
		}
		case "/records/delete": {
			const existing = await cloudflare.getRecord(
				parsed.recordId,
				DELETE_SEQUENCE_BUDGET_MS,
			);
			if (existing === null) {
				return { zoneId: cloudflare.zoneId, deletedId: null };
			}
			if (!recordMatchesLease(existing, parsed.expected)) {
				throw new RecordConflictError(
					"DNS record does not match the lease authorized for deletion",
				);
			}
			const deletedId = await cloudflare.deleteRecord(parsed.recordId);
			return { zoneId: cloudflare.zoneId, deletedId };
		}
	}
}

function publicError(error: unknown): {
	status: number;
	code: string;
	message: string;
	upstreamStatus?: number;
} {
	if (error instanceof HttpError) {
		return { status: error.status, code: error.code, message: error.message };
	}
	if (error instanceof ValidationError) {
		return { status: 400, code: "INVALID_REQUEST", message: error.message };
	}
	if (error instanceof RecordConflictError) {
		return { status: 409, code: "DNS_RECORD_CONFLICT", message: error.message };
	}
	if (error instanceof ZoneIdentityError) {
		return {
			status: 502,
			code: "ZONE_IDENTITY_MISMATCH",
			message: "Cloudflare zone identity verification failed",
		};
	}
	if (error instanceof BrokerDeadlineError) {
		return {
			status: 504,
			code: "BROKER_DEADLINE_EXCEEDED",
			message: "DNS broker request deadline exceeded",
		};
	}
	if (error instanceof CloudflareError) {
		return {
			status: error.timedOut ? 504 : 502,
			code: error.timedOut ? "CLOUDFLARE_TIMEOUT" : "CLOUDFLARE_REQUEST_FAILED",
			message: error.timedOut
				? "Cloudflare DNS request timed out"
				: "Cloudflare DNS request failed",
			upstreamStatus: error.upstreamStatus || undefined,
		};
	}
	return {
		status: 500,
		code: "INTERNAL_ERROR",
		message: "Internal broker error",
	};
}

export function createWorker(overrides: Partial<BrokerDependencies> = {}): {
	fetch(request: Request, env: Env): Promise<Response>;
} {
	const dependencies: BrokerDependencies = {
		fetch: globalThis.fetch.bind(globalThis),
		crypto: globalThis.crypto,
		now: () => Date.now(),
		log: (entry) => console.log(JSON.stringify(entry)),
		setTimer: (callback, delayMs) => setTimeout(callback, delayMs),
		clearTimer: (handle) => clearTimeout(handle),
		...overrides,
	};

	return {
		async fetch(request: Request, env: Env): Promise<Response> {
			const startedAt = dependencies.now();
			const deadlineAt = startedAt + OVERALL_DEADLINE_MS;
			const deadline: DeadlineContext = {
				now: dependencies.now,
				setTimer: dependencies.setTimer,
				clearTimer: dependencies.clearTimer,
				deadlineAt,
				signal: request.signal,
			};
			const requestId = dependencies.crypto.randomUUID();
			const url = new URL(request.url);
			const route = url.pathname;
			let metadata: Pick<AuditEntry, "name" | "recordId"> = {};

			try {
				if (!API_PATHS.has(route)) {
					throw new HttpError(404, "NOT_FOUND", "Route not found");
				}
				if (request.method !== "POST") {
					throw new HttpError(
						405,
						"METHOD_NOT_ALLOWED",
						"Only POST is allowed",
					);
				}
				if (url.search !== "") {
					throw new HttpError(
						400,
						"INVALID_REQUEST",
						"Query parameters are not allowed",
					);
				}
				const contentType = request.headers.get("Content-Type") ?? "";
				if (!/^application\/json(?:\s*;|$)/i.test(contentType)) {
					throw new HttpError(
						415,
						"UNSUPPORTED_MEDIA_TYPE",
						"Content-Type must be application/json",
					);
				}

				validateEnvironment(env);
				const authenticated = await constantTimeSecretEqual(
					env.BROKER_SHARED_SECRET,
					bearerToken(request),
					dependencies.crypto,
				);
				if (!authenticated) {
					throw new HttpError(401, "UNAUTHORIZED", "Authentication failed");
				}
				if (request.signal.aborted || dependencies.now() >= deadlineAt) {
					throw new BrokerDeadlineError();
				}

				const body = await readRequestJson(request, deadline);
				if (request.signal.aborted || dependencies.now() >= deadlineAt) {
					throw new BrokerDeadlineError();
				}
				const parsed = parseBody(route, body);
				metadata = requestMetadata(parsed);
				const result = await dispatch(
					parsed,
					new CloudflareClient(env, dependencies.fetch, deadline),
				);
				emitAudit(dependencies, {
					event: "dns_lease_broker_request",
					requestId,
					route,
					outcome: "success",
					status: 200,
					durationMs: Math.max(0, dependencies.now() - startedAt),
					...metadata,
				});
				return jsonResponse(200, result);
			} catch (error) {
				const exposed = publicError(error);
				emitAudit(dependencies, {
					event: "dns_lease_broker_request",
					requestId,
					route,
					outcome: "error",
					status: exposed.status,
					durationMs: Math.max(0, dependencies.now() - startedAt),
					...metadata,
					errorCode: exposed.code,
					...(exposed.upstreamStatus === undefined
						? {}
						: { upstreamStatus: exposed.upstreamStatus }),
				});
				const response = jsonResponse(exposed.status, {
					error: { code: exposed.code, message: exposed.message },
					requestId,
				});
				if (exposed.status === 405) response.headers.set("Allow", "POST");
				return response;
			}
		},
	};
}

export default createWorker();
