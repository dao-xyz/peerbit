import assert from "node:assert/strict";
import test from "node:test";
import { type Env, createWorker } from "./index.ts";
import { parseManagedDnsRecord, requirePublicAddress } from "./validation.ts";

const ZONE_ID = "1234567890abcdef1234567890abcdef";
const RECORD_ID = "abcdef1234567890abcdef1234567890";
const SECOND_RECORD_ID = "fedcba0987654321fedcba0987654321";
const LEASE_ID = "123e4567-e89b-42d3-a456-426614174000";
const NAME = "p-0123456789abcdefabcd.nodes.peerchecker.com";
const ADDRESS = "203.0.114.25";
const SHARED_SECRET = "s".repeat(43);
const API_TOKEN = "cloudflare-api-token-never-returned";

const env: Env = {
	BROKER_SHARED_SECRET: SHARED_SECRET,
	CLOUDFLARE_API_TOKEN: API_TOKEN,
	CLOUDFLARE_ZONE_ID: ZONE_ID,
};

type FetchCall = { url: URL; init: RequestInit };

function envelope(
	result: unknown,
	options: { status?: number; success?: boolean; totalCount?: number } = {},
): Response {
	return new Response(
		JSON.stringify({
			success: options.success ?? true,
			result,
			...(options.totalCount === undefined
				? {}
				: { result_info: { total_count: options.totalCount } }),
		}),
		{
			status: options.status ?? 200,
			headers: { "Content-Type": "application/json" },
		},
	);
}

function zoneEnvelope(name = "peerchecker.com"): Response {
	return envelope({ id: ZONE_ID, name });
}

function managedRecord(
	overrides: Record<string, unknown> = {},
): Record<string, unknown> {
	return {
		id: RECORD_ID,
		name: NAME,
		type: "A",
		content: ADDRESS,
		ttl: 300,
		proxied: false,
		comment: `Peerbit managed lease ${LEASE_ID}`,
		...overrides,
	};
}

function brokerRequest(
	path: string,
	body: unknown,
	options: {
		token?: string;
		method?: string;
		contentType?: string;
		signal?: AbortSignal;
	} = {},
): Request {
	return new Request(`https://broker.example${path}`, {
		method: options.method ?? "POST",
		headers: {
			Authorization: `Bearer ${options.token ?? SHARED_SECRET}`,
			"Content-Type": options.contentType ?? "application/json",
		},
		body: options.method === "GET" ? undefined : JSON.stringify(body),
		signal: options.signal,
	});
}

function harness(
	handler: (
		call: FetchCall,
		calls: FetchCall[],
	) => Response | Promise<Response>,
) {
	const calls: FetchCall[] = [];
	const logs: unknown[] = [];
	let now = 1_000;
	const worker = createWorker({
		fetch: async (input, init = {}) => {
			const call = { url: new URL(String(input)), init };
			calls.push(call);
			return await handler(call, calls);
		},
		now: () => now++,
		log: (entry) => logs.push(entry),
	});
	return { worker, calls, logs };
}

async function bodyOf(response: Response): Promise<Record<string, any>> {
	return (await response.json()) as Record<string, any>;
}

test("authenticates before reading or forwarding a request", async () => {
	const { worker, calls, logs } = harness(() => {
		throw new Error("Cloudflare must not be called");
	});
	const response = await worker.fetch(
		brokerRequest("/zone", {}, { token: "incorrect-secret" }),
		env,
	);
	assert.equal(response.status, 401);
	assert.equal((await bodyOf(response)).error.code, "UNAUTHORIZED");
	assert.equal(calls.length, 0);
	assert.equal(logs.length, 1);
	assert.doesNotMatch(
		JSON.stringify(logs),
		/incorrect-secret|cloudflare-api-token/,
	);
});

test("requires the same 32-byte unpadded base64url broker secret as Supabase", async () => {
	for (const invalidSecret of [
		"s".repeat(42),
		"s".repeat(44),
		`${"s".repeat(42)}=`,
		`${"s".repeat(42)}+`,
	]) {
		const { worker, calls } = harness(() => {
			throw new Error("Cloudflare must not be called");
		});
		const response = await worker.fetch(brokerRequest("/zone", {}), {
			...env,
			BROKER_SHARED_SECRET: invalidSecret,
		});
		assert.equal(response.status, 503);
		assert.equal((await bodyOf(response)).error.code, "BROKER_MISCONFIGURED");
		assert.equal(calls.length, 0);
	}
});

test("allows only POST JSON on exact routes without query parameters", async () => {
	const { worker, calls } = harness(() => zoneEnvelope());
	const get = await worker.fetch(
		brokerRequest("/zone", {}, { method: "GET" }),
		env,
	);
	assert.equal(get.status, 405);
	assert.equal(get.headers.get("Allow"), "POST");

	const text = await worker.fetch(
		brokerRequest("/zone", {}, { contentType: "text/plain" }),
		env,
	);
	assert.equal(text.status, 415);

	const query = await worker.fetch(brokerRequest("/zone?debug=1", {}), env);
	assert.equal(query.status, 400);

	const unknown = await worker.fetch(brokerRequest("/records", {}), env);
	assert.equal(unknown.status, 404);
	assert.equal(calls.length, 0);
});

test("rejects request bodies over 8 KiB without calling Cloudflare", async () => {
	const { worker, calls } = harness(() => zoneEnvelope());
	const response = await worker.fetch(
		brokerRequest("/zone", { padding: "x".repeat(9 * 1024) }),
		env,
	);
	assert.equal(response.status, 413);
	assert.equal((await bodyOf(response)).error.code, "PAYLOAD_TOO_LARGE");
	assert.equal(calls.length, 0);
});

test("stops the request at one 35-second absolute deadline", async () => {
	let now = 0;
	const calls: FetchCall[] = [];
	const worker = createWorker({
		now: () => now,
		log: () => undefined,
		fetch: async (input, init = {}) => {
			calls.push({ url: new URL(String(input)), init });
			if (calls.length !== 1)
				throw new Error("deadline must prevent record lookup");
			now = 34_500;
			return zoneEnvelope();
		},
	});
	const response = await worker.fetch(
		brokerRequest("/records/list", { name: NAME }),
		env,
	);
	assert.equal(response.status, 504);
	assert.equal((await bodyOf(response)).error.code, "BROKER_DEADLINE_EXCEEDED");
	assert.equal(calls.length, 1);
});

test("times out a slow request body before any Cloudflare call", async () => {
	let called = false;
	const worker = createWorker({
		now: () => 0,
		log: () => undefined,
		setTimer: (callback, delayMs) => {
			if (delayMs === 35_000) queueMicrotask(callback);
			return 0 as unknown as ReturnType<typeof setTimeout>;
		},
		clearTimer: () => undefined,
		fetch: async () => {
			called = true;
			return zoneEnvelope();
		},
	});
	const body = new ReadableStream<Uint8Array>({
		start() {
			// Deliberately never enqueue or close.
		},
	});
	const request = new Request("https://broker.example/zone", {
		method: "POST",
		headers: {
			Authorization: `Bearer ${SHARED_SECRET}`,
			"Content-Type": "application/json",
		},
		body,
		duplex: "half",
	} as RequestInit & { duplex: "half" });
	const response = await worker.fetch(request, env);
	assert.equal(response.status, 504);
	assert.equal((await bodyOf(response)).error.code, "BROKER_DEADLINE_EXCEEDED");
	assert.equal(called, false);
});

test("caps Cloudflare calls at five seconds or the smaller remaining budget", async () => {
	let now = 0;
	const delays: number[] = [];
	const calls: FetchCall[] = [];
	const worker = createWorker({
		now: () => now,
		log: () => undefined,
		setTimer: (callback, delayMs) => {
			delays.push(delayMs);
			return setTimeout(callback, delayMs);
		},
		fetch: async (input, init = {}) => {
			const call = { url: new URL(String(input)), init };
			calls.push(call);
			if (calls.length === 1) {
				now = 32_000;
				return zoneEnvelope();
			}
			return envelope([], { totalCount: 0 });
		},
	});
	const response = await worker.fetch(
		brokerRequest("/records/list", { name: NAME }),
		env,
	);
	assert.equal(response.status, 200);
	assert.deepEqual(delays, [35_000, 5_000, 2_000]);
});

test("refuses a create before mutation when reconciliation no longer fits", async () => {
	let now = 0;
	const calls: FetchCall[] = [];
	const worker = createWorker({
		now: () => now,
		log: () => undefined,
		fetch: async (input, init = {}) => {
			const call = { url: new URL(String(input)), init };
			calls.push(call);
			if (calls.length === 1) {
				now = 10_000;
				return zoneEnvelope();
			}
			if (calls.length === 2) {
				now = 24_500;
				return envelope([], { totalCount: 0 });
			}
			throw new Error("create must not begin without its safety budget");
		},
	});
	const response = await worker.fetch(
		brokerRequest("/records/create", {
			leaseId: LEASE_ID,
			name: NAME,
			type: "A",
			address: ADDRESS,
			ttl: 300,
		}),
		env,
	);
	assert.equal(response.status, 504);
	assert.equal((await bodyOf(response)).error.code, "BROKER_DEADLINE_EXCEEDED");
	assert.equal(calls.length, 2);
	assert.ok(calls.every((call) => call.init.method !== "POST"));
});

test("refuses delete when its mutation window no longer fits", async () => {
	let now = 0;
	const calls: FetchCall[] = [];
	const worker = createWorker({
		now: () => now,
		log: () => undefined,
		fetch: async (input, init = {}) => {
			const call = { url: new URL(String(input)), init };
			calls.push(call);
			if (calls.length === 1) {
				now = 10_000;
				return zoneEnvelope();
			}
			if (calls.length === 2) {
				now = 29_500;
				return envelope(managedRecord());
			}
			throw new Error("delete must not begin without its safety budget");
		},
	});
	const response = await worker.fetch(
		brokerRequest("/records/delete", {
			recordId: RECORD_ID,
			leaseId: LEASE_ID,
			name: NAME,
			type: "A",
			address: ADDRESS,
		}),
		env,
	);
	assert.equal(response.status, 504);
	assert.equal((await bodyOf(response)).error.code, "BROKER_DEADLINE_EXCEEDED");
	assert.equal(calls.length, 2);
	assert.ok(calls.every((call) => call.init.method !== "DELETE"));
});

test("aborts a stalled Cloudflare call at its five-second cap", async () => {
	const delays: number[] = [];
	const worker = createWorker({
		now: () => 0,
		log: () => undefined,
		setTimer: (callback, delayMs) => {
			delays.push(delayMs);
			if (delayMs <= 5_000) queueMicrotask(callback);
			return 0 as unknown as ReturnType<typeof setTimeout>;
		},
		clearTimer: () => undefined,
		fetch: async (_input, init = {}) =>
			await new Promise<Response>((_resolve, reject) => {
				const signal = init.signal;
				if (!signal) throw new Error("broker must pass an abort signal");
				const rejectAbort = () =>
					reject(new DOMException("request aborted", "AbortError"));
				if (signal.aborted) rejectAbort();
				else signal.addEventListener("abort", rejectAbort, { once: true });
			}),
	});
	const response = await worker.fetch(brokerRequest("/zone", {}), env);
	assert.equal(response.status, 504);
	assert.equal((await bodyOf(response)).error.code, "CLOUDFLARE_TIMEOUT");
	assert.deepEqual(delays, [35_000, 5_000]);
});

test("does not call Cloudflare after the caller disconnects", async () => {
	const controller = new AbortController();
	controller.abort();
	let called = false;
	const worker = createWorker({
		log: () => undefined,
		fetch: async () => {
			called = true;
			return zoneEnvelope();
		},
	});
	const response = await worker.fetch(
		brokerRequest("/zone", {}, { signal: controller.signal }),
		env,
	);
	assert.equal(response.status, 504);
	assert.equal((await bodyOf(response)).error.code, "BROKER_DEADLINE_EXCEEDED");
	assert.equal(called, false);
});

test("caller disconnect aborts an in-flight Cloudflare fetch", async () => {
	const controller = new AbortController();
	let downstreamAborted = false;
	const worker = createWorker({
		log: () => undefined,
		fetch: async (_input, init = {}) =>
			await new Promise<Response>((_resolve, reject) => {
				if (!init.signal) throw new Error("broker must pass an abort signal");
				init.signal.addEventListener(
					"abort",
					() => {
						downstreamAborted = true;
						reject(new DOMException("request aborted", "AbortError"));
					},
					{ once: true },
				);
				queueMicrotask(() => controller.abort());
			}),
	});
	const response = await worker.fetch(
		brokerRequest("/zone", {}, { signal: controller.signal }),
		env,
	);
	assert.equal(response.status, 504);
	assert.equal((await bodyOf(response)).error.code, "BROKER_DEADLINE_EXCEEDED");
	assert.equal(downstreamAborted, true);
});

test("verifies zone identity and sends the API token only to Cloudflare", async () => {
	const { worker, calls, logs } = harness((call) => {
		assert.equal(
			call.url.href,
			`${"https://api.cloudflare.com/client/v4"}/zones/${ZONE_ID}`,
		);
		assert.equal(
			new Headers(call.init.headers).get("Authorization"),
			`Bearer ${API_TOKEN}`,
		);
		return zoneEnvelope();
	});
	const response = await worker.fetch(brokerRequest("/zone", {}), env);
	assert.equal(response.status, 200);
	const responseBody = await bodyOf(response);
	assert.deepEqual(responseBody, {
		zoneId: ZONE_ID,
		zoneName: "peerchecker.com",
	});
	assert.equal(calls.length, 1);
	const serialized = JSON.stringify([responseBody, logs]);
	assert.doesNotMatch(serialized, /cloudflare-api-token|shared-secret/);
});

test("does not let an audit sink failure change a completed operation", async () => {
	const worker = createWorker({
		fetch: async () => zoneEnvelope(),
		log: () => {
			throw new Error("audit sink unavailable");
		},
	});
	const response = await worker.fetch(brokerRequest("/zone", {}), env);
	assert.equal(response.status, 200);
	assert.equal((await bodyOf(response)).zoneName, "peerchecker.com");
});

test("fails closed when the configured zone ID belongs to another domain", async () => {
	const { worker, calls } = harness(() => zoneEnvelope("example.com"));
	const response = await worker.fetch(
		brokerRequest("/records/list", { name: NAME }),
		env,
	);
	assert.equal(response.status, 502);
	assert.equal((await bodyOf(response)).error.code, "ZONE_IDENTITY_MISMATCH");
	assert.equal(
		calls.length,
		1,
		"record listing must not run after the failed preflight",
	);
});

test("hard-restricts names, types, public addresses, UUIDs, TTLs, and fields", async () => {
	const { worker, calls } = harness(() => zoneEnvelope());
	const invalidBodies = [
		{
			leaseId: LEASE_ID,
			name: "www.peerchecker.com",
			type: "A",
			address: ADDRESS,
			ttl: 300,
		},
		{
			leaseId: LEASE_ID,
			name: NAME,
			type: "CNAME",
			address: ADDRESS,
			ttl: 300,
		},
		{ leaseId: LEASE_ID, name: NAME, type: "A", address: "10.0.0.1", ttl: 300 },
		{
			leaseId: LEASE_ID,
			name: NAME,
			type: "AAAA",
			address: "2001:db8::1",
			ttl: 300,
		},
		{
			leaseId: "not-a-uuid",
			name: NAME,
			type: "A",
			address: ADDRESS,
			ttl: 300,
		},
		{ leaseId: LEASE_ID, name: NAME, type: "A", address: ADDRESS, ttl: 30 },
		{
			leaseId: LEASE_ID,
			name: NAME,
			type: "A",
			address: ADDRESS,
			ttl: 300,
			debug: true,
		},
	];
	for (const body of invalidBodies) {
		const response = await worker.fetch(
			brokerRequest("/records/create", body),
			env,
		);
		assert.equal(response.status, 400);
	}
	assert.equal(
		calls.length,
		0,
		"validation must complete before zone preflight",
	);
});

test("lists only exact, valid managed records", async () => {
	const { worker, calls } = harness((call) => {
		if (calls.length === 1) return zoneEnvelope();
		assert.equal(call.url.pathname, `/client/v4/zones/${ZONE_ID}/dns_records`);
		assert.equal(call.url.searchParams.get("name"), NAME);
		assert.equal(call.url.searchParams.get("type"), "A");
		assert.equal(call.url.searchParams.get("per_page"), "10");
		return envelope([managedRecord()], { totalCount: 1 });
	});
	const response = await worker.fetch(
		brokerRequest("/records/list", { name: NAME, type: "A" }),
		env,
	);
	assert.equal(response.status, 200);
	const body = await bodyOf(response);
	assert.equal(body.zoneId, ZONE_ID);
	assert.deepEqual(body.records, [managedRecord()]);
	assert.equal(calls.length, 2);
});

test("refuses to reconcile more than ten records at one managed name", async () => {
	const records = Array.from({ length: 11 }, (_value, index) =>
		managedRecord({ id: index.toString(16).padStart(32, "0") }),
	);
	const { worker, calls } = harness(() =>
		calls.length === 1
			? zoneEnvelope()
			: envelope(records, { totalCount: records.length }),
	);
	const response = await worker.fetch(
		brokerRequest("/records/list", { name: NAME }),
		env,
	);
	assert.equal(response.status, 409);
	assert.equal((await bodyOf(response)).error.code, "DNS_RECORD_CONFLICT");
	assert.equal(calls[1].url.searchParams.get("per_page"), "10");
});

test("refuses malformed, proxied, private, or unrelated records returned by Cloudflare", async () => {
	for (const badRecord of [
		managedRecord({ proxied: true }),
		managedRecord({ content: "192.168.1.1" }),
		managedRecord({ comment: "manual record" }),
		managedRecord({ name: "p-fedcba9876543210abcd.nodes.peerchecker.com" }),
	]) {
		const { worker, calls } = harness(() =>
			calls.length === 1
				? zoneEnvelope()
				: envelope([badRecord], { totalCount: 1 }),
		);
		const response = await worker.fetch(
			brokerRequest("/records/list", { name: NAME }),
			env,
		);
		assert.equal(response.status, 409);
		assert.equal((await bodyOf(response)).error.code, "DNS_RECORD_CONFLICT");
	}
});

test("gets a managed record by ID and treats Cloudflare 404 as absence", async () => {
	{
		const { worker, calls } = harness((call) => {
			if (calls.length === 1) return zoneEnvelope();
			assert.equal(
				call.url.pathname,
				`/client/v4/zones/${ZONE_ID}/dns_records/${RECORD_ID}`,
			);
			return envelope(managedRecord());
		});
		const response = await worker.fetch(
			brokerRequest("/records/get", { recordId: RECORD_ID }),
			env,
		);
		assert.equal(response.status, 200);
		assert.deepEqual((await bodyOf(response)).record, managedRecord());
	}

	{
		const { worker, calls } = harness(() =>
			calls.length === 1
				? zoneEnvelope()
				: envelope(null, { status: 404, success: false }),
		);
		const response = await worker.fetch(
			brokerRequest("/records/get", { recordId: RECORD_ID }),
			env,
		);
		assert.equal(response.status, 200);
		assert.equal((await bodyOf(response)).record, null);
	}
});

test("rejects get responses for a different record ID", async () => {
	const { worker, calls } = harness(() =>
		calls.length === 1
			? zoneEnvelope()
			: envelope(managedRecord({ id: SECOND_RECORD_ID })),
	);
	const response = await worker.fetch(
		brokerRequest("/records/get", { recordId: RECORD_ID }),
		env,
	);
	assert.equal(response.status, 409);
});

test("creates an exact DNS-only record and verifies post-create exclusivity", async () => {
	let stored: Record<string, unknown> | undefined;
	const { worker, calls } = harness((call) => {
		if (call.url.pathname === `/client/v4/zones/${ZONE_ID}`)
			return zoneEnvelope();
		if (call.init.method === "GET") {
			return envelope(stored ? [stored] : [], { totalCount: stored ? 1 : 0 });
		}
		if (call.init.method === "POST") {
			const body = JSON.parse(String(call.init.body)) as Record<
				string,
				unknown
			>;
			assert.deepEqual(body, {
				name: NAME,
				type: "A",
				content: ADDRESS,
				ttl: 300,
				proxied: false,
				comment: `Peerbit managed lease ${LEASE_ID}`,
			});
			stored = managedRecord();
			return envelope(stored);
		}
		throw new Error(`Unexpected ${call.init.method}`);
	});
	const response = await worker.fetch(
		brokerRequest("/records/create", {
			leaseId: LEASE_ID,
			name: NAME,
			type: "A",
			address: ADDRESS,
			ttl: 300,
		}),
		env,
	);
	assert.equal(response.status, 200);
	assert.deepEqual((await bodyOf(response)).record, managedRecord());
	assert.deepEqual(
		calls.map((call) => call.init.method),
		["GET", "GET", "POST", "GET"],
	);
});

test("makes an exact existing create idempotent and rejects occupied names", async () => {
	for (const [record, expectedStatus] of [
		[managedRecord(), 200],
		[managedRecord({ content: "203.0.114.26" }), 409],
	] as const) {
		const { worker, calls } = harness(() =>
			calls.length === 1
				? zoneEnvelope()
				: envelope([record], { totalCount: 1 }),
		);
		const response = await worker.fetch(
			brokerRequest("/records/create", {
				leaseId: LEASE_ID,
				name: NAME,
				type: "A",
				address: ADDRESS,
				ttl: 300,
			}),
			env,
		);
		assert.equal(response.status, expectedStatus);
		assert.equal(
			calls.length,
			2,
			"an occupied name must never reach record creation",
		);
	}
});

test("rolls back only the freshly created exact record when a create race is detected", async () => {
	let listCount = 0;
	const competing = managedRecord({
		id: SECOND_RECORD_ID,
		content: "203.0.114.26",
		comment: "Peerbit managed lease 923e4567-e89b-42d3-a456-426614174000",
	});
	const { worker, calls } = harness((call) => {
		if (call.url.pathname === `/client/v4/zones/${ZONE_ID}`)
			return zoneEnvelope();
		if (call.init.method === "POST") return envelope(managedRecord());
		if (call.init.method === "DELETE") {
			assert.equal(call.url.pathname.endsWith(`/${RECORD_ID}`), true);
			return envelope({ id: RECORD_ID });
		}
		if (call.url.pathname.endsWith(`/${RECORD_ID}`)) {
			return envelope(managedRecord());
		}
		listCount += 1;
		return listCount === 1
			? envelope([], { totalCount: 0 })
			: envelope([managedRecord(), competing], { totalCount: 2 });
	});
	const response = await worker.fetch(
		brokerRequest("/records/create", {
			leaseId: LEASE_ID,
			name: NAME,
			type: "A",
			address: ADDRESS,
			ttl: 300,
		}),
		env,
	);
	assert.equal(response.status, 409);
	assert.deepEqual(
		calls.map((call) => call.init.method),
		["GET", "GET", "POST", "GET", "GET", "DELETE"],
	);
});

test("does not begin race rollback after its exact-delete budget is exhausted", async () => {
	let now = 0;
	let listCount = 0;
	const competing = managedRecord({
		id: SECOND_RECORD_ID,
		content: "203.0.114.26",
		comment: "Peerbit managed lease 923e4567-e89b-42d3-a456-426614174000",
	});
	const calls: FetchCall[] = [];
	const worker = createWorker({
		now: () => now,
		log: () => undefined,
		fetch: async (input, init = {}) => {
			const call = { url: new URL(String(input)), init };
			calls.push(call);
			if (call.url.pathname === `/client/v4/zones/${ZONE_ID}`) {
				return zoneEnvelope();
			}
			if (call.init.method === "POST") return envelope(managedRecord());
			listCount += 1;
			if (listCount === 1) return envelope([], { totalCount: 0 });
			now = 24_000;
			return envelope([managedRecord(), competing], { totalCount: 2 });
		},
	});
	const response = await worker.fetch(
		brokerRequest("/records/create", {
			leaseId: LEASE_ID,
			name: NAME,
			type: "A",
			address: ADDRESS,
			ttl: 300,
		}),
		env,
	);
	assert.equal(response.status, 409);
	assert.deepEqual(
		calls.map((call) => call.init.method),
		["GET", "GET", "POST", "GET"],
	);
	assert.ok(calls.every((call) => call.init.method !== "DELETE"));
});

test("deletes only after fetching and matching the exact lease record", async () => {
	const { worker, calls } = harness((call) => {
		if (calls.length === 1) return zoneEnvelope();
		if (call.init.method === "GET") return envelope(managedRecord());
		if (call.init.method === "DELETE") return envelope({ id: RECORD_ID });
		throw new Error("unexpected request");
	});
	const response = await worker.fetch(
		brokerRequest("/records/delete", {
			recordId: RECORD_ID,
			leaseId: LEASE_ID,
			name: NAME,
			type: "A",
			address: ADDRESS,
		}),
		env,
	);
	assert.equal(response.status, 200);
	assert.equal((await bodyOf(response)).deletedId, RECORD_ID);
	assert.deepEqual(
		calls.map((call) => call.init.method),
		["GET", "GET", "DELETE"],
	);
});

test("never deletes a record with a different lease comment or address", async () => {
	for (const record of [
		managedRecord({
			comment: "Peerbit managed lease 923e4567-e89b-42d3-a456-426614174000",
		}),
		managedRecord({ content: "203.0.114.26" }),
	]) {
		const { worker, calls } = harness(() =>
			calls.length === 1 ? zoneEnvelope() : envelope(record),
		);
		const response = await worker.fetch(
			brokerRequest("/records/delete", {
				recordId: RECORD_ID,
				leaseId: LEASE_ID,
				name: NAME,
				type: "A",
				address: ADDRESS,
			}),
			env,
		);
		assert.equal(response.status, 409);
		assert.equal(calls.length, 2);
		assert.ok(calls.every((call) => call.init.method !== "DELETE"));
	}
});

test("makes deletion of an already absent record idempotent", async () => {
	const { worker, calls } = harness(() =>
		calls.length === 1
			? zoneEnvelope()
			: envelope(null, { status: 404, success: false }),
	);
	const response = await worker.fetch(
		brokerRequest("/records/delete", {
			recordId: RECORD_ID,
			leaseId: LEASE_ID,
			name: NAME,
			type: "A",
			address: ADDRESS,
		}),
		env,
	);
	assert.equal(response.status, 200);
	assert.equal((await bodyOf(response)).deletedId, null);
	assert.equal(calls.length, 2);
});

test("normalizes public IPv6 and rejects non-public address families", () => {
	assert.equal(
		requirePublicAddress("2606:4700:4700:0000:0000:0000:0000:1111", "AAAA"),
		"2606:4700:4700::1111",
	);
	assert.throws(() => requirePublicAddress("::1", "AAAA"));
	assert.throws(() => requirePublicAddress("fc00::1", "AAAA"));
	assert.throws(() => requirePublicAddress("203.0.113.1", "A"));
	assert.throws(() => requirePublicAddress("8.8.8.8", "AAAA"));
	assert.equal(
		parseManagedDnsRecord(
			managedRecord({
				type: "AAAA",
				content: "2606:4700:4700:0:0:0:0:1111",
			}),
		).content,
		"2606:4700:4700::1111",
	);
});

test("does not expose Cloudflare error bodies", async () => {
	const upstreamSecret = "upstream-secret-diagnostic";
	const { worker, calls, logs } = harness(() =>
		calls.length === 1
			? zoneEnvelope()
			: new Response(
					JSON.stringify({
						success: false,
						result: null,
						errors: [{ message: upstreamSecret }],
					}),
					{ status: 403, headers: { "Content-Type": "application/json" } },
				),
	);
	const response = await worker.fetch(
		brokerRequest("/records/get", { recordId: RECORD_ID }),
		env,
	);
	assert.equal(response.status, 502);
	const serialized = JSON.stringify([await bodyOf(response), logs]);
	assert.doesNotMatch(serialized, new RegExp(upstreamSecret));
	assert.equal(logs.length, 1);
});
