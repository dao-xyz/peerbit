import assert from "node:assert/strict";
import test from "node:test";
import { createSiteBuildEnvironment } from "./buildSite.mjs";
import {
	resolveSubscribeEndpoint,
	validateSiteDeployment,
} from "./validateSiteDeployment.mjs";

const healthyResponse = () =>
	new Response("ok", {
		status: 200,
		headers: {
			"Access-Control-Allow-Origin": "https://peerbit.org",
			"Access-Control-Allow-Methods": "GET,POST,OPTIONS",
			"Access-Control-Allow-Headers": "Content-Type, Authorization",
		},
	});

test("leaves signup disabled when no endpoint is configured", async () => {
	let touchedNetwork = false;
	const result = await validateSiteDeployment({
		env: {},
		lookupHost: async () => {
			touchedNetwork = true;
			return [];
		},
		fetchImpl: async () => {
			touchedNetwork = true;
			return healthyResponse();
		},
	});

	assert.deepEqual(result, { enabled: false });
	assert.equal(touchedNetwork, false);
});

test("derives and validates the public subscribe endpoint from the sync URL", async () => {
	let requestedUrl;
	let requestedInit;
	const result = await validateSiteDeployment({
		env: {
			SUPABASE_UPDATES_SYNC_URL:
				"https://project.supabase.co/functions/v1/updates-sync",
		},
		lookupHost: async (hostname) => {
			assert.equal(hostname, "project.supabase.co");
			return [{ address: "192.0.2.10", family: 4 }];
		},
		fetchImpl: async (url, init) => {
			requestedUrl = url;
			requestedInit = init;
			return healthyResponse();
		},
	});

	assert.equal(
		requestedUrl,
		"https://project.supabase.co/functions/v1/updates-subscribe",
	);
	assert.equal(requestedInit.method, "OPTIONS");
	assert.equal(requestedInit.headers.Origin, "https://peerbit.org");
	assert.deepEqual(result, {
		enabled: true,
		endpoint: "https://project.supabase.co/functions/v1/updates-subscribe",
		addresses: [{ address: "192.0.2.10", family: 4 }],
	});
});

test("uses the derived subscribe endpoint in the effective Vite build environment", () => {
	const processEnv = {
		PATH: "/test/bin",
		SUPABASE_UPDATES_SYNC_URL:
			"https://project.supabase.co/functions/v1/updates-sync",
	};
	const result = createSiteBuildEnvironment({
		processEnv,
		loadEnvImpl: () => ({}),
	});

	assert.equal(
		result.endpoint.href,
		"https://project.supabase.co/functions/v1/updates-subscribe",
	);
	assert.equal(
		result.buildEnv.VITE_UPDATES_EMAIL_FORM_ACTION,
		result.endpoint.href,
	);
	assert.equal(
		result.buildEnv.SUPABASE_UPDATES_SYNC_URL,
		processEnv.SUPABASE_UPDATES_SYNC_URL,
	);
	assert.equal(result.buildEnv.PATH, "/test/bin");
	assert.equal(processEnv.VITE_UPDATES_EMAIL_FORM_ACTION, undefined);
});

test("accepts matching explicit subscribe and sync endpoints", () => {
	const endpoint = resolveSubscribeEndpoint({
		VITE_UPDATES_EMAIL_FORM_ACTION:
			"https://project.supabase.co/functions/v1/updates-subscribe",
		SUPABASE_UPDATES_SYNC_URL:
			"https://project.supabase.co/functions/v1/updates-sync",
	});

	assert.equal(
		endpoint.href,
		"https://project.supabase.co/functions/v1/updates-subscribe",
	);
});

test("rejects malformed or unsafe endpoint configuration", () => {
	const cases = [
		{
			env: { SUPABASE_UPDATES_SYNC_URL: "not a url" },
			message: /valid absolute URL/,
		},
		{
			env: {
				SUPABASE_UPDATES_SYNC_URL:
					" https://project.supabase.co/functions/v1/updates-sync",
			},
			message: /must not contain leading or trailing whitespace/,
		},
		{
			env: {
				SUPABASE_UPDATES_SYNC_URL:
					"http://project.supabase.co/functions/v1/updates-sync",
			},
			message: /must use HTTPS/,
		},
		{
			env: {
				SUPABASE_UPDATES_SYNC_URL:
					"https://user:secret@project.supabase.co/functions/v1/updates-sync",
			},
			message: /must not contain credentials/,
		},
		{
			env: {
				SUPABASE_UPDATES_SYNC_URL:
					"https://project.supabase.co/functions/v1/updates-sync?token=secret",
			},
			message: /must not contain a query string/,
		},
		{
			env: {
				SUPABASE_UPDATES_SYNC_URL:
					"https://project.supabase.co/functions/v1/updates-subscribe",
			},
			message: /updates-sync/,
		},
		{
			env: {
				VITE_UPDATES_EMAIL_FORM_ACTION:
					"https://project.supabase.co/functions/v1/updates-sync",
			},
			message: /updates-subscribe/,
		},
	];

	for (const { env, message } of cases) {
		assert.throws(() => resolveSubscribeEndpoint(env), message);
	}
});

test("rejects endpoints derived from different origins", () => {
	assert.throws(
		() =>
			resolveSubscribeEndpoint({
				VITE_UPDATES_EMAIL_FORM_ACTION:
					"https://one.supabase.co/functions/v1/updates-subscribe",
				SUPABASE_UPDATES_SYNC_URL:
					"https://two.supabase.co/functions/v1/updates-sync",
			}),
		/same origin/,
	);
});

test("fails closed when DNS resolution fails or returns no addresses", async () => {
	const env = {
		VITE_UPDATES_EMAIL_FORM_ACTION:
			"https://missing.supabase.co/functions/v1/updates-subscribe",
	};

	await assert.rejects(
		validateSiteDeployment({
			env,
			lookupHost: async () => {
				throw new Error("ENOTFOUND");
			},
		}),
		/does not resolve: ENOTFOUND/,
	);
	await assert.rejects(
		validateSiteDeployment({ env, lookupHost: async () => [] }),
		/did not resolve to an address/,
	);
});

test("fails closed when the endpoint cannot be reached or is not healthy", async () => {
	const env = {
		VITE_UPDATES_EMAIL_FORM_ACTION:
			"https://project.supabase.co/functions/v1/updates-subscribe",
	};
	const lookupHost = async () => [{ address: "192.0.2.10", family: 4 }];

	await assert.rejects(
		validateSiteDeployment({
			env,
			lookupHost,
			fetchImpl: async () => {
				throw new Error("connection refused");
			},
		}),
		/health check failed: connection refused/,
	);
	await assert.rejects(
		validateSiteDeployment({
			env,
			lookupHost,
			fetchImpl: async () => new Response("not found", { status: 404 }),
		}),
		/returned HTTP 404/,
	);
});

test("fails closed when the endpoint health check times out", async () => {
	const env = {
		VITE_UPDATES_EMAIL_FORM_ACTION:
			"https://project.supabase.co/functions/v1/updates-subscribe",
	};

	await assert.rejects(
		validateSiteDeployment({
			env,
			lookupHost: async () => [{ address: "192.0.2.10", family: 4 }],
			fetchImpl: async (_url, { signal }) =>
				new Promise((_resolve, reject) => {
					signal.addEventListener(
						"abort",
						() => reject(new Error("request aborted")),
						{ once: true },
					);
				}),
			timeoutMs: 5,
		}),
		/health check timed out after 5ms/,
	);
});

test("fails closed when production CORS is not enabled", async () => {
	const env = {
		VITE_UPDATES_EMAIL_FORM_ACTION:
			"https://project.supabase.co/functions/v1/updates-subscribe",
	};
	const lookupHost = async () => [{ address: "192.0.2.10", family: 4 }];

	await assert.rejects(
		validateSiteDeployment({
			env,
			lookupHost,
			fetchImpl: async () =>
				new Response("ok", {
					status: 200,
					headers: { "Access-Control-Allow-Methods": "POST" },
				}),
		}),
		/does not allow the production origin/,
	);
	await assert.rejects(
		validateSiteDeployment({
			env,
			lookupHost,
			fetchImpl: async () =>
				new Response("ok", {
					status: 200,
					headers: {
						"Access-Control-Allow-Origin": "https://peerbit.org",
						"Access-Control-Allow-Methods": "GET,OPTIONS",
					},
				}),
		}),
		/CORS policy does not allow POST/,
	);
});

test("fails closed when CORS does not allow the JSON content-type header", async () => {
	const env = {
		VITE_UPDATES_EMAIL_FORM_ACTION:
			"https://project.supabase.co/functions/v1/updates-subscribe",
	};
	const lookupHost = async () => [{ address: "192.0.2.10", family: 4 }];
	const headers = {
		"Access-Control-Allow-Origin": "https://peerbit.org",
		"Access-Control-Allow-Methods": "GET,POST,OPTIONS",
	};

	await assert.rejects(
		validateSiteDeployment({
			env,
			lookupHost,
			fetchImpl: async () => new Response("ok", { status: 200, headers }),
		}),
		/CORS policy does not allow the content-type header/,
	);
	await assert.rejects(
		validateSiteDeployment({
			env,
			lookupHost,
			fetchImpl: async () =>
				new Response("ok", {
					status: 200,
					headers: {
						...headers,
						"Access-Control-Allow-Headers": "Authorization, X-Request-ID",
					},
				}),
		}),
		/CORS policy does not allow the content-type header/,
	);
});
