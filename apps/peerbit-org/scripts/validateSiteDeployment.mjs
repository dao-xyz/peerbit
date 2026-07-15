import { lookup } from "node:dns/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadEnv } from "vite";

const SITE_ORIGIN = "https://peerbit.org";
const SUBSCRIBE_PATH = "/functions/v1/updates-subscribe";
const SYNC_PATH = "/functions/v1/updates-sync";
const APP_DIRECTORY = path.resolve(
	path.dirname(fileURLToPath(import.meta.url)),
	"..",
);

function requiredEndpointUrl(value, variable, expectedPath) {
	let url;
	try {
		url = new URL(value);
	} catch {
		throw new Error(`${variable} must be a valid absolute URL.`);
	}

	if (url.protocol !== "https:") {
		throw new Error(`${variable} must use HTTPS.`);
	}
	if (url.username || url.password) {
		throw new Error(`${variable} must not contain credentials.`);
	}
	if (url.port && url.port !== "443") {
		throw new Error(`${variable} must use the default HTTPS port.`);
	}
	if (url.search || url.hash) {
		throw new Error(`${variable} must not contain a query string or fragment.`);
	}
	if (url.pathname !== expectedPath) {
		throw new Error(`${variable} must end with exactly ${expectedPath}.`);
	}

	return url;
}

export function resolveSubscribeEndpoint(env = process.env) {
	const configuredSubscribe = env.VITE_UPDATES_EMAIL_FORM_ACTION ?? "";
	const configuredSync = env.SUPABASE_UPDATES_SYNC_URL ?? "";
	if (configuredSubscribe !== configuredSubscribe.trim()) {
		throw new Error(
			"VITE_UPDATES_EMAIL_FORM_ACTION must not contain leading or trailing whitespace.",
		);
	}
	if (configuredSync !== configuredSync.trim()) {
		throw new Error(
			"SUPABASE_UPDATES_SYNC_URL must not contain leading or trailing whitespace.",
		);
	}

	const subscribeUrl = configuredSubscribe
		? requiredEndpointUrl(
				configuredSubscribe,
				"VITE_UPDATES_EMAIL_FORM_ACTION",
				SUBSCRIBE_PATH,
			)
		: undefined;
	const syncUrl = configuredSync
		? requiredEndpointUrl(
				configuredSync,
				"SUPABASE_UPDATES_SYNC_URL",
				SYNC_PATH,
			)
		: undefined;
	const derivedSubscribeUrl = syncUrl
		? new URL(SUBSCRIBE_PATH, syncUrl.origin)
		: undefined;

	if (
		subscribeUrl &&
		derivedSubscribeUrl &&
		subscribeUrl.href !== derivedSubscribeUrl.href
	) {
		throw new Error(
			"VITE_UPDATES_EMAIL_FORM_ACTION and SUPABASE_UPDATES_SYNC_URL must use the same origin.",
		);
	}

	return subscribeUrl ?? derivedSubscribeUrl;
}

export async function validateSiteDeployment({
	env = process.env,
	lookupHost = (hostname) => lookup(hostname, { all: true }),
	fetchImpl = globalThis.fetch,
	timeoutMs = 10_000,
} = {}) {
	const endpoint = resolveSubscribeEndpoint(env);
	if (!endpoint) return { enabled: false };

	let addresses;
	try {
		addresses = await lookupHost(endpoint.hostname);
	} catch (error) {
		const detail = error instanceof Error ? error.message : String(error);
		throw new Error(
			`Updates signup endpoint host ${endpoint.hostname} does not resolve: ${detail}`,
		);
	}
	if (!Array.isArray(addresses) || addresses.length === 0) {
		throw new Error(
			`Updates signup endpoint host ${endpoint.hostname} did not resolve to an address.`,
		);
	}

	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), timeoutMs);

	let response;
	try {
		response = await fetchImpl(endpoint.href, {
			method: "OPTIONS",
			headers: {
				Origin: SITE_ORIGIN,
				"Access-Control-Request-Method": "POST",
				"Access-Control-Request-Headers": "content-type",
			},
			redirect: "error",
			signal: controller.signal,
		});
	} catch (error) {
		if (controller.signal.aborted) {
			throw new Error(
				`Updates signup endpoint health check timed out after ${timeoutMs}ms.`,
			);
		}
		const detail = error instanceof Error ? error.message : String(error);
		throw new Error(`Updates signup endpoint health check failed: ${detail}`);
	} finally {
		clearTimeout(timeout);
	}

	if (!response.ok) {
		throw new Error(
			`Updates signup endpoint health check returned HTTP ${response.status}.`,
		);
	}

	const allowedOrigin = response.headers.get("access-control-allow-origin");
	if (allowedOrigin !== SITE_ORIGIN) {
		throw new Error(
			`Updates signup endpoint does not allow the production origin ${SITE_ORIGIN}.`,
		);
	}
	const allowedMethods = (
		response.headers.get("access-control-allow-methods") ?? ""
	)
		.split(",")
		.map((method) => method.trim().toUpperCase());
	if (!allowedMethods.includes("POST")) {
		throw new Error("Updates signup endpoint CORS policy does not allow POST.");
	}

	return { enabled: true, endpoint: endpoint.href, addresses };
}

async function main() {
	const fileEnv = loadEnv("production", APP_DIRECTORY, ["VITE_", "SUPABASE_"]);
	const result = await validateSiteDeployment({
		env: { ...fileEnv, ...process.env },
	});
	if (!result.enabled) {
		process.stdout.write(
			"Updates signup is disabled; no service endpoint will be embedded.\n",
		);
		return;
	}
	process.stdout.write(
		`Updates signup endpoint is healthy: ${result.endpoint}\n`,
	);
}

const entrypoint = process.argv[1]
	? pathToFileURL(path.resolve(process.argv[1])).href
	: undefined;
if (entrypoint === import.meta.url) {
	try {
		await main();
	} catch (error) {
		process.stderr.write(
			`${error instanceof Error ? error.message : String(error)}\n`,
		);
		process.exitCode = 1;
	}
}
