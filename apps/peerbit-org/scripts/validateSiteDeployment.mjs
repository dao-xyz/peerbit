import { lookup } from "node:dns/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
	SITE_ORIGIN,
	loadSiteDeploymentEnvironment,
	resolveSubscribeEndpoint,
} from "./siteDeploymentEnvironment.mjs";

export { resolveSubscribeEndpoint };

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
	const allowedHeaders = (
		response.headers.get("access-control-allow-headers") ?? ""
	)
		.split(",")
		.map((header) => header.trim().toLowerCase());
	if (!allowedHeaders.includes("content-type")) {
		throw new Error(
			"Updates signup endpoint CORS policy does not allow the content-type header.",
		);
	}

	return { enabled: true, endpoint: endpoint.href, addresses };
}

async function main() {
	const { env } = loadSiteDeploymentEnvironment();
	const result = await validateSiteDeployment({
		env,
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
