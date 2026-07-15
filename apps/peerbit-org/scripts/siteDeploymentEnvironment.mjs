import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadEnv } from "vite";

export const SITE_ORIGIN = "https://peerbit.org";
export const SUBSCRIBE_PATH = "/functions/v1/updates-subscribe";
export const SYNC_PATH = "/functions/v1/updates-sync";
export const SUBSCRIBE_VARIABLE = "VITE_UPDATES_EMAIL_FORM_ACTION";
export const SYNC_VARIABLE = "SUPABASE_UPDATES_SYNC_URL";
export const APP_DIRECTORY = path.resolve(
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
	const configuredSubscribe = env[SUBSCRIBE_VARIABLE] ?? "";
	const configuredSync = env[SYNC_VARIABLE] ?? "";
	if (configuredSubscribe !== configuredSubscribe.trim()) {
		throw new Error(
			`${SUBSCRIBE_VARIABLE} must not contain leading or trailing whitespace.`,
		);
	}
	if (configuredSync !== configuredSync.trim()) {
		throw new Error(
			`${SYNC_VARIABLE} must not contain leading or trailing whitespace.`,
		);
	}

	const subscribeUrl = configuredSubscribe
		? requiredEndpointUrl(
				configuredSubscribe,
				SUBSCRIBE_VARIABLE,
				SUBSCRIBE_PATH,
			)
		: undefined;
	const syncUrl = configuredSync
		? requiredEndpointUrl(configuredSync, SYNC_VARIABLE, SYNC_PATH)
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
			`${SUBSCRIBE_VARIABLE} and ${SYNC_VARIABLE} must use the same origin.`,
		);
	}

	return subscribeUrl ?? derivedSubscribeUrl;
}

export function resolveSiteDeploymentEnvironment(env = process.env) {
	const endpoint = resolveSubscribeEndpoint(env);
	const effectiveEnv = { ...env };
	if (endpoint) effectiveEnv[SUBSCRIBE_VARIABLE] = endpoint.href;
	return { endpoint, env: effectiveEnv };
}

export function loadSiteDeploymentEnvironment({
	mode = "production",
	processEnv = process.env,
	loadEnvImpl = loadEnv,
} = {}) {
	const fileEnv = loadEnvImpl(mode, APP_DIRECTORY, [
		SUBSCRIBE_VARIABLE,
		SYNC_VARIABLE,
	]);
	const configuredEnv = {};
	for (const variable of [SUBSCRIBE_VARIABLE, SYNC_VARIABLE]) {
		if (fileEnv[variable] !== undefined) {
			configuredEnv[variable] = fileEnv[variable];
		}
		if (processEnv[variable] !== undefined) {
			configuredEnv[variable] = processEnv[variable];
		}
	}
	return resolveSiteDeploymentEnvironment(configuredEnv);
}
