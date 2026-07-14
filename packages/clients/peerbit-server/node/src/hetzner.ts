const HCLOUD_API_BASE = "https://api.hetzner.cloud/v1";

const getToken = (token?: string): string => {
	const resolved =
		token ||
		process.env.HCLOUD_TOKEN ||
		process.env.HETZNER_TOKEN ||
		process.env.HETZNER_CLOUD_TOKEN;
	if (!resolved) {
		throw new Error(
			"Missing Hetzner Cloud API token. Provide --token or set HCLOUD_TOKEN.",
		);
	}
	return resolved;
};

const createHcloudClient = async (token: string) => {
	const { default: axios } = await import("axios");
	return axios.create({
		baseURL: HCLOUD_API_BASE,
		headers: { Authorization: `Bearer ${token}` },
		timeout: 60_000,
	});
};

const parseAxiosError = (error: any): string => {
	const status = error?.response?.status;
	const data = error?.response?.data;
	const message = error?.message || String(error);
	if (!status) {
		return message;
	}
	const details =
		typeof data === "string"
			? data
			: data?.error?.message || JSON.stringify(data);
	return `${message} (HTTP ${status})${details ? `: ${details}` : ""}`;
};

/**
 * Retained so users can clean up Hetzner servers recorded by older releases.
 * Creating new provider-managed servers is no longer supported.
 */
export const terminateNode = async (properties: {
	token?: string;
	serverId: number | string;
}) => {
	const token = getToken(properties.token);
	const client = await createHcloudClient(token);
	try {
		await client.delete(`/servers/${properties.serverId}`);
	} catch (error: any) {
		if (error?.response?.status === 404) {
			return;
		}
		throw new Error(
			`Failed to terminate Hetzner server ${properties.serverId}: ${parseAxiosError(
				error,
			)}`,
		);
	}
};
