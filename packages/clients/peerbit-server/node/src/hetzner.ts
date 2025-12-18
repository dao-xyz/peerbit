/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/naming-convention */
import { type PeerId } from "@libp2p/interface";
import { delay } from "@peerbit/time";

export const HETZNER_LOCATIONS = [
	"fsn1",
	"nbg1",
	"hel1",
	"ash",
	"hil",
] as const;
export type HetznerLocation = (typeof HETZNER_LOCATIONS)[number];

export const HETZNER_SERVER_TYPES = [
	"cx11",
	"cx21",
	"cx31",
	"cx41",
	"cx51",
	"cax11",
	"cax21",
	"cax31",
	"cax41",
] as const;
export type HetznerServerType = (typeof HETZNER_SERVER_TYPES)[number];

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

const setupUserData = (
	email: string,
	grantAccess: PeerId[] = [],
	serverVersion?: string,
) => {
	const peerIdStrings = grantAccess.map((x) => x.toString());
	const grantArgs = peerIdStrings.map((key) => `--ga ${key}`).join(" ");
	const versionSpec = serverVersion ? `@${serverVersion}` : "";

	// better-sqlite3 forces users to install build-essentials for `make` command
	return `#!/bin/bash
set -e
cd /root
curl -fsSL https://deb.nodesource.com/setup_22.x | bash - &&\
apt-get install -y nodejs
apt-get install -y build-essential
npm install -g @peerbit/server${versionSpec}
peerbit domain test --email ${email}
peerbit start ${grantArgs} > log.txt 2>&1 &
`;
};

type HcloudServer = {
	id: number;
	name: string;
	public_net?: { ipv4?: { ip?: string } };
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

const escapeRegExp = (value: string): string =>
	value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const getHighestNameCounter = (
	servers: HcloudServer[],
	prefix: string,
): number => {
	const pattern = new RegExp(`^${escapeRegExp(prefix)}-(\\d+)$`);
	let max = 0;
	for (const server of servers) {
		const match = server.name.match(pattern);
		if (!match) continue;
		const n = Number(match[1]);
		if (Number.isFinite(n)) {
			max = Math.max(max, n);
		}
	}
	return max;
};

const waitForServerPublicIp = async (
	client: Awaited<ReturnType<typeof createHcloudClient>>,
	serverId: number,
	timeoutMs = 3 * 60 * 1000,
	pollIntervalMs = 5000,
): Promise<string> => {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		try {
			const info = await client.get(`/servers/${serverId}`);
			const server: HcloudServer | undefined = info.data?.server;
			const ip = server?.public_net?.ipv4?.ip;
			if (ip) {
				return ip;
			}
		} catch (error: any) {
			if (error?.response?.status !== 404 && error?.response?.status !== 429) {
				throw new Error(
					`Failed while waiting for Hetzner server ${serverId} IP: ${parseAxiosError(
						error,
					)}`,
				);
			}
		}
		await delay(pollIntervalMs);
	}
	throw new Error(
		`Timed out waiting for Hetzner server ${serverId} to get a public IPv4`,
	);
};

export const launchNodes = async (properties: {
	token?: string;
	location?: string;
	email: string;
	count?: number;
	serverType?: string;
	namePrefix?: string;
	grantAccess?: PeerId[];
	serverVersion?: string;
	image?: string;
}): Promise<
	{ serverId: number; publicIp: string; name: string; location: string }[]
> => {
	if (properties.count && properties.count > 10) {
		throw new Error(
			"Unexpected node launch count: " +
				properties.count +
				". To prevent unwanted behaviour you can also launch 10 nodes at once",
		);
	}
	const count = properties.count || 1;

	const token = getToken(properties.token);
	const client = await createHcloudClient(token);

	const location = properties.location || "fsn1";
	const serverType = properties.serverType || "cx11";
	const image = properties.image || "ubuntu-22.04";
	const namePrefix = properties.namePrefix || "peerbit-node";

	const existingServers = (await client.get("/servers")).data?.servers as
		| HcloudServer[]
		| undefined;
	const existingCounter = getHighestNameCounter(
		existingServers || [],
		namePrefix,
	);

	const created: Array<{ serverId: number; name: string }> = [];
	for (let ix = 1; ix <= count; ix++) {
		const name = `${namePrefix}-${existingCounter + ix}`;
		try {
			const resp = await client.post("/servers", {
				name,
				server_type: serverType,
				image,
				location,
				user_data: setupUserData(
					properties.email,
					properties.grantAccess,
					properties.serverVersion,
				),
			});
			const serverId: number | undefined = resp.data?.server?.id;
			if (!serverId) {
				throw new Error("Missing server id in Hetzner response");
			}
			created.push({ serverId, name });
		} catch (error: any) {
			throw new Error(
				`Failed to create Hetzner server '${name}': ${parseAxiosError(error)}`,
			);
		}
	}

	const nodes: Array<{
		serverId: number;
		publicIp: string;
		name: string;
		location: string;
	}> = [];
	for (const { serverId, name } of created) {
		const publicIp = await waitForServerPublicIp(client, serverId);
		nodes.push({ serverId, publicIp, name, location });
	}
	return nodes;
};

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
