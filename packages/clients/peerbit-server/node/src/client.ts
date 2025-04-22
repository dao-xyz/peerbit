import { type PeerId } from "@libp2p/interface";
import { multiaddr } from "@multiformats/multiaddr";
import {
	Ed25519Keypair,
	type Ed25519PublicKey,
	type Identity,
	PublicSignKey,
	getPublicKeyFromPeerId,
} from "@peerbit/crypto";
import type { Address } from "@peerbit/program";
import { waitForResolved } from "@peerbit/time";
import type { RemoteOrigin } from "./remotes.js";
import {
	ADDRESS_PATH,
	BOOTSTRAP_PATH,
	INSTALL_PATH,
	LOCAL_API_PORT,
	LOG_PATH, // <-- Added the log route constant
	PEER_ID_PATH,
	PROGRAMS_PATH,
	PROGRAM_PATH,
	PROGRAM_VARIANTS_PATH,
	REMOTE_API_PORT,
	RESTART_PATH,
	STOP_PATH,
	TRUST_PATH,
} from "./routes.js";
import { signRequest } from "./signed-request.js";
import type { InstallDependency, StartProgram } from "./types.js";

export const createClient = async (
	keypair: Identity<Ed25519PublicKey>,
	remote: { address: string; origin?: RemoteOrigin } = {
		address: "http://localhost:" + LOCAL_API_PORT,
	},
) => {
	// Add missing protocol
	let endpoint = remote.address;
	if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
		if (endpoint.endsWith("localhost:") || endpoint.endsWith("localhost")) {
			endpoint = "http://" + endpoint;
		} else {
			endpoint = "https://" + endpoint;
		}
	}

	// Add missing port
	const isLocalHost = endpoint.startsWith("http://localhost");
	if (new URL(endpoint).port === "" && !endpoint.endsWith(":80")) {
		if (isLocalHost) {
			endpoint = endpoint + ":" + LOCAL_API_PORT;
		} else {
			endpoint = endpoint + ":" + REMOTE_API_PORT;
		}
	}

	const { default: axios } = await import("axios");
	const axiosInstance = axios.create();
	axiosInstance.interceptors.request.use(async (config) => {
		const url = new URL(config.url!);
		await signRequest(
			config.headers,
			config.method!,
			url.pathname + url.search,
			config.data,
			keypair,
		);
		return config;
	});

	const validateStatus = (status: number) => {
		return (status >= 200 && status < 300) || status === 404;
	};

	const throwIfNot200 = (resp: { status: number; data: any }) => {
		if (resp.status !== 200) {
			throw new Error(resp.data);
		}
		return resp;
	};

	const getId = async () =>
		throwIfNot200(
			await axiosInstance.get(endpoint + PEER_ID_PATH, {
				validateStatus,
				timeout: 5000,
			}),
		).data;

	const close = async (address: string) => {
		return throwIfNot200(
			await axiosInstance.delete(
				endpoint + PROGRAM_PATH + "/" + encodeURIComponent(address.toString()),
				{
					validateStatus,
				},
			),
		);
	};

	const drop = async (address: string) => {
		return throwIfNot200(
			await axiosInstance.delete(
				endpoint +
					PROGRAM_PATH +
					"/" +
					encodeURIComponent(address.toString()) +
					"?delete=true",
				{
					validateStatus,
				},
			),
		);
	};

	return {
		peer: {
			id: {
				get: getId,
			},
			addresses: {
				get: async () => {
					return (
						throwIfNot200(
							await axiosInstance.get(endpoint + ADDRESS_PATH, {
								validateStatus,
							}),
						).data as string[]
					).map((x) => multiaddr(x));
				},
			},
		},

		access: {
			allow: async (key: PublicSignKey | PeerId | string) => {
				const result = await axiosInstance.put(
					endpoint +
						TRUST_PATH +
						"/" +
						encodeURIComponent(
							typeof key === "string"
								? key
								: key instanceof PublicSignKey
									? key.hashcode()
									: getPublicKeyFromPeerId(key).hashcode(),
						),
					undefined,
					{ validateStatus },
				);
				if (result.status !== 200 && result.status !== 404) {
					throw new Error(result.data);
				}
				return result.status === 200 ? true : false;
			},
			deny: async (key: PublicSignKey | PeerId) => {
				const result = await axiosInstance.delete(
					endpoint +
						TRUST_PATH +
						"/" +
						encodeURIComponent(
							key instanceof PublicSignKey
								? key.hashcode()
								: getPublicKeyFromPeerId(key).hashcode(),
						),
					{ validateStatus },
				);
				if (result.status !== 200 && result.status !== 404) {
					throw new Error(result.data);
				}
				return result.status === 200 ? true : false;
			},
		},
		program: {
			has: async (address: Address | string): Promise<boolean> => {
				const result = await axiosInstance.head(
					endpoint +
						PROGRAM_PATH +
						"/" +
						encodeURIComponent(address.toString()),
					{ validateStatus },
				);
				if (result.status !== 200 && result.status !== 404) {
					throw new Error(result.data);
				}
				return result.status === 200 ? true : false;
			},

			open: async (args: StartProgram): Promise<Address> => {
				const resp = throwIfNot200(
					await axiosInstance.put(
						endpoint + PROGRAM_PATH,
						JSON.stringify(args),
						{
							validateStatus,
						},
					),
				);
				return resp.data as string;
			},

			close: async (address: string): Promise<void> => {
				await close(address);
			},

			closeAll: async (): Promise<void> => {
				const resp = throwIfNot200(
					await axiosInstance.get(endpoint + PROGRAMS_PATH, {
						validateStatus,
					}),
				);
				await Promise.all(resp.data.map((address: string) => close(address)));
			},

			drop: async (address: string): Promise<void> => {
				await drop(address);
			},

			dropAll: async (): Promise<void> => {
				const resp = throwIfNot200(
					await axiosInstance.get(endpoint + PROGRAMS_PATH, {
						validateStatus,
					}),
				);
				await Promise.all(resp.data.map((address: string) => drop(address)));
			},

			list: async (): Promise<string[]> => {
				const resp = throwIfNot200(
					await axiosInstance.get(endpoint + PROGRAMS_PATH, {
						validateStatus,
					}),
				);
				return resp.data as string[];
			},
			variants: async (): Promise<string[]> => {
				const resp = throwIfNot200(
					await axiosInstance.get(endpoint + PROGRAM_VARIANTS_PATH, {
						validateStatus,
					}),
				);
				return resp.data as string[];
			},
		},
		dependency: {
			install: async (instruction: InstallDependency): Promise<string[]> => {
				const resp = await axiosInstance.put(
					endpoint + INSTALL_PATH,
					JSON.stringify(instruction),
					{
						validateStatus,
					},
				);
				if (resp.status !== 200) {
					throw new Error(
						typeof resp.data === "string" ? resp.data : resp.data.toString(),
					);
				}
				return resp.data;
			},
		},
		network: {
			bootstrap: async (): Promise<void> => {
				throwIfNot200(
					await axiosInstance.post(endpoint + BOOTSTRAP_PATH, undefined, {
						validateStatus,
					}),
				);
			},
		},

		restart: async (): Promise<void> => {
			throwIfNot200(
				await axiosInstance.post(endpoint + RESTART_PATH, undefined, {
					validateStatus,
				}),
			);
		},
		stop: async (): Promise<void> => {
			throwIfNot200(
				await axiosInstance.post(endpoint + STOP_PATH, undefined, {
					validateStatus,
				}),
			);
		},
		log: {
			/**
			 * Fetches the log from the server.
			 * @param n Optional number of last lines to return.
			 * @returns The log content as a string.
			 */
			fetch: async (n?: number): Promise<string> => {
				// Build the URL to the log endpoint, adding the query parameter if n is provided.
				const url = endpoint + LOG_PATH + (n !== undefined ? `?n=${n}` : "");
				const resp = throwIfNot200(
					await axiosInstance.get(url, {
						validateStatus,
					}),
				);
				return resp.data as string;
			},
		},

		terminate: async () => {
			const { terminateNode } = await import("./aws.js");
			if (remote.origin?.type === "aws") {
				await terminateNode({
					instanceId: remote.origin.instanceId,
					region: remote.origin.region,
				});
			}
		},
	};
};

export const waitForDomain = async (
	ip: string,
	timeout: number = 5 * 60 * 1000,
): Promise<string> => {
	const c = await createClient(await Ed25519Keypair.create(), {
		address: "http://" + ip + ":" + LOCAL_API_PORT,
	});
	const result = await waitForResolved(
		async () => {
			const addresses = await c.peer.addresses.get();
			const domain = multiaddr(addresses[0]).nodeAddress().address;
			if (!domain) {
				throw new Error("Not ready");
			}
			return domain;
		},
		{
			delayInterval: 5000,
			timeout,
		},
	);
	if (!result) {
		throw new Error("Failed to resolve domain");
	}
	return result;
};
