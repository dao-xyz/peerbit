import { InstallDependency, StartProgram } from "./types.js";
import {
	ADDRESS_PATH,
	BOOTSTRAP_PATH,
	STOP_PATH,
	INSTALL_PATH,
	LOCAL_API_PORT,
	PEER_ID_PATH,
	PROGRAMS_PATH,
	PROGRAM_PATH,
	RESTART_PATH,
	TRUST_PATH,
	REMOTE_API_PORT,
} from "./routes.js";
import { Address } from "@peerbit/program";
import { multiaddr } from "@multiformats/multiaddr";
import { signRequest } from "./signes-request.js";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Identity,
	PublicSignKey,
	getPublicKeyFromPeerId,
} from "@peerbit/crypto";
import { PeerId } from "@libp2p/interface/peer-id";
import { waitForResolved } from "@peerbit/time";
import { RemoteOrigin } from "./remotes.js";

export const createClient = async (
	keypair: Identity<Ed25519PublicKey>,
	remote: { address: string; origin?: RemoteOrigin } = {
		address: "http://localhost:" + LOCAL_API_PORT,
	}
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
			keypair
		);
		return config;
	});

	const validateStatus = (status: number) => {
		return (status >= 200 && status < 300) || status == 404;
	};

	const throwIfNot200 = (resp: { status: number; data: any }) => {
		if (resp.status !== 200) {
			throw new Error(resp.data);
		}
		return resp;
	};
	const getBodyByStatus = <
		D extends { toString(): string },
		T extends { status: number; data: D }
	>(
		resp: T
	): D | undefined => {
		if (resp.status === 404) {
			return;
		}
		if (resp.status == 200) {
			return resp.data;
		}
		throw new Error(
			typeof resp.data === "string" ? resp.data : resp.data.toString()
		);
	};
	const getId = async () =>
		throwIfNot200(
			await axiosInstance.get(endpoint + PEER_ID_PATH, {
				validateStatus,
				timeout: 5000,
			})
		).data;

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
							})
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
								: getPublicKeyFromPeerId(key).hashcode()
						),
					undefined,
					{ validateStatus }
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
								: getPublicKeyFromPeerId(key).hashcode()
						),
					{ validateStatus }
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
					{ validateStatus }
				);
				if (result.status !== 200 && result.status !== 404) {
					throw new Error(result.data);
				}
				return result.status === 200 ? true : false;
			},

			open: async (program: StartProgram): Promise<Address> => {
				const resp = throwIfNot200(
					await axiosInstance.put(
						endpoint + PROGRAM_PATH,
						JSON.stringify(program),
						{
							validateStatus,
						}
					)
				);
				return resp.data as string;
			},

			close: async (address: string): Promise<void> => {
				throwIfNot200(
					await axiosInstance.delete(
						endpoint +
							PROGRAM_PATH +
							"/" +
							encodeURIComponent(address.toString()),
						{
							validateStatus,
						}
					)
				);
			},

			drop: async (address: string): Promise<void> => {
				throwIfNot200(
					await axiosInstance.delete(
						endpoint +
							PROGRAM_PATH +
							"/" +
							encodeURIComponent(address.toString()) +
							"?delete=true",
						{
							validateStatus,
						}
					)
				);
			},

			list: async (): Promise<string[]> => {
				const resp = throwIfNot200(
					await axiosInstance.get(endpoint + PROGRAMS_PATH, {
						validateStatus,
					})
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
					}
				);
				if (resp.status !== 200) {
					throw new Error(
						typeof resp.data === "string" ? resp.data : resp.data.toString()
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
					})
				);
			},
		},

		restart: async (): Promise<void> => {
			throwIfNot200(
				await axiosInstance.post(endpoint + RESTART_PATH, undefined, {
					validateStatus,
				})
			);
		},
		stop: async (): Promise<void> => {
			throwIfNot200(
				await axiosInstance.post(endpoint + STOP_PATH, undefined, {
					validateStatus,
				})
			);
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
	timeout: number = 5 * 60 * 1000
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
		}
	);
	if (!result) {
		throw new Error("Failed to resolve domain");
	}
	return result;
};
