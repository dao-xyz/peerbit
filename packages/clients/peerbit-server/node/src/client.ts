import { InstallDependency, StartProgram } from "./types.js";
import {
	ADDRESS_PATH,
	BOOTSTRAP_PATH,
	TERMINATE_PATH,
	INSTALL_PATH,
	LOCAL_PORT,
	PEER_ID_PATH,
	PROGRAMS_PATH,
	PROGRAM_PATH,
	RESTART_PATH,
	TRUST_PATH,
} from "./routes.js";
import { Address } from "@peerbit/program";
import { multiaddr } from "@multiformats/multiaddr";
import { signRequest } from "./signes-request.js";
import {
	Ed25519PublicKey,
	Identity,
	PublicSignKey,
	getPublicKeyFromPeerId,
} from "@peerbit/crypto";
import { PeerId } from "@libp2p/interface/peer-id";

export const client = async (
	keypair: Identity<Ed25519PublicKey>,
	endpoint: string = "http://localhost:" + LOCAL_PORT
) => {
	const isLocalHost = endpoint.startsWith("http://localhost");
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
		trust: {
			add: async (key: PublicSignKey | PeerId) => {
				const result = await axiosInstance.put(
					endpoint +
						TRUST_PATH +
						"/" +
						encodeURIComponent(
							key instanceof PublicSignKey
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
			remove: async (key: PublicSignKey | PeerId) => {
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
		terminate: async (): Promise<void> => {
			throwIfNot200(
				await axiosInstance.post(endpoint + TERMINATE_PATH, undefined, {
					validateStatus,
				})
			);
		},
	};
};
