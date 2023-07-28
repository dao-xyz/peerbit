import { StartByBase64, StartByVariant, StartProgram } from "./types.js";
import {
	ADDRESS_PATH,
	BOOTSTRAP_PATH,
	INSTALL_PATH,
	LOCAL_PORT,
	PEER_ID_PATH,
	PROGRAMS_PATH,
	PROGRAM_PATH,
} from "./routes.js";
import { Address } from "@peerbit/program";
import { multiaddr } from "@multiformats/multiaddr";

export const client = async (
	endpoint: string = "http://localhost:" + LOCAL_PORT
) => {
	const { default: axios } = await import("axios");

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
		throwIfNot200(await axios.get(endpoint + PEER_ID_PATH, { validateStatus }))
			.data;

	const getHeaders = async () => {
		const config = await import("./config.js");
		const headers = {
			authorization: "Basic admin:" + (await config.loadPassword()),
		};
		return headers;
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
							await axios.get(endpoint + ADDRESS_PATH, {
								validateStatus,
							})
						).data as string[]
					).map((x) => multiaddr(x));
				},
			},
		},
		program: {
			has: async (address: Address | string): Promise<boolean> => {
				const result = await axios.head(
					endpoint +
						PROGRAM_PATH +
						"/" +
						encodeURIComponent(address.toString()),
					{ validateStatus, headers: await getHeaders() }
				);
				if (result.status !== 200 && result.status !== 404) {
					throw new Error(result.data);
				}
				return result.status === 200 ? true : false;
			},

			open: async (program: StartProgram): Promise<Address> => {
				const resp = throwIfNot200(
					await axios.put(endpoint + PROGRAM_PATH, JSON.stringify(program), {
						validateStatus,
						headers: await getHeaders(),
					})
				);
				return resp.data as string;
			},

			close: async (address: string): Promise<void> => {
				throwIfNot200(
					await axios.delete(
						endpoint +
							PROGRAM_PATH +
							"/" +
							encodeURIComponent(address.toString()),
						{
							validateStatus,
							headers: await getHeaders(),
						}
					)
				);
			},

			drop: async (address: string): Promise<void> => {
				throwIfNot200(
					await axios.delete(
						endpoint +
							PROGRAM_PATH +
							"/" +
							encodeURIComponent(address.toString()) +
							"?delete=true",
						{
							validateStatus,
							headers: await getHeaders(),
						}
					)
				);
			},

			list: async (): Promise<string[]> => {
				const resp = throwIfNot200(
					await axios.get(endpoint + PROGRAMS_PATH, {
						validateStatus,
						headers: await getHeaders(),
					})
				);
				return resp.data as string[];
			},
		},
		dependency: {
			install: async (name: string): Promise<string[]> => {
				const resp = await axios.put(endpoint + INSTALL_PATH, name, {
					validateStatus,
					headers: await getHeaders(),
				});
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
					await axios.post(endpoint + BOOTSTRAP_PATH, undefined, {
						validateStatus,
						headers: await getHeaders(),
					})
				);
			},
		},
	};
};
