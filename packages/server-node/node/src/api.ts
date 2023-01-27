import http from "http";
import { fromBase64, toBase64 } from "@dao-xyz/peerbit-crypto";
import { Peerbit } from "@dao-xyz/peerbit";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { Program, Address } from "@dao-xyz/peerbit-program";
import { multiaddr } from "@multiformats/multiaddr";
import { waitFor } from "@dao-xyz/peerbit-time";
import { v4 as uuid } from "uuid";
import { Libp2p } from "libp2p";
import { getConfigDir, getCredentialsPath, NotFoundError } from "./config.js";
import { setMaxListeners } from "events";
import { createNode } from "./libp2p.js";
import { Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
export const LOCAL_PORT = 8082;
export const SSL_PORT = 9002;

export const getPort = (protocol: string) => {
	if (protocol === "https:") {
		return SSL_PORT;
	}

	if (protocol === "http:") {
		return LOCAL_PORT;
	}

	throw new Error("Unsupported protocol: " + protocol);
};

const IPFS_ID_PATH = "/peer/id";
const ADDRESSES_PATH = "/peer/addresses";
const TOPICS_PATH = "/topics";
const PROGRAM_PATH = "/program";
const LIBRARY_PATH = "/library";


export const checkExistPath = async (path: string) => {
	const fs = await import("fs");

	try {
		if (!fs.existsSync(path)) {
			fs.accessSync(path, fs.constants.W_OK); // will throw if fails
			return false;
		}
		return true;
	} catch (err: any) {
		if (err.message.indexOf("no such file")) {
			return false;
		}
		throw new Error("Can not access path");
	}
};
export const createPassword = async (): Promise<string> => {
	const fs = await import("fs");
	const configDir = await getConfigDir();
	const credentialsPath = await getCredentialsPath(configDir);
	if (await checkExistPath(credentialsPath)) {
		throw new Error(
			"Config path for credentials: " +
			credentialsPath +
			", already exist"
		);
	}
	console.log(`Creating config folder ${configDir}`);

	fs.mkdirSync(configDir, { recursive: true });
	await waitFor(() => fs.existsSync(configDir));

	console.log(`Created config folder ${configDir}`);

	const password = uuid();
	fs.writeFileSync(
		credentialsPath,
		JSON.stringify({ username: "admin", password })
	);
	console.log(`Created credentials at ${credentialsPath}`);
	return password;
};

export const loadPassword = async (): Promise<string> => {
	const fs = await import("fs");
	const configDir = await getConfigDir();
	const credentialsPath = await getCredentialsPath(configDir);
	if (!(await checkExistPath(credentialsPath))) {
		throw new NotFoundError("Credentials file does not exist");
	}
	const password = JSON.parse(
		fs.readFileSync(credentialsPath, "utf-8")
	).password;
	if (!password || password.length === 0) {
		throw new NotFoundError("Password not found");
	}
	return password;
};

export const loadOrCreatePassword = async (): Promise<string> => {
	try {
		return await loadPassword();
	} catch (error) {
		if (error instanceof NotFoundError) {
			return createPassword();
		}
		throw error;
	}
};
export const startServerWithNode = async (relay: boolean) => {
	const node = await createNode();
	const controller = {
		api: node,
		stop: () => node.stop(),
	};
	const peer = relay
		? controller.api
		: await Peerbit.create({ libp2p: controller.api, directory: ".peerbit/data" });
	const server = await startServer(peer);
	const printNodeInfo = async () => {
		console.log("Starting node with address(es): ");
		const id = await (await client()).peer.id.get();
		console.log("id: " + id);
		console.log("Addresses: ");
		for (const a of await (await client()).peer.addresses.get()) {
			console.log(a.toString());
		}
	};

	await printNodeInfo();
	const shutDownHook = async (
		controller: { stop: () => any },
		server: {
			close: () => void;
		}
	) => {
		const { exit } = await import("process");
		process.on("SIGINT", async () => {
			console.log("Shutting down node");
			await server.close();
			await controller.stop();
			exit();
		});
	};
	await shutDownHook(controller, server);
};
export const startServer = async (
	client: Peerbit | Libp2pExtended,
	port: number = LOCAL_PORT
): Promise<http.Server> => {
	const notPeerBitError =
		"Client is just a Libp2p node, not a full Peerbit client. The command is not supported for this node type";
	const libp2p = client instanceof Peerbit ? client.libp2p : client;
	const password = await loadOrCreatePassword();

	const adminACL = (req: http.IncomingMessage): boolean => {
		const auth = req.headers["authorization"];
		if (!auth?.startsWith("Basic ")) {
			return false;
		}
		const credentials = auth?.substring("Basic ".length);
		const username = credentials.split(":")[0];
		if (username !== "admin") {
			return false;
		}
		if (password !== credentials.substring(username.length + 1)) {
			return false;
		}
		return true;
	};

	const getProgramFromPath = (
		req: http.IncomingMessage,
		pathIndex: number
	): Program | undefined => {
		if (!req.url) {
			throw new Error("Missing url");
		}
		if (client instanceof Peerbit) {
			const url = new URL(req.url, "http://localhost:" + port);
			const path = url.pathname
				.substring(
					Math.min(1, url.pathname.length),
					url.pathname.length
				)
				.split("/");
			if (path.length <= pathIndex) {
				throw new Error("Invalid path");
			}
			const address = decodeURIComponent(path[pathIndex]);
			const p = client.programs.get(address);
			if (p) {
				return p.program;
			}
		} else {
			throw new Error(notPeerBitError);
		}
		return;
	};
	const getBody = (
		req: http.IncomingMessage,
		callback: (body: string) => void
	) => {
		let body = "";
		req.on("data", function (d) {
			body += d;
		});
		req.on("end", function () {
			callback(body);
		});
	};

	const e404 = "404";
	const endpoints = (client: Peerbit | Libp2p): http.RequestListener => {
		return async (req, res) => {
			res.setHeader("Access-Control-Allow-Origin", "*");
			res.setHeader("Access-Control-Request-Method", "*");
			res.setHeader("Access-Control-Allow-Headers", "*");
			res.setHeader("Access-Control-Allow-Methods", "*");

			const r404 = () => {
				res.writeHead(404);
				res.end(e404);
			};

			try {
				if (req.url) {
					if (
						!req.url.startsWith(IPFS_ID_PATH) &&
						!(await adminACL(req))
					) {
						res.writeHead(401);
						res.end("Not authorized");
						return;
					} else if (req.url.startsWith(TOPICS_PATH)) {
						switch (req.method) {
							case "GET":
								import("url").then((parse) => {
									const replicateParam = parse.parse(
										req.url!,
										true
									).query["replicate"];
									if (Array.isArray(replicateParam)) {
										res.writeHead(400);
										res.end(
											"Expecting one replicate param"
										);
										return;
									}
									const replicate = replicateParam
										? JSON.parse(replicateParam)
										: replicateParam;

									res.setHeader(
										"Content-Type",
										"application/json"
									);
									res.writeHead(200);
									if (replicate) {
										if (client instanceof Peerbit) {
											res.write(
												JSON.stringify(
													[...client.programs.entries()].filter((x) => x[1].program.replicate).map(x => x[0]),
												)
											);
											res.end();
										} else {
											res.writeHead(400);
											res.write(notPeerBitError);
											res.end();
										}
									} else {
										res.write(
											JSON.stringify(
												[...libp2p.directsub.subscriptions.keys()]
											)
										);
										res.end();
									}
								});

								break;
							default:
								r404();
								break;
						}
					} else if (req.url.startsWith(PROGRAM_PATH)) {
						const url = new URL(
							req.url,
							"http://localhost:" + port
						);
						if (client instanceof Peerbit === false) {
							res.writeHead(400);
							res.write(notPeerBitError);
							res.end();
						} else {
							switch (req.method) {
								case "GET":
									try {
										const program = getProgramFromPath(
											req,
											1
										);
										if (program) {
											res.writeHead(200);
											res.write(
												toBase64(serialize(program))
											);
											res.end();
										} else {
											res.writeHead(404);
											res.end();
										}
									} catch (error: any) {
										res.writeHead(404);
										res.end(error.message);
									}
									break;

								case "PUT":
									getBody(req, (body) => {
										try {
											const parsed = deserialize(
												fromBase64(body),
												Program
											);
											(client as Peerbit)
												.open(parsed)
												.then((program) => {
													res.writeHead(200);
													res.end(
														program.address.toString()
													);
												})
												.catch((error) => {
													res.writeHead(400);
													res.end(
														"Failed to open program: " +
														error.toString()
													);
												});
										} catch (error) {
											res.writeHead(400);
											res.end(
												"Invalid base64 program binary"
											);
										}
									});
									break;

								default:
									r404();
									break;
							}
						}
					} else if (req.url.startsWith(LIBRARY_PATH)) {
						const url = new URL(
							req.url,
							"http://localhost:" + port
						);
						switch (req.method) {
							case "PUT":
								getBody(req, (body) => {
									const name = body;
									if (name && name.length === 0) {
										res.writeHead(400);
										res.end("Invalid library: " + name);
									} else {
										import(/* webpackIgnore: true */ name)
											.then(() => {
												res.writeHead(200);
												res.end();
											})
											.catch((e) => {
												res.writeHead(400);
												res.end(e.message.toString?.());
											});
									}
								});
								break;

							default:
								r404();
								break;
						}
					} else if (req.url.startsWith(IPFS_ID_PATH)) {
						res.writeHead(200);
						res.end(libp2p.peerId.toString());
					} else if (req.url.startsWith(ADDRESSES_PATH)) {
						res.setHeader("Content-Type", "application/json");
						res.writeHead(200);
						const addresses = libp2p
							.getMultiaddrs()
							.map((x) => x.toString());
						res.end(JSON.stringify(addresses));
					} else {
						r404();
					}
				} else {
					r404();
				}
			} catch (error: any) {
				res.writeHead(500);
				console.error(error?.message);
				res.end("Unexpected error");
			}
		};
	};

	setMaxListeners(Infinity); // TODO make this better (lower and large enough)
	process.setMaxListeners(Infinity); // TODO make this better (lower and large enough)

	const server = http.createServer(endpoints(client));
	server.listen(port);
	server.on("error", (e) => {
		console.error("Server error: " + e?.message);
		import("fs").then((fs) => {
			fs.writeFile("error.log", JSON.stringify(e.message), function () {
                /* void */ 0;
			});
		});
	});
	server.on("close", () => {
		console.log("Exit server");
	});

	console.log("API available at port", port);
	return server;
};

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
		throwIfNot200(
			await axios.get(endpoint + IPFS_ID_PATH, { validateStatus })
		).data;
	const getHeaders = async () => {
		const headers = {
			authorization: "Basic admin:" + (await loadPassword()),
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
							await axios.get(endpoint + ADDRESSES_PATH, {
								validateStatus,
								headers: await getHeaders(),
							})
						).data as string[]
					).map((x) => multiaddr(x));
				},
			},
		},
		topics: {
			get: async (replicate: boolean): Promise<string[]> => {
				const result = throwIfNot200(
					await axios.get(
						endpoint + TOPICS_PATH + "?replicate=" + replicate,
						{
							validateStatus,
							headers: await getHeaders(),
						}
					)
				);
				return result.data as string[];
			},
		},
		program: {
			get: async (
				address: Address | string
			): Promise<Program | undefined> => {
				const result = getBodyByStatus<string, any>(
					await axios.get(
						endpoint +
						PROGRAM_PATH +
						"/" +
						encodeURIComponent(address.toString()),
						{ validateStatus, headers: await getHeaders() }
					)
				);
				return !result
					? undefined
					: deserialize(fromBase64(result), Program);
			},

			/**
			 * @param program Program, or base64 string representation
			 * @param topic, topic
			 * @returns
			 */
			put: async (
				program: Program | string
			): Promise<Address> => {
				const base64 =
					program instanceof Program
						? toBase64(serialize(program))
						: program;
				const resp = throwIfNot200(
					await axios.put(
						endpoint +
						PROGRAM_PATH,
						base64,
						{ validateStatus, headers: await getHeaders() }
					)
				);
				return Address.parse(resp.data);
			},
		},
		library: {
			put: async (name: string): Promise<void> => {
				throwIfNot200(
					await axios.put(endpoint + LIBRARY_PATH, name, {
						validateStatus,
						headers: await getHeaders(),
					})
				);
				return;
			},
		},
	};
};
