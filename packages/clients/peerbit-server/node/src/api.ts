import http from "http";
import { fromBase64, toBase64 } from "@peerbit/crypto";
import { serialize, deserialize } from "@dao-xyz/borsh";
import {
	Program,
	Address,
	ProgramClient,
	getProgramFromVariant,
	getProgramFromVariants,
} from "@peerbit/program";
import { multiaddr } from "@multiformats/multiaddr";
import { waitFor } from "@peerbit/time";
import { v4 as uuid } from "uuid";
import { Libp2p } from "libp2p";
import { getConfigDir, getCredentialsPath, NotFoundError } from "./config.js";
import { setMaxListeners } from "events";
import { create } from "./client.js";
import { Peerbit } from "peerbit";
import { getSchema } from "@dao-xyz/borsh";

export const SSL_PORT = 9002;
export const LOCAL_PORT = 8082;

export const getPort = (protocol: string) => {
	if (protocol === "https:") {
		return SSL_PORT;
	}

	if (protocol === "http:") {
		return LOCAL_PORT;
	}

	throw new Error("Unsupported protocol: " + protocol);
};

const PEER_ID_PATH = "/peer/id";
const ADDRESS_PATH = "/peer/address";
const PROGRAM_PATH = "/program";
const PROGRAMS_PATH = "/programs";
const DEPENDENCY_PATH = "/library";
const BOOTSTRAP_PATH = "/network/bootstrap";

interface StartByVariant {
	variant: string;
}
interface StartByBase64 {
	base64: string;
}
export type StartProgram = StartByVariant | StartByBase64;
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
			"Config path for credentials: " + credentialsPath + ", already exist"
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
export const startServerWithNode = async (properties: {
	directory?: string;
	domain?: string;
	bootstrap?: boolean;
}) => {
	const peer = await create({
		directory: properties.directory,
		domain: properties.domain,
	});

	if (properties.bootstrap) {
		await peer.bootstrap();
	}

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
	await shutDownHook(peer, server);
	return { server, node: peer };
};

const getProgramFromPath = (
	client: Peerbit,
	req: http.IncomingMessage,
	pathIndex: number
): Program | undefined => {
	if (!req.url) {
		throw new Error("Missing url");
	}
	const url = new URL(req.url, "http://localhost:" + 1234);
	const path = url.pathname
		.substring(Math.min(1, url.pathname.length), url.pathname.length)
		.split("/");
	if (path.length <= pathIndex) {
		throw new Error("Invalid path");
	}
	const address = decodeURIComponent(path[pathIndex]);
	return client.handler.items.get(address);
};

export const startServer = async (
	client: ProgramClient,
	port: number = LOCAL_PORT
): Promise<http.Server> => {
	const notPeerBitError =
		"Client is just a Libp2p node, not a full Peerbit client. The command is not supported for this node type";
	const notSupportedError = "Not implemted";

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

	const endpoints = (client: ProgramClient): http.RequestListener => {
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
						!req.url.startsWith(PEER_ID_PATH) &&
						!req.url.startsWith(ADDRESS_PATH) &&
						!(await adminACL(req))
					) {
						res.writeHead(401);
						res.end("Not authorized");
						return;
					} else if (req.url.startsWith(PROGRAMS_PATH)) {
						if (client instanceof Peerbit === false) {
							res.writeHead(400);
							res.end("Server node is not running a native client");
							return;
						}
						switch (req.method) {
							case "GET":
								try {
									const keys = JSON.stringify([
										...(client as Peerbit).handler.items.keys(),
									]);
									res.setHeader("Content-Type", "application/json");
									res.writeHead(200);
									res.end(keys);
								} catch (error: any) {
									res.writeHead(404);
									res.end(error.message);
								}
								break;

							default:
								r404();
								break;
						}
					} else if (req.url.startsWith(PROGRAM_PATH)) {
						if (client instanceof Peerbit === false) {
							res.writeHead(400);
							res.end("Server node is not running a native client");
							return;
						}
						switch (req.method) {
							case "HEAD":
								try {
									const program = getProgramFromPath(client as Peerbit, req, 1);
									if (program) {
										res.writeHead(200);
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

							case "DELETE":
								try {
									const url = new URL(req.url, "http://localhost:" + 1234);
									const queryData = url.searchParams.get("delete");

									const program = getProgramFromPath(client as Peerbit, req, 1);
									if (program) {
										if (queryData === "true") {
											await program.drop();
										} else {
											await program.close();
										}
										res.writeHead(200);
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
										const startArguments: StartProgram = JSON.parse(body);

										let program: Program;
										if ((startArguments as StartByVariant).variant) {
											const P = getProgramFromVariant(
												(startArguments as StartByVariant).variant
											);
											if (!P) {
												res.writeHead(400);
												res.end("Missing program with variant: " + body);
												return;
											}
											program = new P();
										} else {
											program = deserialize(
												fromBase64((startArguments as StartByBase64).base64),
												Program
											);
										}
										client
											.open(program) // TODO all users to pass args
											.then((program) => {
												res.writeHead(200);
												res.end(program.address.toString());
											})
											.catch((error) => {
												res.writeHead(400);
												res.end("Failed to open program: " + error.toString());
											});
									} catch (error: any) {
										res.writeHead(400);
										res.end(error.toString());
									}
								});
								break;

							default:
								r404();
								break;
						}
					} else if (req.url.startsWith(DEPENDENCY_PATH)) {
						switch (req.method) {
							case "PUT":
								getBody(req, (body) => {
									const name = body;
									console.log("IMPORT '" + name + "'");
									if (name && name.length === 0) {
										res.writeHead(400);
										res.end("Invalid library: " + name);
									} else {
										const programsPre = new Set(
											getProgramFromVariants().map((x) => getSchema(x).variant)
										);

										import(/* webpackIgnore: true */ /* @vite-ignore */ name)
											.then(() => {
												const programsPost = getProgramFromVariants()?.map(
													(x) => getSchema(x)
												);
												const newPrograms: { variant: string }[] = [];
												for (const p of programsPost) {
													if (!programsPre.has(p.variant)) {
														newPrograms.push(p as { variant: string });
													}
												}

												res.writeHead(200);
												res.end(
													JSON.stringify(newPrograms.map((x) => x.variant))
												);
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
					} else if (req.url.startsWith(BOOTSTRAP_PATH)) {
						switch (req.method) {
							case "POST":
								if (client instanceof Peerbit === false) {
									res.writeHead(400);
									res.end("Server node is not running a native client");
									return;
								}
								await (client as Peerbit).bootstrap();
								res.writeHead(200);
								res.end();
								break;

							default:
								r404();
								break;
						}
					} else if (req.url.startsWith(PEER_ID_PATH)) {
						res.writeHead(200);
						res.end(client.peerId.toString());
					} else if (req.url.startsWith(ADDRESS_PATH)) {
						res.setHeader("Content-Type", "application/json");
						res.writeHead(200);
						const addresses = client.getMultiaddrs().map((x) => x.toString());
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
		throwIfNot200(await axios.get(endpoint + PEER_ID_PATH, { validateStatus }))
			.data;

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
							await axios.get(endpoint + ADDRESS_PATH, {
								validateStatus,
								headers: await getHeaders(),
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
			put: async (name: string): Promise<string[]> => {
				const resp = await axios.put(endpoint + DEPENDENCY_PATH, name, {
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
