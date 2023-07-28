import http from "http";
import { fromBase64 } from "@peerbit/crypto";
import { deserialize } from "@dao-xyz/borsh";
import {
	Program,
	ProgramClient,
	getProgramFromVariant,
	getProgramFromVariants,
} from "@peerbit/program";
import { waitFor } from "@peerbit/time";
import { v4 as uuid } from "uuid";
import {
	checkExistPath,
	getConfigDir,
	getCredentialsPath,
	getPackageName,
	loadPassword,
	NotFoundError,
} from "./config.js";
import { setMaxListeners } from "events";
import { create } from "./peerbit.js";
import { Peerbit } from "peerbit";
import { getSchema } from "@dao-xyz/borsh";
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
import { client } from "./client.js";

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
		callback: (body: string) => Promise<void> | void
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
												res.end(
													"Missing program with variant: " +
														(startArguments as StartByVariant).variant
												);
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
					} else if (req.url.startsWith(INSTALL_PATH)) {
						switch (req.method) {
							case "PUT":
								getBody(req, async (body) => {
									const name = body;

									let packageName = name;
									if (name.endsWith(".tgz")) {
										packageName = await getPackageName(name);
									}

									if (!name || name.length === 0) {
										res.writeHead(400);
										res.end("Invalid package: " + name);
									} else {
										const child_process = await import("child_process");
										try {
											child_process.execSync(
												`npm install ${name} --no-save --no-package-lock`
											); // TODO omit=dev ? but this makes breaks the tests after running once?
										} catch (error: any) {
											res.writeHead(400);
											res.end(
												"Failed ot install library: " +
													name +
													". " +
													error.toString()
											);
											return;
										}

										try {
											const programsPre = new Set(
												getProgramFromVariants().map(
													(x) => getSchema(x).variant
												)
											);

											await import(
												/* webpackIgnore: true */ /* @vite-ignore */ packageName
											);
											const programsPost = getProgramFromVariants()?.map((x) =>
												getSchema(x)
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
										} catch (e: any) {
											res.writeHead(400);
											res.end(e.message.toString?.());
										}
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
