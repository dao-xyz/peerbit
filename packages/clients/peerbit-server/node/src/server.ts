import http from "http";
import { fromBase64, sha256Base64Sync } from "@peerbit/crypto";
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
	getHomeConfigDir,
	getCredentialsPath,
	getPackageName,
	loadPassword,
	NotFoundError,
	getNodePath,
} from "./config.js";
import { setMaxListeners } from "events";
import { create } from "./peerbit.js";
import { Peerbit } from "peerbit";
import { getSchema } from "@dao-xyz/borsh";
import {
	InstallDependency,
	StartByBase64,
	StartByVariant,
	StartProgram,
} from "./types.js";
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
} from "./routes.js";
import { Session } from "./session.js";
import fs from "fs";
import { exit } from "process";
import { spawn, fork, execSync } from "child_process";
import tmp from "tmp";
import path from "path";
import { base58btc } from "multiformats/bases/base58";
import { dirname } from "path";
import { fileURLToPath } from "url";
import { Level } from "level";
import { MemoryLevel } from "memory-level";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const stopAndWait = (server: http.Server) => {
	let closed = false;
	server.on("close", () => {
		closed = true;
	});
	server.close();
	return waitFor(() => closed);
};

export const createPassword = async (
	configDirectory: string,
	password?: string
): Promise<string> => {
	const configDir = configDirectory ?? (await getHomeConfigDir());
	const credentialsPath = await getCredentialsPath(configDir);
	if (!password && (await checkExistPath(credentialsPath))) {
		throw new Error(
			"Config path for credentials: " + credentialsPath + ", already exist"
		);
	}
	console.log(`Creating config folder ${configDir}`);

	fs.mkdirSync(configDir, { recursive: true });
	await waitFor(() => fs.existsSync(configDir));

	console.log(`Created config folder ${configDir}`);

	password = password || uuid();
	fs.writeFileSync(
		credentialsPath,
		JSON.stringify({ username: "admin", password })
	);
	console.log(`Created credentials at ${credentialsPath}`);
	return password!;
};

export const loadOrCreatePassword = async (
	configDirectory: string,
	password?: string
): Promise<string> => {
	if (!password) {
		try {
			return await loadPassword(configDirectory);
		} catch (error) {
			if (error instanceof NotFoundError) {
				return createPassword(configDirectory, password);
			}
			throw error;
		}
	} else {
		return createPassword(configDirectory, password);
	}
};
export const startServerWithNode = async (properties: {
	directory?: string;
	domain?: string;
	bootstrap?: boolean;
	newSession?: boolean;
	password?: string;
	ports?: {
		node: number;
		api: number;
	};
	restart?: () => void;
}) => {
	const peer = await create({
		directory:
			properties.directory != null
				? getNodePath(properties.directory)
				: undefined,
		domain: properties.domain,
		listenPort: properties.ports?.node,
	});

	if (properties.bootstrap) {
		await peer.bootstrap();
	}
	const sessionDirectory =
		properties.directory != null
			? path.join(properties.directory, "session")
			: undefined;
	const session = new Session(
		sessionDirectory
			? new Level<string, Uint8Array>(sessionDirectory, {
					valueEncoding: "view",
					keyEncoding: "utf-8",
			  })
			: new MemoryLevel({ valueEncoding: "view", keyEncoding: "utf-8" })
	);
	if (!properties.newSession) {
		for (const [string] of await session.imports.all()) {
			await import(string);
		}
		for (const [address] of await session.programs.all()) {
			// TODO args
			try {
				await peer.open(address, { timeout: 3000 });
			} catch (error) {
				console.error(error);
			}
		}
	} else {
		await session.clear();
	}

	const server = await startApiServer(peer, {
		port: properties.ports?.api,
		configDirectory:
			properties.directory != null
				? path.join(properties.directory, "server")
				: undefined || getHomeConfigDir(),
		session,
		password: properties.password,
	});
	const printNodeInfo = async () => {
		console.log("Starting node with address(es): ");
		const id = peer.peerId.toString();
		console.log("id: " + id);
		console.log("Addresses: ");
		for (const a of peer.getMultiaddrs()) {
			console.log(a.toString());
		}
	};

	await printNodeInfo();
	const shutDownHook = async (
		controller: { stop: () => any },
		server: http.Server
	) => {
		["SIGTERM", "SIGINT", "SIGUSR1", "SIGUSR2"].forEach((code) => {
			process.on(code, async () => {
				if (server.listening) {
					console.log("Shutting down node");
					await stopAndWait(server);
					await waitFor(() => closed);
					await controller.stop();
				}
				exit();
			});
		});
		process.on("exit", async () => {
			if (server.listening) {
				console.log("Shutting down node");
				await stopAndWait(server);
				await waitFor(() => closed);
				await controller.stop();
			}
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
	return client.handler?.items.get(address);
};
function findPeerbitProgramFolder(inputDirectory: string): string | null {
	let currentDir = path.resolve(inputDirectory);

	while (currentDir !== "/") {
		// Stop at the root directory
		const nodeModulesPath = path.join(currentDir, "node_modules");
		const packageJsonPath = path.join(
			nodeModulesPath,
			"@peerbit",
			"program",
			"package.json"
		);

		if (fs.existsSync(packageJsonPath)) {
			const packageJsonContent = fs.readFileSync(packageJsonPath, "utf-8");
			const packageData = JSON.parse(packageJsonContent);

			if (packageData.name === "@peerbit/program") {
				return currentDir;
			}
		}

		currentDir = path.dirname(currentDir);
	}

	return null;
}

export const startApiServer = async (
	client: ProgramClient,
	options: {
		configDirectory: string;
		session?: Session;
		port?: number;
		password?: string;
	}
): Promise<http.Server> => {
	const port = options?.port ?? LOCAL_PORT;
	const password = await loadOrCreatePassword(
		options.configDirectory,
		options?.password
	);

	const restart = async () => {
		await client.stop();
		await stopAndWait(server);

		// We filter out the reset command, since restarting means that we want to resume something
		spawn(
			process.argv.shift()!,
			[
				...process.execArgv,
				...process.argv.filter((x) => x !== "--reset" && x !== "-r"),
			],
			{
				cwd: process.cwd(),
				detached: true,
				stdio: "inherit",
				gid: process.getgid!(),
			}
		);

		/* process.on("exit", async () => {
			child.kill("SIGINT")
		});
		process.on("SIGINT", async () => {
			child.kill("SIGINT")
		}); */
		process.exit(0);
	};

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
									const ref = (client as Peerbit).handler?.items?.keys() || [];
									const keys = JSON.stringify([...ref]);
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
										let closed = false;
										if (queryData === "true") {
											closed = await program.drop();
										} else {
											closed = await program.close();
										}
										if (closed) {
											await options?.session?.programs.remove(program.address);
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
											.then(async (program) => {
												// TODO what if this is a reopen?
												console.log(
													"OPEN ADDRESS",
													program.address,
													(client as Peerbit).directory,
													await client.services.blocks.has(program.address)
												);
												await options?.session?.programs.add(
													program.address,
													new Uint8Array()
												);
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
									const installArgs: InstallDependency = JSON.parse(body);

									const packageName = installArgs.name; // @abc/123
									let installName = installArgs.name; // abc123.tgz or @abc/123 (npm package name)
									let clear: (() => void) | undefined;
									if (installArgs.type === "tgz") {
										const binary = fromBase64(installArgs.base64);
										const tempFile = tmp.fileSync({
											name:
												base58btc.encode(Buffer.from(installName)) +
												uuid() +
												".tgz",
										});
										fs.writeFileSync(tempFile.fd, binary);
										clear = () => tempFile.removeCallback();
										installName = tempFile.name;
									} else {
										clear = undefined;
									}

									if (!installName || installName.length === 0) {
										res.writeHead(400);
										res.end("Invalid package: " + packageName);
									} else {
										try {
											// TODO do this without sudo. i.e. for servers provide arguments so that this app folder is writeable by default by the user
											const installDir =
												process.env.PEERBIT_MODULES_PATH ||
												findPeerbitProgramFolder(__dirname);
											let permission = "";
											if (!installDir) {
												res.writeHead(400);
												res.end("Missing installation directory");
												return;
											}
											try {
												fs.accessSync(installDir, fs.constants.W_OK);
											} catch (error) {
												permission = "sudo";
											}

											console.log("Installing package: " + installName);
											execSync(
												`${permission} npm install ${installName} --prefix ${installDir} --no-save --no-package-lock`
											); // TODO omit=dev ? but this makes breaks the tests after running once?
										} catch (error: any) {
											res.writeHead(400);
											res.end(
												"Failed to install library: " +
													packageName +
													". " +
													error.toString()
											);
											clear?.();
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
											await options?.session?.imports.add(
												packageName,
												new Uint8Array()
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
											clear?.();
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
					} else if (req.url.startsWith(RESTART_PATH)) {
						switch (req.method) {
							case "POST":
								res.writeHead(200);
								res.end();
								restart();
								break;

							default:
								r404();
								break;
						}
					} else if (req.url.startsWith(TERMINATE_PATH)) {
						switch (req.method) {
							case "POST":
								res.writeHead(200);
								res.end();
								process.exit(0);
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
	await waitFor(() => server.listening);
	console.log("API available at port", port);
	return server;
};
