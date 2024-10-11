/* eslint-disable no-console */
import { deserialize, getSchema } from "@dao-xyz/borsh";
import { peerIdFromString } from "@libp2p/peer-id";
import { fromBase64, getPublicKeyFromPeerId } from "@peerbit/crypto";
import {
	Program,
	type ProgramClient,
	getProgramFromVariant,
	getProgramFromVariants,
} from "@peerbit/program";
import { waitFor } from "@peerbit/time";
import { execSync, spawn } from "child_process";
import { setMaxListeners } from "events";
import fs from "fs";
import http from "http";
import { Level } from "level";
import { MemoryLevel } from "memory-level";
import { base58btc } from "multiformats/bases/base58";
import path, { dirname } from "path";
import { Peerbit } from "peerbit";
import { exit } from "process";
import tmp from "tmp";
import { fileURLToPath } from "url";
import { v4 as uuid } from "uuid";
import { getKeypair, getNodePath, getTrustPath } from "./config.js";
import { create } from "./peerbit.js";
import {
	ADDRESS_PATH,
	BOOTSTRAP_PATH,
	INSTALL_PATH,
	LOCAL_API_PORT,
	PEER_ID_PATH,
	PROGRAMS_PATH,
	PROGRAM_PATH,
	PROGRAM_VARIANTS_PATH,
	RESTART_PATH,
	STOP_PATH,
	TRUST_PATH,
} from "./routes.js";
import { Session } from "./session.js";
import { getBody, verifyRequest } from "./signed-request.js";
import { Trust } from "./trust.js";
import type {
	InstallDependency,
	StartByBase64,
	StartByVariant,
	StartProgram,
} from "./types.js";

// eslint-disable-next-line @typescript-eslint/naming-convention
const __dirname = dirname(fileURLToPath(import.meta.url));

export const stopAndWait = (server: http.Server) => {
	let closed = false;
	server.on("close", () => {
		closed = true;
	});
	server.close();
	return waitFor(() => closed);
};

export const startServerWithNode = async (properties: {
	directory: string;
	domain?: string;
	bootstrap?: boolean;
	newSession?: boolean;
	grantAccess?: string[];
	ports?: {
		node: number;
		api: number;
	};
	restart?: () => void;
}) => {
	if (!fs.existsSync(properties.directory)) {
		fs.mkdirSync(properties.directory, { recursive: true });
	}

	const trustPeerIds =
		properties.grantAccess && properties.grantAccess.length > 0
			? properties.grantAccess.map((x) => peerIdFromString(x))
			: [];

	const keypair = await getKeypair(properties.directory);

	const peer = await create({
		directory: getNodePath(properties.directory),
		domain: properties.domain,
		listenPort: properties.ports?.node,
		keypair,
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
			: new MemoryLevel({ valueEncoding: "view", keyEncoding: "utf-8" }),
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

	const trust = new Trust(getTrustPath(properties.directory));
	const server = await startApiServer(peer, {
		port: properties.ports?.api,
		trust,
		session,
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
		server: http.Server,
	) => {
		["SIGTERM", "SIGINT", "SIGUSR1", "SIGUSR2"].forEach((code) => {
			process.on(code, async () => {
				if (server.listening) {
					console.log("Shutting down node: " + code);
					await stopAndWait(server);
					await controller.stop();
				}
				exit();
			});
		});

		process.on("uncaughtException", (err) => {
			console.error("Uncaught exception", err);
		});

		process.on("exit", async () => {
			if (server.listening) {
				console.log("Shutting down node (exit)");

				await stopAndWait(server);
				await controller.stop();
			}
		});
	};
	await shutDownHook(peer, server);

	if (trustPeerIds.length > 0) {
		for (const id of trustPeerIds) {
			trust.add(getPublicKeyFromPeerId(id).hashcode());
		}
	}
	return { server, node: peer };
};

const getPathValue = (req: http.IncomingMessage, pathIndex: number): string => {
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
	return address;
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
			"package.json",
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
	properties: {
		trust: Trust;
		session?: Session;
		port?: number;
	},
): Promise<http.Server> => {
	const port = properties?.port ?? LOCAL_API_PORT;

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
			},
		);
		process.exit(0);
	};
	if (!client.peerId.equals(await client.identity.publicKey.toPeerId())) {
		throw new Error("Expecting node identity to equal peerId");
	}

	const getVerifiedBody = async (req: http.IncomingMessage) => {
		const body = await getBody(req);
		const result = await verifyRequest(
			req.headers,
			req.method!,
			req.url!,
			body,
		);
		if (result.equals(client.identity.publicKey)) {
			return body;
		}
		if (properties.trust.isTrusted(result.hashcode())) {
			return body;
		}
		throw new Error("Not trusted");
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
					let body: string;
					try {
						body =
							req.url.startsWith(PEER_ID_PATH) ||
							req.url.startsWith(ADDRESS_PATH)
								? await getBody(req)
								: await getVerifiedBody(req);
					} catch (error: any) {
						res.writeHead(401);
						res.end("Not authorized: " + error.toString());
						return;
					}

					if (req.url.startsWith(PROGRAMS_PATH)) {
						if (client instanceof Peerbit === false) {
							res.writeHead(400);
							res.end("Server node is not running a native client");
							return;
						}
						switch (req.method) {
							case "GET":
								try {
									const ref: any =
										(client as Peerbit).handler?.items?.keys() || [];
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
					} else if (req.url.startsWith(PROGRAM_VARIANTS_PATH)) {
						if (client instanceof Peerbit === false) {
							res.writeHead(400);
							res.end("Server node is not running a native client");
							return;
						}
						switch (req.method) {
							case "GET":
								try {
									res.setHeader("Content-Type", "application/json");
									res.writeHead(200);
									res.end(
										JSON.stringify(
											getProgramFromVariants().map((x) => getSchema(x).variant),
										),
									);
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
									const program = (client as Peerbit).handler?.items.get(
										getPathValue(req, 1),
									);
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

									const program = (client as Peerbit).handler?.items.get(
										getPathValue(req, 1),
									);
									if (program) {
										let closed = false;
										if (queryData === "true") {
											closed = await program.drop();
										} else {
											closed = await program.close();
										}
										if (closed) {
											await properties?.session?.programs.remove(
												program.address,
											);
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
								try {
									const startArguments: StartProgram = JSON.parse(body);

									let program: Program;
									if ((startArguments as StartByVariant).variant) {
										const P = getProgramFromVariant(
											(startArguments as StartByVariant).variant,
										);
										if (!P) {
											res.writeHead(400);
											res.end(
												"Missing program with variant: " +
													(startArguments as StartByVariant).variant,
											);
											return;
										}
										program = new P();
									} else {
										program = deserialize(
											fromBase64((startArguments as StartByBase64).base64),
											Program,
										);
									}
									client
										.open(program) // TODO all users to pass args
										.then(async (program) => {
											// TODO what if this is a reopen?
											await properties?.session?.programs.add(
												program.address,
												new Uint8Array(),
											);
											res.writeHead(200);
											res.end(program.address.toString());
										})
										.catch((error) => {
											console.error(error);
											res.writeHead(400);
											res.end("Failed to open program: " + error.toString());
										});
								} catch (error: any) {
									res.writeHead(400);
									console.error(error);
									res.end(error.toString());
								}
								break;

							default:
								r404();
								break;
						}
					} else if (req.url.startsWith(INSTALL_PATH)) {
						switch (req.method) {
							case "PUT": {
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
											`${permission} npm install ${installName} --prefix ${installDir} --no-save --no-package-lock`,
										); // TODO omit=dev ? but this makes breaks the tests after running once?
									} catch (error: any) {
										res.writeHead(400);
										res.end(
											"Failed to install library: " +
												packageName +
												". " +
												error.toString(),
										);
										clear?.();
										return;
									}

									try {
										const programsPre = new Set(
											getProgramFromVariants().map((x) => getSchema(x).variant),
										);

										await import(
											/* webpackIgnore: true */ /* @vite-ignore */ packageName
										);
										await properties?.session?.imports.add(
											packageName,
											new Uint8Array(),
										);
										const programsPost = getProgramFromVariants()?.map((x) =>
											getSchema(x),
										);
										const newPrograms: { variant: string }[] = [];
										for (const p of programsPost) {
											if (!programsPre.has(p.variant)) {
												newPrograms.push(p as { variant: string });
											}
										}

										res.writeHead(200);
										res.end(JSON.stringify(newPrograms.map((x) => x.variant)));
									} catch (e: any) {
										res.writeHead(400);
										res.end(e.message.toString?.());
										clear?.();
									}
								}
								break;
							}

							default: {
								r404();
								break;
							}
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
					} else if (req.url.startsWith(TRUST_PATH)) {
						switch (req.method) {
							case "PUT": {
								properties.trust.add(getPathValue(req, 1));
								res.writeHead(200);
								res.end();
								break;
							}
							case "DELETE": {
								const removed = properties.trust.remove(getPathValue(req, 1));
								res.writeHead(200);
								res.end(removed);
								break;
							}
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
					} else if (req.url.startsWith(STOP_PATH)) {
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
					} /* else if (req.url.startsWith(TERMINATE_PATH)) {
						switch (req.method) {
							case "POST":
								execSync("shutdown -h now")
								process.exit(0);
								break;

							default:
								r404();
								break;
						}
					}  */ else if (req.url.startsWith(PEER_ID_PATH)) {
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
			fs.writeFile("error.log", JSON.stringify(e.message), function () {});
		});
	});
	await waitFor(() => server.listening);
	console.log("API available at port", port);
	return server;
};
