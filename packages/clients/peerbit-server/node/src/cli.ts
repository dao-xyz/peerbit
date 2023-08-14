import {
	createTestDomain,
	getDomainFromConfig,
	loadConfig,
	startCertbot,
} from "./domain.js";
import { startServerWithNode } from "./server.js";
import { createRecord } from "./aws.js";
import {
	getHomeConfigDir,
	getKeypair,
	getPackageName,
	getRemotesPath,
} from "./config.js";
import chalk from "chalk";
import { client } from "./client.js";
import { InstallDependency, StartProgram } from "./types.js";
import { exit } from "process";
import yargs from "yargs";
import readline from "readline";
import fs from "fs";
import path from "path";
import { toBase64 } from "@peerbit/crypto";
import { Remotes } from "./remotes.js";
import { peerIdFromString } from "@libp2p/peer-id";

const padString = function (string: string, padding: number, padChar = " ") {
	const val = string.valueOf();
	if (Math.abs(padding) <= val.length) {
		return val;
	}
	const m = Math.max(Math.abs(padding) - string.length || 0, 0);
	const pad = Array(m + 1).join(String(padChar).charAt(0));
	//      var pad = String(c || ' ').charAt(0).repeat(Math.abs(n) - this.length);
	return padding < 0 ? pad + val : val + pad;
	//      return (n < 0) ? val + pad : pad + val;
};

export const cli = async (args?: string[]) => {
	const yargs = await import("yargs");

	if (!args) {
		const { hideBin } = await import("yargs/helpers");
		args = hideBin(process.argv);
	}

	return yargs
		.default(args)
		.command({
			command: "start",
			describe: "Start node",
			builder: (yargs: yargs.Argv) => {
				yargs
					.option("directory", {
						describe: "Peerbit directory",
						defaultDescription: "~.peerbit",
						type: "string",
						alias: "d",
						default: getHomeConfigDir(),
					})
					.option("bootstrap", {
						describe: "Whether to connect to bootstap nodes on startup",
						type: "boolean",
						default: false,
					})
					.option("reset", {
						describe:
							"If true, then programs opened during last session will not be opened",
						type: "boolean",
						default: false,
						alias: "r",
					})
					.option("port-api", {
						describe:
							"Set API server port. Only modify this when testing locally, since NGINX config depends on the default value",
						type: "number",
						default: undefined,
					})
					.option("port-node", {
						describe:
							"Set Libp2p listen port. Only modify this when testing locally, since NGINX config depends on the default value",
						type: "number",
						default: undefined,
					});
				return yargs;
			},
			handler: async (args) => {
				await startServerWithNode({
					directory: args.directory,
					domain: await loadConfig().then((config) =>
						config ? getDomainFromConfig(config) : undefined
					),
					ports: { api: args["port-api"], node: args["port-node"] },
					bootstrap: args.bootstrap,
					newSession: args.reset,
				});
			},
		})
		.command({
			command: "id",
			describe: "Get peer id",
			builder: (yargs: yargs.Argv) => {
				yargs.option("directory", {
					describe: "Peerbit directory",
					defaultDescription: "~.peerbit",
					type: "string",
					alias: "d",
					default: getHomeConfigDir(),
				});
				return yargs;
			},
			handler: async (args) => {
				const kp = await getKeypair(args.directory);
				console.log((await kp.toPeerId()).toString());
			},
		})
		.command(
			"domain",
			"Setup a domain and certificate for this node",
			(yargs) => {
				yargs
					.command({
						command: "test",
						describe:
							"Setup a testing domain with SSL (no guarantess on how long the domain will be available)",
						builder: {
							email: {
								describe: "Email for Lets encrypt autorenewal messages",
								type: "string",
								demandOption: true,
							},
							outdir: {
								describe: "Output path for Nginx config",
								type: "string",
								alias: "o",
							},
							wait: {
								alias: "w",
								describe: "Wait for setup to succeed (or fail)",
								type: "boolean",
								default: false,
							},
						},
						handler: async (args) => {
							const domain = await createTestDomain();
							await startCertbot(domain, args.email, args.outdir, args.wait);
							exit();
						},
					})
					.command({
						command: "aws",
						describe:
							"Setup a domain with an AWS account. You either have to setup you AWS credentials in the .aws folder, or pass the credentials in the cli",
						builder: {
							domain: {
								describe: "domain, e.g. abc.example.com, example.com",
								alias: "d",
								type: "string",
								demandOption: true,
							},
							hostedZoneId: {
								describe: 'The id of the hosted zone "HostedZoneId"',
								alias: "hz",
								type: "string",
								require: true,
							},
							accessKeyId: {
								describe: "Access key id of the AWS user",
								alias: "ak",
								type: "string",
							},
							region: {
								describe: "AWS region",
								alias: "r",
								type: "string",
							},
							secretAccessKey: {
								describe: "Secret key id of the AWS user",
								alias: "sk",
								type: "string",
							},
							email: {
								describe: "Email for Lets encrypt auto-renewal messages",
								type: "string",
								demandOption: true,
							},
							outdir: {
								describe: "Output path for Nginx config",
								type: "string",
								alias: "o",
							},
							wait: {
								alias: "w",
								describe: "Wait for setup to succeed (or fail)",
								type: "boolean",
								default: false,
							},
						},
						handler: async (args) => {
							if (
								!!args.accessKeyId !== !!args.secretAccessKey ||
								!!args.region !== !!args.secretAccessKey
							) {
								throw new Error(
									"Expecting either all 'accessKeyId', 'region' and 'secretAccessKey' to be provided or none"
								);
							}
							await createRecord({
								domain: args.domain,
								hostedZoneId: args.hostedZoneId,
								region: args.region,
								credentials: args.accessKeyId
									? {
											accessKeyId: args.accessKeyId,
											secretAccessKey: args.secretAccessKey,
									  }
									: undefined,
							});
							await startCertbot(
								args.domain,
								args.email,
								args.outdir,
								args.wait
							);
							exit();
						},
					})
					.strict()
					.demandCommand();
			}
		)
		.command("remote", "Handle remote nodes", (innerYargs) => {
			innerYargs
				.command({
					command: "list",
					aliases: "ls",
					describe: "List remotes",
					builder: (yargs: yargs.Argv) => {
						yargs.option("directory", {
							describe: "Peerbit directory",
							defaultDescription: "~.peerbit",
							type: "string",
							alias: "d",
							default: getHomeConfigDir(),
						});

						return yargs;
					},
					handler: async (args) => {
						const remotes = new Remotes(getRemotesPath(args.directory));
						const allRemotes = await remotes.all();
						const maxNameLength = allRemotes
							.map((x) => x.name.length)
							.reduce((prev, c, i) => {
								return Math.max(prev, c);
							}, 0);
						const all = await remotes.all();
						if (all.length > 0) {
							for (const remote of all) {
								console.log(
									padString(remote.name, maxNameLength + 10),
									remote.address
								);
							}
						} else {
							console.log("No remotes found!");
						}
					},
				})
				.command({
					command: "add <name> <address>",
					describe: "Add remote",
					builder: (yargs: yargs.Argv) => {
						yargs
							.positional("address", {
								type: "string",
								describe: "Remote name",
								demandOption: true,
							})
							.positional("name", {
								type: "string",
								describe: "Remote address",
								demandOption: true,
							})
							.option("directory", {
								describe: "Peerbit directory",
								defaultDescription: "~.peerbit",
								type: "string",
								alias: "d",
								default: getHomeConfigDir(),
							});

						return yargs;
					},
					handler: async (args) => {
						if (args.name === "localhost") {
							throw new Error("Remote can not be named 'localhost'");
						}
						const api = await client(
							await getKeypair(args.directory),
							args.address
						);
						try {
							await api.program.list();
						} catch (error) {
							throw new Error("Failed to add remote: " + error?.toString());
						}
						if (!fs.existsSync(args.directory)) {
							fs.mkdirSync(args.directory, { recursive: true });
						}
						const remotes = new Remotes(getRemotesPath(args.directory));
						remotes.add(args.name, args.address);
					},
				})
				.command({
					command: "remove <name>",
					describe: "Remove a remote",
					builder: (yargs: yargs.Argv) => {
						yargs

							.positional("name", {
								type: "string",
								describe: "Remote address",
								demandOption: true,
							})
							.option("directory", {
								describe: "Peerbit directory",
								defaultDescription: "~.peerbit",
								type: "string",
								alias: "d",
								default: getHomeConfigDir(),
							});

						return yargs;
					},
					handler: async (args) => {
						const remotes = new Remotes(getRemotesPath(args.directory));
						if (remotes.remove(args.name)) {
							console.log(
								chalk.green("Removed remote with name: " + args.name)
							);
						} else {
							console.log(
								chalk.red("Did not find any remote with name: " + args.name)
							);
						}
					},
				})
				.command({
					command: "connect [name...]",
					describe: "Connect to remote(s)",
					builder: (yargs: yargs.Argv) => {
						yargs
							.positional("name", {
								type: "string",
								describe: "Remote name",
								default: "localhost",
								demandOption: false,
								array: true,
							})
							.option("directory", {
								describe: "Peerbit directory",
								defaultDescription: "~.peerbit",
								type: "string",
								alias: "d",
								default: getHomeConfigDir(),
							});
						return yargs;
					},
					handler: async (connectArgs) => {
						const names = connectArgs.name;
						const apis: {
							log: (string: string) => void;
							name: string;
							api: Awaited<ReturnType<typeof client>>;
						}[] = [];
						console.log(getRemotesPath(connectArgs.directory));
						const config = await import("./config.js");
						const keypair = await config.getKeypair(connectArgs.directory);

						if (names.length > 0) {
							const remotes = new Remotes(
								getRemotesPath(connectArgs.directory)
							);
							for (const name of names) {
								if (name === "localhost") {
									apis.push({
										log: (string) => console.log("localhost: " + string),
										name: "localhost",
										api: await client(keypair),
									});
								} else {
									const remote = remotes.getByName(name);
									if (!remote) {
										throw new Error("Missing remote with name: " + name);
									}
									let logFn: (name: string) => void;
									if (names.length > 0) {
										logFn = (string) => console.log(name + ": " + string);
									} else {
										logFn = (string) => console.log(string);
									}

									apis.push({
										log: logFn,
										name,
										api: await client(keypair, remote.address),
									});
								}
							}
						}

						// try if authenticated
						for (const api of apis) {
							try {
								await api.api.program.list();
							} catch (error) {
								throw new Error(
									`Failed to connect to '${api.name}': ${error?.toString()}`
								);
							}
						}
						const capi = () =>
							yargs
								.default()
								.command("peer", "Peer info", (yargs) => {
									yargs
										.command({
											command: "id",
											describe: "Get peer id",
											handler: async (args) => {
												for (const api of apis) {
													api.log((await api.api.peer.id.get()).toString());
												}
											},
										})
										.command({
											command: "address",
											describe: "Get addresses",
											handler: async (args) => {
												for (const api of apis) {
													(await api.api.peer.addresses.get()).forEach((x) =>
														api.log(x.toString())
													);
												}
											},
										})
										.strict()
										.demandCommand();
									return yargs;
								})
								.command(
									"access",
									"Modify access control for this node",
									(yargs) => {
										yargs
											.command({
												command: "grant <peer-id>",
												describe: "Give a peer-id admin capabilities",
												builder: (yargs: yargs.Argv) => {
													yargs.positional("peer-id", {
														describe: "Peer id",
														type: "string",
														demandOption: true,
													});
													return yargs;
												},
												handler: async (args) => {
													const peerId = peerIdFromString(args["peer-id"]);
													for (const api of apis) {
														await api.api.trust.add(peerId);
													}
												},
											})
											.command({
												command: "deny <peer-id>",
												describe: "Remove admin capabilities from peer-id",
												builder: (yargs: yargs.Argv) => {
													yargs.positional("peer-id", {
														describe: "Peer id",
														demandOption: true,
													});
													return yargs;
												},
												handler: async (args) => {
													const peerId = peerIdFromString(args["peer-id"]);
													for (const api of apis) {
														await api.api.trust.remove(peerId);
													}
												},
											})
											.strict()
											.demandCommand();
									}
								)
								.command("network", "Manage network", (yargs) => {
									yargs
										.command({
											command: "bootstrap",
											describe: "Connect to bootstrap nodes",
											handler: async () => {
												for (const api of apis) {
													await api.api.network.bootstrap();
												}
											},
										})
										.strict()
										.demandCommand();
								})

								.command(
									"topic",
									"Manage topics the node is listening to",
									(yargs) => {
										yargs
											.command({
												command: "list",
												aliases: "ls",
												describe: "List all topics",
												builder: (yargs: any) => {
													yargs.option("replicate", {
														type: "boolean",
														describe: "Replicate data on this topic",
														aliases: "r",
														default: false,
													});
													return yargs;
												},
												handler: async (args) => {
													/* const c = await client();
										const topics = await c.topics.get(args.replicate);
										if (topics?.length > 0) {
											console.log("Topic (" + topics.length + "):");
											for (const t of topics) {
												console.log(t);
											}
										} else {
											console.log("Not subscribed to any topics");
										} */
													console.error("Not implemented");
												},
											})
											.strict()
											.demandCommand();
										return yargs;
									}
								)
								.command("program", "Manage programs", (yargs) => {
									yargs
										.command({
											command: "status <address>",
											describe: "Is a program open",
											builder: (yargs: any) => {
												yargs.positional("address", {
													type: "string",
													describe: "Program address",
													demandOption: true,
												});
												return yargs;
											},

											handler: async (args) => {
												for (const api of apis) {
													const program = await api.api.program.has(
														args.address
													);
													if (!program) {
														api.log(chalk.red("Closed"));
													} else {
														api.log(chalk.green("Open"));
													}
												}
											},
										})
										.command({
											command: "drop <address>",
											describe: "Drop a program",
											builder: (yargs: any) => {
												yargs.positional("address", {
													type: "string",
													describe: "Program address",
													demandOption: true,
												});
												return yargs;
											},

											handler: async (args) => {
												for (const api of apis) {
													try {
														await api.api.program.drop(args.address);
													} catch (error: any) {
														api.log(
															chalk.red(
																`Failed to drop ${
																	args.address
																}: ${error.toString()}`
															)
														);
													}
												}
											},
										})
										.command({
											command: "close <address>",
											describe: "Close a program",
											builder: (yargs: any) => {
												yargs.positional("address", {
													type: "string",
													describe: "Program address",
													demandOption: true,
												});
												return yargs;
											},

											handler: async (args) => {
												for (const api of apis) {
													await api.api.program.close(args.address);
												}
											},
										})
										.command({
											command: "list",
											describe: "List all running programs",
											aliases: "ls",
											handler: async (args) => {
												for (const api of apis) {
													const list = await api.api.program.list();
													api.log(`Running programs (${list.length}):`);
													list.forEach((p) => {
														api.log(chalk.green(p));
													});
												}
											},
										})

										.command({
											command: "open [program]",
											describe: "Open program",
											builder: (yargs: any) => {
												yargs.positional("program", {
													type: "string",
													describe: "Identifier",
													demandOption: true,
												});
												yargs.option("base64", {
													type: "string",
													describe: "Base64 encoded serialized",
													aliases: "b",
												});
												yargs.option("variant", {
													type: "string",
													describe: "Variant name",
													aliases: "v",
												});
												return yargs;
											},
											handler: async (args) => {
												if (!args.base64 && !args.variant) {
													throw new Error(
														"Either base64 or variant argument needs to be provided"
													);
												}
												let startArg: StartProgram;
												if (args.base64) {
													startArg = {
														base64: args.base64,
													};
												} else {
													startArg = {
														variant: args.variant,
													};
												}
												for (const api of apis) {
													const address = await api.api.program.open(startArg);
													api.log("Started program with address: ");
													api.log(chalk.green(address.toString()));
												}
											},
										})
										.strict()
										.demandCommand();
									return yargs;
								})
								.command({
									command: "install <package-spec>",
									describe: "install and import a dependency",
									builder: (yargs: any) => {
										yargs.positional("package-spec", {
											type: "string",
											describe:
												"Installed dependency will be loaded with js import(...)",
											demandOption: true,
										});

										return yargs;
									},
									handler: async (args) => {
										// if ends with .tgz assume it is a file

										let installCommand: InstallDependency;
										const packageName: string = args["package-spec"];
										if (packageName.endsWith(".tgz")) {
											const packagePath = path.isAbsolute(packageName)
												? packageName
												: path.join(process.cwd(), packageName);

											const buffer = fs.readFileSync(packagePath);
											const base64 = toBase64(buffer);
											installCommand = {
												type: "tgz",
												name: await getPackageName(packageName),
												base64,
											};
										} else {
											installCommand = { type: "npm", name: packageName };
										}

										for (const api of apis) {
											const newPrograms = await api.api.dependency.install(
												installCommand
											);
											api.log(
												`New programs available (${newPrograms.length}):`
											);
											newPrograms.forEach((p) => {
												api.log(chalk.green(p));
											});
										}
									},
								})
								.command({
									command: "restart",
									describe: "Restart the server",
									handler: async () => {
										for (const api of apis) {
											await api.api.restart();
										}
									},
								})
								.command({
									command: "terminate",
									describe: "Terminate the server",
									handler: async () => {
										for (const api of apis) {
											await api.api.terminate();
										}
									},
								})
								.help()
								.strict()
								.scriptName("")
								.demandCommand()
								.showHelpOnFail(true)
								.exitProcess(false);
						const rl = readline.createInterface({
							input: process.stdin,
							output: process.stdout,
							terminal: true,
							historySize: 100,
						});
						console.log(chalk.green("Connected"));
						console.log("Write 'help' to show commands.\n");
						const first = true;
						rl.prompt(false);
						rl.on("line", async (cargs) => {
							const cmds = capi();
							try {
								await cmds.parse(cargs);
							} catch (error: any) {
								/* console.log(chalk.red("Error parsing command: " + cargs))*/
							}
							rl.prompt(true);
						});
					},
				})

				.help()
				.strict()
				.demandCommand();
			return innerYargs;
		})
		.help()
		.strict()
		.demandCommand().argv;
};
