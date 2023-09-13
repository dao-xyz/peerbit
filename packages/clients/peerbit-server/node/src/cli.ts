import {
	createTestDomain,
	getDomainFromConfig,
	loadConfig,
	startCertbot
} from "./domain.js";
import { startServerWithNode } from "./server.js";
import {
	AWS_LINUX_ARM_AMIs,
	createRecord,
	launchNodes,
	terminateNode
} from "./aws.js";
import {
	getHomeConfigDir,
	getKeypair,
	getPackageName,
	getRemotesPath
} from "./config.js";
import chalk from "chalk";
import { createClient, waitForDomain } from "./client.js";
import { InstallDependency, StartProgram } from "./types.js";
import { exit } from "process";
import yargs from "yargs";
import readline from "readline";
import fs from "fs";
import path from "path";
import { toBase64 } from "@peerbit/crypto";
import { DEFAULT_REMOTE_GROUP, RemoteObject, Remotes } from "./remotes.js";
import { peerIdFromString } from "@libp2p/peer-id";
import { LOCAL_API_PORT } from "./routes.js";
import { type PeerId } from "@libp2p/interface/peer-id";
import Table from "tty-table";

const colors = [
	"#00FF00",
	"#0000FF",
	"#FF0000",
	"#01FFFE",
	"#FFA6FE",
	"#FFDB66",
	"#006401",
	"#010067",
	"#95003A",
	"#007DB5",
	"#FF00F6",
	"#FFEEE8",
	"#774D00",
	"#90FB92",
	"#0076FF",
	"#D5FF00",
	"#FF937E",
	"#6A826C",
	"#FF029D",
	"#FE8900",
	"#7A4782",
	"#7E2DD2",
	"#85A900",
	"#FF0056",
	"#A42400",
	"#00AE7E",
	"#683D3B",
	"#BDC6FF",
	"#263400",
	"#BDD393",
	"#00B917",
	"#9E008E",
	"#001544",
	"#C28C9F",
	"#FF74A3",
	"#01D0FF",
	"#004754",
	"#E56FFE",
	"#788231",
	"#0E4CA1",
	"#91D0CB",
	"#BE9970",
	"#968AE8",
	"#BB8800",
	"#43002C",
	"#DEFF74",
	"#00FFC6",
	"#FFE502",
	"#620E00",
	"#008F9C",
	"#98FF52",
	"#7544B1",
	"#B500FF",
	"#00FF78",
	"#FF6E41",
	"#005F39",
	"#6B6882",
	"#5FAD4E",
	"#A75740",
	"#A5FFD2",
	"#FFB167",
	"#009BFF",
	"#E85EBE"
];
const padString = function (
	string: string,
	padding: number,
	padChar = " ",
	stringLength = string.valueOf().length
) {
	const val = string.valueOf();
	if (Math.abs(padding) <= stringLength) {
		return val;
	}

	const m = Math.max(Math.abs(padding) - stringLength || 0, 0);
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
						default: getHomeConfigDir()
					})
					.option("bootstrap", {
						describe: "Whether to connect to bootstap nodes on startup",
						type: "boolean",
						default: false
					})
					.option("grant-access", {
						describe: "Grant access to public keys on start",
						defaultDescription:
							"The publickey of this device located in 'directory'",
						type: "string",
						array: true,
						alias: "ga"
					})
					.option("reset", {
						describe:
							"If true, then programs opened during last session will not be opened",
						type: "boolean",
						default: false,
						alias: "r"
					})
					.option("port-api", {
						describe:
							"Set API server port. Only modify this when testing locally, since NGINX config depends on the default value",
						type: "number",
						default: undefined
					})
					.option("port-node", {
						describe:
							"Set Libp2p listen port. Only modify this when testing locally, since NGINX config depends on the default value",
						type: "number",
						default: undefined
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
					grantAccess: args["grant-access"]
				});
			}
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
					default: getHomeConfigDir()
				});
				return yargs;
			},
			handler: async (args) => {
				const kp = await getKeypair(args.directory);
				console.log((await kp.toPeerId()).toString());
			}
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
								demandOption: true
							},
							outdir: {
								describe: "Output path for Nginx config",
								type: "string",
								alias: "o"
							},
							wait: {
								alias: "w",
								describe: "Wait for setup to succeed (or fail)",
								type: "boolean",
								default: false
							}
						},
						handler: async (args) => {
							const domain = await createTestDomain();
							await startCertbot(domain, args.email, args.outdir, args.wait);
							exit();
						}
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
								demandOption: true
							},
							hostedZoneId: {
								describe: 'The id of the hosted zone "HostedZoneId"',
								alias: "hz",
								type: "string",
								require: true
							},
							accessKeyId: {
								describe: "Access key id of the AWS user",
								alias: "ak",
								type: "string"
							},
							region: {
								describe: "AWS region",
								alias: "r",
								type: "string"
							},
							secretAccessKey: {
								describe: "Secret key id of the AWS user",
								alias: "sk",
								type: "string"
							},
							email: {
								describe: "Email for Lets encrypt auto-renewal messages",
								type: "string",
								demandOption: true
							},
							outdir: {
								describe: "Output path for Nginx config",
								type: "string",
								alias: "o"
							},
							wait: {
								alias: "w",
								describe: "Wait for setup to succeed (or fail)",
								type: "boolean",
								default: false
							}
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
											secretAccessKey: args.secretAccessKey
									  }
									: undefined
							});
							await startCertbot(
								args.domain,
								args.email,
								args.outdir,
								args.wait
							);
							exit();
						}
					})
					.strict()
					.demandCommand();
			}
		)
		.command("remote", "Handle remote nodes", (innerYargs) => {
			innerYargs
				.command("spawn", "Spawn remote nodes", (spawnYargs) => {
					spawnYargs
						.command({
							command: "aws",
							describe: "Spawn remote nodes on AWS",
							builder: (awsArgs: yargs.Argv) => {
								awsArgs.option("count", {
									describe: "Amount of nodes to spawn",
									defaultDescription: "One node",
									type: "number",
									alias: "c",
									default: 1
								});
								awsArgs.option("region", {
									describe: "Region",
									type: "string",
									defaultDescription: "Region defined in ~.aws/config",
									choices: Object.keys(AWS_LINUX_ARM_AMIs)
								});
								awsArgs.option("group", {
									describe: "Remote group to launch nodes in",
									type: "string",
									alias: "g",
									default: DEFAULT_REMOTE_GROUP
								});
								awsArgs.option("size", {
									describe: "Instance size",
									type: "string",
									alias: "s",
									choices: [
										"micro",
										"small",
										"medium",
										"large",
										"xlarge",
										"2xlarge"
									],
									default: "micro"
								});

								awsArgs.option("name", {
									describe: "Name prefix for spawned nodes",
									type: "string",
									alias: "n",
									default: "peerbit-node"
								});
								awsArgs.option("grant-access", {
									describe: "Grant access to public keys on start",
									defaultDescription:
										"The publickey of this device located in 'directory'",
									type: "string",
									array: true,
									alias: "ga"
								});
								awsArgs.option("directory", {
									describe: "Peerbit directory",
									defaultDescription: "~.peerbit",
									type: "string",
									alias: "d",
									default: getHomeConfigDir()
								});
								return awsArgs;
							},
							handler: async (args) => {
								const accessGrant: PeerId[] =
									args.access?.length > 0
										? args.access.map((x) => peerIdFromString(x))
										: [
												await (
													await getKeypair(args.directory)
												).publicKey.toPeerId()
										  ];
								const nodes = await launchNodes({
									email: "marcus@dao.xyz",
									count: args.count,
									namePrefix: args.name,
									region: args.region,
									grantAccess: accessGrant,
									size: args.size
								});

								console.log(
									`Waiting for ${args.count} ${
										args.count > 1 ? "nodes" : "node"
									} to spawn. This might take a few minutes. You can watch the progress in your AWS console.`
								);
								const twirlTimer = (function () {
									const P = ["\\", "|", "/", "-"];
									let x = 0;
									return setInterval(function () {
										process.stdout.write(
											"\r" + "Loading: " + chalk.hex(colors[x])(P[x++])
										);
										x &= 3;
									}, 250);
								})();
								for (const node of nodes) {
									try {
										const domain = await waitForDomain(node.publicIp);
										const remotes = new Remotes(getRemotesPath(args.directory));
										remotes.add({
											name: node.name,
											address: domain,
											group: args.group,
											origin: {
												type: "aws",
												instanceId: node.instanceId,
												region: node.region
											}
										});
									} catch (error: any) {
										process.stdout.write("\r");
										console.error(
											`Error waiting for domain for ip: ${
												node.publicIp
											} to be available: ${error?.toString()}`
										);
									}
								}
								process.stdout.write("\r");
								clearInterval(twirlTimer);
								console.log(`New nodes available (${nodes.length}):`);
								for (const node of nodes) {
									console.log(chalk.green(node.name));
								}
							}
						})
						.strict()
						.demandCommand();
				})
				.command({
					command: "terminate [name...]",
					describe: "Terminate remote instances that was previously spawned",
					builder: (killArgs: yargs.Argv) => {
						killArgs.option("all", {
							describe: "Kill all nodes",
							type: "boolean",
							default: false
						});
						killArgs.positional("name", {
							type: "string",
							describe: "Remote name",
							default: "localhost",
							demandOption: false,
							array: true
						});
						killArgs.option("directory", {
							describe: "Peerbit directory",
							defaultDescription: "~.peerbit",
							type: "string",
							alias: "d",
							default: getHomeConfigDir()
						});
						return killArgs;
					},
					handler: async (args) => {
						const remotes = new Remotes(getRemotesPath(args.directory));
						const allRemotes = await remotes.all();
						for (const remote of allRemotes) {
							if (args.all || args.name.includes(remote.name)) {
								if (remote.origin?.type === "aws") {
									await terminateNode({
										instanceId: remote.origin.instanceId,
										region: remote.origin.region
									});
								}
							}
						}
					}
				})
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
							default: getHomeConfigDir()
						});

						return yargs;
					},
					handler: async (args) => {
						const remotes = new Remotes(getRemotesPath(args.directory));
						const allRemotes = await remotes.all();

						const all = allRemotes;
						const apis = await Promise.all(
							all.map(async (remote) =>
								createClient(await getKeypair(args.directory), remote)
							)
						);
						const resolvedOrRejected = await Promise.allSettled(
							apis.map((x) => x.peer.id.get())
						);

						if (all.length > 0) {
							const rows: string[][] = [];
							for (const [ix, remote] of all.entries()) {
								const row = [
									remote.name,
									remote.group || "",
									remote.origin?.type === "aws"
										? `aws\n${remote.origin.region}\n${remote.origin.instanceId}`
										: "",
									resolvedOrRejected[ix].status === "fulfilled"
										? chalk.green("Y")
										: chalk.red("N"),
									remote.address
								];
								rows.push(row);
							}
							const table = Table(
								["Name", "Group", "Origin", "Online", "Address"].map((x) => {
									return { value: x, align: "left" };
								}),
								rows
							);
							console.log(table.render());
						} else {
							console.log("No remotes found!");
						}
					}
				})
				.command({
					command: "add <name> <address>",
					describe: "Add remote",
					builder: (yargs: yargs.Argv) => {
						yargs
							.positional("name", {
								type: "string",
								describe: "Remote address",
								demandOption: true
							})
							.positional("address", {
								type: "string",
								describe: "Remote name",
								demandOption: true
							})
							.option("group", {
								describe: "Group name",
								type: "string",
								alias: "g",
								default: DEFAULT_REMOTE_GROUP
							})
							.option("directory", {
								describe: "Peerbit directory",
								defaultDescription: "~.peerbit",
								type: "string",
								alias: "d",
								default: getHomeConfigDir()
							});

						return yargs;
					},
					handler: async (args) => {
						if (args.name === "localhost") {
							throw new Error("Remote can not be named 'localhost'");
						}
						const api = await createClient(await getKeypair(args.directory), {
							address: args.address
						});
						try {
							await api.program.list();
						} catch (error) {
							throw new Error("Failed to add remote: " + error?.toString());
						}
						if (!fs.existsSync(args.directory)) {
							fs.mkdirSync(args.directory, { recursive: true });
						}
						const remotes = new Remotes(getRemotesPath(args.directory));
						remotes.add({
							name: args.name,
							address: args.address,
							group: args.group
						});
					}
				})
				.command({
					command: "remove <name>",
					describe: "Remove a remote",
					builder: (yargs: yargs.Argv) => {
						yargs

							.positional("name", {
								type: "string",
								describe: "Remote address",
								demandOption: true
							})
							.option("directory", {
								describe: "Peerbit directory",
								defaultDescription: "~.peerbit",
								type: "string",
								alias: "d",
								default: getHomeConfigDir()
							});

						return yargs;
					},
					handler: async (args) => {
						const remotes = new Remotes(getRemotesPath(args.directory));
						if (remotes.remove(args.name)) {
							console.log(
								chalk.green("Removed remote with name: " + args.name)
							);
							remotes.save();
						} else {
							console.log(
								chalk.red("Did not find any remote with name: " + args.name)
							);
						}
					}
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
								array: true
							})
							.option("all", {
								type: "boolean",
								describe: "Connect to all nodes",
								default: false
							})
							.option("group", {
								type: "string",
								describe: "Remote group name",
								alias: "g",
								default: [],
								array: true
							})
							.option("directory", {
								describe: "Peerbit directory",
								defaultDescription: "~.peerbit",
								type: "string",
								alias: "d",
								default: getHomeConfigDir()
							});
						return yargs;
					},
					handler: async (connectArgs) => {
						const remotes = new Remotes(getRemotesPath(connectArgs.directory));
						let names: string[] = connectArgs.name;
						if (
							names.length === 0 ||
							connectArgs.all ||
							connectArgs.group.length > 0
						) {
							names = (await remotes.all()).map((x) => x.name);
						}

						const apis: {
							log: (string: string) => void;
							name: string;
							api: Awaited<ReturnType<typeof createClient>>;
						}[] = [];

						const config = await import("./config.js");
						const keypair = await config.getKeypair(connectArgs.directory);

						const selectedRemotes: RemoteObject[] = [];
						if (names.length > 0) {
							for (const [ix, name] of names.entries()) {
								if (name === "localhost") {
									selectedRemotes.push({
										address: "http://localhost:" + LOCAL_API_PORT,
										name: "localhost",
										group: DEFAULT_REMOTE_GROUP
									});
								} else {
									const remote = remotes.getByName(name);

									if (!remote) {
										throw new Error("Missing remote with name: " + name);
									}

									if (
										connectArgs.group.length > 0 &&
										!connectArgs.group.includes(remote.group)
									) {
										continue;
									}

									selectedRemotes.push(remote);
								}
							}
						}

						const maxNameLength = selectedRemotes
							.map((x) => x.name.length)
							.reduce((prev, c, i) => {
								return Math.max(prev, c);
							}, 0);

						if (selectedRemotes.length === 0) {
							console.log(
								chalk.red("No remotes matched your connection condition")
							);
						} else {
							console.log(`Connected to (${selectedRemotes.length}):`);

							for (const [ix, remote] of selectedRemotes.entries()) {
								const chalkBg = chalk.bgHex(colors[ix]);
								console.log(chalkBg(remote.name));
								const logFn: (name: string) => void = (string) =>
									console.log(
										padString(
											chalkBg(remote.name),
											maxNameLength,
											" ",
											remote.name.length
										) +
											": " +
											string
									);

								apis.push({
									log: logFn,
									name: remote.name,
									api: await createClient(keypair, remote)
								});
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

							const rl = readline.createInterface({
								input: process.stdin,
								output: process.stdout,
								terminal: true,
								historySize: 100
							});

							console.log("Write 'help' to show commands.\n");
							rl.prompt(false);
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
												}
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
												}
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
															demandOption: true
														});
														return yargs;
													},
													handler: async (args) => {
														const peerId: PeerId = peerIdFromString(
															args["peer-id"]
														);
														for (const api of apis) {
															await api.api.access.allow(peerId);
														}
													}
												})
												.command({
													command: "deny <peer-id>",
													describe: "Remove admin capabilities from peer-id",
													builder: (yargs: yargs.Argv) => {
														yargs.positional("peer-id", {
															describe: "Peer id",
															demandOption: true
														});
														return yargs;
													},
													handler: async (args) => {
														const peerId = peerIdFromString(args["peer-id"]);
														for (const api of apis) {
															await api.api.access.deny(peerId);
														}
													}
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
												}
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
															default: false
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
													}
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
														demandOption: true
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
												}
											})
											.command({
												command: "drop <address>",
												describe: "Drop a program",
												builder: (yargs: any) => {
													yargs.positional("address", {
														type: "string",
														describe: "Program address",
														demandOption: true
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
												}
											})
											.command({
												command: "close <address>",
												describe: "Close a program",
												builder: (yargs: any) => {
													yargs.positional("address", {
														type: "string",
														describe: "Program address",
														demandOption: true
													});
													return yargs;
												},

												handler: async (args) => {
													for (const api of apis) {
														await api.api.program.close(args.address);
													}
												}
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
												}
											})
											.command({
												command: "variants",
												describe: "List all programs variants",
												aliases: "v",
												handler: async (args) => {
													for (const api of apis) {
														const list = await api.api.program.variants();
														api.log(`Program variants (${list.length}):`);
														list.forEach((p) => {
															api.log(chalk.green(p));
														});
													}
												}
											})
											.command({
												command: "open [program]",
												describe: "Open program",
												builder: (yargs: any) => {
													yargs.positional("program", {
														type: "string",
														describe: "Identifier",
														demandOption: true
													});
													yargs.option("base64", {
														type: "string",
														describe: "Base64 encoded serialized",
														aliases: "b"
													});
													yargs.option("variant", {
														type: "string",
														describe: "Variant name",
														aliases: "v"
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
															base64: args.base64
														};
													} else {
														startArg = {
															variant: args.variant
														};
													}
													for (const api of apis) {
														const address =
															await api.api.program.open(startArg);
														api.log("Started program with address: ");
														api.log(chalk.green(address.toString()));
													}
												}
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
												demandOption: true
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
													base64
												};
											} else {
												installCommand = { type: "npm", name: packageName };
											}

											for (const api of apis) {
												const newPrograms =
													await api.api.dependency.install(installCommand);
												api.log(
													`New programs available (${newPrograms.length}):`
												);
												newPrograms.forEach((p) => {
													api.log(chalk.green(p));
												});
											}
										}
									})
									.command({
										command: "restart",
										describe: "Restart the server",
										handler: async () => {
											for (const api of apis) {
												await api.api.restart();
											}
										}
									})
									.command({
										command: "stop",
										describe: "Stop the server",
										handler: async () => {
											for (const api of apis) {
												await api.api.stop();
											}
										}
									})
									.help()
									.strict()
									.scriptName("")
									.demandCommand()
									.showHelpOnFail(true)
									.exitProcess(false);

							rl.on("line", async (cargs) => {
								const cmds = capi();
								try {
									await cmds.parse(cargs);
								} catch (error: any) {
									/* console.log(chalk.red("Error parsing command: " + cargs))*/
								}
								rl.prompt(true);
							});
						}
					}
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
