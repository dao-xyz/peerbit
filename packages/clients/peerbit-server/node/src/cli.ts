/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/naming-convention */
import type { PeerId } from "@libp2p/interface";
import { peerIdFromString } from "@libp2p/peer-id";
import { toBase64 } from "@peerbit/crypto";
import chalk from "chalk";
import fs from "fs";
import sodium from "libsodium-wrappers";
import path from "path";
import readline from "readline";
import Table from "tty-table";
import type { Argv } from "yargs";
import { createClient } from "./client.js";
import {
	getHomeConfigDir,
	getKeypair,
	getPackageName,
	getRemotesPath,
} from "./config.js";
import {
	DNS_LEASE_ACCESS_TOKEN_ENV,
	DNS_LEASE_SERVICE_URL_ENV,
	DNS_LEASE_STATE_FILE_ENV,
	getDnsLeaseStatePath,
	provisionDnsLease,
	readDnsLeaseState,
	releaseDnsLease,
	renewDnsLease,
	startDnsLeaseRenewal,
} from "./domain-lease.js";
import { getDomainFromConfig, loadConfig, startCertbot } from "./domain.js";
import { terminateNode as terminateHetznerNode } from "./hetzner.js";
import {
	DEFAULT_REMOTE_GROUP,
	type RemoteObject,
	Remotes,
	getRetiredAWSManagementError,
} from "./remotes.js";
import { LOCAL_API_PORT } from "./routes.js";
import { startServerWithNode } from "./server.js";
import type { InstallDependency, StartProgram } from "./types.js";

await sodium.ready;

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
	"#E85EBE",
];
const padString = function (
	string: string,
	padding: number,
	padChar = " ",
	stringLength = string.valueOf().length,
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
			builder: (yargs: Argv) => {
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
					.option("grant-access", {
						describe: "Grant access to public keys on start",
						defaultDescription:
							"The publickey of this device located in 'directory'",
						type: "string",
						array: true,
						alias: "ga",
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
					})
					.option("dns-lease-state-file", {
						describe: `Managed DNS lease state file (or ${DNS_LEASE_STATE_FILE_ENV})`,
						type: "string",
					});
				return yargs;
			},
			handler: async (args) => {
				let stopDnsLeaseRenewal: () => void = () => undefined;
				try {
					stopDnsLeaseRenewal = await startDnsLeaseRenewal({
						statePath:
							(args["dns-lease-state-file"] as string | undefined) ||
							process.env[DNS_LEASE_STATE_FILE_ENV],
						onError: (message) => console.warn(message),
					});
				} catch {
					console.warn(
						"Managed DNS lease renewal could not start; use 'peerbit domain lease renew' to inspect it",
					);
				}
				try {
					const started = await startServerWithNode({
						directory: args.directory,
						domain: await loadConfig().then((config) =>
							config ? getDomainFromConfig(config) : undefined,
						),
						ports: { api: args["port-api"], node: args["port-node"] },
						bootstrap: args.bootstrap,
						newSession: args.reset,
						grantAccess: args["grant-access"],
					});
					started.server.once("close", stopDnsLeaseRenewal);
				} catch (error) {
					stopDnsLeaseRenewal();
					throw error;
				}
			},
		})
		.command({
			command: "id",
			describe: "Get peer id",
			builder: (yargs: Argv) => {
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
			"Configure a domain and certificate for this node",
			(yargs) => {
				yargs
					.command({
						command: "lease [operation]",
						describe: "Manage a temporary peerchecker.com DNS lease",
						builder: (leaseArgs: Argv) =>
							leaseArgs
								.positional("operation", {
									choices: ["claim", "renew", "release", "status"] as const,
									default: "claim",
									describe: "Lease operation",
									type: "string",
								})
								.option("service-url", {
									describe: `Lease service URL (or ${DNS_LEASE_SERVICE_URL_ENV})`,
									type: "string",
								})
								.option("access-token", {
									describe: `Claim access token (prefer ${DNS_LEASE_ACCESS_TOKEN_ENV} to avoid shell history)`,
									type: "string",
								})
								.option("address", {
									describe: "Public IPv4 or IPv6 address for a new lease",
									type: "string",
								})
								.option("email", {
									describe: "Email for Let's Encrypt security messages",
									type: "string",
								})
								.option("configure", {
									default: true,
									describe: "Configure NGINX and Let's Encrypt after claiming",
									type: "boolean",
								})
								.option("wait", {
									alias: "w",
									default: true,
									describe: "Wait for HTTPS setup to succeed",
									type: "boolean",
								})
								.option("state-file", {
									default: getDnsLeaseStatePath(),
									describe: "Managed DNS lease state file",
									type: "string",
								}),
						handler: async (args) => {
							const operation = args.operation || "claim";
							const statePath = args["state-file"] as string;
							const serviceUrl =
								(args["service-url"] as string | undefined) ||
								process.env[DNS_LEASE_SERVICE_URL_ENV];
							if (operation === "status") {
								const state = readDnsLeaseState(statePath);
								console.log(
									JSON.stringify(
										state
											? {
													address: state.address,
													configuredAt: state.configuredAt,
													domain: state.domain,
													expiresAt: state.expiresAt,
													status: state.status,
												}
											: { status: "none" },
										undefined,
										2,
									),
								);
								return;
							}
							if (operation === "renew") {
								const active = await renewDnsLease({ statePath, serviceUrl });
								console.log(`DNS lease renewed until ${active.expiresAt}`);
								return;
							}
							if (operation === "release") {
								await releaseDnsLease({ statePath, serviceUrl });
								console.log("DNS lease released");
								return;
							}
							if (args.configure && !args.email) {
								throw new Error(
									"--email is required when configuring the leased domain",
								);
							}
							const active = await provisionDnsLease({
								accessToken:
									(args["access-token"] as string | undefined) ||
									process.env[DNS_LEASE_ACCESS_TOKEN_ENV],
								address: args.address as string | undefined,
								configure: args.configure
									? (domain) =>
											startCertbot(domain, args.email as string, args.wait)
									: undefined,
								serviceUrl,
								statePath,
							});
							console.log(`DNS lease active until ${active.expiresAt}`);
						},
					})
					.command({
						command: "configure <domain>",
						describe:
							"Configure NGINX and Let's Encrypt for a domain you control",
						builder: {
							domain: {
								describe:
									"Domain whose DNS record already points to this server",
								type: "string",
								demandOption: true,
							},
							email: {
								describe: "Email for Let's Encrypt security messages",
								type: "string",
								demandOption: true,
							},
							wait: {
								alias: "w",
								describe:
									"Wait for HTTPS setup to succeed (use --no-wait to return early)",
								type: "boolean",
								default: true,
							},
						},
						handler: async (args) => {
							await startCertbot(args.domain, args.email, args.wait);
						},
					})
					.command({
						command: "test",
						describe: false,
						builder: (retiredArgs: Argv) => retiredArgs.strict(false),
						handler: () => {
							throw new Error(
								"Automatic test domains have been retired. Point a domain you control to this server, then run 'peerbit domain configure <domain> --email <email>'.",
							);
						},
					})
					.command({
						command: "aws",
						describe: false,
						builder: (retiredArgs: Argv) => retiredArgs.strict(false),
						handler: () => {
							throw new Error(
								"Automatic AWS DNS configuration has been retired. Configure DNS with your provider, then run 'peerbit domain configure <domain> --email <email>'.",
							);
						},
					})
					.strict()
					.demandCommand();
			},
		)
		.command("remote", "Handle remote nodes", (innerYargs) => {
			innerYargs
				.command({
					command: "spawn [provider]",
					describe: false,
					builder: (spawnArgs: Argv) =>
						spawnArgs.positional("provider", { type: "string" }).strict(false),
					handler: () => {
						throw new Error(
							"Automatic cloud provisioning has been retired. Provision a server with your provider, configure a domain you control, then register it with 'peerbit remote add <name> <address>'.",
						);
					},
				})
				.command({
					command: "terminate [name...]",
					describe:
						"Terminate legacy Hetzner instances recorded by older releases",
					builder: (killArgs: Argv) => {
						killArgs.option("all", {
							describe: "Kill all nodes",
							type: "boolean",
							default: false,
						});
						killArgs.option("token", {
							describe: "Used for Hetzner Cloud API (or set HCLOUD_TOKEN)",
							type: "string",
							alias: ["tok"],
						});
						killArgs.positional("name", {
							type: "string",
							describe: "Remote name",
							default: "localhost",
							demandOption: false,
							array: true,
						});
						killArgs.option("directory", {
							describe: "Peerbit directory",
							defaultDescription: "~.peerbit",
							type: "string",
							alias: "d",
							default: getHomeConfigDir(),
						});
						return killArgs;
					},
					handler: async (args) => {
						const remotes = new Remotes(getRemotesPath(args.directory));
						const allRemotes = await remotes.all();
						const selectedRemotes = allRemotes.filter(
							(remote) => args.all || args.name.includes(remote.name),
						);
						const retiredAWSRemote = selectedRemotes.find(
							(remote) => remote.origin?.type === "aws",
						);
						if (retiredAWSRemote?.origin?.type === "aws") {
							throw getRetiredAWSManagementError(retiredAWSRemote.origin);
						}

						for (const remote of selectedRemotes) {
							if (remote.origin?.type === "hetzner") {
								await terminateHetznerNode({
									serverId: remote.origin.serverId,
									token: (args.token as string) || undefined,
								});
							}
						}
					},
				})
				.command({
					command: "list",
					aliases: "ls",
					describe: "List remotes",
					builder: (yargs: Argv) => {
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

						const all = allRemotes;
						const apis = await Promise.all(
							all.map(async (remote) =>
								createClient(await getKeypair(args.directory), remote),
							),
						);
						const resolvedOrRejected = await Promise.allSettled(
							apis.map((x) => x.peer.id.get()),
						);

						if (all.length > 0) {
							const rows: string[][] = [];
							for (const [ix, remote] of all.entries()) {
								const row = [
									remote.name,
									remote.group || "",
									remote.origin?.type === "aws"
										? `aws\n${remote.origin.region}\n${remote.origin.instanceId}`
										: remote.origin?.type === "hetzner"
											? `hetzner\n${remote.origin.location}\n${remote.origin.serverId}`
											: "",
									resolvedOrRejected[ix].status === "fulfilled"
										? chalk.green("Y")
										: chalk.red("N"),
									remote.address,
								];
								rows.push(row);
							}
							const table = Table(
								["Name", "Group", "Origin", "Online", "Address"].map((x) => {
									return { value: x, align: "left" };
								}),
								rows,
							);
							console.log(table.render());
						} else {
							console.log("No remotes found!");
						}
					},
				})
				.command({
					command: "add <name> <address>",
					describe: "Add remote",
					builder: (yargs: Argv) => {
						yargs
							.positional("name", {
								type: "string",
								describe: "Remote address",
								demandOption: true,
							})
							.positional("address", {
								type: "string",
								describe: "Remote name",
								demandOption: true,
							})
							.option("group", {
								describe: "Group name",
								type: "string",
								alias: "g",
								default: DEFAULT_REMOTE_GROUP,
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
						const api = await createClient(await getKeypair(args.directory), {
							address: args.address,
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
							group: args.group,
						});
					},
				})
				.command({
					command: "remove <name>",
					describe: "Remove a remote",
					builder: (yargs: Argv) => {
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
								chalk.green("Removed remote with name: " + args.name),
							);
							remotes.save();
						} else {
							console.log(
								chalk.red("Did not find any remote with name: " + args.name),
							);
						}
					},
				})
				.command({
					command: "connect [name...]",
					describe: "Connect to remote(s)",
					builder: (yargs: Argv) => {
						yargs
							.positional("name", {
								type: "string",
								describe: "Remote name",
								default: "localhost",
								demandOption: false,
								array: true,
							})
							.option("all", {
								type: "boolean",
								describe: "Connect to all nodes",
								default: false,
							})
							.option("group", {
								type: "string",
								describe: "Remote group name",
								alias: "g",
								default: [],
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
							for (const [_ix, name] of names.entries()) {
								if (name === "localhost") {
									selectedRemotes.push({
										address: "http://localhost:" + LOCAL_API_PORT,
										name: "localhost",
										group: DEFAULT_REMOTE_GROUP,
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
								chalk.red("No remotes matched your connection condition"),
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
											remote.name.length,
										) +
											": " +
											string,
									);

								apis.push({
									log: logFn,
									name: remote.name,
									api: await createClient(keypair, remote),
								});
							}

							// try if authenticated
							for (const api of apis) {
								try {
									await api.api.program.list();
								} catch (error) {
									throw new Error(
										`Failed to connect to '${api.name}': ${error?.toString()}`,
									);
								}
							}

							const rl = readline.createInterface({
								input: process.stdin,
								output: process.stdout,
								terminal: true,
								historySize: 100,
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
												},
											})
											.command({
												command: "stats",
												describe: "Get connection and dial queue stats",
												handler: async () => {
													for (const api of apis) {
														const s = await api.api.peer.stats.get();
														api.log(
															`connections total=${s.connections.total} inbound=${s.connections.inbound} outbound=${s.connections.outbound} | dialQueue pending=${s.dialQueue.pending}`,
														);
													}
												},
											})
											.command({
												command: "address",
												describe: "Get addresses",
												handler: async (args) => {
													for (const api of apis) {
														(await api.api.peer.addresses.get()).forEach((x) =>
															api.log(x.toString()),
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
													builder: (yargs: Argv) => {
														yargs.positional("peer-id", {
															describe: "Peer id",
															type: "string",
															demandOption: true,
														});
														return yargs;
													},
													handler: async (args) => {
														const peerId: PeerId = peerIdFromString(
															args["peer-id"],
														);
														for (const api of apis) {
															await api.api.access.allow(peerId);
														}
													},
												})
												.command({
													command: "deny <peer-id>",
													describe: "Remove admin capabilities from peer-id",
													builder: (yargs: Argv) => {
														yargs.positional("peer-id", {
															describe: "Peer id",
															demandOption: true,
														});
														return yargs;
													},
													handler: async (args) => {
														const peerId = peerIdFromString(args["peer-id"]);
														for (const api of apis) {
															await api.api.access.deny(peerId);
														}
													},
												})
												.strict()
												.demandCommand();
										},
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
										},
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
															args.address,
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
																	}: ${error.toString()}`,
																),
															);
														}
													}
												},
											})
											.command({
												command: "drop-all",
												describe: "Drop all programs",
												handler: async () => {
													for (const api of apis) {
														await api.api.program.dropAll();
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
												command: "close-all",
												describe: "Close all programs",
												handler: async () => {
													for (const api of apis) {
														await api.api.program.closeAll();
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
														api.log(
															`Running programs (${[...Object.keys(list)].length}):`,
														);
														for (const key of Object.keys(list)) {
															let programOpenArgs = list[key];
															let argsString = "";
															if (
																programOpenArgs &&
																Object.keys(programOpenArgs).length > 0
															) {
																// show but indent
																argsString =
																	"  " + JSON.stringify(programOpenArgs);
															}

															api.log(chalk.green(key + argsString));
														}
													}
												},
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
															"Either base64 or variant argument needs to be provided",
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
													const knownKeys = new Set([
														"program",
														"base64",
														"variant",
														"_",
														"$0",
													]);
													const extraArgs = Object.fromEntries(
														Object.entries(args).filter(
															([key]) => !knownKeys.has(key),
														),
													);
													const mergedArgs = { ...startArg, ...extraArgs };

													for (const api of apis) {
														const address =
															await api.api.program.open(mergedArgs);
														api.log("Started program with address: ");
														api.log(chalk.green(address.toString()));
													}
												},
											})
											.strict(false) // because we have generic args
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
												const newPrograms =
													await api.api.dependency.install(installCommand);
												api.log(
													`New variants available (${newPrograms.length}):`,
												);
												newPrograms.forEach((p) => {
													api.log(chalk.green(p));
												});
											}
										},
									})
									.command({
										command: "versions",
										describe:
											"Print <pkg>@<version> for every dependency installed on the target node",

										handler: async (args) => {
											for (const api of apis) {
												const packageWithVersion: Record<string, string> =
													await api.api.dependency.versions();

												for (const [packageName, version] of Object.entries(
													packageWithVersion,
												)) {
													api.log(`${packageName}@${version}`);
												}
											}
										},
									})
									.command({
										command: "uninstall <package>",
										describe: "uninstall a previously installed dependency",
										builder: (yargs: any) => {
											yargs.positional("package", {
												type: "string",
												describe: "NPM package name to uninstall",
												demandOption: true,
											});
											return yargs;
										},
										handler: async (args) => {
											for (const api of apis) {
												await api.api.dependency.uninstall(args.package);
												api.log(`Uninstalled ${args.package}`);
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
										command: "self-update [version]",
										describe:
											"Update @peerbit/server on the target node to [version] (default latest) and restart",
										builder: (yargs) => {
											yargs.positional("version", {
												describe:
													"Version spec for @peerbit/server (e.g. 5.4.16)",
												type: "string",
											});
											return yargs;
										},
										handler: async (args) => {
											for (const api of apis) {
												const { version } = await api.api.selfUpdate(
													args.version as string | undefined,
												);
												api.log(
													`Self-update to @peerbit/server@${version} initiated`,
												);
											}
										},
									})
									.command({
										command: "stop",
										describe: "Stop the server",
										handler: async () => {
											for (const api of apis) {
												await api.api.stop();
											}
										},
									})
									.command({
										command: "log",
										describe: "Fetch log file contents from the server",
										builder: (yargs) => {
											yargs.option("n", {
												alias: "lines",
												type: "number",
												describe: "Number of last lines to display",
											});
											return yargs;
										},
										handler: async (args) => {
											for (const api of apis) {
												api.log(await api.api.log.fetch(args.n));
											}
										},
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
