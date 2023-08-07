import {
	createTestDomain,
	getDomainFromConfig,
	loadConfig,
	startCertbot,
} from "./domain.js";
import { startServerWithNode } from "./server.js";
import { createRecord } from "./aws.js";
import { getHomeConfigDir } from "./config.js";
import chalk from "chalk";
import { client } from "./client.js";
import { StartProgram } from "./types.js";

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
			builder: {
				directory: {
					describe: "Directory for all data created by the node",
					defaultDescription: "~.peerbit",
					type: "string",
					default: await getHomeConfigDir(),
				},

				bootstrap: {
					describe: "Whether to connect to bootstap nodes on startup",
					type: "boolean",
					default: false,
				},
			},
			handler: async (args) => {
				await startServerWithNode({
					directory: args.directory,
					domain: await loadConfig().then((config) =>
						config ? getDomainFromConfig(config) : undefined
					),
					bootstrap: args.bootstrap,
				});
			},
		})
		.command("domain", "Setup a domain and certificate", (yargs) => {
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
						const { exit } = await import("process");
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
						await startCertbot(args.domain, args.email, args.outdir, args.wait);
						const { exit } = await import("process");
						exit();
					},
				})
				.strict()
				.demandCommand();
		})
		.command("network", "Manage network", (yargs) => {
			yargs
				.command({
					command: "bootstrap",
					describe: "Connect to bootstrap nodes",
					handler: async () => {
						const c = await client();
						await c.network.bootstrap();
					},
				})
				.strict()
				.demandCommand();
		})

		.command("topic", "Manage topics the node is listening to", (yargs) => {
			yargs
				.command({
					command: "list",
					aliases: "ls",
					describe: "List all topics",
					builder: (yargs: any) => {
						yargs.option("replicate", {
							type: "boolean",
							describe: "Replicate data on this topic",
							alias: "r",
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
		})
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
						const c = await client();
						const program = await c.program.has(args.address);
						if (!program) {
							console.log(chalk.red("Closed"));
						} else {
							console.log(chalk.green("Open"));
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
						const c = await client();
						await c.program.drop(args.address);
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
						const c = await client();
						await c.program.close(args.address);
					},
				})
				.command({
					command: "list",
					describe: "List all running programs",
					aliases: "ls",
					handler: async (args) => {
						const c = await client();
						const list = await c.program.list();

						console.log(`Running programs (${list.length}):`);
						list.forEach((p) => {
							console.log(chalk.green(p));
						});
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
							alias: "b",
						});
						yargs.option("variant", {
							type: "string",
							describe: "Variant name",
							alias: "v",
						});
						return yargs;
					},
					handler: async (args) => {
						const c = await client();
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
						const address = await c.program.open(startArg);
						console.log("Started program with address: ");
						console.log(chalk.green(address.toString()));
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
					describe: "Installed dependency will be loaded with js import(...)",
					demandOption: true,
				});

				return yargs;
			},
			handler: async (args) => {
				const c = await client();
				const newPrograms = await c.dependency.install(args["package-spec"]);

				console.log(`New programs available (${newPrograms.length}):`);
				newPrograms.forEach((p) => {
					console.log(chalk.green(p));
				});
			},
		})
		.help()
		.strict()
		.demandCommand().argv;
};
