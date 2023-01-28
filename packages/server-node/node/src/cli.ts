import { createTestDomain, startCertbot } from "./domain.js";
import { serialize } from "@dao-xyz/borsh";
import { client, startServerWithNode } from "./api.js";
import { createRecord } from "./aws.js";
import { toBase64 } from "@dao-xyz/peerbit-crypto";

const KEY_EXAMPLE =
    'E.g. [CHAIN TYPE]/[PUBLICKEY]. e.g. if ethereum: "ethereum/0x4e54fD83..."';
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
                relay: {
                    describe: "Relay only. No replication functionality",
                    type: "boolean",
                    default: false,
                },
            },
            handler: async (args) => {
                await startServerWithNode(args.relay);
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
                            describe:
                                "Email for Lets encrypt autorenewal messages",
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
                        await startCertbot(
                            domain,
                            args.email,
                            args.outdir,
                            args.wait
                        );
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
                            describe:
                                "domain, e.g. abc.example.com, example.com",
                            alias: "d",
                            type: "string",
                            demandOption: true,
                        },
                        hostedZoneId: {
                            describe:
                                'The id of the hosted zone "HostedZoneId"',
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
                            describe:
                                "Email for Lets encrypt autorenewal messages",
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
                        const { exit } = await import("process");
                        exit();
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
                        const c = await client();
                        const topics = await c.topics.get(args.replicate);
                        if (topics?.length > 0) {
                            console.log("Topic (" + topics.length + "):");
                            for (const t of topics) {
                                console.log(t);
                            }
                        } else {
                            console.log("Not subscribed to any topics");
                        }
                    },
                })
                .strict()
                .demandCommand();
            return yargs;
        })
        .command("program", "Manage programs", (yargs) => {
            yargs
                .command({
                    command: "get <address>",
                    describe: "Get program manifest/serialized in base64",
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
                        const program = await c.program.get(args.address);
                        if (!program) {
                            console.log("Program does not exist");
                        } else {
                            console.log(toBase64(serialize(program)));
                        }
                    },
                })
                .command({
                    command: "add <program>",
                    describe: "Add program",
                    builder: (yargs: any) => {
                        yargs.positional("program", {
                            type: "string",
                            describe: "base64 serialized",
                            demandOption: true,
                        });
                        return yargs;
                    },
                    handler: async (args) => {
                        const c = await client();
                        const address = await c.program.put(args.program);
                        console.log(address.toString());
                    },
                })
                .command({
                    command: "import <library>",
                    describe: "import a library that contains programs",
                    builder: (yargs: any) => {
                        yargs.positional("library", {
                            type: "array",
                            describe:
                                "Library name (will be loaded with js import(...)). Onlu libraries that are globally installed and can be imported",
                            demandOption: true,
                        });
                        return yargs;
                    },
                    handler: async (args) => {
                        for (const lib of args.library) {
                            const importedLib = await import(
                                /* webpackIgnore: true */ /* @vite-ignore */ lib
                            );
                            console.log("imported lib:", importedLib);
                        }
                    },
                })
                .strict()
                .demandCommand();
            return yargs;
        })
        .command("library", "Manage libraries", (yargs) => {
            yargs
                .command({
                    command: "add <library>",
                    describe: "add a library that contains programs",
                    builder: (yargs: any) => {
                        yargs.positional("library", {
                            type: "string",
                            describe:
                                "Library name (will be loaded with js import(...)). Onlu libraries that are globally installed and can be imported",
                            demandOption: true,
                        });
                        return yargs;
                    },
                    handler: async (args) => {
                        const c = await client();
                        await c.library.put(args.library);
                    },
                })
                .strict()
                .demandCommand();
            return yargs;
        })
        .help()
        .strict()
        .demandCommand().argv;
};
