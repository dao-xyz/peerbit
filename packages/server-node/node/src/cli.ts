import { Peerbit } from "@dao-xyz/peerbit";
import { createTestDomain, startCertbot } from "./domain.js";
import { serialize } from "@dao-xyz/borsh";
import { client, startServer } from "./api.js";
import { parsePublicKey } from "./utils.js";
import { createRecord } from "./aws.js";
import { toBase64Sync } from "@dao-xyz/peerbit-crypto";
import { createNode } from "./libp2p.js";

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
        .command<{
            /* ipfs: "js" | "go";
            disposable: boolean;
            timeout: number;
             */
            relay: boolean;
        }>({
            command: "start",
            describe: "Start node",
            builder: {
                relay: {
                    describe: "Relay only. No replication functionality",
                    type: "boolean",
                    default: false,
                },
                /*   ipfs: {
                      describe: "IPFS type",
                      type: "string",
                      choices: ["go", "js"],
                      default: "go",
                  },
                
                  disposable: {
                      describe:
                          "Run IPFS node as disposable (will be destroyed on termination)",
                      boolean: true,
                  }, */
            },
            handler: async (args) => {
                /*  const controller =
                     args.disposable || args.ipfs !== "go"
                         ? await startIpfs(args.ipfs, {
                             module: { disposable: args.disposable },
                         })
                         : await ipfsDocker(); */
                const node = await createNode();
                const controller = {
                    api: node,
                    stop: () => node.stop(),
                };
                const peer = args.relay
                    ? controller.api
                    : await Peerbit.create(controller.api);
                const server = await startServer(peer);
                const printNodeInfo = async () => {
                    console.log("Starting node with address(es): ");
                    const id = await (await client()).peer.id.get();
                    console.log("id: " + id);
                    console.log("Addresses: ");
                    for (const a of await (
                        await client()
                    ).peer.addresses.get()) {
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
            },
        })
        .command<{ email: string; outdir?: string; wait: boolean }>(
            "domain",
            "Setup a domain and certificate",
            (yargs) => {
                yargs
                    .command<{ email: string; outdir: string; wait: boolean }>({
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
                    .command<{
                        domain: string;
                        hostedZoneId: string;
                        email: string;
                        region: string;
                        outdir: string;
                        wait: boolean;
                        accessKeyId: string;
                        secretAccessKey: string;
                    }>({
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
            }
        )
        .command("topic", "Manage topics the node is listening to", (yargs) => {
            yargs
                .command<{ replicate: boolean }>({
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
                .command<{ topic: string; replicate: boolean }>({
                    command: "add <topic>",
                    describe: "add topic",
                    builder: (yargs: any) => {
                        yargs.positional("topic", {
                            describe: "Topic to add",
                            type: "string",
                            demandOption: true,
                        });
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
                        await c.topic.put(args.topic, args.replicate);
                        console.log(
                            "Topic: " + args.topic + " is now subscribed to"
                        );
                    },
                })
                .strict()
                .demandCommand();
            return yargs;
        })
        .command("network", "Manage networks", (yargs) => {
            yargs
                .command(
                    "relations",
                    "Manage relations in a network",
                    (yargs) => {
                        yargs
                            .command<{ address: string }>({
                                command: "list <address>",
                                describe: "List all relations in a network",
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
                                    const relations = await c.network.peers.get(
                                        args.address
                                    );
                                    if (!relations) {
                                        console.log("Network does not exist");
                                    } else {
                                        for (const r of relations) {
                                            console.log(
                                                r.from.toString() +
                                                    " -> " +
                                                    r.to.toString()
                                            );
                                        }
                                    }
                                },
                            })
                            .command<{ address: string; publicKey: string }>({
                                command: "add <address> <publicKey>",
                                describe: "Add trust to a peer in a network",
                                builder: (yargs: any) => {
                                    yargs.positional("address", {
                                        type: "string",
                                        describe: "Program address",
                                        demandOption: true,
                                    });
                                    yargs.positional("publicKey", {
                                        type: "string",
                                        describe: KEY_EXAMPLE,
                                        alias: "pk",
                                        demandOption: true,
                                    });
                                    return yargs;
                                },
                                handler: async (args) => {
                                    const c = await client();
                                    const pk = parsePublicKey(args.publicKey);
                                    if (!pk) {
                                        throw new Error(
                                            "Invalid public key: " +
                                                args.publicKey
                                        );
                                    }
                                    const relation = await c.network.peer.put(
                                        args.address,
                                        pk
                                    );
                                    console.log(
                                        "Added relation: " +
                                            relation.from.toString() +
                                            " -> " +
                                            relation.to.toString()
                                    );
                                },
                            })
                            .strict()
                            .demandCommand();
                        return yargs;
                    }
                )
                .strict()
                .demandCommand();
            return yargs;
        })
        .command("program", "Manage programs", (yargs) => {
            yargs
                .command<{ address: string }>({
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
                            console.log(toBase64Sync(serialize(program)));
                        }
                    },
                })
                .command<{ program: string; topic?: string }>({
                    command: "add <program>",
                    describe: "Add program",
                    builder: (yargs: any) => {
                        yargs.positional("program", {
                            type: "string",
                            describe: "base64 serialized",
                            demandOption: true,
                        });

                        yargs.option("topic", {
                            type: "string",
                            describe: "Replication topic",
                            alias: "t",
                        });

                        return yargs;
                    },
                    handler: async (args) => {
                        const c = await client();
                        const address = await c.program.put(
                            args.program,
                            args.topic
                        );
                        console.log(address.toString());
                    },
                })
                .command<{ library: string[] }>({
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
                                /* webpackIgnore: true */ lib
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
                .command<{ library: string }>({
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
