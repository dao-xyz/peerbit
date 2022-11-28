import { ControllerType, createController, IPFSOptions } from "ipfsd-ctl";
import { installDocker, startContainer } from "./docker";
import { IPFS } from "ipfs-core-types";
import { delay } from "@dao-xyz/peerbit-time";

interface Module {
    type: ControllerType;
    test: boolean;
    disposable: boolean;
    args?: string[];
    ipfsHttpModule?: any;
    ipfsBin?: any;
    ipfsModule?: any;
    ipfsOptions?: IPFSOptions; // to be set later
}

/**
 * If Browser, it will resolve js, else you can choose go or js
 * Will start as disposable as default
 * At the moment, swarm settings has to be Swarm: ["/ip4/0.0.0.0/tcp/4001", "/ip4/0.0.0.0/tcp/8081/ws", "/ip6/::/tcp/4001"] to match nginx config
 *
 */
export const startIpfs = async (
    type: "js" | string,
    options?: {
        ipfsOptions?: IPFSOptions;
        module?: { disposable: boolean; go?: { args?: string[] } };
    }
) => {
    let module: Module;
    if (type === "go") {
        throw new Error("Not supported");
    }

    const disposable = options?.module?.disposable || false;
    if (type === "js") {
        const ipfsModule = await import("ipfs");
        module = {
            type: "proc",
            disposable,
            test: false,
            ipfsModule,
        };
    } /* else if (type === "go") {
        const ipfsHttpModule = await import("ipfs-http-client");
        const ipfsBin = await import("go-ipfs");
        const extraArgs = options?.module?.go?.args
            ? options?.module.go?.args
            : [];
        module = {
            type: "go",
            test: false,
            disposable,
            args: ["--enable-pubsub-experiment", ...extraArgs],
            ipfsHttpModule,
            ipfsBin: ipfsBin.path(),
        };
    } */ else {
        throw new Error("Unexpected IPFS module type: " + type);
    }
    const ipfsOptions = options?.ipfsOptions || {
        preload: {
            enabled: false,
        },
        EXPERIMENTAL: {
            pubsub: true,
        } as any,
        config: {
            Addresses: {
                API: "/ip4/127.0.0.1/tcp/0",
                Swarm: [
                    "/ip4/0.0.0.0/tcp/4001",
                    "/ip4/0.0.0.0/tcp/8081/ws",
                    "/ip6/::/tcp/4001",
                ], //
                Gateway: "/ip4/0.0.0.0/tcp/0",
            },
            Bootstrap: [],
            // TODO Remove delegators?
            Discovery: {
                MDNS: {
                    Enabled: false, // should we?
                },
                webRTCStar: {
                    Enabled: false,
                },
            },
            Pubsub: {
                enabled: true,
            },
            Swarm: {
                RelayService: {
                    Enabled: true,
                },
            },
        },
    };

    module.ipfsOptions = ipfsOptions;

    const controller = await createController(module);
    if (!controller.initialized) {
        await controller.init();
    }
    if (!controller.started) {
        await controller.start();
    }
    return controller;
};

export const ipfsDocker = async (): Promise<{
    api: IPFS;
    stop: () => Promise<void>;
}> => {
    const { exec } = await import("child_process");
    await new Promise((resolve, reject) => {
        exec(
            'echo "#!/bin/sh \nset -ex \nipfs bootstrap rm all \nipfs config Addresses.Swarm \'[\\"/ip4/0.0.0.0/tcp/4001\\", \\"/ip4/0.0.0.0/tcp/8081/ws\\", \\"/ip6/::/tcp/4001\\"]\' --json\nipfs config --json Pubsub.Enabled true \nipfs config Swarm.RelayService \'{\\"Enabled\\": true}\' --json" > ipfs-config.sh',
            (error, stdout, stderr) => {
                if (error || stderr) {
                    reject("Failed to create config file" + stderr);
                }
                resolve(stdout);
            }
        );
    });

    await installDocker();
    await startContainer(
        "sudo docker start ipfs_host 2>/dev/null || docker run -d --name ipfs_host -v $(pwd)/ipfs-config.sh:/container-init.d/001-test.sh  -p 4001:4001 -p 4001:4001/udp -p 127.0.0.1:8081:8081 -p 127.0.0.1:5001:5001 ipfs/kubo:latest daemon"
    );
    const c = await import("ipfs-http-client");
    const http = await import("http");
    for (let i = 0; i < 3; i++) {
        try {
            const client = c.create({
                timeout: 10 * 1000,
                agent: new http.Agent({
                    keepAlive: true,
                    maxSockets: Infinity,
                }),
            });
            return {
                api: client,
                stop: async () => undefined,
            };
        } catch (error: any) {
            console.log(
                `Faield to create client, retrying ${i}: ${error?.message}`
            );
        }
    }
    throw new Error("Failed to create ipfs-http-client");
};
