import isNode from "is-node";
import { ControllerType, createController, IPFSOptions } from "ipfsd-ctl";

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
    type: "js" | "go",
    options?: {
        ipfsOptions?: IPFSOptions;
        module?: { disposable: boolean; go?: { args?: string[] } };
    }
) => {
    let module: Module;
    if (!isNode) {
        if (type === "go") throw new Error("Not supported");
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
    } else if (type === "go") {
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
    } else {
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
            Discovery: {
                MDNS: {
                    Enabled: false, // should we?
                },
                webRTCStar: {
                    Enabled: false,
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
