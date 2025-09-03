import { bindService } from "@dao-xyz/borsh-rpc";
import type { RpcTransport } from "@dao-xyz/borsh-rpc";
import { PeerbitProxyContract } from "./peerbit.service.js";

export class PeerbitRPCHost {
    #unbind: (() => void) | undefined;
    #contract: PeerbitProxyContract;

    constructor(
        private readonly transport: RpcTransport
    ) {
        this.#contract = new PeerbitProxyContract(transport as any);
    }

    async start(): Promise<void> {
        if (this.#unbind) {
            throw new Error("Host already started");
        }
        this.#unbind = bindService(PeerbitProxyContract, this.transport, this.#contract);
    }

    async stop(): Promise<void> {
        if (this.#unbind) {
            this.#unbind();
            this.#unbind = undefined;
        }
    }
}
