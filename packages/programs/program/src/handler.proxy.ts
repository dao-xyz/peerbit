import { ctor, method, service, struct } from "@dao-xyz/borsh-rpc";
import type { RpcProxy } from "@dao-xyz/borsh-rpc";
import { OptionKind, deserialize, serialize } from "@dao-xyz/borsh";

export interface HandlerLike {
    open(address: string, options?: { args?: unknown; timeout?: number; existing?: "replace" | "reject" | "reuse" }): Promise<any>;
    stop?(): Promise<void>;
}

type MergeStrategy = 0 | 1 | 2; // replace=0, reject=1, reuse=2
const fromWireExisting = (v?: MergeStrategy): "replace" | "reject" | "reuse" | undefined =>
    v === 0 ? "replace" : v === 1 ? "reject" : v === 2 ? "reuse" : undefined;

@service()
export class HandlerRPCContract {
    #impl: HandlerLike;
    constructor(impl: HandlerLike) { this.#impl = impl; }

    @method(struct({ address: "string", args: new OptionKind(Uint8Array), argsSchema: new OptionKind(ctor("any")), timeout: new OptionKind("u32"), existing: new OptionKind("u8") }), "void")
    async open(args: { address: string; args?: Uint8Array; argsSchema?: new (...a: any[]) => any; timeout?: number; existing?: MergeStrategy }): Promise<void> {
        let decoded: unknown | undefined = undefined;
        if (args.args && args.argsSchema) {
            const ctorFn = args.argsSchema as any;
            decoded = ctorFn.deserialize ? ctorFn.deserialize(args.args) : deserialize(args.args, ctorFn);
        }
        await this.#impl.open(args.address, { args: decoded, timeout: args.timeout, existing: fromWireExisting(args.existing) });
    }

    @method({ returns: "void" })
    async stop(): Promise<void> { await this.#impl.stop?.(); }
}

export class HandlerClient {
    #rpc: RpcProxy<HandlerRPCContract>;
    constructor(rpc: RpcProxy<HandlerRPCContract>) { this.#rpc = rpc; }
    async open<Args>(address: string, options?: { args?: Args; argsSchema?: new (...a: any[]) => Args; timeout?: number; existing?: "replace" | "reject" | "reuse" }): Promise<void> {
        let wireArgs: Uint8Array | undefined = undefined;
        if (options?.args != null && options.argsSchema) {
            wireArgs = serialize(options.args);
        }
        const existingWire: MergeStrategy | undefined = options?.existing === "replace" ? 0 : options?.existing === "reject" ? 1 : options?.existing === "reuse" ? 2 : undefined;
        await this.#rpc.open({ address, args: wireArgs, argsSchema: options?.argsSchema as any, timeout: options?.timeout, existing: existingWire });
    }
    stop(): Promise<void> { return this.#rpc.stop(); }
}


