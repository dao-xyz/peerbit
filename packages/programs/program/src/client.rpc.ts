import { method, service, struct, ctor, union } from "@dao-xyz/borsh-rpc";
import { OptionKind, vec } from "@dao-xyz/borsh";
import type { Manageable, OpenOptions } from "./handler.js";
import type { Address } from "./address.js";
import { deserialize } from "@dao-xyz/borsh";
import type { Client } from "./client.js";
import type { Multiaddr } from "@multiformats/multiaddr";

type MergeStrategy = 0 | 1 | 2; // replace=0, reject=1, reuse=2
const fromWireExisting = (v?: MergeStrategy): "replace" | "reject" | "reuse" | undefined =>
    v === 0 ? "replace" : v === 1 ? "reject" : v === 2 ? "reuse" : undefined;

@service()
export class ProgramClientRPCContract<T extends Manageable<any>> implements Client<T> {
    #impl: Client<T>
    constructor(impl: Client<T>) { this.#impl = impl; }

    @method({ returns: "void" })
    async start(): Promise<void> { await this.#impl.start(); }

    @method({ returns: "void" })
    async stop(): Promise<void> { await this.#impl.stop(); }

    @method({ returns: vec("string") })
    getMultiaddrs(): Multiaddr[] { return this.#impl.getMultiaddrs() }

    @method({ args: union(["string", vec("string")]), returns: "bool" })
    async dial(address: string | Multiaddr | Multiaddr[]): Promise<boolean> {
        const target = Array.isArray(addr) ? addr.map((a) => a) : addr;
        return this.#impl.dial(target as unknown as string);
    }

    @method(["string"], "void")
    async hangUp(id: string): Promise<void> { await this.#impl.hangUp(id); }

    @method(struct({ address: "string", args: new OptionKind(Uint8Array), argsSchema: new OptionKind(ctor("any")), timeout: new OptionKind("u32"), existing: new OptionKind("u8") }), "void")
    async open(args: { address: string; args?: Uint8Array; argsSchema?: new (...a: any[]) => any; timeout?: number; existing?: MergeStrategy }): Promise<void> {
        let decoded: unknown | undefined = undefined;
        if (args.args && args.argsSchema) {
            const ctorFn = args.argsSchema as any;
            decoded = ctorFn.deserialize ? ctorFn.deserialize(args.args) : deserialize(args.args, ctorFn);
        }
        await this.#impl.open(args.address as unknown as Address, { args: decoded as any, timeout: args.timeout, existing: fromWireExisting(args.existing) } as any);
    }
}


