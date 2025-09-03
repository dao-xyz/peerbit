import { expect } from "chai";
import { LoopbackPair, bindService, createProxyFromService, registerDependencies } from "@dao-xyz/borsh-rpc";
import { HandlerClient, HandlerRPCContract } from "../src/handler.proxy.js";
import { field, variant } from "@dao-xyz/borsh";

describe("program handler proxy", () => {
    it("forwards open with args/options and stop", async () => {
        const loop = new LoopbackPair();
        const calls: string[] = [];
        @variant(0)
        class Args {
            @field({ type: "string" }) name!: string;
            constructor(name?: string) { if (name) this.name = name; }
        }
        const impl = {
            async open(address: string, options?: { args?: unknown; timeout?: number; existing?: string }) {
                const a = options?.args as Args | undefined;
                calls.push(`open:${address}:${a?.name ?? "-"}:${options?.timeout ?? 0}:${options?.existing ?? ""}`);
            },
            async stop() { calls.push("stop"); },
        };
        // Register ctor dependencies for Args so ctor("any") arg schema can cross the wire
        registerDependencies(HandlerRPCContract as any, { Args });
        const unbind = bindService(HandlerRPCContract, loop.a, new HandlerRPCContract(impl));
        const rpc = createProxyFromService(HandlerRPCContract, loop.b);
        const client = new HandlerClient(rpc);
        await client.open("addr1", { args: new Args("x"), argsSchema: Args, timeout: 7, existing: "replace" });
        await client.stop();
        expect(calls).to.deep.equal(["open:addr1:x:7:replace", "stop"]);
        unbind();
    });
});


