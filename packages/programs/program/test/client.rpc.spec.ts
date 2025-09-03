import { expect } from "chai";
import { LoopbackPair, bindService, createProxyFromService, registerDependencies } from "@dao-xyz/borsh-rpc";
import { ProgramClientRPCContract } from "../src/client.rpc.js";
import { ProgramClientProxy } from "../src/client.proxy.js";
import { field, variant, serialize } from "@dao-xyz/borsh";
import { multiaddr } from "@multiformats/multiaddr";
import { Ed25519PublicKey } from "@peerbit/crypto";

describe("ProgramClientRPCContract", () => {
    it("open(args/options), network and lifecycle", async () => {
        const loop = new LoopbackPair();
        const calls: string[] = [];
        @variant(0)
        class Args { @field({ type: "string" }) name!: string; constructor(name?: string) { if (name) this.name = name; } }
        const impl = {
            getMultiaddrs: () => [multiaddr("/ip4/127.0.0.1/tcp/1234")],
            async dial(a: any) { calls.push("dial:" + a.toString()); return true; },
            async hangUp(i: string) { calls.push("hangUp:" + i); },
            async start() { calls.push("start"); },
            async stop() { calls.push("stop"); },
            async open(address: string, options?: { args?: any; timeout?: number; existing?: string }) {
                calls.push(`open:${address}:${options?.args?.name ?? "-"}:${options?.timeout ?? 0}:${options?.existing ?? ""}`);
            },
        };
        registerDependencies(ProgramClientRPCContract as any, { Args });
        const unbind = bindService(ProgramClientRPCContract as any, loop.a, new (ProgramClientRPCContract as any)(impl));
        const rpc = createProxyFromService(ProgramClientRPCContract as any, loop.b) as any;

        await rpc.start();
        expect(await rpc.getMultiaddrs()).to.deep.equal(["/ip4/127.0.0.1/tcp/1234"]);
        expect(await rpc.dial("/ip4/1/tcp/2")).to.equal(true);
        await rpc.hangUp("id");
        const bytes = serialize(new Args("x"));
        await rpc.open({ address: "addr", args: bytes, argsSchema: Args, timeout: 9, existing: 0 });
        await rpc.stop();

        expect(calls[0]).to.equal("start");
        expect(calls).to.include("dial:/ip4/1/tcp/2");
        expect(calls).to.include("hangUp:id");
        expect(calls).to.include("stop");
        unbind();
    });

    it("ProgramClientProxy implements Client<T> with Multiaddr support", async () => {
        const loop = new LoopbackPair();
        const calls: string[] = [];
        const impl = {
            getMultiaddrs: () => [multiaddr("/ip4/127.0.0.1/tcp/1234")],
            async dial(a: any) { calls.push("dial:" + a.toString()); return true; },
            async hangUp(i: string) { calls.push("hangUp:" + i); },
            async start() { calls.push("start"); },
            async stop() { calls.push("stop"); },
            async open(address: string, options?: { args?: any; timeout?: number; existing?: string }) {
                calls.push(`open:${address}:${options?.args?.name ?? "-"}:${options?.timeout ?? 0}:${options?.existing ?? ""}`);
            },
        };
        const unbind = bindService(ProgramClientRPCContract as any, loop.a, new (ProgramClientRPCContract as any)(impl));
        const rpc = createProxyFromService(ProgramClientRPCContract as any, loop.b) as any;

        const peerId = { toString: () => "peer-id" } as any;
        const identity = { publicKey: new Ed25519PublicKey({ publicKey: new Uint8Array(32) }) } as any;
        const services = { pubsub: {} as any, blocks: {} as any, keychain: {} as any };
        const storage = {} as any;
        const indexer = {} as any;

        const proxy = new ProgramClientProxy(rpc, peerId, identity, services, storage, indexer);

        expect(proxy.peerId).to.equal(peerId);
        expect(proxy.identity).to.equal(identity);
        expect(proxy.services).to.equal(services);
        expect(proxy.storage).to.equal(storage);
        expect(proxy.indexer).to.equal(indexer);

        await proxy.start();
        const addrs = proxy.getMultiaddrs();
        expect(addrs).to.deep.equal([multiaddr("/ip4/127.0.0.1/tcp/1234")]);
        expect(addrs[0].toString()).to.equal("/ip4/127.0.0.1/tcp/1234");

        const multiaddr1 = multiaddr("/ip4/1.2.3.4/tcp/5678");
        expect(await proxy.dial(multiaddr1)).to.equal(true);
        expect(calls).to.include("dial:/ip4/1.2.3.4/tcp/5678");

        await proxy.hangUp("peer-id");
        expect(calls).to.include("hangUp:peer-id");

        await proxy.stop();
        expect(calls).to.include("stop");
        unbind();
    });
});


