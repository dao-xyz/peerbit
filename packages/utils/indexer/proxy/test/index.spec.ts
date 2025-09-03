import { expect } from "chai";
import { LoopbackPair, bindService, createProxyFromService, registerDependencies } from "@dao-xyz/borsh-rpc";
import { IndicesClient, IndicesRPCContract } from "../src/indices.rpc.js";
import type { Index, IndexEngineInitProperties, Indices } from "@peerbit/indexer-interface";
import { field, variant } from "@dao-xyz/borsh";

// A minimal fake Index implementation
class FakeIndex<T extends Record<string, any>> implements Index<T, any> {
    async init(_props: IndexEngineInitProperties<T, any>): Promise<Index<T, any>> { return this as any; }
    async drop(): Promise<void> { }
    async get(): Promise<any> { return undefined; }
    async put(): Promise<void> { }
    async del(): Promise<any> { return []; }
    async sum(): Promise<any> { return 0; }
    async count(): Promise<number> { return 0; }
    iterate(): any { return { next: async () => [], all: async () => [], done: () => true, pending: async () => 0, close: async () => { } }; }
    async getSize(): Promise<number> { return 0; }
    async start(): Promise<void> { }
    async stop(): Promise<void> { }
}

// A minimal fake Indices implementation wiring FakeIndex
class FakeIndices implements Indices {
    #started = false;
    async init<T extends Record<string, any>>(_props: IndexEngineInitProperties<T, any>): Promise<Index<T, any>> { return new FakeIndex<T>() as any; }
    async scope(_name: string): Promise<Indices> { return new FakeIndices(); }
    async start(): Promise<void> { this.#started = true; }
    async stop(): Promise<void> { this.#started = false; }
    async drop(): Promise<void> { }
    get started() { return this.#started; }
}

// A borsh-decorated class to use as schema
@variant(0)
class Model {
    @field({ type: "u32" }) x: number;
    constructor(x: number = 0) { this.x = x; }
}

describe("IndicesRPCContract (proxy)", () => {
    it("binds and proxies init/scope/start/stop/drop and returns handles", async () => {
        const loop = new LoopbackPair();
        const serverImpl = new FakeIndices();
        registerDependencies(IndicesRPCContract as any, { Model });
        const unbind = bindService(IndicesRPCContract as any, loop.a, new (IndicesRPCContract as any)(serverImpl));
        const client = createProxyFromService(IndicesRPCContract as any, loop.b) as any;

        await client.start();
        const indexHandle = await client.init({ schema: Model, indexBy: ["x"] });
        expect(indexHandle).to.be.a("string").and.not.empty;
        const scopeHandle = await client.scope("test");
        expect(scopeHandle).to.be.a("string").and.not.empty;
        await client.stop();
        await client.drop();

        unbind();
    });

    it("IndicesClient.init returns an Index-like proxy (subservice-backed if available)", async () => {
        const loop = new LoopbackPair();
        const serverImpl = new FakeIndices();
        registerDependencies(IndicesRPCContract as any, { Model });
        const unbindA = bindService(IndicesRPCContract as any, loop.a, new (IndicesRPCContract as any)(serverImpl));
        const rpc = createProxyFromService(IndicesRPCContract as any, loop.b) as any;
        const client = new IndicesClient(rpc);

        await client.start();
        const index = await client.init<{ x: number }, any>({ schema: Model, indexBy: ["x"] });
        expect(index).to.include.keys("put", "count", "del");
        await index.put(new Model(1));
        const c = await index.count();
        expect(c).to.equal(0);
        const dels = await index.del({ query: { x: 1 } as any });
        expect(dels).to.be.an("array");
        await client.stop();
        await client.drop();
        unbindA();
    });

    it("IndicesClient.scope returns a scoped Indices that also returns Index proxies", async () => {
        const loop = new LoopbackPair();
        const serverImpl = new FakeIndices();
        registerDependencies(IndicesRPCContract as any, { Model });
        const unbindA = bindService(IndicesRPCContract as any, loop.a, new (IndicesRPCContract as any)(serverImpl));
        const rpc = createProxyFromService(IndicesRPCContract as any, loop.b) as any;
        const client = new IndicesClient(rpc);

        const scoped = await client.scope("my-scope");
        const index = await scoped.init<{ x: number }, any>({ schema: Model, indexBy: ["x"] });
        expect(index).to.include.keys("put", "count", "del");
        unbindA();
    });
});