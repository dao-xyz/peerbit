import { expect } from "chai";
import { LoopbackPair, bindService, createProxyFromService, registerDependencies } from "@dao-xyz/borsh-rpc";
import { field, variant } from "@dao-xyz/borsh";
import { IndicesClient, IndicesRPCContract } from "../src/indices.rpc.js";
import type { Index, IndexEngineInitProperties, IndexedResult, Indices, IterateOptions, DeleteOptions, CountOptions } from "@peerbit/indexer-interface";
import { toId, type IdKey, toQuery } from "@peerbit/indexer-interface";

// A simple in-memory Index implementation to validate end-to-end RPC
class MemoryIndex<T extends Record<string, any>> implements Index<T, any> {
    #map = new Map<string, { id: IdKey; value: T }>();
    async init(_props: IndexEngineInitProperties<T, any>): Promise<Index<T, any>> { return this as unknown as Index<T, any>; }
    async drop(): Promise<void> { this.#map.clear(); }
    async get(id: IdKey): Promise<IndexedResult<T> | undefined> {
        const key = id.primitive.toString();
        const v = this.#map.get(key);
        return v ? { id: v.id, value: v.value } : undefined;
    }
    async put(value: T, id?: IdKey): Promise<void> {
        const keyId = id ?? toId((value as any).id ?? Math.random().toString());
        this.#map.set(keyId.primitive.toString(), { id: keyId, value });
    }
    async del(req: DeleteOptions): Promise<IdKey[]> {
        const q = toQuery(req.query);
        const removed: IdKey[] = [];
        const getVal = (obj: any, path: string[]) => path.reduce((o, k) => (o == null ? undefined : o[k]), obj);
        const matchOne = (obj: any): boolean => {
            if (q.length === 0) return true;
            return q.some((m: any) => {
                const key: string[] = m.key ?? m?.and?.[0]?.key ?? m?.or?.[0]?.key;
                const val = getVal(obj, key ?? []);
                if (m.value !== undefined) {
                    // eslint-disable-next-line eqeqeq
                    return val == (m.value.value ?? m.value);
                }
                if (m.not) return !matchOne(obj);
                if (m.and) return m.and.every((mm: any) => matchOne(obj));
                if (m.or) return m.or.some((mm: any) => matchOne(obj));
                if (m.constructor?.name === "IsNull") return val == null;
                return false;
            });
        };

        for (const { id, value } of Array.from(this.#map.values())) {
            if (matchOne(value)) {
                this.#map.delete(id.primitive.toString());
                removed.push(id);
            }
        }
        return removed;
    }
    async sum(options?: { key: string | string[]; query?: any }): Promise<bigint | number> {
        const keys = Array.isArray(options?.key) ? options!.key : options?.key ? [options.key] : [];
        let big = false;
        let nsum = 0;
        let bsum = 0n;
        for (const { value } of this.#map.values()) {
            const getVal = (obj: any, path: string) => path.split(".").reduce((o, k) => (o == null ? undefined : o[k]), obj);
            const vals = keys.length ? keys.map((k) => getVal(value as any, k)) : [];
            for (const v of vals) {
                if (typeof v === "bigint") { big = true; bsum += v; }
                else if (typeof v === "number") { nsum += v; }
            }
        }
        return big ? bsum : nsum;
    }
    async count(_query?: CountOptions): Promise<number> { return this.#map.size; }
    iterate(_request?: IterateOptions): any {
        const entries = Array.from(this.#map.values()).map(({ id, value }) => ({ id, value }));
        let i = 0;
        return {
            next: async (amount: number) => {
                const slice = entries.slice(i, i + amount);
                i += slice.length;
                return slice;
            },
            all: async () => entries,
            done: () => i >= entries.length,
            pending: async () => Math.max(0, entries.length - i),
            close: async () => { /* no-op */ },
        };
    }
    async getSize(): Promise<number> { return this.#map.size; }
    async start(): Promise<void> { }
    async stop(): Promise<void> { }
}

class MemoryIndices implements Indices {
    async init<T extends Record<string, any>>(_props: IndexEngineInitProperties<T, any>): Promise<Index<T, any>> {
        return new MemoryIndex<T>() as unknown as Index<T, any>;
    }
    async scope(_name: string): Promise<Indices> { return new MemoryIndices(); }
    async start(): Promise<void> { }
    async stop(): Promise<void> { }
    async drop(): Promise<void> { }
}

@variant(0)
class Model {
    @field({ type: "string" })
    id!: string;
    @field({ type: "u32" })
    x!: number;
    constructor(id?: string, x?: number) { if (id) this.id = id; if (x != null) this.x = x; }
}

@variant(1)
class ModelBig {
    @field({ type: "string" })
    id!: string;
    @field({ type: "u64" })
    y!: bigint;
    constructor(id?: string, y?: bigint) { if (id) this.id = id; if (y != null) this.y = y; }
}

class Stats {
    @field({ type: "u32" })
    n!: number;
    constructor(n?: number) { if (n != null) this.n = n; }
}

@variant(2)
class ModelNested {
    @field({ type: "string" })
    id!: string;
    @field({ type: Stats })
    stats!: Stats;
    constructor(id?: string, stats?: Stats) { if (id) this.id = id; if (stats) this.stats = stats; }
}

@variant(3)
class ModelMulti {
    @field({ type: "string" })
    id!: string;
    @field({ type: "u32" })
    a!: number;
    @field({ type: "u32" })
    b!: number;
    constructor(id?: string, a?: number, b?: number) { if (id) this.id = id; if (a != null) this.a = a; if (b != null) this.b = b; }
}

describe("Index iterator/CRUD via single service (proxy)", () => {
    it("put/get/count/del/iterate end-to-end", async () => {
        const loop = new LoopbackPair();
        const serverImpl = new MemoryIndices();
        registerDependencies(IndicesRPCContract as any, { Model, ModelBig, Stats, ModelNested, ModelMulti });
        const unbind = bindService(IndicesRPCContract as any, loop.a, new (IndicesRPCContract as any)(serverImpl));
        const rpc = createProxyFromService(IndicesRPCContract as any, loop.b) as any;
        const client = new IndicesClient(rpc);

        await client.start();
        const index = await client.init<Model, any>({ schema: Model, indexBy: ["id", "x"] });
        // index-level lifecycle calls should succeed via RPC
        await index.start();
        await index.stop();
        await index.put(new Model("a", 1));
        await index.put(new Model("b", 2));
        await index.put(new Model("c", 3));

        expect(await index.count()).to.equal(3);
        expect(await index.getSize()).to.equal(3);

        // get by id should return the stored value
        const got = await index.get(toId("b"));
        expect(got?.value.x).to.equal(2);
        expect(String(got?.id.primitive)).to.equal("b");

        const it = index.iterate();
        const firstTwo = await it.next(2);
        expect(firstTwo).to.have.length(2);
        expect(it.done()).to.be.false;
        const pending = await it.pending();
        expect(pending).to.be.greaterThan(0);
        const rest = await it.all();
        expect(rest).to.have.length(3);
        expect(it.done()).to.equal(true);
        await it.close();
        expect(it.done()).to.equal(true);

        const nsum = await index.sum({ key: "x" } as any);
        expect(nsum).to.equal(6);

        const removed = await index.del({ query: { x: 1 } as any });
        expect(removed).to.have.length(1);
        await index.del({ query: {} as any });
        expect(await index.count()).to.equal(0);

        const bi = await client.init<ModelBig, any>({ schema: ModelBig, indexBy: ["id", "y"] });
        await bi.put(new ModelBig("d", 4n));
        await bi.put(new ModelBig("e", 5n));
        const bsum = await bi.sum({ key: "y" } as any);
        expect(bsum).to.equal(9);

        await bi.put(new ModelBig("f", BigInt(Number.MAX_SAFE_INTEGER) + 10n));
        const huge = await bi.sum({ key: "y" } as any);
        const hugeBig = typeof huge === "bigint" ? huge : BigInt(huge);
        expect(hugeBig > BigInt(Number.MAX_SAFE_INTEGER)).to.equal(true);

        const nested = await client.init<ModelNested, any>({ schema: ModelNested, indexBy: ["id", "stats"] });
        await nested.put(new ModelNested("g", new Stats(7)));
        await nested.put(new ModelNested("h", new Stats(8)));
        const nsumNested = await nested.sum({ key: "stats.n" } as any);
        expect(nsumNested).to.equal(15);

        const multi = await client.init<ModelMulti, any>({ schema: ModelMulti, indexBy: ["id", "a", "b"] });
        await multi.put(new ModelMulti("i", 1, 2));
        await multi.put(new ModelMulti("j", 3, 4));
        const multiSum = await multi.sum({ key: ["a", "b"] } as any);
        expect(multiSum).to.equal(1 + 2 + 3 + 4);

        await client.stop();
        await client.drop();
        unbind();
    });
});