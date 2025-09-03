import type { CountOptions, DeleteOptions, Index, IndexEngineInitProperties, Indices, IndexIterator, IterateOptions, Shape } from "@peerbit/indexer-interface";
import type { AbstractType } from "@dao-xyz/borsh";
import { IdKey, Query, toQuery } from "@peerbit/indexer-interface";
import { ctor, method, service, struct } from "@dao-xyz/borsh-rpc";
import { OptionKind, vec, serialize, deserialize } from "@dao-xyz/borsh";
import type { RpcProxy } from "@dao-xyz/borsh-rpc";
import { v4 as uuid } from "uuid";
import { iteratorOpen, iteratorNext, iteratorDone, iteratorPending, iteratorClose, iteratorAll } from "./iterator-contract.js";

// Shared registries so multiple RPC services can cooperate around handles
const indicesRegistry = new Map<string, Index<any, any>>();
const scopesRegistry = new Map<string, Indices>();

@service()
export class IndicesRPCContract {
    #impl: Indices;

    constructor(impl: Indices) {
        this.#impl = impl;
    }

    @method(
        struct({
            indexBy: vec("string"),
            schema: ctor("any"),
        }),
        "string",
    )
    async init<T extends Record<string, any>, NestedType>(
        properties: IndexEngineInitProperties<T, NestedType>,
    ): Promise<string> {
        const index = await this.#impl.init<T, NestedType>(properties);
        const handle = uuid();
        indicesRegistry.set(handle, index);
        return handle;
    }

    @method(["string"], "string")
    async scope(name: string): Promise<string> {
        const scoped = await this.#impl.scope(name);
        const handle = uuid();
        scopesRegistry.set(handle, scoped);
        return handle;
    }

    @method({ returns: "void" })
    async start(): Promise<void> { await this.#impl.start?.(); }

    @method({ returns: "void" })
    async stop(): Promise<void> { await this.#impl.stop?.(); }

    @method({ returns: "void" })
    async drop(): Promise<void> { await this.#impl.drop?.(); }

    @method(struct({ handle: "string", id: IdKey }), new OptionKind(Uint8Array))
    async get(args: { handle: string; id: IdKey }): Promise<Uint8Array | undefined> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        const res = await index.get(args.id);
        return res ? serialize(res.value as any) : undefined;
    }

    @method(struct({ handle: "string" }), "void")
    async dropIndex(args: { handle: string }): Promise<void> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        await index.drop();
    }

    @method(struct({ handle: "string", value: Uint8Array, schema: ctor("any"), id: new OptionKind(IdKey) }), "void")
    async put(args: { handle: string; value: Uint8Array; schema: new (...args: any[]) => any; id?: IdKey }): Promise<void> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        const ctorFn = args.schema as any;
        const value = ctorFn.deserialize ? ctorFn.deserialize(args.value) : deserialize(args.value, ctorFn as any);
        await index.put(value, args.id);
    }

    @method(struct({ handle: "string", query: vec(Query) }), vec(IdKey))
    async del(args: { handle: string; query: Query[] }): Promise<IdKey[]> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        return await index.del({ query: args.query });
    }

    @method(struct({ handle: "string", query: new OptionKind(vec(Query)) }), "u32")
    async count(args: { handle: string; query?: Query[] }): Promise<number> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        return await index.count({ query: args.query });
    }

    @method(struct({ handle: "string", key: vec("string"), query: new OptionKind(vec(Query)) }), "string")
    async sum(args: { handle: string; key: string[]; query?: Query[] }): Promise<string> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        const r = await index.sum({ key: args.key, query: args.query });
        return typeof r === "bigint" ? r.toString() : String(r);
    }

    @method(struct({ handle: "string" }), "u32")
    async getSize(args: { handle: string }): Promise<number> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        return await index.getSize();
    }

    @method(struct({ handle: "string" }), "void")
    async startIndex(args: { handle: string }): Promise<void> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        await index.start?.();
    }

    @method(struct({ handle: "string" }), "void")
    async stopIndex(args: { handle: string }): Promise<void> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        await index.stop?.();
    }

    @method(struct({ handle: "string" }), "string")
    async iterateOpen(args: { handle: string }): Promise<string> {
        const index = indicesRegistry.get(args.handle);
        if (!index) throw new Error("Unknown index handle");
        return iteratorOpen(index);
    }

    @method(struct({ iterator: "string", amount: "u32" }), vec(struct({ id: IdKey, value: Uint8Array })))
    async iterateNext(args: { iterator: string; amount: number }): Promise<Array<{ id: IdKey; value: Uint8Array }>> {
        const res = await iteratorNext(args.iterator, args.amount);
        return res as any;
    }

    @method(struct({ iterator: "string" }), "bool")
    async iterateDone(args: { iterator: string }): Promise<boolean> {
        return iteratorDone(args.iterator);
    }

    @method(struct({ iterator: "string" }), "u32")
    async iteratePending(args: { iterator: string }): Promise<number> {
        return await iteratorPending(args.iterator);
    }

    @method(struct({ iterator: "string" }), "void")
    async iterateClose(args: { iterator: string }): Promise<void> {
        await iteratorClose(args.iterator);
    }

    @method(struct({ iterator: "string" }), vec(struct({ id: IdKey, value: Uint8Array })))
    async iterateAll(args: { iterator: string }): Promise<Array<{ id: IdKey; value: Uint8Array }>> {
        const res = await iteratorAll(args.iterator);
        return res as any;
    }

    @method(
        struct({ scope: "string", indexBy: vec("string"), schema: ctor("any") }),
        "string",
    )
    async scopedInit<T extends Record<string, any>, NestedType>(
        args: { scope: string; indexBy?: string[]; schema: any },
    ): Promise<string> {
        const scoped = scopesRegistry.get(args.scope);
        if (!scoped) throw new Error("Unknown scope handle");
        const index = await scoped.init<T, NestedType>({ indexBy: args.indexBy, schema: args.schema });
        const handle = uuid();
        indicesRegistry.set(handle, index);
        return handle;
    }
}

// Client-side helper: typed index proxy from a handle
export function createIndexProxy<T extends Record<string, any>, N = any>(
    client: RpcProxy<IndicesRPCContract>,
    handle: string,
    schema: AbstractType<T>,
): Index<T, N> {
    const parseSum = (v: string): bigint | number => {
        try {
            const b = BigInt(v);
            return b <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : b;
        } catch {
            return Number(v);
        }
    };

    type Ctor<U> = new (...args: any[]) => U;
    const ctorSchema = schema as unknown as Ctor<T>;

    const proxy = {
        init: async () => proxy as unknown as Index<T, N>,
        drop: () => client.dropIndex({ handle }),
        get: async (id: IdKey, _options?: { shape: Shape }) => {
            const res = await client.get({ handle, id });
            if (!res) return undefined;
            const value = (ctorSchema as any).deserialize
                ? (ctorSchema as any).deserialize(res)
                : deserialize(res, ctorSchema as any);
            return { id, value } as any;
        },
        put: (value: T, id?: IdKey) => client.put({ handle, value: serialize(value), schema: ctorSchema, id }),
        del: (req: DeleteOptions) => client.del({ handle, query: toQuery(req.query) }),
        sum: async (req: { key: string | string[]; query?: any }) =>
            parseSum(
                await client.sum({
                    handle,
                    key: Array.isArray(req.key) ? req.key : [req.key],
                    query: req.query ? toQuery(req.query) : undefined,
                }),
            ),
        count: (query?: CountOptions) => client.count({ handle, query: query?.query ? toQuery(query.query) : undefined }),
        iterate: undefined as any,
        getSize: () => client.getSize({ handle }),
        start: () => client.startIndex({ handle }),
        stop: () => client.stopIndex({ handle }),
    } as unknown as Index<T, N>;
    attachIterateToIndexProxy<T>(proxy, client, handle, schema);
    return proxy;
}

export function createIteratorProxy<T extends Record<string, any>, S extends Shape | undefined = undefined>(
    client: RpcProxy<IndicesRPCContract>,
    iterator: string,
    schema: AbstractType<T>,
): IndexIterator<T, S> {
    let lastDone: boolean | undefined = undefined;
    const api: IndexIterator<T, S> = {
        next: async (amount: number) => {
            const res = await client.iterateNext({ iterator, amount });
            const ctorSchema = schema as any;
            const out = res.map((r) => {
                const value = ctorSchema.deserialize
                    ? ctorSchema.deserialize(r.value)
                    : deserialize(r.value, ctorSchema as any);
                return { id: r.id, value } as any;
            });
            try { lastDone = await client.iterateDone({ iterator }); } catch { }
            return out;
        },
        all: async () => {
            const res = await client.iterateAll({ iterator });
            const ctorSchema = schema as any;
            const out = res.map((r) => {
                const value = ctorSchema.deserialize
                    ? ctorSchema.deserialize(r.value)
                    : deserialize(r.value, ctorSchema as any);
                return { id: r.id, value } as any;
            });
            lastDone = true;
            return out;
        },
        done: () => lastDone,
        pending: async () => {
            const p = await client.iteratePending({ iterator });
            try { lastDone = await client.iterateDone({ iterator }); } catch { }
            return p;
        },
        close: async () => {
            await client.iterateClose({ iterator });
            lastDone = true;
        },
    };
    return api;
}

export function attachIterateToIndexProxy<T extends Record<string, any>>(
    proxy: any,
    client: RpcProxy<IndicesRPCContract>,
    handle: string,
    schema: AbstractType<T>,
) {
    proxy.iterate = <S extends Shape | undefined = undefined>(_request?: IterateOptions, _options?: { shape?: S; reference?: boolean }): IndexIterator<T, S> => {
        let itHandle: string | undefined;
        let lastDone: boolean | undefined = undefined;
        const ensureOpen = async () => {
            if (!itHandle) {
                itHandle = await client.iterateOpen({ handle });
            }
            return itHandle;
        };
        const api: IndexIterator<T, S> = {
            next: async (amount: number) => {
                const h = await ensureOpen();
                const res = await client.iterateNext({ iterator: h, amount });
                const ctorSchema = schema as any;
                const out = res.map((r) => {
                    const value = ctorSchema.deserialize
                        ? ctorSchema.deserialize(r.value)
                        : deserialize(r.value, ctorSchema as any);
                    return { id: r.id, value } as any;
                });
                try { lastDone = await client.iterateDone({ iterator: h }); } catch { }
                return out;
            },
            all: async () => {
                const h = await ensureOpen();
                const res = await client.iterateAll({ iterator: h });
                const ctorSchema = schema as any;
                const out = res.map((r) => {
                    const value = ctorSchema.deserialize
                        ? ctorSchema.deserialize(r.value)
                        : deserialize(r.value, ctorSchema as any);
                    return { id: r.id, value } as any;
                });
                lastDone = true;
                return out;
            },
            done: () => lastDone,
            pending: async () => {
                const h = await ensureOpen();
                const p = await client.iteratePending({ iterator: h });
                try { lastDone = await client.iterateDone({ iterator: h }); } catch { }
                return p;
            },
            close: async () => {
                if (itHandle) {
                    await client.iterateClose({ iterator: itHandle });
                    itHandle = undefined;
                }
                lastDone = true;
            },
        };
        return api;
    };
}

export class IndicesClient implements Indices {
    #rpc: RpcProxy<IndicesRPCContract>;
    #scope?: string;
    constructor(rpc: RpcProxy<IndicesRPCContract>, scopeHandle?: string) {
        this.#rpc = rpc;
        this.#scope = scopeHandle;
    }
    async init<T extends Record<string, any>, NestedType>(
        properties: IndexEngineInitProperties<T, NestedType>,
    ): Promise<Index<T, NestedType>> {
        const handle = this.#scope
            ? await this.#rpc.scopedInit({ scope: this.#scope, indexBy: properties.indexBy, schema: properties.schema })
            : await this.#rpc.init({ indexBy: properties.indexBy, schema: properties.schema });
        return createIndexProxy<T, NestedType>(this.#rpc, handle, properties.schema);
    }
    async scope(name: string): Promise<Indices> {
        const handle = await this.#rpc.scope(name);
        return new IndicesClient(this.#rpc, handle);
    }
    start(): Promise<void> { return this.#rpc.start(); }
    stop(): Promise<void> { return this.#rpc.stop(); }
    drop(): Promise<void> { return this.#rpc.drop(); }
}
