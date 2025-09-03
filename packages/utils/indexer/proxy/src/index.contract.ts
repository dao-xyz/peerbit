import { OptionKind, serialize, deserialize, vec } from "@dao-xyz/borsh";
import { ctor, method, service, struct } from "@dao-xyz/borsh-rpc";
import type { AbstractType } from "@dao-xyz/borsh";
import type { Index, IndexIterator, IterateOptions, Shape } from "@peerbit/indexer-interface";
import { IdKey, Query, Sort } from "@peerbit/indexer-interface";
import { v4 as uuid } from "uuid";

@service()
export class IndexContract<T extends Record<string, any> = Record<string, any>, N = any> {
    #index: Index<T, N>;
    #iterators = new Map<string, IndexIterator<T, any>>();

    constructor(index: Index<T, N>) {
        this.#index = index;
    }

    // lifecycle
    @method({ returns: "void" })
    async start(): Promise<void> { await this.#index.start?.(); }

    @method({ returns: "void" })
    async stop(): Promise<void> { await this.#index.stop?.(); }

    @method({ returns: "void" })
    async drop(): Promise<void> { await this.#index.drop(); }

    // CRUD
    @method(struct({ id: IdKey }), new OptionKind(Uint8Array))
    async get(args: { id: IdKey }): Promise<Uint8Array | undefined> {
        const res = await this.#index.get(args.id);
        return res ? serialize(res.value as any) : undefined;
    }

    @method(struct({ value: Uint8Array, schema: ctor("any"), id: new OptionKind(IdKey) }), "void")
    async put(args: { value: Uint8Array; schema: new (...a: any[]) => any; id?: IdKey }): Promise<void> {
        type BorshCtor<U> = { deserialize?: (buf: Uint8Array) => U } & (new (...args: any[]) => U);
        const ctorFn = args.schema as BorshCtor<T>;
        const value = ctorFn.deserialize ? ctorFn.deserialize(args.value) : deserialize(args.value, ctorFn as any);
        await this.#index.put(value as T, args.id);
    }

    @method(struct({ query: vec(Query) }), vec(IdKey))
    async del(args: { query: Query[] }): Promise<IdKey[]> {
        return await this.#index.del({ query: args.query });
    }

    @method(struct({ key: vec("string"), query: new OptionKind(vec(Query)) }), "string")
    async sum(args: { key: string[]; query?: Query[] }): Promise<string> {
        const r = await this.#index.sum({ key: args.key, query: args.query });
        return typeof r === "bigint" ? r.toString() : String(r);
    }

    @method(struct({ query: new OptionKind(vec(Query)) }), "u32")
    async count(args: { query?: Query[] }): Promise<number> {
        return await this.#index.count({ query: args.query });
    }

    @method({ returns: "u32" })
    async getSize(): Promise<number> { return await this.#index.getSize(); }

    // iteration (per-instance iterator registry)
    @method(struct({ query: new OptionKind(vec(Query)), sort: new OptionKind(vec(Sort)) }), "string")
    async iterateOpen(args: { query?: Query[]; sort?: Sort[] }): Promise<string> {
        const req: IterateOptions | undefined = (args.query || args.sort) ? { query: args.query, sort: args.sort as any } : undefined;
        const it = this.#index.iterate(req, undefined);
        const handle = uuid();
        this.#iterators.set(handle, it as IndexIterator<T, any>);
        return handle;
    }

    @method(struct({ iterator: "string", amount: "u32" }), vec(struct({ id: IdKey, value: Uint8Array })))
    async iterateNext(args: { iterator: string; amount: number }): Promise<Array<{ id: IdKey; value: Uint8Array }>> {
        const it = this.#iterators.get(args.iterator);
        if (!it) throw new Error("Unknown iterator");
        const res = await it.next(args.amount);
        return res.map((r: any) => ({ id: r.id, value: serialize(r.value) }));
    }

    @method(struct({ iterator: "string" }), vec(struct({ id: IdKey, value: Uint8Array })))
    async iterateAll(args: { iterator: string }): Promise<Array<{ id: IdKey; value: Uint8Array }>> {
        const it = this.#iterators.get(args.iterator);
        if (!it) throw new Error("Unknown iterator");
        const res = await it.all();
        return res.map((r: any) => ({ id: r.id, value: serialize(r.value) }));
    }

    @method(struct({ iterator: "string" }), "bool")
    async iterateDone(args: { iterator: string }): Promise<boolean> {
        const it = this.#iterators.get(args.iterator);
        if (!it) throw new Error("Unknown iterator");
        return it.done() ?? false;
    }

    @method(struct({ iterator: "string" }), "u32")
    async iteratePending(args: { iterator: string }): Promise<number> {
        const it = this.#iterators.get(args.iterator);
        if (!it) throw new Error("Unknown iterator");
        return await it.pending();
    }

    @method(struct({ iterator: "string" }), "void")
    async iterateClose(args: { iterator: string }): Promise<void> {
        const it = this.#iterators.get(args.iterator);
        if (!it) return;
        this.#iterators.delete(args.iterator);
        await it.close();
    }
}

// Client-side wrapper: turn IndexContract RPC proxy into Index<T,N>
export function wrapIndexSubserviceProxy<T extends Record<string, any>, N = any>(
    sub: any,
    schema: AbstractType<T>,
): Index<T, N> {
    const parseSum = (v: string): bigint | number => {
        try { const b = BigInt(v); return b <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : b; } catch { return Number(v); }
    };
    type BorshCtor<U> = { deserialize?: (buf: Uint8Array) => U } & (new (...args: any[]) => U);
    const ctorSchema = schema as unknown as BorshCtor<T>;
    const api = {
        init: async () => api as unknown as Index<T, N>,
        drop: () => sub.drop(),
        get: async (id: IdKey) => {
            const res = await sub.get({ id });
            if (!res) return undefined;
            const value = ctorSchema.deserialize ? ctorSchema.deserialize(res) : deserialize(res, ctorSchema as any);
            return { id, value } as any;
        },
        put: (value: T, id?: IdKey) => sub.put({ value: serialize(value), schema: ctorSchema as unknown as new (...a: any[]) => any, id }),
        del: (req: { query: Query[] }) => sub.del({ query: req.query }),
        sum: async (req: { key: string | string[]; query?: Query[] }) => parseSum(await sub.sum({ key: Array.isArray(req.key) ? req.key : [req.key], query: req.query })),
        count: (q?: { query?: Query[] }) => sub.count({ query: q?.query }),
        iterate: undefined as any,
        getSize: () => sub.getSize(),
        start: () => sub.start(),
        stop: () => sub.stop(),
    } as unknown as Index<T, N>;

    // attach iterate using subservice iterate* methods
    (api as unknown as { iterate: any }).iterate = <S extends Shape | undefined = undefined>(request?: IterateOptions): IndexIterator<T, S> => {
        let itHandle: string | undefined;
        let lastDone: boolean | undefined = undefined;
        const ensureOpen = async () => {
            if (!itHandle) {
                itHandle = await sub.iterateOpen({ query: request?.query, sort: request?.sort });
            }
            return itHandle;
        };
        return {
            next: async (amount: number) => {
                const h = await ensureOpen();
                const res = await sub.iterateNext({ iterator: h, amount });
                const ctor = schema as unknown as BorshCtor<T>;
                const out = res.map((r: any) => {
                    const value = ctor.deserialize ? ctor.deserialize(r.value) : deserialize(r.value, ctor as any);
                    return { id: r.id, value } as any;
                });
                try { lastDone = await sub.iterateDone({ iterator: h }); } catch { }
                return out;
            },
            all: async () => {
                const h = await ensureOpen();
                const res = await sub.iterateAll({ iterator: h });
                const ctor = schema as unknown as BorshCtor<T>;
                const out = res.map((r: any) => {
                    const value = ctor.deserialize ? ctor.deserialize(r.value) : deserialize(r.value, ctor as any);
                    return { id: r.id, value } as any;
                });
                lastDone = true;
                return out;
            },
            done: () => lastDone,
            pending: async () => {
                const h = await ensureOpen();
                const p = await sub.iteratePending({ iterator: h });
                try { lastDone = await sub.iterateDone({ iterator: h }); } catch { }
                return p;
            },
            close: async () => {
                if (itHandle) {
                    await sub.iterateClose({ iterator: itHandle });
                    itHandle = undefined;
                }
                lastDone = true;
            },
        };
    };

    return api;
}


