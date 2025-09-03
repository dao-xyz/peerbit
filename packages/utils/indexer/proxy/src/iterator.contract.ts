import { method, service, struct } from "@dao-xyz/borsh-rpc";
import { vec, serialize, deserialize } from "@dao-xyz/borsh";
import type { AbstractType, Constructor } from "@dao-xyz/borsh";
import type { IdKey } from "@peerbit/indexer-interface";
import type { IndexIterator, Shape } from "@peerbit/indexer-interface";

@service()
export class IteratorContract {
    #it: IndexIterator<any, any>;
    constructor(it: IndexIterator<any, any>) {
        this.#it = it;
    }

    @method(struct({ amount: "u32" }), vec(struct({ id: Object as any, value: Uint8Array })))
    async next(args: { amount: number }): Promise<Array<{ id: IdKey; value: Uint8Array }>> {
        const res = await this.#it.next(args.amount);
        return res.map((r: any) => ({ id: r.id, value: serialize(r.value) }));
    }

    @method(struct({}), vec(struct({ id: Object as any, value: Uint8Array })))
    async all(): Promise<Array<{ id: IdKey; value: Uint8Array }>> {
        const res = await this.#it.all();
        return res.map((r: any) => ({ id: r.id, value: serialize(r.value) }));
    }

    @method(struct({}), "bool")
    async done(): Promise<boolean> { return this.#it.done() ?? false; }

    @method(struct({}), "u32")
    async pending(): Promise<number> { return await this.#it.pending(); }

    @method(struct({}), "void")
    async close(): Promise<void> { await this.#it.close(); }
}

export function wrapIteratorSubserviceProxy<T extends Record<string, any>, S extends Shape | undefined = undefined>(
    sub: any,
    schema: AbstractType<T>,
): IndexIterator<T, S> {
    const ctor = schema as Constructor<T>;
    let lastDone: boolean | undefined;
    const iterator: IndexIterator<T, S> = {
        next: async (amount: number) => {
            const res = await sub.next({ amount });
            const out = res.map((r: any) => ({ id: r.id, value: deserialize(r.value, ctor) }));
            try { lastDone = await sub.done(); } catch { }
            return out as any;
        },
        all: async () => {
            const res = await sub.all();
            const out = res.map((r: any) => ({ id: r.id, value: deserialize(r.value, ctor) }));
            lastDone = true;
            return out as any;
        },
        done: () => lastDone,
        pending: async () => {
            const p = await sub.pending();
            try { lastDone = await sub.done(); } catch { }
            return p;
        },
        close: async () => { await sub.close(); lastDone = true; },
    };
    return iterator;
}


