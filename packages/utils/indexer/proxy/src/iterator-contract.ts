import { v4 as uuid } from "uuid";
import type { Index, IndexIterator, Shape } from "@peerbit/indexer-interface";
import { serialize } from "@dao-xyz/borsh";

// Shared iterator registry used by RPC handlers
export const iteratorsRegistry = new Map<string, IndexIterator<any, any>>();

export const iteratorOpen = <T extends Record<string, any>, S extends Shape | undefined = undefined>(
    index: Index<T>,
    _options?: Uint8Array, // reserved for future binary-encoded options
): string => {
    const it = index.iterate(undefined, undefined);
    const h = uuid();
    iteratorsRegistry.set(h, it as IndexIterator<T, S>);
    return h;
};

export const iteratorNext = async (
    iterator: string,
    amount: number,
): Promise<{ id: any; value: Uint8Array }[]> => {
    const it = iteratorsRegistry.get(iterator);
    if (!it) throw new Error("Unknown iterator handle");
    const results = await it.next(amount);
    return results.map((r: any) => ({ id: r.id, value: serialize(r.value) }));
};

export const iteratorAll = async (
    iterator: string,
): Promise<{ id: any; value: Uint8Array }[]> => {
    const it = iteratorsRegistry.get(iterator);
    if (!it) throw new Error("Unknown iterator handle");
    const results = await it.all();
    return results.map((r: any) => ({ id: r.id, value: serialize(r.value) }));
};

export const iteratorDone = (iterator: string): boolean => {
    const it = iteratorsRegistry.get(iterator);
    if (!it) throw new Error("Unknown iterator handle");
    return it.done() ?? false;
};

export const iteratorPending = async (iterator: string): Promise<number> => {
    const it = iteratorsRegistry.get(iterator);
    if (!it) throw new Error("Unknown iterator handle");
    return await it.pending();
};

export const iteratorClose = async (iterator: string): Promise<void> => {
    const it = iteratorsRegistry.get(iterator);
    if (!it) return;
    iteratorsRegistry.delete(iterator);
    await it.close();
};
