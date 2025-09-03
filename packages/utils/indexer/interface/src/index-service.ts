import type { CountOptions, DeleteOptions, Index, IndexedResult, IndexEngineInitProperties, IndexIterator, IterateOptions, Shape, SumOptions } from "./index-engine";
import type { IdKey } from "./id";
import { ctor, fn, method, service, struct } from "@dao-xyz/borsh-rpc";
import { vec } from "@dao-xyz/borsh";

@service()
export class IndicesRPC {
    @method(struct({
        indexBy?: vec('string')
   /*      nested?: NestedProperties<N>; */
        schema: ctor
    }))
    init<T extends Record<string, any>, NestedType>(
        properties: IndexEngineInitProperties<T, NestedType>,
    ): MaybePromise<Index<T, NestedType>>;
    scope(name: string): MaybePromise<Indices>;
    start(): MaybePromise<void>;
    stop(): MaybePromise<void>;
    drop(): MaybePromise<void>;
}


