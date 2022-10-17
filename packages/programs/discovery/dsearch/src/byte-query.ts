import { field, variant, vec } from "@dao-xyz/borsh";
import { U8IntArraySerializer } from "@dao-xyz/borsh-utils";
import { Query } from "./query-interface.js";

@variant(0)
export class MemoryCompare {

    @field(U8IntArraySerializer)
    bytes: Uint8Array

    @field({ type: 'u64' })
    offset: bigint

    constructor(opts?: {
        bytes: Uint8Array,
        offset: bigint
    }) {
        if (opts) {
            this.bytes = opts.bytes;
            this.offset = opts.offset;
        }
    }
}

@variant(3)
export class MemoryCompareQuery extends Query {

    @field({ type: vec(MemoryCompare) })
    compares: MemoryCompare[]

    constructor(opts?: {
        compares: MemoryCompare[]
    }) {
        super();
        if (opts) {
            this.compares = opts.compares;
        }
    }
}