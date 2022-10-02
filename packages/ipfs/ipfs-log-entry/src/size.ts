import { variant, field } from "@dao-xyz/borsh";

@variant(0)
export class Size {

    @field({ type: 'u64' })
    size: bigint;

    constructor(properties?: { size: bigint }) {
        if (properties) {
            this.size = properties.size;
        }
    }
}