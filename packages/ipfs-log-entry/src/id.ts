import { field, variant } from "@dao-xyz/borsh";

@variant(0)
export class Id {
    @field({ type: 'String' })
    id: string;
}