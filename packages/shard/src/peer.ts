import { field, variant, vec } from "@dao-xyz/borsh";
import { ResultSource } from "@dao-xyz/bquery";
import BN from "bn.js";
import { PublicKey } from "./key";

@variant([0, 3])
export class Peer extends ResultSource {

    @field({ type: PublicKey })
    key: PublicKey

    @field({ type: vec('String') })
    addresses: string[] // address

    @field({ type: 'u64' })
    timestamp: BN

    @field({ type: 'u64' })
    memoryBudget: BN // how much memory left to space

    constructor(obj?: {
        key: PublicKey,
        addresses: string[],
        timestamp: BN,
        memoryBudget: BN
    }) {
        super();
        if (obj) {
            Object.assign(this, obj);
        }
    }
}
