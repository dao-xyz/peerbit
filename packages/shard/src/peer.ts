import { field, variant, vec } from "@dao-xyz/borsh";
import BN from "bn.js";
import { PublicKey } from "./key";

@variant(2)
export class Peer {

    @field({ type: PublicKey })
    key: PublicKey

    @field({ type: vec('String') })
    addresses: string[] // address

    @field({ type: 'u64' })
    timestamp: BN

    constructor(obj?: {
        key: PublicKey,
        addresses: string[],
        timestamp: BN
    }) {
        if (obj) {
            Object.assign(this, obj);
        }
    }
}
