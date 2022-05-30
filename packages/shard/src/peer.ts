import { field, variant, vec } from "@dao-xyz/borsh";
import { PublicKey } from "./key";

@variant(2)
export class Peer {

    @field({ type: PublicKey })
    key: PublicKey

    @field({ type: vec('String') })
    addresses: string[] // address

    constructor(obj?: {
        key: PublicKey,
        addresses: string[]
    }) {
        if (obj) {
            Object.assign(this, obj);
        }
    }
}
