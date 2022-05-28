import { field, variant, vec } from "@dao-xyz/borsh";

@variant(2)
export class Peer {

    @field({ type: vec('String') })
    addresses: string[] // address

    constructor(obj?: Peer) {
        if (obj) {
            Object.assign(this, obj);
        }
    }
}
