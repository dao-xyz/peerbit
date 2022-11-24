import { field, variant } from "@dao-xyz/borsh";
import { PlainKey } from "./key.js";

@variant(0)
export class IPFSAddress extends PlainKey {
    @field({ type: "string" })
    address: string;

    constructor(properties?: { address: string }) {
        super();
        if (properties) {
            this.address = properties.address;
        }
    }

    equals(other: any): boolean {
        if (other instanceof IPFSAddress) {
            return this.address === other.address;
        }
        return false;
    }
    toString(): string {
        return "ipfs/" + this.address;
    }
}
