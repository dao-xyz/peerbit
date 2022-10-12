import { field, variant, vec } from "@dao-xyz/borsh";
import { PlainKey, PUBLIC_KEY_WIDTH, SignKey } from './key.js';

@variant(3)
export class IPFSAddress extends PlainKey {

    @field({ type: 'string' })
    address: string;

    @field({ type: vec('u8') })
    padding: number[];  // we do padding because we want all publicsignkeys to have same size (64 bytes) excluding descriminators. This allows us to efficiently index keys and use byte search to find them we predetermined offsets

    constructor(properties?: { address: string }) {
        super();
        if (properties) {
            this.address = properties.address;
            this.padding = new Array(PUBLIC_KEY_WIDTH - 15 - this.address.length).fill(0) // -15 comes from all the bytes that are parts of discriinators, field lengths etc. to ensure a size of IPFSAddress  of PUBLIC_KEY_WIDTH
        }
    }

    equals(other: SignKey): boolean {
        if (other instanceof IPFSAddress) {
            return this.address === other.address;
        }
        return false;
    }
    toString(): string {
        return "ipfs/" + this.address
    }

}