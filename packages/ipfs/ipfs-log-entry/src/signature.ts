import { field, variant } from "@dao-xyz/borsh";
import { U8IntArraySerializer } from "@dao-xyz/borsh-utils";

@variant(0)
export class Signature {

    @field(U8IntArraySerializer)
    signature: Uint8Array;

    constructor(props?: { signature: Uint8Array }) {
        if (props) {
            this.signature = props?.signature;
        }
    }
}