import { field, variant } from "@dao-xyz/borsh";
import { U8IntArraySerializer } from "@dao-xyz/borsh-utils";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";

@variant(0)
export class Signature {

    @field({ type: PublicSignKey })
    publicKey: PublicSignKey;

    @field(U8IntArraySerializer)
    signature: Uint8Array;

    constructor(props?: { publicKey: PublicSignKey, signature: Uint8Array }) {
        if (props) {
            this.publicKey = props.publicKey;
            this.signature = props?.signature;
        }
    }
}