import { field, variant } from "@dao-xyz/borsh";
import { IPFS } from "ipfs";
import { PeerId } from "@libp2p/interface-peer-id";
@variant(0)
export class Id {
    @field({ type: "string" })
    id: string;
    constructor(props?: { id: string }) {
        if (props) {
            this.id = props.id;
        }
    }
}

export const getPeerID = async (ipfs: IPFS): Promise<PeerId> => {
    return (await ipfs.id()).id;
};
