import { field, variant } from "@dao-xyz/borsh";

@variant(0)
export class Id {
    @field({ type: 'string' })
    id: string;
    constructor(props?: { id: string }) {
        this.id = props?.id;
    }
}


export const getPeerID = async (ipfs: { id(): Promise<{ id: string }> }) => {
    /*   const peerInfo = await ipfs.id()
      return peerInfo.id */
    let id: string = undefined;
    const idFromIpfs: string | { toString: () => string } = (await ipfs.id()).id;
    if (typeof idFromIpfs !== 'string') {
        id = idFromIpfs.toString(); //  ipfs 57+ seems to return an id object rather than id
    }
    else {
        id = idFromIpfs
    }
    return id;

}
