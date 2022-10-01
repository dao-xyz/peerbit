
import { IPFS } from "ipfs-core-types"
import type { PeerId } from '@libp2p/interface-peer-id';

export const getPeerID = async (ipfs: IPFS): Promise<PeerId> => {
  return (await ipfs.id()).id;
}



