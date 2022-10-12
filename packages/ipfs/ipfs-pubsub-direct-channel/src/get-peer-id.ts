
import { IPFS } from "ipfs-core-types"
export const getPeerID = async (ipfs: IPFS): Promise<string> => {
  return (await ipfs.id()).id.toString();
}



