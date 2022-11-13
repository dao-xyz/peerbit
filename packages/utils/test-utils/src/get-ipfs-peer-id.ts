import { IPFS } from "ipfs-core-types";

const getIpfsPeerId = async (ipfs: IPFS) => {
    const peerId = await ipfs.id();
    return peerId.id;
};

export default getIpfsPeerId;
