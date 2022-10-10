
import { IPFS } from 'ipfs-core-types';
const defaultFilter = (addr: { toString(): string }) => addr.toString().includes('127.0.0.1')

const connectIpfsNodes = async (ipfs1: IPFS, ipfs2: IPFS, options: {
  filter: (address: string) => boolean
} = { filter: defaultFilter }) => {
  const id1 = await ipfs1.id()
  const id2 = await ipfs2.id()

  const addresses1 = id1.addresses.filter((address => options.filter(address.toString())));
  const addresses2 = id2.addresses.filter((address => options.filter(address.toString())));

  await ipfs1.swarm.connect(addresses2[0])
  await ipfs2.swarm.connect(addresses1[0])
}

export default connectIpfsNodes
