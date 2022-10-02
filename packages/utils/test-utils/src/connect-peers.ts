const defaultFilter = () => true

const connectIpfsNodes = async (ipfs1, ipfs2, options: {
  filter: (address: string) => boolean
} = { filter: defaultFilter }) => {
  const id1 = await ipfs1.id()
  const id2 = await ipfs2.id()

  const addresses1 = id1.addresses.filter(options.filter)
  const addresses2 = id2.addresses.filter(options.filter)

  await ipfs1.swarm.connect(addresses2[0])
  await ipfs2.swarm.connect(addresses1[0])
}

export default connectIpfsNodes
