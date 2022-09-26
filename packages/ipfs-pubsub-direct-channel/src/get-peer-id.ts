
export const getPeerID = async (ipfs: { id(): Promise<{ id: string }> }) => {
  const peerInfo = await ipfs.id()
  return peerInfo.id
}


