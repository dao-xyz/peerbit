// Set utils
import type { PeerId } from '@libp2p/interface-peer-id';

export const difference = (set1: PeerId[], set2: PeerId[]): PeerId[] => {

  // assume size of set1 and set2 are small
  return set1.filter(p => !set2.find(p2 => p2.equals(p)))
}