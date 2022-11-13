import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";

export interface Network {
  inNetwork: true;
  get network(): TrustedNetwork;
}
export const inNetwork = (object: any): object is Network => {
  return object.inNetwork === true;
};
