import { Peerbit } from "@dao-xyz/peerbit";
import type { Multiaddr } from "@multiformats/multiaddr";

const peer = await Peerbit.create();
const peer2 = await Peerbit.create();

// When testing locally you can do
await peer.dial(peer2);

// In "real" scenarios you only have the other peers address
// , you can do
const multaddrs: Multiaddr[] = peer2.libp2p.getMultiaddrs();

/* [
'/ip4/127.0.0.1/tcp/62991/ws/p2p/12D3KooWDebLLpnJJQvUo5LEkEURtLUGJwkiH5cAgS1mnSLfup7N',
'/ip4/127.0.0.1/tcp/62992/p2p/12D3KooWDebLLpnJJQvUo5LEkEURtLUGJwkiH5cAgS1mnSLfup7N'
] */
console.log(multaddrs.map((x) => x.toString()));

await peer.dial(multaddrs);

// or dial an address directly from a string. This one below is malformed and will fail
await expect(peer.dial("/ip4/123.4.5...")).rejects.toThrowError(
	"invalid ip address"
);

await peer.stop();
await peer2.stop();
