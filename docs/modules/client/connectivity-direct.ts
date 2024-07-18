import type { Multiaddr } from "@multiformats/multiaddr";
import { expect } from "chai";
import { Peerbit } from "peerbit";

const peerA = await Peerbit.create();
const peerB = await Peerbit.create();

// When testing locally you can do
await peerA.dial(peerB);

// In "real" scenarios you only have the other peers address
// you can do
const multaddrs: Multiaddr[] = peerB.libp2p.getMultiaddrs();

/* [
'/ip4/127.0.0.1/tcp/62991/ws/p2p/12D3KooWDebLLpnJJQvUo5LEkEURtLUGJwkiH5cAgS1mnSLfup7N',
'/ip4/127.0.0.1/tcp/62992/p2p/12D3KooWDebLLpnJJQvUo5LEkEURtLUGJwkiH5cAgS1mnSLfup7N'
] */
console.log(multaddrs.map((x) => x.toString()));

await peerA.dial(multaddrs);

// Connected!

// We can dial an address directly from a string. This one below is malformed and will fail
await expect(peerA.dial("/ip4/123.4.5...")).rejectedWith("invalid ip address");

await peerA.stop();
await peerB.stop();
