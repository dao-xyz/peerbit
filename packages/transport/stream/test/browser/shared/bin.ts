import { test, expect } from "@playwright/test";
import { createLibp2p } from "libp2p";
import { webSockets } from "@libp2p/websockets";
import { circuitRelayServer } from "@libp2p/circuit-relay-v2";
import { noise } from "@dao-xyz/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { identify } from "@libp2p/identify";
import { all } from "@libp2p/websockets/filters";
import { TestDirectStream } from "./utils.js";

// Run with "node --loader ts-node/esm ./test/browser/shared/bin.ts"
const relay = await createLibp2p<{
	relay: any;
	identify: any;
	stream: TestDirectStream;
}>({
	addresses: {
		listen: ["/ip4/127.0.0.1/tcp/0/ws"]
	},
	services: {
		relay: circuitRelayServer({ reservations: { maxReservations: 1000 } }),
		identify: identify(),
		stream: (c) => new TestDirectStream(c)
	},
	transports: [webSockets({ filter: all })],
	streamMuxers: [yamux()],
	connectionEncryption: [noise()]
});
console.log(relay.getMultiaddrs().map((x) => x.toString()));
