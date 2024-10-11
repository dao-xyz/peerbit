import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { circuitRelayServer } from "@libp2p/circuit-relay-v2";
import { identify } from "@libp2p/identify";
import { webSockets } from "@libp2p/websockets";
import { all } from "@libp2p/websockets/filters";
import { createLibp2p } from "libp2p";
import { TestDirectStream } from "./utils.js";

// Run with "node --loader ts-node/esm ./test/browser/shared/bin.ts"
const relay = await createLibp2p<{
	relay: any;
	identify: any;
	stream: TestDirectStream;
}>({
	addresses: {
		listen: ["/ip4/127.0.0.1/tcp/0/ws"],
	},
	services: {
		// applyDefaultLimit: false because of https://github.com/libp2p/js-libp2p/issues/2622
		relay: circuitRelayServer({
			reservations: { applyDefaultLimit: false, maxReservations: 1000 },
		}),
		identify: identify(),
		stream: (c) => new TestDirectStream(c),
	},
	transports: [webSockets({ filter: all })],
	streamMuxers: [yamux()],
	connectionEncrypters: [noise()],
});
console.log(relay.getMultiaddrs().map((x) => x.toString()));
