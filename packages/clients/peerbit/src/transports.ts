import {
	circuitRelayServer,
	circuitRelayTransport,
} from "@libp2p/circuit-relay-v2";
import { tcp } from "@libp2p/tcp";

/* import { webRTCDirect } from "@libp2p/webrtc"; */
import { webSockets } from "@libp2p/websockets";
import { all } from "@libp2p/websockets/filters";

export const transports = () => [
	webSockets({ filter: all }),
	circuitRelayTransport({
		reservationCompletionTimeout: 5000,
	}),
	/* webRTCDirect({}), */ // TODO: add back when webrtc-direct is supported in browser
	tcp(),
];
export const relay = () =>
	// applyDefaultLimit: false because of https://github.com/libp2p/js-libp2p/issues/2622
	circuitRelayServer({
		reservations: { applyDefaultLimit: false, maxReservations: 1000 },
	});

export const listen: () => string[] | undefined = () => [
	"/ip4/127.0.0.1/tcp/0",
	"/ip4/127.0.0.1/tcp/0/ws",
	/* "/ip4/127.0.0.1/udp/0/webrtc-direct", */ // TODO: add back when webrtc-direct is supported in browser
	"/p2p-circuit",
];
