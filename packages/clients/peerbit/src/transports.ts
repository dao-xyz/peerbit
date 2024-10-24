import {
	circuitRelayServer,
	circuitRelayTransport,
} from "@libp2p/circuit-relay-v2";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { all } from "@libp2p/websockets/filters";

export const transports = () => [
	webSockets({ filter: all }),
	circuitRelayTransport({
		reservationCompletionTimeout: 5000,
	}),
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
	"/p2p-circuit",
];
