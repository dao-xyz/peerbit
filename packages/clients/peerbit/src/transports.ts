import { webSockets } from "@libp2p/websockets";
import { circuitRelayTransport } from "libp2p/circuit-relay";
import { tcp } from "@libp2p/tcp";
import { circuitRelayServer } from "libp2p/circuit-relay";
import { all } from "@libp2p/websockets/filters";
export const transports = () => [
	webSockets({ filter: all }),
	circuitRelayTransport(),
	tcp()
];
export const relay = () =>
	circuitRelayServer({ reservations: { maxReservations: 1000 } });

export const listen: () => string[] | undefined = () => [
	"/ip4/127.0.0.1/tcp/0",
	"/ip4/127.0.0.1/tcp/0/ws"
];
