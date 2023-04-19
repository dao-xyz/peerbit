import { webSockets } from "@libp2p/websockets";
import { circuitRelayTransport } from "libp2p/circuit-relay";
import { tcp } from "@libp2p/tcp";
import { circuitRelayServer } from "libp2p/circuit-relay";
import { all } from "@libp2p/websockets/filters";
export const transports = () => [
	webSockets({
		filter: all,
	}),
	circuitRelayTransport({
		discoverRelays: 1,
	}),
	tcp(),
];

export const relay = () => circuitRelayServer({});
