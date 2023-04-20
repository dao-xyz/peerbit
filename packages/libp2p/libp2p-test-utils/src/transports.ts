import { circuitRelayTransport } from "libp2p/circuit-relay";
import { tcp } from "@libp2p/tcp";
import { circuitRelayServer } from "libp2p/circuit-relay";
export const transports = () => [
	circuitRelayTransport({
		discoverRelays: 1,
	}),
	tcp(),
];

export const relay = () => circuitRelayServer({});
