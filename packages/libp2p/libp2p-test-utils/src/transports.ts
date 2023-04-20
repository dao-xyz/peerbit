import { circuitRelayTransport } from "libp2p/circuit-relay";
import { tcp } from "@libp2p/tcp";
import { circuitRelayServer } from "libp2p/circuit-relay";
/* import { webSockets } from "@libp2p/websockets";
import * as filters from '@libp2p/websockets/filters' */
export const transports = () => [
	circuitRelayTransport({
		discoverRelays: 1,
	}),
	tcp(),
	/* webSockets({ filter: filters.all }) */
];

export const relay = () => circuitRelayServer({});
