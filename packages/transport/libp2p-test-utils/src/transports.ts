import { circuitRelayTransport } from "libp2p/circuit-relay";
import { circuitRelayServer } from "libp2p/circuit-relay";
import { webRTC } from "@libp2p/webrtc";
import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";
import { tcp } from "@libp2p/tcp";
import { Components } from "libp2p/components";
import type { Transport } from "@libp2p/interface/transport";

export const transports = (
	browser: boolean
): Array<(components: Components) => Transport> =>
	browser
		? [
				circuitRelayTransport({
					discoverRelays: 1,
				}),
				webRTC({}),
				webSockets({ filter: filters.all }),
		  ]
		: [tcp()];

export const relay = () => circuitRelayServer({});
