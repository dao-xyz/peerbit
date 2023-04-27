import { circuitRelayTransport } from "libp2p/circuit-relay";
import { circuitRelayServer } from "libp2p/circuit-relay";
import { webRTC } from "@libp2p/webrtc";
import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";
export const transports = (browser: boolean) =>
	browser
		? [
				circuitRelayTransport({
					discoverRelays: 1,
				}),
				webRTC({}),
				webSockets({ filter: filters.all }),
		  ]
		: [webSockets({ filter: filters.all })];

export const relay = () => circuitRelayServer({});
