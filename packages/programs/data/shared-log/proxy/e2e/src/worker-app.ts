import { webSockets } from "@libp2p/websockets";
import {
	CanonicalHost,
	PeerbitCanonicalRuntime,
} from "@peerbit/canonical-host";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { installSharedLogModule } from "@peerbit/shared-log-proxy/host";
import { debugModule } from "./debug-module.js";

export const createCanonicalHost = () => {
	const runtime = new PeerbitCanonicalRuntime({
		peerOptions: {
			relay: false,
			indexer: createSimpleIndexer,
			libp2p: {
				transports: [webSockets()],
				addresses: { listen: [] },
			},
		},
	});
	// Keep this relatively low so abrupt-close tests release refs quickly and reliably.
	// Clients run their own keep-alive pings while tabs are open, so active sessions
	// should not be collected.
	const host = new CanonicalHost(runtime, {
		idleTimeoutMs: 5_000,
		idleCheckIntervalMs: 1_000,
	});
	installSharedLogModule(host);
	host.registerModule(debugModule);
	return host;
};
