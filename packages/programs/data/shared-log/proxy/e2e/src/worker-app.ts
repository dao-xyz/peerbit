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
	const host = new CanonicalHost(runtime, { idleTimeoutMs: 10_000 });
	installSharedLogModule(host);
	host.registerModule(debugModule);
	return host;
};
