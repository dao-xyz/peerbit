import { webSockets } from "@libp2p/websockets";
import {
	CanonicalHost,
	PeerbitCanonicalRuntime,
} from "@peerbit/canonical-host";
import { installDocumentModule } from "@peerbit/document-proxy/host";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { counterModule } from "./counter-host.js";
import { debugModule } from "./debug-module.js";
import { TEST_DOC_TYPE, TestMessage } from "./types.js";

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
	installDocumentModule(host, { [TEST_DOC_TYPE]: TestMessage });
	host.registerModule(counterModule);
	host.registerModule(debugModule);
	return host;
};
