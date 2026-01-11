import { webSockets } from "@libp2p/websockets";
import {
	CanonicalHost,
	PeerbitCanonicalRuntime,
} from "@peerbit/canonical-host";
import {
	installDocumentModule,
	registerDocumentType,
} from "@peerbit/document-proxy/host";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { debugModule } from "./debug-module.js";
import { TestMessage } from "./types.js";

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
	registerDocumentType(TestMessage);
	installDocumentModule(host);
	host.registerModule(debugModule);
	return host;
};
