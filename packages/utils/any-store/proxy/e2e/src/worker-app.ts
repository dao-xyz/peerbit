import { webSockets } from "@libp2p/websockets";
import { createAnyStoreModule } from "@peerbit/any-store-proxy/host";
import {
	CanonicalHost,
	PeerbitCanonicalRuntime,
} from "@peerbit/canonical-host";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { MemoryStore } from "./memory-store.js";

const store = new MemoryStore();

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

	host.registerModule(
		createAnyStoreModule({
			createStore: async () => store,
		}),
	);

	return host;
};
