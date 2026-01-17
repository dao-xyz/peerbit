import { webSockets } from "@libp2p/websockets";
import { installServiceWorkerHost } from "@peerbit/canonical-host/service-worker";
import {
	documentModule,
	registerDocumentType,
} from "@peerbit/document-proxy/host";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { Buffer } from "buffer";
import { CanonicalPost } from "./src/canonical-types";

if (!globalThis.Buffer) {
	globalThis.Buffer = Buffer;
}

// Some browser-focused deps still reference `window` even in worker contexts.
// Provide a minimal alias so the canonical host can start inside a ServiceWorkerGlobalScope.
if (!(globalThis as any).window) {
	(globalThis as any).window = globalThis;
}

registerDocumentType(CanonicalPost);

installServiceWorkerHost({
	modules: [documentModule],
	peerOptions: {
		relay: false,
		indexer: createSimpleIndexer,
		libp2p: {
			transports: [webSockets()],
			addresses: { listen: [] },
		},
	},
	lifecycle: { skipWaiting: true, clientsClaim: true },
	onError: (error) => {
		console.error(
			"peerbit-react canonical service worker host init failed",
			error,
		);
	},
});
