import { installServiceWorkerHost } from "@peerbit/canonical-host/service-worker";
import { Buffer } from "buffer";
import { createCanonicalHost } from "./src/worker-app.js";

if (!globalThis.Buffer) {
	globalThis.Buffer = Buffer;
}

installServiceWorkerHost({
	createHost: createCanonicalHost,
	lifecycle: { skipWaiting: true, clientsClaim: true },
	onError: (error) => {
		console.error("service worker host init failed", error);
	},
});
