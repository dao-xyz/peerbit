import { installServiceWorkerHost } from "@peerbit/canonical-host/service-worker";
import {
	documentModule,
	registerDocumentType,
} from "@peerbit/document-proxy/host";
import { MyDoc } from "./canonical-types.js";

registerDocumentType(MyDoc);

installServiceWorkerHost({
	// `documentModule` already exposes `docs.log` through an embedded shared-log service.
	// Add `sharedLogModule` if you want to open `SharedLog` directly.
	modules: [documentModule],
	lifecycle: { skipWaiting: true, clientsClaim: true },
});
