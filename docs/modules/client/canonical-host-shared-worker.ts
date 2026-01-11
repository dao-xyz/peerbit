import { installSharedWorkerHost } from "@peerbit/canonical-host/shared-worker";
import {
	documentModule,
	registerDocumentType,
} from "@peerbit/document-proxy/host";
import { MyDoc } from "./canonical-types.js";

registerDocumentType(MyDoc);

installSharedWorkerHost({
	directory: "peerbit",
	// `documentModule` already exposes `docs.log` through an embedded shared-log service.
	// Add `sharedLogModule` if you want to open `SharedLog` directly.
	modules: [documentModule],
});
