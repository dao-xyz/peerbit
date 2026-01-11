import { installWindowHost } from "@peerbit/canonical-host/window";
import {
	documentModule,
	registerDocumentType,
} from "@peerbit/document-proxy/host";
import { MyDoc } from "./canonical-types.js";

registerDocumentType(MyDoc);

installWindowHost({
	// `documentModule` already exposes `docs.log` through an embedded shared-log service.
	// Add `sharedLogModule` if you want to open `SharedLog` directly.
	modules: [documentModule],
	channel: "my-canonical-channel",
	targetOrigin: "*",
});
