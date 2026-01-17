import { connectSharedWorkerPeerbit } from "@peerbit/canonical-client";
import { Documents, type DocumentsLike } from "@peerbit/document";
import { documentAdapter } from "@peerbit/document-proxy/auto";
import { sharedLogAdapter } from "@peerbit/shared-log-proxy/auto";
import { MyDoc } from "./canonical-types.js";

const worker = new SharedWorker(
	new URL("./canonical-host-shared-worker.ts", import.meta.url),
	{ type: "module" },
);

const peer = await connectSharedWorkerPeerbit(worker, {
	adapters: [documentAdapter, sharedLogAdapter],
});

const docs: DocumentsLike<MyDoc, MyDoc> = await peer.open(
	new Documents<MyDoc>({ id: new Uint8Array(32) }),
	{
		existing: "reuse",
		args: { type: MyDoc },
	},
);

await docs.put(new MyDoc({ id: "a", text: "hello" }));
