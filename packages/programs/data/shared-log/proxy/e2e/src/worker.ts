import type { CanonicalHost } from "@peerbit/canonical-host";
import { Buffer } from "buffer";

if (!globalThis.Buffer) {
	globalThis.Buffer = Buffer;
}

let hostPromise: Promise<CanonicalHost> | undefined;

const getHost = async (): Promise<CanonicalHost> => {
	if (!hostPromise) {
		hostPromise = import("./worker-app.js").then((mod) =>
			mod.createCanonicalHost(),
		);
	}
	return hostPromise;
};

const scope = self as unknown as SharedWorkerGlobalScope;
const onConnect = (event: MessageEvent) => {
	const port: MessagePort = (event as any).ports?.[0];
	if (!port) return;
	void getHost()
		.then((host) => host.attachControlPort(port))
		.catch((error) => {
			console.error("shared worker host init failed", error);
		});
};

scope.addEventListener("connect", onConnect as unknown as EventListener);
