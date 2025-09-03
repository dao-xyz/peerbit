import { PeerbitProxyClient, connection } from "@peerbit/proxy";

// A MessageNode backed by a SharedWorker MessagePort.
class SharedWorkerNode extends connection.MessageNode {
	private port: MessagePort;
	constructor(port: MessagePort) {
		super({
			addEventListener: (type, fn) => {
				const messageKey = `peerbit-proxy-${type}`;
				const onMessage = (ev: MessageEvent) => {
					if (ev.data?.type !== messageKey) return;
					const msg = ev.data.message;
					fn(msg, {
						id: msg.from,
						parent: true, // the worker is considered the parent
						publishMessage: (out) =>
							this.port.postMessage(
								{ type: `peerbit-proxy-${out.type}`, message: out },
								out.type === "data" ? [out.data] : [],
							),
					});
				};
				this.port.addEventListener("message", onMessage as any);
				// Ensure the port is active
				this.port.start();
			},
			dispatchEvent: (msg) => {
				this.port.postMessage(
					{ type: `peerbit-proxy-${msg.type}`, message: msg },
					msg.type === "data" ? [msg.data] : [],
				);
			},
		});
		this.port = port;
	}
}

/**
 * Create a Peerbit client connected to a SharedWorker host.
 * If no URL is provided, it uses the library's default worker entry.
 */
export const createSharedWorkerClient = async (workerUrl?: URL) => {
	// Resolve default worker entry relative to this module so bundlers include it.
	const resolvedUrl = workerUrl ?? new URL("./host.js", import.meta.url);
	const worker = new SharedWorker(resolvedUrl, {
		type: "module",
		name: "@peerbit/proxy",
	});
	const node = new SharedWorkerNode(worker.port);
	const client = new PeerbitProxyClient(node);
	await client.connect();
	return client;
};
