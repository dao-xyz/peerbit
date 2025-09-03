/* eslint-env shared-worker */
import { PeerbitProxyHost, connection } from "@peerbit/proxy";
import { Peerbit } from "peerbit";

// Keep all connected ports to broadcast events
const ports = new Set<MessagePort>();

// Bridge MessagePort events into the MessageNode pattern
class WorkerHubNode extends connection.MessageNode {
	constructor() {
		super({
			addEventListener: (type, fn) => {
				const messageKey = `peerbit-proxy-${type}`;
				// Attach per-port listener when ports connect (below we forward to a global handler)
				self.addEventListener("message", ((
					ev: MessageEvent & { port?: MessagePort },
				) => {
					// In SharedWorker, the worker context doesn't receive normal 'message' from ports.
					// We'll manually re-dispatch port messages via dispatchPortMessage below.
				}) as any);

				// Custom event type: we dispatch from port listeners
				const onPortMessage = (ev: MessageEvent & { port: MessagePort }) => {
					if (ev.data?.type !== messageKey) return;
					const msg = ev.data.message;
					fn(msg, {
						id: msg.from,
						parent: false,
						publishMessage: (out) =>
							ev.port.postMessage(
								{ type: `peerbit-proxy-${out.type}`, message: out },
								out.type === "data" ? [out.data] : [],
							),
					});
				};

				// Store the handler globally so each port can forward to it
				(globalThis as any).__peerbit_proxy_onPortMessage = onPortMessage;
			},
			dispatchEvent: (msg) => {
				for (const port of ports) {
					port.postMessage(
						{ type: `peerbit-proxy-${msg.type}`, message: msg },
						(msg as any).type === "data" ? [(msg as any).data] : [],
					);
				}
			},
		});
	}
}

let host: PeerbitProxyHost | undefined;

(self as any).onconnect = async (e: MessageEvent) => {
	const port = (e as any).ports[0] as MessagePort;
	// Ensure host and handler exists before wiring port listeners
	if (!host) {
		const node = await Peerbit.create();
		host = new PeerbitProxyHost(node, new WorkerHubNode());
		await host.init();
	}

	ports.add(port);
	const forward = (ev: MessageEvent) => {
		const handler = (globalThis as any).__peerbit_proxy_onPortMessage as
			| ((ev: MessageEvent & { port: MessagePort }) => void)
			| undefined;
		if (handler) handler(Object.assign(ev, { port }));
	};
	port.addEventListener("message", forward);
	port.start();

	const cleanup = () => {
		ports.delete(port);
		port.removeEventListener("message", forward);
	};
	port.addEventListener("close", cleanup as any);
	// Ports do not fire 'close' in all browsers; best-effort only.
};
