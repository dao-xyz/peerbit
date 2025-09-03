import {
	PeerbitProxyClient,
	PeerbitProxyHost,
	connection,
} from "@peerbit/proxy";
import { Peerbit } from "peerbit";

type WindowFunctions = {
	addEventListener: <K extends keyof WindowEventMap>(
		type: K,
		listener: (this: Window, ev: WindowEventMap[K]) => any,
		options?: boolean | AddEventListenerOptions,
	) => void;
	postMessage: (
		message: any,
		targetOrigin: string,
		transfer?: Transferable[],
	) => void;
	top: WindowFunctions | null;
};

export class PostMessageNode extends connection.MessageNode {
	static messagePrefixKey = "peerbit-proxy-";
	constructor(
		readonly targetOrigin = "*",
		readonly windowHandle: WindowFunctions = globalThis,
	) {
		super({
			addEventListener: (k, fn) => {
				const messageKey = PostMessageNode.messagePrefixKey + k;
				windowHandle.addEventListener("message", (ev) => {
					if (ev.data.type === messageKey) {
						fn(
							ev.data.message,
							ev.source
								? {
										id: (
											ev.data.message as
												| connection.Hello
												| connection.DataMessage
										).from,
										publishMessage: (msg) =>
											ev.source?.postMessage(
												{
													type: PostMessageNode.messagePrefixKey + msg.type,
													message: msg,
												},
												{ targetOrigin: "*" },
											),
										parent: ev.source === window.top,
									}
								: undefined,
						);
					}
				});
			},
			dispatchEvent: (msg) => {
				const messageKey = PostMessageNode.messagePrefixKey + msg.type;
				(windowHandle.top || windowHandle).postMessage(
					{
						type: messageKey,
						message: msg,
					},
					this.targetOrigin,
					msg.type === "data" ? [msg.data] : undefined,
				); // TODO transferable
			},
		});
	}
}

export const createClient = async (targetOrigin = "*") => {
	const client = new PeerbitProxyClient(new PostMessageNode(targetOrigin));
	await client.connect();
	return client;
};
export const createHost = async (node: Peerbit, targetOrigin = "*") => {
	const client = new PeerbitProxyHost(node, new PostMessageNode(targetOrigin));
	client.messages.start();
	await client.init();
	return client;
};

// SharedWorker-based multi-tab APIs
export { createSharedWorkerClient } from "./sharedworker/client.js";
export const sharedWorkerHostUrl = new URL(
	"./sharedworker/host.js",
	import.meta.url,
);
