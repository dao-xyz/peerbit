import type {
	CanonicalChannel,
	CanonicalModule,
} from "@peerbit/canonical-host";
import { SharedLog } from "@peerbit/shared-log";
import { getSharedLogModuleStats } from "@peerbit/shared-log-proxy/host";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const ensureCustomEvent = () => {
	if (typeof (globalThis as any).CustomEvent === "function") {
		return;
	}

	class CustomEventPolyfill<T = any> extends Event {
		detail: T;
		constructor(type: string, params?: CustomEventInit<T>) {
			super(type, params);
			this.detail = params?.detail as T;
		}
	}

	(globalThis as any).CustomEvent = CustomEventPolyfill;
};

type DebugRequest =
	| { id: number; op: "stats" }
	| { id: number; op: "append"; logId: string; text: string }
	| { id: number; op: "saveProgram"; logId: string }
	| { id: number; op: "emitEvent"; logId: string; type: string };

type DebugResponse =
	| { id: number; ok: true; stats?: unknown; address?: string }
	| { id: number; ok: false; error: string };

const encode = (value: DebugResponse) => encoder.encode(JSON.stringify(value));

const decode = (payload: Uint8Array): DebugRequest => {
	const text = decoder.decode(payload);
	return JSON.parse(text) as DebugRequest;
};

const fromHex = (hex: string): Uint8Array => {
	const cleaned = hex.startsWith("0x") ? hex.slice(2) : hex;
	if (cleaned.length % 2 !== 0) throw new Error("Invalid hex");
	const out = new Uint8Array(cleaned.length / 2);
	for (let i = 0; i < out.length; i++) {
		const start = i * 2;
		out[i] = Number.parseInt(cleaned.slice(start, start + 2), 16);
	}
	return out;
};

const buildStats = () => {
	return {
		sharedLogs: getSharedLogModuleStats(),
	};
};

export const debugModule: CanonicalModule = {
	name: "@peerbit/debug",
	open: async (ctx, channel: CanonicalChannel) => {
		ensureCustomEvent();

		channel.onMessage((payload) => {
			let request: DebugRequest | undefined;
			try {
				request = decode(payload);
			} catch (error) {
				const resp: DebugResponse = {
					id: 0,
					ok: false,
					error: error instanceof Error ? error.message : String(error),
				};
				channel.send(encode(resp));
				return;
			}

			if (request?.op === "stats") {
				const resp: DebugResponse = {
					id: request.id,
					ok: true,
					stats: buildStats(),
				};
				channel.send(encode(resp));
				return;
			}

			if (request?.op === "append") {
				void ctx
					.peer()
					.then(async (peer) => {
						const id = fromHex(request!.logId);
						const log = await peer.open(new SharedLog<Uint8Array>({ id }), {
							existing: "reuse",
							args: { replicate: { factor: 1 } } as any,
						});
						await log.append(encoder.encode(request!.text));
						const resp: DebugResponse = { id: request!.id, ok: true };
						channel.send(encode(resp));
					})
					.catch((error) => {
						const resp: DebugResponse = {
							id: request!.id,
							ok: false,
							error: error instanceof Error ? error.message : String(error),
						};
						channel.send(encode(resp));
					});
				return;
			}

			if (request?.op === "saveProgram") {
				void ctx
					.peer()
					.then(async (peer) => {
						const id = fromHex(request!.logId);
						const log = await peer.open(new SharedLog<Uint8Array>({ id }), {
							existing: "reuse",
							args: { replicate: { factor: 1 } } as any,
						});
						const address = await log.save(peer.services.blocks);
						const resp: DebugResponse = {
							id: request!.id,
							ok: true,
							address,
						};
						channel.send(encode(resp));
					})
					.catch((error) => {
						const resp: DebugResponse = {
							id: request!.id,
							ok: false,
							error: error instanceof Error ? error.message : String(error),
						};
						channel.send(encode(resp));
					});
				return;
			}

			if (request?.op === "emitEvent") {
				void ctx
					.peer()
					.then(async (peer) => {
						const id = fromHex(request!.logId);
						const log = await peer.open(new SharedLog<Uint8Array>({ id }), {
							existing: "reuse",
							args: { replicate: { factor: 1 } } as any,
						});

						log.events.dispatchEvent(
							new CustomEvent(request!.type, {
								detail: { publicKey: log.node.identity.publicKey },
							}),
						);

						const resp: DebugResponse = { id: request!.id, ok: true };
						channel.send(encode(resp));
					})
					.catch((error) => {
						const resp: DebugResponse = {
							id: request!.id,
							ok: false,
							error: error instanceof Error ? error.message : String(error),
						};
						channel.send(encode(resp));
					});
				return;
			}

			const resp: DebugResponse = {
				id: request?.id ?? 0,
				ok: false,
				error: `Unknown op '${String((request as any)?.op)}'`,
			};
			channel.send(encode(resp));
		});
	},
};
