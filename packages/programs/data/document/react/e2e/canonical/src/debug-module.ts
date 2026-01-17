import type {
	CanonicalChannel,
	CanonicalModule,
} from "@peerbit/canonical-host";
import { getDocumentModuleStats } from "@peerbit/document-proxy/host";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

type DebugRequest = { id: number; op: "stats" };
type DebugResponse =
	| { id: number; ok: true; stats: unknown }
	| { id: number; ok: false; error: string };

const encode = (value: DebugResponse) => encoder.encode(JSON.stringify(value));

const decode = (payload: Uint8Array): DebugRequest => {
	const text = decoder.decode(payload);
	return JSON.parse(text) as DebugRequest;
};

const buildStats = () => {
	return {
		documents: getDocumentModuleStats(),
	};
};

export const debugModule: CanonicalModule = {
	name: "@peerbit/debug",
	open: async (_ctx, channel: CanonicalChannel) => {
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

			const resp: DebugResponse = {
				id: request?.id ?? 0,
				ok: false,
				error: `Unknown op '${String((request as any)?.op)}'`,
			};
			channel.send(encode(resp));
		});
	},
};
