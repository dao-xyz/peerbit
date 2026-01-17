import { deserialize } from "@dao-xyz/borsh";
import { type RpcTransport, bindService } from "@dao-xyz/borsh-rpc";
import {
	type CanonicalChannel,
	type CanonicalContext,
	type CanonicalModule,
	createMessagePortTransport,
} from "@peerbit/canonical-host";
import { CounterProgram } from "./counter-program.js";
import { CounterService, OpenCounterRequest } from "./counter-protocol.js";

const toHex = (bytes: Uint8Array): string => {
	let out = "";
	for (const b of bytes) out += b.toString(16).padStart(2, "0");
	return out;
};

const openCounters: Map<string, { program: CounterProgram; refs: number }> =
	new Map();

export type CounterModuleStats = {
	total: number;
	entries: Array<{ key: string; refs: number }>;
};

export const getCounterModuleStats = (): CounterModuleStats => {
	return {
		total: openCounters.size,
		entries: [...openCounters.entries()].map(([key, value]) => ({
			key,
			refs: value.refs,
		})),
	};
};

const acquireCounter = async (properties: {
	ctx: CanonicalContext;
	id: Uint8Array;
}): Promise<{ program: CounterProgram; release: () => Promise<void> }> => {
	const key = toHex(properties.id);
	const existing = openCounters.get(key);
	if (existing) {
		existing.refs += 1;
		return {
			program: existing.program,
			release: async () => releaseCounter(key),
		};
	}

	const peer = await properties.ctx.peer();
	const program = await peer.open(new CounterProgram({ id: properties.id }), {
		existing: "reuse",
	});
	openCounters.set(key, { program, refs: 1 });
	return { program, release: async () => releaseCounter(key) };
};

const releaseCounter = async (key: string): Promise<void> => {
	const existing = openCounters.get(key);
	if (!existing) return;
	existing.refs -= 1;
	if (existing.refs > 0) return;
	openCounters.delete(key);
	await existing.program.close();
};

export const counterModule: CanonicalModule = {
	name: "@peerbit/counter",
	open: async (
		ctx: CanonicalContext,
		port: CanonicalChannel,
		payload: Uint8Array,
	) => {
		const request = deserialize(payload, OpenCounterRequest);
		const acquired = await acquireCounter({ ctx, id: request.id });

		let closed = false;
		let unbind: (() => void) | undefined;

		const transport: RpcTransport = createMessagePortTransport(port);
		const service = new CounterService({
			get: async () => acquired.program.get(),
			increment: async (amount) => acquired.program.increment(amount),
			close: async () => {
				if (closed) return;
				closed = true;
				unbind?.();
				await acquired.release();
			},
		});

		unbind = bindService(CounterService, transport, service);
		port.onClose?.(() => {
			void service.close();
		});
	},
};
