import { serialize } from "@dao-xyz/borsh";
import { createProxyFromService } from "@dao-xyz/borsh-rpc";
import {
	type CanonicalClient,
	createMessagePortTransport,
} from "@peerbit/canonical-client";
import { CounterService, OpenCounterRequest } from "./counter-protocol.js";

export type CounterProxy = {
	raw: CounterService;
	get: () => Promise<bigint>;
	increment: (amount?: bigint | number) => Promise<bigint>;
	close: () => Promise<void>;
};

export const openCounter = async (properties: {
	client: CanonicalClient;
	id: Uint8Array;
}): Promise<CounterProxy> => {
	const channel = await properties.client.openPort(
		"@peerbit/counter",
		serialize(new OpenCounterRequest({ id: properties.id })),
	);

	const transport = createMessagePortTransport(channel, {
		requestTimeoutMs: 30_000,
	});
	const raw = createProxyFromService(
		CounterService,
		transport,
	) as unknown as CounterService;

	return {
		raw,
		get: async () => raw.get(),
		increment: async (amount = 1n) => raw.increment(BigInt(amount)),
		close: async () => {
			try {
				await raw.close();
			} finally {
				channel.close?.();
			}
		},
	};
};
