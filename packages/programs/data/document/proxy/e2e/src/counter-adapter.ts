import { getSchema } from "@dao-xyz/borsh";
import type { CanonicalOpenAdapter } from "@peerbit/canonical-client";
import { openCounter } from "./counter-client.js";
import { CounterProgram } from "./counter-program.js";

const toHex = (bytes: Uint8Array): string => {
	let out = "";
	for (const b of bytes) out += b.toString(16).padStart(2, "0");
	return out;
};

export const counterAdapter: CanonicalOpenAdapter<CounterProgram> = {
	name: "@peerbit/counter",
	canOpen: (program): program is CounterProgram => {
		if (!program || typeof program !== "object") return false;
		try {
			return getSchema(program.constructor)?.variant === "counter_program";
		} catch {
			return false;
		}
	},
	getKey: (program) => toHex(program.id),
	open: async ({ program, client }) => {
		const address = (await program.calculateAddress()).address;
		const proxy = await openCounter({ client, id: program.id });
		return { proxy, address };
	},
};
