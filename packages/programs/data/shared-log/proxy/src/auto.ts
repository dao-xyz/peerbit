import { getSchema } from "@dao-xyz/borsh";
import type { CanonicalOpenAdapter } from "@peerbit/canonical-client";
import { SharedLog } from "@peerbit/shared-log";
import { createSharedLogCacheKey, openSharedLog } from "./client.js";

const isSharedLogProgram = (program: any): program is SharedLog<any> => {
	if (!program || typeof program !== "object") return false;
	try {
		return getSchema(program.constructor)?.variant === "shared_log";
	} catch {
		return false;
	}
};

type SharedLogProxy = Awaited<ReturnType<typeof openSharedLog>>;

export const createSharedLogAdapter = (): CanonicalOpenAdapter<
	SharedLog<any>,
	SharedLogProxy
> => {
	return {
		name: "@peerbit/shared-log",
		canOpen: isSharedLogProgram,
		getKey: (program) => {
			const id = program.log?.id;
			return createSharedLogCacheKey(id);
		},
		open: async ({ program, client }) => {
			const id = program.log?.id;
			if (!id) {
				throw new Error("Canonical SharedLog open requires a log id");
			}
			const address = (await program.calculateAddress()).address;
			const proxy = await openSharedLog({ client, id });
			return { proxy, address };
		},
	};
};

export const sharedLogAdapter = createSharedLogAdapter();
