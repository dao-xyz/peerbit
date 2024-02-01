import {
	DeliveryMode,
	PriorityOptions,
	SilentDelivery
} from "@peerbit/stream-interface";
import { RPC } from "./controller";
import {
	EncryptionOptions,
	RPCRequestResponseOptions,
	RPCResponse
} from "./io";
import { Constructor } from "@dao-xyz/borsh";
export class MissingResponsesError extends Error {
	constructor(message: string) {
		super(message);
	}
}
export type RPCRequestAllOptions<R> = RPCRequestResponseOptions<R> &
	EncryptionOptions & { mode?: Constructor<DeliveryMode> } & PriorityOptions;

export const queryAll = <Q, R>(
	rpc: RPC<Q, R>,
	groups: string[][],
	request: Q,
	responseHandler: (response: RPCResponse<R>[]) => Promise<void> | void,
	options?: RPCRequestAllOptions<R> | undefined
) => {
	// In each shard/group only query a subset
	groups = [...groups].filter(
		(x) => !x.find((y) => y === rpc.node.identity.publicKey.hashcode())
	);

	const sendModeType = options?.mode || SilentDelivery;
	let rng = Math.round(Math.random() * groups.length);
	const startRng = rng;
	const fn = async () => {
		let missingReponses = false;
		while (groups.length > 0) {
			const peersToQuery: string[] = new Array(groups.length);
			let counter = 0;
			const peerToGroupIndex = new Map<string, number>();
			for (let i = 0; i < groups.length; i++) {
				const group = groups[i];
				peersToQuery[counter] = group[rng % group.length];
				peerToGroupIndex.set(peersToQuery[counter], i);
				counter++;
			}
			if (peersToQuery.length > 0) {
				const results = await rpc.request(request, {
					...options,
					mode: new sendModeType({ to: peersToQuery, redundancy: 1 }) // TODO configuration redundancy?
				});

				for (const result of results) {
					if (!result.from) {
						throw new Error("Unexpected, missing from");
					}
					peerToGroupIndex.delete(result.from.hashcode());
				}

				await responseHandler(results);

				const indicesLeft = new Set([...peerToGroupIndex.values()]);

				rng += 1;
				groups = groups.filter((v, ix) => {
					if (indicesLeft.has(ix)) {
						const peerIndex = rng % v.length;
						if (rng === startRng || peerIndex === startRng % v.length) {
							// TODO Last condition needed?
							missingReponses = true;
							return false;
						}
						return true;
					}
					return false;
				});
			}
		}
		if (missingReponses) {
			throw new MissingResponsesError(
				"Did not receive responses from all shards"
			);
		}
	};
	return fn();
};
