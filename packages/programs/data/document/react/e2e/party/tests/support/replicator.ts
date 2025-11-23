import { Peerbit } from "peerbit";
import { PartyDocumentStore } from "../../shared/dist/data.js";

export type RunningRelay = {
	peer: Peerbit;
	store?: PartyDocumentStore;
};

export async function startRelay(
	options?: { replicate?: boolean } | false,
): Promise<RunningRelay> {
	const peer = await Peerbit.create();

	const store =
		options === false
			? undefined
			: await peer.open(PartyDocumentStore.createFixed(), {
					existing: "reuse",
					args: { replicate: options?.replicate ?? true },
				});

	return {
		peer,
		store,
	};
}
