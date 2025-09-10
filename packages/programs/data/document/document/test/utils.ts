import type { ProgramClient } from "@peerbit/program";
import type { DirectSub } from "@peerbit/pubsub";
import { delay } from "@peerbit/time";

export const slowDownSend = (
	from: ProgramClient,
	to: ProgramClient,
	ms = 3000,
) => {
	const directsub = from.services.pubsub as DirectSub;
	for (const [_key, peer] of directsub.peers) {
		if (peer.publicKey.equals(to.identity.publicKey)) {
			const writeFn = peer.write.bind(peer);
			peer.write = async (msg, priority) => {
				await delay(ms);
				if (peer.rawOutboundStreams?.length > 0) {
					return writeFn(msg, priority);
				}
			};
			return;
		}
	}
	throw new Error("Could not find peer");
};
