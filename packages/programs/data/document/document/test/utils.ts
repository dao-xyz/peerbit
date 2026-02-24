import type { ProgramClient } from "@peerbit/program";
import type { TopicControlPlane } from "@peerbit/pubsub";
import { delay } from "@peerbit/time";

export const slowDownSend = (
	from: ProgramClient,
	to: ProgramClient,
	ms = 3000,
) => {
	const pubsub = from.services.pubsub as TopicControlPlane;
	for (const [_key, peer] of pubsub.peers) {
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
