import type { PeerId, Stream } from "@libp2p/interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import { PeerStreams } from "../src/index.js";

const createInboundStream = (id: string, close: () => Promise<void>): Stream =>
	({
		id,
		protocol: "/test",
		[Symbol.asyncIterator]: async function* () {},
		abort: () => {},
		close,
	}) as unknown as Stream;

describe("peer stream cleanup", () => {
	it("handles a rejected close while pruning an inactive inbound stream", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "remote-peer" } as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-connection",
		});
		const closeError = new AggregateError(
			[new Error("FIN_ACK timed out")],
			"All promises were rejected",
		);
		const unhandled: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => {
			unhandled.push(reason);
		};
		let staleCloseCalls = 0;

		process.on("unhandledRejection", onUnhandledRejection);
		try {
			const survivor = streams.attachInboundStream(
				createInboundStream("survivor", async () => {}),
			);
			const stale = streams.attachInboundStream(
				createInboundStream("stale", () => {
					staleCloseCalls += 1;
					return Promise.reject(closeError);
				}),
			);

			survivor.lastActivity = Date.now();
			stale.lastActivity = 0;
			streams.forcePruneInbound();

			expect(staleCloseCalls).to.equal(1);
			expect(streams.inboundStreams).to.deep.equal([survivor]);
			await delay(25);
			expect(unhandled).to.deep.equal([]);
		} finally {
			process.off("unhandledRejection", onUnhandledRejection);
			await streams.close();
		}
	});
});
