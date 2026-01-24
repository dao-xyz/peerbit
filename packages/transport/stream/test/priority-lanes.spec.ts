import { type PeerId } from "@libp2p/interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { PeerStreams } from "../src/index.js";

class TestOutboundStream extends EventTarget {
	id = "test-outbound";
	sentPayloads: number[] = [];
	private expectedLength?: number;
	private currentPayloadLength = 0;

	send(bytes: Uint8Array): boolean {
		let completedFrame = false;

		for (const byte of bytes) {
			if (this.expectedLength == null) {
				// 1-byte varint length prefix (our test payloads are <= 127 bytes)
				this.expectedLength = byte;
				this.currentPayloadLength = 0;
				continue;
			}

			this.currentPayloadLength += 1;
			if (this.currentPayloadLength === this.expectedLength) {
				this.sentPayloads.push(byte);
				this.expectedLength = undefined;
				this.currentPayloadLength = 0;
				completedFrame = true;
			}
		}

		// Block at framed-message boundaries so the test can deterministically interleave writes.
		return completedFrame ? false : true;
	}
}

describe("priority lanes", () => {
	it("preempts queued low-priority traffic with higher priority", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "test-peer" } as unknown as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-conn",
		});

		const outbound = new TestOutboundStream();
		await streams.attachOutboundStream(outbound as any);

		// Queue a backlog of low-priority messages first.
		for (let i = 0; i < 5; i++) {
			streams.write(new Uint8Array([i + 1]), 0);
		}

		await waitForResolved(() =>
			expect(outbound.sentPayloads.length).to.equal(1),
		);
		expect(outbound.sentPayloads[0]).to.equal(1);

		// While the stream is blocked on drain, enqueue a high-priority message.
		streams.write(new Uint8Array([200]), 3);

		// Allow the next frame to be flushed.
		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads.length).to.equal(2),
		);

		// The high-priority message should jump ahead of the remaining low-priority backlog.
		expect(outbound.sentPayloads[1]).to.equal(200);

		// Drain the rest (sanity).
		for (let expected = 3; expected <= 6; expected++) {
			outbound.dispatchEvent(new Event("drain"));
			await waitForResolved(() =>
				expect(outbound.sentPayloads.length).to.equal(expected),
			);
		}

		await streams.close();
	});
});
